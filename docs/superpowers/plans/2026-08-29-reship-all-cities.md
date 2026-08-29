# 全 298 都市を変換し直して送り直す 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `scripts/reimport/reship_all.sh` を足し、298 都市すべてを変換し直して送り直せるようにする。

**Architecture:** `ship_all.sh` を呼ぶだけのスクリプトを 1 本足す。やり直しに固有の 3 つ（送信済みの記録を初回だけ空にする、macOS のスリープを抑止する、失敗した都市を 1 回だけやり直す）だけを行い、変換と転送には手を入れない。初回かどうかは印のファイルの有無で判断する。

**Tech Stack:** bash、既存の `ship_all.sh` と `ship.env`、pytest（`subprocess` で bash を実行する形）。

## Global Constraints

- 設計文書: `docs/superpowers/specs/2026-08-29-reship-all-cities-design.md`
- `ship_all.sh` と `ship_city.sh` は変更しない
- サーバ側は変更しない。手元の処理は転送までで終わる
- 終了コードは `ship_all.sh` のものをそのまま返す（0 成功、1 一部失敗、2 ディスク不足、3 設定の誤り、4 一覧の転送失敗）
- 公開リポジトリなので、サーバのホスト名と内部の場所をスクリプトに書かない。
  すべて `ship.env`（Git 管理外）から読む
- 既存のテストを壊さない
- 日本語のコメントは、実際の動作を示す語で書く。比喩の動詞を使わない

---

### Task 1: 骨組みと設定の検査

**Files:**
- Create: `scripts/reimport/reship_all.sh`
- Create: `tests/test_reship_all.py`

**Interfaces:**
- Consumes: `SHIP_ENV`（既定は `$HERE/ship.env`）、その中の `WORK_ROOT`、`SHIPPED_TXT`、`SHIP_HOST`
- Produces: `ship.env` が無ければ、または必須の値が欠けていれば、何もせずに終了コード 3 で止まる

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reship_all.py` を作ります。

```python
"""reship_all.sh の初回準備と再実行を固定する。"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
RESHIP_ALL = REPO / 'scripts' / 'reimport' / 'reship_all.sh'


def _stub(bin_dir: Path, name: str, body: str):
    p = bin_dir / name
    p.write_text('#!/usr/bin/env bash\n' + body + '\n')
    p.chmod(0o755)
    return p


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    work = tmp_path / 'work'
    work.mkdir()
    shipped = tmp_path / 'shipped.txt'
    called = tmp_path / 'called.txt'

    # 既定は 1 周で全部成功する ship_all.sh。呼ばれた回数を called.txt に積む。
    _stub(bin_dir, 'ship_all_stub', 'echo call >> "%s"\nexit 0' % called)

    ship_env = tmp_path / 'ship.env'
    ship_env.write_text(
        'WORK_ROOT="%s"\n'
        'SHIPPED_TXT="%s"\n'
        'SHIP_HOST="stubhost"\n'
        'SHIP_PATH="/stub/import"\n'
        % (work, shipped)
    )

    run_env = dict(os.environ)
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])
    run_env['SHIP_ENV'] = str(ship_env)
    run_env['SHIP_ALL_CMD'] = str(bin_dir / 'ship_all_stub')

    class Env:
        pass

    e = Env()
    e.bin = bin_dir
    e.work = work
    e.shipped = shipped
    e.called = called
    e.ship_env = ship_env
    e.run_env = run_env
    e.tmp = tmp_path
    e.marker = work / 'reship_in_progress'
    return e


def _run(e):
    return subprocess.run(['bash', str(RESHIP_ALL)],
                          env=e.run_env, capture_output=True, text=True,
                          timeout=60)


def _calls(e):
    if not e.called.exists():
        return 0
    return len([x for x in e.called.read_text().splitlines() if x])


class TestConfigCheck:
    """設定が無い、または必須の値が欠けていれば、何もせずに 3 で止まる。"""

    def test_stops_when_ship_env_is_missing(self, env):
        env.ship_env.unlink()
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 0

    def test_stops_when_work_root_is_missing(self, env):
        env.ship_env.write_text('SHIPPED_TXT="%s"\nSHIP_HOST="h"\n' % env.shipped)
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 0

    def test_stops_when_shipped_txt_is_missing(self, env):
        env.ship_env.write_text('WORK_ROOT="%s"\nSHIP_HOST="h"\n' % env.work)
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 0
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -q`
Expected: `reship_all.sh` がまだ無いので、bash が「No such file or directory」で終了コード 127 を返し、
`assert r.returncode == 3` が FAIL します。

- [ ] **Step 3: 実装を書く**

`scripts/reimport/reship_all.sh` を作ります。

```bash
#!/usr/bin/env bash
# 全都市を変換し直して送り直す。
#
#     reship_all.sh
#
# ship_all.sh を呼ぶ前に shipped.txt を空にして、計画の全都市を対象に戻す。
# 空にするのは初回だけで、印のファイルが残っているあいだは続きから進む。
#
# 設定は ship.env から読む。場所は環境変数 SHIP_ENV で変えられる。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SHIP_ENV:=$HERE/ship.env}"
if [ ! -f "$SHIP_ENV" ]; then
  echo "設定が無い: $SHIP_ENV (ship.env.example を複製する)" >&2
  exit 3
fi
# shellcheck disable=SC1090
. "$SHIP_ENV"

: "${SHIP_ALL_CMD:=bash $HERE/ship_all.sh}"
# スリープを抑止するコマンド。テストでは存在しない名前に差し替えて、
# 無い環境での動きを確かめる。
: "${CAFFEINATE_BIN:=caffeinate}"

# 必須の値は個別に確かめる。${VAR:?} は終了コード 1 を返すので、
# 設定の誤りを表す 3 と区別が付かなくなる。
for v in WORK_ROOT SHIPPED_TXT SHIP_HOST; do
  eval "val=\${$v:-}"
  if [ -z "$val" ]; then
    echo "$v が未設定 ($SHIP_ENV を確認する)" >&2
    exit 3
  fi
done

if ! mkdir -p "$WORK_ROOT"; then
  echo "WORK_ROOT を作れない (値: $WORK_ROOT)" >&2
  exit 3
fi

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] $*"; }

say "=== reship_all 開始 ==="
```

実行できるようにします。

```bash
chmod +x scripts/reimport/reship_all.sh
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -q`
Expected: 3 passed

- [ ] **Step 5: コミット**

```bash
git add scripts/reimport/reship_all.sh tests/test_reship_all.py
git commit -m "feat(reimport): reship_all.sh の骨組みと設定の検査を足す"
```

---

### Task 2: 初回だけ送信済みの記録を空にする

**Files:**
- Modify: `scripts/reimport/reship_all.sh`（`say "=== reship_all 開始 ==="` の後ろに追加）
- Test: `tests/test_reship_all.py`

**Interfaces:**
- Consumes: Task 1 の `WORK_ROOT`、`SHIPPED_TXT`、`say`
- Produces: 印のファイル `$WORK_ROOT/reship_in_progress`。
  中身は 1 行目が開始時刻、2 行目が改名した `shipped.txt` の名前。
  変数 `STAMP`（`YYYYmmdd-HHMMSS`）を後続のタスクでも使う。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reship_all.py` の末尾に足します。

```python
class TestFirstRun:
    """印が無ければ初回。shipped.txt を改名して空にし、印を置く。"""

    def test_renames_shipped_and_leaves_it_empty(self, env):
        env.shipped.write_text('01100 50\n01202 30\n')
        r = _run(env)
        assert r.returncode == 0
        assert env.shipped.read_text() == ''
        backups = sorted(env.tmp.glob('shipped.txt.*'))
        assert len(backups) == 1, '改名した控えが 1 つだけ残る'
        assert backups[0].read_text() == '01100 50\n01202 30\n'

    def test_writes_the_marker(self, env):
        env.shipped.write_text('01100 50\n')
        _run(env)
        assert env.marker.exists()
        lines = env.marker.read_text().splitlines()
        assert lines[0].startswith('開始 ')
        assert 'shipped.txt.' in lines[1]

    def test_works_when_shipped_does_not_exist(self, env):
        # shipped.txt がまだ無い状態でも止まらない。控えは作らない。
        assert not env.shipped.exists()
        r = _run(env)
        assert r.returncode == 0
        assert env.shipped.read_text() == ''
        assert sorted(env.tmp.glob('shipped.txt.*')) == []


class TestSecondRun:
    """印があれば再実行。shipped.txt に触らない。"""

    def test_keeps_shipped_when_marker_exists(self, env):
        env.marker.write_text('開始 2026-08-29 00:00:00\n退避 shipped.txt.old\n')
        env.shipped.write_text('01100 50\n01202 30\n')
        r = _run(env)
        assert r.returncode == 0
        assert env.shipped.read_text() == '01100 50\n01202 30\n'
        assert sorted(env.tmp.glob('shipped.txt.*')) == []

    def test_does_not_overwrite_the_marker(self, env):
        original = '開始 2026-08-29 00:00:00\n退避 shipped.txt.old\n'
        env.marker.write_text(original)
        _run(env)
        assert env.marker.read_text() == original
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -k "FirstRun or SecondRun" -q`
Expected: `test_renames_shipped_and_leaves_it_empty` が
`assert env.shipped.read_text() == ''` で FAIL します（まだ何もしていないので中身が残る）。

- [ ] **Step 3: 実装を書く**

`say "=== reship_all 開始 ==="` の行を、次に差し替えます。

```bash
MARKER="$WORK_ROOT/reship_in_progress"
STAMP=$(date '+%Y%m%d-%H%M%S')

say "=== reship_all 開始 ==="

if [ -e "$MARKER" ]; then
  # 前回の実行が途中で終わっている。shipped.txt には送信済みの都市が
  # 入っているので、そのまま ship_all.sh に飛ばさせる。
  say "前回の続きから進む ($(head -1 "$MARKER"))"
else
  # 初回。送信済みの記録を控えに移してから空にする。
  # これをしないと ship_all.sh が 298 都市すべてを飛ばして 1 都市も送らない。
  BACKUP_NAME="(元から無し)"
  if [ -e "$SHIPPED_TXT" ]; then
    BACKUP="$SHIPPED_TXT.$STAMP"
    if ! mv "$SHIPPED_TXT" "$BACKUP"; then
      say "中止: shipped.txt を改名できない ($SHIPPED_TXT)"
      exit 3
    fi
    BACKUP_NAME=$(basename "$BACKUP")
    say "shipped.txt を $BACKUP_NAME に改名した"
  fi
  if ! : > "$SHIPPED_TXT"; then
    say "中止: shipped.txt を作れない ($SHIPPED_TXT)"
    exit 3
  fi
  printf '開始 %s\n退避 %s\n' "$(ts)" "$BACKUP_NAME" > "$MARKER"
  say "やり直しを開始する。計画の全都市が対象になる"
fi
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -q`
Expected: 8 passed

- [ ] **Step 5: コミット**

```bash
git add scripts/reimport/reship_all.sh tests/test_reship_all.py
git commit -m "feat(reimport): 初回だけ送信済みの記録を空にする"
```

---

### Task 3: ship_all.sh を実行し、失敗したら 1 回だけやり直す

**Files:**
- Modify: `scripts/reimport/reship_all.sh`（末尾に追加）
- Test: `tests/test_reship_all.py`

**Interfaces:**
- Consumes: Task 1 の `SHIP_ALL_CMD`、Task 2 の `MARKER`
- Produces: 変数 `EXIT` に最後の実行の終了コードが入る。スクリプトはこの値で終わる。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reship_all.py` の末尾に足します。

```python
def _stub_exits(env, *codes):
    """呼ばれるたびに codes の順で終了コードを返す ship_all.sh に差し替える。"""
    seq = env.tmp / 'seq.txt'
    seq.write_text('\n'.join(str(c) for c in codes) + '\n')
    _stub(env.bin, 'ship_all_stub',
          'echo call >> "%s"\n'
          'N=$(wc -l < "%s" | tr -d " ")\n'
          'I=$(wc -l < "%s" | tr -d " ")\n'
          'CODE=$(sed -n "${I}p" "%s")\n'
          '[ -z "$CODE" ] && CODE=$(sed -n "${N}p" "%s")\n'
          'exit "$CODE"' % (env.called, seq, env.called, seq, seq))


class TestRetry:
    """1 周目が一部失敗 (1) のときだけ、もう 1 度だけ実行する。"""

    def test_runs_once_when_everything_succeeds(self, env):
        _stub_exits(env, 0)
        r = _run(env)
        assert r.returncode == 0
        assert _calls(env) == 1

    def test_runs_twice_when_some_cities_fail(self, env):
        _stub_exits(env, 1, 0)
        r = _run(env)
        assert r.returncode == 0, '2 周目で全部成功したので 0 になる'
        assert _calls(env) == 2

    def test_stops_after_the_second_round(self, env):
        _stub_exits(env, 1, 1)
        r = _run(env)
        assert r.returncode == 1
        assert _calls(env) == 2, '3 周目は実行しない'

    def test_does_not_retry_on_disk_shortage(self, env):
        _stub_exits(env, 2)
        r = _run(env)
        assert r.returncode == 2
        assert _calls(env) == 1, 'ディスク不足はやり直しても直らない'

    def test_does_not_retry_on_config_error(self, env):
        _stub_exits(env, 3)
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 1

    def test_does_not_retry_on_transfer_failure(self, env):
        _stub_exits(env, 4)
        r = _run(env)
        assert r.returncode == 4
        assert _calls(env) == 1
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -k TestRetry -q`
Expected: `test_runs_once_when_everything_succeeds` が
`assert _calls(env) == 1` で FAIL します（まだ 1 度も実行していないので 0 になる）。

- [ ] **Step 3: 実装を書く**

`reship_all.sh` の末尾に足します。

```bash
say "=== 1 周目 ==="
$SHIP_ALL_CMD
EXIT=$?
say "1 周目の終了コード $EXIT"

# 1 は「一部の都市が失敗した」。配布元が一時的に応答しなかった都市は、
# もう 1 度実行すると成功することがある。成功した都市は shipped.txt に
# 入っているので、2 周目は失敗した都市だけを処理する。
# 2 (ディスク不足)、3 (設定の誤り)、4 (一覧の転送失敗) はやり直しても
# 同じ結果になるので、1 周で終わる。
if [ "$EXIT" -eq 1 ]; then
  say "=== 2 周目 (失敗した都市だけ) ==="
  $SHIP_ALL_CMD
  EXIT=$?
  say "2 周目の終了コード $EXIT"
fi
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -q`
Expected: 14 passed

- [ ] **Step 5: コミット**

```bash
git add scripts/reimport/reship_all.sh tests/test_reship_all.py
git commit -m "feat(reimport): 失敗した都市を 1 回だけやり直す"
```

---

### Task 4: 印の後始末と、次に実行する手順の表示

**Files:**
- Modify: `scripts/reimport/reship_all.sh`（末尾に追加）
- Modify: `scripts/reimport/ship.env.example`（`REIMPORT_DONE_PATH` を足す）
- Test: `tests/test_reship_all.py`

**Interfaces:**
- Consumes: Task 3 の `EXIT`、Task 2 の `MARKER`、`SHIP_HOST`、任意の `REIMPORT_DONE_PATH`
- Produces: 全都市が成功したときだけ印を消す。終了コード `EXIT` で終わる。

**なぜ済みの記録の話を出すか:** サーバには 153 都市が済みとして記録されています。
消さずに取り込みを起動すると、その 153 都市は飛ばされて古いデータのまま残ります。
場所は `ship.env` から読み、公開リポジトリには書きません。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reship_all.py` の末尾に足します。

```python
class TestFinish:
    """印は全都市が成功したときだけ消す。次の手順を表示する。"""

    def test_removes_the_marker_on_success(self, env):
        _stub_exits(env, 0)
        _run(env)
        assert not env.marker.exists()

    def test_keeps_the_marker_when_some_cities_fail(self, env):
        _stub_exits(env, 1, 1)
        _run(env)
        assert env.marker.exists(), '続きから進めるように印を残す'

    def test_keeps_the_marker_on_disk_shortage(self, env):
        _stub_exits(env, 2)
        _run(env)
        assert env.marker.exists()

    def test_tells_how_to_resume_when_it_failed(self, env):
        _stub_exits(env, 1, 1)
        r = _run(env)
        assert '再実行' in r.stdout

    def test_mentions_clearing_the_done_record(self, env):
        _stub_exits(env, 0)
        r = _run(env)
        assert 'done.txt' in r.stdout
        assert '153' in r.stdout or '済み' in r.stdout

    def test_prints_the_concrete_command_when_the_path_is_configured(self, env):
        env.ship_env.write_text(
            env.ship_env.read_text()
            + 'REIMPORT_DONE_PATH="/stub/logs/done.txt"\n')
        _stub_exits(env, 0)
        r = _run(env)
        assert '/stub/logs/done.txt' in r.stdout
        assert 'stubhost' in r.stdout

    def test_hides_the_path_when_it_is_not_configured(self, env):
        _stub_exits(env, 0)
        r = _run(env)
        assert 'REIMPORT_DONE_PATH' in r.stdout, '未設定であることを伝える'
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -k TestFinish -q`
Expected: `test_removes_the_marker_on_success` が
`assert not env.marker.exists()` で FAIL します（まだ消していないので残る）。

- [ ] **Step 3: 実装を書く**

`reship_all.sh` の末尾に足します。

```bash
if [ "$EXIT" -eq 0 ]; then
  rm -f "$MARKER"
  say "=== 全都市の送信が終わった ==="
  TARGETS=$(ls -t "$WORK_ROOT"/reimport_targets_*.txt 2>/dev/null | head -1)
  if [ -n "$TARGETS" ]; then
    say "対象一覧: $(basename "$TARGETS") ($(grep -c . "$TARGETS") 都市)"
  fi
  echo
  echo "次にサーバで取り込みを起動する。"
  echo "その前に、サーバの done.txt を別の名前に改名する必要がある。"
  echo "153 都市が済みとして記録されており、消さないとその都市は飛ばされ、"
  echo "古いデータのまま残る。"
  echo
  if [ -n "${REIMPORT_DONE_PATH:-}" ]; then
    echo "  ssh $SHIP_HOST \"mv '$REIMPORT_DONE_PATH' '$REIMPORT_DONE_PATH.\$(date +%Y%m%d-%H%M%S)'\""
    echo "  ssh $SHIP_HOST 'bash ~/start_reimport.sh'"
  else
    echo "  ship.env に REIMPORT_DONE_PATH が未設定のため、コマンドを表示しない。"
    echo "  サーバの done.txt を退けてから start_reimport.sh を実行する。"
  fi
else
  say "=== 終了コード $EXIT で終わった。印は残す ==="
  say "同じコマンドで再実行すれば、送信済みの都市を飛ばして続きから進む"
fi

exit "$EXIT"
```

- [ ] **Step 4: `ship.env.example` に項目を足す**

末尾に足します。

```bash
# サーバの済みの記録の場所。reship_all.sh が終わったときに、
# これを改名するコマンドを表示するために使う。
# 未設定でも動くが、そのときは場所を伏せた案内だけを表示する。
REIMPORT_DONE_PATH=""
```

- [ ] **Step 5: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -q`
Expected: 21 passed

- [ ] **Step 6: コミット**

```bash
git add scripts/reimport/reship_all.sh scripts/reimport/ship.env.example tests/test_reship_all.py
git commit -m "feat(reimport): 印を後始末し、次にサーバで実行する手順を表示する"
```

---

### Task 5: スリープの抑止とログ

**Files:**
- Modify: `scripts/reimport/reship_all.sh`（`STAMP` を決めている行の直後と、1 周目の直前）
- Test: `tests/test_reship_all.py`

**Interfaces:**
- Consumes: Task 1 の `CAFFEINATE_BIN`、Task 2 の `STAMP`、`WORK_ROOT`、`say`
- Produces: `$WORK_ROOT/reship_<STAMP>.log` に画面と同じ内容が残る。
  環境変数 `RESHIP_LOG` が設定されているときは、既にログを通した後だと判断して二重に実行しない。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reship_all.py` の末尾に足します。

```python
class TestSleepAndLog:
    """スリープの抑止は、あれば使い、無ければ警告して続ける。"""

    def test_calls_caffeinate_when_available(self, env):
        marker = env.tmp / 'caffeinate_called.txt'
        _stub(env.bin, 'caffeinate', 'echo "$@" >> "%s"' % marker)
        # caffeinate は背景で実行するので、ship_all.sh を待たせて時間を作る。
        # 待たせないと、書き込みの前にスクリプトが終わって結果が揺れる。
        _stub(env.bin, 'ship_all_stub',
              'echo call >> "%s"\nsleep 1\nexit 0' % env.called)
        r = _run(env)
        assert r.returncode == 0
        assert marker.exists(), 'caffeinate が呼ばれていない'
        assert '-i' in marker.read_text()

    def test_continues_without_caffeinate(self, env):
        env.run_env['CAFFEINATE_BIN'] = 'caffeinate_not_installed'
        r = _run(env)
        assert r.returncode == 0
        assert '警告' in r.stdout
        assert 'caffeinate_not_installed' in r.stdout

    def test_writes_a_log_file(self, env):
        _run(env)
        logs = sorted(env.work.glob('reship_*.log'))
        assert len(logs) == 1
        assert 'reship_all 開始' in logs[0].read_text()

    def test_the_log_holds_the_same_lines_as_the_screen(self, env):
        r = _run(env)
        log = sorted(env.work.glob('reship_*.log'))[0].read_text()
        assert log == r.stdout
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -k TestSleepAndLog -q`
Expected: `test_writes_a_log_file` が `assert len(logs) == 1` で FAIL します（ログをまだ書いていないので 0 になる）。

- [ ] **Step 3: ログの実装を書く**

`STAMP=$(date '+%Y%m%d-%H%M%S')` の直後、`say "=== reship_all 開始 ==="` の直前に足します。

```bash
# 画面とファイルの両方に記録を残す。自分自身をもう一度実行して tee に通す。
# exec > >(tee ...) の形は、スクリプトが終わるときに tee が書き終える前に
# 切れることがあり、ログの末尾が欠ける。パイプなら最後まで書き切る。
# 終了コードは PIPESTATUS[0] から取るので、tee の成否に影響されない。
if [ -z "${RESHIP_LOG:-}" ]; then
  export RESHIP_LOG="$WORK_ROOT/reship_$STAMP.log"
  bash "$0" "$@" 2>&1 | tee -a "$RESHIP_LOG"
  exit "${PIPESTATUS[0]}"
fi
```

- [ ] **Step 4: スリープの抑止を書く**

`say "=== 1 周目 ==="` の直前に足します。

```bash
# 手元が macOS だと、無操作のままスリープして処理が止まる。前回の送信では
# 30 分を超える中断が 10 回、合計 10.1 時間あった (実働は 12.6 時間)。
# -w に自分のプロセス ID を渡すので、このスクリプトが終われば抑止も解ける。
if command -v "$CAFFEINATE_BIN" > /dev/null 2>&1; then
  "$CAFFEINATE_BIN" -i -w $$ &
  say "スリープを抑止する ($CAFFEINATE_BIN pid=$!)"
else
  # macOS 以外には caffeinate が無い。抑止できないことは、
  # やり直しを止める理由にならない。
  say "警告: $CAFFEINATE_BIN が無い。スリープを抑止せずに続ける"
fi
```

- [ ] **Step 5: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reship_all.py -q`
Expected: 25 passed

- [ ] **Step 6: 全体のテストを走らせる**

Run: `python3 -m pytest -q`
Expected: 529 passed, 26 skipped（main の 504 件に 25 件が増える）

- [ ] **Step 7: コミット**

```bash
git add scripts/reimport/reship_all.sh tests/test_reship_all.py
git commit -m "feat(reimport): スリープを抑止し、実行の記録をログに残す"
```

---

### Task 6: 手引きへの追記と、公開前の確認

**Files:**
- Modify: `scripts/reimport/README.md`

- [ ] **Step 1: README に節を足す**

`ship_all.sh` の説明の後ろに足します。

```markdown
## 全都市をやり直す

規則を変えたあと、既に送った都市にもそれを効かせたいときに使う。

    bash scripts/reimport/reship_all.sh

`shipped.txt` を控えに移してから空にするので、計画の全都市が対象に戻る。
空にするのは初回だけで、`WORK_ROOT/reship_in_progress` が残っているあいだは
`ship_all.sh` の通常の飛ばしで続きから進む。

途中で止まったら、同じコマンドをもう一度実行する。

実行中は macOS のスリープを抑止する。
`caffeinate` が無い環境では警告を出して抑止せずに続ける。

1 周目で一部の都市が失敗したときは、もう 1 度だけ実行して失敗した都市を処理する。
それでも失敗した都市は一覧に出す。

終わったら、サーバで取り込みを起動する手順を表示する。
サーバの済みの記録を改名する必要があるので、その手順も含める。
```

- [ ] **Step 2: 機微情報の確認**

検査に使う文字列そのものを残さないため、形で見ます。
接続先の名前など、形では見つけられないものは、実行するときに手元のメモから足します。

```bash
git diff main -U0 | grep -nE '(^\+.*([0-9]{1,3}\.){3}[0-9]{1,3})|(^\+.*/(opt|var|etc|home)/)|(^\+.*sudo )|(^\+.*postgresql://)' || echo "当たりなし"
```

Expected: 「当たりなし」

- [ ] **Step 3: コミットメッセージの確認**

```bash
git log main..HEAD --format='%s%n%b' | grep -nE '(([0-9]{1,3}\.){3}[0-9]{1,3})|(/(opt|var|etc|home)/)|(sudo )|(postgresql://)' || echo "当たりなし"
```

Expected: 「当たりなし」

- [ ] **Step 4: 全体のテストと変更の一覧**

```bash
python3 -m pytest -q 2>&1 | tail -3 && git log --oneline main..HEAD
```

Expected: 529 passed, 26 skipped と、Task 1 から Task 6 までのコミット。

- [ ] **Step 5: コミット**

```bash
git add scripts/reimport/README.md
git commit -m "docs(reimport): 全都市をやり直す手順を手引きに足す"
```

---

## この計画が扱わないこと

サーバでの取り込みは含みません。
`reship_all.sh` は転送までで終わり、取り込みの起動は人が行います。

サーバの済みの記録を消す処理も含みません。
手順を表示するだけで、実行はしません。

`ckan_download_plan.csv` の未コミットの 150 行は、この計画では触りません。
やり直しを始める前に、利用者がコミットするかどうかを判断します。
