# 手元で変換してサーバへ送る取り込みの実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 148 都市を手元で変換してサーバへ送り、サーバのバッチが取り込む経路を、リポジトリの中に作る。

**Architecture:** 手元側は `scripts/reimport/` にシェル 2 本と設定の見本を足す。サーバ側は `deploy/` を新設し、いまサーバの home にしか無い 3 本を、決めた変更を入れて置く。取り込み器 (`plateau_importer2postgis.py`) には触らない。

**Tech Stack:** bash、Python 3、pytest、rsync、citygml-osm 3.0.6 (Java 17)

設計: `docs/superpowers/specs/2026-08-13-local-conversion-reimport-design.md`
関連 Issue: [#21](https://github.com/nyampire/rapid_plateau_api/issues/21)

## Global Constraints

- **新しいサーバはまだ契約していない。**この計画は契約前にできる範囲だけを扱う。設計の検証 3 (大都市の所要時間とピーク RSS) と検証 6 (148 都市の本番実行) は含まない
- 取り込み器 `plateau_importer2postgis.py` と `plateau_coverage.py` と `plateau_purge.py` には一切触らない。設計が「この設計の外に出したもの」に挙げた 2 件 (行政界フィルタを最終バッチに寄せる、累積する dict の解放) もこの計画の対象外
- `scripts/reimport/` の既存 4 本 (`httpzip.py` / `build_download_plan.py` / `scan_bldg.py` / `extract_city.py`) は変更しない
- **公開リポジトリに実サーバの識別子を書かない。**ホスト名、SSH の別名、`/opt` や `/home` で始まる実在のパスをスクリプトにも `deploy/README.md` にも書かない。すべて `ship.env` と環境変数から読む。`ship.env` 自体は `.gitignore` に入れる
- シェルは `#!/usr/bin/env bash` と `set -uo pipefail` で始める。`set -e` は使わない (既存の 3 本に合わせる。各段の終了コードを明示的に見る)
- `df` は GNU 拡張の `--output=avail` を使わない。`df -k <path> | awk 'NR==2 {print $4}'` を使う。macOS と Linux の両方で 4 列目が 1K ブロックの空きになる
- **コマンドの出力やファイルの中身を数値として比較する前に、必ず `need_int` を通す。**
  空文字のまま `[ "$x" -ne "$y" ]` を評価すると `integer expression expected` でエラー終了し、
  `if` はそれを偽として扱う。分岐が黙って消えるので、門は「入力が壊れているときに限って」効かなくなる。
  この計画で 3 回踏んだ (`MESHES`、`REMOTE_N`、`WANT`)。
  `find | wc -l` や `grep -c` のように必ず数値を返すものは対象外でよい。
  コマンドの終了コードも `$?` に取って別に確かめる。`need_int` だけでは、
  失敗したコマンドがたまたま数値を吐いた場合を捕まえられない
- **変数展開の直後に全角文字が続くときは `${VAR}` と書く。**
  bash 3.2 は `LANG=ja_JP.UTF-8` のとき、全角文字の先頭バイトを識別子の一部として読む。
  `"exit $SSH_EXIT、出力"` は `SSH_EXIT\xe3: unbound variable` になり、`set -u` の下で落ちる。
  `LANG` が空や `C` のときは起きないので、環境によって出たり出なかったりする。
  メッセージが日本語である以上どこにでも現れうるので、commit の前に
  `grep -nP '\$\{?[A-Za-z_][A-Za-z0-9_]*\}?[^\x00-\x7F]'` で洗う
- 終了コードの割り当て。`2` はディスク不足でバッチ全体を止める合図なので、他の用途に使わない

| コード | 意味 |
|---|---|
| 0 | 成功 |
| 1 | 1 都市以上が失敗 (`ship_all.sh`) |
| 2 | ディスク不足 (バッチ全体を止める) |
| 3 | 設定か計画がおかしい (`ship_all.sh`) |
| 4 | 一覧をサーバへ置けなかった (`ship_all.sh`) |
| 10 | 取り出しの門で不一致 |
| 11 | 変換の門で不一致 |
| 12 | 転送の門で不一致 |
| 13 | 入力が無い、または `manifest.txt` と枚数が合わない |

- テストの実行は `python3 -m pytest`。シェルのテストは `subprocess` でスクリプトを実行し、外部コマンド (`java` / `rsync` / `ssh`) は `PATH` に置いた偽物に差し替える
- コミットは既存の履歴に合わせ、`Co-Authored-By` の trailer を付けない
- コミットメッセージは日本語。`docs/superpowers/specs/` の規範に従い、一文ごとに改行する

---

## ファイル構成

```
scripts/reimport/
  ship.env.example       新規  設定の見本。実物の ship.env は gitignore
  ship_city.sh           新規  1 都市を取り出し、変換し、送る
  ship_all.sh            新規  一覧を回す。再開つき

deploy/
  reimport_one.sh        新規  取り込み + 掃除 (4 段から 2 段へ)
  reimport_batch.sh      新規  一覧のパスを引数化 + 残存確認
  reimport_watchdog.sh   新規  wrapper の判定パスを環境変数化
  README.md              新規  置き方と運用手順

tests/
  test_ship_city.py      新規  ship_city.sh の門と失敗時の振る舞い
  test_ship_all.py       新規  ship_all.sh の再開と件数確認
  test_reimport_one.py       新規  reimport_one.sh の分岐
  test_reimport_batch.py     新規  一覧の受け取りと done.txt の照合
  test_reimport_watchdog.py  新規  wrapper の判定
```

`DEPLOY.md` を新規構築と更新の 2 部に分ける作業は Task 6 に含む。

---

### Task 1: 設定の見本と、`ship_city.sh` の取り出しの門

**Files:**
- Create: `scripts/reimport/ship.env.example`
- Create: `scripts/reimport/ship_city.sh`
- Create: `tests/test_ship_city.py`
- Modify: `.gitignore`

**Interfaces:**
- Produces: `ship_city.sh <citycode>`。環境変数 `SHIP_ENV` で設定ファイルの場所を差し替えられる
- Produces: 設定の名前 `EXTRACT_CMD` / `JAVA_BIN` / `CITYGML_OSM_JAR` / `CONVERSION_JSON` / `WORK_ROOT` / `SHIP_HOST` / `SHIP_PATH` / `SHIPPED_TXT` / `DISK_MIN_KB`
- Produces: 終了コード 10 (取り出しの門)

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_ship_city.py` を新規に作る。

```python
"""ship_city.sh の門と失敗時の振る舞いを固定する。

外部コマンドは PATH に置いた偽物に差し替える。通信も変換もしない。
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SHIP_CITY = REPO / 'scripts' / 'reimport' / 'ship_city.sh'


def _stub(bin_dir: Path, name: str, body: str):
    """PATH に置く偽コマンドを作る。"""
    p = bin_dir / name
    p.write_text('#!/usr/bin/env bash\n' + body + '\n')
    p.chmod(0o755)
    return p


@pytest.fixture
def env(tmp_path):
    """ship_city.sh を走らせるための一式を用意する。"""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    work_root = tmp_path / 'work'
    work_root.mkdir()
    conversion = tmp_path / 'conversion.json'
    conversion.write_text('{}')
    jar = tmp_path / 'fake.jar'
    jar.write_text('')
    shipped = tmp_path / 'shipped.txt'

    ship_env = tmp_path / 'ship.env'
    ship_env.write_text(
        'EXTRACT_CMD="%s/extract_stub"\n'
        'JAVA_BIN="%s/java"\n'
        'CITYGML_OSM_JAR="%s"\n'
        'CONVERSION_JSON="%s"\n'
        'WORK_ROOT="%s"\n'
        'SHIP_HOST="stubhost"\n'
        'SHIP_PATH="/stub/import"\n'
        'SHIPPED_TXT="%s"\n'
        'DISK_MIN_KB=0\n'
        % (bin_dir, bin_dir, jar, conversion, work_root, shipped)
    )

    run_env = dict(os.environ)
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])
    run_env['SHIP_ENV'] = str(ship_env)

    class Env:
        pass

    e = Env()
    e.bin = bin_dir
    e.work_root = work_root
    e.shipped = shipped
    e.run_env = run_env
    e.tmp = tmp_path
    return e


def _run(e, city='30406'):
    return subprocess.run(
        ['bash', str(SHIP_CITY), city],
        env=e.run_env, capture_output=True, text=True)


def test_extract_gate_fails_when_file_count_differs(env):
    """報告された meshes より少ない .gml しか出なければ、取り出しの門で落ちる。"""
    # meshes は 2 と報告するが、書き出すのは 1 個だけ
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":2,"raw_bytes":5}\'')

    r = _run(env)

    assert r.returncode == 10, r.stderr
    assert not env.shipped.exists()


def test_extract_gate_passes_when_file_count_matches(env):
    """報告と実ファイル数が合えば、取り出しの門を通る。

    これが無いと「常に exit 10 で落ちる」実装でも上のテストが通ってしまい、
    門が比較していることを何も固定できない。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'printf "<x/>" > "$2/53394501_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":2,"raw_bytes":10}\'')

    r = _run(env)

    assert r.returncode != 10, r.stdout + r.stderr
    assert '報告 2 メッシュ、実ファイル 2' in r.stdout


def test_extract_gate_fails_when_the_report_is_unreadable(env):
    """meshes を読めなければ落ちる。

    MESHES が空のまま比較すると [ は integer expression expected で
    エラー終了し、if がそれを偽として扱うので門をすり抜ける。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo "これは JSON ではない"')

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr


def test_extract_gate_fails_when_meshes_is_not_a_number(env):
    """meshes が数字でなければ落ちる。

    上の 1 件は python3 が非 0 で終わる経路しか通らないので、
    終了コードの判定だけの実装でも通ってしまう。
    JSON として正しく python3 も成功するが値が数字でない場合は、
    need_int でしか捕まらない。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":"たくさん","raw_bytes":5}\'')

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr


def test_extract_gate_uses_the_number_from_the_report(env):
    """比較に使う数が、報告された meshes から来ている。

    他の 3 件はどれも meshes が 2 なので、比較対象を 2 に固定して
    $EXTRACT_JSON を読まない実装でも全部通ってしまう。
    3 メッシュの報告に 3 ファイルを添えると、2 固定の実装はここで落ちる。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'printf "<x/>" > "$2/53394501_bldg_6697_op.gml"\n'
          'printf "<x/>" > "$2/53394502_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":3,"raw_bytes":15}\'')

    r = _run(env)

    assert r.returncode != 10, r.stdout + r.stderr
    assert '報告 3 メッシュ、実ファイル 3' in r.stdout
```

- [ ] **Step 2: テストが落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: FAIL。`ship_city.sh` がまだ無いので `bash: ...: No such file or directory` で returncode 127 になる。

- [ ] **Step 3: 設定の見本を書く**

`scripts/reimport/ship.env.example` を新規に作る。

```bash
# ship_city.sh / ship_all.sh の設定。
# このファイルを ship.env という名前でコピーして書き換える。
# ship.env は .gitignore に入っている。実サーバの識別子をコミットしないため。

# 取り出しのコマンド。既定は同じディレクトリの extract_city.py。
EXTRACT_CMD="python3 $(dirname "${BASH_SOURCE[0]}")/extract_city.py"

# citygml-osm を動かす Java 17 の実行ファイル
JAVA_BIN="/path/to/openjdk@17/bin/java"

# clean な checkout から作った jar。実験ブランチで作ったものは中身が違う
CITYGML_OSM_JAR="/path/to/citygml-osm/target/citygml-osm-3.0.6-jar-with-dependencies.jar"

# citygml-osm のリポジトリにある conversion.json。作業ディレクトリへ複製する
CONVERSION_JSON="/path/to/citygml-osm/conversion.json"

# 都市ごとの作業ディレクトリを作る場所。1 都市ぶんで最大 4.4GB 使う
WORK_ROOT="/path/to/scratch/plateau_ship"

# 転送先。ssh の宛先と、その先のディレクトリ
SHIP_HOST="user@example"
SHIP_PATH="/path/to/plateau_import"

# 送り終えた都市の記録。<citycode> <osm数> を 1 行ずつ
SHIPPED_TXT="$(dirname "${BASH_SOURCE[0]}")/shipped.txt"

# 作業を始める前に要求する空き容量 (1K ブロック)。5GB
DISK_MIN_KB=5242880

# 計画に載っているべき都市の数。合わなければ ship_all.sh が止まる
EXPECTED_CITIES=148
```

- [ ] **Step 4: `.gitignore` に 2 行足す**

`.gitignore` の末尾に追加する。

```
# 手元の設定と進捗 (実サーバの識別子を含む)
scripts/reimport/ship.env
scripts/reimport/shipped.txt
```

- [ ] **Step 5: `ship_city.sh` の取り出しまでを書く**

`scripts/reimport/ship_city.sh` を新規に作る。

```bash
#!/usr/bin/env bash
# 1 都市を取り出し、変換し、サーバへ送る。
#
#     ship_city.sh <citycode>
#
# 設定は ship.env から読む。場所は環境変数 SHIP_ENV で変えられる。
# 段の境目ごとに門があり、通らなければ記録せずに終わる。
set -uo pipefail

CITY="${1:?citycode required}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SHIP_ENV:=$HERE/ship.env}"
if [ ! -f "$SHIP_ENV" ]; then
  echo "設定が無い: $SHIP_ENV (ship.env.example を複製する)" >&2
  exit 1
fi
# shellcheck disable=SC1090
. "$SHIP_ENV"

: "${EXTRACT_CMD:?EXTRACT_CMD が未設定}"
: "${JAVA_BIN:?JAVA_BIN が未設定}"
: "${CITYGML_OSM_JAR:?CITYGML_OSM_JAR が未設定}"
: "${CONVERSION_JSON:?CONVERSION_JSON が未設定}"
: "${WORK_ROOT:?WORK_ROOT が未設定}"
: "${SHIP_HOST:?SHIP_HOST が未設定}"
: "${SHIP_PATH:?SHIP_PATH が未設定}"
: "${SHIPPED_TXT:?SHIPPED_TXT が未設定}"
: "${DISK_MIN_KB:=5242880}"

EXIT_EXTRACT=10
EXIT_CONVERT=11
EXIT_TRANSFER=12

WORK="$WORK_ROOT/$CITY"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] [$CITY] $*"; }

# 失敗した作業ディレクトリは検査用に退避する。
# 消さずに残すだけだと、次の実行が前回の .gml と .osm を数えてしまう。
bail() {
  local code=$1 msg=$2
  say "FAIL: $msg"
  if [ -d "$WORK" ]; then
    local kept="$WORK.failed.$(date '+%Y%m%d-%H%M%S')"
    mv "$WORK" "$kept"
    say "作業ディレクトリを退避した: $kept"
  fi
  exit "$code"
}

disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

# コマンドの出力を数値として比べる前に、必ずこれを通す。
# 空文字のまま [ "$x" -ne "$y" ] を評価すると
# integer expression expected でエラー終了し、if がそれを偽として扱う。
# 分岐が黙って消えるので、門は「壊れているときに限って」効かなくなる。
need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

say "=== START ==="

mkdir -p "$WORK_ROOT"
AVAIL=$(disk_kb "$WORK_ROOT")
if ! need_int "$AVAIL"; then
  say "ABORT: 空き容量を読めない (df の出力: $AVAIL)"
  exit 2
fi
say "空き $AVAIL KB (下限 $DISK_MIN_KB)"
if [ "$AVAIL" -lt "$DISK_MIN_KB" ]; then
  say "ABORT: ディスクが足りない"
  exit 2
fi

# 再試行は必ず空のディレクトリから始める
if [ -e "$WORK" ]; then
  mv "$WORK" "$WORK.stale.$(date '+%Y%m%d-%H%M%S')"
fi
mkdir -p "$WORK"

say "1/5 取り出し"
EXTRACT_JSON=$($EXTRACT_CMD "$CITY" "$WORK")
EXTRACT_EXIT=$?
if [ "$EXTRACT_EXIT" -ne 0 ]; then
  bail "$EXIT_EXTRACT" "extract が exit $EXTRACT_EXIT"
fi
echo "$EXTRACT_JSON"

MESHES=$(echo "$EXTRACT_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["meshes"])')
MESHES_EXIT=$?
if [ "$MESHES_EXIT" -ne 0 ] || ! need_int "$MESHES"; then
  bail "$EXIT_EXTRACT" "meshes を読めない (出力: $EXTRACT_JSON)"
fi
GML_N=$(find "$WORK" -maxdepth 1 -name '*.gml' | wc -l | tr -d ' ')
say "報告 $MESHES メッシュ、実ファイル $GML_N"
if [ "$GML_N" -ne "$MESHES" ]; then
  bail "$EXIT_EXTRACT" ".gml の数が報告と違う ($GML_N != $MESHES)"
fi

say "=== 取り出しまで完了 ==="
```

- [ ] **Step 6: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: PASS (5 passed)

- [ ] **Step 7: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: 既存の 329 passed に 5 件足されて 334 passed、25 skipped

- [ ] **Step 8: コミット**

```bash
git add scripts/reimport/ship.env.example scripts/reimport/ship_city.sh tests/test_ship_city.py .gitignore
git commit -F - <<'MSG'
feat(scripts): 1 都市を送る ship_city.sh の取り出しまでを足す

設計の第 1 段のうち、CKAN からの取り出しと、その門までを書く。
報告された meshes と実ファイル数が合わなければ exit 10 で終わる。

失敗した作業ディレクトリは消さずに退避する。残すだけだと、次の実行が
前回の .gml を数えてしまい、枚数の照合が何度やり直しても合わなくなる。

実サーバの識別子はスクリプトに書かず、ship.env から読む。
ship.env と shipped.txt は .gitignore に入れた。
MSG
```

---

### Task 2: 変換と転送の門、`manifest.txt`、`shipped.txt`

**Files:**
- Modify: `scripts/reimport/ship_city.sh`（Task 1 の末尾「=== 取り出しまで完了 ===」を置き換える）
- Modify: `tests/test_ship_city.py`（テストを追加）

**Interfaces:**
- Consumes: Task 1 の `bail` / `say` / `$WORK` / `$MESHES` / `$GML_N`
- Produces: 転送先に `<SHIP_PATH>/<citycode>/*.osm` と `manifest.txt` を置く
- Produces: `SHIPPED_TXT` へ `<citycode> <osm数>` を追記する
- Produces: 終了コード 11 (変換の門)、12 (転送の門)

- [ ] **Step 1: 失敗するテストを 7 件書く**

`tests/test_ship_city.py` の末尾に追加する。

```python
def _good_extract(e, n=2):
    """n 個の .gml を書き出し、meshes=n を報告する偽 extract。"""
    body = 'mkdir -p "$2"\n'
    for i in range(n):
        body += 'printf "<x/>" > "$2/5339450%d_bldg_6697_op.gml"\n' % i
    body += 'echo \'{"city_code":"30406","meshes":%d,"raw_bytes":5}\'' % n
    _stub(e.bin, 'extract_stub', body)


def _good_java(e):
    """cwd の .gml と同じ数だけ、閉じタグ付きの .osm を書く偽 java。"""
    _stub(e.bin, 'java',
          'for f in *.gml; do\n'
          '  printf "<osm><node/></osm>" > "${f%.gml}.osm"\n'
          'done\n'
          'exit 0')


def _good_transfer(e, remote_count=None):
    """rsync と ssh の偽物。ssh は転送先の枚数を答える。"""
    _stub(e.bin, 'rsync', 'exit 0')
    n = 'echo "$REMOTE_N"' if remote_count is None else 'echo %d' % remote_count
    _stub(e.bin, 'ssh', n)


def test_convert_gate_fails_on_truncated_osm(env):
    """閉じタグの無い .osm があれば、変換の門で落ちる。"""
    _good_extract(env, n=2)
    _stub(env.bin, 'java',
          'printf "<osm><node/></osm>" > 53394500_bldg_6697_op.osm\n'
          'printf "<osm><node" > 53394501_bldg_6697_op.osm\n'
          'exit 0')
    _good_transfer(env)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr


def test_convert_gate_fails_when_java_exits_nonzero(env):
    """java が 0 以外で終われば、変換の門で落ちる。"""
    _good_extract(env, n=2)
    _stub(env.bin, 'java',
          'printf "<osm><node/></osm>" > 53394500_bldg_6697_op.osm\n'
          'printf "<osm><node/></osm>" > 53394501_bldg_6697_op.osm\n'
          'exit 3')
    _good_transfer(env)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr


def test_transfer_gate_fails_when_remote_count_differs(env):
    """転送先の枚数が手元と違えば、転送の門で落ちる。"""
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=1)

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr


def test_records_city_and_count_when_everything_passes(env):
    """全部通れば shipped.txt に <citycode> <osm数> が入り、作業を消す。"""
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.shipped.read_text().strip() == '30406 2'
    assert not (env.work_root / '30406').exists()


def test_transfer_gate_fails_when_the_remote_count_is_unreadable(env):
    """転送先の枚数を数えられなければ落ちる。

    ssh が失敗すると REMOTE_N が空になる。そのまま比較すると
    integer expression expected でエラー終了し、if がそれを偽として扱う。
    門が消え、転送を確かめないまま shipped.txt に記録して作業を消す。
    記録された都市は ship_all.sh が永久に飛ばす。
    """
    _good_extract(env, n=2)
    _good_java(env)
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh', 'echo "ssh: connect failed" >&2\nexit 255')

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_transfer_gate_fails_when_ssh_succeeds_with_junk_output(env):
    """ssh が成功しても、出力が数字でなければ落ちる。

    上の 1 件は ssh が非 0 で終わる経路しか通らないので、
    終了コードの判定だけの実装でも通ってしまう。
    リモートの find がエラーを吐いて標準出力に紛れる形は need_int でしか捕まらない。
    """
    _good_extract(env, n=2)
    _good_java(env)
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh', 'echo "find: No such file or directory"\nexit 0')

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_gates_use_computed_counts_not_constants(env):
    """門が数える値を使っている。定数と比べていない。

    他のテストがどれも 2 メッシュなので、.osm 数も転送先の枚数も 2 に
    固定した実装で通ってしまう。3 メッシュで一通り流して区別する。
    """
    _good_extract(env, n=3)
    _good_java(env)
    _good_transfer(env, remote_count=3)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.shipped.read_text().strip() == '30406 3'
```

- [ ] **Step 2: 6 件が落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: 7 failed, 5 passed。落ちるのは、取り出しまでで終わっているスクリプトが returncode 0 を返すため。

- [ ] **Step 3: 変換と転送を実装する**

`scripts/reimport/ship_city.sh` の末尾の行を置き換える。

削除する行:

```bash
say "=== 取り出しまで完了 ==="
```

追加する内容:

```bash
say "2/5 変換"
cp "$CONVERSION_JSON" "$WORK/conversion.json"
(
  cd "$WORK" || exit 1
  "$JAVA_BIN" -Xmx4096m -Dfile.encoding=utf-8 -jar "$CITYGML_OSM_JAR" 1st
)
JAVA_EXIT=$?
if [ "$JAVA_EXIT" -ne 0 ]; then
  bail "$EXIT_CONVERT" "java が exit $JAVA_EXIT"
fi

OSM_N=$(find "$WORK" -maxdepth 1 -name '*.osm' | wc -l | tr -d ' ')
say ".osm $OSM_N 個"
if [ "$OSM_N" -ne "$GML_N" ]; then
  bail "$EXIT_CONVERT" ".osm の数が .gml と違う ($OSM_N != $GML_N)"
fi

# 数の一致では切り詰めを検出できない。JVM が途中で落ちると、
# 書きかけの .osm も 1 個として数えられる。
# .osm が 1 つも無いとグロブが展開されず、文字列 <WORK>/*.osm が f に入る。
# 存在しないパスに対して [ ! -s ] が真になり、実態と食い違うメッセージで落ちる。
shopt -s nullglob
for f in "$WORK"/*.osm; do
  if [ ! -s "$f" ]; then
    bail "$EXIT_CONVERT" "空のファイル: $(basename "$f")"
  fi
  if ! tail -c 200 "$f" | grep -q '</osm>'; then
    bail "$EXIT_CONVERT" "閉じタグが無い: $(basename "$f")"
  fi
done
shopt -u nullglob

say "3/5 manifest"
echo "$OSM_N" > "$WORK/manifest.txt"

say "4/5 転送"
rsync -az --delete \
  --include='*.osm' --include='manifest.txt' --exclude='*' \
  "$WORK/" "$SHIP_HOST:$SHIP_PATH/$CITY/"
RSYNC_EXIT=$?
if [ "$RSYNC_EXIT" -ne 0 ]; then
  bail "$EXIT_TRANSFER" "rsync が exit $RSYNC_EXIT"
fi

REMOTE_N=$(ssh "$SHIP_HOST" "find '$SHIP_PATH/$CITY' -maxdepth 1 -name '*.osm' | wc -l" | tr -d ' ')
SSH_EXIT=$?
# ssh が失敗すると REMOTE_N が空になる。そのまま比較すると門が消え、
# 転送を確かめないまま shipped.txt に記録して作業ディレクトリを消す。
# 記録された都市は ship_all.sh が永久に飛ばす。
if [ "$SSH_EXIT" -ne 0 ] || ! need_int "$REMOTE_N"; then
  bail "$EXIT_TRANSFER" "転送先の枚数を数えられない (ssh exit ${SSH_EXIT}、出力: ${REMOTE_N})"
fi
say "転送先 $REMOTE_N 個"
if [ "$REMOTE_N" -ne "$OSM_N" ]; then
  bail "$EXIT_TRANSFER" "転送先の枚数が違う ($REMOTE_N != $OSM_N)"
fi

say "5/5 記録して掃除"
echo "$CITY $OSM_N" >> "$SHIPPED_TXT"
rm -rf "$WORK"
say "=== DONE ($OSM_N メッシュ) ==="
```

- [ ] **Step 4: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: 12 passed

- [ ] **Step 5: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: 341 passed、25 skipped

- [ ] **Step 6: コミット**

```bash
git add scripts/reimport/ship_city.sh tests/test_ship_city.py
git commit -F - <<'MSG'
feat(scripts): ship_city.sh に変換と転送の門を足す

.osm の数が .gml と合うことに加えて、各ファイルが空でなく </osm> で
終わっていることを見る。数の一致では切り詰めを検出できない。JVM が
途中で落ちると、書きかけの .osm も 1 個として数えられる。

java の終了コードは set -e に任せず明示的に見る。

転送先に manifest.txt を置いて枚数を書く。reimport_targets に載るのは
都市コードだけなので、これが無いと shipped.txt を失ったときに枚数を
復元できない。取り込みの開始条件もこの数と照合する。
MSG
```

---

### Task 3: `ship_all.sh`

**Files:**
- Create: `scripts/reimport/ship_all.sh`
- Create: `tests/test_ship_all.py`

**Interfaces:**
- Consumes: Task 2 までの `ship_city.sh`
- Produces: `ship_all.sh`。`SHIP_CITY_CMD` で呼ぶコマンドを差し替えられる
- Produces: 転送先の親に `reimport_targets_<日時>.txt` を置く

- [ ] **Step 1: 失敗するテストを 8 件書く**

`tests/test_ship_all.py` を新規に作る。

```python
"""ship_all.sh の件数確認と再開を固定する。"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SHIP_ALL = REPO / 'scripts' / 'reimport' / 'ship_all.sh'


def _stub(bin_dir: Path, name: str, body: str):
    p = bin_dir / name
    p.write_text('#!/usr/bin/env bash\n' + body + '\n')
    p.chmod(0o755)
    return p


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    plan = tmp_path / 'plan.csv'
    shipped = tmp_path / 'shipped.txt'
    called = tmp_path / 'called.txt'

    _stub(bin_dir, 'ship_city_stub',
          'echo "$1" >> "%s"\n'
          'echo "$1 3" >> "%s"\n'
          'exit 0' % (called, shipped))
    _stub(bin_dir, 'rsync', 'exit 0')
    _stub(bin_dir, 'ssh', 'exit 0')

    ship_env = tmp_path / 'ship.env'
    ship_env.write_text(
        'SHIP_CITY_CMD="%s/ship_city_stub"\n'
        'PLAN_CSV="%s"\n'
        'SHIPPED_TXT="%s"\n'
        'SHIP_HOST="stubhost"\n'
        'SHIP_PATH="/stub/import"\n'
        'WORK_ROOT="%s"\n'
        'DISK_MIN_KB=0\n'
        'EXPECTED_CITIES=3\n'
        % (bin_dir, plan, shipped, tmp_path)
    )

    run_env = dict(os.environ)
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])
    run_env['SHIP_ENV'] = str(ship_env)

    class Env:
        pass

    e = Env()
    e.bin = bin_dir
    e.plan = plan
    e.shipped = shipped
    e.called = called
    e.run_env = run_env
    e.tmp = tmp_path
    return e


def _write_plan(e, codes):
    lines = ['city_code,package,year,citygml_v,bytes,url']
    for c in codes:
        lines.append('%s,plateau-%s-x-2025,2025,5,1,https://example.invalid/x.zip' % (c, c))
    e.plan.write_text('\n'.join(lines) + '\n')


def _run(e):
    return subprocess.run(['bash', str(SHIP_ALL)],
                          env=e.run_env, capture_output=True, text=True)


def test_stops_when_plan_has_wrong_city_count(env):
    """計画の件数が EXPECTED_CITIES と違えば、1 都市も処理せずに止まる。

    147 件のまま流すと targets も 147 件になり、20 時間後の最終確認まで
    誰も気づかない。
    """
    _write_plan(env, ['30406', '43213'])   # 2 件。EXPECTED_CITIES は 3

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.called.exists()


def test_processes_every_city_in_the_plan(env):
    _write_plan(env, ['13402', '30406', '43213'])

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']


def test_skips_cities_already_in_shipped(env):
    """shipped.txt にある都市は飛ばす。途中で止めても続きから走る。"""
    _write_plan(env, ['13402', '30406', '43213'])
    env.shipped.write_text('13402 8\n30406 103\n')

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['43213']


def test_one_failure_does_not_stop_the_rest(env):
    """1 都市が失敗しても続ける。終了コードは 1 になる。"""
    _write_plan(env, ['13402', '30406', '43213'])
    _stub(env.bin, 'ship_city_stub',
          'echo "$1" >> "%s"\n'
          'if [ "$1" = "30406" ]; then exit 11; fi\n'
          'echo "$1 3" >> "%s"\n'
          'exit 0' % (env.called, env.shipped))

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']
    assert '30406' in r.stdout


def test_disk_shortage_aborts_the_whole_run(env):
    """空きが下限を割ったら、その場で全体を止める。

    このテストが無いと、ディスクチェックを丸ごと消した実装でも通る。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    env.run_env['SHIP_ENV'] = str(env.tmp / 'ship_tight.env')
    (env.tmp / 'ship_tight.env').write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'DISK_MIN_KB=0', 'DISK_MIN_KB=999999999999'))

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr
    assert not env.called.exists()


def test_ship_city_disk_exit_aborts_the_whole_run(env):
    """ship_city.sh が exit 2 を返したら、次の都市へ進まずに止める。

    2 はディスク不足の合図で、他の失敗と混ぜてはいけない。
    このテストが無いと、exit 2 の分岐を消した実装でも通る。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    _stub(env.bin, 'ship_city_stub',
          'echo "$1" >> "%s"\n'
          'if [ "$1" = "30406" ]; then exit 2; fi\n'
          'echo "$1 3" >> "%s"\n'
          'exit 0' % (env.called, env.shipped))

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406']


def test_target_list_holds_only_city_codes(env):
    """サーバへ渡す一覧は都市コード 1 列だけにする。

    shipped.txt は <citycode> <osm数> の 2 列。第 2 段のバッチは行から
    空白を全部除くので、2 列のまま渡すと 43213 103 が 43213103 になり、
    その都市は永久に取り込まれない。
    """
    _write_plan(env, ['13402', '30406', '43213'])

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    written = sorted(env.tmp.glob('reimport_targets_*.txt'))
    assert written, '一覧が作られていない'
    lines = [ln for ln in written[-1].read_text().splitlines() if ln]
    assert lines == ['13402', '30406', '43213'], lines


def test_resume_does_not_skip_on_a_prefix_match(env):
    """都市コードの前方一致で誤って飛ばさない。

    照合が行頭と末尾の空白で挟まれていないと、1340 が 13402 に一致する。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    env.shipped.write_text('1340 8\n')

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']
```

- [ ] **Step 2: 8 件が落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_all.py -v`
Expected: 8 failed。`ship_all.sh` がまだ無いので returncode 127。

- [ ] **Step 3: `ship_all.sh` を書く**

`scripts/reimport/ship_all.sh` を新規に作る。

```bash
#!/usr/bin/env bash
# 計画の都市を順に送る。
#
#     ship_all.sh
#
# shipped.txt にある都市は飛ばすので、途中で止めて再実行できる。
# 1 都市の失敗では止まらない。ディスクが閾値を割ったときだけ全体を止める。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SHIP_ENV:=$HERE/ship.env}"
if [ ! -f "$SHIP_ENV" ]; then
  echo "設定が無い: $SHIP_ENV (ship.env.example を複製する)" >&2
  exit 1
fi
# shellcheck disable=SC1090
. "$SHIP_ENV"

: "${SHIP_CITY_CMD:=bash $HERE/ship_city.sh}"
: "${PLAN_CSV:=$HERE/ckan_download_plan.csv}"
: "${SHIPPED_TXT:?SHIPPED_TXT が未設定}"
: "${SHIP_HOST:?SHIP_HOST が未設定}"
: "${SHIP_PATH:?SHIP_PATH が未設定}"
: "${WORK_ROOT:?WORK_ROOT が未設定}"
: "${DISK_MIN_KB:=5242880}"
: "${EXPECTED_CITIES:=148}"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] $*"; }

disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

# コマンドの出力を数値として比べる前に、必ずこれを通す。
# 空文字のまま [ "$x" -lt "$y" ] を評価すると
# integer expression expected でエラー終了し、if がそれを偽として扱う。
need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# ship.env はファイルなので、そこから来る数値も検査する。
# DISK_MIN_KB が壊れているとディスク不足の判定が黙って消える。
# 安全装置そのものが、設定を書き損じたときに限って効かなくなる。
for v in DISK_MIN_KB EXPECTED_CITIES; do
  eval "val=\$$v"
  if ! need_int "$val"; then
    say "ABORT: ${v} が数字でない (値: ${val})"
    exit 3
  fi
done

touch "$SHIPPED_TXT"

CODES=$(tail -n +2 "$PLAN_CSV" | cut -d, -f1 | grep -c .)
say "計画 $CODES 都市 (期待 $EXPECTED_CITIES)"
if [ "$CODES" -ne "$EXPECTED_CITIES" ]; then
  say "ABORT: 計画の件数が合わない。build_download_plan.py で足りない都市を足す"
  exit 3
fi

failed=""
ok=0
skip=0
i=0
# tr -d '\r' は CRLF の CSV に備える。\r が残ると shipped.txt との照合が
# 一致しなくなり、再開のたびに全都市を送り直す。
for CITY in $(tail -n +2 "$PLAN_CSV" | cut -d, -f1 | tr -d '\r'); do
  i=$((i + 1))

  # 行頭と末尾の空白で挟む。挟まないと 1340 が 13402 に前方一致する。
  if grep -q "^$CITY " "$SHIPPED_TXT"; then
    skip=$((skip + 1))
    continue
  fi

  AVAIL=$(disk_kb "$WORK_ROOT")
  if ! need_int "$AVAIL"; then
    say "ABORT: 空き容量を読めない (df の出力: $AVAIL)"
    exit 2
  fi
  if [ "$AVAIL" -lt "$DISK_MIN_KB" ]; then
    say "ABORT: 空きが $AVAIL KB で下限 $DISK_MIN_KB を割った"
    exit 2
  fi

  say "[$i/$CODES] $CITY: START"
  $SHIP_CITY_CMD "$CITY"
  EXIT=$?
  if [ "$EXIT" -eq 0 ]; then
    ok=$((ok + 1))
    say "[$i/$CODES] $CITY: OK"
  elif [ "$EXIT" -eq 2 ]; then
    say "ABORT: $CITY でディスク不足"
    exit 2
  else
    failed="$failed $CITY"
    say "[$i/$CODES] $CITY: FAIL exit=$EXIT"
  fi
done

# 一覧は毎回新しい名前で置く。第 2 段の走行中に送り直しても、
# 走っているバッチが読むファイルを書き換えない。
STAMP=$(date '+%Y%m%d-%H%M%S')
TARGETS="$WORK_ROOT/reimport_targets_$STAMP.txt"
# 都市コードの 1 列だけにする。shipped.txt は 2 列である。
# 第 2 段のバッチは行から空白を全部除くので、2 列のまま渡すと
# 43213 103 が 43213103 になり、その都市は永久に取り込まれない。
cut -d' ' -f1 "$SHIPPED_TXT" > "$TARGETS"
rsync -az "$TARGETS" "$SHIP_HOST:$SHIP_PATH/../reimport_targets_$STAMP.txt"
TARGETS_EXIT=$?
# ここで失敗を握りつぶすと、5〜6 時間かけて送ったあとに一覧だけ届いておらず、
# ログ上は成功して見える。第 2 段が始まらない理由が判らなくなる。
if [ "$TARGETS_EXIT" -ne 0 ]; then
  say "ABORT: 一覧の転送が exit ${TARGETS_EXIT}。手元には ${TARGETS} が残っている"
  exit 4
fi
say "一覧を置いた: reimport_targets_${STAMP}.txt ($(grep -c . "$TARGETS") 都市)"

say "=== DONE === ok=$ok skip=$skip failed=$(echo "$failed" | wc -w | tr -d ' ')"
if [ -n "$failed" ]; then
  say "失敗した都市:$failed"
  say "ship_all.sh を再実行すればこの都市だけをやり直せる"
  exit 1
fi
exit 0
```

- [ ] **Step 4: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_all.py -v`
Expected: 8 passed

- [ ] **Step 5: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: 349 passed、25 skipped

- [ ] **Step 6: コミット**

```bash
git add scripts/reimport/ship_all.sh tests/test_ship_all.py
git commit -F - <<'MSG'
feat(scripts): 計画を順に送る ship_all.sh を足す

shipped.txt にある都市を飛ばすので、途中で止めて再実行できる。
1 都市の失敗では止まらない。ディスクが下限を割ったときだけ全体を止める。

開始時に計画の件数を確かめる。147 件のまま流すと targets も 147 件になり、
門も第 2 段も何も検出しないまま、最終確認まで誰も気づかない。

一覧は reimport_targets_<日時>.txt という新しい名前で置く。バッチは一覧を
ループの開始時に 1 回開いてそのまま読み進むので、固定名を上書きすると
走行中の読み位置がずれる。
MSG
```

---

### Task 4: `deploy/reimport_one.sh`

**Files:**
- Create: `deploy/reimport_one.sh`
- Create: `tests/test_reimport_one.py`

**Interfaces:**
- Produces: `reimport_one.sh <citycode>`。設定は環境変数から読む
- Produces: 環境変数 `PLATEAU_APP_DIR` / `PLATEAU_VENV` / `PLATEAU_ENV_FILE` / `PLATEAU_IMPORT_DIR` / `PLATEAU_LOG_DIR` / `THRESHOLD_KB` / `PYTHON_BIN`
- Produces: 終了コード 2 (ディスク不足)、13 (入力が無い、または枚数が合わない)

- [ ] **Step 1: 失敗するテストを 9 件書く**

`tests/test_reimport_one.py` を新規に作る。

```python
"""reimport_one.sh の分岐を固定する。

シェルの分岐は実行時に気づきにくい。取り込み器は偽物に差し替える。
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
ONE = REPO / 'deploy' / 'reimport_one.sh'


@pytest.fixture
def env(tmp_path):
    app = tmp_path / 'app'
    app.mkdir()
    import_dir = tmp_path / 'import'
    import_dir.mkdir()
    logs = tmp_path / 'logs'
    env_file = tmp_path / 'env'
    env_file.write_text('DATABASE_URL=postgresql://stub/stub\n')

    # 取り込み器の偽物。呼ばれた引数を記録して成功する。
    called = tmp_path / 'called.txt'
    stub = app / 'plateau_importer2postgis.py'
    stub.write_text(
        'import sys, pathlib\n'
        'pathlib.Path(%r).write_text(" ".join(sys.argv[1:]))\n'
        'sys.exit(0)\n' % str(called)
    )

    run_env = dict(os.environ)
    run_env.update({
        'PLATEAU_APP_DIR': str(app),
        'PLATEAU_ENV_FILE': str(env_file),
        'PLATEAU_IMPORT_DIR': str(import_dir),
        'PLATEAU_LOG_DIR': str(logs),
        'PYTHON_BIN': 'python3',
        'THRESHOLD_KB': '0',
    })

    class Env:
        pass

    e = Env()
    e.app = app
    e.import_dir = import_dir
    e.called = called
    e.stub = stub
    e.run_env = run_env
    e.tmp = tmp_path
    return e


def _city(e, code='30406', osm=2, manifest=None):
    d = e.import_dir / code
    d.mkdir()
    for i in range(osm):
        (d / ('5339450%d_bldg_6697_op.osm' % i)).write_text('<osm/>')
    (d / 'manifest.txt').write_text(str(osm if manifest is None else manifest) + '\n')
    return d


def _run(e, city='30406'):
    return subprocess.run(['bash', str(ONE), city],
                          env=e.run_env, capture_output=True, text=True)


def test_missing_input_exits_with_dedicated_code(env):
    """入力ディレクトリが無ければ専用の終了コードで落ちる。

    取り込み器は mkdir(exist_ok=True) を parents 無しで呼ぶので、
    そのまま渡すと生のトレースバックが出る。
    """
    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_count_mismatch_exits_with_dedicated_code(env):
    """.osm の枚数が manifest.txt と違えば落ちる。

    「1 つ以上」では、枚数が欠けた都市がそのまま取り込まれる。
    """
    _city(env, osm=2, manifest=3)

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_unreadable_manifest_exits_with_dedicated_code(env):
    """manifest.txt が数字でなければ落ちる。

    空や壊れた manifest をそのまま比較すると integer expression expected で
    エラー終了し、if がそれを偽として扱う。壊れた manifest を弾くのが
    この門の目的なので、素通りさせると門を置いた意味が無くなる。
    """
    _city(env, osm=2, manifest='')

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_passes_citycode_and_no_zip_explicitly(env):
    """--citycode と --no-zip を明示して渡す。推定に頼らない。"""
    _city(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    args = env.called.read_text()
    assert '--citycode 30406' in args
    assert '--no-zip' in args


def test_input_survives_a_failed_import(env):
    """取り込みが失敗したら、入力ディレクトリを消さない。

    以前は再ダウンロードできたので消してよかったが、いまは取り出しから
    転送までをやり直すことになる。
    """
    d = _city(env, osm=2)
    env.stub.write_text('import sys\nsys.exit(7)\n')

    r = _run(env)

    assert r.returncode == 7, r.stdout + r.stderr
    assert d.exists()
    assert len(list(d.glob('*.osm'))) == 2


def test_input_removed_after_a_successful_import(env):
    """成功したときだけ入力を消す。"""
    d = _city(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert not d.exists()


def test_low_disk_exits_2(env):
    """ディスク不足は 2。バッチ全体を止める合図なので他と混ぜない。"""
    _city(env, osm=2)
    env.run_env['THRESHOLD_KB'] = '999999999999'

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr


def test_counts_the_osm_files_instead_of_assuming_two(env):
    """.osm の枚数を実際に数えている。定数と比べていない。

    他のテストがどれも 2 枚なので、OSM_N=2 と決め打ちした実装でも通る。
    3 枚で manifest を 2 にすると、数えている実装だけが落ちる。
    """
    _city(env, osm=3, manifest=2)

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_kills_the_importer_when_the_wrapper_is_terminated(env):
    """wrapper が SIGTERM を受けたら、取り込みの子も落とす。

    watchdog の kill は wrapper にしか届かない。子が生き残ると、
    次の都市の取り込みと id の採番が重なってノードが別都市の建物に
    ぶら下がる。例外も出ず行数も 0 にならないので他では検出できない。

    本体を `{ ... } | tee` にすると、IMPORT_PID=$! がサブシェルの中で
    起きて親に伝わらず、親の cleanup は空の IMPORT_PID を見て空振りする。
    """
    import signal
    import time

    _city(env, osm=2)
    # 取り込み器を、すぐには終わらない偽物に差し替える
    env.stub.write_text('import time\ntime.sleep(60)\n')

    proc = subprocess.Popen(['bash', str(ONE), '30406'], env=env.run_env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # 子が起動するまで待つ
    deadline = time.time() + 20
    child = None
    while time.time() < deadline:
        out = subprocess.run(['pgrep', '-f', 'plateau_importer2postgis.py'],
                             capture_output=True, text=True)
        if out.stdout.strip():
            child = out.stdout.split()[0]
            break
        time.sleep(0.2)
    assert child, '取り込みの子が起動しなかった'

    proc.send_signal(signal.SIGTERM)
    proc.wait(timeout=30)

    # 子が落ちるまで少し待つ
    deadline = time.time() + 15
    alive = True
    while time.time() < deadline:
        if subprocess.run(['kill', '-0', child],
                          capture_output=True).returncode != 0:
            alive = False
            break
        time.sleep(0.2)
    if alive:
        subprocess.run(['kill', '-9', child], capture_output=True)
    assert not alive, '取り込みの子が生き残った (IMPORT_PID が親に伝わっていない)'
```

- [ ] **Step 2: テストが落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_one.py -v`
Expected: 9 failed。`deploy/reimport_one.sh` がまだ無いので returncode 127。

- [ ] **Step 3: `deploy/reimport_one.sh` を書く**

`deploy/reimport_one.sh` を新規に作る。

```bash
#!/usr/bin/env bash
# 1 都市を取り込む。
#
#     reimport_one.sh <citycode>
#
# 入力は手元から送られてきた .osm で、PLATEAU_IMPORT_DIR/<citycode>/ に置かれる。
# ダウンロードは手元へ移った。purge は取り込みが内包しているので呼ばない。
#
# 環境変数
#   PLATEAU_APP_DIR     リポジトリの置き場所
#   PLATEAU_VENV        Python の仮想環境 (省略時は PYTHON_BIN をそのまま使う)
#   PLATEAU_ENV_FILE    DATABASE_URL を含む設定
#   PLATEAU_IMPORT_DIR  手元から送られた .osm の置き場所
#   PLATEAU_LOG_DIR     ログの置き場所
#   THRESHOLD_KB        取り込み前に要求する空き (1K ブロック、既定 5GB)
set -uo pipefail

CITY="${1:?citycode required}"

: "${PLATEAU_APP_DIR:?PLATEAU_APP_DIR が未設定}"
: "${PLATEAU_ENV_FILE:?PLATEAU_ENV_FILE が未設定}"
: "${PLATEAU_IMPORT_DIR:?PLATEAU_IMPORT_DIR が未設定}"
: "${PLATEAU_LOG_DIR:=$HOME/reimport_logs}"
: "${PYTHON_BIN:=python3}"
: "${THRESHOLD_KB:=5242880}"

EXIT_DISK=2
EXIT_INPUT=13

mkdir -p "$PLATEAU_LOG_DIR"
LOG="$PLATEAU_LOG_DIR/${CITY}.log"
SRC="$PLATEAU_IMPORT_DIR/$CITY"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

# コマンドやファイルから読んだ値を数値として比べる前に、必ずこれを通す。
# 空文字のまま [ "$x" -ne "$y" ] を評価すると
# integer expression expected でエラー終了し、if がそれを偽として扱う。
# 分岐が黙って消えるので、門は「入力が壊れているときに限って」効かなくなる。
need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# ログはプロセス置換で複製する。`{ ... } | tee` にすると本体がサブシェルで
# 走り、そこでの IMPORT_PID=$! が親に伝わらない。watchdog が SIGTERM を
# 送るのは親なので、親の cleanup は常に空の IMPORT_PID を見て空振りする。
# 落とすべき取り込みが生き残り、この機構を置いた意味が消える。
exec > >(tee -a "$LOG") 2>&1

# 取り込み器を子として起動し、自分が終わるときに確実に落とす。
# watchdog の kill は wrapper の PID にしか届かないので、これが無いと
# 打ち切られたあとも取り込みが走り続け、次の都市と採番が重なる。
IMPORT_PID=""
cleanup() {
  if [ -n "$IMPORT_PID" ] && kill -0 "$IMPORT_PID" 2>/dev/null; then
    echo "[$(ts)] [$CITY] 取り込みの子 $IMPORT_PID を落とす"
    kill -TERM "$IMPORT_PID" 2>/dev/null || true
    sleep 5
    kill -KILL "$IMPORT_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

{
  echo "[$(ts)] [$CITY] === START ==="

  AVAIL=$(disk_kb "$PLATEAU_APP_DIR")
  if ! need_int "$AVAIL"; then
    echo "[$(ts)] [$CITY] ABORT: 空き容量を読めない (df の出力: $AVAIL)"
    exit $EXIT_DISK
  fi
  echo "[$(ts)] [$CITY] 空き $AVAIL KB (下限 $THRESHOLD_KB)"
  if [ "$AVAIL" -lt "$THRESHOLD_KB" ]; then
    echo "[$(ts)] [$CITY] ABORT: ディスクが足りない"
    exit $EXIT_DISK
  fi

  if [ ! -d "$SRC" ]; then
    echo "[$(ts)] [$CITY] ABORT: 入力が無い: $SRC"
    exit $EXIT_INPUT
  fi

  OSM_N=$(find "$SRC" -maxdepth 1 -name '*.osm' | wc -l | tr -d ' ')
  if [ ! -f "$SRC/manifest.txt" ]; then
    echo "[$(ts)] [$CITY] ABORT: manifest.txt が無い"
    exit $EXIT_INPUT
  fi
  WANT=$(tr -d '[:space:]' < "$SRC/manifest.txt")
  # manifest.txt が空だったり数字以外だったりすると、そのまま比較した時点で
  # 門が消える。壊れた manifest を弾くのがこの門の目的なので、
  # ここを素通りさせると門を置いた意味が無くなる。
  if ! need_int "$WANT"; then
    echo "[$(ts)] [$CITY] ABORT: manifest.txt が数字でない (中身: $WANT)"
    exit $EXIT_INPUT
  fi
  echo "[$(ts)] [$CITY] .osm $OSM_N 個 (manifest $WANT)"
  if [ "$OSM_N" -ne "$WANT" ]; then
    echo "[$(ts)] [$CITY] ABORT: 枚数が manifest と違う"
    exit $EXIT_INPUT
  fi

  cd "$PLATEAU_APP_DIR" || exit $EXIT_INPUT
  if [ -n "${PLATEAU_VENV:-}" ]; then
    # shellcheck disable=SC1091
    . "$PLATEAU_VENV/bin/activate"
  fi
  set -a
  # shellcheck disable=SC1090
  . "$PLATEAU_ENV_FILE"
  set +a

  echo "[$(ts)] [$CITY] 取り込み"
  # --citycode は明示する。推定が外れて "unknown" になると、既存データの
  # 削除と行政界フィルタの両方が黙って飛ぶ。
  "$PYTHON_BIN" plateau_importer2postgis.py \
    --data-dir "$SRC" --no-zip --citycode "$CITY" \
    --postgres-url "$DATABASE_URL" &
  IMPORT_PID=$!
  wait "$IMPORT_PID"
  IMP_EXIT=$?
  IMPORT_PID=""
  if [ "$IMP_EXIT" -ne 0 ]; then
    echo "[$(ts)] [$CITY] 取り込みが exit ${IMP_EXIT}。入力は残す"
    exit $IMP_EXIT
  fi

  # 成功したときだけ消す。取り込みは --no-zip でも <data-dir>/extracted を
  # 作るので、.osm だけを消すと空のディレクトリが残る。
  rm -rf "$SRC"
  echo "[$(ts)] [$CITY] 空き $(disk_kb "$PLATEAU_APP_DIR") KB"
  echo "[$(ts)] [$CITY] === DONE ==="
}
```

`{ ... }` はまとまりを示すだけで、パイプに繋がないのでサブシェルにならない。
`exit` はトップレベルのシェルを終わらせ、その終了コードがそのまま呼び出し元へ返る。

- [ ] **Step 4: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_one.py -v`
Expected: 9 passed

- [ ] **Step 5: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: 358 passed、25 skipped

- [ ] **Step 6: コミット**

```bash
git add deploy/reimport_one.sh tests/test_reimport_one.py
git commit -F - <<'MSG'
feat(deploy): 取り込みだけに縮めた reimport_one.sh をリポジトリに置く

いままでサーバの home にしか無く、どのリポジトリにも入っていなかった。
138 都市を完走させた実績のある部分なので、サーバを捨てると失われる。

4 段から 2 段に縮む。ダウンロードは手元へ移り、purge は取り込みが
--citycode 指定時に内包している。

入力の削除を成功時だけにした。以前は trap cleanup EXIT で、失敗しても
watchdog に kill されても消えていた。再ダウンロードできたので無害
だったが、いまは取り出しから転送までをやり直すことになる。

開始条件を manifest.txt の枚数との一致にした。「1 つ以上」だと、枚数が
欠けた都市が行を持ち ref_mlit_plateau も入るので、他のどの門でも
検出できない。

終了時に取り込みの子を確実に落とす。watchdog の kill は wrapper の PID
にしか届かないので、これが無いと打ち切られたあとも取り込みが走り続け、
次の都市と osm_id の採番が重なる。

パスはすべて環境変数から読む。
MSG
```

---

### Task 5: `deploy/reimport_batch.sh` と `deploy/reimport_watchdog.sh`

**Files:**
- Create: `deploy/reimport_batch.sh`
- Create: `deploy/reimport_watchdog.sh`
- Create: `tests/test_reimport_batch.py`
- Create: `tests/test_reimport_watchdog.py`

**Interfaces:**
- Consumes: Task 4 の `reimport_one.sh`
- Produces: `reimport_batch.sh [一覧のパス]`。省略時は `$HOME/reimport_targets.txt`
- Produces: `reimport_watchdog.sh`。環境変数 `WRAPPER_PATH` で wrapper の判定に使うパスを渡す

この 2 本はサーバ上の現物をもとにする。変更は 1 箇所ずつに留める。
再開、pause、apt の時間帯回避、失敗の数え方、ディスクとメモリの監視には触らない。

- [ ] **Step 1: 失敗するテストを 2 件書く**

`tests/test_reimport_batch.py` を新規に作る。

```python
"""reimport_batch.sh の一覧の受け取りと done.txt の照合を固定する。"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
BATCH = REPO / 'deploy' / 'reimport_batch.sh'


@pytest.fixture
def batch_env(tmp_path):
    logs = tmp_path / 'logs'
    logs.mkdir()
    called = tmp_path / 'called.txt'
    one = tmp_path / 'one_stub.sh'
    one.write_text(
        '#!/usr/bin/env bash\n'
        'echo "$1" >> "%s"\n'
        'exit 0\n' % called
    )
    one.chmod(0o755)

    run_env = dict(os.environ)
    run_env.update({
        'HOME': str(tmp_path),
        'REIMPORT_LOG_DIR': str(logs),
        'REIMPORT_ONE': str(one),
    })

    class Env:
        pass

    e = Env()
    e.tmp = tmp_path
    e.logs = logs
    e.called = called
    e.run_env = run_env
    return e


def test_batch_reads_the_list_given_as_an_argument(batch_env):
    """一覧のパスを引数で受け取る。固定名だけだと新しい一覧を渡せない。"""
    targets = batch_env.tmp / 'reimport_targets_20260814-120000.txt'
    targets.write_text('13402\n30406\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['13402', '30406']


def test_batch_skips_cities_listed_in_done(batch_env):
    """done.txt にある都市は飛ばす。照合は行全体との一致。"""
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n30406\n')
    (batch_env.logs / 'done.txt').write_text('13402\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['30406']
```

- [ ] **Step 2: 2 件が落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_batch.py -v`
Expected: 2 failed。`deploy/reimport_batch.sh` がまだ無いので returncode 127。

- [ ] **Step 3: `deploy/reimport_batch.sh` を書く**

`deploy/reimport_batch.sh` を新規に作る。
サーバ上の現物から変えたのは、一覧のパスを引数で受け取ることと、都市を始める前に取り込みの残存を確かめることの 2 点だけである。

```bash
#!/usr/bin/env bash
# 一覧の都市を順に取り込む。
#
#     reimport_batch.sh [一覧のパス]
#
# 一覧のパスを省略すると $HOME/reimport_targets.txt を読む。
# done.txt にある都市は飛ばす。$HOME/reimport_pause があるか、
# wrapper が exit 2 (ディスク不足) を返したら止まる。
# apt-daily-upgrade の時間帯 (06:00-06:35 JST) は都市の手前で待つ。
set -uo pipefail

LIST="${1:-$HOME/reimport_targets.txt}"
: "${REIMPORT_LOG_DIR:=$HOME/reimport_logs}"
: "${REIMPORT_ONE:=$HOME/reimport_one.sh}"

DONE="$REIMPORT_LOG_DIR/done.txt"
FAILED="$REIMPORT_LOG_DIR/failed.txt"
PAUSE="$HOME/reimport_pause"
SUMMARY="$REIMPORT_LOG_DIR/summary.log"
STATUS="$REIMPORT_LOG_DIR/batch_status"

mkdir -p "$REIMPORT_LOG_DIR"
touch "$DONE" "$FAILED"

rm -f "$STATUS"
echo "[$(date '+%F %T')] === BATCH START === list=$LIST" | tee -a "$SUMMARY"
TOTAL=$(grep -c . "$LIST")
echo "[$(date '+%F %T')] Total cities: $TOTAL" | tee -a "$SUMMARY"

wait_through_upgrade_window() {
  local logged=0
  while true; do
    local h m mod
    h=$(date +%H)
    m=$(date +%M)
    mod=$((10#$h * 60 + 10#$m))
    if [ $mod -ge 360 ] && [ $mod -lt 395 ]; then
      if [ $logged -eq 0 ]; then
        echo "[$(date '+%F %T')] In apt-daily-upgrade window, waiting until 06:35" | tee -a "$SUMMARY"
        logged=1
      fi
      sleep 120
    else
      [ $logged -eq 1 ] && echo "[$(date '+%F %T')] Upgrade window passed, resuming" | tee -a "$SUMMARY"
      return
    fi
  done
}

# 取り込みが 2 つ同時に走ると osm_id の採番が重なり、ノードが別都市の
# 建物にぶら下がる。例外も出ず行数も 0 にならないので、他では検出できない。
assert_no_stray_import() {
  if pgrep -f plateau_importer2postgis.py > /dev/null 2>&1; then
    echo "[$(date '+%F %T')] ABORT: 取り込みが既に走っている" | tee -a "$SUMMARY"
    echo STRAY_IMPORT > "$STATUS"
    exit 5
  fi
}

i=0
ok=0
fail=0
skip=0
while IFS= read -r CITY; do
  i=$((i+1))
  CITY=$(echo "$CITY" | tr -d '[:space:]')
  [ -z "$CITY" ] && continue

  if [ -f "$PAUSE" ]; then
    echo "[$(date '+%F %T')] PAUSE detected. Stop at $i/$TOTAL (ok=$ok fail=$fail skip=$skip)" | tee -a "$SUMMARY"
    echo PAUSED > "$STATUS"
    exit 0
  fi

  if grep -qx "$CITY" "$DONE"; then
    skip=$((skip+1))
    continue
  fi

  wait_through_upgrade_window
  assert_no_stray_import

  echo "[$(date '+%F %T')] [$i/$TOTAL] $CITY: START" | tee -a "$SUMMARY"
  bash "$REIMPORT_ONE" "$CITY"
  EXIT=$?
  if [ "$EXIT" -eq 0 ]; then
    echo "$CITY" >> "$DONE"
    ok=$((ok+1))
    echo "[$(date '+%F %T')] [$i/$TOTAL] $CITY: OK (cumulative ok=$ok)" | tee -a "$SUMMARY"
  else
    echo "$CITY $EXIT" >> "$FAILED"
    fail=$((fail+1))
    echo "[$(date '+%F %T')] [$i/$TOTAL] $CITY: FAIL exit=$EXIT" | tee -a "$SUMMARY"
    if [ "$EXIT" -eq 2 ]; then
      echo "[$(date '+%F %T')] Disk threshold breached. ABORT." | tee -a "$SUMMARY"
      echo DISK_ABORT > "$STATUS"
      exit 2
    fi
  fi
done < "$LIST"

echo "[$(date '+%F %T')] === BATCH DONE === ok=$ok fail=$fail skip=$skip total=$i" | tee -a "$SUMMARY"
echo DONE > "$STATUS"
```

- [ ] **Step 4: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_batch.py -v`
Expected: 2 passed

- [ ] **Step 5: watchdog の判定を固定する失敗するテストを 2 件書く**

`tests/test_reimport_watchdog.py` を新規に作る。

ループ全体は回さない。打ち切りも `kill` も黙って効かなくなる原因は `is_real_wrapper` の 1 つで、
そこだけを切り出して確かめる。

```python
"""reimport_watchdog.sh の is_real_wrapper を固定する。

置き場所を変えると判定が常に false に倒れ、90 分の打ち切りも kill も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

ループ本体は回さない。関数だけを source して呼ぶ。
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
WATCHDOG = REPO / 'deploy' / 'reimport_watchdog.sh'


def _ask(wrapper_path, cmdline, tmp_path):
    """is_real_wrapper に cmdline を判定させ、終了コードを返す。

    /proc を読む実装なので、cmdline を返す偽の tr を PATH に置く。
    """
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir(exist_ok=True)
    tr_stub = bin_dir / 'tr'
    tr_stub.write_text(
        '#!/usr/bin/env bash\n'
        'if [ "$1" = "\\\\0" ]; then printf "%s" %s; else exec /usr/bin/tr "$@"; fi\n'
        % ('%s', repr(cmdline).replace("'", '"'))
    )
    tr_stub.chmod(0o755)

    script = (
        'set -uo pipefail\n'
        'WRAPPER_PATH=%s\n'
        'is_real_wrapper() {\n'
        '  local cmdline\n'
        '  cmdline=$(tr "\\\\0" " " < /dev/null) || return 1\n'
        '  case "$cmdline" in\n'
        '    "bash $WRAPPER_PATH "*) return 0 ;;\n'
        '    *) return 1 ;;\n'
        '  esac\n'
        '}\n'
        'is_real_wrapper 1\n'
    ) % wrapper_path

    env = dict(os.environ)
    env['PATH'] = '%s:%s' % (bin_dir, env['PATH'])
    return subprocess.run(['bash', '-c', script], env=env,
                          capture_output=True, text=True).returncode


def test_watchdog_reads_wrapper_path_from_the_environment():
    """判定に使うパスが環境変数から来ている。

    絶対パスが直接書き込まれていると、置き場所を変えたときに一致しなくなる。
    """
    body = WATCHDOG.read_text()
    assert 'WRAPPER_PATH' in body
    assert '"bash $WRAPPER_PATH "*)' in body


def test_watchdog_does_not_hardcode_an_absolute_wrapper_path():
    """判定に使うパスが直に埋め込まれていない。

    実サーバのパスを名指しで書くと、この公開リポジトリにそれを残すことになる。
    絶対パスの形だけを見て、どこを指しているかは問わない。
    """
    body = WATCHDOG.read_text()
    hard = re.findall(r'"bash /[^"$]*reimport_one\.sh', body)
    assert not hard, hard


def test_matches_when_the_cmdline_uses_the_configured_path(tmp_path):
    assert _ask('/somewhere/reimport_one.sh',
                'bash /somewhere/reimport_one.sh 30406 ', tmp_path) == 0


def test_does_not_match_when_the_wrapper_moved(tmp_path):
    """WRAPPER_PATH がずれていれば一致しない。これが黙って起きる失敗の形。"""
    assert _ask('/elsewhere/reimport_one.sh',
                'bash /somewhere/reimport_one.sh 30406 ', tmp_path) == 1
```

- [ ] **Step 6: 4 件が落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_watchdog.py -v`
Expected: 4 failed。`deploy/reimport_watchdog.sh` がまだ無いので `read_text` が `FileNotFoundError` になる。

- [ ] **Step 7: `deploy/reimport_watchdog.sh` を書く**

`deploy/reimport_watchdog.sh` を新規に作る。
サーバ上の現物から変えたのは、`is_real_wrapper` が比べる文字列を環境変数から作ることだけである。

```bash
#!/usr/bin/env bash
# バッチの見張り。ディスク、連続失敗、1 都市の所要時間、メモリを見る。
#
#     WRAPPER_PATH=/path/to/reimport_one.sh reimport_watchdog.sh
#
# 打ち切りや連続失敗で pause を立て、自分も終わる。
# 再開には reimport_pause を消して、バッチと watchdog の両方を起動し直す。
set -uo pipefail
INTERVAL=${INTERVAL:-60}
DISK_WARN_KB=${DISK_WARN_KB:-5242880}
DISK_HALT_KB=${DISK_HALT_KB:-3145728}
MAX_CITY_MIN=${MAX_CITY_MIN:-90}
: "${REIMPORT_LOG_DIR:=$HOME/reimport_logs}"
# wrapper の判定に使うパス。置き場所を変えるとここが一致しなくなり、
# 打ち切りも kill も黙って効かなくなる。ログだけは正常に出続ける。
: "${WRAPPER_PATH:=$HOME/reimport_one.sh}"

WATCH_LOG="$REIMPORT_LOG_DIR/watchdog.log"
PAUSE="$HOME/reimport_pause"
DIAG="$REIMPORT_LOG_DIR/watchdog_diagnostics.log"
low_mem_streak=0

mkdir -p "$REIMPORT_LOG_DIR"
START_LINES=$(wc -l < "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null || echo 0)
echo "[$(date '+%F %T')] === WATCHDOG START === baseline_lines=$START_LINES interval=${INTERVAL}s warn=${DISK_WARN_KB}KB halt=${DISK_HALT_KB}KB max_city=${MAX_CITY_MIN}min wrapper=$WRAPPER_PATH" | tee -a "$WATCH_LOG"

is_real_wrapper() {
  local pid=$1
  local cmdline
  cmdline=$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null) || return 1
  case "$cmdline" in
    "bash $WRAPPER_PATH "*) return 0 ;;
    *) return 1 ;;
  esac
}

trigger_pause() {
  local reason="$1"
  echo "[$(date '+%F %T')] PAUSE TRIGGERED: $reason" | tee -a "$WATCH_LOG"
  touch "$PAUSE"
  {
    echo "=== Diagnostics $(date '+%F %T') ==="
    echo "Reason: $reason"
    echo
    echo '--- df -h ---'
    df -h /
    echo
    echo '--- free -m ---'
    free -m
    echo
    echo '--- top RSS ---'
    ps -e -o pid,rss,etimes,cmd --sort=-rss | head -10
    echo
    echo '--- recent summary ---'
    tail -30 "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null
    echo
    echo '--- real wrappers ---'
    for p in $(pgrep -f "$(basename "$WRAPPER_PATH")" 2>/dev/null); do
      if is_real_wrapper $p; then
        echo "PID=$p ETIMES=$(ps -o etimes= -p $p 2>/dev/null) CMD=$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null)"
      fi
    done
  } >> "$DIAG"
  for p in $(pgrep -f "$(basename "$WRAPPER_PATH")" 2>/dev/null); do
    if is_real_wrapper $p; then
      kill -TERM $p 2>/dev/null || true
    fi
  done
}

tick=0
while true; do
  tick=$((tick+1))
  TS=$(date '+%F %T')

  if [ -f "$REIMPORT_LOG_DIR/batch_status" ]; then
    STATUS=$(cat "$REIMPORT_LOG_DIR/batch_status" 2>/dev/null)
    echo "[$TS] WATCHDOG: batch_status=$STATUS, exiting" | tee -a "$WATCH_LOG"
    exit 0
  fi

  AVAIL_KB=$(df -k / | awk 'NR==2 {print $4}')

  if [ "$AVAIL_KB" -lt "$DISK_HALT_KB" ]; then
    trigger_pause "disk HALT: ${AVAIL_KB}KB < ${DISK_HALT_KB}KB"
    exit 2
  fi
  if [ "$AVAIL_KB" -lt "$DISK_WARN_KB" ]; then
    echo "[$TS] WARN disk=${AVAIL_KB}KB" >> "$WATCH_LOG"
  fi

  TOTAL_LINES=$(wc -l < "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null || echo 0)
  if [ "$TOTAL_LINES" -gt "$START_LINES" ]; then
    NEW_TAIL=$((TOTAL_LINES - START_LINES))
    LOOK=$(( NEW_TAIL < 12 ? NEW_TAIL : 12 ))
    RECENT_FAIL=$(tail -n "$NEW_TAIL" "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null | tail -n "$LOOK" | grep -c "FAIL exit=")
    if [ "$RECENT_FAIL" -ge 3 ]; then
      trigger_pause "$RECENT_FAIL FAILs in last $LOOK observed entries (since watchdog start)"
      exit 3
    fi
  fi

  for pid in $(pgrep -f "$(basename "$WRAPPER_PATH")" 2>/dev/null); do
    if is_real_wrapper "$pid"; then
      ETIMES=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')
      if [ -n "$ETIMES" ] && [ "$ETIMES" -gt "$((MAX_CITY_MIN * 60))" ]; then
        trigger_pause "wrapper stuck pid=$pid etimes=${ETIMES}s"
        exit 4
      fi
    fi
  done

  MEM_AVAIL=$(free -m | awk '/^Mem:/{print $7}')
  if [ -n "$MEM_AVAIL" ] && [ "$MEM_AVAIL" -lt 30 ]; then
    low_mem_streak=$((low_mem_streak+1))
    if [ "$low_mem_streak" -ge 3 ]; then
      echo "[$TS] WARN sustained low mem avail=${MEM_AVAIL}MB streak=$low_mem_streak" >> "$WATCH_LOG"
    fi
  else
    low_mem_streak=0
  fi

  if [ $((tick % 10)) -eq 0 ]; then
    DONE_N=$(wc -l < "$REIMPORT_LOG_DIR/done.txt" 2>/dev/null || echo 0)
    FAIL_N=$(wc -l < "$REIMPORT_LOG_DIR/failed.txt" 2>/dev/null || echo 0)
    echo "[$TS] HB disk=${AVAIL_KB}KB mem=${MEM_AVAIL}MB done=${DONE_N} failed=${FAIL_N}" >> "$WATCH_LOG"
  fi

  sleep "$INTERVAL"
done
```

- [ ] **Step 8: watchdog のテストが通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_watchdog.py -v`
Expected: 4 passed

- [ ] **Step 9: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: 364 passed、25 skipped

- [ ] **Step 10: 実行権限を付けてコミット**

```bash
chmod +x deploy/reimport_one.sh deploy/reimport_batch.sh deploy/reimport_watchdog.sh
git add deploy/reimport_batch.sh deploy/reimport_watchdog.sh tests/test_reimport_batch.py tests/test_reimport_watchdog.py
git update-index --chmod=+x deploy/reimport_one.sh deploy/reimport_batch.sh deploy/reimport_watchdog.sh
git commit -F - <<'MSG'
feat(deploy): バッチと見張りをリポジトリに置く

どちらもサーバの home にしか無かった。ダッシュボードの deploy/ と同じ形にする。

reimport_batch.sh は一覧のパスを第 1 引数で受け取るようにした。固定名の 1 本
しか読めないと、一覧を毎回新しい名前で置く方針が実現できない。ループは
done < "$LIST" で 1 回開いてそのまま読み進むので、固定名を上書きすると
走行中の読み位置がずれる。

都市を始める前に取り込みの残存を確かめる。2 つ同時に走ると osm_id の採番が
重なり、ノードが別都市の建物にぶら下がる。例外も出ず行数も 0 にならない。

reimport_watchdog.sh は wrapper の判定に使うパスを環境変数にした。判定は
コマンドラインの前方一致なので、置き場所を変えると常に false に倒れ、
90 分の打ち切りも kill も黙って効かなくなる。ログだけは正常に出続ける。

どちらも変更はこの 1 箇所ずつで、再開、pause、apt の時間帯回避、失敗の
数え方、ディスクとメモリの監視には触っていない。
MSG
```

---

### Task 6: `deploy/README.md` と `DEPLOY.md` の 2 部構成

**Files:**
- Create: `deploy/README.md`
- Modify: `DEPLOY.md`

**Interfaces:**
- Consumes: Task 1 から 5 のすべて

- [ ] **Step 1: `deploy/README.md` を書く**

`deploy/README.md` を新規に作る。実サーバの識別子は書かない。

```markdown
# サーバ側の取り込みスクリプト

手元で変換した `.osm` を取り込むための 3 本。
設計は `docs/superpowers/specs/2026-08-13-local-conversion-reimport-design.md`。

## 置き方

3 本をサーバの実行ユーザの home に置き、実行権限を付ける。
パスはすべて環境変数から読むので、置き場所は自由に決めてよい。

```bash
scp deploy/reimport_*.sh <サーバ>:~/
ssh <サーバ> 'chmod +x ~/reimport_*.sh'
```

環境変数は実行ユーザの `~/.profile` などにまとめて書く。

| 変数 | 用途 |
|---|---|
| `PLATEAU_APP_DIR` | リポジトリの置き場所 |
| `PLATEAU_VENV` | Python の仮想環境。省略すると `PYTHON_BIN` をそのまま使う |
| `PLATEAU_ENV_FILE` | `DATABASE_URL` を含む設定 |
| `PLATEAU_IMPORT_DIR` | 手元から送られた `.osm` の置き場所 |
| `PLATEAU_LOG_DIR` | ログの置き場所。既定は `$HOME/reimport_logs` |
| `THRESHOLD_KB` | 取り込み前に要求する空き。既定 5GB |
| `WRAPPER_PATH` | watchdog が wrapper を見分けるために使う `reimport_one.sh` の絶対パス |
| `REIMPORT_ONE` | バッチが呼ぶ `reimport_one.sh` の場所 |

**`WRAPPER_PATH` は必ず実際の置き場所に合わせる。**
判定はコマンドラインの前方一致なので、ずれていると 90 分の打ち切りも `kill` も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

## 148 都市を流す

第 1 段 (手元) が終わってから始める。並行させない。

開始前に確かめること。

1. `done.txt` と `failed.txt` が空である。過去の実行のぶんが残ると都市が飛ばされる
2. 148 都市すべてが `dash_city_master` に行を持ち、`boundary_geom` が NULL でない
3. `plateau_building_nodes.building_id` の外部キーが `ON DELETE CASCADE` である

2 と 3 はどちらも、取り込み時にしか効かず後から掛け直せない。

```sql
-- 1. 境界が全都市そろっているか (0 行なら合格)
SELECT b.city_code
FROM (SELECT DISTINCT city_code FROM plateau_buildings) b
LEFT JOIN dash_city_master m ON m.city_code = b.city_code
WHERE m.city_code IS NULL OR m.boundary_geom IS NULL;

-- 2. ノードの外部キーが CASCADE か ('c' なら合格)
SELECT confdeltype FROM pg_constraint
WHERE conname = 'plateau_building_nodes_building_id_fkey';
```

流す。

```bash
nohup bash ~/reimport_batch.sh ~/reimport_targets_<日時>.txt > /dev/null 2>&1 &
WRAPPER_PATH=$HOME/reimport_one.sh nohup bash ~/reimport_watchdog.sh > /dev/null 2>&1 &
```

## 1 都市目で止めて確かめる

1 都市目が終わったところで、いったん見る。
ここで見つかる不具合は全都市に及ぶので、148 都市を流し切ってからでは遅い。

- `ref_mlit_plateau` が NULL の行が 0 件であること。jar が想定と違えば全都市が NULL になる
- 取り込みログの行政界フィルタの除外件数
- 「建物の中に建物」が無いこと

## 止まったときの再開

pause が立つと、watchdog 自身も終わる。
バッチも次の都市の手前で止まる。

```bash
rm ~/reimport_pause
# 取り込みが残っていないことを確かめてから
pgrep -f plateau_importer2postgis.py
# 両方を起動し直す
```

`failed.txt` に載った都市は `done.txt` に入らないので、再実行で必ずやり直される。

## 全部終わったあと

対応エリアのビューを 1 回だけ作る。
都市ごとには走らせない。メモリの小さいサーバでは OOM する。

```bash
python3 plateau_coverage.py --init --postgres-url "$DATABASE_URL"
```

`--init` はビューを作ったあとリフレッシュまで済ませる。
**続けて `--refresh` を叩かない。**ビューが populated になっているので、
`--no-concurrent` を付けない限り CONCURRENTLY を選び、避けたい経路を踏む。
やり直すときは `--refresh --no-concurrent` を使う。

メモリが足りないときは、一時的に swap を足してから実行する。
swap の増設には管理者権限が要る。
```

- [ ] **Step 2: `DEPLOY.md` を 2 部に分ける**

`DEPLOY.md` の見出しを「第 1 部 新規構築」と「第 2 部 更新」に分ける。
既存の 1 から 7 章は第 1 部に入れ、「8. 運用」を第 2 部の一部にする。

第 1 部の取り込みの節を、止まった配信元 (`plateau_downloader.py`) 前提から本設計の経路に差し替える。
`deploy/README.md` を参照させ、手順は重複させない。

`scp` するファイルの一覧に `plateau_coverage.py` と `plateau_purge.py` と `deploy/` 一式を足す。

Rapid の配信物を作る節に `npm run build` を足す。
`dist/data/l10n/*.min.json` を作るのは `build` 側なので、`dist` だけを流すと JS は新しいのに文言が古いまま配信される。

第 2 部に次の 4 つを書く。

| 場面 | 要点 |
|---|---|
| API のコード更新 | `git pull` → 必要ならスキーマ移行 → 再起動。起動中のプロセスは再起動まで古いコードを持つ |
| Rapid の更新 | `npm run build` → `npm run dist` → ドライラン → `--exclude '/dashboard/'` 付き rsync |
| 都市を足す、取り込み直す | 手元で `ship_city.sh`、サーバで `reimport_one.sh` |
| 対応エリアの再計算 | 都市を足したあとに 1 回。一時 swap を足して `--no-concurrent` |

第 1 部の 4-3 にある次の行を直す。

```bash
rsync -avz --delete dist/ user@vps:/var/www/rapid/
```

`--exclude '/dashboard/'` を足し、送信前にドライランで削除対象が 0 件であることを確かめる手順にする。
ダッシュボードは web root の中にあり `dist/` には含まれないので、この行はダッシュボードを全削除する。

- [ ] **Step 3: 実サーバの識別子が混ざっていないか確かめる**

Run:

```bash
git diff | grep -niE "<サーバのホスト名>|<SSH の別名>|<DB のパスワード>|/home/<サーバの実行ユーザ>|<手元のホームの絶対パス>"
```

Expected: 該当なし。`DEPLOY.md` にあるパスは、他人が自分の環境を立てるための汎用手順としての記述なので、既にある分は残す。新しく足さない。

- [ ] **Step 4: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: 364 passed、25 skipped

- [ ] **Step 5: コミット**

```bash
git add deploy/README.md DEPLOY.md
git commit -F - <<'MSG'
docs: deploy/ の手順を書き、DEPLOY.md を新規構築と更新の 2 部に分ける

実際に人が辿るのは DEPLOY.md だが、参照されるのは更新のときのほうが多い。
更新の章が無く、代わりに事故を起こす記述が 1 行残っていた。

rsync -avz --delete dist/ に --exclude '/dashboard/' が無い。ダッシュボードは
web root の中にあり dist/ には含まれないので、この行は更新のたびに
ダッシュボードを全削除する。新規構築の時点では存在しないので実害が出ない。

新規構築側の取り込みの節は、止まった配信元を前提にしていた。本設計の経路に
差し替え、手順は deploy/README.md に寄せる。scp の一覧と、Rapid の
配信物を作る節も直した。

deploy/README.md には第 2 段の開始前に確かめる SQL を書いた。行政界フィルタも
外部キーの CASCADE も取り込み時にしか効かず、後から掛け直す経路が無い。
MSG
```

---

## この計画に含まれないもの

新しいサーバが要るため、契約後に回す。

- 検証 3。大きい都市 1 件 (横浜市 14100、868K 建物) で所要時間とピーク RSS を測り、`MAX_CITY_MIN` を決める。浜松市 22130 (1,048 メッシュ) でも測る
- ディスクの閾値の逆算。空きが最小になるのは終了直前 (`.osm` 0GB + DB 36GB) なので、そこを基準にする
- 上り帯域の実測と、都市 N の転送を都市 N+1 の変換と重ねるかの判断
- 検証 6。148 都市の本番実行

取り込み器の変更も含まない。どちらも検証 3 の実測を待って必要かを決める。

- 行政界フィルタを最終バッチだけに寄せる
- 都市 1 件のあいだ累積する `node_coordinate_map` と `processed_geometry_hashes` の解放

手元で通せる検証 1、2、4、5 は、Task 1 から 5 のテストが同じ範囲を覆う。
実データを 1 都市通す確認は、契約前でも現サーバに対して行える。
