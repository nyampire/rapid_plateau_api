# 取り込み中の入力の保護と、退避の掃除 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 取り込みの走行中に同じ都市を送り直しても取り込み中の入力が変わらないようにし、手元に溜まる退避ディレクトリに上限を設ける。

**Architecture:** 転送先を `<根>/.incoming/<都市>/` に分け、取り込み側が開始時に `<根>/<都市>` へ rename して自分のものにしてから読む。rename は同一ファイルシステム内で不可分なので、確定後の入力を送る側が触ることが無くなる。あわせて `ship_city.sh` が作る退避ディレクトリを新しい順に既定 3 件へ絞る。

**Tech Stack:** bash (`set -uo pipefail`)、pytest + `subprocess`、PATH に置く偽コマンドによる差し替え。

## Global Constraints

- 設計文書: `docs/superpowers/specs/2026-08-20-reimport-input-handover-design.md`
- 設定値 `SHIP_PATH` と `PLATEAU_IMPORT_DIR` の意味は変えない。どちらもこれまでどおり同じ絶対パス（転送先の根）を指す
- 新しい終了コードを増やさない。入力に関する失敗は既存の `EXIT_INPUT=13`、転送は `EXIT_TRANSFER=12` を使う
- 数値として比較する値は、比較の前に必ず既存の `need_int` を通す。通らなければ止める（黙って分岐を消さない）
- グロブを `for` に渡すところは `shopt -s nullglob` で囲む。展開されないとリテラルが入る
- 数えた結果を後で使う `while read` は、パイプの右側に置かない。プロセス置換 `< <(...)` から読む
- テストは落ちる側と通る側の両方を書く。件数は 2 種類使い、1 つの値に合わせた実装が通らないようにする
- 公開リポジトリなので、実ホスト名・実パス・認証情報をコード・テスト・コミットメッセージに書かない。テストの宛先は既存の `stubhost` と `/stub/import` を使う
- コミットメッセージは ですます調にしない（コード側の既存の慣習に合わせる）。Issue / PR 本文は ですます調

---

## File Structure

| ファイル | 役割 | この計画での扱い |
|---|---|---|
| `scripts/reimport/ship_city.sh` | 手元。1 都市の取り出し・変換・転送 | 退避の掃除を足す（Task 1）、転送先を `.incoming` に変える（Task 3） |
| `scripts/reimport/ship_all.sh` | 手元。計画の全都市を順に回す | 起動時に退避の残量を出す（Task 1） |
| `scripts/reimport/ship.env.example` | 手元の設定の見本 | `KEEP_RETAINED_DIRS` を足す（Task 1） |
| `deploy/reimport_one.sh` | サーバ。1 都市の取り込み | 入力を確定する段を足す（Task 2） |
| `tests/test_ship_city.py` | `ship_city.sh` の門を固定 | Task 1 と Task 3 のテスト |
| `tests/test_ship_all.py` | `ship_all.sh` の門を固定 | Task 1 のテスト |
| `tests/test_reimport_one.py` | `reimport_one.sh` の分岐を固定 | Task 2 のテスト |
| `scripts/reimport/README.md` | 第 1 段の手順 | Task 4 |
| `deploy/README.md` | 第 2 段の手順 | Task 4 |

タスクの順序は「取り込み側 → 送る側」にする。
Task 2 の取り込み側は `.incoming` が無い配置も受けるので、Task 2 だけが入った時点でも従来の送る側と組み合わせて動く。

---

## Task 1: 手元の退避に上限を設ける (#58)

**Files:**
- Modify: `scripts/reimport/ship_city.sh`（設定の読み取り 29 行目付近、`bail()` 56-65 行、前回の退避 112-116 行）
- Modify: `scripts/reimport/ship_all.sh`（`mkdir -p "$WORK_ROOT"` の直後、72-75 行付近）
- Modify: `scripts/reimport/ship.env.example`（末尾）
- Test: `tests/test_ship_city.py`、`tests/test_ship_all.py`

**Interfaces:**
- Consumes: 既存の `need_int`、`say`、`WORK_ROOT`、`bail()`
- Produces: `ship_city.sh` の `prune_retained()`（引数なし、`WORK_ROOT` を見て古い退避を消す）、`ship_all.sh` の `report_retained()`（引数なし、件数と合計 KB を 1 行出す）、設定 `KEEP_RETAINED_DIRS`（既定 3、`0` で無効）

- [ ] **Step 1: 退避の掃除の失敗する側と通る側のテストを書く**

`tests/test_ship_city.py` の末尾に足す。

```python
def _retained(e, name):
    """退避ディレクトリを 1 件作る。中身も置いて du が 0 にならないようにする。"""
    d = e.work_root / name
    d.mkdir()
    (d / 'dummy.osm').write_text('<osm/>')
    return d


def _fails_at_extract(e):
    """取り出しで落ちる偽 extract。作業ディレクトリは作るので退避が起きる。"""
    _stub(e.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":2,"raw_bytes":5}\'')


def test_retained_dirs_are_pruned_to_the_cap(env):
    """上限 3、退避 5 件のところへ 1 件増えると、古い 3 件が消えて 3 件残る。

    掃除が走るのは退避を作った瞬間だけなので、失敗経路を通す。
    """
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000',
               '20260813-000000', '20260814-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 3, kept
    # 新しい 2 件と、いま作られた 30406 の退避が残る
    assert '11111.stale.20260813-000000' in kept, kept
    assert '11111.stale.20260814-000000' in kept, kept
    assert any(n.startswith('30406.failed.') for n in kept), kept
    assert '11111.stale.20260810-000000' not in kept, kept


def test_prune_cap_of_one_keeps_only_the_newest(env):
    """上限 1 のとき、いま作った 1 件だけが残る。

    件数を 2 種類 (3 と 1) 使う。上限 3 に固定した実装を通さないため。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=1\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 1, kept
    assert kept[0].startswith('30406.failed.'), kept


def test_prune_disabled_by_zero(env):
    """上限 0 では 1 件も消さない。3 件 + 新しい 1 件で 4 件残る。"""
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=0\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 4, kept


def test_success_does_not_prune(env):
    """成功した回は退避を作らないので、掃除も走らない。

    上限 1 で退避 3 件のまま成功させても消えない。
    「起動時に無条件で掃除する」実装をここで落とす。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=1\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert len(list(env.work_root.glob('*.stale.*'))) == 3


def test_prune_refuses_when_the_cap_is_not_a_number(env):
    """上限が数字でなければ、掃除を飛ばさずに止める。

    数値でないまま [ "$n" -le "$KEEP_RETAINED_DIRS" ] を評価すると
    integer expression expected でエラー終了し、if がそれを偽として扱う。
    掃除が黙って消えるので、ここは止める側でなければならない。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=three\n')

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert 'KEEP_RETAINED_DIRS' in r.stdout + r.stderr


def test_prune_orders_by_name_not_mtime(env):
    """並べ替えは名前の末尾の日時で行う。

    mv はディレクトリの mtime を退避した時刻に更新しないので、
    mtime 順に消すと実際の退避の順と食い違う。
    名前と mtime の順を逆にしておき、名前順で消えることを見る。

    上限 2、退避 2 件 + 新しい 1 件 = 3 件なので、消えるのは 1 件だけ。
    名前順なら 20260810 が、mtime 順なら 20260812 が消える。
    """
    import os
    import time
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=2\n')
    old_name = _retained(env, '11111.stale.20260810-000000')
    new_name = _retained(env, '11111.stale.20260812-000000')
    now = time.time()
    os.utime(new_name, (now - 100000, now - 100000))  # 名前は新しいが mtime は古い
    os.utime(old_name, (now, now))                    # 名前は古いが mtime は新しい
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert '11111.stale.20260812-000000' in kept, kept
    assert '11111.stale.20260810-000000' not in kept, kept


def test_prune_is_quiet_when_there_is_nothing_else_to_remove(env):
    """他に退避が無くてもエラーにならない。

    グロブが展開されないとリテラルの文字列が for に渡る。
    nullglob が無いと存在しないパスを見に行くことになる。
    """
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 1, kept
```

- [ ] **Step 2: テストが落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -k "prune or retained" -v`
Expected: FAIL。掃除がまだ無いので、件数の表明が全て食い違う。
`test_prune_refuses_when_the_cap_is_not_a_number` は returncode が 1 にならずに落ちる。

- [ ] **Step 3: `ship.env.example` に設定を足す**

末尾（`EXPECTED_CITIES=148` の下）に足す。

```bash

# 退避した作業ディレクトリを何件残すか。0 で掃除しない。
# 失敗時の <都市>.failed.<日時> と、再実行時の <都市>.stale.<日時> を
# まとめて WORK_ROOT 全体で数える。1 件で最大 4.4GB 使う。
KEEP_RETAINED_DIRS=3
```

- [ ] **Step 4: `ship_city.sh` に掃除を実装する**

`: "${DISK_MIN_KB:=5242880}"`（29 行目）の直後に足す。

```bash
: "${KEEP_RETAINED_DIRS:=3}"
```

`need_int` の定義より後、`say "=== START ==="` より前にある `DISK_MIN_KB` の
検査の直後に、同じ形の検査を足す。

```bash
# KEEP_RETAINED_DIRS も ship.env から来る。書き損じると
# [ "$n" -gt "$KEEP_RETAINED_DIRS" ] が integer expression expected で
# エラー終了し、if がそれを偽として扱う。掃除が黙って消えるので、ここで止める。
if ! need_int "$KEEP_RETAINED_DIRS"; then
  say "ABORT: KEEP_RETAINED_DIRS が数字でない (値: $KEEP_RETAINED_DIRS)"
  exit 1
fi
```

`bail()` の定義より前に、掃除の本体を置く。

```bash
# 退避した作業ディレクトリを、新しい順に KEEP_RETAINED_DIRS 件だけ残す。
#
# 並べ替えは名前の末尾の日時で行う。日時は %Y%m%d-%H%M%S なので辞書順が
# 時刻順に一致する。mtime 順にはしない。mv はディレクトリの mtime を
# 退避した時刻に更新せず、中身を最後に変えた時刻のまま残すので、
# 実際の退避の順と食い違う。
#
# 数えた結果を後で使うので while read はパイプの右側に置かない。
# パイプの左側はサブシェルになり、そこでの代入が親に伝わらない。
prune_retained() {
  if [ "$KEEP_RETAINED_DIRS" -eq 0 ]; then
    return 0
  fi
  local d ts line
  local -a entries=()
  shopt -s nullglob
  for d in "$WORK_ROOT"/*.failed.* "$WORK_ROOT"/*.stale.*; do
    [ -d "$d" ] || continue
    ts="${d##*.}"
    entries+=("$ts"$'\t'"$d")
  done
  shopt -u nullglob
  if [ "${#entries[@]}" -le "$KEEP_RETAINED_DIRS" ]; then
    return 0
  fi
  while IFS= read -r line; do
    d="${line#*$'\t'}"
    rm -rf "$d"
    say "古い退避を消した: $(basename "$d")"
  done < <(printf '%s\n' "${entries[@]}" | sort -r | tail -n +$((KEEP_RETAINED_DIRS + 1)))
}
```

`bail()` の中、`mv "$WORK" "$kept"` と `say "作業ディレクトリを退避した: $kept"` の
あとに掃除を呼ぶ。

```bash
bail() {
  local code=$1 msg=$2
  say "FAIL: $msg"
  if [ -d "$WORK" ]; then
    local kept="$WORK.failed.$(date '+%Y%m%d-%H%M%S')"
    mv "$WORK" "$kept"
    say "作業ディレクトリを退避した: $kept"
    prune_retained
  fi
  exit "$code"
}
```

前回の残骸を退かす経路（112-116 行）でも、`mv` が成功したあとに呼ぶ。

```bash
if [ -e "$WORK" ]; then
  if ! mv "$WORK" "$WORK.stale.$(date '+%Y%m%d-%H%M%S')"; then
    say "ABORT: 前回の作業ディレクトリを退避できない (値: $WORK)"
    exit 3
  fi
  prune_retained
fi
```

- [ ] **Step 5: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: PASS（既存の 30 件と、足した 7 件）

- [ ] **Step 6: `ship_all.sh` の残量表示のテストを書く**

`tests/test_ship_all.py` の末尾に足す。

このファイルの `env` fixture は `WORK_ROOT` に `tmp_path` そのものを渡している
(`e.work_root` は無い。`e.tmp` を使う)。
`EXPECTED_CITIES=3` なので、`_write_plan(env, [...])` で 3 都市の計画を必ず置く。
置かないと計画のファイルが無くて exit 3 になり、残量の行まで到達しない。

```python
def test_reports_retained_dirs_at_start(env):
    """起動時に退避の件数と合計サイズを出す。"""
    _write_plan(env, ['11111', '22222', '33333'])
    for ts in ['20260810-000000', '20260811-000000']:
        d = env.tmp / ('11111.failed.' + ts)
        d.mkdir()
        (d / 'dummy.osm').write_text('<osm/>' * 100)

    r = _run(env)

    assert '退避 2 件' in r.stdout, r.stdout
    assert '合計' in r.stdout, r.stdout


def test_reports_zero_retained_dirs_without_failing(env):
    """退避が 0 件でも落ちない。

    グロブが展開されないとリテラルが for に渡り、du が存在しないパスを見る。
    """
    _write_plan(env, ['11111', '22222', '33333'])

    r = _run(env)

    assert '退避 0 件' in r.stdout, r.stdout
```

- [ ] **Step 7: テストが落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_all.py -k retained -v`
Expected: FAIL。`退避 2 件` も `退避 0 件` も出力に無い。

- [ ] **Step 8: `ship_all.sh` に残量表示を実装する**

`need_int` の定義の後に本体を置く。

```bash
# 退避した作業ディレクトリの残量を見せる。ship_city.sh の
# KEEP_RETAINED_DIRS で自動的に減るが、残っている量は目に入れておく。
# 148 都市を流している最中に空き容量の門で止まったとき、原因が
# 退避の堆積だと気づけるようにするための 1 行である。
report_retained() {
  local d kb
  local -a dirs=()
  shopt -s nullglob
  for d in "$WORK_ROOT"/*.failed.* "$WORK_ROOT"/*.stale.*; do
    [ -d "$d" ] && dirs+=("$d")
  done
  shopt -u nullglob
  if [ "${#dirs[@]}" -eq 0 ]; then
    say "退避 0 件"
    return 0
  fi
  kb=$(du -sk "${dirs[@]}" 2>/dev/null | awk '{s+=$1} END {print s+0}')
  say "退避 ${#dirs[@]} 件 (合計 ${kb} KB)"
}
```

`mkdir -p "$WORK_ROOT"` が成功した直後（72-75 行の `fi` の後）で呼ぶ。
`WORK_ROOT` が無いうちに呼ぶとグロブが何も拾わず、常に 0 件と出てしまう。

```bash
report_retained
```

- [ ] **Step 9: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_all.py tests/test_ship_city.py -v`
Expected: PASS

- [ ] **Step 10: コミット**

```bash
git add scripts/reimport/ship_city.sh scripts/reimport/ship_all.sh \
        scripts/reimport/ship.env.example \
        tests/test_ship_city.py tests/test_ship_all.py
git commit -m "feat(reimport): 手元の退避に保持件数の上限を設ける (#58)

失敗時の <都市>.failed.<日時> と再実行時の <都市>.stale.<日時> は
作られるだけで消す処理が無く、1 件最大 4.4GB で溜まる。

KEEP_RETAINED_DIRS (既定 3、0 で無効) を足し、退避を作った直後に
WORK_ROOT 全体で新しい順に絞る。並べ替えは名前の末尾の日時で行う。
mv はディレクトリの mtime を退避した時刻に更新しないため。

ship_all.sh は起動時に残量を 1 行出す。"
```

---

## Task 2: 取り込み側で入力を確定する (#57 サーバ側)

**Files:**
- Modify: `deploy/reimport_one.sh`（ディスクの門の直後、`if [ ! -d "$SRC" ]` の直前）
- Test: `tests/test_reimport_one.py`

**Interfaces:**
- Consumes: 既存の `SRC="$PLATEAU_IMPORT_DIR/$CITY"`、`EXIT_INPUT=13`、`ts()`
- Produces: 取り込み側は `$PLATEAU_IMPORT_DIR/.incoming/<都市>` を入力の受け口として読む。確定後の入力は `$PLATEAU_IMPORT_DIR/<都市>`。前回の残骸は `$PLATEAU_IMPORT_DIR/<都市>.stale`（日時なし、毎回置き換え）

- [ ] **Step 1: 確定の 4 通りの分岐のテストを書く**

`tests/test_reimport_one.py` の末尾に足す。

```python
def _incoming(e, code='30406', osm=2, manifest=None):
    """送る側が置く受け口 .incoming/<都市>/ を作る。"""
    d = e.import_dir / '.incoming' / code
    d.mkdir(parents=True)
    for i in range(osm):
        (d / ('5339450%d_bldg_6697_op.osm' % i)).write_text('<osm/>')
    (d / 'manifest.txt').write_text(str(osm if manifest is None else manifest) + '\n')
    return d


def test_claims_incoming_when_only_incoming_exists(env):
    """.incoming だけあるとき、rename して取り込む。"""
    inc = _incoming(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert not inc.exists()
    assert env.called.read_text().split()[0:2] == ['--data-dir',
                                                   str(env.import_dir / '30406')]


def test_uses_src_directly_when_only_src_exists(env):
    """<都市> だけあるとき、rename せずそのまま取り込む。

    確定したあとに落ちた回の再実行がこの経路になる。
    ここで止めると、その都市は手で片付けるまで何度でも失敗し続ける。
    """
    _city(env, osm=3)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split()[0:2] == ['--data-dir',
                                                   str(env.import_dir / '30406')]


def test_incoming_wins_and_old_src_is_retained(env):
    """両方あるとき、新しい .incoming を採り、古い <都市> を .stale へ退避する。

    枚数を 2 種類 (2 と 5) 使い、どちらが取り込まれたかを manifest で見分ける。
    """
    _city(env, osm=5)
    _incoming(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    # 古い方 (5 枚) が退避され、新しい方 (2 枚) が取り込まれた
    stale = env.import_dir / '30406.stale'
    assert stale.is_dir()
    assert stale.joinpath('manifest.txt').read_text().strip() == '5'
    assert not (env.import_dir / '.incoming' / '30406').exists()
    # 取り込み器には確定後のパスが渡る
    assert env.called.read_text().split()[0:2] == ['--data-dir',
                                                   str(env.import_dir / '30406')]


def test_stale_is_replaced_not_accumulated(env):
    """<都市>.stale が既にあっても、日時を増やさず置き換える。

    サーバの空き容量の門は 5GB が既定で、1 都市の入力は最大 4.4GB ある。
    退避を積むとこの門に当たる。
    """
    old = env.import_dir / '30406.stale'
    old.mkdir()
    (old / 'ancient.osm').write_text('<osm/>')
    _city(env, osm=5)
    _incoming(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    stale_dirs = sorted(p.name for p in env.import_dir.glob('30406.stale*'))
    assert stale_dirs == ['30406.stale'], stale_dirs
    assert not (env.import_dir / '30406.stale' / 'ancient.osm').exists()


def test_aborts_when_neither_exists(env):
    """どちらも無ければ従来どおり exit 13。"""
    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr
    assert '入力が無い' in r.stdout


def test_resend_during_import_does_not_touch_the_claimed_input(env):
    """確定後に .incoming へ送り直しても、取り込み中の入力は変わらない。

    取り込み器の偽物を、走っている最中に .incoming へ書き込む形にする。
    確定済みの <都市> の中身が変わらないことを、取り込み器自身に数えさせる。
    """
    _incoming(env, osm=2)
    env.stub.write_text(
        'import pathlib, sys, os\n'
        'src = pathlib.Path(sys.argv[sys.argv.index("--data-dir") + 1])\n'
        'before = sorted(p.name for p in src.glob("*.osm"))\n'
        # 取り込み中に送り直しが起きたことにする
        'inc = src.parent / ".incoming" / "30406"\n'
        'inc.mkdir(parents=True, exist_ok=True)\n'
        '(inc / "99999999_bldg_6697_op.osm").write_text("<osm/>")\n'
        'after = sorted(p.name for p in src.glob("*.osm"))\n'
        'pathlib.Path(%r).write_text(repr((before, after)))\n'
        'sys.exit(0 if before == after else 1)\n' % str(env.called))

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr + env.called.read_text()
```

- [ ] **Step 2: テストが落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_one.py -k "claims or incoming or stale or neither or resend or uses_src" -v`
Expected: FAIL。`.incoming` を読む処理がまだ無いので、`_incoming` だけを置いた回は
`入力が無い` の exit 13 になる。

- [ ] **Step 3: 確定の段を実装する**

`deploy/reimport_one.sh` の、ディスクの門（`exit $EXIT_DISK` で終わる `if` ブロック）の
直後、`if [ ! -d "$SRC" ]` の直前に足す。

```bash
  # 取り込む入力を rename で自分のものにする。
  #
  # 送る側は .incoming/<都市>/ にだけ書く。確定したあとの $SRC を触らないので、
  # 取り込みの走行中に同じ都市を送り直されても入力が入れ替わらない。
  # rename は同じファイルシステム内で不可分に起きる。
  #
  # これが無いと、開始時の枚数の門を通ったあとに rsync --delete が
  # ファイルを消せてしまう。取り込みは成功として記録され、送り直した .osm は
  # 一度も取り込まれないまま消えるので、どちらの記録からも気づけない。
  INCOMING="$PLATEAU_IMPORT_DIR/.incoming/$CITY"
  if [ -d "$INCOMING" ]; then
    if [ -e "$SRC" ]; then
      # 確定後に落ちた前回の残骸。日時は付けず 1 件だけ持つ。
      # サーバの空き容量の門は 5GB が既定で、1 都市の入力は最大 4.4GB ある。
      echo "[$(ts)] [$CITY] 前回の入力を ${SRC}.stale へ退避する"
      rm -rf "${SRC}.stale"
      if ! mv "$SRC" "${SRC}.stale"; then
        echo "[$(ts)] [$CITY] ABORT: 前回の入力を退避できない: $SRC"
        exit $EXIT_INPUT
      fi
    fi
    if ! mv "$INCOMING" "$SRC"; then
      echo "[$(ts)] [$CITY] ABORT: 入力を確定できない: $INCOMING"
      exit $EXIT_INPUT
    fi
    echo "[$(ts)] [$CITY] 入力を確定した: $SRC"
  fi
```

`.incoming` が無く `$SRC` がある場合は、この `if` を素通りして既存の検査へ進む。
どちらも無い場合も素通りし、既存の `if [ ! -d "$SRC" ]` が exit 13 を出す。

- [ ] **Step 4: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_one.py -v`
Expected: PASS（既存の件数と、足した 6 件）

- [ ] **Step 5: コミット**

```bash
git add deploy/reimport_one.sh tests/test_reimport_one.py
git commit -m "feat(reimport): 取り込み側で入力を rename して確定する (#57)

開始時の枚数の門を通ったあと、rsync --delete が入力を書き換えられた。
取り込みは成功として記録され、送り直した .osm は一度も取り込まれない
まま消えるので、手元の shipped.txt にもサーバの done.txt にも
その都市が古いまま残った痕跡が出なかった。

.incoming/<都市> を受け口にし、取り込み側が開始時に <都市> へ rename する。
確定後の入力は送る側が触らないので、既存の枚数の門が初めて意味を持つ。

.incoming が無く <都市> がある場合はそのまま続行する。確定後に落ちた回を
手で片付けずにやり直せるようにするため。"
```

---

## Task 3: 送る側の宛先を `.incoming` にする (#57 手元側)

**Files:**
- Modify: `scripts/reimport/ship_city.sh`（転送の節、189-203 行）
- Test: `tests/test_ship_city.py`

**Interfaces:**
- Consumes: Task 2 が定めた受け口 `$PLATEAU_IMPORT_DIR/.incoming/<都市>`
- Produces: 送る側は `$SHIP_HOST:$SHIP_PATH/.incoming/<都市>/` へ rsync し、同じ場所の `.osm` を数える。`SHIP_PATH` の意味は変えない

- [ ] **Step 1: 宛先と mkdir のテストを書く**

`tests/test_ship_city.py` の末尾に足す。

```python
def test_transfer_targets_the_incoming_directory(env):
    """rsync の宛先が .incoming/<都市>/ を指す。"""
    _good_extract(env, n=2)
    _good_java(env)
    seen = env.tmp / 'rsync_args.txt'
    _stub(env.bin, 'rsync', 'echo "$@" >> %s\nexit 0' % seen)
    _stub(env.bin, 'ssh', 'echo "$@" >> %s.ssh\necho 2' % seen)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert 'stubhost:/stub/import/.incoming/30406/' in seen.read_text()


def test_transfer_creates_the_incoming_directory_first(env):
    """rsync の前に mkdir -p が走る。

    rsync は宛先の最後の 1 段しか作らない。.incoming と <都市> の
    2 段を作る必要があるので、先に作っておかないと転送が落ちる。
    """
    _good_extract(env, n=2)
    _good_java(env)
    order = env.tmp / 'order.txt'
    _stub(env.bin, 'ssh', 'echo "ssh $2" >> %s\ncase "$2" in *mkdir*) ;; *) echo 2 ;; esac' % order)
    _stub(env.bin, 'rsync', 'echo rsync >> %s\nexit 0' % order)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    lines = order.read_text().splitlines()
    mkdir_at = next(i for i, l in enumerate(lines) if 'mkdir' in l)
    rsync_at = next(i for i, l in enumerate(lines) if l == 'rsync')
    assert mkdir_at < rsync_at, lines
    assert '/stub/import/.incoming/30406' in lines[mkdir_at]


def test_transfer_fails_when_the_destination_cannot_be_created(env):
    """転送先を作れなければ、転送の門で落ちる。"""
    _good_extract(env, n=2)
    _good_java(env)
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh',
          'case "$2" in *mkdir*) echo "mkdir: 権限がない" >&2; exit 1 ;; esac\necho 2')

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_remote_count_is_read_from_incoming(env):
    """枚数を数える先も .incoming/<都市> になる。

    宛先だけ変えて数える先を元のままにすると、常に 0 件を数えて
    転送の門が落ち続ける。逆に数える先だけ変えても素通りする。
    """
    _good_extract(env, n=2)
    _good_java(env)
    seen = env.tmp / 'ssh_args.txt'
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh',
          'echo "$2" >> %s\ncase "$2" in *mkdir*) ;; *) echo 2 ;; esac' % seen)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    find_lines = [l for l in seen.read_text().splitlines() if 'find' in l]
    assert find_lines, seen.read_text()
    assert '/stub/import/.incoming/30406' in find_lines[0]


def test_end_to_end_transfer_lands_in_incoming(env):
    """偽 rsync に実際にコピーさせ、.incoming/<都市>/ に届くことを見る。"""
    _good_extract(env, n=2)
    _good_java(env)
    remote = env.tmp / 'remote'
    _transfer_copies_for_real(env, remote)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    landed = remote / '.incoming' / '30406'
    assert landed.is_dir(), sorted(p.name for p in remote.rglob('*'))
    assert len(list(landed.glob('*.osm'))) == 2
    assert (landed / 'manifest.txt').read_text().strip() == '2'
```

- [ ] **Step 2: テストが落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -k "incoming or destination_cannot" -v`
Expected: FAIL。宛先が `/stub/import/30406/` のままなので、`.incoming` を含む表明が全て落ちる。

- [ ] **Step 3: 転送の節を書き換える**

`say "4/5 転送"`（189 行）から `REMOTE_N=...`（198 行）までを差し替える。

```bash
say "4/5 転送"
# 送る先を .incoming に分ける。取り込み側は開始時にこれを <都市> へ
# rename して自分のものにするので、確定後の入力を送る側が触らない。
# 走行中に送り直しても取り込み中の入力が入れ替わらない。
DEST="$SHIP_PATH/.incoming/$CITY"
# rsync は宛先の最後の 1 段しか作らない。.incoming と <都市> の 2 段を
# 作る必要があるので、先に作る。転送先の根が無いこともある。
if ! ssh "$SHIP_HOST" "mkdir -p '$DEST'"; then
  bail "$EXIT_TRANSFER" "転送先を作れない ($DEST)"
fi
# --delete は残す。このディレクトリを読むのは送る側だけになったので、
# 取り込み中のファイルを消す心配が無い。
rsync -az --delete \
  --include='*.osm' --include='manifest.txt' --exclude='*' \
  "$WORK/" "$SHIP_HOST:$DEST/"
RSYNC_EXIT=$?
if [ "$RSYNC_EXIT" -ne 0 ]; then
  bail "$EXIT_TRANSFER" "rsync が exit $RSYNC_EXIT"
fi

REMOTE_N=$(ssh "$SHIP_HOST" "find '$DEST' -maxdepth 1 -name '*.osm' | wc -l" | tr -d ' ')
```

`SSH_EXIT=$?` から下の枚数の門は変えない。

- [ ] **Step 4: テストが通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: PASS

- [ ] **Step 5: 全体のテストを流す**

Run: `python3 -m pytest -q`
Expected: PASS。421 件 + この計画で足した分。

- [ ] **Step 6: コミット**

```bash
git add scripts/reimport/ship_city.sh tests/test_ship_city.py
git commit -m "feat(reimport): 送る先を .incoming/<都市>/ に分ける (#57)

取り込み側が rename で確定する受け口へ送る。SHIP_PATH の意味は変えない。
rsync は宛先の最後の 1 段しか作らないので、先に mkdir -p する。"
```

---

## Task 4: 手順書を配置の変更に合わせる

**Files:**
- Modify: `scripts/reimport/README.md`（設定の節、第 1 段の流し方、注意）
- Modify: `deploy/README.md`（環境変数の表、手元との対応、開始前確認、終了コード）
- Test: 無し（文書のみ）

**Interfaces:**
- Consumes: Task 1〜3 が決めた配置と設定名
- Produces: 無し

- [ ] **Step 1: `scripts/reimport/README.md` を直す**

「第 1 段 (手元) の流し方」の `ship.env` の説明に `KEEP_RETAINED_DIRS` を足す。

```markdown
`ship.env` は `JAVA_BIN`、`CITYGML_OSM_JAR`、`SHIP_HOST`、`SHIP_PATH` など、
手元の環境と転送先に合わせて書き換える値を持つ。
`KEEP_RETAINED_DIRS` は退避した作業ディレクトリを何件残すかで、既定は 3、`0` で掃除しない。
各項目の意味は `ship.env.example` のコメントに書いてある。
```

同じ節に、退避の掃除を説明する段落を足す。

```markdown
失敗した都市の作業ディレクトリは `<都市>.failed.<日時>` として残る。
再実行のときに前回の残骸を退かしたものは `<都市>.stale.<日時>` になる。
どちらも `WORK_ROOT` 全体で新しい順に `KEEP_RETAINED_DIRS` 件だけ残り、古いものから消える。
`ship_all.sh` は起動時に残っている件数と合計サイズを 1 行出す。
```

「使い方」の節の、変換後の説明を配置の変更に合わせる。

```markdown
出力した `.gml` は citygml-osm にそのまま渡せる。
変換したあと `.osm` をサーバの `<SHIP_PATH>/.incoming/<都市>/` へ送り、`--no-zip` を付けて取り込む。
取り込み側は開始時にこれを `<SHIP_PATH>/<都市>` へ rename してから読む。
```

- [ ] **Step 2: `deploy/README.md` の環境変数の表を直す**

`PLATEAU_IMPORT_DIR` の行の説明を差し替える。

```markdown
| `PLATEAU_IMPORT_DIR` | 手元から送られた `.osm` の置き場所。必須 (既定なし)。手元の `SHIP_PATH` と同じ絶対パスを指す。この下に受け口の `.incoming/` と、確定した `<都市>/` が並ぶ |
```

- [ ] **Step 3: `deploy/README.md` に入力の確定の節を足す**

「手元との対応」の表の直後に足す。

```markdown
## 入力の確定

`PLATEAU_IMPORT_DIR` の下は 2 つに分かれる。

```
<PLATEAU_IMPORT_DIR>/
  .incoming/<都市>/     手元が送る先
  <都市>/               取り込み側が rename で確定した入力
```

`reimport_one.sh` は開始時に `.incoming/<都市>` を `<都市>` へ rename してから読む。
rename は同じファイルシステム内で不可分に起きるので、取り込みの走行中に
同じ都市を送り直されても、取り込み中の入力は変わらない。

`.incoming/<都市>` が無く `<都市>` だけがある場合は、そのまま取り込む。
確定したあとに落ちた回を、手で片付けずにやり直せるようにするためである。

両方ある場合は新しい `.incoming/<都市>` を採り、古い `<都市>` を `<都市>.stale` へ移す。
`<都市>.stale` は日時を付けず毎回置き換わるので、1 件までしか残らない。
```

- [ ] **Step 4: 開始前確認の 4 番を直す**

`comm` を使う確認は、入力が届く先が変わったので読み替える。

```markdown
4 を確かめずに始めると、一覧だけ届いて入力が届いていない状態のまま流してしまう。
`REIMPORT_ONE` の実体はバッチが起動時に検査するが、入力の有無は検査しないので、
1 都市目から「入力が無い」で数秒のうちに全都市が失敗し、`DONE` になる。

```bash
# 開始前: 一覧にあって入力が届いていない都市 (何も出なければ合格)
comm -23 <(sort ~/reimport_targets_<日時>.txt) \
         <(ls "$PLATEAU_IMPORT_DIR/.incoming" | sort)
```

前回の実行で確定まで進んだ都市は `.incoming` から消えて `<都市>/` に移っているので、
やり直しのときはこの確認に出てくる。`<都市>/` があればそのまま取り込まれるため、
出てきた都市がすべて `<都市>/` を持つなら送り直さなくてよい。
```

- [ ] **Step 5: 終了コードの表に確定の失敗を書き足す**

`reimport_one.sh` の 13 の行を差し替える。

```markdown
| 13 | 入力が無い、`manifest.txt` が無いか数字でない、`.osm` の枚数が manifest と違う、入力を確定できない (`.incoming` からの rename、または前回の入力の退避に失敗) |
```

- [ ] **Step 6: 公開してよい内容か確かめる**

この計画も手順書も公開リポジトリに入る。
**検査に使う文字列そのものをここに書かない。**
書いた瞬間に、検査したかった値が公開文書の本文に入る。

代わりに、足した行が次の形を含まないことを目で見る。

- リポジトリの外を指す絶対パス（システムのディレクトリから始まるもの）
- サーバの識別子（ホスト名、接続の別名、IP アドレス）
- 認証情報（パスワード、接続文字列に埋まった資格情報）
- 管理者権限を要する操作の具体的な手順
- 手元にしか無い設定ファイルへの言及

この節で足す文には、いずれも入れる理由が無い。
配置の説明は `<PLATEAU_IMPORT_DIR>` や `<SHIP_PATH>` のような変数名で書き、
実際の値を書かない。

```bash
git diff --stat
git diff -- scripts/reimport/README.md deploy/README.md | grep '^+'
```

足した行を目で読み、上の 5 つに当たるものが無いことを確かめる。

- [ ] **Step 7: コミット**

```bash
git add scripts/reimport/README.md deploy/README.md
git commit -m "docs(reimport): 入力の確定と退避の掃除を手順書に反映する (#57, #58)"
```

- [ ] **Step 8: 全体のテストと、公開前の最終確認**

```bash
python3 -m pytest -q
git log --format='%H%n%B' origin/main..HEAD
```

Expected: テストは PASS。

commit メッセージを目で読み、Step 6 の 5 つの形が無いことを確かめる。
**`git grep` はファイルの中身しか見ないので、メッセージはこの方法でしか見えない。**
`git filter-branch --tree-filter` もメッセージには触らない。
ファイルだけ直して push し、メッセージに残っていた事故が 2026-08-18 に起きている。

---

## 完了の条件

- `python3 -m pytest -q` が通る
- `.incoming` を使う経路と、`<都市>` だけの経路の両方にテストがある
- 退避の上限が 2 種類の件数 (3 と 1) で確かめてある
- `SHIP_PATH` と `PLATEAU_IMPORT_DIR` の設定値の意味が変わっていない
- 手順書の `comm` を使う確認が `.incoming` を見ている
- commit メッセージに機微情報が無い
