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
| `PLATEAU_APP_DIR` | リポジトリの置き場所。必須 (既定なし)。未設定だと `reimport_one.sh` は exit 1 で落ちる |
| `PLATEAU_VENV` | Python の仮想環境。省略すると `PYTHON_BIN` をそのまま使う |
| `PYTHON_BIN` | `reimport_one.sh` が呼ぶ Python の実行ファイル。既定 `python3` |
| `PLATEAU_ENV_FILE` | `DATABASE_URL` を含む設定。必須 (既定なし)。未設定だと exit 1、設定されていても実体が無いと exit 15 で止まる |
| `PLATEAU_IMPORT_DIR` | 手元から送られた `.osm` の置き場所。必須 (既定なし)。手元の `SHIP_PATH` と同じ絶対パスを指す |
| `PLATEAU_LOG_DIR` | `reimport_one.sh` が都市ごとのログを書く場所。既定は `$HOME/reimport_logs` |
| `REIMPORT_LOG_DIR` | `reimport_batch.sh` と `reimport_watchdog.sh` が読み書きする場所 (`done.txt`、`failed.txt`、`summary.log`、`batch_status` など)。既定は `$HOME/reimport_logs` |
| `THRESHOLD_KB` | `reimport_one.sh` が取り込み前に要求する空き (1K ブロック)。既定 5GB。数字でないと exit 15 で止まる |
| `DISK_WARN_KB` | watchdog がログに警告を出す空きの下限 (1K ブロック)。既定 5GB |
| `DISK_HALT_KB` | watchdog が pause を立てて止める空きの下限 (1K ブロック)。既定 3GB |
| `INTERVAL` | watchdog の監視間隔 (秒)。既定 60 |
| `MAX_CITY_MIN` | 1 都市の取り込みがこれを超えて続いたら watchdog が打ち切る時間 (分)。既定 90 |
| `WRAPPER_PATH` | watchdog が wrapper を見分けるために使う `reimport_one.sh` の絶対パス |
| `REIMPORT_ONE` | バッチが呼ぶ `reimport_one.sh` の場所。既定 `$HOME/reimport_one.sh`。実体が無いと exit 6 で止まる |
| `STRAY_IMPORT_PATTERN` | バッチが二重取り込み検出に使う `pgrep -f` の対象文字列。既定 `plateau_importer2postgis.py` |
| `EARLY_ABORT_FAILS` | バッチが連続失敗を早期に打ち切る閾値。既定 3、0 で無効。成功が 1 件も無いまま連続失敗が閾値に達すると exit 8 で止まる |

`PLATEAU_LOG_DIR` と `REIMPORT_LOG_DIR` は既定値が同じなので黙って一致しているように見えるが、別の変数である。
表にある方だけを設定すると、都市ごとのログとバッチのログが別の場所へ散る。

`THRESHOLD_KB` は `PLATEAU_APP_DIR` の在る区画を測る。
DB と入力が別ボリュームの構成では、ディスクの門がその区画しか見ていないことになるので別途見張ること。

## 手元との対応

2 つの機械で同じ場所を指す設定がある。ずれると転送先と読み先が食い違う。

| 手元 (`ship.env`) | サーバ | 関係 |
|---|---|---|
| `SHIP_PATH` | `PLATEAU_IMPORT_DIR` | 同じ絶対パスを指す |
| `SHIP_HOST` | — | ssh の宛先 |

`ship_all.sh` は都市の一覧を `$SHIP_PATH` の親へ置く。
そこを読むのは運用者なので、**`SHIP_PATH` の親をサーバのホームにしておく**と手順が短くなる。
`ship.env.example` の `/path/to/plateau_import` をそのまま使うと親が `/path/to` になり、
一覧を探す場所が変わる。

**`WRAPPER_PATH` は必ず実際の置き場所に合わせる。**
判定はコマンドラインの前方一致なので、ずれていると 90 分の打ち切りも `kill` も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

## 終了コード

`summary.log` には `FAIL exit=13` のような形で終了コードだけが残る。
3 本で終了コードの意味が違うので、スクリプトごとに分けて示す。
同じ番号でも別のスクリプトでは別の意味を持つことがある。

### `reimport_one.sh`

| コード | 意味 |
|---|---|
| 0 | 成功 |
| 1 | 必須の環境変数 (`PLATEAU_APP_DIR` / `PLATEAU_ENV_FILE` / `PLATEAU_IMPORT_DIR`) が未設定。bash 自身が落ちる |
| 2 | ディスク不足、または空き容量を読めない |
| 13 | 入力が無い、`manifest.txt` が無いか数字でない、`.osm` の枚数が manifest と違う |
| 14 | 取り込み器が exit 2 を返した写し (引数の不整合の可能性) |
| 15 | `PLATEAU_ENV_FILE` の実体が無い、または `THRESHOLD_KB` が数字でない |
| 127 | `reimport_one.sh` 自体が見つからない。bash がコマンドを実行できないときの標準の終了コード |
| 143 | watchdog による `SIGTERM` 打ち切り (128+15)。取り込み器ではなく watchdog が原因 |
| それ以外 | 取り込み器 (`plateau_importer2postgis.py`) の終了コードをそのまま返す |

127 と 143 は取り込み器の終了コードではない。
`summary.log` に `FAIL exit=143` と出ても取り込み器を疑わないこと。

### `reimport_batch.sh`

| コード | 意味 |
|---|---|
| 0 | 全都市を回し終えた、または `PAUSE` を検出して止まった |
| 2 | いずれかの都市で `reimport_one.sh` が exit 2 (ディスク不足) を返し、全体を止めた |
| 5 | 二重取り込み検出 (`STRAY_IMPORT`)。都市の切れ目で `STRAY_IMPORT_PATTERN` に一致するプロセスが既に走っている |
| 6 | 一覧が無い (`NO_LIST`)、一覧が空か件数を数えられない (`EMPTY_LIST`)、`REIMPORT_ONE` の実体が無い (`NO_WRAPPER`)、または `EARLY_ABORT_FAILS` が数字でない (`BAD_CONFIG`) |
| 8 | 早期打ち切り (`EARLY_ABORT`)。成功が 1 件も無いまま連続失敗が `EARLY_ABORT_FAILS` に達した |

`STRAY_IMPORT_PATTERN` の既定は `plateau_importer2postgis.py` で、環境変数で変えられる。

### `reimport_watchdog.sh`

| コード | 意味 |
|---|---|
| 0 | `batch_status` の出現を見て正常終了 |
| 2 | ディスクが `DISK_HALT_KB` を割った、または空き容量を読めない (pause を立てる) |
| 3 | 起動時の設定検査 (`INTERVAL` / `DISK_WARN_KB` / `DISK_HALT_KB` / `MAX_CITY_MIN` が数字でない)、または `WRAPPER_PATH` の実体が無い。pause は立てず watchdog だけが終わる |
| 4 | 1 都市の所要が `MAX_CITY_MIN` を超えて打ち切った (pause を立てる) |
| 7 | 直近 12 件の観測行のうち 3 件以上が `FAIL exit=` だった (pause を立てる) |

pause を立てる経路はディスク (2)、打ち切り (4)、連続失敗 (7) の 3 つに限る。
設定検査の失敗 (3) は pause を立てず、watchdog だけが終わる。
バッチは走り続けるので、監視が止まったと気づいたら watchdog を上げ直す。

## 148 都市を流す

第 1 段 (手元) が終わってから始める。並行させない。

開始前に確かめること。

1. `done.txt` と `failed.txt` が空である。過去の実行のぶんが残ると都市が飛ばされる
2. 148 都市すべてが `dash_city_master` に行を持ち、`boundary_geom` が NULL でない
3. `plateau_building_nodes.building_id` の外部キーが `ON DELETE CASCADE` である
4. 一覧に載った都市の入力が `PLATEAU_IMPORT_DIR` に届いている

2 と 3 はどちらも、取り込み時にしか効かず後から掛け直せない。

2 はダッシュボードを併設する構成では必須である。
併設しない構成では、行政界フィルタが効かず隣接市との重複が残ることを承知のうえでスキップしてよい
(`DEPLOY.md` の `dash_city_master` の説明も参照する)。

**確かめる相手はこれから流す都市であって、いま入っている都市ではない。**
新規構築のサーバは `plateau_buildings` が空なので、そこを起点にした問い合わせは
必ず 0 行を返し、何も確かめないまま合格に見える。

一覧の都市コードを一時表に読ませて突き合わせる。

```bash
# 一覧を一時表へ流し込み、境界の無い都市を挙げる (0 行なら合格)
psql "$DATABASE_URL" <<'SQL'
CREATE TEMP TABLE targets (city_code TEXT);
\copy targets FROM PROGRAM 'cat ~/reimport_targets_<日時>.txt'
SELECT t.city_code
FROM targets t
LEFT JOIN dash_city_master m ON m.city_code = t.city_code
WHERE m.city_code IS NULL OR m.boundary_geom IS NULL;
SQL
```

```sql
-- ノードの外部キーが CASCADE か ('c' なら合格)
SELECT confdeltype FROM pg_constraint
WHERE conname = 'plateau_building_nodes_building_id_fkey';
```

4 を確かめずに始めると、一覧だけ届いて入力が届いていない状態のまま流してしまう。
`REIMPORT_ONE` の実体はバッチが起動時に検査するが、入力の有無は検査しないので、
1 都市目から「入力が無い」で数秒のうちに全都市が失敗し、`DONE` になる。

```bash
# 開始前: 一覧にあって入力が届いていない都市 (何も出なければ合格)
comm -23 <(sort ~/reimport_targets_<日時>.txt) <(ls "$PLATEAU_IMPORT_DIR" | sort)
```

流す。

**順序を守る。バッチを先に起動する。**
watchdog は `batch_status` があると 1 秒で終了する。
前回が `PAUSED` や `DISK_ABORT` で終わっていると、そのファイルが残っている。
watchdog を先に上げると即座に落ち、残りの十数時間が無監視になる。
ログには 1 行出るだけで、`> /dev/null` に捨てられる。
バッチは起動時に `batch_status` を消すので、先に上げれば問題は出ない。

```bash
export REIMPORT_ONE=$HOME/reimport_one.sh
nohup bash ~/reimport_batch.sh ~/reimport_targets_<日時>.txt > /dev/null 2>&1 &
sleep 5   # バッチが batch_status を消すのを待つ
nohup bash ~/reimport_watchdog.sh > /dev/null 2>&1 &
```

`WRAPPER_PATH` は `REIMPORT_ONE` を既定にしてあるので、export しておけば片方で足りる。
2 つがずれると watchdog は一致するプロセスを 1 つも見つけないまま完走する。

## 1 都市目で止めて確かめる

`touch ~/reimport_pause` は `summary.log` に `[1/148] <都市>: START` が出てから行う。
バッチはループの先頭、つまり次の都市を始める手前でこのファイルを見る。
起動直後にすぐ触ると、1 都市目を走らせる前にこれを検出してしまい、
1 都市も取り込まないまま `PAUSED` を書いて exit 0 する。
5 秒後に上げる watchdog も `batch_status` にその `PAUSED` を見つけて即座に終了するので、
両方が消えて何も確かめないまま節の目的を失う。

`START` を確かめてから触れば、1 都市目の取り込みが進んでいる間に pause が立ち、
バッチは 1 都市目を終えたところで次の都市に進まず止まる。

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
```

起動し直す順序は最初と同じで、バッチを先に上げる。
バッチが起動時に `batch_status` を消すので、手で消す必要は無い。

```bash
export REIMPORT_ONE=$HOME/reimport_one.sh
nohup bash ~/reimport_batch.sh ~/reimport_targets_<日時>.txt > /dev/null 2>&1 &
sleep 5   # バッチが batch_status を消すのを待つ
nohup bash ~/reimport_watchdog.sh > /dev/null 2>&1 &
```

`failed.txt` に載った都市は `done.txt` に入らないので、再実行で必ずやり直される。

## 終わったことを確かめる

`failed.txt` に載った都市は目に付くが、バッチが途中で落ちて一覧の後半に
一度も到達しなかった都市は `failed.txt` にも `done.txt` にも現れないまま消える。
一覧の件数と `done.txt` の行数が一致したかを、`DONE` を見たあとに必ず確かめる。

```bash
# 終端: 一覧にあって done.txt に無い都市 (何も出なければ合格)
comm -23 <(sort ~/reimport_targets_<日時>.txt) <(sort "$REIMPORT_LOG_DIR/done.txt")
```

## 全部終わったあと

対応エリアのビューを 1 回だけ作る。
都市ごとには走らせない。メモリの小さいサーバでは OOM する。

```bash
cd "$PLATEAU_APP_DIR"
[ -n "${PLATEAU_VENV:-}" ] && . "$PLATEAU_VENV/bin/activate"
set -a; . "$PLATEAU_ENV_FILE"; set +a
"${PYTHON_BIN:-python3}" plateau_coverage.py --init --postgres-url "$DATABASE_URL"
```

`.env` 相当の `PLATEAU_ENV_FILE` を読み込む前に実行すると、`$DATABASE_URL` が
空文字のまま渡ってしまう。
システムの `python3` には `psycopg2` が無いので、venv の有効化も先に行う。

`--init` はビューを作ったあとリフレッシュまで済ませる。
**続けて `--refresh` を叩かない。**ビューが populated になっているので、
`--no-concurrent` を付けない限り CONCURRENTLY を選び、避けたい経路を踏む。
やり直すときは `--refresh --no-concurrent` を使う。

メモリが足りないときは、一時的に swap を足してから実行する。
swap の増設には管理者権限が要る。
