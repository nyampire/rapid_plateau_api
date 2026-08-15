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
| `PLATEAU_IMPORT_DIR` | 手元から送られた `.osm` の置き場所。手元の `SHIP_PATH` と同じ絶対パスを指す |
| `PLATEAU_LOG_DIR` | ログの置き場所。既定は `$HOME/reimport_logs` |
| `THRESHOLD_KB` | 取り込み前に要求する空き。既定 5GB |
| `WRAPPER_PATH` | watchdog が wrapper を見分けるために使う `reimport_one.sh` の絶対パス |
| `REIMPORT_ONE` | バッチが呼ぶ `reimport_one.sh` の場所 |

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

## 148 都市を流す

第 1 段 (手元) が終わってから始める。並行させない。

開始前に確かめること。

1. `done.txt` と `failed.txt` が空である。過去の実行のぶんが残ると都市が飛ばされる
2. 148 都市すべてが `dash_city_master` に行を持ち、`boundary_geom` が NULL でない
3. `plateau_building_nodes.building_id` の外部キーが `ON DELETE CASCADE` である

2 と 3 はどちらも、取り込み時にしか効かず後から掛け直せない。

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
rm -f ~/reimport_logs/batch_status   # 消さないと watchdog が即座に終わる
# 取り込みが残っていないことを確かめてから
pgrep -f plateau_importer2postgis.py
# バッチ、5 秒待って watchdog の順で起動し直す
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
