# Plateau Building Data Pipeline for Rapid Editor

[PLATEAU](https://www.mlit.go.jp/plateau/)の建物データをPostGISに取り込み、[Rapidエディタ](https://github.com/nyampire/Rapid)が読めるOSM XMLで配信するバックエンドです。
扱うのはPLATEAUの建物だけで、橋梁、トンネル、植生などは対象外です。

エディタは<https://rapid.nyampire.info/>で公開しています。

関連するリポジトリは3つあります。

| リポジトリ | 役割 |
|---|---|
| [nyampire/rapid_plateau_api](https://github.com/nyampire/rapid_plateau_api) | このリポジトリ。取り込みと配信 |
| [nyampire/Rapid](https://github.com/nyampire/Rapid) | PLATEAUの建物を読み込む機能を足したRapidのフォーク |
| [nyampire/rapid_plateau_dashboard](https://github.com/nyampire/rapid_plateau_dashboard) | OSMへの取り込みの進み具合を集計するダッシュボード |

## 全体の流れ

建物データの入口は2つあります。
一つは、CityGMLをOSM形式に変換済みのデータを配信元からダウンロードする道です。
もう一つは、CityGMLを手元でcitygml-osmに通して変換する道で、配信元が止まっているあいだの代わりに使っています。

```
 配信元の変換済みデータ ── plateau_downloader.py ──┐
                                                    │
 G空間情報センターの CityGML ─ scripts/reimport/ ───┤  (citygml-osm で .osm に変換して送る)
                                                    ▼
                                  plateau_importer2postgis.py
                                                    │
                                                    ▼
                         PostGIS (plateau_buildings, plateau_building_nodes)
                                   │                          │
                                   ▼                          ▼
                          osmfj_plateau_api.py        rapid_plateau_dashboard
                          (OSM XML、対応エリア)       (同じデータベースを読む)
                                   │
                                   ▼
                              Rapid エディタ
```

取り込みの入力になる`.osm`の性質と、取り込みの側でどこまで対処できるかは[docs/converter-output.md](docs/converter-output.md)にあります。
変換から配信までの各段階が建物の形に何をするかの説明は、[docs/geometry-pipeline.md](docs/geometry-pipeline.md)です。

## はじめかた

Python 3と、PostGISを入れたPostgreSQLが要ります。
CIはPython 3.12で動かしています。

```bash
pip install -r requirements.txt
```

テーブルを作るSQLは[DEPLOY.md](DEPLOY.md)の「テーブル作成」にあります。
インポーターは足りない列のいくつかを自分で足しますが、テーブルそのものと`city_code`の列は作りません。

APIは、環境変数`DATABASE_URL`に書いた接続先を使います。

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/dbname"
python osmfj_plateau_api.py   # http://localhost:8000 で起動する
```

`DATABASE_URL`を設定しないと、コードに書いた開発用の接続先を使います。
サーバへの置き方（systemd、nginx、証明書）の手順も、[DEPLOY.md](DEPLOY.md)の中です。

## データの取り込み

### 配信元からダウンロードする

`plateau_downloader.py`は、配信元<http://surveyor.mydns.jp/task-bldg/>からメッシュごとのデータを取得します。

```bash
python plateau_downloader.py --list                # 取得できる市区町村の一覧 (配信元から毎回取得する)
python plateau_downloader.py --cityname 米子       # 市区町村名で探す (部分一致)
python plateau_downloader.py --citycode 31202      # 市区町村コードを指定して取得する
python plateau_downloader.py --citycode 13101 --output-dir ./chiyoda_data
python plateau_downloader.py --all                 # すべての市区町村を取得する (都市のあいだに 30 秒待つ)
python plateau_downloader.py --all --city-interval 60
```

取得したデータは`./plateau_data/{市区町村コード}/`に入ります。
多数の都市をまとめて取得して取り込むときは、`batch_import_2024.py`が2つの処理を都市ごとに続けて実行します。

### 手元で変換する

CityGMLを手元で変換して取り込む手順は[scripts/reimport/README.md](scripts/reimport/README.md)にあります。
手元で取り出しから変換、サーバへの転送までを行い、サーバ側の取り込みは[deploy/README.md](deploy/README.md)の手順で動かします。

### PostGISに取り込む

```bash
# 市区町村コードは data-dir のディレクトリ名から推定する
python plateau_importer2postgis.py --data-dir ./plateau_data/31202 --postgres-url "$DATABASE_URL"

# 市区町村コードを指定する
python plateau_importer2postgis.py --data-dir ./plateau_data/31202 --citycode 31202 --postgres-url "$DATABASE_URL"

# 座標の範囲を検査する (min_lat,max_lat,min_lon,max_lon)
python plateau_importer2postgis.py --data-dir ./plateau_data/31202 --coord-bounds "35.2,35.6,133.0,133.5" --postgres-url "$DATABASE_URL"

# zip を展開せず、data-dir の下の .osm を直接読む (手元で変換したデータ)
python plateau_importer2postgis.py --data-dir <ディレクトリ> --citycode 30406 --no-zip --postgres-url "$DATABASE_URL"
```

市区町村コードが決まっていれば、インポーターは取り込みの前にその都市の既存のデータを消します。
そのため、取り込み直すときに`plateau_purge.py`を先に実行する必要はありません。

ダッシュボードの`dash_city_master`テーブルがあれば、インポーターは市区町村の行政界の外に重心が落ちる建物を消します。
隣の都市と重なるメッシュで、同じ建物が両方の都市に入るのを防ぐためです。
このテーブルがなければ、この処理だけを警告を出して飛ばします。

取り込んだあとは、対応エリアの表示を更新するために`python plateau_coverage.py --refresh`を実行してください。

## API

| 窓口 | 内容 |
|---|---|
| `GET /` | APIの情報 |
| `GET /health` | 動作確認 |
| `GET /api/stats` | データベースの統計 |
| `GET /api/mapwithai/buildings?bbox=min_lon,min_lat,max_lon,max_lat` | 範囲内の建物（OSM XML） |
| `GET /api/mapwithai/coverage` | 対応エリア。都市ごとの凹包のGeoJSON FeatureCollection |

ダッシュボードの`/api/dashboard/*`は、ダッシュボードのモジュールが置かれているときだけ組み込まれます。

### 建物の取得のパラメータ

| パラメータ | 型 | 既定値 | 説明 |
|---|---|---|---|
| `bbox` | string | （必須） | `min_lon,min_lat,max_lon,max_lat` |
| `limit` | int | 1000 | 返す建物の最大件数 |
| `use_intersects` | bool | true | trueなら範囲と交わる建物を、falseなら重心が範囲に入る建物を返す |

### LOD2の建物とリレーション

LOD2の建物は、外形と、屋根などの部分から成ります。
APIはこれを`type=building`のリレーションとして返し、部分は`building:part`のwayになります。
外形と部分が同じ座標を通る箇所ではノードを共有させているので、エディタで形を編集しても外形と部分がずれません。

同じ建物が隣り合う2つの都市のデータに入っているときは、応答を作る段階で1件にまとめます。
`dash_city_master`に行政界があれば、重心がその都市の行政界の外にある建物も返しません。

### 返すOSMタグ

| タグ | 説明 |
|---|---|
| `building` | 建物の種類 |
| `height` | 高さ（m） |
| `ele` | 標高（m） |
| `building:levels` | 階数 |
| `name` | 名称 |
| `addr:housenumber` | 番地 |
| `addr:street` | 通り名 |
| `start_date` | 建設年 |
| `building:material` | 建材 |
| `roof:material` | 屋根材 |
| `roof:shape` | 屋根の形 |
| `amenity` | 施設の種類 |
| `shop` | 店舗の種類 |
| `tourism` | 観光施設 |
| `leisure` | レジャー施設 |
| `landuse` | 土地利用 |

各wayには`representative_point`も付けます。
外形の内側に必ず入る代表点で、`lon,lat`の形です。
Rapidのタグ転記の機能は、PLATEAUの外形がどのOSMの建物に当たるかを、この点で判定します。
凹んだ形では重心が外形の外に出るので、重心ではなく内側の点を使っています。
形が壊れていて代表点を計算できない建物には、このタグを付けません。

高さが疑わしい建物には`plateau:height_warning`を付けます。
値は、当てはまった検査の名前を`degenerate-area`、`part-over-outline`、`absolute`、`needle`、`floor-height`の順に`;`でつないだものです。
`absolute`は高さが1.0 m未満のときだけ、`floor-height`は`building`がhouse、apartments、residentialのときだけ当てはまります。
`needle`は部分には付けません。
`needle`が当てはまったときは、底面積を`plateau:footprint_m2`に平方メートル単位、小数点以下4桁で付けます。

`plateau:height_warning`と`plateau:footprint_m2`はOSMのタグではないので、エディタがOSMに書き込む前に取り除く必要があります。
APIのこの変更をデプロイするときは、これらのタグを取り除くエディタ側の変更を先に本番へ入れてください。

## データベース

テーブルの作成は[DEPLOY.md](DEPLOY.md)のSQLで行います。
次の表の列は、そのSQLと、インポーターが足す列を合わせたものです。

### plateau_buildings

建物の外形と部分を1行ずつ持ちます。

| 列 | 型 | 説明 |
|---|---|---|
| id | serial | 主キー |
| osm_id | bigint | OSM互換のID |
| building | text | 建物の種類（`yes`、`apartments`など） |
| height | double precision | 高さ（m） |
| ele | double precision | 標高（m） |
| building_levels | integer | 階数 |
| building_levels_underground | integer | 地下の階数。インポーターは値を入れない |
| source_dataset | text | 取り込み元（`plateau_{市区町村コード}_{ファイル}`） |
| plateau_id | text | 変換出力の元の要素。way由来は`w-123`、multipolygon由来は`r-456`。変換をやり直すと値が変わるので、安定した識別子としては`ref_mlit_plateau`を使う |
| geometry_wkt | text | 外形のWKT |
| name | text | 名称 |
| addr_full | text | 住所（通り名と番地をつないだもの） |
| addr_housenumber | text | 番地 |
| addr_street | text | 通り名 |
| start_date | text | 建設年 |
| building_material | text | 建材 |
| roof_material | text | 屋根材 |
| roof_shape | text | 屋根の形 |
| amenity | text | 施設の種類 |
| shop | text | 店舗の種類 |
| tourism | text | 観光施設 |
| leisure | text | レジャー施設 |
| landuse | text | 土地利用 |
| geom | geometry(Polygon, 4326) | 外形 |
| centroid | geometry(Point, 4326) | 重心 |
| city_code | varchar(5) NOT NULL | 市区町村コード |
| building_part | text | 部分のとき`building:part`の値。外形ではNULL |
| parent_building_id | integer | 部分のとき、親の外形の`id`。親を消すと部分も消える（ON DELETE CASCADE） |
| ref_mlit_plateau | text | PLATEAUの建物ID |

索引は`geom`と`centroid`（GiST）、`osm_id`、`city_code`、`parent_building_id`に張ってあります。

### plateau_building_nodes

外形と部分の頂点を1行ずつ持つテーブルです。

| 列 | 型 | 説明 |
|---|---|---|
| id | serial | 主キー |
| osm_id | bigint | ノードのID（負の値） |
| building_id | integer | 建物の`id`（外部キー。建物を消すとノードも消える） |
| sequence_id | integer | ノードの順番。環ごとに0から数え直す |
| ring_id | integer NOT NULL DEFAULT 0 | 環の種類。0は外側の環、1以上は穴 |
| lat | double precision | 緯度 |
| lon | double precision | 経度 |
| geom | geometry(Point, 4326) | 点 |

### plateau_coverage（マテリアライズドビュー）

対応エリアを表示するために、都市ごとの凹包を持ちます。
`plateau_coverage.py --init`で作ります。

| 列 | 型 | 説明 |
|---|---|---|
| city_code | varchar(5) | 市区町村コード（主キー） |
| geom | geometry | 建物の重心から作った凹包（ConcaveHull、target_percent=0.5） |
| building_count | int | 都市の建物の数 |

建物を取り込んだり消したりしたあとは、`REFRESH MATERIALIZED VIEW CONCURRENTLY`で作り直します。
作り直すたびに全都市の凹包を計算し直すので、都市が多いほど時間とメモリを使います。
多数の都市を続けて処理するときは、`plateau_purge.py --skip-coverage-refresh`で都市ごとの作り直しを省き、最後に`plateau_coverage.py --refresh`を一度だけ実行してください。

### plateau_purge_history

`plateau_purge.py`で都市のデータを消した記録です。
`plateau_purge.py --init-audit-table`で作ります。

| 列 | 型 | 説明 |
|---|---|---|
| id | serial | 主キー |
| city_code | varchar(5) | 消した都市 |
| buildings_deleted | int | 消した建物の数 |
| nodes_deleted | int | 消したノードの数 |
| executed_at | timestamp | 実行した日時 |
| executed_by | text | 実行したユーザ |
| hostname | text | 実行したサーバ |
| duration_seconds | real | かかった時間 |

## 運用の道具

どの道具も、既定では何も変えずに結果を表示するだけです。
`--execute`を付けたときだけデータベースを変えます。

### 対応エリアのビュー（`plateau_coverage.py`）

```bash
python plateau_coverage.py --init      # ビューを作り、初回の REFRESH をする
python plateau_coverage.py --refresh   # データを変えたあとに REFRESH する (CONCURRENTLY)
python plateau_coverage.py --reinit    # ビューの定義を変えたあとに作り直す (DROP、CREATE、REFRESH)
python plateau_coverage.py --status    # 状態を見る
```

調べるときは、`--refresh`に`--no-concurrent`を付けるとCONCURRENTLYを使わずにREFRESHします。

### 都市のデータを消す（`plateau_purge.py`）

OSMへの取り込みが終わった都市のデータを消す道具です。

```bash
python plateau_purge.py --init-audit-table     # 記録のテーブルを作る (初回だけ)
python plateau_purge.py --citycode 13112       # 消す対象を確かめる
python plateau_purge.py --citycode 13112 --execute
python plateau_purge.py --citycode 13112 --execute --skip-coverage-refresh   # 対応エリアの REFRESH を省く
```

`--execute`を付けると、市区町村コードと`DELETE`の文字列を続けて入力するよう求めます。
同時に2つ実行しないよう`pg_advisory_lock`を取り、消した結果を`plateau_purge_history`に記録します。
消したあとは`VACUUM ANALYZE`と対応エリアのREFRESHを自動で実行します。

### 建物の高さの調査（`plateau_height_outliers.py`）

高さが疑わしい建物を、検査ごとに数えます。
データベースは読むだけです。
検査の内容と閾値は[docs/height-outlier-survey.md](docs/height-outlier-survey.md)にあります。

```bash
python plateau_height_outliers.py --postgres-url "$DATABASE_URL"
python plateau_height_outliers.py --postgres-url "$DATABASE_URL" --check needle --city 13101
```

APIが付ける`plateau:height_warning`は、この調査と同じ閾値を使います。

### 既存のデータベースの移行

`plateau_migrate.py`と`plateau_migrate_fk_cascade.py`は、古い定義のデータベースを今の定義に揃える一度きりの移行です。
新しく作るデータベースでは要りません。

`plateau_migrate.py`は`plateau_buildings`に`city_code`の列を足し、既存の行に値を入れて索引とNOT NULL制約を付けます。
途中で失敗しても、もう一度実行すれば続きから進みます。
元に戻すときは`ALTER TABLE plateau_buildings DROP COLUMN city_code;`を実行します。

```bash
python plateau_migrate.py                    # 確かめるだけ (ディスクの空きや、コードを取り出せるか)
python plateau_migrate.py --execute          # 実行する (確認のプロンプトあり)
python plateau_migrate.py --execute --yes    # プロンプトを省く
```

`plateau_migrate_fk_cascade.py`の役割は、`plateau_building_nodes.building_id`の外部キーにON DELETE CASCADEを付けることです。

## テスト

```bash
pip install -r requirements-dev.txt
pytest
pytest --cov=. --cov-report=term   # カバレッジを表示する
```

GitHub Actionsでも同じテストを実行します。
一部のテストは実際のPostgreSQLを使い、環境変数`PLATEAU_TEST_DATABASE_URL`を設定したときだけ動きます。
詳しくは[tests/README.md](tests/README.md)にあります。

## 関連する文書

| 文書 | 内容 |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 全体の構造と、これまでの設計判断 |
| [DEPLOY.md](DEPLOY.md) | サーバの構築とデプロイの手順 |
| [deploy/README.md](deploy/README.md) | サーバ側の再取り込みの手順 |
| [scripts/reimport/README.md](scripts/reimport/README.md) | 手元でCityGMLを変換して送る手順 |
| [docs/converter-output.md](docs/converter-output.md) | citygml-osmの出力の性質 |
| [docs/geometry-pipeline.md](docs/geometry-pipeline.md) | 各段階が建物の形に何をするか |
| [docs/height-outlier-survey.md](docs/height-outlier-survey.md) | 建物の高さの調査 |
| [docs/upstream-consultation.md](docs/upstream-consultation.md) | citygml-osmの上流への相談 |

## ライセンス

PLATEAUの建物データは、[PLATEAU](https://www.mlit.go.jp/plateau/)の利用規約に従います。
