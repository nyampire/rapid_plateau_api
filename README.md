# Plateau Building Data Pipeline for RapiD Editor

[PLATEAU](https://www.mlit.go.jp/plateau/) の建物データを PostGIS に格納し、[RapiD エディタ](https://github.com/facebook/Rapid) 向けに OSM XML 形式で配信するパイプラインです。

## 構成

```
plateau_downloader.py        surveyor.mydns.jp からメッシュデータをダウンロード
        |
        v
plateau_importer2postgis.py  ダウンロードしたデータを PostGIS にインポート
        |
        v
osmfj_plateau_api.py         PostGIS から RapiD 向けに OSM XML を配信 (FastAPI)
```

## 必要環境

- Python 3.9+
- PostgreSQL + PostGIS
- 依存パッケージ:

```bash
pip install fastapi uvicorn psycopg2-binary requests beautifulsoup4
```

## 使い方

### 1. データのダウンロード

```bash
# 利用可能な市区町村一覧を表示 (全289市区町村)
python plateau_downloader.py --list

# 市区町村名で検索
python plateau_downloader.py --cityname 米子

# 市区町村コード指定でダウンロード
python plateau_downloader.py --citycode 31202

# 出力先を指定
python plateau_downloader.py --citycode 13101 --output-dir ./chiyoda_data

# 全市区町村を一括ダウンロード (市区町村間30秒インターバル)
python plateau_downloader.py --all

# インターバルを60秒に変更
python plateau_downloader.py --all --city-interval 60
```

データは `./plateau_data/{citycode}/` に保存されます。

データソース: http://surveyor.mydns.jp/task-bldg/

### 2. PostGIS へのインポート

```bash
# data-dir のディレクトリ名から市区町村コードを自動推定
python plateau_importer2postgis.py --data-dir ./plateau_data/31202

# 市区町村コードを明示指定
python plateau_importer2postgis.py --data-dir ./plateau_data/31202 --citycode 31202

# 座標範囲チェックを指定 (min_lat,max_lat,min_lon,max_lon)
python plateau_importer2postgis.py --data-dir ./plateau_data/31202 --coord-bounds "35.2,35.6,133.0,133.5"

# PostgreSQL 接続先を指定
python plateau_importer2postgis.py --data-dir ./plateau_data/31202 \
  --postgres-url "postgresql://user:pass@localhost:5432/dbname"
```

デフォルトの接続先: `postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau`

### 3. API サーバーの起動

```bash
python osmfj_plateau_api.py
```

http://localhost:8000 で起動します。

### API エンドポイント

| エンドポイント | 説明 |
|---------------|------|
| `GET /` | API 情報 |
| `GET /health` | ヘルスチェック |
| `GET /api/stats` | データベース統計情報 |
| `GET /api/mapwithai/buildings?bbox=min_lon,min_lat,max_lon,max_lat` | 建物データ取得 (OSM XML) |
| `GET /api/mapwithai/coverage` | Plateau対応エリアの GeoJSON FeatureCollection（都市単位の凹包） |

#### 建物データ取得のパラメータ

| パラメータ | 型 | デフォルト | 説明 |
|-----------|-----|-----------|------|
| `bbox` | string | (必須) | `min_lon,min_lat,max_lon,max_lat` 形式 |
| `limit` | int | 1000 | 最大取得件数 |
| `use_intersects` | bool | true | true: ST_Intersects, false: ST_Contains(centroid) |

## データベーススキーマ

インポーターは以下の2テーブルを使用します。

### plateau_buildings

| カラム | 型 | 説明 |
|--------|-----|------|
| id | serial | 主キー |
| osm_id | bigint | OSM 互換 ID |
| building | text | 建物タイプ (`yes`, `apartments` 等) |
| height | float | 建物高さ (m) |
| ele | float | 標高 (m) |
| building_levels | int | 階数 |
| source_dataset | text | データソース識別子 (`plateau_{citycode}_{file}`) |
| plateau_id | text | Plateau 元データの way ID |
| name | text | 建物名称 |
| addr_full | text | 住所 (通り名+番地の結合) |
| addr_housenumber | text | 番地 |
| addr_street | text | 通り名 |
| start_date | text | 建設年 |
| building_material | text | 建材 |
| roof_material | text | 屋根材 |
| roof_shape | text | 屋根形状 |
| amenity | text | 施設種別 |
| shop | text | 店舗種別 |
| tourism | text | 観光施設 |
| leisure | text | レジャー施設 |
| landuse | text | 土地利用 |
| **city_code** | varchar(5) NOT NULL | 市区町村コード（indexed、都市単位クエリ用） |
| geom | geometry | ポリゴン (SRID 4326) |
| centroid | geometry | 重心点 (SRID 4326) |

Indexes:
- `idx_buildings_geom` (GiST on geom)
- `idx_buildings_centroid` (GiST on centroid)
- `idx_buildings_osm_id` (btree)
- `idx_buildings_city_code` (btree) — 都市単位のクエリで活用

### plateau_building_nodes

| カラム | 型 | 説明 |
|--------|-----|------|
| id | serial | 主キー |
| osm_id | bigint | ノード ID (負の値) |
| building_id | int | 建物 ID (外部キー → plateau_buildings.id) |
| sequence_id | int | ノード順序 |
| lat | float | 緯度 |
| lon | float | 経度 |
| geom | geometry | ポイント |

### plateau_coverage (Materialized View)

対応エリア表示用に都市単位の凹包を保持。

| カラム | 型 | 説明 |
|--------|-----|------|
| city_code | varchar(5) | 市区町村コード (PK) |
| geom | geometry | ConcaveHull (target_percent=0.5) |
| building_count | int | 都市内の建物数 |

データ更新後（インポート・パージ）に `REFRESH MATERIALIZED VIEW CONCURRENTLY` で再構築する。

### plateau_purge_history

パージ実行履歴の監査ログ。

| カラム | 型 | 説明 |
|--------|-----|------|
| id | serial | PK |
| city_code | varchar(5) | パージした都市 |
| buildings_deleted | int | 削除した建物数 |
| nodes_deleted | int | 削除したノード数 |
| executed_at | timestamp | 実行日時 |
| executed_by | text | 実行ユーザ |
| hostname | text | 実行サーバ |
| duration_seconds | real | 所要時間 |

## 運用ツール

メンテナンス用の CLI ツール群。すべて Dry Run モードがデフォルトで、変更は `--execute` 指定時のみ実行されます。

### スキーマ・マイグレーション (`plateau_migrate.py`)

`city_code` カラムを `plateau_buildings` に追加する一回限りのマイグレーション。
Phase 1〜4 のスキーマ変更 + 既存データへの city_code 設定 + インデックス作成 + NOT NULL 制約。

```bash
# Dry Run（読み取り専用、ディスク容量・抽出可否などをレポート）
python plateau_migrate.py

# 本番実行（確認プロンプトあり）
python plateau_migrate.py --execute

# 自動化向け（プロンプトスキップ）
python plateau_migrate.py --execute --yes
```

冪等性あり。途中で失敗しても再実行で安全に続行可能。
ロールバック: `ALTER TABLE plateau_buildings DROP COLUMN city_code;`

### カバレッジビュー管理 (`plateau_coverage.py`)

`plateau_coverage` マテリアライズドビューの管理。

```bash
# 初回のみ: ビュー作成 + 初期 REFRESH
python plateau_coverage.py --init

# データ更新後の REFRESH（CONCURRENTLY）
python plateau_coverage.py --refresh

# ビュー定義変更時の再構築（DROP + CREATE + REFRESH）
python plateau_coverage.py --reinit

# 状態確認
python plateau_coverage.py --status
```

### 都市データのパージ (`plateau_purge.py`)

OSM への取り込みが完了した都市のデータを安全に削除。

```bash
# 監査ログテーブル初期化（初回のみ）
python plateau_purge.py --init-audit-table

# Dry Run（削除対象を確認）
python plateau_purge.py --citycode 13112

# 本番実行（2段階確認プロンプト）
python plateau_purge.py --citycode 13112 --execute
```

安全装置:
- デフォルト Dry Run
- 2段階確認: city_code 再入力 + `DELETE` 文字列入力
- pg_advisory_lock で同時実行防止
- 監査ログテーブルに自動記録
- 削除後の VACUUM ANALYZE + coverage REFRESH を自動実行

## テスト

```bash
pip install -r requirements-dev.txt
pytest                                # 159テスト
pytest --cov=. --cov-report=term      # カバレッジ表示
```

詳細は `tests/README.md` 参照。

## RapiD エディタとの連携

RapiD エディタの Plateau データセット設定で、この API の URL を指定することで建物データが地図上に表示されます。API は OSM XML 形式でデータを返すため、RapiD の MapWithAI サービスと互換性があります。

配信される OSM タグ:

| タグ | 説明 |
|------|------|
| `building` | 建物タイプ |
| `height` | 建物高さ (m) |
| `ele` | 標高 (m) |
| `building:levels` | 階数 |
| `name` | 建物名称 |
| `addr:housenumber` | 番地 |
| `addr:street` | 通り名 |
| `start_date` | 建設年 |
| `building:material` | 建材 |
| `roof:material` | 屋根材 |
| `roof:shape` | 屋根形状 |
| `amenity` | 施設種別 |
| `shop` | 店舗種別 |
| `tourism` | 観光施設 |
| `leisure` | レジャー施設 |
| `landuse` | 土地利用 |

## ライセンス

データソースのライセンスは [PLATEAU](https://www.mlit.go.jp/plateau/) の利用規約に従います。
