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
| geom | geometry | ポリゴン (SRID 4326) |
| centroid | geometry | 重心点 (SRID 4326) |

### plateau_building_nodes

| カラム | 型 | 説明 |
|--------|-----|------|
| id | serial | 主キー |
| osm_id | bigint | ノード ID (負の値) |
| building_id | int | 建物 ID (外部キー) |
| sequence_id | int | ノード順序 |
| lat | float | 緯度 |
| lon | float | 経度 |
| geom | geometry | ポイント |

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
