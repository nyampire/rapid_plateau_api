# アーキテクチャ

このドキュメントは、本リポジトリの全体構造と Phase 1〜3 + Coverage API までの実装経緯をまとめたものです。

## システム全体図

```
                            ┌────────────────────────┐
                            │   surveyor.mydns.jp    │ (外部、Plateau配信元)
                            │   Plateau OSMデータ     │
                            └───────────┬────────────┘
                                        │ HTTPS
                                        │
                            ┌───────────▼────────────┐
                            │  plateau_downloader.py │ メッシュ単位ダウンロード
                            └───────────┬────────────┘
                                        │ ZIP→OSM展開
                                        │
                       ┌────────────────▼──────────────────┐
                       │  plateau_importer2postgis.py      │ パース・タグ変換
                       └────────────────┬──────────────────┘
                                        │ INSERT
                                        ▼
            ┌───────────────────────────────────────────────────┐
            │                  PostgreSQL/PostGIS                │
            │  ┌──────────────────┐  ┌─────────────────────┐    │
            │  │ plateau_buildings│  │ plateau_building_   │    │
            │  │  (city_code idx) │◄─┤ nodes (FK)          │    │
            │  └────────┬─────────┘  └─────────────────────┘    │
            │           │ 集計                                  │
            │           ▼                                       │
            │  ┌─────────────────────┐                          │
            │  │ plateau_coverage    │ MatView (ConcaveHull)    │
            │  │ (city_code, geom)   │                          │
            │  └─────────────────────┘                          │
            │  ┌─────────────────────┐                          │
            │  │plateau_purge_history│ 監査ログ                  │
            │  └─────────────────────┘                          │
            └────────────────┬──────────────────────────────────┘
                             │ SELECT
                             ▼
                    ┌────────────────────┐
                    │osmfj_plateau_api.py│ FastAPI
                    │  /buildings        │ → OSM XML
                    │  /coverage         │ → GeoJSON
                    └─────────┬──────────┘
                              │ HTTPS
                              ▼
                    ┌────────────────────┐
                    │  Rapid Editor      │
                    │  (nyampire/Rapid)  │
                    └────────────────────┘
```

## モジュール一覧

| ファイル | 役割 | テスト |
|---|---|---|
| `plateau_downloader.py` | surveyor.mydns.jp からダウンロード | ❌ |
| `plateau_importer2postgis.py` | OSM データを PostGIS に投入 | ❌ |
| `osmfj_plateau_api.py` | FastAPI で配信 | ✅ test_osmfj_plateau_api.py + test_buildings_xml.py |
| `plateau_migrate.py` | スキーマ変更マイグレーション | ✅ test_plateau_migrate.py |
| `plateau_purge.py` | 都市単位パージツール | ✅ test_plateau_purge.py |
| `plateau_coverage.py` | カバレッジビュー管理 | ✅ test_plateau_coverage.py |
| `batch_import_2024.py` | 一括インポート（CITIES_2024 定義） | ❌ |

## データフロー: 新都市追加

```
1. plateau_downloader.py --citycode XXXXX
   └─→ plateau_data/XXXXX/ にZIPダウンロード→OSM展開
2. plateau_importer2postgis.py --data-dir plateau_data/XXXXX
   └─→ plateau_buildings に INSERT（city_code 自動設定）
   └─→ plateau_building_nodes に INSERT（FK）
3. plateau_coverage.py --refresh
   └─→ plateau_coverage マテリアライズドビューを REFRESH CONCURRENTLY
4. rm -rf plateau_data/XXXXX  （容量節約）
```

## データフロー: 都市データ削除（OSM完了時）

```
1. plateau_purge.py --citycode XXXXX
   └─→ Dry Run で対象件数表示
2. plateau_purge.py --citycode XXXXX --execute
   └─→ pg_advisory_lock 取得
   └─→ plateau_building_nodes 削除（FK）
   └─→ plateau_buildings 削除（city_code = X）
   └─→ plateau_purge_history に監査ログ INSERT
   └─→ ロック解放
   └─→ VACUUM ANALYZE
   └─→ plateau_coverage REFRESH（自動）
```

## 設計判断と経緯

### city_code カラム追加 (Phase 1, PR #7)

**背景**: 都市単位の集計・パージで `source_dataset LIKE '%XXXXX%'` を使っており、Seq Scan で 42秒かかっていた。

**対応**:
1. `city_code VARCHAR(5)` カラム追加
2. 既存12.7M行に値を埋める（バッチUPDATE + VACUUM）
3. `idx_buildings_city_code` (btree) 作成
4. NOT NULL 制約付与

**結果**: 都市単位クエリが 42秒 → 22ms（約1900倍高速化）。

### インポーター修正 (Phase 2, PR #8)

新規インポート時に city_code が自動設定されるよう `plateau_importer2postgis.py` を改修。
既存の `source_dataset LIKE` クエリも `city_code = %s` に置換。

### パージツール (Phase 3, PR #9)

OSMへの取り込みが完了した都市のデータを安全に削除する CLI。
2段階確認プロンプト + 監査ログ + pg_advisory_lock。

### Coverage API & マテリアライズドビュー (PR #10)

Rapid editor の zoom 5-14 で対応エリアを表示するため、
都市単位の `ST_ConcaveHull(ST_Collect(centroid), 0.5)` を MatView として保持。

- 初回 REFRESH: 約66s (ConvexHull) → 154s (ConcaveHull)
- 通常 REFRESH (CONCURRENTLY): 約140s
- API レスポンス: 約500ms / 177KB / 141都市
- 頂点数（中央値）: 33（横浜57が最大）

### テスト整備 (PR #12, #13)

- フレームワーク: pytest + DBモック（実DB不要）
- 計 159 テスト、実行 0.4 秒
- 副産物: `CITY_CODE_PATTERN` の全角数字バグ発見 → `re.ASCII` で修正

## 既存の制約・既知の問題

| 問題 | 回避策 |
|---|---|
| `plateau_importer2postgis.py` は coverage REFRESH を自動実行しない | インポート後に手動 `plateau_coverage.py --refresh` |
| `osmfj_plateau_api.py` の `api = OSMFJPlateauAPI()` グローバルがテストを煩雑にする | 将来的に DI 化を検討 |
| `plateau_importer2postgis.py` / `plateau_downloader.py` にテストなし | 別途追加検討 |

## 関連 Issue / PR

過去の主要な実装:
- #2 (closed) - 完了済み地域DBパージ機能（親）
  - #3 Phase 1: city_code カラム追加
  - #4 Phase 2: インポーター修正
  - #5 Phase 3: パージツール
- #10 - Coverage API & マテリアライズドビュー
- #11 - テストフレームワーク導入
- #12 (PR) - pytest テストスイート初期導入
- #13 (PR) - テスト強化 +84テスト + バグ修正
