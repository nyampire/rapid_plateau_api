# テスト

このディレクトリには `pytest` を用いたユニットテストが含まれています。
通常テストは `unittest.mock` で DB 接続をモック化しているため実 DB 不要、
`@pytest.mark.integration` 付きの統合テストだけが実 PostgreSQL を必要とします。

## セットアップ

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
```

## テスト実行

```bash
# 全テスト実行 (mock-only、実 DB 不要)
pytest

# 個別ファイル
pytest tests/test_plateau_coverage.py

# 詳細ログ付き
pytest -v

# カバレッジレポート付き
pytest --cov=. --cov-report=term-missing
```

## 統合テスト (実 PostgreSQL が必要)

`@pytest.mark.integration` 付きのテストは、本物の DDL/DML 挙動 (FK CASCADE 等)
を実 DB で検証します。`--run-integration` 未指定時は自動的に skip されるので、
mock-only の CI ではコケません。

### ローカル DB 準備

```bash
createdb plateau_api_test
# 統合テストの一部 (例: 将来のジオメトリ系) は PostGIS が必要になるかもしれないので
# 入れておくと安全。今ある FK migration テストは PostGIS 無しでも動く。
psql -d plateau_api_test -c "CREATE EXTENSION IF NOT EXISTS postgis"
```

### 実行

```bash
PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest --run-integration
```

`PLATEAU_TEST_DATABASE_URL` 未設定だと `integration_db_url` フィクスチャが skip
されるので、`--run-integration` を付けても安全に no-op になります。

## テストファイル一覧

ファイル数・テスト数は実体に合わせて随時更新。基本は `pytest --collect-only`
で正確な現状を取れます。整数を覚えるよりも `pytest -v` で見るのが早い。

## カバレッジ範囲

DBやネットワークI/Oが絡まないロジック部分を中心にカバーしています:

- 定数・SQL DDL の妥当性
- メソッド単体の引数→戻り値マッピング
- エッジケース（NULL、空テーブル、不正入力など）
- 確認プロンプトの入力検証ロジック
- APIエンドポイントのレスポンス構造とエラーハンドリング

## 未テスト箇所

以下は本テストスイートのスコープ外（別途追加検討）:

- `plateau_importer2postgis.py` のインポート処理（複雑、実データ必要）
- `plateau_downloader.py` のダウンロード処理（外部HTTP通信）
- 大規模データでのパフォーマンステスト

## 共通フィクスチャ

`conftest.py` で以下のフィクスチャを提供:

- `mock_connection`: psycopg2 connection のモック
- `patch_psycopg2_connect`: モジュール内の `psycopg2.connect` 差し替え
- `fake_cursor_result`: cursor.fetchone/fetchall の戻り値を簡単に設定
- `integration_db_url`: `PLATEAU_TEST_DATABASE_URL` 環境変数を読む。未設定なら skip
- `fresh_plateau_schema`: `plateau_buildings` と `plateau_building_nodes` を
  pre-migration 状態 (nodes FK = NO ACTION) で毎テスト DROP+CREATE して返す

## CI

未設定。導入時は `requirements-dev.txt` をインストール後 `pytest` を実行。
