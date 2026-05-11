# テスト

このディレクトリには `pytest` を用いたユニットテストが含まれています。
DB 接続は `unittest.mock` でモック化しているため、実DB は不要です。

## セットアップ

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-dev.txt
```

## テスト実行

```bash
# 全テスト実行
pytest

# 個別ファイル
pytest tests/test_plateau_coverage.py

# 詳細ログ付き
pytest -v

# カバレッジレポート付き
pytest --cov=. --cov-report=term-missing

# 統合テスト（実DBが必要、現状未実装）
pytest --run-integration
```

## テストファイル一覧

| ファイル | 対象 | テスト数 |
|---|---|---|
| `test_plateau_coverage.py` | `plateau_coverage.py` | 18 |
| `test_plateau_purge.py` | `plateau_purge.py` | 20 |
| `test_plateau_migrate.py` | `plateau_migrate.py` | 30 |
| `test_osmfj_plateau_api.py` | `osmfj_plateau_api.py` (FastAPI) | 7 |
| **合計** | | **75** |

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
- 実DB に対する統合テスト
- 大規模データでのパフォーマンステスト

## 共通フィクスチャ

`conftest.py` で以下のフィクスチャを提供:

- `mock_connection`: psycopg2 connection のモック
- `patch_psycopg2_connect`: モジュール内の `psycopg2.connect` 差し替え
- `fake_cursor_result`: cursor.fetchone/fetchall の戻り値を簡単に設定

## CI

未設定。導入時は `requirements-dev.txt` をインストール後 `pytest` を実行。
