"""
pytest 共通フィクスチャ

DB接続をモック化するヘルパーと、統合テスト用のフィクスチャを提供。
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# モジュールがimport可能なようにパスを通す
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def pytest_addoption(parser):
    parser.addoption(
        '--run-integration',
        action='store_true',
        default=False,
        help='Run integration tests (requires real PostgreSQL/PostGIS)',
    )


def pytest_collection_modifyitems(config, items):
    """`--run-integration` 未指定時は integration マーカー付きテストをスキップ"""
    if config.getoption('--run-integration'):
        return
    skip_integration = pytest.mark.skip(reason='use --run-integration to run')
    for item in items:
        if 'integration' in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture(scope='session')
def integration_db_url():
    """統合テスト用 PostgreSQL の接続 URL。

    `PLATEAU_TEST_DATABASE_URL` が未設定なら統合テストを skip する。
    例: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest --run-integration`
    """
    url = os.environ.get('PLATEAU_TEST_DATABASE_URL')
    if not url:
        pytest.skip('PLATEAU_TEST_DATABASE_URL not set; skipping DB integration tests')
    return url


@pytest.fixture
def fresh_plateau_schema(integration_db_url):
    """`plateau_buildings` と `plateau_building_nodes` を初期状態 (nodes FK = NO ACTION)
    で作り直し、autocommit な接続を返す。

    各テストが migration の前提状態から始められるよう、毎回 DROP + CREATE する。
    PostGIS 拡張は使わない最小スキーマ (id / building_part / parent_building_id /
    building_id のみ) なので、PostGIS 無しの素の DB でも動く。
    """
    import psycopg2
    conn = psycopg2.connect(integration_db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS plateau_building_nodes CASCADE')
        cur.execute('DROP TABLE IF EXISTS plateau_buildings CASCADE')
        # parent_building_id は importer の ALTER 経由の挙動を踏襲して
        # ここで ON DELETE CASCADE を直接付ける (本番と同じ pre-migration 状態)。
        cur.execute('''
            CREATE TABLE plateau_buildings (
                id SERIAL PRIMARY KEY,
                building_part TEXT,
                parent_building_id INTEGER
                    REFERENCES plateau_buildings(id) ON DELETE CASCADE
            )
        ''')
        # 子側 FK の名前は PG デフォルト規則で
        # `plateau_building_nodes_building_id_fkey` となり、migration スクリプトが
        # 参照する定数 (CONSTRAINT_NAME) と一致する。
        cur.execute('''
            CREATE TABLE plateau_building_nodes (
                id SERIAL PRIMARY KEY,
                building_id INTEGER REFERENCES plateau_buildings(id)
            )
        ''')
    yield conn
    conn.close()


@pytest.fixture
def mock_connection():
    """
    psycopg2.connect をモック化したコネクション。
    .cursor() は context manager をサポートする MagicMock を返す。
    """
    conn = MagicMock(name='Connection')

    # cursor() を context manager として使えるようにする
    cursor = MagicMock(name='Cursor')
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)
    conn.cursor.return_value = cursor

    return conn


@pytest.fixture
def patch_psycopg2_connect(monkeypatch, mock_connection):
    """
    psycopg2.connect をモジュール内で差し替え、毎回 mock_connection を返すようにする。
    各テストモジュールが個別に呼び出す。
    """
    def _patch(module_name):
        monkeypatch.setattr(
            f'{module_name}.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        return mock_connection

    return _patch


@pytest.fixture
def fake_cursor_result(mock_connection):
    """
    cursor.fetchone() / fetchall() の戻り値を簡単に設定するヘルパー。

    使い方:
        def test_x(fake_cursor_result):
            cur = fake_cursor_result(fetchone=(42,))
            ...
    """
    def _setup(fetchone=None, fetchall=None):
        cursor = mock_connection.cursor.return_value
        if fetchone is not None:
            cursor.fetchone.return_value = fetchone
        if fetchall is not None:
            cursor.fetchall.return_value = fetchall
        return cursor

    return _setup


@pytest.fixture
def bare_importer(monkeypatch, tmp_path):
    """`PlateauImporter2PostGIS` を DB 接続なしで生成する factory fixture。

    `_test_connection` / `_initialize_id_counters` / `_ensure_schema` の 3 つを
    monkeypatch で no-op 化し、citycode 別の `tmp_path/<citycode>` を `data_dir`
    として idempotent に用意した importer を返す。返り値は呼び出し可能な builder で、
    citycode を任意に切り替えられる。

    使い方::

        def test_x(bare_importer):
            importer = bare_importer()                  # citycode='99999'
            importer = bare_importer(citycode='13203')  # 任意の citycode
            importer = bare_importer(citycode=None)     # None ハンドリングの検証
    """
    from plateau_importer2postgis import PlateauImporter2PostGIS

    monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
    monkeypatch.setattr(PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None)
    monkeypatch.setattr(PlateauImporter2PostGIS, '_ensure_schema', lambda self: None)

    def _build(citycode='99999'):
        data_dir = tmp_path / (citycode or 'unknown')
        data_dir.mkdir(parents=True, exist_ok=True)
        return PlateauImporter2PostGIS(
            data_dir=str(data_dir),
            postgres_url='fake',
            citycode=citycode,
        )

    return _build
