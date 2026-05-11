"""
pytest 共通フィクスチャ

DB接続をモック化するヘルパーと、統合テスト用のフィクスチャを提供。
"""

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
