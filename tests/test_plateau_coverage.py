"""
plateau_coverage.py のテスト
"""

from unittest.mock import MagicMock

import pytest

import plateau_coverage
from plateau_coverage import (
    CONCAVE_HULL_PERCENT,
    CoverageManager,
    MATERIALIZED_VIEW_DDL,
    UNIQUE_INDEX_DDL,
    GEOM_INDEX_DDL,
)


# ----------------------------------------------------------------------
# 定数 / SQL の妥当性
# ----------------------------------------------------------------------

class TestConstants:
    def test_concave_hull_percent_in_valid_range(self):
        """target_percent は 0..1 でなければならない"""
        assert 0.0 <= CONCAVE_HULL_PERCENT <= 1.0

    def test_view_ddl_uses_concave_hull(self):
        """ビュー定義に ST_ConcaveHull が含まれる（凸包から変更済み）"""
        assert 'ST_ConcaveHull' in MATERIALIZED_VIEW_DDL
        # フォールバックの ConvexHull も併用
        assert 'ST_ConvexHull' in MATERIALIZED_VIEW_DDL
        assert 'COALESCE' in MATERIALIZED_VIEW_DDL

    def test_view_ddl_uses_if_not_exists(self):
        """CREATE は IF NOT EXISTS で冪等"""
        assert 'CREATE MATERIALIZED VIEW IF NOT EXISTS' in MATERIALIZED_VIEW_DDL

    def test_unique_index_for_concurrent_refresh(self):
        """CONCURRENTLY REFRESH に必要な UNIQUE INDEX 定義がある"""
        assert 'UNIQUE INDEX' in UNIQUE_INDEX_DDL
        assert 'city_code' in UNIQUE_INDEX_DDL

    def test_gist_index_for_spatial_query(self):
        """空間検索用 GIST インデックス定義がある"""
        assert 'USING GIST' in GEOM_INDEX_DDL


# ----------------------------------------------------------------------
# CoverageManager のメソッド単体
# ----------------------------------------------------------------------

class TestCoverageManager:
    def test_uses_database_url_env_var(self, monkeypatch):
        """DATABASE_URL 環境変数を読む"""
        monkeypatch.setenv('DATABASE_URL', 'postgresql://test:test@localhost/test')
        mgr = CoverageManager()
        assert mgr.postgres_url == 'postgresql://test:test@localhost/test'

    def test_explicit_url_overrides_env(self, monkeypatch):
        """明示的に渡されたURLは環境変数より優先"""
        monkeypatch.setenv('DATABASE_URL', 'postgresql://env')
        mgr = CoverageManager('postgresql://explicit')
        assert mgr.postgres_url == 'postgresql://explicit'

    def test_view_exists_true(self, monkeypatch, mock_connection):
        """ビューが存在する場合 True"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (True,)
        mgr = CoverageManager('postgresql://x')
        assert mgr.view_exists(mock_connection) is True

    def test_view_exists_false(self, monkeypatch, mock_connection):
        """ビューが存在しない場合 False"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (False,)
        mgr = CoverageManager('postgresql://x')
        assert mgr.view_exists(mock_connection) is False

    def test_view_has_data_true(self, mock_connection):
        """ispopulated=True ならデータあり"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (True,)
        mgr = CoverageManager('postgresql://x')
        assert mgr.view_has_data(mock_connection) is True

    def test_view_has_data_when_view_missing(self, mock_connection):
        """ビューがなければ False（fetchone が None を返す）"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = None
        mgr = CoverageManager('postgresql://x')
        assert mgr.view_has_data(mock_connection) is False

    def test_init_view_creates_view_and_indexes(self, monkeypatch, mock_connection):
        """init_view() は3つのDDLを順に実行する"""
        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        mgr = CoverageManager('postgresql://x')
        mgr.init_view()

        cursor = mock_connection.cursor.return_value
        # 3回 execute されたはず（view + unique idx + gist idx）
        assert cursor.execute.call_count == 3
        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        assert any('CREATE MATERIALIZED VIEW' in sql for sql in executed_sqls)
        assert any('UNIQUE INDEX' in sql for sql in executed_sqls)
        assert any('USING GIST' in sql for sql in executed_sqls)
        # トランザクション commit
        mock_connection.commit.assert_called_once()

    def test_drop_view(self, monkeypatch, mock_connection):
        """drop_view() は DROP MATERIALIZED VIEW IF EXISTS を実行"""
        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        mgr = CoverageManager('postgresql://x')
        mgr.drop_view()

        cursor = mock_connection.cursor.return_value
        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        assert any('DROP MATERIALIZED VIEW IF EXISTS' in sql for sql in executed_sqls)

    def test_refresh_uses_concurrently_when_populated(self, monkeypatch, mock_connection):
        """ビューが populated なら CONCURRENTLY を使う"""
        # 2回呼ばれる: view_exists, view_has_data (両方 True)
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.side_effect = [(True,), (True,)]

        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        mgr = CoverageManager('postgresql://x')
        mgr.refresh(concurrent=True)

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        # 最後のexecuteが REFRESH ... CONCURRENTLY
        assert any('REFRESH MATERIALIZED VIEW CONCURRENTLY' in sql for sql in executed_sqls)

    def test_refresh_skips_concurrently_when_not_populated(self, monkeypatch, mock_connection):
        """初回（populated=False）は CONCURRENTLY なしで通常 REFRESH"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.side_effect = [(True,), (False,)]

        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        mgr = CoverageManager('postgresql://x')
        mgr.refresh(concurrent=True)

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        # 通常 REFRESH のみ、CONCURRENTLY は含まれない
        refresh_calls = [s for s in executed_sqls if 'REFRESH' in s]
        assert len(refresh_calls) == 1
        assert 'CONCURRENTLY' not in refresh_calls[0]

    def test_refresh_returns_early_when_view_missing(self, monkeypatch, mock_connection):
        """ビュー未作成なら error を返して REFRESH しない"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (False,)  # view_exists -> False

        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        mgr = CoverageManager('postgresql://x')
        result = mgr.refresh()

        assert result == {'error': 'view_not_found'}
        # REFRESH は呼ばれないはず
        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        assert not any('REFRESH' in s for s in executed_sqls)


# ----------------------------------------------------------------------
# get_coverage_geojson の戻り値構造
# ----------------------------------------------------------------------

class TestGetCoverageGeojson:
    def test_returns_feature_collection_structure(self, monkeypatch, mock_connection):
        """正しい GeoJSON FeatureCollection を返す"""
        # RealDictCursor が dict-like な行を返すのを再現
        rows = [
            {
                'city_code': '13104',
                'geom': {'type': 'Polygon', 'coordinates': [[[139.7, 35.7]]]},
                'building_count': 302156,
            },
            {
                'city_code': '13112',
                'geom': {'type': 'Polygon', 'coordinates': [[[139.6, 35.6]]]},
                'building_count': 262658,
            },
        ]

        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=None)
        cursor.fetchall.return_value = rows
        mock_connection.cursor.return_value = cursor

        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )

        mgr = CoverageManager('postgresql://x')
        result = mgr.get_coverage_geojson()

        assert result['type'] == 'FeatureCollection'
        assert len(result['features']) == 2

        f0 = result['features'][0]
        assert f0['type'] == 'Feature'
        assert f0['id'] == '13104'
        assert f0['geometry']['type'] == 'Polygon'
        assert f0['properties']['city_code'] == '13104'
        assert f0['properties']['building_count'] == 302156

    def test_returns_empty_features_when_no_data(self, monkeypatch, mock_connection):
        """データなしでも構造は維持"""
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=None)
        cursor.fetchall.return_value = []
        mock_connection.cursor.return_value = cursor

        monkeypatch.setattr(
            'plateau_coverage.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )

        mgr = CoverageManager('postgresql://x')
        result = mgr.get_coverage_geojson()
        assert result == {'type': 'FeatureCollection', 'features': []}
