"""
osmfj_plateau_api.py のエンドポイントテスト

FastAPI の TestClient を使い、各エンドポイントの HTTP 動作をテストする。
DB接続はモック化。
"""

import sys
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# osmfj_plateau_api はモジュール読み込み時に DB 接続を試みるため、
# テスト用のモック connect を先に仕込む必要がある。
@pytest.fixture(scope='module', autouse=True)
def patch_psycopg2_before_import():
    """モジュールロード前に psycopg2.connect をモック化"""
    # PostGIS_Version() 用のレスポンスを準備
    cursor = MagicMock()
    cursor.fetchone.return_value = ('3.4 USE_GEOS=1',)
    cursor.fetchall.return_value = [('plateau_buildings',), ('plateau_building_nodes',)]
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)

    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch('psycopg2.connect', return_value=conn):
        # 既にモジュールが読み込み済みなら再リロード
        if 'osmfj_plateau_api' in sys.modules:
            del sys.modules['osmfj_plateau_api']
        # サブモジュールも巻き込まれるので一掃
        for mod in list(sys.modules.keys()):
            if mod.startswith('osmfj_plateau_api'):
                del sys.modules[mod]
        yield


@pytest.fixture
def client(patch_psycopg2_before_import):
    """FastAPI TestClient"""
    import osmfj_plateau_api  # ここで初めて import
    return TestClient(osmfj_plateau_api.app)


# ----------------------------------------------------------------------
# 基本エンドポイント
# ----------------------------------------------------------------------

class TestBasicEndpoints:
    def test_root_returns_api_info(self, client):
        response = client.get('/')
        assert response.status_code == 200
        data = response.json()
        assert 'name' in data
        assert data['name'] == 'OSMFJ Plateau API'
        assert 'endpoints' in data

    def test_health_check(self, client):
        response = client.get('/health')
        assert response.status_code == 200
        assert response.json() == {'status': 'healthy'}


# ----------------------------------------------------------------------
# CORS preflight
# ----------------------------------------------------------------------

class TestCORSPreflight:
    def test_options_buildings_returns_cors_headers(self, client):
        response = client.options('/api/mapwithai/buildings')
        assert response.status_code == 200
        assert 'access-control-allow-origin' in {k.lower() for k in response.headers}
        assert 'access-control-allow-methods' in {k.lower() for k in response.headers}


# ----------------------------------------------------------------------
# /api/mapwithai/coverage エンドポイント
# ----------------------------------------------------------------------

class TestCoverageEndpoint:
    def test_returns_feature_collection_with_correct_headers(self, client):
        """正常時 200 + GeoJSON FeatureCollection + キャッシュヘッダー"""
        import osmfj_plateau_api

        # CoverageManager.get_coverage_geojson をモック
        fake_data = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'id': '13104',
                    'geometry': {'type': 'Polygon', 'coordinates': [[[139.7, 35.7]]]},
                    'properties': {'city_code': '13104', 'building_count': 100},
                },
            ],
        }

        with patch('plateau_coverage.CoverageManager.get_coverage_geojson',
                   return_value=fake_data):
            response = client.get('/api/mapwithai/coverage')

        assert response.status_code == 200
        assert response.headers['content-type'].startswith('application/json')
        assert 'cache-control' in response.headers
        assert 'max-age' in response.headers['cache-control']
        assert response.headers.get('access-control-allow-origin') == '*'

        data = response.json()
        assert data['type'] == 'FeatureCollection'
        assert len(data['features']) == 1
        assert data['features'][0]['properties']['city_code'] == '13104'

    def test_returns_503_when_view_not_initialized(self, client):
        """ビュー未作成時は 503 で初期化方法を案内"""
        import psycopg2

        # UndefinedTable エラーを発生させる
        with patch('plateau_coverage.CoverageManager.get_coverage_geojson',
                   side_effect=psycopg2.errors.UndefinedTable("table 'plateau_coverage' does not exist")):
            response = client.get('/api/mapwithai/coverage')

        assert response.status_code == 503
        data = response.json()
        assert 'plateau_coverage' in data['detail']
        assert 'plateau_coverage.py --init' in data['detail']

    def test_returns_500_on_generic_error(self, client):
        """予期しないエラーは 500"""
        with patch('plateau_coverage.CoverageManager.get_coverage_geojson',
                   side_effect=RuntimeError('something exploded')):
            response = client.get('/api/mapwithai/coverage')

        assert response.status_code == 500

    def test_empty_features_returned(self, client):
        """データが空でも 200 で空の FeatureCollection を返す"""
        fake_data = {'type': 'FeatureCollection', 'features': []}
        with patch('plateau_coverage.CoverageManager.get_coverage_geojson',
                   return_value=fake_data):
            response = client.get('/api/mapwithai/coverage')

        assert response.status_code == 200
        assert response.json() == fake_data
