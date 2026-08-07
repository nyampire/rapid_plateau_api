"""エンドポイント経由の通し確認 (#39 の除外)。

クエリと XML 生成はそれぞれ検証済みだが、つないだ状態を一度も通していな
かった。ここでは FastAPI のルートを実際に叩き、実 DB から取得して XML を
組み立てるまでを 1 本で確認する。

取り込み側の way 番号衝突により、部分立体が数 km 離れた外形に紐づいている
行が本番に 16,175 件ある。API は部分立体を「親が範囲内にあるとき」に出力
し、部分立体自身の位置を見ないため、小さな範囲の要求に遠方のジオメトリが
返る。それが起きないことを確認する。
"""
import xml.etree.ElementTree as ET

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def integration_client(integration_db_url, monkeypatch):
    monkeypatch.setenv('DATABASE_URL', integration_db_url)
    import importlib
    import osmfj_plateau_api
    importlib.reload(osmfj_plateau_api)
    return TestClient(osmfj_plateau_api.app)


@pytest.mark.integration
class TestFarPartIsNotServed:
    def _seed(self, conn):
        cur = conn.cursor()
        # 外形 (bbox 内)
        cur.execute("""
            INSERT INTO plateau_buildings (id, osm_id, building, city_code, geom, centroid)
            VALUES (1, -1, 'yes', '39201',
              ST_GeomFromText('POLYGON((133.0 33.0,133.001 33.0,133.001 33.001,133.0 33.001,133.0 33.0))', 4326),
              ST_PointOnSurface(ST_GeomFromText('POLYGON((133.0 33.0,133.001 33.0,133.001 33.001,133.0 33.001,133.0 33.0))', 4326)))
        """)
        # 正しい部分立体 (外形と重なる)
        cur.execute("""
            INSERT INTO plateau_buildings
              (id, osm_id, building_part, parent_building_id, city_code, geom, centroid)
            VALUES (2, -2, 'yes', 1, '39201',
              ST_GeomFromText('POLYGON((133.0002 33.0002,133.0006 33.0002,133.0006 33.0006,133.0002 33.0006,133.0002 33.0002))', 4326),
              ST_PointOnSurface(ST_GeomFromText('POLYGON((133.0002 33.0002,133.0006 33.0002,133.0006 33.0006,133.0002 33.0006,133.0002 33.0002))', 4326)))
        """)
        # 取り違えられた部分立体 (約 150km 離れている)
        cur.execute("""
            INSERT INTO plateau_buildings
              (id, osm_id, building_part, parent_building_id, city_code, geom, centroid)
            VALUES (3, -3, 'yes', 1, '39201',
              ST_GeomFromText('POLYGON((134.5 34.5,134.501 34.5,134.501 34.501,134.5 34.501,134.5 34.5))', 4326),
              ST_PointOnSurface(ST_GeomFromText('POLYGON((134.5 34.5,134.501 34.5,134.501 34.501,134.5 34.501,134.5 34.5))', 4326)))
        """)
        # ノード (way の描画に要る)
        rings = {
            1: [(33.0, 133.0), (33.0, 133.001), (33.001, 133.001), (33.001, 133.0)],
            2: [(33.0002, 133.0002), (33.0002, 133.0006), (33.0006, 133.0006), (33.0006, 133.0002)],
            3: [(34.5, 134.5), (34.5, 134.501), (34.501, 134.501), (34.501, 134.5)],
        }
        node_id = -100
        for building_id, ring in rings.items():
            for seq, (lat, lon) in enumerate(ring):
                cur.execute(
                    "INSERT INTO plateau_building_nodes"
                    " (osm_id, building_id, sequence_id, lat, lon)"
                    " VALUES (%s, %s, %s, %s, %s)",
                    (node_id, building_id, seq, lat, lon),
                )
                node_id -= 1
        conn.commit()

    def test_far_part_absent_and_near_part_present(
        self, fresh_plateau_full_schema, integration_client
    ):
        self._seed(fresh_plateau_full_schema)

        res = integration_client.get(
            '/api/mapwithai/buildings',
            params={'bbox': '132.999,32.999,133.002,33.002'},
        )
        assert res.status_code == 200

        root = ET.fromstring(res.text)
        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(1 * 1000)) in way_ids, '外形が返っていない'
        assert str(-(2 * 1000)) in way_ids, '正しい部分立体が落ちている'
        assert '-3' not in way_ids, '取り違えられた部分立体が返っている'

        # 応答が要求範囲を大きくはみ出していないこと
        lats = [float(n.get('lat')) for n in root.findall('node')]
        lons = [float(n.get('lon')) for n in root.findall('node')]
        assert max(lats) < 33.01, f'遠方の緯度が混入: {max(lats)}'
        assert max(lons) < 133.01, f'遠方の経度が混入: {max(lons)}'

    def test_relation_does_not_list_the_far_part(
        self, fresh_plateau_full_schema, integration_client
    ):
        self._seed(fresh_plateau_full_schema)

        res = integration_client.get(
            '/api/mapwithai/buildings',
            params={'bbox': '132.999,32.999,133.002,33.002'},
        )
        root = ET.fromstring(res.text)

        rel = root.find('relation')
        assert rel is not None, 'relation が作られていない'
        refs = {m.get('ref') for m in rel.findall('member')}
        assert str(-(2 * 1000)) in refs
        assert '-3' not in refs, 'relation に遠方の部分立体が残っている'
