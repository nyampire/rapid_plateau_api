"""`get_buildings_in_bbox` が ring_id → sequence_id の順でノードを返すこと。

importer は環ごとに sequence_id を 0 から振り直すため、SQL 側で
`ORDER BY n.ring_id, n.sequence_id` を明示しないと、中庭のある建物の
外環と内環のノードが不定の順序で混ざる。これは単体テスト
(`tests/test_buildings_xml.py`) では検出できない: そちらは Python の
list を直接 `buildings_to_osm_xml` に渡すので、SQL の ORDER BY 自体は
一度も通らない。

本テストは実 DB に対して `plateau_building_nodes` を内環 (ring_id=1) が
先、外環 (ring_id=0) が後という、挿入順序が要求される出力順序とわざと
食い違う状態で作り、実際のクエリメソッド経由で正しく並び直ることを
確認する。

`PLATEAU_TEST_DATABASE_URL` を設定した上で `pytest --run-integration`
で実行する。
"""
import pytest


def _square_wkt(lat: float, lon: float, size_deg: float = 0.0001) -> str:
    """Tiny square polygon WKT centered on (lat, lon)."""
    return (
        f"POLYGON(("
        f"{lon - size_deg} {lat - size_deg},"
        f"{lon + size_deg} {lat - size_deg},"
        f"{lon + size_deg} {lat + size_deg},"
        f"{lon - size_deg} {lat + size_deg},"
        f"{lon - size_deg} {lat - size_deg}"
        f"))"
    )


def _seed_building(conn, *, osm_id, lat, lon, city_code='13206'):
    """Insert one plateau_buildings row (courtyard outline), return its DB id."""
    wkt = _square_wkt(lat, lon)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plateau_buildings
                (osm_id, building, city_code, geom, centroid)
            VALUES (%s, 'yes', %s, ST_GeomFromText(%s, 4326),
                    ST_Centroid(ST_GeomFromText(%s, 4326)))
            RETURNING id
            """,
            (osm_id, city_code, wkt, wkt),
        )
        return cur.fetchone()[0]


def _insert_node(conn, *, building_id, osm_id, ring_id, sequence_id,
                  lat, lon):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plateau_building_nodes
                (osm_id, building_id, ring_id, sequence_id, lat, lon)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (osm_id, building_id, ring_id, sequence_id, lat, lon),
        )


@pytest.mark.integration
def test_nodes_are_grouped_by_ring_then_ordered_by_sequence(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    building_id = _seed_building(conn, osm_id=1, lat=lat, lon=lon)

    # 挿入順序は意図的に ring 1 (内環) → ring 0 (外環) にして、DB の物理
    # 挿入順序と要求される出力順序 (ring_id, sequence_id) を食い違わせる。
    # 各 ring 内も sequence_id が降順で入る行を混ぜ、sequence_id 単独の
    # ORDER BY (旧実装) では通らないことも同時に確認する。
    for seq, offset in [(2, 0.00003), (0, 0.00001), (1, 0.00002)]:
        _insert_node(
            conn, building_id=building_id, osm_id=-(100 + seq),
            ring_id=1, sequence_id=seq,
            lat=lat + offset, lon=lon + offset,
        )
    for seq, offset in [(3, 0.0004), (1, 0.0002), (2, 0.0003), (0, 0.0001)]:
        _insert_node(
            conn, building_id=building_id, osm_id=-(200 + seq),
            ring_id=0, sequence_id=seq,
            lat=lat + offset, lon=lon + offset,
        )

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.01, lat - 0.01, lon + 0.01, lat + 0.01, limit=100,
    )

    assert len(results) == 1
    nodes = results[0]['nodes']
    assert nodes is not None

    ring_ids = [n['ring_id'] for n in nodes]
    # 外環 (0) が内環 (1) より前に、かつ一度出た ring_id には戻らない
    # (=ring でグループ化されている)。
    assert ring_ids == sorted(ring_ids)
    assert ring_ids[0] == 0

    # ring ごとに見て sequence_id が昇順であること。
    seen_rings = {}
    for n in nodes:
        seen_rings.setdefault(n['ring_id'], []).append(n['sequence_id'])
    for ring_id, seqs in seen_rings.items():
        assert seqs == sorted(seqs), (
            f"ring {ring_id} の sequence_id が昇順でない: {seqs}"
        )

    # 具体的な期待順序も確認しておく (0,0),(0,1),(0,2),(0,3),(1,0),(1,1),(1,2)
    assert [(n['ring_id'], n['sequence_id']) for n in nodes] == [
        (0, 0), (0, 1), (0, 2), (0, 3),
        (1, 0), (1, 1), (1, 2),
    ]
