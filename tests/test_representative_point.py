"""Integration tests for the `representative_point` tag on the
``/api/mapwithai/buildings`` XML response (PLATEAU height-transfer Task 2).

These tests require a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. They are skipped by default; run with
``pytest --run-integration``.
"""
import sys

import pytest
import xml.etree.ElementTree as ET
from fastapi.testclient import TestClient

pytestmark = pytest.mark.integration


# ----------------------------------------------------------------------
# Seed helpers (mirror tests/test_dedup_city_duplicates.py's
# `_square_wkt` / `_seed_building` pattern, extended with node rows so the
# building actually survives `buildings_to_osm_xml`'s >=3-valid-node check
# and comes back as a `<way>` in the HTTP response).
# ----------------------------------------------------------------------

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


def _insert_building(conn, *, osm_id, city_code, wkt, points, height=None,
                      building_levels=None):
    """Insert one plateau_buildings row + its plateau_building_nodes ring.

    ``points`` is an ordered list of (lat, lon) tuples for the *open* ring
    (no repeated closing point — `buildings_to_osm_xml` closes it itself).
    Returns the new building's DB id.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plateau_buildings
                (osm_id, building, height, building_levels, city_code, geom, centroid)
            VALUES (%s, 'yes', %s, %s, %s,
                    ST_GeomFromText(%s, 4326),
                    ST_Centroid(ST_GeomFromText(%s, 4326)))
            RETURNING id
            """,
            (osm_id, height, building_levels, city_code, wkt, wkt),
        )
        building_id = cur.fetchone()[0]

        for seq, (lat, lon) in enumerate(points):
            cur.execute(
                """
                INSERT INTO plateau_building_nodes
                    (osm_id, lat, lon, sequence_id, building_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (osm_id * 100 + seq, lat, lon, seq, building_id),
            )
    return building_id


def _seed_building(conn, *, lat, lon, height=None, building_levels=None,
                   osm_id=1001, city_code='13101'):
    """Seed a simple square building with a well-known geometry."""
    wkt = _square_wkt(lat, lon)
    size_deg = 0.0001
    points = [
        (lat - size_deg, lon - size_deg),
        (lat - size_deg, lon + size_deg),
        (lat + size_deg, lon + size_deg),
        (lat + size_deg, lon - size_deg),
    ]
    return _insert_building(conn, osm_id=osm_id, city_code=city_code, wkt=wkt,
                            points=points, height=height,
                            building_levels=building_levels)


def _seed_l_shaped_building(conn, *, osm_id=1002, city_code='13101'):
    """Seed a non-convex L-shaped building.

    The L covers the square (139.75,35.65)-(139.7504,35.6504) minus its
    top-right quadrant. A naive vertex-average "centroid" lands right on
    the reflex corner; `ST_PointOnSurface` is guaranteed to land strictly
    inside the polygon, which is what this test wants to verify.
    """
    points = [
        (35.65, 139.75),
        (35.65, 139.7504),
        (35.6502, 139.7504),
        (35.6502, 139.7502),
        (35.6504, 139.7502),
        (35.6504, 139.75),
    ]
    lon_lat_pairs = [(lon, lat) for (lat, lon) in points]
    ring = ", ".join(f"{lon} {lat}" for lon, lat in lon_lat_pairs)
    first_lon, first_lat = lon_lat_pairs[0]
    wkt = f"POLYGON(({ring}, {first_lon} {first_lat}))"
    return _insert_building(conn, osm_id=osm_id, city_code=city_code, wkt=wkt,
                            points=points, height=8, building_levels=2)


def _seed_invalid_geometry_building(conn, *, osm_id=1003, city_code='13101'):
    """Seed a self-intersecting ("bowtie") building outline.

    PostGIS accepts this at INSERT time (`ST_IsValid` = false, but no
    constraint enforces validity), matching option (b) in the task's
    ambiguity resolution. In this PostGIS version `ST_PointOnSurface` does
    *not* actually return NULL/error for a bowtie (verified against the
    local PostGIS 3.6 test DB: it still resolves an interior point of one
    of the two triangles), so this test cannot assert the tag is *absent*.
    Instead it asserts the documented weaker invariant: the endpoint never
    crashes, the response is always valid XML, and if a representative_point
    tag is present its value still parses as two floats.
    """
    points = [
        (35.65, 139.75),
        (35.651, 139.751),
        (35.65, 139.751),
        (35.651, 139.75),
    ]
    lon_lat_pairs = [(lon, lat) for (lat, lon) in points]
    ring = ", ".join(f"{lon} {lat}" for lon, lat in lon_lat_pairs)
    first_lon, first_lat = lon_lat_pairs[0]
    wkt = f"POLYGON(({ring}, {first_lon} {first_lat}))"
    return _insert_building(conn, osm_id=osm_id, city_code=city_code, wkt=wkt,
                            points=points, height=5, building_levels=1)


def _insert_building_raw(conn, *, osm_id, city_code, geom_sql, points,
                          height=None, building_levels=None,
                          building_part=None, parent_building_id=None):
    """Like `_insert_building` but takes a raw SQL geometry expression
    (e.g. ``'NULL'``) instead of a WKT literal, and supports
    `building_part` / `parent_building_id` so it can seed a broken-geometry
    *part* row parented to a valid outline.

    `centroid` is left NULL too — `related_parts` (unlike `bbox_outlines` /
    `orphan_parts`) has no spatial filter on its own rows, so a NULL
    centroid doesn't exclude the row from that CTE.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO plateau_buildings
                (osm_id, building, height, building_levels, city_code,
                 building_part, parent_building_id, geom, centroid)
            VALUES (%s, 'yes', %s, %s, %s, %s, %s, {geom_sql}, NULL)
            RETURNING id
            """,
            (osm_id, height, building_levels, city_code, building_part,
             parent_building_id),
        )
        building_id = cur.fetchone()[0]

        for seq, (lat, lon) in enumerate(points):
            cur.execute(
                """
                INSERT INTO plateau_building_nodes
                    (osm_id, lat, lon, sequence_id, building_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (osm_id * 100 + seq, lat, lon, seq, building_id),
            )
    return building_id


def _seed_part_with_broken_geometry(conn, *, parent_building_id, osm_id=1004,
                                    city_code='13101', lat=35.6795,
                                    lon=139.7563):
    """Seed a `building:part='yes'` row with `geom = NULL`, parented to
    `parent_building_id`.

    Carries a valid 4-point node ring so it still survives
    `buildings_to_osm_xml`'s >=3-valid-node check and comes back as a
    `<way>` — it's the *geometry* that's broken, not the node ring. This
    reaches the HTTP response via `related_parts`, which (unlike
    `bbox_outlines` / `orphan_parts`) has no `{spatial_condition}` filter on
    its own rows and so doesn't require `geom` to be non-NULL.
    """
    size_deg = 0.0001
    points = [
        (lat - size_deg, lon - size_deg),
        (lat - size_deg, lon + size_deg),
        (lat + size_deg, lon + size_deg),
        (lat + size_deg, lon - size_deg),
    ]
    return _insert_building_raw(
        conn, osm_id=osm_id, city_code=city_code, geom_sql='NULL',
        points=points, height=5, building_levels=1,
        building_part='yes', parent_building_id=parent_building_id,
    )


def _tags(elem):
    return {t.get('k'): t.get('v') for t in elem.findall('tag')}


def _reconstruct_polygon(root, way):
    """Look up each `<nd ref>` of `way` in `root`'s `<node>` elements and
    return an ordered list of (lon, lat) tuples."""
    node_coords = {}
    for node in root.findall('node'):
        node_coords[node.get('id')] = (float(node.get('lon')), float(node.get('lat')))
    polygon = []
    for nd in way.findall('nd'):
        ref = nd.get('ref')
        if ref in node_coords:
            polygon.append(node_coords[ref])
    return polygon


def _point_in_polygon(point, polygon):
    """Standard ray-casting point-in-polygon test. `point` and `polygon`
    entries are (lon, lat) tuples."""
    x, y = point
    n = len(polygon)
    inside = False
    x1, y1 = polygon[0]
    for i in range(1, n + 1):
        x2, y2 = polygon[i % n]
        if y > min(y1, y2):
            if y <= max(y1, y2):
                if x <= max(x1, x2):
                    if y1 != y2:
                        xinters = (y - y1) * (x2 - x1) / (y2 - y1) + x1
                    if x1 == x2 or x <= xinters:
                        inside = not inside
        x1, y1 = x2, y2
    return inside


# ----------------------------------------------------------------------
# Client fixture
# ----------------------------------------------------------------------

def _make_client(integration_db_url, monkeypatch):
    """Build a TestClient against a fresh import of osmfj_plateau_api with
    DATABASE_URL pointed at the integration test DB.

    `osmfj_plateau_api` instantiates a module-level `OSMFJPlateauAPI()`
    singleton at import time (used by every request handler) which eagerly
    calls `_test_connection()` against `DATABASE_URL`, defaulting to a
    production-shaped URL that isn't reachable locally. We set
    `DATABASE_URL` to the test DB and force a fresh import so the
    module-level `api` singleton — and therefore the `/api/mapwithai/buildings`
    endpoint — reads from the same schema `fresh_plateau_full_schema` seeds
    into. This mirrors the `plateau_api_class` fixture in conftest.py.
    """
    monkeypatch.setenv('DATABASE_URL', integration_db_url)
    for mod in list(sys.modules.keys()):
        if mod.startswith('osmfj_plateau_api'):
            del sys.modules[mod]
    import osmfj_plateau_api
    return TestClient(osmfj_plateau_api.app)


# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------

def test_representative_point_present_on_every_building(
    fresh_plateau_full_schema, integration_db_url, monkeypatch
):
    """Every <way> that represents a building carries representative_point."""
    conn = fresh_plateau_full_schema
    _seed_building(conn, lat=35.6795, lon=139.7563, height=12.5, building_levels=3)
    client = _make_client(integration_db_url, monkeypatch)

    resp = client.get('/api/mapwithai/buildings',
                      params={'bbox': '139.755,35.679,139.758,35.680'})
    assert resp.status_code == 200
    root = ET.fromstring(resp.content)

    ways = root.findall('way')
    assert len(ways) >= 1
    for way in ways:
        tags = _tags(way)
        assert 'representative_point' in tags, \
            f"way {way.get('id')} is missing representative_point"
        lon_str, lat_str = tags['representative_point'].split(',')
        lon, lat = float(lon_str), float(lat_str)
        assert 139.755 <= lon <= 139.758
        assert 35.679 <= lat <= 35.680


def test_representative_point_falls_inside_polygon(
    fresh_plateau_full_schema, integration_db_url, monkeypatch
):
    """The representative point must lie inside the way's polygon, not just its bbox."""
    conn = fresh_plateau_full_schema
    _seed_l_shaped_building(conn)
    client = _make_client(integration_db_url, monkeypatch)

    resp = client.get('/api/mapwithai/buildings',
                      params={'bbox': '139.7,35.6,139.8,35.7'})
    root = ET.fromstring(resp.content)

    way = root.find('way')
    assert way is not None
    tags = _tags(way)
    assert 'representative_point' in tags
    rp = tuple(map(float, tags['representative_point'].split(',')))
    polygon = _reconstruct_polygon(root, way)
    assert _point_in_polygon(rp, polygon)


def test_absent_when_building_geometry_broken(
    fresh_plateau_full_schema, integration_db_url, monkeypatch
):
    """Companion to `test_representative_point_omitted_for_broken_part_geometry`
    below, which is the test that actually verifies the tag is *omitted*.

    If ST_PointOnSurface can't resolve a point on invalid geometry, the
    tag is absent — the endpoint must not crash either way.

    See `_seed_invalid_geometry_building`'s docstring: in this PostGIS
    version a bowtie polygon doesn't reliably make ST_PointOnSurface return
    NULL, so this test asserts the weaker (but still meaningful) invariant
    documented in the task's ambiguity resolutions — no crash, valid XML,
    and any representative_point tag that IS present still parses cleanly.
    This is still worth keeping: it's a different broken-geometry shape
    (self-intersecting outline, reached via `bbox_outlines`'s own spatial
    filter) than the NULL-geometry *part* case below.
    """
    conn = fresh_plateau_full_schema
    _seed_invalid_geometry_building(conn)
    client = _make_client(integration_db_url, monkeypatch)

    resp = client.get('/api/mapwithai/buildings',
                      params={'bbox': '139.7,35.6,139.8,35.7'})
    assert resp.status_code == 200
    root = ET.fromstring(resp.content)  # response is valid XML

    for way in root.findall('way'):
        tags = _tags(way)
        if 'representative_point' in tags:
            lon_str, lat_str = tags['representative_point'].split(',')
            float(lon_str), float(lat_str)  # must parse without raising


def test_representative_point_omitted_for_broken_part_geometry(
    fresh_plateau_full_schema, integration_db_url, monkeypatch
):
    """A building:part row with `geom = NULL` reaches the HTTP response via
    `related_parts`, which — unlike `bbox_outlines` / `orphan_parts` — has
    no `{spatial_condition}` filter on its own rows (its `WHERE` only checks
    `b.parent_building_id IN (SELECT id FROM bbox_outlines)`). So a part
    with unusable geometry isn't excluded before it reaches the SELECT
    list, and per the code's own NULL-handling in
    `get_buildings_in_bbox`/`_emit_building_tags` it should come back as a
    `<way>` that lacks `representative_point`, while its parent outline
    keeps its tag.
    """
    conn = fresh_plateau_full_schema
    lat, lon = 35.6795, 139.7563
    outline_id = _seed_building(conn, lat=lat, lon=lon, height=12.5,
                                building_levels=3, osm_id=2001)
    part_id = _seed_part_with_broken_geometry(
        conn, parent_building_id=outline_id, osm_id=2002, lat=lat, lon=lon,
    )
    client = _make_client(integration_db_url, monkeypatch)

    resp = client.get('/api/mapwithai/buildings',
                      params={'bbox': '139.755,35.679,139.758,35.680'})
    assert resp.status_code == 200
    root = ET.fromstring(resp.content)

    ways_by_id = {w.get('id'): w for w in root.findall('way')}
    outline_way = ways_by_id.get(str(-outline_id))
    part_way = ways_by_id.get(str(-part_id))

    # Both ways must be present: the broken-geometry part must survive
    # `related_parts`' WHERE and be emitted by `buildings_to_osm_xml`
    # (it has a valid 4-point node ring, independent of `geom`).
    assert outline_way is not None, \
        f"outline way -{outline_id} missing from response ways: {sorted(ways_by_id)}"
    assert part_way is not None, \
        f"broken-geometry part way -{part_id} missing from response ways " \
        f"(related_parts should not filter it out): {sorted(ways_by_id)}"

    assert 'representative_point' in _tags(outline_way), \
        "parent outline should still carry representative_point"
    assert 'representative_point' not in _tags(part_way), \
        "part with geom=NULL must NOT carry representative_point"
