"""Integration tests for cross-city building dedup at API output (#31).

These tests require a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. They are skipped by default; run with
``pytest --run-integration``.
"""
import logging

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


def _seed_building(conn, *, osm_id, city_code, lat, lon,
                   height=None, building_levels=None,
                   building_part=None, parent_building_id=None):
    """Insert one plateau_buildings row, return its DB id."""
    wkt = _square_wkt(lat, lon)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plateau_buildings
                (osm_id, building, height, building_levels, city_code,
                 building_part, parent_building_id, geom, centroid)
            VALUES (%s, 'yes', %s, %s, %s, %s, %s,
                    ST_GeomFromText(%s, 4326),
                    ST_Centroid(ST_GeomFromText(%s, 4326)))
            RETURNING id
            """,
            (osm_id, height, building_levels, city_code, building_part,
             parent_building_id, wkt, wkt),
        )
        return cur.fetchone()[0]


def _seed_city_boundary(conn, *, city_code, polygon_wkt):
    """Insert one dash_city_master row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dash_city_master (city_code, boundary_geom)
            VALUES (%s, ST_Multi(ST_GeomFromText(%s, 4326)))
            """,
            (city_code, polygon_wkt),
        )


@pytest.mark.integration
def test_fixture_round_trips_a_single_building(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """Smoke test: fixture and seed helpers wired up; OSMFJPlateauAPI can read."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=1, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 1


@pytest.mark.integration
def test_dedup_picks_n03_contained_city(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """When city A's N03 contains the centroid and city B has no boundary row,
    both pass the input filter; dedup must keep city A."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    # 13206 boundary contains the centroid → passes city_boundary_filter
    _seed_city_boundary(conn, city_code='13206',
                        polygon_wkt=_square_wkt(lat, lon, size_deg=0.01))
    # 13214 has no dash_city_master row → also passes (LEFT-JOIN behavior)

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 101  # 13206 wins via N03 priority


@pytest.mark.integration
def test_dedup_picks_smallest_city_code_when_neither_n03_contains(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """Neither city has a dash_city_master row → smallest city_code wins."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 102  # 13206 wins via smallest city_code


@pytest.mark.integration
def test_dedup_picks_smallest_city_code_when_both_n03_contain(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """Both boundaries overlap and contain the centroid → smallest city_code wins."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)
    inside = _square_wkt(lat, lon, size_deg=0.01)
    _seed_city_boundary(conn, city_code='13206', polygon_wkt=inside)
    _seed_city_boundary(conn, city_code='13214', polygon_wkt=inside)

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 102


@pytest.mark.integration
def test_height_difference_preserves_both(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """Same centroid but different height → two distinct buildings → both kept."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13214',
                   lat=lat, lon=lon, height=23, building_levels=5)

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert {r['osm_id'] for r in results} == {101, 102}


@pytest.mark.integration
def test_related_parts_follow_surviving_outline(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """When outline A is deduped out, A's children parts must also disappear."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    outline_a = _seed_building(conn, osm_id=101, city_code='13214',
                               lat=lat, lon=lon, height=22, building_levels=5)
    outline_b = _seed_building(conn, osm_id=102, city_code='13206',
                               lat=lat, lon=lon, height=22, building_levels=5)
    # Children: same shape per parent, offset slightly so they don't collide
    _seed_building(conn, osm_id=201, city_code='13214',
                   lat=lat + 0.00002, lon=lon, height=10, building_levels=3,
                   building_part='yes', parent_building_id=outline_a)
    _seed_building(conn, osm_id=202, city_code='13206',
                   lat=lat + 0.00002, lon=lon, height=10, building_levels=3,
                   building_part='yes', parent_building_id=outline_b)

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    # Surviving outline = 102 (13206 wins via smallest city_code).
    # 102's child = 202. Outline 101 and its child 201 are gone.
    osm_ids = {r['osm_id'] for r in results}
    assert osm_ids == {102, 202}


@pytest.mark.integration
def test_orphan_parts_dedup(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """Orphan building:part rows (parent_building_id IS NULL) must also be
    deduped across cities by the same key + tiebreaker as outlines."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=301, city_code='13214',
                   lat=lat, lon=lon, height=10, building_levels=3,
                   building_part='yes')
    _seed_building(conn, osm_id=302, city_code='13206',
                   lat=lat, lon=lon, height=10, building_levels=3,
                   building_part='yes')

    api = plateau_api_class(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 302  # 13206 wins via smallest city_code


@pytest.mark.integration
def test_logs_deduped_count(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class, caplog
):
    """The info log line must include the deduped count, derived from
    COUNT(*) OVER () minus the returned row count."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = plateau_api_class(database_url=integration_db_url)
    with caplog.at_level(logging.INFO, logger='osmfj_plateau_api'):
        api.get_buildings_in_bbox(
            lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
        )

    info_msgs = [r.message for r in caplog.records if r.levelno == logging.INFO]
    matched = [m for m in info_msgs if 'deduped' in m and '1件' in m]
    assert matched, f"expected a 'deduped: 1件' info log, got: {info_msgs!r}"
