"""Integration tests for cross-city building dedup at API output (#31).

These tests require a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. They are skipped by default; run with
``pytest --run-integration``.
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
