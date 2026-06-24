"""Integration tests for cross-city building dedup at API output (#31).

These tests require a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. They are skipped by default; run with
``pytest --run-integration``.
"""
import logging
import sys
from unittest.mock import MagicMock, patch

import pytest


# osmfj_plateau_api tries to connect to DB at module import time,
# so we must patch psycopg2.connect before importing it.
def _import_plateau_api_with_mock():
    """Import OSMFJPlateauAPI with mocked psycopg2.connect, then return the class."""
    cursor = MagicMock()
    cursor.fetchone.return_value = ('3.4 USE_GEOS=1',)
    cursor.fetchall.return_value = [('plateau_buildings',), ('plateau_building_nodes',)]
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)

    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch('psycopg2.connect', return_value=conn):
        for mod in list(sys.modules.keys()):
            if mod.startswith('osmfj_plateau_api'):
                del sys.modules[mod]
        from osmfj_plateau_api import OSMFJPlateauAPI
        return OSMFJPlateauAPI


# Import at module level, but with mock; after import completes, the mock is discarded
PlateauAPI = _import_plateau_api_with_mock()


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
    fresh_plateau_full_schema, integration_db_url
):
    """Smoke test: fixture and seed helpers wired up; PlateauAPI can read."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=1, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 1
