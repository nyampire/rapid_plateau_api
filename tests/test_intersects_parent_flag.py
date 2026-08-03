"""Integration test for the ``intersects_parent`` SQL column (#39).

Requires a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. Skipped by default; run with
``pytest --run-integration``.

Kept in its own file (rather than ``tests/test_osmfj_plateau_api.py``)
because that module has a module-scoped ``autouse`` fixture
(``patch_psycopg2_before_import``) that globally mocks ``psycopg2.connect``
for its unit tests. Placing a real-DB integration test in that module would
make ``psycopg2.connect`` return the mock instead of a real connection,
silently turning the test into a no-op. The existing integration suites
(``test_dedup_city_duplicates.py`` etc.) already follow this same
separation for the same reason.
"""
import pytest


@pytest.mark.integration
class TestIntersectsParentFlag:
    """SQL が返す intersects_parent が、実ジオメトリの交差と一致すること。"""

    def test_far_part_flagged_false_and_near_part_true(
        self, fresh_plateau_full_schema, plateau_api_class
    ):
        conn = fresh_plateau_full_schema
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO plateau_buildings (id, osm_id, building, city_code, geom, centroid)
            VALUES (1, -1, 'yes', '39201',
                    ST_GeomFromText('POLYGON((133.0 33.0,133.001 33.0,133.001 33.001,133.0 33.001,133.0 33.0))', 4326),
                    ST_GeomFromText('POINT(133.0005 33.0005)', 4326))
        """)
        cur.execute("""
            INSERT INTO plateau_buildings
                (id, osm_id, building_part, parent_building_id, city_code, geom, centroid)
            VALUES
                (2, -2, 'yes', 1, '39201',
                 ST_GeomFromText('POLYGON((133.0002 33.0002,133.0006 33.0002,133.0006 33.0006,133.0002 33.0006,133.0002 33.0002))', 4326),
                 ST_GeomFromText('POINT(133.0004 33.0004)', 4326)),
                (3, -3, 'yes', 1, '39201',
                 ST_GeomFromText('POLYGON((134.0 34.0,134.001 34.0,134.001 34.001,134.0 34.001,134.0 34.0))', 4326),
                 ST_GeomFromText('POINT(134.0005 34.0005)', 4326))
        """)
        conn.commit()

        api = plateau_api_class()
        rows = api.get_buildings_in_bbox(132.99, 32.99, 133.01, 33.01)

        by_id = {r['id']: r for r in rows}
        assert by_id[2]['intersects_parent'] is True
        assert by_id[3]['intersects_parent'] is False
