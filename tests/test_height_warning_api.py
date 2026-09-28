"""get_buildings_in_bbox が行に高さの警告を書き込むことの統合試験

PostGIS が要るので、--run-integration のときだけ動く。
"""

import pytest

from tests.test_dedup_city_duplicates import _seed_building, _square_wkt
from tests.test_plateau_height_outliers import (
    _bowtie_with_hole_wkt, _set_wkt,
)

pytestmark = pytest.mark.integration

LAT, LON = 36.70, 137.20
BBOX = (137.19, 36.69, 137.21, 36.71)

# API は、重心、高さ、階数、部分立体かどうかがすべて同じ建物を 1 つにまとめる。
# 同じ高さの建物を並べる試験では、経度をずらして別の建物にする。
STEP = 0.001


def _set_building(conn, building_id, value):
    with conn.cursor() as cur:
        cur.execute(
            'UPDATE plateau_buildings SET building = %s WHERE id = %s',
            (value, building_id),
        )


def _rows_by_id(plateau_api_class, integration_db_url):
    api = plateau_api_class(database_url=integration_db_url)
    rows = api.get_buildings_in_bbox(*BBOX)
    return {r['id']: r for r in rows}


def test_rows_carry_warnings_and_drop_the_planar_area(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    ok = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=7.0,
    )
    low = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON, height=0.5,
    )

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[ok]['height_warnings'] == []
    assert rows[low]['height_warnings'] == ['absolute']
    assert 'planar_area' not in rows[ok]


def test_needle_gets_a_footprint_in_square_metres(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    needle = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=18.4,
    )
    _set_wkt(conn, needle, _square_wkt(LAT, LON, 0.00001))
    short = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON, height=7.0,
    )

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[needle]['height_warnings'] == ['needle']
    assert 0 < rows[needle]['footprint_m2'] < 20
    # 高さ 15 m 以下の建物は、球面上の面積を計算しない。
    assert rows[short]['footprint_m2'] is None


def test_negative_area_does_not_stop_the_query(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    broken = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=20.0,
    )
    _set_wkt(conn, broken, _bowtie_with_hole_wkt(LAT, LON))

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[broken]['height_warnings'] == ['degenerate-area']
    assert rows[broken]['footprint_m2'] is None


def test_part_is_compared_with_its_outline_in_the_same_response(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    outline = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=9.1,
    )
    part = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON, height=149.2,
        building_part='yes', parent_building_id=outline,
    )

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[part]['height_warnings'] == ['part-over-outline']
    assert rows[outline]['height_warnings'] == []


def test_floor_height_warns_only_for_residential(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    house = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON,
        height=24.6, building_levels=2,
    )
    _set_building(conn, house, 'house')
    factory = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON + STEP,
        height=24.6, building_levels=2,
    )
    _set_building(conn, factory, 'industrial')

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[house]['height_warnings'] == ['floor-height']
    assert rows[factory]['height_warnings'] == []
