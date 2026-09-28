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


def test_api_agrees_with_the_survey_on_the_same_buildings(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """同じ建物に対して、調査と API が同じ建物を選ぶ。

    判定を SQL と Python の 2 か所で持つので、食い違いをここで捕まえる。
    警告にしない条件（absolute の上側、住宅以外の floor-height）は、
    調査の結果から除いて比べる。
    """
    from plateau_height_outliers import HeightOutlierSurvey
    from plateau_height_warning import RESIDENTIAL_BUILDING_VALUES

    conn = fresh_plateau_full_schema
    seeds = [
        dict(height=7.0),
        dict(height=0.5),
        dict(height=250.0),
        dict(height=18.4, tiny=True),
        dict(height=24.6, levels=2, building='house'),
        dict(height=24.6, levels=2, building='warehouse'),
        dict(height=20.0, broken=True),
    ]
    for osm_id, s in enumerate(seeds, start=1):
        lon = LON + STEP * osm_id
        bid = _seed_building(
            conn, osm_id=osm_id, city_code='16201', lat=LAT, lon=lon,
            height=s['height'], building_levels=s.get('levels'),
        )
        if 'building' in s:
            _set_building(conn, bid, s['building'])
        if s.get('tiny'):
            _set_wkt(conn, bid, _square_wkt(LAT, lon, 0.00001))
        if s.get('broken'):
            _set_wkt(conn, bid, _bowtie_with_hole_wkt(LAT, lon))
    outline = _seed_building(
        conn, osm_id=100, city_code='16201', lat=LAT, lon=LON, height=9.1,
    )
    _seed_building(
        conn, osm_id=101, city_code='16201', lat=LAT, lon=LON, height=149.2,
        building_part='yes', parent_building_id=outline,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    rows = _rows_by_id(plateau_api_class, integration_db_url)

    for check in ('degenerate-area', 'part-over-outline', 'needle',
                  'absolute', 'floor-height'):
        result = survey.run_check(check, samples=100)
        expected = set()
        for s in result.samples:
            if check == 'absolute' and s['height'] >= 1.0:
                continue
            if (check == 'floor-height'
                    and s['building'] not in RESIDENTIAL_BUILDING_VALUES):
                continue
            expected.add(s['id'])
        actual = {
            bid for bid, r in rows.items() if check in r['height_warnings']
        }
        assert actual == expected, check
