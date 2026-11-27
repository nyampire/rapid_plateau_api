"""外形と重ならない部分立体を、独立した建物の行として返す問い合わせの統合試験。

実際の PostgreSQL + PostGIS が要ります。PLATEAU_TEST_DATABASE_URL を設定し、
``pytest --run-integration`` で実行します。
tests/test_intersects_parent_flag.py と同じ理由で、単体試験のモジュールとは分けています。
"""
import pytest


def _square(lon, lat, size):
    return (
        f"POLYGON(({lon} {lat},{lon + size} {lat},{lon + size} {lat + size},"
        f"{lon} {lat + size},{lon} {lat}))"
    )


def _insert_outline(cur, lon, lat, height=None):
    cur.execute(
        """
        INSERT INTO plateau_buildings (id, osm_id, building, height, city_code, geom, centroid)
        VALUES (1, -1, 'yes', %s, '39201',
                ST_GeomFromText(%s, 4326),
                ST_GeomFromText(%s, 4326))
        """,
        (height, _square(lon, lat, 0.001), f"POINT({lon + 0.0005} {lat + 0.0005})"),
    )


def _insert_part(cur, part_id, lon, lat, ref, parent_id=1, size=0.0005,
                 height=None, null_geom=False):
    geom = None if null_geom else _square(lon, lat, size)
    cur.execute(
        """
        INSERT INTO plateau_buildings
            (id, osm_id, building_part, parent_building_id, ref_mlit_plateau,
             height, city_code, geom, centroid)
        VALUES (%s, %s, 'yes', %s, %s, %s, '39201',
                CASE WHEN %s::text IS NULL THEN NULL
                     ELSE ST_GeomFromText(%s::text, 4326) END,
                ST_GeomFromText(%s, 4326))
        """,
        (part_id, -part_id, parent_id, ref, height, geom, geom,
         f"POINT({lon + size / 2} {lat + size / 2})"),
    )


@pytest.fixture
def seeded_rows(fresh_plateau_full_schema, plateau_api_class):
    conn = fresh_plateau_full_schema
    cur = conn.cursor()
    _insert_outline(cur, 133.0, 33.0)
    _insert_part(cur, 2, 133.0002, 33.0002, 'A-2', size=0.0004)  # 外形の内側
    _insert_part(cur, 3, 133.003, 33.003, 'A-3')  # 離れていて、条件を満たす
    _insert_part(cur, 4, 133.005, 33.005, 'Null')  # 建物IDが Null
    _insert_part(cur, 5, 133.007, 33.007, 'A-2')  # 同じ建物IDの兄弟がいる
    _insert_part(cur, 6, 133.009, 33.003, None)  # 建物IDが NULL
    _insert_part(cur, 7, 133.003, 33.009, 'A-7', null_geom=True)  # geom が無い
    conn.commit()
    api = plateau_api_class()
    return api.get_buildings_in_bbox(132.99, 32.99, 133.01, 33.01)


@pytest.mark.integration
def test_detached_part_is_returned_as_a_building(seeded_rows):
    promoted = [r for r in seeded_rows if r['promoted_part']]
    assert [r['id'] for r in promoted] == [3]
    assert promoted[0]['building_part'] is None
    assert promoted[0]['parent_building_id'] is None


@pytest.mark.integration
def test_part_inside_outline_is_unchanged(seeded_rows):
    row = next(r for r in seeded_rows if r['id'] == 2)
    assert row['promoted_part'] is False
    assert row['building_part'] == 'yes'
    assert row['parent_building_id'] == 1


@pytest.mark.integration
def test_detached_part_is_returned_when_its_outline_is_out_of_range(
    fresh_plateau_full_schema, plateau_api_class
):
    conn = fresh_plateau_full_schema
    cur = conn.cursor()
    _insert_outline(cur, 134.0, 34.0)
    _insert_part(cur, 3, 133.003, 33.003, 'A-3')
    conn.commit()

    rows = plateau_api_class().get_buildings_in_bbox(132.99, 32.99, 133.01, 33.01)

    promoted = [r for r in rows if r['promoted_part']]
    assert [r['id'] for r in promoted] == [3]
    assert promoted[0]['building_part'] is None


@pytest.mark.integration
def test_orphan_part_is_returned_as_a_building(
    fresh_plateau_full_schema, plateau_api_class
):
    conn = fresh_plateau_full_schema
    cur = conn.cursor()
    _insert_part(cur, 8, 133.003, 33.003, 'A-8', parent_id=None)
    conn.commit()

    rows = plateau_api_class().get_buildings_in_bbox(132.99, 32.99, 133.01, 33.01)

    promoted = [r for r in rows if r['promoted_part']]
    assert [r['id'] for r in promoted] == [8]
    assert promoted[0]['building_part'] is None


@pytest.mark.integration
def test_detached_part_gets_needle_and_not_part_over_outline(
    fresh_plateau_full_schema, plateau_api_class
):
    conn = fresh_plateau_full_schema
    cur = conn.cursor()
    _insert_outline(cur, 133.0, 33.0, height=9.1)
    # 0.00003 度四方は約 9 平方メートルで、DEFAULT_NEEDLE_AREA_M2 より小さい
    _insert_part(cur, 3, 133.003, 33.003, 'A-3', size=0.00003, height=149.2)
    conn.commit()

    rows = plateau_api_class().get_buildings_in_bbox(132.99, 32.99, 133.01, 33.01)

    row = next(r for r in rows if r['id'] == 3 and r['promoted_part'])
    assert 'needle' in row['height_warnings']
    assert 'part-over-outline' not in row['height_warnings']
