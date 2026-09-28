"""plateau_height_outliers.py の試験

高さのはずれ値を数える調査スクリプトの検査を確かめる。
DB を使う試験は integration マーカーを付けている。
"""

import pytest

from tests.test_dedup_city_duplicates import _seed_building, _square_wkt


def _resize(conn, building_id, lat, lon, size_deg):
    """建物の輪郭を指定した大きさの正方形に置き換える。

    `_seed_building` は大きさが固定なので、底面積を変える試験で使う。
    """
    with conn.cursor() as cur:
        cur.execute(
            'UPDATE plateau_buildings SET geom = ST_GeomFromText(%s, 4326) '
            'WHERE id = %s',
            (_square_wkt(lat, lon, size_deg), building_id),
        )


@pytest.mark.integration
def test_part_over_outline_finds_part_taller_than_its_outline(
    fresh_plateau_full_schema, integration_db_url
):
    """部分立体が親の外形より高い場合だけを拾う。"""
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    outline = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=10.0
    )
    taller = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=15.0,
        building_part='yes', parent_building_id=outline,
    )
    _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon, height=8.0,
        building_part='yes', parent_building_id=outline,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('part-over-outline')

    assert result.total == 1
    assert [row['id'] for row in result.samples] == [taller]


@pytest.mark.integration
def test_sibling_spike_flags_one_part_far_taller_than_its_siblings(
    fresh_plateau_full_schema, integration_db_url
):
    """部分立体を 3 つ以上持つ建物で、最大が中央値の N 倍を超えるものを拾う。

    塔屋のある建物と区別できないため、既定の倍率は 3.0 にしている。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20

    # 突出した部分立体を持つ建物。
    # 兄弟の高さは 8, 9, 10, 40 で中央値は 9.5 になる。
    spiked = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=10.0
    )
    for osm_id, height in ((2, 8.0), (3, 9.0), (4, 10.0)):
        _seed_building(
            conn, osm_id=osm_id, city_code='16201', lat=lat, lon=lon,
            height=height, building_part='yes', parent_building_id=spiked,
        )
    spike = _seed_building(
        conn, osm_id=5, city_code='16201', lat=lat, lon=lon, height=40.0,
        building_part='yes', parent_building_id=spiked,
    )

    # ばらつきの小さい建物。
    # 兄弟の高さは 8, 9, 10, 11 で中央値は 9.5 になる。
    even = _seed_building(
        conn, osm_id=6, city_code='16201', lat=lat, lon=lon, height=11.0
    )
    for osm_id, height in ((7, 8.0), (8, 9.0), (9, 10.0), (10, 11.0)):
        _seed_building(
            conn, osm_id=osm_id, city_code='16201', lat=lat, lon=lon,
            height=height, building_part='yes', parent_building_id=even,
        )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('sibling-spike')

    assert result.total == 1
    assert [row['id'] for row in result.samples] == [spike]


@pytest.mark.integration
def test_floor_height_flags_values_outside_the_configured_range(
    fresh_plateau_full_schema, integration_db_url
):
    """1 階あたりの高さが範囲の外にあるものを拾う。

    範囲の内側にある建物は拾わない。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    too_tall = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon,
        height=30.0, building_levels=2,
    )
    too_short = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon,
        height=2.0, building_levels=2,
    )
    # 1 階 3.75 m は実データの中央値で、範囲の内側にある。
    _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon,
        height=7.5, building_levels=2,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('floor-height')

    assert result.total == 2
    assert sorted(row['id'] for row in result.samples) == sorted(
        [too_tall, too_short]
    )


@pytest.mark.integration
def test_needle_flags_a_tall_building_on_a_tiny_footprint(
    fresh_plateau_full_schema, integration_db_url
):
    """高さの割に底面積が小さいものを拾う。

    高さか底面積のどちらかが条件を外れていれば拾わない。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    tiny = 1.42e-5  # 一辺およそ 3.2 m で、面積は 10 m2 を下回る

    needle = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=20.0
    )
    _resize(conn, needle, lat, lon, tiny)

    # 高いが底面積は普通。
    _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=20.0
    )

    # 底面積は小さいが低い。
    low = _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon, height=5.0
    )
    _resize(conn, low, lat, lon, tiny)

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('needle')

    assert result.total == 1
    assert [row['id'] for row in result.samples] == [needle]


@pytest.mark.integration
def test_absolute_flags_heights_outside_the_plausible_range(
    fresh_plateau_full_schema, integration_db_url
):
    """高さ自体が範囲の外にあるものを拾う。

    階数が入っているのは全体の約半分なので、
    floor-height が使えない建物をこの検査が受け持つ。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    too_low = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=0.4
    )
    too_high = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=250.0
    )
    # 実データの中央値。
    _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon, height=7.0
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('absolute')

    assert result.total == 2
    assert sorted(row['id'] for row in result.samples) == sorted(
        [too_low, too_high]
    )


def test_resolve_checks_defaults_to_every_check_in_registry_order():
    """--check を省いたときは、登録してある順にすべて返す。

    順序は確からしさの高いものからにしてあり、報告の読み順になる。
    """
    from plateau_height_outliers import CHECKS, resolve_checks

    assert resolve_checks(None) == list(CHECKS)


def test_resolve_checks_rejects_an_unknown_name():
    """綴りの誤りを黙って無視しない。"""
    from plateau_height_outliers import resolve_checks

    with pytest.raises(ValueError, match='不明な検査'):
        resolve_checks(['needle', 'noodle'])


@pytest.mark.integration
def test_check_result_counts_by_city_in_descending_order(
    fresh_plateau_full_schema, integration_db_url
):
    """該当件数を都市ごとにも数える。

    偏りが分かると、変換の不具合か特定の都市のデータかを切り分けられる。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    for osm_id in (1, 2, 3):
        _seed_building(
            conn, osm_id=osm_id, city_code='16201',
            lat=36.70, lon=137.20, height=250.0,
        )
    _seed_building(
        conn, osm_id=4, city_code='42201',
        lat=32.75, lon=129.87, height=250.0,
    )
    _seed_building(
        conn, osm_id=5, city_code='42201',
        lat=32.75, lon=129.87, height=7.0,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('absolute')

    assert result.total == 4
    assert result.by_city == [('16201', 3), ('42201', 1)]


@pytest.mark.integration
def test_floor_height_samples_show_both_ends_of_the_range(
    fresh_plateau_full_schema, integration_db_url
):
    """代表例は、範囲からの外れ方が大きい順に並ぶ。

    高い側だけで埋まると、低すぎる建物が目に入らなくなる。
    低い側は 0 で頭打ちになるため、差ではなく範囲に対する倍率で比べる。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    # 1 階 0.1 m。下限 1.5 m の 15 分の 1。
    too_low = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon,
        height=0.2, building_levels=2,
    )
    # 1 階 50 m。上限 10.2 m の約 4.9 倍。
    too_high = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon,
        height=100.0, building_levels=2,
    )
    # 1 階 11 m。上限をわずかに超えるだけ。
    _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon,
        height=22.0, building_levels=2,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('floor-height', samples=2)

    assert result.total == 3
    assert [row['id'] for row in result.samples] == [too_low, too_high]


@pytest.mark.integration
def test_absolute_samples_rank_by_how_far_outside_the_range(
    fresh_plateau_full_schema, integration_db_url
):
    """代表例は、高さの順ではなく範囲からの外れ方の大きい順に並ぶ。

    高さの順だと低い側だけが代表例を占める。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    # 下限 1.0 m をわずかに下回るだけ。
    _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=0.9
    )
    # 上限 200 m の 4 倍。
    far_out = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=800.0
    )
    _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon, height=7.0
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('absolute', samples=1)

    assert result.total == 2
    assert [row['id'] for row in result.samples] == [far_out]


@pytest.mark.integration
def test_run_check_can_limit_to_one_city(
    fresh_plateau_full_schema, integration_db_url
):
    """1 都市に絞って数えられる。

    全件は 2,700 万件を超えるので、試すときに使う。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    for osm_id in (1, 2):
        _seed_building(
            conn, osm_id=osm_id, city_code='16201',
            lat=36.70, lon=137.20, height=250.0,
        )
    kept = _seed_building(
        conn, osm_id=3, city_code='42201',
        lat=32.75, lon=129.87, height=250.0,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('absolute', city='42201')

    assert result.total == 1
    assert result.by_city == [('42201', 1)]
    assert [row['id'] for row in result.samples] == [kept]
