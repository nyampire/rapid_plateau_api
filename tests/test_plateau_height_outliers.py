"""plateau_height_outliers.py の試験

高さのはずれ値を数える調査スクリプトの検査を確かめる。
DB を使う試験は integration マーカーを付けている。
"""

import pytest

from tests.test_dedup_city_duplicates import _seed_building, _square_wkt


def _set_wkt(conn, building_id, wkt):
    """建物の輪郭を指定した WKT に置き換える。"""
    with conn.cursor() as cur:
        cur.execute(
            'UPDATE plateau_buildings SET geom = ST_GeomFromText(%s, 4326) '
            'WHERE id = %s',
            (wkt, building_id),
        )


def _resize(conn, building_id, lat, lon, size_deg):
    """建物の輪郭を指定した大きさの正方形に置き換える。

    `_seed_building` は大きさが固定なので、底面積を変える試験で使う。
    """
    _set_wkt(conn, building_id, _square_wkt(lat, lon, size_deg))


def _bowtie_wkt(lat, lon, size_deg=0.0002):
    """面積が 0 になる自己交差した輪郭。

    対角どうしを結んで 2 つの葉が打ち消し合う形にしている。
    """
    return (
        f"POLYGON(("
        f"{lon} {lat},"
        f"{lon + size_deg} {lat + size_deg},"
        f"{lon + size_deg} {lat},"
        f"{lon} {lat + size_deg},"
        f"{lon} {lat}"
        f"))"
    )


@pytest.mark.integration
def test_part_over_outline_ignores_rooftop_scale_excess(
    fresh_plateau_full_schema, integration_db_url
):
    """外形より高い部分立体のうち、超過が大きいものだけを検出する。

    屋上の塔屋や階段室は外形より高くなるので、小さな超過は正常である。
    実データで超過 10 m 以下の 143 件は面積比の中央値が 0.004 から 0.145 で、
    外形のごく一部しか覆っていない。
    超過 10 m 超の 37 件だけ面積比の中央値が 4.481 で、性質が違う。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    outline = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=10.0
    )
    far_above = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=25.0,
        building_part='yes', parent_building_id=outline,
    )
    # 超過 5 m は塔屋の高さにあたるので検出しない。
    _seed_building(
        conn, osm_id=3, city_code='16201', lat=lat, lon=lon, height=15.0,
        building_part='yes', parent_building_id=outline,
    )
    # 外形より低い部分立体は検出しない。
    _seed_building(
        conn, osm_id=4, city_code='16201', lat=lat, lon=lon, height=8.0,
        building_part='yes', parent_building_id=outline,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('part-over-outline')

    assert result.total == 1
    assert [row['id'] for row in result.samples] == [far_above]


@pytest.mark.integration
def test_sibling_spike_flags_one_part_far_taller_than_its_siblings(
    fresh_plateau_full_schema, integration_db_url
):
    """部分立体を 3 つ以上持つ建物で、最大が中央値の N 倍を超えるものを検出する。

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
    """1 階あたりの高さが範囲の外にあるものを検出する。

    範囲の内側にある建物は検出しない。
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
    """高さの割に底面積が小さいものを検出する。

    高さか底面積のどちらかが条件を外れていれば検出しない。
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
    """高さ自体が範囲の外にあるものを検出する。

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


def _bowtie_with_hole_wkt(lat, lon, size_deg=0.0002):
    """平面での面積が負になる自己交差した輪郭。

    PostGIS は外環の面積の絶対値から、内環の面積の絶対値を引く。
    面積が 0 の蝶形に小さな内環を足すと、差し引きが負になる。
    本番で球面の面積計算を止めたのと同じ形になる。
    """
    step = size_deg / 20
    hole = (
        f"({lon + step} {lat + step},"
        f"{lon + step} {lat + 2 * step},"
        f"{lon + 2 * step} {lat + 2 * step},"
        f"{lon + 2 * step} {lat + step},"
        f"{lon + step} {lat + step})"
    )
    return _bowtie_wkt(lat, lon, size_deg)[:-1] + ',' + hole + ')'


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


@pytest.mark.integration
def test_needle_skips_polygons_whose_area_is_not_positive(
    fresh_plateau_full_schema, integration_db_url
):
    """平面での面積が正でない多角形を対象から外す。

    球面の面積計算は負の値を受け取ると内部エラーになる。
    本番には該当する多角形が 7 件あり、走査の途中で落ちる。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    broken = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=20.0
    )
    _set_wkt(conn, broken, _bowtie_wkt(lat, lon))

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('needle')

    assert result.total == 0


@pytest.mark.integration
def test_needle_skips_polygons_whose_area_is_negative(
    fresh_plateau_full_schema, integration_db_url
):
    """平面での面積が負の多角形でも、走査が止まらない。

    面積がちょうど 0 の輪郭では、球面の面積計算は止まらない。
    本番で起きた内部エラーは、面積が負のときにだけ出る。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    broken = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=20.0
    )
    _set_wkt(conn, broken, _bowtie_with_hole_wkt(lat, lon))

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    assert survey.run_check('needle').total == 0
    degenerate = survey.run_check('degenerate-area')
    assert [row['id'] for row in degenerate.samples] == [broken]


@pytest.mark.integration
def test_survey_opens_the_database_read_only(integration_db_url):
    """検査の SQL は、読み取り専用のトランザクションの中で実行する。

    読むだけであることを、コードを読まなくても接続の側で確かめられる。
    """
    from plateau_height_outliers import HeightOutlierSurvey, MatchQuery

    probe = MatchQuery(
        sql="""
            SELECT 1 AS id, '16201' AS city_code,
                   current_setting('transaction_read_only') AS read_only
        """,
        params=(),
        order_by='id',
    )
    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey._run('probe', probe, samples=1)

    assert result.samples[0]['read_only'] == 'on'


@pytest.mark.integration
def test_degenerate_area_reports_polygons_with_non_positive_area(
    fresh_plateau_full_schema, integration_db_url
):
    """面積が正でない多角形を、それ自体の誤りとして報告する。

    needle はこれを対象から外すので、外したものがどこかに出る必要がある。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    broken = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=20.0
    )
    _set_wkt(conn, broken, _bowtie_wkt(lat, lon))
    # 通常の建物は報告しない。
    _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=7.0
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('degenerate-area')

    assert result.total == 1
    assert [row['id'] for row in result.samples] == [broken]


@pytest.mark.integration
def test_part_over_outline_reports_the_area_ratio(
    fresh_plateau_full_schema, integration_db_url
):
    """代表例に、外形に対する部分立体の面積比を添える。

    屋上の構造物は平面が小さく、比が 0.1 前後になる。
    外形より広い部分立体は構造が壊れており、比が 1 を超える。
    高さの差だけでは区別できないので、この列を判断材料にする。
    """
    from plateau_height_outliers import HeightOutlierSurvey

    conn = fresh_plateau_full_schema
    lat, lon = 36.70, 137.20
    outline = _seed_building(
        conn, osm_id=1, city_code='16201', lat=lat, lon=lon, height=10.0
    )
    part = _seed_building(
        conn, osm_id=2, city_code='16201', lat=lat, lon=lon, height=25.0,
        building_part='yes', parent_building_id=outline,
    )
    # 一辺を半分にすると面積は 4 分の 1 になる。
    _resize(conn, part, lat, lon, 0.00005)

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    result = survey.run_check('part-over-outline')

    assert result.total == 1
    assert result.samples[0]['area_ratio'] == pytest.approx(0.25, rel=1e-6)
