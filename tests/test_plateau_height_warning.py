"""plateau_height_warning.py の試験

API が返す行 1 件ごとの判定を確かめる。
DB を使わない。
"""

import pytest

from plateau_height_warning import (
    annotate_height_warnings, height_warnings,
)


def _row(**kwargs):
    """判定に関係しない列を既定値で埋めた行。"""
    row = {
        'id': 1, 'parent_building_id': None, 'building_part': None,
        'building': 'yes',
        'height': 7.0, 'building_levels': None,
        'planar_area': 1e-8, 'footprint_m2': None,
    }
    row.update(kwargs)
    return row


def test_ordinary_building_has_no_warning():
    assert height_warnings(_row()) == []


@pytest.mark.parametrize('area, expected', [
    (0.0, ['degenerate-area']),
    (-2.4e-08, ['degenerate-area']),
    (1e-9, []),
    (None, []),
])
def test_degenerate_area_is_zero_or_negative_planar_area(area, expected):
    assert height_warnings(_row(planar_area=area)) == expected


def test_part_over_outline_needs_more_than_the_tolerance():
    outline = _row(id=1, height=9.0)
    over = _row(id=2, parent_building_id=1, height=19.5)
    at_limit = _row(id=3, parent_building_id=1, height=19.0)

    assert height_warnings(over, outline) == ['part-over-outline']
    assert height_warnings(at_limit, outline) == []


def test_part_over_outline_is_skipped_without_both_heights():
    outline = _row(id=1, height=None)
    part = _row(id=2, parent_building_id=1, height=150.0)
    assert height_warnings(part, outline) == []
    assert height_warnings(part, None) == []


@pytest.mark.parametrize('height, area, expected', [
    (18.4, 0.0036, ['needle']),
    (15.0, 1.0, []),       # 高さはちょうど 15 m では対象外
    (18.4, 20.0, []),      # 底面積はちょうど 20 m² では対象外
    (18.4, None, []),      # 底面積を計算していない行
])
def test_needle(height, area, expected):
    assert height_warnings(_row(height=height, footprint_m2=area)) == expected


def test_needle_skips_building_parts():
    # 屋上の塔屋や階段室は、地面からの高さが 15 m を超え底面積が小さい。
    part = _row(
        id=2, parent_building_id=1, building_part='yes',
        height=18.4, footprint_m2=0.0036,
    )
    assert height_warnings(part) == []


@pytest.mark.parametrize('height, expected', [
    (0.5, ['absolute']),
    (0.99, ['absolute']),
    (1.0, []),
    (250.0, []),           # 上側は警告にしない
    (None, []),
])
def test_absolute_warns_only_on_the_low_side(height, expected):
    assert height_warnings(_row(height=height)) == expected


@pytest.mark.parametrize('building, height, levels, expected', [
    ('house', 24.6, 2, ['floor-height']),       # 12.3 m / 階
    ('apartments', 2.97, 29, ['floor-height']),  # 0.10 m / 階
    ('residential', 20.4, 2, []),                # ちょうど 10.2 m / 階
    ('house', 3.0, 2, []),                       # ちょうど 1.5 m / 階
    ('industrial', 24.6, 2, []),                 # 住宅以外は警告にしない
    ('house', 24.6, 0, []),
    ('house', 24.6, None, []),
])
def test_floor_height_warns_only_for_residential(
    building, height, levels, expected
):
    row = _row(building=building, height=height, building_levels=levels)
    assert height_warnings(row) == expected


def test_warnings_follow_the_order_of_the_survey_checks():
    row = _row(
        planar_area=0.0, height=0.5, building='house', building_levels=3,
    )
    assert height_warnings(row) == [
        'degenerate-area', 'absolute', 'floor-height'
    ]


def test_height_given_as_a_string_is_read_as_a_number():
    assert height_warnings(_row(height='0.5')) == ['absolute']


def test_annotate_finds_the_outline_among_the_rows():
    outline = _row(id=10, height=9.1)
    part = _row(id=11, parent_building_id=10, height=149.2)
    orphan = _row(id=12, parent_building_id=99, height=149.2)

    annotate_height_warnings([outline, part, orphan])

    assert outline['height_warnings'] == []
    assert part['height_warnings'] == ['part-over-outline']
    assert orphan['height_warnings'] == []
