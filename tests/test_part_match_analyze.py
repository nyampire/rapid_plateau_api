"""書き出しの問い合わせと、突き合わせの本体のテスト。

幾何は shapely で組み立てた合成データを使う。
報告された建物 (高い棟 1 つに低い下屋が付き、OSM では 2 つの way に分かれている) と
同じ形を作り、取りこぼしが 1 件出ることを確かめる。
"""

import sys
from pathlib import Path

from shapely.geometry import box

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'scripts' / 'part_match'))

import analyze  # noqa: E402
import export_city  # noqa: E402


class TestBuildCopySql:
    def test_selects_the_columns_the_analysis_needs(self):
        sql = export_city.build_copy_sql()
        for column in ('id', 'parent_building_id', 'building_part', 'height', 'ele', 'wkb'):
            assert column in sql

    def test_passes_the_city_code_as_a_parameter(self):
        sql = export_city.build_copy_sql()
        assert '%s' in sql
        assert '39201' not in sql
        assert '23202' not in sql

    def test_applies_the_city_boundary_filter(self):
        # API の入口フィルタと同じ条件。隣接市の重複を持ち込まない
        sql = export_city.build_copy_sql()
        assert 'dash_city_master' in sql
        assert 'NOT EXISTS' in sql


def _reported_building():
    """報告の建物と同じ構成を作る。

    外形は高い棟と低い下屋を合わせた縦長。OSM は 2 つの way に分かれている。
    """
    tall = box(0.0, 0.0, 1.0, 2.0)      # 高い棟
    annex = box(0.0, 2.0, 1.0, 2.5)     # 低い下屋
    outline = box(0.0, 0.0, 1.0, 2.5)
    outlines = [{'id': 1, 'height': 14.3, 'geom': outline}]
    parts = {1: [{'height': 14.3, 'geom': tall}, {'height': 5.5, 'geom': annex}]}
    osm = [
        {'id': 'w469808835', 'height': None, 'geom': box(0.02, 0.02, 0.98, 1.98)},
        {'id': 'w1543990755', 'height': None, 'geom': box(0.02, 2.02, 0.98, 2.48)},
    ]
    return outlines, parts, osm


class TestCollectRows:
    def test_finds_both_osm_buildings_under_one_outline(self):
        outlines, parts, osm = _reported_building()
        rows = analyze.collect_rows(outlines, parts, osm)
        assert len(rows) == 2
        assert all(r['n_osm'] == 2 for r in rows)

    def test_only_one_of_them_is_the_current_match(self):
        outlines, parts, osm = _reported_building()
        rows = analyze.collect_rows(outlines, parts, osm)
        current = [r for r in rows if r['is_current_match']]
        assert len(current) == 1
        assert current[0]['osm_id'] == 'w469808835'

    def test_the_annex_matches_the_low_part_by_shape(self):
        outlines, parts, osm = _reported_building()
        rows = analyze.collect_rows(outlines, parts, osm)
        annex = next(r for r in rows if r['osm_id'] == 'w1543990755')
        assert annex['part_height'] == 5.5
        assert annex['iou_part'] > annex['iou_outline']

    def test_a_single_osm_building_covering_everything_is_not_split(self):
        outlines, parts, _ = _reported_building()
        osm = [{'id': 'w1', 'height': None, 'geom': box(0.02, 0.02, 0.98, 2.48)}]
        rows = analyze.collect_rows(outlines, parts, osm)
        assert len(rows) == 1
        assert rows[0]['n_osm'] == 1
        assert rows[0]['coverage'] > 0.8


class TestSummarize:
    def test_counts_the_annex_as_missed_and_reachable(self):
        outlines, parts, osm = _reported_building()
        rows = analyze.collect_rows(outlines, parts, osm)
        s = analyze.summarize(rows)
        assert s['missed'] == 1
        assert s['missed_reachable_by_part'] == 1

    def test_an_osm_building_that_already_has_a_height_is_not_missed(self):
        outlines, parts, osm = _reported_building()
        osm[1]['height'] = '5.5'
        rows = analyze.collect_rows(outlines, parts, osm)
        s = analyze.summarize(rows)
        assert s['missed'] == 0

    def test_counts_the_current_value_as_too_high(self):
        # 外形は 14.3、下屋は 5.5。いま値が入る先は高い棟なので、そこは食い違わない
        outlines, parts, osm = _reported_building()
        rows = analyze.collect_rows(outlines, parts, osm)
        s = analyze.summarize(rows)
        assert s['wrong_value'] == 0

    def test_a_current_match_on_the_low_wing_is_counted_as_too_high(self):
        # 外形の代表点が下屋の側に落ちる形にすると、下屋の way に 14.3 が入る
        tall = box(0.0, 0.0, 1.0, 0.4)
        annex = box(0.0, 0.4, 1.0, 2.5)
        outline = box(0.0, 0.0, 1.0, 2.5)
        outlines = [{'id': 1, 'height': 14.3, 'geom': outline}]
        parts = {1: [{'height': 14.3, 'geom': tall}, {'height': 5.5, 'geom': annex}]}
        osm = [
            {'id': 'w_tall', 'height': None, 'geom': box(0.02, 0.02, 0.98, 0.38)},
            {'id': 'w_annex', 'height': None, 'geom': box(0.02, 0.42, 0.98, 2.48)},
        ]
        rows = analyze.collect_rows(outlines, parts, osm)
        s = analyze.summarize(rows)
        assert s['wrong_value'] == 1


def _row(**kw):
    base = {
        'osm_id': 'w1', 'n_osm': 2, 'iou_outline': 0.30, 'iou_part': 0.90,
        'outline_height': 14.3, 'part_height': 5.5,
        'osm_has_height': False, 'is_current_match': False, 'coverage': 0.4,
    }
    base.update(kw)
    return base


class TestSweep:
    def test_counts_a_new_reach_when_the_part_wins(self):
        r = analyze.sweep([_row()], margin=0.20)
        assert r['gain'] == 1
        assert r['improved'] == 0
        assert r['regressed'] == 0

    def test_no_new_reach_when_the_margin_suppresses_the_part(self):
        r = analyze.sweep([_row(iou_outline=0.75, iou_part=0.80)], margin=0.20)
        assert r['gain'] == 0

    def test_a_split_building_flipping_to_the_part_is_an_improvement(self):
        r = analyze.sweep([_row(is_current_match=True)], margin=0.20)
        assert r['improved'] == 1
        assert r['regressed'] == 0

    def test_a_whole_building_flipping_to_the_part_is_a_regression(self):
        r = analyze.sweep([_row(is_current_match=True, n_osm=1, coverage=0.95)], margin=0.20)
        assert r['regressed'] == 1
        assert r['improved'] == 0

    def test_a_large_regression_is_counted_separately(self):
        r = analyze.sweep([_row(is_current_match=True, n_osm=1, coverage=0.95)], margin=0.20)
        assert r['regressed_2m'] == 1

    def test_a_small_regression_is_not_counted_as_large(self):
        row = _row(is_current_match=True, n_osm=1, coverage=0.95,
                   outline_height=7.1, part_height=7.0)
        r = analyze.sweep([row], margin=0.20)
        assert r['regressed'] == 1
        assert r['regressed_2m'] == 0

    def test_equal_heights_change_nothing(self):
        row = _row(is_current_match=True, n_osm=1, coverage=0.95,
                   outline_height=8.0, part_height=8.0)
        r = analyze.sweep([row], margin=0.20)
        assert r['regressed'] == 0
        assert r['improved'] == 0
