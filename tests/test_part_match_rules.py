"""転記元を外形と part のどちらに取るかを決める規則のテスト。

規則そのものは `scripts/part_match/rules.py` に置き、幾何の計算から切り離してある。
境目の値を跨ぐ 2 通りの数字を、通る側と落ちる側の両方で確かめる。
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'scripts' / 'part_match'))

import rules  # noqa: E402


class TestChooseSource:
    """part が外形より一定以上よく合うときだけ part を採る。"""

    def test_part_wins_when_clearly_better(self):
        assert rules.choose_source(iou_outline=0.30, iou_part=0.91, margin=0.20) == 'part'

    def test_part_wins_with_a_different_pair_of_values(self):
        assert rules.choose_source(iou_outline=0.24, iou_part=0.51, margin=0.20) == 'part'

    def test_outline_wins_when_part_is_better_but_inside_the_margin(self):
        assert rules.choose_source(iou_outline=0.73, iou_part=0.75, margin=0.20) == 'outline'

    def test_outline_wins_when_the_gap_equals_the_margin(self):
        # 差がちょうど margin のときは外形を採る。境目では現状維持を選ぶ
        assert rules.choose_source(iou_outline=0.60, iou_part=0.80, margin=0.20) == 'outline'

    def test_outline_wins_when_the_outline_fits_better(self):
        assert rules.choose_source(iou_outline=0.72, iou_part=0.35, margin=0.20) == 'outline'

    def test_margin_zero_lets_any_better_part_win(self):
        assert rules.choose_source(iou_outline=0.70, iou_part=0.71, margin=0.0) == 'part'

    def test_margin_zero_still_keeps_the_outline_on_a_tie(self):
        assert rules.choose_source(iou_outline=0.70, iou_part=0.70, margin=0.0) == 'outline'

    def test_negative_margin_is_rejected(self):
        with pytest.raises(ValueError):
            rules.choose_source(iou_outline=0.30, iou_part=0.91, margin=-0.1)


class TestAreaGate:
    """現在の照合が使っている面積比の門を写したもの。

    比は 外形の面積 / OSM 建物の面積 で、0.5 未満は候補にせず、
    2.0 超は注記を付けたうえで候補にする。
    """

    def test_ratio_near_one_passes(self):
        assert rules.area_gate(1.37) == 'ok'

    def test_another_ratio_near_one_passes(self):
        assert rules.area_gate(0.83) == 'ok'

    def test_far_smaller_outline_is_dropped(self):
        assert rules.area_gate(0.40) == 'skip'

    def test_lower_bound_itself_passes(self):
        assert rules.area_gate(0.50) == 'ok'

    def test_far_larger_outline_is_flagged(self):
        assert rules.area_gate(2.50) == 'area_mismatch'

    def test_upper_bound_itself_passes(self):
        assert rules.area_gate(2.00) == 'ok'

    def test_zero_area_is_dropped(self):
        assert rules.area_gate(0.0) == 'skip'


class TestClassifyOsmRole:
    """OSM の 1 つの way が建物全体を写しているのか、棟 1 つだけなのかを分ける。

    外形をどれだけ覆うかで決める。この境目は計測の都合で置いたもので、
    結果がここに依存することを README に書いてある。
    """

    def test_covering_most_of_the_outline_is_the_whole_building(self):
        assert rules.classify_osm_role(0.95) == 'whole'

    def test_another_high_coverage_is_the_whole_building(self):
        assert rules.classify_osm_role(0.87) == 'whole'

    def test_covering_a_fraction_is_a_single_wing(self):
        assert rules.classify_osm_role(0.42) == 'wing'

    def test_another_low_coverage_is_a_single_wing(self):
        assert rules.classify_osm_role(0.18) == 'wing'

    def test_the_threshold_itself_counts_as_a_wing(self):
        assert rules.classify_osm_role(0.80) == 'wing'

    def test_the_threshold_can_be_overridden(self):
        assert rules.classify_osm_role(0.42, threshold=0.30) == 'whole'
