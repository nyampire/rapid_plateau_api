"""転記元を外形と part のどちらに取るかを決める規則。

幾何の計算を持たない純粋な判定だけを置く。
Rapid 側で同じ規則を実装するときの参照にもなるよう、閾値をここに集めてある。
"""

# 現在の照合が使っている面積比の門 (Rapid の HeightTransferMatcher と同じ値)
AREA_RATIO_MIN = 0.5
AREA_RATIO_MAX = 2.0

# 転記元を part に切り替えるために必要な、形の一致の差
DEFAULT_MARGIN = 0.20

# OSM の 1 つの way が建物全体を写していると見なす、外形の被覆率
DEFAULT_WHOLE_THRESHOLD = 0.80


def choose_source(iou_outline, iou_part, margin=DEFAULT_MARGIN):
    """転記元を返す。

    part が外形より `margin` を超えてよく合うときだけ 'part' を返す。
    差がちょうど `margin` のときは 'outline' を返す。境目では現状を変えない。

    Args:
        iou_outline: OSM 建物と PLATEAU 外形の形の一致 (0 から 1)
        iou_part:    OSM 建物と最もよく合う part の形の一致 (0 から 1)
        margin:      part に切り替えるために必要な差

    Returns:
        'part' または 'outline'
    """
    if margin < 0:
        raise ValueError(f'margin は 0 以上である必要があります: {margin}')
    return 'part' if iou_part > iou_outline + margin else 'outline'


def area_gate(ratio):
    """面積比が現在の門をどう通るかを返す。

    Args:
        ratio: 外形の面積 / OSM 建物の面積

    Returns:
        'skip'           候補にしない (外形が OSM よりずっと小さい)
        'area_mismatch'  注記を付けて候補にする (外形が OSM よりずっと大きい)
        'ok'             そのまま候補にする
    """
    if ratio < AREA_RATIO_MIN:
        return 'skip'
    if ratio > AREA_RATIO_MAX:
        return 'area_mismatch'
    return 'ok'


def classify_osm_role(coverage, threshold=DEFAULT_WHOLE_THRESHOLD):
    """OSM の 1 つの way が何を写しているかを返す。

    Args:
        coverage:  その way が PLATEAU 外形を覆う割合 (0 から 1)
        threshold: 建物全体と見なす下限。これ自体は含まない

    Returns:
        'whole'  建物全体を 1 つの way で写している
        'wing'   棟 1 つだけが OSM にある
    """
    return 'whole' if coverage > threshold else 'wing'
