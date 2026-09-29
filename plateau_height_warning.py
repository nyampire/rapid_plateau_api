#!/usr/bin/env python3
"""PLATEAU の建物の高さが怪しいかを、API の応答 1 件ごとに判定します。

判定の条件の値は、plateau_height_outliers.py の DEFAULT_ 定数を使います。
調査と警告で閾値がずれないようにするためです。

調査は SQL で全件を数えます。
ここでは、API が取り出した行を Python で判定します。

警告にするのは、調査の検査のうち誤りの可能性が高いものだけです。
degenerate-area と part-over-outline はすべて警告にします。
needle は、部分立体を除いて警告にします。
absolute は、高さが低すぎる側だけを警告にします。
floor-height は、住宅だけを警告にします。
"""

from typing import Dict, Iterable, List, Optional

from plateau_height_outliers import (
    CHECKS,
    DEFAULT_ABSOLUTE_MIN_M,
    DEFAULT_FLOOR_HEIGHT_MAX_M,
    DEFAULT_FLOOR_HEIGHT_MIN_M,
    DEFAULT_NEEDLE_AREA_M2,
    DEFAULT_NEEDLE_HEIGHT_M,
    DEFAULT_PART_TOLERANCE_M,
)

# floor-height を警告にする building の値です。
# 工場や倉庫は 1 階が高いことが多く、正常な建物に警告が出てしまうため外します。
RESIDENTIAL_BUILDING_VALUES = frozenset({'house', 'apartments', 'residential'})


def _num(value) -> Optional[float]:
    """数として読めない値を None にします。"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def height_warnings(row: Dict, outline: Optional[Dict] = None) -> List[str]:
    """1 件の建物について、該当した検査の名前を返します。

    row は get_buildings_in_bbox が返す行です。
    planar_area は平面での面積、footprint_m2 は平方メートルでの底面積です。
    outline は、row が部分立体のときの外形の行です。
    名前は CHECKS の並びで返します。
    """
    found = set()
    height = _num(row.get('height'))

    planar_area = _num(row.get('planar_area'))
    if planar_area is not None and planar_area <= 0:
        found.add('degenerate-area')

    if outline is not None:
        outline_height = _num(outline.get('height'))
        if (height is not None and outline_height is not None
                and height > outline_height + DEFAULT_PART_TOLERANCE_M):
            found.add('part-over-outline')

    # 部分立体は needle にしない。
    # 屋上の塔屋や階段室は地面からの高さが 15 m を超え、底面積が小さいため。
    footprint = _num(row.get('footprint_m2'))
    if (row.get('building_part') is None
            and height is not None and footprint is not None
            and height > DEFAULT_NEEDLE_HEIGHT_M
            and footprint < DEFAULT_NEEDLE_AREA_M2):
        found.add('needle')

    if height is not None and height < DEFAULT_ABSOLUTE_MIN_M:
        found.add('absolute')

    levels = _num(row.get('building_levels'))
    if (row.get('building') in RESIDENTIAL_BUILDING_VALUES
            and height is not None and levels is not None and levels > 0):
        per_floor = height / levels
        if (per_floor < DEFAULT_FLOOR_HEIGHT_MIN_M
                or per_floor > DEFAULT_FLOOR_HEIGHT_MAX_M):
            found.add('floor-height')

    return [name for name in CHECKS if name in found]


def annotate_height_warnings(rows: Iterable[Dict]) -> None:
    """各行に height_warnings を書き込みます。

    部分立体の外形は、同じ応答の行から探します。
    外形が応答に含まれない部分立体は、part-over-outline を判定しません。
    """
    rows = list(rows)
    by_id = {r.get('id'): r for r in rows}
    for r in rows:
        parent_id = r.get('parent_building_id')
        outline = by_id.get(parent_id) if parent_id is not None else None
        r['height_warnings'] = height_warnings(r, outline)
