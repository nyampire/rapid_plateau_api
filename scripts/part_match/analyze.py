#!/usr/bin/env python3
"""PLATEAU の建物と OSM の建物を突き合わせ、転記元の取り方を比べる。

`export_city.py` が書き出した CSV と、OSM の PBF から作った GeoJSON Text Sequence を
入力に取る。作り方は README を参照。

    python3 analyze.py --plateau plateau_39201.csv.gz --osm osm_39201.geojsonseq

判定の閾値は `rules.py` に集めてある。
"""

import argparse
import collections
import csv
import gzip
import json
import sys

import numpy as np
import shapely
from shapely import STRtree, from_wkb
from shapely.geometry import shape

import rules

csv.field_size_limit(10 ** 9)

# 高さが「違う」と見なす下限 (m)。丸めの差を拾わないための幅
HEIGHT_EPS = 0.05
# 大きくずれた件数を別に数える下限 (m)
HEIGHT_LARGE = 2.0


def load_plateau(path):
    """外形と part を読む。part は親の id ごとにまとめる。"""
    outlines = []
    parts = collections.defaultdict(list)
    with gzip.open(path, 'rt') as fh:
        for row in csv.DictReader(fh):
            geom = from_wkb(bytes.fromhex(row['wkb']))
            height = float(row['height']) if row['height'] else None
            if row['building_part'] == 'yes':
                parent = row['parent_building_id']
                if parent:
                    parts[int(parent)].append({'height': height, 'geom': geom})
            else:
                outlines.append({'id': int(row['id']), 'height': height, 'geom': geom})
    return outlines, parts


def load_osm(path):
    """OSM の建物を読む。

    `osmium export` は閉じた way を LineString と (Multi)Polygon の 2 つで出す。
    面のほうだけを取る。id は `a<way_id*2>` の形で来るので way の id に戻す。
    """
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip().lstrip('\x1e')
            if not line:
                continue
            feature = json.loads(line)
            geom = feature.get('geometry') or {}
            if geom.get('type') not in ('Polygon', 'MultiPolygon'):
                continue
            tags = feature.get('properties') or {}
            building = tags.get('building')
            if not building or building == 'no':
                continue
            out.append({'id': _way_id(feature.get('id')),
                        'height': tags.get('height'),
                        'geom': shape(geom)})
    return out


def _way_id(feature_id):
    if isinstance(feature_id, str) and feature_id.startswith('a'):
        n = int(feature_id[1:])
        return f'w{n // 2}' if n % 2 == 0 else f'r{n // 2}'
    return feature_id


def _iou(a, b):
    try:
        inter = a.intersection(b).area
    except Exception:
        return 0.0
    if inter == 0:
        return 0.0
    return inter / (a.area + b.area - inter)


def collect_rows(outlines, parts, osm):
    """part を持つ外形の下にある OSM 建物ごとに 1 行を作る。

    行が持つのは、その OSM がいま候補になっているか、外形と part のどちらに形が合うか、
    そして双方の高さである。判定はここでは行わない。
    """
    if not osm:
        return []
    osm_geoms = np.array([o['geom'] for o in osm], dtype=object)
    tree = STRtree(osm_geoms)
    osm_rp = shapely.point_on_surface(osm_geoms)

    rows = []
    for outline in outlines:
        outline_parts = parts.get(outline['id'])
        if not outline_parts:
            continue
        geom = outline['geom']
        # 代表点が外形の中にある OSM 建物。OSM がこの建物をいくつに分けているかを表す
        nearby = tree.query(geom, predicate='intersects')
        inside = [int(i) for i in nearby if geom.covers(osm_rp[int(i)])]
        if not inside:
            continue

        # いまの照合: 外形の代表点を含む OSM がちょうど 1 つのときだけ候補になる
        hits = [int(i) for i in tree.query(shapely.point_on_surface(geom), predicate='within')]
        current = None
        if len(hits) == 1 and rules.area_gate(_ratio(geom, osm[hits[0]]['geom'])) != 'skip':
            current = hits[0]

        for i in inside:
            candidate = osm[i]
            best = max(outline_parts, key=lambda p: _iou(candidate['geom'], p['geom']))
            rows.append({
                'osm_id': candidate['id'],
                'n_osm': len(inside),
                'iou_outline': _iou(candidate['geom'], geom),
                'iou_part': _iou(candidate['geom'], best['geom']),
                'outline_height': outline['height'],
                'part_height': best['height'],
                'osm_has_height': bool(candidate['height']),
                'is_current_match': i == current,
                'coverage': _coverage(candidate['geom'], geom),
            })
    return rows


def _ratio(outline_geom, osm_geom):
    return outline_geom.area / osm_geom.area if osm_geom.area else 0.0


def _coverage(osm_geom, outline_geom):
    if not outline_geom.area:
        return 0.0
    try:
        return osm_geom.intersection(outline_geom).area / outline_geom.area
    except Exception:
        return 0.0


def _height_gap(row):
    """外形と part の高さの差。どちらかが無ければ None。"""
    if row['outline_height'] is None or row['part_height'] is None:
        return None
    return row['outline_height'] - row['part_height']


def summarize(rows):
    """いまの実装で何が起きているかを数える。規則の比較は sweep で行う。"""
    split = [r for r in rows if r['n_osm'] >= 2]
    missed = [r for r in split if not r['is_current_match'] and not r['osm_has_height']]
    reachable = [r for r in missed
                 if rules.choose_source(r['iou_outline'], r['iou_part']) == 'part']
    wrong = [r for r in split if r['is_current_match']
             and (_height_gap(r) or 0) > HEIGHT_EPS]
    return {
        'osm_under_parted_outlines': len(rows),
        'split_rows': len(split),
        'missed': len(missed),
        'missed_reachable_by_part': len(reachable),
        'wrong_value': len(wrong),
        'wrong_value_large': sum(1 for r in wrong if (_height_gap(r) or 0) >= HEIGHT_LARGE),
    }


def sweep(rows, margin):
    """ある差の門で規則を切り替えたときの増減を数える。

    いま候補でない OSM が part で拾えるようになるものを gain、
    いま値が入っている OSM の値が変わるものを、その OSM が何を写しているかで
    improved と regressed に分ける。
    """
    result = {'gain': 0, 'improved': 0, 'regressed': 0, 'regressed_2m': 0}
    for row in rows:
        pick = rules.choose_source(row['iou_outline'], row['iou_part'], margin)
        if pick != 'part':
            continue
        if not row['is_current_match']:
            if not row['osm_has_height']:
                result['gain'] += 1
            continue
        gap = _height_gap(row)
        if gap is None or abs(gap) <= HEIGHT_EPS:
            continue
        whole = row['n_osm'] == 1 and rules.classify_osm_role(row['coverage']) == 'whole'
        if whole:
            result['regressed'] += 1
            if abs(gap) >= HEIGHT_LARGE:
                result['regressed_2m'] += 1
        else:
            result['improved'] += 1
    return result


MARGINS = (0.0, 0.05, 0.10, 0.20, 0.30)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('--plateau', required=True, help='export_city.py が書いた CSV')
    p.add_argument('--osm', required=True, help='osmium export が書いた GeoJSON Text Sequence')
    args = p.parse_args(argv)

    outlines, parts = load_plateau(args.plateau)
    osm = load_osm(args.osm)
    rows = collect_rows(outlines, parts, osm)
    s = summarize(rows)

    print(f'PLATEAU 外形 {len(outlines):,} / part を持つ外形 {len(parts):,}')
    print(f'OSM 建物 {len(osm):,}')
    print(f'part を持つ外形の下にある OSM 建物 {s["osm_under_parted_outlines"]:,}'
          f' (うち OSM が 2 棟以上に分けているもの {s["split_rows"]:,})')
    print(f'いま候補にならず height も無い OSM {s["missed"]:,}'
          f' (part で拾えるもの {s["missed_reachable_by_part"]:,})')
    print(f'いま高すぎる値が入る OSM {s["wrong_value"]:,}'
          f' (2m 以上 {s["wrong_value_large"]:,})')
    print()
    print('差     新たに拾える  高すぎた値が直る  正しかった値が変わる  うち 2m 以上')
    for m in MARGINS:
        r = sweep(rows, m)
        print(f'{m:4.2f}  {r["gain"]:>11,}  {r["improved"]:>15,}'
              f'  {r["regressed"]:>19,}  {r["regressed_2m"]:>12,}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
