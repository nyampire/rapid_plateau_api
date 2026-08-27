#!/usr/bin/env python3
"""無壁舎 (bldg:class 3003/3004) の建物を `building=roof` にする。

    python3 apply_walless_roof.py <作業ディレクトリ>

変換の直後、`manifest.txt` を作る前に走らせる。
区分は元データ (`.gml`) にしかなく、変換器は出力しない。
`.gml` と `.osm` が同じディレクトリに並ぶのはこの時点だけである。

`uro:buildingID` と `.osm` の `ref:MLIT_PLATEAU` を鍵に突き合わせる。
"""
import json
import os
import sys
import xml.etree.ElementTree as ET

# 標準製品仕様書 表 4-28 の Building_class。9999 と Null はこの表に無い。
WALLLESS = {'3003', '3004'}
KNOWN_CLASSES = {'3000', '3001', '3002', '3003', '3004'}


def _local(tag):
    """名前空間を落として要素名だけ返す。"""
    return tag.rsplit('}', 1)[-1]


def parse_gml_classes(path):
    """建物 ID から区分コードへの辞書と、区分を 1 つでも見たかを返す。

    uro の名前空間 URI はデータセットごとに違う (1.4 / 3.1 / 3.2 を実データで
    確認) ので、URI では引かず要素名で引く。
    `bldg:class` だけは名前空間で絞る。他のスキーマにも `class` があるため。
    """
    classes = {}
    saw_class = False
    cur = None
    for event, elem in ET.iterparse(path, events=('start', 'end')):
        name = _local(elem.tag)
        if event == 'start':
            if name == 'Building':
                cur = {'id': None, 'class': None}
            continue
        if cur is not None:
            if name == 'class' and '/building/' in elem.tag and cur['class'] is None:
                cur['class'] = (elem.text or '').strip()
                saw_class = True
            elif name == 'buildingID' and cur['id'] is None:
                cur['id'] = (elem.text or '').strip()
        if name == 'Building':
            if cur and cur['id'] and cur['class']:
                classes[cur['id']] = cur['class']
            cur = None
            elem.clear()
    return classes, saw_class


def _atomic_write(tree, path):
    """書き終えてから名前を付ける。切り詰めた .osm を残さない。"""
    part = path + '.part'
    tree.write(part, encoding='utf-8', xml_declaration=True)
    os.replace(part, path)


def rewrite_osm(path, classes):
    """無壁舎の way を `roof` にして書き戻す。要約を返す。

    `building` と `building:part` の両方を見る。取り込み器は `building` を
    優先し、無ければ `building:part` を採る。融合は取り込まれる側の way の
    キーを `building:part` へ降格させるので、無壁舎の過半は降格側に載る。
    """
    stat = {'buildings': 0, 'joined': 0, 'rewritten': 0,
            'unknown_code': 0, 'no_class': 0, 'unjoinable': 0}
    tree = ET.parse(path)
    root = tree.getroot()
    changed = False
    for way in root.findall('way'):
        tags = {t.get('k'): t for t in way.findall('tag')}
        if 'building' in tags:
            key = 'building'
        elif 'building:part' in tags:
            key = 'building:part'
        else:
            continue
        stat['buildings'] += 1
        ref = tags.get('ref:MLIT_PLATEAU')
        if ref is None:
            # 融合が作る合成形状。取り込み器も取り込まないので扱わない。
            stat['unjoinable'] += 1
            continue
        code = classes.get(ref.get('v'))
        if code is None:
            stat['no_class'] += 1
            continue
        stat['joined'] += 1
        if code in WALLLESS:
            tags[key].set('v', 'roof')
            stat['rewritten'] += 1
            changed = True
        elif code not in KNOWN_CLASSES:
            stat['unknown_code'] += 1
    if changed:
        _atomic_write(tree, path)
    return stat


def apply_dir(work_dir):
    """作業ディレクトリの `.osm` を全部処理して合計を返す。"""
    total = {'meshes': 0, 'meshes_without_class': 0, 'buildings': 0,
             'joined': 0, 'rewritten': 0, 'unknown_code': 0,
             'no_class': 0, 'unjoinable': 0}
    names = sorted(n for n in os.listdir(work_dir) if n.endswith('.osm'))
    for name in names:
        osm = os.path.join(work_dir, name)
        gml = osm[:-4] + '.gml'
        # 対の .gml が無いと区分を引けない。黙って素通りさせると、
        # 直っていないことに気づけないまま転送まで進む。
        if not os.path.exists(gml):
            raise SystemExit('対の .gml が無い: %s' % os.path.basename(gml))
        classes, saw_class = parse_gml_classes(gml)
        if not saw_class:
            total['meshes_without_class'] += 1
        stat = rewrite_osm(osm, classes)
        total['meshes'] += 1
        for k, v in stat.items():
            total[k] += v
    return total


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    print(json.dumps(apply_dir(sys.argv[1]), ensure_ascii=False))
