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
