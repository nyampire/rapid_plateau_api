"""無壁舎 (bldg:class 3003/3004) を building=roof にする後処理を固定する。

区分は元データにしか無く、変換器は出力しない。
`.gml` と `.osm` が並んでいる変換直後にしか当てられない。

通信はしない。すべてローカルの一時ファイルで組む。
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

import apply_walless_roof

REPO = Path(__file__).resolve().parent.parent
TOOL = REPO / 'scripts' / 'reimport' / 'apply_walless_roof.py'

# uro の名前空間はデータセットごとに違う。実データで 3 つ確認している。
URO_31 = 'https://www.geospatial.jp/iur/uro/3.1'
URO_14 = 'http://www.kantei.go.jp/jp/singi/tiiki/toshisaisei/itoshisaisei/iur/uro/1.4'


def _gml(buildings, uro_ns=URO_31):
    """buildings は (建物ID, 区分) の並び。区分が None なら class 要素を出さない。

    建物 ID を出さない建物は (None, 区分) で表す。
    """
    parts = []
    for bid, cls in buildings:
        body = ''
        if cls is not None:
            body += ('<bldg:class codeSpace="../../codelists/Building_class.xml">'
                     '%s</bldg:class>' % cls)
        if bid is not None:
            body += ('<uro:buildingIDAttribute><uro:BuildingIDAttribute>'
                     '<uro:buildingID>%s</uro:buildingID>'
                     '</uro:BuildingIDAttribute></uro:buildingIDAttribute>' % bid)
        parts.append('<core:cityObjectMember><bldg:Building gml:id="x">'
                     '%s</bldg:Building></core:cityObjectMember>' % body)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<core:CityModel xmlns:core="http://www.opengis.net/citygml/2.0" '
        'xmlns:bldg="http://www.opengis.net/citygml/building/2.0" '
        'xmlns:gml="http://www.opengis.net/gml" '
        'xmlns:gen="http://www.opengis.net/citygml/generics/2.0" '
        'xmlns:uro="%s">%s</core:CityModel>' % (uro_ns, ''.join(parts))
    )


def test_parse_gml_classes_reads_id_and_class(tmp_path):
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('27203-bldg-1', '3003'), ('27203-bldg-2', '3001')]),
                 encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {'27203-bldg-1': '3003', '27203-bldg-2': '3001'}
    assert saw is True


def test_parse_gml_classes_survives_other_uro_namespace(tmp_path):
    """uro の URI は 1.4 / 3.1 / 3.2 と版で変わる。URI で引くと空になる。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('53391550-bldg-1', '3004')], uro_ns=URO_14),
                 encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {'53391550-bldg-1': '3004'}
    assert saw is True


def test_parse_gml_classes_skips_building_without_class(tmp_path):
    """区分の無い建物は辞書に載せない。載せると 9999 と見分けが付かない。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('27209-bldg-1', None)]), encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {}
    assert saw is False


def test_parse_gml_classes_keeps_out_of_codelist_value(tmp_path):
    """9999 も Null も表に無いが、判定は呼び出し側でする。ここでは落とさない。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('27203-bldg-9', '9999'), ('27203-bldg-8', 'Null')]),
                 encoding='utf-8')
    classes, _ = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {'27203-bldg-9': '9999', '27203-bldg-8': 'Null'}


def test_parse_gml_classes_ignores_building_without_id(tmp_path):
    """建物 ID を持たない建物は突き合わせようがない。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([(None, '3003')]), encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {}
    assert saw is True
