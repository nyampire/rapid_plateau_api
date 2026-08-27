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


def _osm(ways):
    """ways は (way id, タグの辞書) の並び。"""
    parts = []
    for wid, tags in ways:
        t = ''.join('<tag k="%s" v="%s"/>' % (k, v) for k, v in tags.items())
        parts.append('<way id="%s">%s</way>' % (wid, t))
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<osm version="0.6" generator="test">%s</osm>' % ''.join(parts))


def _tags_of(path, way_id):
    import xml.etree.ElementTree as ET
    root = ET.parse(path).getroot()
    for w in root.findall('way'):
        if w.get('id') == way_id:
            return {t.get('k'): t.get('v') for t in w.findall('tag')}
    raise AssertionError('way %s が無い' % way_id)


def test_rewrite_osm_rewrites_building_key(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-1', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    assert _tags_of(p, '-1')['building'] == 'roof'
    assert stat['rewritten'] == 1


def test_rewrite_osm_rewrites_demoted_key(tmp_path):
    """融合で building:part に降格した無壁舎も書き換える。

    豊中市 50 メッシュでは無壁舎 26,842 本のうち 15,507 本が降格側だった。
    building だけを見る実装は、ここで 58% を取りこぼす。
    """
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-2', {'building:part': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-2'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {'c-bldg-2': '3004'})
    assert _tags_of(p, '-2')['building:part'] == 'roof'
    assert stat['rewritten'] == 1


def test_rewrite_osm_leaves_ordinary_buildings(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-3', {'building': 'house',
                               'ref:MLIT_PLATEAU': 'c-bldg-3'}),
                       ('-4', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-4'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(
        str(p), {'c-bldg-3': '3001', 'c-bldg-4': '3002'})
    assert _tags_of(p, '-3')['building'] == 'house'
    assert _tags_of(p, '-4')['building'] == 'yes'
    assert stat['rewritten'] == 0
    assert stat['joined'] == 2


def test_rewrite_osm_counts_out_of_codelist_code(tmp_path):
    """9999 と Null は表に無い。書き換えず、件数だけ数える。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-5', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-5'}),
                       ('-6', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-6'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(
        str(p), {'c-bldg-5': '9999', 'c-bldg-6': 'Null'})
    assert _tags_of(p, '-5')['building'] == 'yes'
    assert stat['rewritten'] == 0
    assert stat['unknown_code'] == 2


def test_rewrite_osm_skips_way_without_identifier(tmp_path):
    """識別子の無い way は融合の合成形状で、取り込み器も取り込まない。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-7', {'building': 'yes'})]), encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {})
    assert _tags_of(p, '-7')['building'] == 'yes'
    assert stat['unjoinable'] == 1
    assert stat['joined'] == 0


def test_rewrite_osm_counts_building_without_class(tmp_path):
    """区分を持たない都市がある。突き合わせは通るが直らない。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-8', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-8'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {})
    assert stat['no_class'] == 1
    assert stat['joined'] == 0


def test_rewrite_osm_leaves_non_building_ways_alone(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-9', {'highway': 'residential'})]), encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {})
    assert stat['buildings'] == 0


def test_rewrite_osm_is_idempotent(tmp_path):
    """再実行しても区分は変わらないので結果も変わらない。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-1', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
                 encoding='utf-8')
    apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    stat = apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    assert _tags_of(p, '-1')['building'] == 'roof'
    assert stat['rewritten'] == 1


def test_rewrite_osm_leaves_no_part_file(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-1', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
                 encoding='utf-8')
    apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    assert not (tmp_path / 'a.osm.part').exists()
