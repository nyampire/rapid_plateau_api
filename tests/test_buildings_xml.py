"""
osmfj_plateau_api.py の buildings エンドポイントと buildings_to_osm_xml のテスト

主要な機能なのにテストがなかったため新規追加。
"""

import sys
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient


# osmfj_plateau_api はモジュール読み込み時に DB 接続を試みるため、
# テスト用のモック connect を先に仕込む必要がある。
@pytest.fixture(scope='module', autouse=True)
def patch_psycopg2_before_import():
    """モジュールロード前に psycopg2.connect をモック化"""
    cursor = MagicMock()
    cursor.fetchone.return_value = ('3.4 USE_GEOS=1',)
    cursor.fetchall.return_value = [('plateau_buildings',), ('plateau_building_nodes',)]
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)

    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch('psycopg2.connect', return_value=conn):
        for mod in list(sys.modules.keys()):
            if mod.startswith('osmfj_plateau_api'):
                del sys.modules[mod]
        yield


@pytest.fixture
def client(patch_psycopg2_before_import):
    import osmfj_plateau_api
    return TestClient(osmfj_plateau_api.app)


@pytest.fixture
def api(patch_psycopg2_before_import):
    import osmfj_plateau_api
    return osmfj_plateau_api.api


# ----------------------------------------------------------------------
# bbox バリデーション
# ----------------------------------------------------------------------

class TestBuildingsEndpointBboxValidation:
    """`/api/mapwithai/buildings?bbox=...` のパラメータ検証"""

    @pytest.mark.parametrize('bbox', [
        # 不正なフォーマット
        '139.7,35.7,139.8',           # 座標3つ
        '139.7,35.7,139.8,35.8,extra', # 座標5つ
        'a,b,c,d',                    # 数字でない
        '',                           # 空
        # 範囲外
        '-181,35.7,-180,35.8',        # 経度 < -180
        '180,35.7,181,35.8',          # 経度 > 180
        '139.7,-91,139.8,-90',        # 緯度 < -90
        '139.7,90,139.8,91',          # 緯度 > 90
        # 順序逆転
        '139.8,35.7,139.7,35.8',      # min_lon > max_lon
        '139.7,35.8,139.8,35.7',      # min_lat > max_lat
        # min == max（無効）
        '139.7,35.7,139.7,35.8',
        '139.7,35.7,139.8,35.7',
    ])
    def test_invalid_bbox_returns_400(self, client, bbox):
        response = client.get(f'/api/mapwithai/buildings?bbox={bbox}')
        # 400 (Bad Request) を期待。実装によっては 422 もありえる
        assert response.status_code in (400, 422, 500), \
            f'bbox={bbox!r} should be rejected (got {response.status_code})'

    def test_valid_bbox_calls_get_buildings_in_bbox(self, client, api):
        """有効な bbox で get_buildings_in_bbox が呼ばれる"""
        with patch.object(api, 'get_buildings_in_bbox', return_value=[]) as mock_get:
            response = client.get('/api/mapwithai/buildings?bbox=139.7,35.7,139.8,35.8')
        assert response.status_code == 200
        mock_get.assert_called_once()
        args = mock_get.call_args[0]
        # 引数の順序: (min_lon, min_lat, max_lon, max_lat, limit, city)
        assert args[0] == 139.7
        assert args[1] == 35.7
        assert args[2] == 139.8
        assert args[3] == 35.8

    def test_limit_parameter_passed_through(self, client, api):
        with patch.object(api, 'get_buildings_in_bbox', return_value=[]) as mock_get:
            client.get('/api/mapwithai/buildings?bbox=139.7,35.7,139.8,35.8&limit=500')
        args = mock_get.call_args[0]
        assert 500 in args

    def test_use_intersects_default_true(self, client, api):
        with patch.object(api, 'get_buildings_in_bbox', return_value=[]) as mock_get:
            client.get('/api/mapwithai/buildings?bbox=139.7,35.7,139.8,35.8')
        kwargs = mock_get.call_args[1]
        assert kwargs.get('use_intersects') is True


# ----------------------------------------------------------------------
# 空データレスポンス
# ----------------------------------------------------------------------

class TestBuildingsEndpointEmptyResponse:
    def test_empty_returns_valid_osm_xml(self, client, api):
        """データなしの場合も有効なOSM XMLを返す"""
        with patch.object(api, 'get_buildings_in_bbox', return_value=[]):
            response = client.get('/api/mapwithai/buildings?bbox=139.7,35.7,139.8,35.8')
        assert response.status_code == 200
        assert response.headers['content-type'].startswith('application/xml')
        # XMLとしてパース可能
        root = ET.fromstring(response.text)
        assert root.tag == 'osm'
        assert root.get('version') == '0.6'
        # 子要素なし
        assert len(root) == 0

    def test_empty_response_has_cors_header(self, client, api):
        with patch.object(api, 'get_buildings_in_bbox', return_value=[]):
            response = client.get('/api/mapwithai/buildings?bbox=139.7,35.7,139.8,35.8')
        assert response.headers.get('access-control-allow-origin') == '*'


# ----------------------------------------------------------------------
# buildings_to_osm_xml の単体テスト
# ----------------------------------------------------------------------

def _make_building(building_id=1, nodes=None, **tags):
    """テスト用 building dict のヘルパー"""
    if nodes is None:
        nodes = [
            {'id': 10, 'lat': 35.7, 'lon': 139.7},
            {'id': 11, 'lat': 35.7, 'lon': 139.71},
            {'id': 12, 'lat': 35.71, 'lon': 139.71},
            {'id': 13, 'lat': 35.71, 'lon': 139.7},
        ]
    d = {'id': building_id, 'nodes': nodes}
    d.update(tags)
    return d


class TestBuildingsToOsmXml:
    def test_basic_polygon_produces_valid_xml(self, api):
        """4頂点の建物 → 有効な OSM XML"""
        building = _make_building(building_id=1)
        xml_str = api.buildings_to_osm_xml([building])

        root = ET.fromstring(xml_str)
        assert root.tag == 'osm'
        ways = root.findall('way')
        assert len(ways) == 1

    def test_node_ids_are_negative(self, api):
        """新規データの慣習: ID は負の値"""
        building = _make_building(building_id=42)
        xml_str = api.buildings_to_osm_xml([building])
        root = ET.fromstring(xml_str)

        way = root.find('way')
        assert way.get('id') == '-42'  # building_id を負にしたもの

        for node in root.findall('node'):
            node_id = int(node.get('id'))
            assert node_id < 0

    def test_too_few_nodes_skipped(self, api):
        """3頂点未満の建物は除外"""
        small = _make_building(
            building_id=1,
            nodes=[
                {'id': 1, 'lat': 35.7, 'lon': 139.7},
                {'id': 2, 'lat': 35.7, 'lon': 139.71},
            ],
        )
        xml_str = api.buildings_to_osm_xml([small])
        root = ET.fromstring(xml_str)
        assert root.findall('way') == []

    def test_three_node_polygon_kept(self, api):
        """ちょうど3頂点はOK（境界値）"""
        triangle = _make_building(
            building_id=1,
            nodes=[
                {'id': 1, 'lat': 35.7, 'lon': 139.7},
                {'id': 2, 'lat': 35.7, 'lon': 139.71},
                {'id': 3, 'lat': 35.71, 'lon': 139.705},
            ],
        )
        xml_str = api.buildings_to_osm_xml([triangle])
        root = ET.fromstring(xml_str)
        assert len(root.findall('way')) == 1

    def test_invalid_coordinates_filtered(self, api):
        """範囲外の座標は除外。残りの有効ノードが3未満なら building も除外"""
        building = _make_building(
            building_id=1,
            nodes=[
                {'id': 1, 'lat': 35.7, 'lon': 139.7},
                {'id': 2, 'lat': 999, 'lon': 139.71},      # 緯度範囲外
                {'id': 3, 'lat': 35.71, 'lon': 999},        # 経度範囲外
                {'id': 4, 'lat': 35.71, 'lon': 139.7},
            ],
        )
        xml_str = api.buildings_to_osm_xml([building])
        root = ET.fromstring(xml_str)
        # 残りの有効ノードが 2件のみ → building 除外
        assert root.findall('way') == []

    def test_none_nodes_skipped(self, api):
        """nodes が None や空のものはスキップ"""
        b1 = {'id': 1, 'nodes': None}
        b2 = {'id': 2, 'nodes': []}
        b3 = {'id': 3, 'nodes': [None, None]}
        xml_str = api.buildings_to_osm_xml([b1, b2, b3])
        root = ET.fromstring(xml_str)
        assert root.findall('way') == []

    def test_closed_polygon_first_last_deduplicated(self, api):
        """最初と最後のノードが同じ座標なら最後を削除（自動閉鎖前提）"""
        building = _make_building(
            building_id=1,
            nodes=[
                {'id': 1, 'lat': 35.70, 'lon': 139.70},
                {'id': 2, 'lat': 35.70, 'lon': 139.71},
                {'id': 3, 'lat': 35.71, 'lon': 139.71},
                {'id': 4, 'lat': 35.71, 'lon': 139.70},
                {'id': 5, 'lat': 35.70, 'lon': 139.70},  # 最初と同じ
            ],
        )
        xml_str = api.buildings_to_osm_xml([building])
        root = ET.fromstring(xml_str)
        # 結果: 4ノード + 5番目の <nd> 参照(閉鎖) = nd 5個
        way = root.find('way')
        nds = way.findall('nd')
        assert len(nds) == 5  # 4頂点 + 閉じる nd
        # node 要素は 4 個
        assert len(root.findall('node')) == 4

    def test_tags_added_for_present_attributes(self, api):
        """height, building などのタグが追加される"""
        building = _make_building(
            building_id=1,
            building='residential',
            height=10.5,
            name='Test Building',
        )
        xml_str = api.buildings_to_osm_xml([building])
        root = ET.fromstring(xml_str)
        way = root.find('way')
        tags = {t.get('k'): t.get('v') for t in way.findall('tag')}
        assert tags.get('building') == 'residential'
        assert tags.get('height') == '10.5'

    def test_empty_input_returns_empty_osm(self, api):
        """空配列 → 空の osm 要素"""
        xml_str = api.buildings_to_osm_xml([])
        root = ET.fromstring(xml_str)
        assert root.tag == 'osm'
        assert len(root) == 0

    def test_multiple_buildings_distinct_way_ids(self, api):
        """複数の building には異なる way ID が振られる"""
        b1 = _make_building(building_id=1)
        b2 = _make_building(building_id=2)
        xml_str = api.buildings_to_osm_xml([b1, b2])
        root = ET.fromstring(xml_str)
        ways = root.findall('way')
        ids = [w.get('id') for w in ways]
        assert len(ids) == 2
        assert len(set(ids)) == 2  # 重複なし

    def test_endpoint_strips_invalid_control_chars(self, client, api):
        """
        エンドポイント側で XML不正制御文字を除去している
        （buildings_to_osm_xml 自体は除去しないが、ハンドラ層で re.sub する）
        """
        import re
        building = _make_building(building_id=1, name='Normal\x00Name')
        with patch.object(api, 'get_buildings_in_bbox', return_value=[building]):
            response = client.get('/api/mapwithai/buildings?bbox=139.7,35.7,139.8,35.8')
        assert response.status_code == 200
        # レスポンスXMLから制御文字（タブ・改行・キャリッジリターン除く）が除かれている
        body = response.text
        assert not re.search(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', body)


def _make_part(part_id, parent_id, **tags):
    """テスト用 part dict のヘルパー。building_part='yes' と parent_building_id を設定。"""
    nodes = [
        {'id': 100 + part_id, 'lat': 35.705, 'lon': 139.705},
        {'id': 101 + part_id, 'lat': 35.705, 'lon': 139.706},
        {'id': 102 + part_id, 'lat': 35.706, 'lon': 139.706},
        {'id': 103 + part_id, 'lat': 35.706, 'lon': 139.705},
    ]
    d = {
        'id': part_id,
        'nodes': nodes,
        'building_part': 'yes',
        'parent_building_id': parent_id,
    }
    d.update(tags)
    return d


class TestBuildingsToOsmXmlRelations:
    """Phase 2: building:part way と type=building relation の生成テスト"""

    def test_part_emits_building_part_tag_not_building(self, api):
        """part 単体: way に building:part=yes が乗り、building タグは出ない"""
        part = _make_part(part_id=10, parent_id=None, height=5.4, ele=3)
        xml_str = api.buildings_to_osm_xml([part])
        root = ET.fromstring(xml_str)
        way = root.find('way')
        tags = {t.get('k'): t.get('v') for t in way.findall('tag')}
        assert tags.get('building:part') == 'yes'
        assert 'building' not in tags
        assert tags.get('height') == '5.4'
        assert tags.get('ele') == '3'

    def test_orphan_part_emits_no_relation(self, api):
        """parent_building_id=None の part は way のみ、relation 出力なし"""
        orphan = _make_part(part_id=20, parent_id=None, height=3)
        xml_str = api.buildings_to_osm_xml([orphan])
        root = ET.fromstring(xml_str)
        assert len(root.findall('way')) == 1
        assert len(root.findall('relation')) == 0

    def test_simple_building_no_relation(self, api):
        """普通の building (parts 無し) は way のみ、relation 出力なし"""
        b = _make_building(building_id=1, building='yes', height=7)
        xml_str = api.buildings_to_osm_xml([b])
        root = ET.fromstring(xml_str)
        assert len(root.findall('way')) == 1
        assert len(root.findall('relation')) == 0

    def test_outline_with_parts_generates_relation(self, api):
        """outline + part(s) が同じバッチに含まれていれば relation が生成される"""
        outline = _make_building(building_id=1, building='yes', height=8.4, ele=2.7)
        p1 = _make_part(part_id=2, parent_id=1, height=5.4, ele=3)
        p2 = _make_part(part_id=3, parent_id=1, height=6.1, ele=3)
        xml_str = api.buildings_to_osm_xml([outline, p1, p2])
        root = ET.fromstring(xml_str)

        # way 3つ (outline 1 + parts 2)
        assert len(root.findall('way')) == 3
        # relation 1つ
        rels = root.findall('relation')
        assert len(rels) == 1
        rel = rels[0]

        # relation の member 構成
        members = rel.findall('member')
        roles = [(m.get('type'), m.get('ref'), m.get('role')) for m in members]
        # outline メンバー
        assert ('way', '-1', 'outline') in roles
        # part メンバー
        assert ('way', '-2', 'part') in roles
        assert ('way', '-3', 'part') in roles

    def test_relation_tags_duplicate_outline_tags(self, api):
        """relation には type=building と outline のタグを duplicate"""
        outline = _make_building(building_id=1, building='yes', height=10, ele=4)
        p1 = _make_part(part_id=2, parent_id=1, height=8, ele=4)
        xml_str = api.buildings_to_osm_xml([outline, p1])
        root = ET.fromstring(xml_str)
        rel = root.find('relation')
        tags = {t.get('k'): t.get('v') for t in rel.findall('tag')}
        assert tags.get('type') == 'building'
        assert tags.get('building') == 'yes'
        assert tags.get('height') == '10'
        assert tags.get('ele') == '4'
        # building:part は relation には出ない (outline 由来なので)
        assert 'building:part' not in tags

    def test_relation_id_negative_and_distinct_from_ways(self, api):
        """relation の id は -1_000_000 - outline_db_id で way と衝突しない"""
        outline = _make_building(building_id=42)
        p = _make_part(part_id=100, parent_id=42)
        xml_str = api.buildings_to_osm_xml([outline, p])
        root = ET.fromstring(xml_str)
        rel = root.find('relation')
        rel_id = int(rel.get('id'))
        # way id は -outline_db_id, -part_db_id
        way_ids = {int(w.get('id')) for w in root.findall('way')}
        assert rel_id not in way_ids
        assert rel_id < -1_000_000  # オフセット適用済み

    def test_part_without_outline_in_batch_emits_part_only(self, api):
        """parent が同じバッチに含まれない場合、part は単独 way、relation 無し"""
        # outline (id=1) は含めず、part (id=2, parent_id=1) のみ
        p = _make_part(part_id=2, parent_id=1)
        xml_str = api.buildings_to_osm_xml([p])
        root = ET.fromstring(xml_str)
        assert len(root.findall('way')) == 1
        assert len(root.findall('relation')) == 0  # outline 未提供 → relation 組まない

    def test_multiple_outlines_each_with_parts(self, api):
        """複数の outline それぞれの parts は別 relation"""
        o1 = _make_building(building_id=1)
        p1 = _make_part(part_id=2, parent_id=1)
        o2 = _make_building(building_id=10)
        p2 = _make_part(part_id=11, parent_id=10)
        xml_str = api.buildings_to_osm_xml([o1, p1, o2, p2])
        root = ET.fromstring(xml_str)
        rels = root.findall('relation')
        assert len(rels) == 2
        # 各 relation の outline メンバーが正しい
        outline_refs = set()
        for r in rels:
            for m in r.findall('member'):
                if m.get('role') == 'outline':
                    outline_refs.add(m.get('ref'))
        assert outline_refs == {'-1', '-10'}
