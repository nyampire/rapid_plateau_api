"""
osmfj_plateau_api.py の buildings エンドポイントと buildings_to_osm_xml のテスト

主要な機能なのにテストがなかったため新規追加。
"""

import logging
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


def _josm_duplicate_node_coords(root):
    """Replicate JOSM's "Duplicated nodes" validator: group the emitted <node>
    elements by coordinate and return every coordinate carried by more than one
    node. The emitter writes lat/lon at 7 decimals (``f"{v:.7f}"``), which is
    OSM's storage precision — the same rounding JOSM keys duplicates on — so a
    plain string-equality group here flags exactly what JOSM would."""
    from collections import Counter
    counts = Counter((n.get('lat'), n.get('lon')) for n in root.findall('node'))
    return [coord for coord, n in counts.items() if n > 1]


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
        assert way.get('id') == str(-(42 * 1000))  # building_id * 1000 を負にしたもの

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

    def test_valid_start_date_is_emitted(self, api):
        """妥当な建設年 (YYYY) は start_date として出力される"""
        building = _make_building(building_id=1, start_date='2020')
        xml_str = api.buildings_to_osm_xml([building])
        root = ET.fromstring(xml_str)
        tags = {t.get('k'): t.get('v') for t in root.find('way').findall('tag')}
        assert tags.get('start_date') == '2020'

    def test_placeholder_start_date_0001_is_dropped(self, api):
        """欠損年のプレースホルダ '0001' は start_date として出力しない"""
        building = _make_building(building_id=1, start_date='0001')
        xml_str = api.buildings_to_osm_xml([building])
        root = ET.fromstring(xml_str)
        tags = {t.get('k'): t.get('v') for t in root.find('way').findall('tag')}
        assert 'start_date' not in tags

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


class TestGetBuildingsInBboxQuery:
    """get_buildings_in_bbox の SQL クエリ構造を検証 (Phase 2 拡張)"""

    def _setup_api_with_mock_cursor(self, api):
        """api.get_connection() が返す cursor をモックし、execute された SQL を捕捉"""
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = ('3.4',)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        # context manager protocol も
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=None)
        return conn, cursor

    def test_query_uses_ctes_for_outlines_parts_orphans(self, api):
        """SQL に bbox_outlines / related_parts / orphan_parts の CTE が含まれている"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        # 最後に execute された SQL を取得
        assert cursor.execute.called
        sql = cursor.execute.call_args[0][0]
        assert 'bbox_outlines' in sql
        assert 'related_parts' in sql
        assert 'orphan_parts' in sql

    def test_query_filters_outlines_with_building_part_is_null(self, api):
        """outline CTE は building_part IS NULL でフィルタする"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        sql = cursor.execute.call_args[0][0]
        assert 'building_part IS NULL' in sql

    def test_query_joins_parts_via_parent_building_id(self, api):
        """related_parts は parent_building_id 経由で結合"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        sql = cursor.execute.call_args[0][0]
        assert 'parent_building_id IN' in sql or 'parent_building_id in' in sql.lower()

    def test_query_selects_building_part_and_parent_columns(self, api):
        """SELECT に building_part / parent_building_id が含まれる"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        sql = cursor.execute.call_args[0][0]
        # ub.building_part, ub.parent_building_id 等の参照があること
        assert 'building_part' in sql
        assert 'parent_building_id' in sql

    def test_query_params_count_and_order(self, api):
        """params は 9 個 (bbox×2 + limit) で正しい順序"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        params = cursor.execute.call_args[0][1]
        # 9 個: bbox_outlines spatial(4) + LIMIT(1) + orphan_parts spatial(4)
        assert len(params) == 9
        # bbox_outlines: min_lon, min_lat, max_lon, max_lat
        assert params[0:4] == [139.7, 35.7, 139.8, 35.8]
        # limit
        assert params[4] == 10
        # orphan_parts: 同じ bbox を再度
        assert params[5:9] == [139.7, 35.7, 139.8, 35.8]

    def test_query_placeholder_count_matches_params(self, api):
        """SQL の %s プレースホルダ数 = params 数 (psycopg2 が hard fail する条件)"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        sql = cursor.execute.call_args[0][0]
        params = cursor.execute.call_args[0][1]
        placeholder_count = sql.count('%s')
        assert placeholder_count == len(params), (
            f"%s placeholders ({placeholder_count}) != params ({len(params)})"
        )

    def test_query_filters_buildings_outside_their_city_boundary(self, api):
        """Rapid#35: cross-city mesh duplicate 抑制のフィルタが SQL に含まれている"""
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        sql = cursor.execute.call_args[0][0]
        # boundary 照合用に dash_city_master と centroid を参照していること
        assert 'dash_city_master' in sql
        assert 'boundary_geom' in sql
        # boundary が定義されている city のみ厳格化（NULL は素通り）
        assert 'boundary_geom IS NOT NULL' in sql
        # centroid を含むかを ST_Contains で判定
        assert 'ST_Contains' in sql and 'b.centroid' in sql

    def test_city_boundary_filter_applies_to_outlines_and_orphans(self, api):
        """フィルタは bbox_outlines と orphan_parts の両方に効いていること

        related_parts は parent_building_id 経由でついてくるため、outline が
        フィルタで生き残れば自然に連動する。orphan_parts は parent を持たない
        ため独立にフィルタする必要がある。
        """
        conn, cursor = self._setup_api_with_mock_cursor(api)
        with patch.object(api, 'get_connection', return_value=conn):
            api.get_buildings_in_bbox(139.7, 35.7, 139.8, 35.8, limit=10)
        sql = cursor.execute.call_args[0][0]
        # dash_city_master を参照する EXISTS が SQL 全体で 4 回現れること:
        #   - bbox_outlines の city_boundary_filter (1 回)
        #   - bbox_outlines の dedup_tiebreaker (#31 で追加、1 回)
        #   - orphan_parts の city_boundary_filter (1 回)
        #   - orphan_parts の dedup_tiebreaker (#31 Task 3 で追加、1 回)
        boundary_filter_occurrences = sql.count('FROM dash_city_master m')
        assert boundary_filter_occurrences == 4, (
            f"dash_city_master subquery should appear 4 times "
            f"(outlines filter + outlines dedup tiebreaker + orphan parts filter "
            f"+ orphan parts dedup tiebreaker), "
            f"got {boundary_filter_occurrences}"
        )


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
        # Default to a normal, valid child row: it intersects its parent
        # outline. Orphan parts (parent_id=None) get intersects_parent=None
        # below since they have no parent to test against; tests exercising
        # the drop-on-NULL/False behavior pass intersects_parent explicitly.
        'intersects_parent': None if parent_id is None else True,
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
        assert ('way', str(-(1 * 1000)), 'outline') in roles
        # part メンバー
        assert ('way', str(-(2 * 1000)), 'part') in roles
        assert ('way', str(-(3 * 1000)), 'part') in roles

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
        """relation の id は -(outline_db_id * 10 + 1) で way と衝突しない"""
        outline = _make_building(building_id=42)
        p = _make_part(part_id=100, parent_id=42)
        xml_str = api.buildings_to_osm_xml([outline, p])
        root = ET.fromstring(xml_str)
        rel = root.find('relation')
        rel_id = int(rel.get('id'))
        assert rel_id == -(42 * 10 + 1)
        # way id は -outline_db_id*1000, -part_db_id*1000
        way_ids = {int(w.get('id')) for w in root.findall('way')}
        assert rel_id not in way_ids

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
        assert outline_refs == {str(-(1 * 1000)), str(-(10 * 1000))}


class TestBuildingsToOsmXmlNodeSharing:
    """Rapid#33: outline と parts で同一座標のノードは共有されること"""

    def test_outline_part_share_corner_nodes(self, api):
        """outline と part が同座標を持つとき、両ウェイの <nd ref> が同じ id を指す"""
        # outline: 4 corners around a small square
        outline_nodes = [
            {'id': 10, 'lat': 35.7000000, 'lon': 139.7000000},
            {'id': 11, 'lat': 35.7000000, 'lon': 139.7010000},
            {'id': 12, 'lat': 35.7010000, 'lon': 139.7010000},
            {'id': 13, 'lat': 35.7010000, 'lon': 139.7000000},
        ]
        # part: shares 2 corners with outline (NE and NW), 2 unique inside
        part_nodes = [
            {'id': 20, 'lat': 35.7000000, 'lon': 139.7010000},  # = outline id=11
            {'id': 21, 'lat': 35.7005000, 'lon': 139.7010000},  # unique
            {'id': 22, 'lat': 35.7005000, 'lon': 139.7005000},  # unique
            {'id': 23, 'lat': 35.7000000, 'lon': 139.7000000},  # = outline id=10
        ]
        outline = _make_building(building_id=1, nodes=outline_nodes, building='yes')
        part = _make_part(part_id=2, parent_id=1)
        part['nodes'] = part_nodes

        xml_str = api.buildings_to_osm_xml([outline, part])
        root = ET.fromstring(xml_str)

        # Build id → (lat, lon) map for emitted <node> elements
        emitted = {n.get('id'): (n.get('lat'), n.get('lon')) for n in root.findall('node')}

        # The two shared corners should appear ONCE each in <node> output
        # (4 outline + 2 unique part = 6, not 8)
        shared_coords = [
            ('35.7000000', '139.7010000'),
            ('35.7000000', '139.7000000'),
        ]
        for lat, lon in shared_coords:
            matches = [nid for nid, (la, lo) in emitted.items() if (la, lo) == (lat, lon)]
            assert len(matches) == 1, f"shared coord ({lat},{lon}) emitted {len(matches)} times: {matches}"

        # Both outline and part should reference the SAME node id at each shared coord
        outline_way = next(w for w in root.findall('way') if w.get('id') == str(-(1 * 1000)))
        part_way = next(w for w in root.findall('way') if w.get('id') == str(-(2 * 1000)))
        outline_refs = [nd.get('ref') for nd in outline_way.findall('nd')]
        part_refs = [nd.get('ref') for nd in part_way.findall('nd')]

        # Find the ref for shared coord (35.7,139.701) in both ways
        for lat, lon in shared_coords:
            canonical = next(nid for nid, (la, lo) in emitted.items() if (la, lo) == (lat, lon))
            assert canonical in outline_refs, f"outline missing canonical ref {canonical} for ({lat},{lon})"
            assert canonical in part_refs, f"part missing canonical ref {canonical} for ({lat},{lon})"

    def test_separate_buildings_share_coincident_corner_nodes(self, api):
        """Two separate buildings that touch at a corner must share that node.

        Adjacent Plateau buildings arrive with their own node id at an identical
        coordinate; emitting both unshared is exactly what JOSM flags as a
        "Duplicated nodes" error (api#38). The emitter dedupes coincident
        coordinates across the whole response, so the shared corner becomes one
        <node> that both ways reference and JOSM finds nothing to flag."""
        shared = {'id': 10, 'lat': 35.7000000, 'lon': 139.7000000}
        o1_nodes = [shared,
                    {'id': 11, 'lat': 35.7000000, 'lon': 139.7010000},
                    {'id': 12, 'lat': 35.7010000, 'lon': 139.7010000}]
        o2_nodes = [{'id': 20, 'lat': 35.7000000, 'lon': 139.7000000},  # same coord as shared
                    {'id': 21, 'lat': 35.7000000, 'lon': 139.7020000},
                    {'id': 22, 'lat': 35.7020000, 'lon': 139.7020000}]
        o1 = _make_building(building_id=1, nodes=o1_nodes, building='yes')
        o2 = _make_building(building_id=2, nodes=o2_nodes, building='yes')

        xml_str = api.buildings_to_osm_xml([o1, o2])
        root = ET.fromstring(xml_str)

        # JOSM-equivalent check: no coordinate may carry more than one node.
        assert _josm_duplicate_node_coords(root) == []

        # The (35.7, 139.7) coord is emitted as exactly ONE node...
        at_shared = [n for n in root.findall('node')
                     if (n.get('lat'), n.get('lon')) == ('35.7000000', '139.7000000')]
        assert len(at_shared) == 1
        shared_nid = at_shared[0].get('id')

        # ...and both ways reference that same node id at the shared corner.
        ways = root.findall('way')
        assert len(ways) == 2
        for way in ways:
            refs = [nd.get('ref') for nd in way.findall('nd')]
            assert shared_nid in refs, f"way {way.get('id')} does not reference shared node"


class TestValidStartDate:
    """_valid_start_date: which construction years are emitted vs dropped.

    PLATEAU stores start_date as a bare 'YYYY' year; a missing year comes
    through as the placeholder '0001'. Only plausible 4-digit years in
    [1000, current+1] should be emitted.
    """

    def test_plausible_years_accepted(self):
        import osmfj_plateau_api as m
        from datetime import datetime
        for v in ['1000', '1868', '2020', str(datetime.now().year), str(datetime.now().year + 1)]:
            assert m._valid_start_date(v) is True, v

    def test_placeholder_and_out_of_range_rejected(self):
        import osmfj_plateau_api as m
        from datetime import datetime
        for v in ['0001', '0000', '0999', str(datetime.now().year + 2)]:
            assert m._valid_start_date(v) is False, v

    def test_empty_and_malformed_rejected(self):
        import osmfj_plateau_api as m
        for v in [None, '', '   ', 'abcd', '20x0']:
            assert m._valid_start_date(v) is False, v

    def test_full_date_judged_by_leading_year(self):
        # If a full 'YYYY-MM-DD' ever appears, judge by the leading year.
        import osmfj_plateau_api as m
        assert m._valid_start_date('2020-05-01') is True
        assert m._valid_start_date('0001-01-01') is False


class TestDropPartsNotIntersectingParent:
    """記録上の親と交差しない部分立体は出力しない。

    importer の way 番号衝突により、部分立体が数 km 離れた別の建物に
    紐づいている行が本番に 16,175 件ある。そのまま出すと、小さな範囲の
    要求に対して遠方のジオメトリが返る。
    """

    def _square(self, base_id, lat, lon, d=0.0002):
        return [
            {'id': base_id + i, 'lat': la, 'lon': lo, 'sequence_id': i}
            for i, (la, lo) in enumerate([
                (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)
            ])
        ]

    def test_non_intersecting_part_is_not_emitted(self, api):
        buildings = [
            {'id': 1, 'building': 'yes', 'building_part': None,
             'parent_building_id': None, 'intersects_parent': None,
             'nodes': self._square(100, 33.0, 133.0)},
            {'id': 2, 'building': None, 'building_part': 'yes',
             'parent_building_id': 1, 'intersects_parent': False,
             'nodes': self._square(200, 34.0, 134.0)},
        ]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))

        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(2 * 1000)) not in way_ids
        assert str(-(1 * 1000)) in way_ids

    def test_intersecting_part_is_kept(self, api):
        buildings = [
            {'id': 1, 'building': 'yes', 'building_part': None,
             'parent_building_id': None, 'intersects_parent': None,
             'nodes': self._square(100, 33.0, 133.0)},
            {'id': 2, 'building': None, 'building_part': 'yes',
             'parent_building_id': 1, 'intersects_parent': True,
             'nodes': self._square(200, 33.0, 133.0, d=0.0001)},
        ]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))

        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(2 * 1000)) in way_ids

    def test_part_with_null_intersects_parent_is_still_emitted(self, api):
        # ST_Intersects returns NULL (not false) when a geometry involved is
        # NULL. NULL means "cannot tell", not "does not intersect". The OSM
        # output is built from the node ring, not from `geom`, so a part with
        # a broken or missing `geom` still emits correctly — dropping it would
        # discard sound data. tests/test_representative_point.py pins the same
        # contract end to end against a real database.
        buildings = [
            {'id': 1, 'building': 'yes', 'building_part': None,
             'parent_building_id': None, 'intersects_parent': None,
             'nodes': self._square(100, 33.0, 133.0)},
            {'id': 2, 'building': None, 'building_part': 'yes',
             'parent_building_id': 1, 'intersects_parent': None,
             'nodes': self._square(200, 34.0, 134.0)},
        ]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))

        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(2 * 1000)) in way_ids
        assert str(-(1 * 1000)) in way_ids

    def test_outline_row_with_null_intersects_parent_is_still_emitted(self, api):
        # Outlines carry intersects_parent = None by design (they have no
        # parent to intersect against) and must never be dropped.
        buildings = [
            {'id': 1, 'building': 'yes', 'building_part': None,
             'parent_building_id': None, 'intersects_parent': None,
             'nodes': self._square(100, 33.0, 133.0)},
        ]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))

        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(1 * 1000)) in way_ids

    def test_orphan_part_with_null_intersects_parent_is_still_emitted(self, api):
        # Orphan parts (building_part='yes', parent_building_id None) also
        # carry intersects_parent = None by design — they have no recorded
        # parent to test against. Their handling is a separate open issue
        # and must be unaffected by this fix.
        buildings = [
            {'id': 2, 'building': None, 'building_part': 'yes',
             'parent_building_id': None, 'intersects_parent': None,
             'nodes': self._square(200, 34.0, 134.0)},
        ]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))

        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(2 * 1000)) in way_ids


class TestMultipolygonOutput:
    """穴のある建物は type=multipolygon で返す。

    タグは relation に付ける。変換器の出力形式と同じにする。
    """

    def _hole_building(self):
        def ring(base, lat, lon, d, r):
            return [{'id': base + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': r}
                    for i, (la, lo) in enumerate([
                        (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)])]
        return {
            'id': 1, 'building': 'public', 'height': 14.6, 'building_part': None,
            'parent_building_id': None,
            'nodes': ring(100, 33.0, 133.0, 0.001, 0) + ring(200, 33.0003, 133.0003, 0.0004, 1),
        }

    def test_relation_is_multipolygon(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building()]))
        rel = root.find('relation')
        assert rel is not None, 'relation が出ていない'
        tags = {t.get('k'): t.get('v') for t in rel.findall('tag')}
        assert tags.get('type') == 'multipolygon'
        assert tags.get('building') == 'public'

    def test_roles_are_outer_and_inner(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building()]))
        roles = sorted(m.get('role') for m in root.find('relation').findall('member'))
        assert roles == ['inner', 'outer']

    def test_outer_role_is_ring0_way_and_inner_role_is_hole_way(self, api):
        """role の値だけでなく、role がどの way を指すかまで確認する。

        sorted(role) だけを見る旧テストは 'outer'/'inner' の割り当てが逆転
        していても (ring 0 に inner を振っても) 素通りしてしまう。ここでは
        outer メンバーが ring 0 (外形) の way を、inner メンバーが穴の座標を
        含む way を指していることをそれぞれ確認する。
        """
        building = self._hole_building()
        root = ET.fromstring(api.buildings_to_osm_xml([building]))
        rel = root.find('relation')
        members = {m.get('role'): m.get('ref') for m in rel.findall('member')}
        assert set(members) == {'outer', 'inner'}

        # outer 側の way id は単一 way のときと同じ規則 (ring 0) であるはず。
        # これは ring 0 (外形) の way にしか成り立たない。
        assert members['outer'] == str(-(building['id'] * 1000))

        # inner 側の way は、穴 (ring_id == 1) の座標をすべて含んでいるはず。
        node_coord_by_id = {
            n.get('id'): (n.get('lat'), n.get('lon')) for n in root.findall('node')
        }
        hole_nodes = [n for n in building['nodes'] if n['ring_id'] == 1]
        hole_coords = {(f"{n['lat']:.7f}", f"{n['lon']:.7f}") for n in hole_nodes}

        inner_way = next(w for w in root.findall('way') if w.get('id') == members['inner'])
        inner_way_coords = {
            node_coord_by_id[nd.get('ref')] for nd in inner_way.findall('nd')
        }
        assert hole_coords <= inner_way_coords

    def test_ways_carry_no_building_tag(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building()]))
        for w in root.findall('way'):
            tags = {t.get('k') for t in w.findall('tag')}
            assert 'building' not in tags, 'タグは relation に付ける'

    def test_building_without_hole_is_still_one_way(self, api):
        b = self._hole_building()
        b['nodes'] = [n for n in b['nodes'] if n['ring_id'] == 0]
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.find('relation') is None
        assert len(root.findall('way')) == 1

    def test_invalid_ring_leaves_no_orphan_nodes(self, api):
        """1環でも検証に失敗したら、他の環の <node> も含めて building 全体を出さない。

        ring 0 (外形) は有効なまま ring 1 (穴) だけを検証失敗させる (3点未満に
        削る)。検証 (_valid_nodes_from_ring) と生成 (_make_way_elem) を2パスに
        分離した理由そのものを守るテスト: 1パスに戻して ring 0 の way を先に
        作ってしまうと、ring 1 の検証失敗時に ring 0 の <node> だけが way から
        参照されないまま response に残る (孤立点としてクライアントに描画される)。
        """
        b = self._hole_building()
        b['nodes'] = [n for n in b['nodes'] if n['ring_id'] == 0 or n['sequence_id'] < 2]
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.findall('node') == []
        assert root.findall('way') == []
        assert root.findall('relation') == []

    def test_multipolygon_relation_id_does_not_collide_with_parts_relation(self, api):
        """1 建物が「穴を持つ outline」かつ「part の親」のとき、2 つの
        relation の id が別であることを確認する。

        採番は type=building が -(建物id * 10 + 1)、type=multipolygon が
        -(建物id * 10 + 2) なので、同じ建物でも種別の桁で分かれる。

        なお、この入力の組み合わせは DB には存在しえない。
        parent_building_id を埋める親マップは取り込み側で常に空であり、
        ring_id >= 1 は multipolygon 経路からしか出ない。両者は同じ
        取り込み実行で決まるので、1 行が両方を持つことはない。
        この assert は 2 つの relation が出る形を正しいと認めるものではなく、
        id が別であることだけを固定している。
        """
        outline = self._hole_building()  # id=1, 穴あり, parent_building_id=None
        part = {
            'id': 2, 'building': None, 'height': 5.0, 'building_part': 'yes',
            'parent_building_id': 1,
            'nodes': [{'id': 300 + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (33.0, 133.0), (33.0002, 133.0), (33.0002, 133.0002), (33.0, 133.0002)])],
        }
        root = ET.fromstring(api.buildings_to_osm_xml([outline, part]))
        relations = root.findall('relation')
        ids = [r.get('id') for r in relations]
        assert len(ids) == len(set(ids)), f'relation id が重複している: {ids}'

        types = sorted(
            t.get('v') for r in relations for t in r.findall('tag') if t.get('k') == 'type'
        )
        assert types == ['building', 'multipolygon']


class TestSyntheticIdScheme:
    """合成 OSM id は (建物id, 役割) から型ごとに一意に決まる。

    way      : -(建物id * 1000 + 環番号)
    relation : -(建物id * 10   + 種別)

    定数 offset 方式は建物 id が offset の桁に達すると族の帯が重なる。
    本番の建物 id は約 1,490 万で、旧方式の relation オフセット (-1,000,000) を
    既に越えていた。
    """

    def _hole_building(self, db_id=1, inner_rings=1):
        def ring(base, lat, lon, d, r):
            return [{'id': base + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': r}
                    for i, (la, lo) in enumerate([
                        (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)])]
        nodes = ring(1000, 33.0, 133.0, 0.01, 0)
        for r in range(1, inner_rings + 1):
            nodes += ring(1000 + r * 10, 33.0 + 0.0005 * r, 133.0 + 0.0005 * r, 0.0002, r)
        return {
            'id': db_id, 'building': 'public', 'height': 14.6, 'building_part': None,
            'parent_building_id': None, 'nodes': nodes,
        }

    def _plain_building(self, db_id):
        return {
            'id': db_id, 'building': 'yes', 'height': 5.0, 'building_part': None,
            'parent_building_id': None,
            'nodes': [{'id': db_id * 100 + i, 'lat': la, 'lon': lo,
                       'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (34.0, 134.0), (34.01, 134.0), (34.01, 134.01), (34.0, 134.01)])],
        }

    def test_plain_way_id_uses_the_ring_multiplier(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._plain_building(7)]))
        assert root.find('way').get('id') == str(-(7 * 1000))

    def test_inner_ring_way_ids_follow_the_ring_number(self, api):
        b = self._hole_building(db_id=7, inner_rings=2)
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        members = {m.get('role'): [] for m in root.find('relation').findall('member')}
        for m in root.find('relation').findall('member'):
            members[m.get('role')].append(m.get('ref'))
        assert members['outer'] == [str(-(7 * 1000 + 0))]
        assert sorted(members['inner']) == sorted(
            [str(-(7 * 1000 + 1)), str(-(7 * 1000 + 2))]
        )

    def test_multipolygon_relation_id_uses_kind_two(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building(db_id=7)]))
        assert root.find('relation').get('id') == str(-(7 * 10 + 2))

    def test_building_relation_id_uses_kind_one(self, api):
        outline = self._plain_building(7)
        part = self._plain_building(8)
        part['building_part'] = 'yes'
        part['parent_building_id'] = 7
        root = ET.fromstring(api.buildings_to_osm_xml([outline, part]))
        rel = root.find('relation')
        assert rel.get('id') == str(-(7 * 10 + 1))
        members = {m.get('role'): m.get('ref') for m in rel.findall('member')}
        assert members['outline'] == str(-(7 * 1000))
        assert members['part'] == str(-(8 * 1000))

    def test_ids_are_unique_within_each_type(self, api):
        """旧方式で衝突していた組を含めても、型の中で重複しない。

        旧方式では type=building relation の id (-1_000_000 - id_C) と
        type=multipolygon relation の id (-3_000_000 - id_A) が
        id_C == id_A + 2_000_000 のときに一致した。
        courtyard 建物 (id_A) を db_id=5 に固定し、その衝突相手 id_C =
        2,000,005 を parts parent にして type=building relation を
        実際に発生させる。プレーンな建物のままでは type=building
        relation が出ないので衝突の検証にならない。
        """
        buildings = [self._hole_building(db_id=5)]
        for db_id in (1, 2, 3, 100):
            buildings.append(self._plain_building(db_id))
        colliding_outline = self._plain_building(2_000_005)
        colliding_part = self._plain_building(2_000_006)
        colliding_part['building_part'] = 'yes'
        colliding_part['parent_building_id'] = 2_000_005
        buildings += [colliding_outline, colliding_part]
        outline = self._plain_building(9)
        part = self._plain_building(10)
        part['building_part'] = 'yes'
        part['parent_building_id'] = 9
        buildings += [outline, part]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))
        way_ids = [w.get('id') for w in root.findall('way')]
        rel_ids = [r.get('id') for r in root.findall('relation')]
        assert len(way_ids) == len(set(way_ids)), f'way id が重複: {way_ids}'
        assert len(rel_ids) == len(set(rel_ids)), f'relation id が重複: {rel_ids}'


class TestRingLimitAndIdUniqueness:
    """環番号の上限は API の出力側だけで守る。

    上限は id の採番式の制約であって、データの制約ではない。
    取り込み側は何も捨てないので、式を直せば次のリクエストから出る。
    """

    def _hole_building(self, db_id=1, inner_rings=1):
        def ring(base, lat, lon, d, r):
            return [{'id': base + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': r}
                    for i, (la, lo) in enumerate([
                        (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)])]
        nodes = ring(10 ** 7, 33.0, 133.0, 0.5, 0)
        for r in range(1, inner_rings + 1):
            base = 10 ** 7 + r * 10
            nodes += ring(base, 33.0 + 0.0001 * r, 133.0 + 0.0001 * r, 0.00002, r)
        return {
            'id': db_id, 'building': 'public', 'height': 14.6, 'building_part': None,
            'parent_building_id': None, 'nodes': nodes,
        }

    def test_ring_999_is_served(self, api):
        b = self._hole_building(db_id=3, inner_rings=999)
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.find('relation') is not None, '環 999 本の建物が出ていない'
        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(3 * 1000 + 999)) in way_ids

    def test_ring_1000_is_dropped_from_the_response(self, api, caplog):
        b = self._hole_building(db_id=3, inner_rings=1000)
        with caplog.at_level(logging.WARNING):
            root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.find('relation') is None, '環 1000 本の建物が出力されている'
        assert root.findall('way') == [], '一部の way だけ残っている'
        assert root.findall('node') == [], '参照されないノードが残っている'
        assert any('1000' in r.message for r in caplog.records), '警告が出ていない'

    def test_other_buildings_survive_when_one_is_dropped(self, api):
        big = self._hole_building(db_id=3, inner_rings=1000)
        ok = {
            'id': 4, 'building': 'yes', 'height': 5.0, 'building_part': None,
            'parent_building_id': None,
            'nodes': [{'id': 900 + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (35.0, 135.0), (35.01, 135.0), (35.01, 135.01), (35.0, 135.01)])],
        }
        root = ET.fromstring(api.buildings_to_osm_xml([big, ok]))
        assert [w.get('id') for w in root.findall('way')] == [str(-(4 * 1000))]

    def test_duplicate_id_is_reported(self, api, caplog, monkeypatch):
        """採番式が壊れたときの網。式を潰して衝突を作り、警告が出ることを見る。

        現在の式では同型の id が衝突しないので、衝突そのものは作れない。
        検知の経路が生きていることだけを確かめる。
        """
        monkeypatch.setattr(type(api), '_way_id',
                            lambda self, building_db_id, ring_no=0: -1)
        b1 = {
            'id': 1, 'building': 'yes', 'height': 5.0, 'building_part': None,
            'parent_building_id': None,
            'nodes': [{'id': 10 + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (36.0, 136.0), (36.01, 136.0), (36.01, 136.01), (36.0, 136.01)])],
        }
        b2 = dict(b1, id=2)
        with caplog.at_level(logging.WARNING):
            api.buildings_to_osm_xml([b1, b2])
        assert any('衝突' in r.message for r in caplog.records), '重複が報告されていない'


# ----------------------------------------------------------------------
# 座標から決まるノード id (#51)
# ----------------------------------------------------------------------

class TestNodeIdFromCoordinate:
    """ノード id が座標だけで決まること。

    応答に含まれる建物で id が変わると、同じ角が取得のたびに別のノードとして
    Rapid に届き、承認すると重複ノードとして OSM に上がる (#51)。
    """

    def test_id_is_negative_and_nonzero(self, api):
        nid = api._node_id(35.7000000, 139.7000000, 12345)
        assert nid < 0

    def test_id_fits_in_int64(self, api):
        # 範囲の隅がもっとも大きな値になるのは、緯度・経度とも刻み数の
        # 最大値 (NODE_LAT_STEPS - 1 / NODE_LON_STEPS - 1) を取る座標、
        # すなわちちょうど 46.0 / 154.0 のとき。
        nid = api._node_id(46.0, 154.0, 1)
        assert nid == -83_200_000_580_000_001
        assert abs(nid) < 2 ** 63

    def test_same_coordinate_gives_same_id_regardless_of_db_row(self, api):
        # 同じ角に属する DB の行は建物ごとに違う id を持つ
        assert api._node_id(32.6445365, 130.6984598, 272155034) == \
               api._node_id(32.6445365, 130.6984598, 272163761)

    def test_distinct_coordinates_never_collide(self, api):
        seen = set()
        for i in range(200):
            for j in range(200):
                nid = api._node_id(32.6445365 + i * 1e-7,
                                   130.6984598 + j * 1e-7, 1)
                seen.add(nid)
        assert len(seen) == 200 * 200

        # 上のグリッドは 1 つの連続したブロックに収まっているので、
        # NODE_LON_STEPS がそのブロック幅より大きければ何でも単射になり、
        # 乗数の値そのものは検証できない。単射性を支えているのは乗数だけ
        # なので、1 ストライドの両端で衝突しないことを別に固定する。
        # これは NODE_LON_STEPS を留めるテスト。
        assert api._node_id(20.0000001, 122.0, 1) != api._node_id(20.0, 154.0, 1)

    def test_id_matches_printed_coordinate(self, api):
        # 出力は f"{lat:.7f}" で丸めるので、丸めた値と生の値で id が割れては
        # ならない。35.00000015 は半端 (half-way) 値で、掛け算してから丸める
        # 実装と、先に 7 桁へ丸めてから掛ける実装とで結果が分かれる。
        lat, lon = 35.00000015, 139.00000015
        printed_lat, printed_lon = f'{lat:.7f}', f'{lon:.7f}'
        assert api._node_id(lat, lon, 1) == \
               api._node_id(float(printed_lat), float(printed_lon), 1)

    def test_out_of_range_falls_back_to_db_id(self, api, caplog):
        with caplog.at_level(logging.WARNING):
            nid = api._node_id(60.0, 139.7, 4242)   # 緯度が範囲外
        assert nid == -4242
        assert any('範囲外' in r.message for r in caplog.records)

    def test_shared_corner_id_does_not_depend_on_which_buildings_are_present(self, api):
        """#51 の再現。隣の建物が応答に入るかどうかで角の id が変わってはならない。"""
        corner = {'lat': 32.6445365, 'lon': 130.6984598}
        b1 = _make_building(building_id=1, building='yes', nodes=[
            {'id': 272155033, 'lat': 32.6444162, 'lon': 130.6988233},
            {'id': 272155034, **corner},
            {'id': 272155035, 'lat': 32.6444965, 'lon': 130.6984413},
        ])
        # 隣の建物。同じ角を自分の行 id で持つ
        b2 = _make_building(building_id=2, building='yes', nodes=[
            {'id': 272163761, **corner},
            {'id': 272163762, 'lat': 32.6446000, 'lon': 130.6984598},
            {'id': 272163763, 'lat': 32.6446000, 'lon': 130.6985000},
        ])

        def corner_id(xml_str):
            root = ET.fromstring(xml_str)
            hit = [n for n in root.findall('node')
                   if (n.get('lat'), n.get('lon')) == ('32.6445365', '130.6984598')]
            assert len(hit) == 1
            return hit[0].get('id')

        both = corner_id(api.buildings_to_osm_xml([b1, b2]))
        only_b2 = corner_id(api.buildings_to_osm_xml([b2]))
        reversed_order = corner_id(api.buildings_to_osm_xml([b2, b1]))

        assert both == only_b2 == reversed_order

    def test_adjacent_buildings_share_the_corner_node(self, api):
        corner = {'lat': 32.6445365, 'lon': 130.6984598}
        b1 = _make_building(building_id=1, building='yes', nodes=[
            {'id': 272155033, 'lat': 32.6444162, 'lon': 130.6988233},
            {'id': 272155034, **corner},
            {'id': 272155035, 'lat': 32.6444965, 'lon': 130.6984413},
        ])
        b2 = _make_building(building_id=2, building='yes', nodes=[
            {'id': 272163761, **corner},
            {'id': 272163762, 'lat': 32.6446000, 'lon': 130.6984598},
            {'id': 272163763, 'lat': 32.6446000, 'lon': 130.6985000},
        ])

        root = ET.fromstring(api.buildings_to_osm_xml([b1, b2]))

        # 応答の中の重複は 0 件のまま (#38 の退行が無いこと)
        assert _josm_duplicate_node_coords(root) == []

        at_corner = [n for n in root.findall('node')
                     if (n.get('lat'), n.get('lon')) == ('32.6445365', '130.6984598')]
        assert len(at_corner) == 1
        corner_nid = at_corner[0].get('id')

        ways = root.findall('way')
        assert len(ways) == 2
        for way in ways:
            refs = [nd.get('ref') for nd in way.findall('nd')]
            assert corner_nid in refs


class TestOsmChangePlaceholderContract:
    """アップロードを受ける側が仮 id に課す条件を、応答の側で満たしているか。

    条件の出どころは 2 つ。
    cgimap は id を int64 で読み、作成する要素には「0 でない」と「負である」を
    課す (osmobject.hpp)。Rails は加えて「作成する要素の仮 id は型ごとに
    一意であること」を課す (lib/diff_reader.rb)。
    """

    def _response(self, api):
        corner = {'lat': 32.6445365, 'lon': 130.6984598}
        b1 = _make_building(building_id=1, building='yes', nodes=[
            {'id': 272155033, 'lat': 32.6444162, 'lon': 130.6988233},
            {'id': 272155034, **corner},
            {'id': 272155035, 'lat': 32.6444965, 'lon': 130.6984413},
        ])
        b2 = _make_building(building_id=2, building='yes', nodes=[
            {'id': 272163761, **corner},
            {'id': 272163762, 'lat': 32.6446000, 'lon': 130.6984598},
            {'id': 272163763, 'lat': 32.6446000, 'lon': 130.6985000},
        ])
        # b1 を outline とする building:part を追加する。
        # これで b1 が type=building relation の outline になり、応答に
        # node/way/relation の 3 種類が揃う。
        # (TestBuildingsToOsmXmlRelations.test_outline_with_parts_generates_relation
        # と同じ組み方)
        part = _make_part(part_id=3, parent_id=1, height=5)
        root = ET.fromstring(api.buildings_to_osm_xml([b1, b2, part]))

        # このクラスの各テストは node/way/relation の3種類をループで確認する。
        # relation が1つも無いとループ本体が空振りして、何も検証せずに
        # テストが通ってしまう。ここで前提を強制する。
        for kind in ('node', 'way', 'relation'):
            assert root.findall(kind), \
                f'fixture の応答に {kind} が含まれていない (このクラスの前提が崩れている)'
        return root

    def test_all_ids_are_negative_and_nonzero(self, api):
        root = self._response(api)
        for kind in ('node', 'way', 'relation'):
            for elem in root.findall(kind):
                value = int(elem.get('id'))
                assert value < 0, f'{kind} id {value} が負でない'

    def test_all_ids_fit_in_int64(self, api):
        root = self._response(api)
        for kind in ('node', 'way', 'relation'):
            for elem in root.findall(kind):
                assert abs(int(elem.get('id'))) < 2 ** 63

    def test_ids_are_unique_within_each_type(self, api):
        root = self._response(api)
        for kind in ('node', 'way', 'relation'):
            ids = [elem.get('id') for elem in root.findall(kind)]
            assert len(ids) == len(set(ids)), f'{kind} の id が重複している'

    def test_every_nd_ref_resolves_to_an_emitted_node(self, api):
        root = self._response(api)
        emitted = {n.get('id') for n in root.findall('node')}
        for way in root.findall('way'):
            for nd in way.findall('nd'):
                assert nd.get('ref') in emitted, \
                    f"way {way.get('id')} が未出力のノード {nd.get('ref')} を参照している"
