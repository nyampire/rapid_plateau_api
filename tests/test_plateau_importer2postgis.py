"""
plateau_importer2postgis.py のユニットテスト

主に DB に投入する直前のロジックを検証する:
- `_dedupe_and_remap_nodes`: ノード行の重複排除と building_id 差し替え
- `_resolve_part_parents`: building:part の parent_building_id 解決
- `parse_osm_file_safe`: relation 解析と building:part way の検出
"""

import io
import os
import tempfile
import textwrap
from unittest.mock import MagicMock

from plateau_importer2postgis import PlateauImporter2PostGIS


def _make_row(osm_id, building_id, seq, lat, lon):
    """nodes_data の1行を組み立てるヘルパー。

    insert_to_database_batch の append と同じレイアウト:
    (osm_id, building_id, sequence_id, lat, lon, lon, lat)
    """
    return (osm_id, building_id, seq, lat, lon, lon, lat)


class TestDedupeAndRemapNodes:
    def test_closure_within_single_building_is_deduped(self):
        """単一building内の閉路重複 (refs[0] == refs[-1]) は1件にまとめられる"""
        nodes_data = [
            _make_row(-50, 100, 0, 33.5687, 133.5237),
            _make_row(-51, 100, 1, 33.5686, 133.5237),
            _make_row(-52, 100, 2, 33.5686, 133.5238),
            _make_row(-53, 100, 3, 33.5687, 133.5238),
            _make_row(-50, 100, 4, 33.5687, 133.5237),  # closure (same osm_id)
        ]
        osm_id_to_db_id = {100: 1}

        mapped, skipped, orphan = PlateauImporter2PostGIS._dedupe_and_remap_nodes(
            nodes_data, osm_id_to_db_id
        )

        assert len(mapped) == 4  # 5 input rows - 1 closure dup
        assert skipped == 1
        assert orphan == 0
        # 最初の -50 は seq=0 で残る
        seqs = sorted(r[2] for r in mapped if r[0] == -50)
        assert seqs == [0]

    def test_shared_corner_node_preserved_across_buildings(self):
        """隣接する2建物が同じosm_idのコーナーノードを共有していても、両方で保持される。

        これが本クラスの主目的: 以前のグローバル重複排除では後発buildingで
        共有コーナーが脱落し、ジオメトリが1点欠ける不具合があった。
        """
        nodes_data = [
            # Building A (osm_id=100): 4つの頂点 + 閉路
            _make_row(-50, 100, 0, 33.5687, 133.5237),  # ← 共有コーナー
            _make_row(-51, 100, 1, 33.5686, 133.5237),
            _make_row(-52, 100, 2, 33.5686, 133.5238),
            _make_row(-53, 100, 3, 33.5687, 133.5238),
            _make_row(-50, 100, 4, 33.5687, 133.5237),  # closure
            # Building B (osm_id=101): 共有コーナー -50 を seq=0 と seq=4 で使う
            _make_row(-50, 101, 0, 33.5687, 133.5237),  # ← 共有コーナー
            _make_row(-54, 101, 1, 33.5688, 133.5237),
            _make_row(-55, 101, 2, 33.5688, 133.5238),
            _make_row(-56, 101, 3, 33.5687, 133.5238),
            _make_row(-50, 101, 4, 33.5687, 133.5237),  # closure
        ]
        osm_id_to_db_id = {100: 1, 101: 2}

        mapped, skipped, orphan = PlateauImporter2PostGIS._dedupe_and_remap_nodes(
            nodes_data, osm_id_to_db_id
        )

        # 各buildingに closure 1件ずつ、計2件が重複扱い
        assert skipped == 2
        assert orphan == 0
        # building A と B それぞれ4頂点ずつ
        assert len(mapped) == 8

        building_a_rows = [r for r in mapped if r[1] == 1]
        building_b_rows = [r for r in mapped if r[1] == 2]
        assert len(building_a_rows) == 4
        assert len(building_b_rows) == 4

        # 共有コーナー -50 が両方の building に保持されていること
        assert any(r[0] == -50 for r in building_a_rows), "Building A から共有コーナーが脱落"
        assert any(r[0] == -50 for r in building_b_rows), "Building B から共有コーナーが脱落"

    def test_orphan_nodes_counted_when_building_not_in_map(self):
        """osm_id_to_db_id に存在しない building の行は orphan として除外される"""
        nodes_data = [
            _make_row(-50, 100, 0, 33.5687, 133.5237),
            _make_row(-51, 100, 1, 33.5686, 133.5237),
            # 存在しない building 999 の行 (例: 建物投入時にスキップされた場合)
            _make_row(-60, 999, 0, 33.5680, 133.5230),
            _make_row(-61, 999, 1, 33.5681, 133.5231),
        ]
        osm_id_to_db_id = {100: 1}

        mapped, skipped, orphan = PlateauImporter2PostGIS._dedupe_and_remap_nodes(
            nodes_data, osm_id_to_db_id
        )

        assert len(mapped) == 2
        assert orphan == 2
        assert skipped == 0
        assert all(r[1] == 1 for r in mapped)

    def test_building_id_remapped_to_db_id(self):
        """osm_building_id が DB の自動採番 building_id に差し替えられる"""
        nodes_data = [
            _make_row(-50, 100, 0, 33.5687, 133.5237),
            _make_row(-51, 200, 0, 33.5688, 133.5237),
        ]
        osm_id_to_db_id = {100: 42, 200: 43}

        mapped, _, _ = PlateauImporter2PostGIS._dedupe_and_remap_nodes(
            nodes_data, osm_id_to_db_id
        )

        # row[1] が DB 上の id に置き換わっている
        building_ids = sorted(r[1] for r in mapped)
        assert building_ids == [42, 43]

    def test_empty_input(self):
        """空の入力に対してエラーなく動作する"""
        mapped, skipped, orphan = PlateauImporter2PostGIS._dedupe_and_remap_nodes(
            [], {}
        )
        assert mapped == []
        assert skipped == 0
        assert orphan == 0


class TestBuildPartParentUpdates:
    """parts_parent_map → UPDATE 用ペアの構築ロジック (pure 関数)"""

    def test_basic_resolution(self):
        parts_parent_map = [(-100, -200), (-101, -200)]
        osm_to_db = {-100: 1, -101: 2, -200: 99}

        updates, unresolved = PlateauImporter2PostGIS._build_part_parent_updates(
            parts_parent_map, osm_to_db
        )

        assert sorted(updates) == [(1, 99), (2, 99)]
        assert unresolved == 0

    def test_skips_unresolved_parent(self):
        parts_parent_map = [(-100, -200), (-101, -999)]  # -999 不明
        osm_to_db = {-100: 1, -101: 2, -200: 99}

        updates, unresolved = PlateauImporter2PostGIS._build_part_parent_updates(
            parts_parent_map, osm_to_db
        )

        assert updates == [(1, 99)]
        assert unresolved == 1

    def test_skips_unresolved_child(self):
        parts_parent_map = [(-100, -200), (-888, -200)]  # part osm_id 不明
        osm_to_db = {-100: 1, -200: 99}

        updates, unresolved = PlateauImporter2PostGIS._build_part_parent_updates(
            parts_parent_map, osm_to_db
        )

        assert updates == [(1, 99)]
        assert unresolved == 1

    def test_empty(self):
        updates, unresolved = PlateauImporter2PostGIS._build_part_parent_updates([], {})
        assert updates == []
        assert unresolved == 0


class TestResolvePartParents:
    """_resolve_part_parents の早期 return ガード"""

    def test_empty_parts_parent_map_no_execute(self):
        """空の入力は SELECT も発行しない"""
        cur = MagicMock()
        n = PlateauImporter2PostGIS._resolve_part_parents(cur, [])
        assert n == 0
        cur.execute.assert_not_called()


# --- relation parsing test ---

_MIN_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.0001" lon="133.0"/>
  <node id="-3" lat="33.0001" lon="133.0001"/>
  <node id="-4" lat="33.0" lon="133.0001"/>
  <node id="-5" lat="33.00005" lon="133.00005"/>
  <node id="-6" lat="33.00008" lon="133.00005"/>
  <node id="-7" lat="33.00008" lon="133.00008"/>
  <node id="-8" lat="33.00005" lon="133.00008"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
    <tag k="building" v="yes"/>
    <tag k="height" v="10"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
    <tag k="height" v="3.5"/>
    <tag k="ele" v="10"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outline"/>
    <member type="way" ref="-20" role="part"/>
    <tag k="type" v="building"/>
    <tag k="building" v="yes"/>
    <tag k="height" v="10"/>
  </relation>
</osm>
""")


class TestParseOsmFileRelations:
    """relation 経由で building:part way が抽出されることを検証"""

    def _make_importer(self, monkeypatch):
        """DB 接続を avoid して importer を生成"""
        # __init__ の DB 呼び出しを skip
        monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
        monkeypatch.setattr(PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None)
        monkeypatch.setattr(PlateauImporter2PostGIS, '_ensure_schema', lambda self: None)
        with tempfile.TemporaryDirectory() as tmpdir:
            os.makedirs(os.path.join(tmpdir, '39999'), exist_ok=True)
            importer = PlateauImporter2PostGIS(
                data_dir=os.path.join(tmpdir, '39999'),
                postgres_url='fake',
                citycode='39999',
            )
            return importer

    def test_outline_and_part_both_extracted(self, monkeypatch, tmp_path):
        """outline + part の両方が buildings リストに含まれ、role が正しく付与される"""
        # __init__ が DB を触らないように mock
        monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
        monkeypatch.setattr(PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None)
        monkeypatch.setattr(PlateauImporter2PostGIS, '_ensure_schema', lambda self: None)
        data_dir = tmp_path / '39999'
        data_dir.mkdir()
        osm_file = data_dir / 'test.osm'
        osm_file.write_text(_MIN_OSM)

        importer = PlateauImporter2PostGIS(
            data_dir=str(data_dir),
            postgres_url='fake',
            citycode='39999',
        )
        nodes, buildings = importer.parse_osm_file_safe(osm_file)

        assert len(buildings) == 2

        by_way_id = {b['way_id']: b for b in buildings}
        # outline way -10
        assert '-10' in by_way_id
        outline = by_way_id['-10']
        assert outline['is_part'] is False
        assert outline['parent_outline_way_id'] is None

        # part way -20
        assert '-20' in by_way_id
        part = by_way_id['-20']
        assert part['is_part'] is True
        assert part['parent_outline_way_id'] == '-10'

    def test_standalone_building_part_without_relation(self, monkeypatch, tmp_path):
        """relation 無しでも building:part だけの way は part として抽出される"""
        monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
        monkeypatch.setattr(PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None)
        monkeypatch.setattr(PlateauImporter2PostGIS, '_ensure_schema', lambda self: None)
        # relation を除いた XML
        osm_no_rel = _MIN_OSM.replace(
            '<relation id="-30">\n    <member type="way" ref="-10" role="outline"/>\n'
            '    <member type="way" ref="-20" role="part"/>\n'
            '    <tag k="type" v="building"/>\n    <tag k="building" v="yes"/>\n'
            '    <tag k="height" v="10"/>\n  </relation>\n',
            ''
        )
        data_dir = tmp_path / '39999'
        data_dir.mkdir()
        osm_file = data_dir / 'test.osm'
        osm_file.write_text(osm_no_rel)
        importer = PlateauImporter2PostGIS(
            data_dir=str(data_dir),
            postgres_url='fake',
            citycode='39999',
        )
        nodes, buildings = importer.parse_osm_file_safe(osm_file)

        by_way_id = {b['way_id']: b for b in buildings}
        # part の parent_outline_way_id は None (relation 無いので)
        assert by_way_id['-20']['is_part'] is True
        assert by_way_id['-20']['parent_outline_way_id'] is None
