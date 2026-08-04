"""
plateau_importer2postgis.py のユニットテスト

主に DB に投入する直前のロジックを検証する:
- `_dedupe_and_remap_nodes`: ノード行の重複排除と building_id 差し替え
- `_resolve_part_parents`: building:part の parent_building_id 解決
- `parse_osm_file_safe`: relation 解析と building:part way の検出
"""

import io
import logging
import os
import shutil
import textwrap
from pathlib import Path
from unittest.mock import MagicMock

from plateau_importer2postgis import PlateauImporter2PostGIS


def _make_row(osm_id, building_id, seq, lat, lon, ring_id=0):
    """nodes_data の1行を組み立てるヘルパー。

    insert_to_database_batch の append と同じレイアウト:
    (osm_id, building_id, sequence_id, lat, lon, lon, lat, ring_id)
    """
    return (osm_id, building_id, seq, lat, lon, lon, lat, ring_id)


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
    <tag k="ref:MLIT_PLATEAU" v="39999-bldg-10"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
    <tag k="height" v="3.5"/>
    <tag k="ele" v="10"/>
    <tag k="ref:MLIT_PLATEAU" v="39999-bldg-20"/>
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
    """importer source-fidelity task 1 により、type=building の relation は
    もう読まれない（融合で作られた合成 outline とその親子関係を取り込まない
    ため）。このクラスはその relation が存在しても無視されることを検証する。
    two way にはどちらも `ref:MLIT_PLATEAU` を付けてある。これが無い way は
    合成形状とみなされて丸ごと落ちるため（TestDropSynthesizedShapes 参照）、
    ここでは実在建物として残るケースだけを見ている。
    """

    def test_outline_and_part_both_extracted(self, bare_importer):
        """outline + part の両方が buildings リストに含まれる。relation は
        読まれないので、building:part だった way も is_part=False になり、
        parent_outline_way_id は常に None になる。"""
        importer = bare_importer(citycode='39999')
        osm_file = Path(importer.data_dir) / 'test.osm'
        osm_file.write_text(_MIN_OSM)

        nodes, buildings = importer.parse_osm_file_safe(osm_file)

        assert len(buildings) == 2

        by_way_id = {b['way_id']: b for b in buildings}
        # outline way -10
        assert '-10' in by_way_id
        outline = by_way_id['-10']
        assert outline['is_part'] is False
        assert outline['parent_outline_way_id'] is None

        # part way -20: ref:MLIT_PLATEAU を持つので実在建物として扱われる。
        # relation を読まなくなったので is_part は False、parent は None。
        assert '-20' in by_way_id
        part = by_way_id['-20']
        assert part['is_part'] is False
        assert part['parent_outline_way_id'] is None

    def test_standalone_building_part_without_relation(self, bare_importer):
        """relation の有無に関わらず、建物 ID を持つ building:part way は
        独立した建物として抽出される（is_part は常に False）。"""
        # relation を除いた XML
        osm_no_rel = _MIN_OSM.replace(
            '<relation id="-30">\n    <member type="way" ref="-10" role="outline"/>\n'
            '    <member type="way" ref="-20" role="part"/>\n'
            '    <tag k="type" v="building"/>\n    <tag k="building" v="yes"/>\n'
            '    <tag k="height" v="10"/>\n  </relation>\n',
            ''
        )
        importer = bare_importer(citycode='39999')
        osm_file = Path(importer.data_dir) / 'test.osm'
        osm_file.write_text(osm_no_rel)

        nodes, buildings = importer.parse_osm_file_safe(osm_file)

        by_way_id = {b['way_id']: b for b in buildings}
        assert by_way_id['-20']['is_part'] is False
        assert by_way_id['-20']['parent_outline_way_id'] is None


# ----------------------------------------------------------------------
# citygml-osm v3.x fixtures (api#21 Phase 0 / #25)
# ----------------------------------------------------------------------

class TestCitygmlOsmFixtures:
    """Pin the citygml-osm v3.x output contract so a future parser tweak or
    upstream tool bump can't silently regress the fields importer reads.

    Phase 0 (api#21) confirmed `parse_osm_file_safe` tolerates the upstream
    non-standard XML extensions (`@area`/`@fix`/`@visible` on `<way>`,
    `<memberWay>` child elements, `<complete>`/`<marged>` on `<relation>`).
    These fixtures lock that in: ElementTree must keep ignoring the extras
    while still extracting the standard tags / node refs / relation members
    importer relies on.
    """

    FIX_DIR = Path(__file__).parent / 'fixtures' / 'citygml-osm'

    def test_v4_outline_only_fixture_parses(self, bare_importer):
        """V4 mesh shape: outlines only, no `building:part`, no `<relation>`.

        Two outline buildings, every way carries the upstream non-standard
        attrs + `<memberWay>` child. None of that should leak into the parsed
        building records; importer should just see 2 plain outlines.
        """
        importer = bare_importer(citycode='13308')
        nodes, buildings = importer.parse_osm_file_safe(
            self.FIX_DIR / 'v4_outline_only.osm'
        )

        # 2 outlines, no parts.
        assert len(buildings) == 2
        assert all(b['is_part'] is False for b in buildings)
        assert all(b['parent_outline_way_id'] is None for b in buildings)
        # Standard tags survive the round-trip.
        for b in buildings:
            assert b['tags'].get('building') == 'yes'
            assert 'ref:MLIT_PLATEAU' in b['tags']
        # `area` / `fix` / `visible` and the synthetic `<memberWay>` child are
        # upstream extensions, NOT OSM tags — they must not appear on the
        # parsed tag dict.
        for b in buildings:
            assert 'area' not in b['tags']
            assert 'fix' not in b['tags']
            assert 'visible' not in b['tags']
            assert 'memberWay' not in b['tags']
        # Every node ref resolves into the parsed nodes table — i.e., the
        # `visible='true'` attr didn't break node ingestion.
        assert len(nodes) == 8

    def test_v5_outline_with_part_via_relation(self, bare_importer):
        """V5 mesh shape: outline + `building:part` linked by a
        `<relation type='building'>` carrying the non-standard
        `<complete>` / `<marged>` child elements.

        importer source-fidelity task 1: the `type=building` relation is no
        longer read — it links a synthesized merge outline to the real
        buildings it fused, not a source BuildingPart, so the parent link
        would point at a shape that doesn't exist in the source. Both ways
        here carry `ref:MLIT_PLATEAU` (the fixture models the part as a real
        building demoted by the converter's fusion), so both survive as
        independent buildings with `is_part=False` and no parent.
        """
        importer = bare_importer(citycode='02321')
        nodes, buildings = importer.parse_osm_file_safe(
            self.FIX_DIR / 'v5_outline_with_part.osm'
        )

        assert len(buildings) == 2
        by_way = {b['way_id']: b for b in buildings}
        # outline
        assert by_way['-100']['is_part'] is False
        assert by_way['-100']['parent_outline_way_id'] is None
        # part — has a real building ID, so it's promoted to a standalone
        # building; the relation is not consulted anymore.
        assert by_way['-200']['is_part'] is False
        assert by_way['-200']['parent_outline_way_id'] is None
        # Non-standard relation children don't leak into either building's tags.
        for b in buildings:
            assert 'complete' not in b['tags']
            assert 'marged' not in b['tags']


# ----------------------------------------------------------------------
# 行政界 N03 フィルタ (Rapid#35 part C)
# ----------------------------------------------------------------------

class TestCityBoundaryFilter:
    """`_apply_city_boundary_filter` の挙動。

    PLATEAU は都市別配布だが標準地域メッシュが複数 city にまたがるため、
    共有メッシュ内の建物が両都市の bundle で重複して取り込まれる
    (Rapid#35)。本フィルタは source city の N03 行政界
    (dash_city_master.boundary_geom) に centroid が含まれない建物を import
    の最終段で削除し、cross-city 重複の根本除去をする。

    検証ケース:
      1. 境界内 (within)    : 削除対象 0 件 → DELETE 発行されない
      2. 境界外 (outside)   : 該当 ID を nodes / buildings から 2 段階で削除
      3. NULL boundary      : SQL の `boundary_geom IS NOT NULL` 句で
                              SELECT 結果が空になり、pass-through される
    """

    SQL = None  # 各テストで _build_boundary_filter_select_sql() を再取得

    def test_filter_sql_structure(self):
        """SELECT SQL に Part A と同じ NOT EXISTS 相当の構造が含まれる。

        - dash_city_master を JOIN
        - boundary_geom IS NOT NULL → NULL boundary city は素通り
        - NOT ST_Contains で境界外を選ぶ
        - centroid カラムを使う (Part A と同じ)
        """
        sql = PlateauImporter2PostGIS._build_boundary_filter_select_sql()
        assert 'dash_city_master' in sql
        assert 'boundary_geom IS NOT NULL' in sql
        assert 'NOT ST_Contains' in sql
        assert 'b.centroid' in sql
        # city_code でスコープされている
        assert 'b.city_code = %s' in sql

    def test_within_boundary_keeps_all(self, bare_importer):
        """全件境界内: SELECT が [] → DELETE は呼ばれない、戻り値 (0, 0)"""
        importer = bare_importer(citycode='13203')
        cursor = MagicMock()
        cursor.fetchall.return_value = []  # 境界外 0 件

        b, n = importer._apply_city_boundary_filter(cursor)

        assert (b, n) == (0, 0)
        # SELECT は 1 度だけ呼ばれる (フィルタ判定用)
        assert cursor.execute.call_count == 1
        called_sql = cursor.execute.call_args_list[0][0][0]
        assert 'SELECT' in called_sql
        # DELETE 系は 1 度も呼ばれない
        assert all(
            'DELETE' not in call.args[0]
            for call in cursor.execute.call_args_list
        )

    def test_outside_boundary_deletes_buildings(self, bare_importer):
        """境界外: SELECT が ID リスト → 単一の DELETE FROM plateau_buildings が発行される。

        api#20: ノードと子 part の連鎖削除は ON DELETE CASCADE 任せなので、
        importer 側は親 building の DELETE 1 文だけ走らせる。nodes_deleted は
        CASCADE 経由で計測不能なので戻り値は (buildings_deleted, 0)。
        """
        importer = bare_importer(citycode='13203')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(101,), (102,), (103,)]
        cursor.rowcount = 3  # buildings の DELETE rowcount

        b, n = importer._apply_city_boundary_filter(cursor)

        # 2 回 execute: SELECT, DELETE buildings
        assert cursor.execute.call_count == 2
        sqls = [call.args[0] for call in cursor.execute.call_args_list]
        assert 'SELECT' in sqls[0]
        assert 'DELETE FROM plateau_buildings' in sqls[1]
        # 削除対象 ID は SELECT 結果と一致
        assert cursor.execute.call_args_list[1].args[1] == ([101, 102, 103],)
        # ノード DELETE は importer から直接は発行されない (CASCADE)
        assert all(
            'DELETE FROM plateau_building_nodes' not in call.args[0]
            for call in cursor.execute.call_args_list
        )
        # SAVEPOINT もない (CASCADE で FK violation が起きないため)
        assert all(
            'SAVEPOINT' not in call.args[0]
            for call in cursor.execute.call_args_list
        )
        # 戻り値: buildings_deleted=rowcount, nodes_deleted=0 (CASCADE で計測不能)
        assert b == 3 and n == 0

    def test_null_boundary_passes_through(self, bare_importer):
        """NULL boundary の都市 (13999 / 27999 など): SQL の IS NOT NULL 句で
        SELECT が空となり、DELETE は呼ばれない。

        SELECT 結果のモックは「境界内」ケースと同形だが、テスト名で意図を分離する。
        併せて SQL に IS NOT NULL があることは `test_filter_sql_structure` で保証。
        """
        importer = bare_importer(citycode='13999')
        cursor = MagicMock()
        cursor.fetchall.return_value = []  # 行政界なしなので SELECT 結果も空

        b, n = importer._apply_city_boundary_filter(cursor)

        assert (b, n) == (0, 0)
        # citycode が SELECT パラメータとして渡されていることを確認
        select_params = cursor.execute.call_args_list[0].args[1]
        assert select_params == ('13999',)

    def test_unknown_citycode_skipped(self, bare_importer):
        """citycode='unknown' / None のときはフィルタを完全スキップする
        (誤って他都市の行を巻き込まないための安全策)。
        """
        for code in ('unknown', None):
            importer = bare_importer(citycode='unknown')
            importer.citycode = code  # 直接書き換えて検証対象の値にする
            cursor = MagicMock()
            b, n = importer._apply_city_boundary_filter(cursor)
            assert (b, n) == (0, 0)
            cursor.execute.assert_not_called()

    def test_select_failure_falls_back_to_pass_through(self, bare_importer):
        """dash_city_master 不在等で SELECT が例外を投げても import は止めない。

        本フィルタは Part A (API 側の同等フィルタ) の補助層で、欠落しても
        重複が出るだけで重大な破壊は起きないため pass-through が安全。
        """
        importer = bare_importer(citycode='13203')
        cursor = MagicMock()
        cursor.execute.side_effect = Exception('relation "dash_city_master" does not exist')

        b, n = importer._apply_city_boundary_filter(cursor)

        assert (b, n) == (0, 0)
        # SELECT 1 回で諦め、DELETE は呼ばれない
        assert cursor.execute.call_count == 1

    def test_no_savepoint_or_node_delete_after_cascade_migration(self, bare_importer):
        """api#20 (CASCADE 化) 後: SAVEPOINT もノード DELETE も発行されないこと。

        plateau_migrate_fk_cascade.py で plateau_building_nodes.building_id を
        ON DELETE CASCADE に揃えてあれば、DELETE FROM plateau_buildings 1 文で
        ノードと子 part も連鎖削除される。SAVEPOINT は不要。
        旧来の 2 段階 DELETE + SAVEPOINT パターンが回帰しないことを確認する。
        """
        importer = bare_importer(citycode='13203')
        cursor = MagicMock()
        cursor.fetchall.return_value = [(501,)]
        cursor.rowcount = 1

        importer._apply_city_boundary_filter(cursor)

        sqls = [call.args[0] for call in cursor.execute.call_args_list]
        assert not any('SAVEPOINT' in s for s in sqls), \
            "SAVEPOINT should not appear after the CASCADE migration"
        assert not any('DELETE FROM plateau_building_nodes' in s for s in sqls), \
            "Nodes are now removed via CASCADE; importer should not delete them explicitly"


class TestDiscoverOsmFiles:
    """`_discover_osm_files()` picks the .osm source based on the no_zip flag."""

    FIX_DIR = Path(__file__).parent / 'fixtures' / 'citygml-osm'

    def _place(self, data_dir: Path, rel: str) -> Path:
        """Copy the v4 fixture to data_dir/<rel> and return the path."""
        dest = data_dir / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(self.FIX_DIR / 'v4_outline_only.osm', dest)
        return dest

    def test_no_zip_rglob_finds_nested_osm_and_ignores_zip(self, bare_importer):
        importer = bare_importer(citycode='43100')
        importer.no_zip = True
        data_dir = Path(importer.data_dir)
        self._place(data_dir, 'extracted/53385729/53385729.osm')
        self._place(data_dir, 'extracted/53385730/53385730.osm')
        # A stray .zip must be ignored entirely in no-zip mode.
        (data_dir / 'leftover.zip').write_bytes(b'not a real zip')

        osm_files, zip_count = importer._discover_osm_files()

        names = sorted(p.name for p in osm_files)
        assert names == ['53385729.osm', '53385730.osm']
        assert zip_count == 0

    def test_no_zip_empty_dir_returns_empty(self, bare_importer):
        importer = bare_importer(citycode='43100')
        importer.no_zip = True
        osm_files, zip_count = importer._discover_osm_files()
        assert osm_files == []
        assert zip_count == 0


class TestFileKey:
    """`_file_key()` namespaces meshes by path, not basename, so two files
    that share a basename (adjacent cities share a mesh tile) do not collide."""

    def test_same_basename_different_subdir_distinct_keys(self, bare_importer):
        importer = bare_importer(citycode='43100')
        data_dir = Path(importer.data_dir)
        f1 = data_dir / 'extracted' / '53385729_a' / '53385729.osm'
        f2 = data_dir / 'extracted' / '53385729_b' / '53385729.osm'

        k1 = importer._file_key(f1)
        k2 = importer._file_key(f2)

        assert k1 != k2
        assert k1 == 'extracted/53385729_a/53385729.osm'
        assert k2 == 'extracted/53385729_b/53385729.osm'

    def test_file_outside_data_dir_falls_back_to_full_path(self, bare_importer):
        importer = bare_importer(citycode='43100')
        outside = Path('/somewhere/else/mesh.osm')
        assert importer._file_key(outside) == str(outside)

    def test_flat_layout_keys_to_basename(self, bare_importer):
        """No-op invariant: a file directly in data_dir keys to its bare
        basename, identical to the pre-hardening behavior."""
        importer = bare_importer(citycode='43100')
        data_dir = Path(importer.data_dir)
        f = data_dir / '53385729.osm'
        assert importer._file_key(f) == '53385729.osm'


# ----------------------------------------------------------------------
# way_id namespace per source file
# ----------------------------------------------------------------------

_COLLIDING_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="{lat}" lon="{lon}"/>
  <node id="-2" lat="{lat}1" lon="{lon}"/>
  <node id="-3" lat="{lat}1" lon="{lon}1"/>
  <node id="-4" lat="{lat}" lon="{lon}1"/>
  <node id="-5" lat="{lat}05" lon="{lon}05"/>
  <node id="-6" lat="{lat}08" lon="{lon}05"/>
  <node id="-7" lat="{lat}08" lon="{lon}08"/>
  <node id="-8" lat="{lat}05" lon="{lon}08"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
    <tag k="building" v="yes"/>
    <tag k="name" v="{name}-outline"/>
    <tag k="ref:MLIT_PLATEAU" v="43100-bldg-{name}-10"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
    <tag k="name" v="{name}-part"/>
    <tag k="ref:MLIT_PLATEAU" v="43100-bldg-{name}-20"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outline"/>
    <member type="way" ref="-20" role="part"/>
    <tag k="type" v="building"/>
    <tag k="building" v="yes"/>
  </relation>
</osm>
""")


class TestWayIdNamespacePerFile:
    """citygml-osm numbers each mesh file from -1, so the same way id appears in
    every file of a city.

    Historically this class also verified that the part -> outline link (via
    the `type=building` relation) stayed scoped to its own file. importer
    source-fidelity task 1 stopped reading that relation entirely — it links
    a synthesized merge outline to the real buildings it fused, not a source
    BuildingPart — so no parent link is produced anymore, from any file.
    `test_part_links_to_outline_from_its_own_file` now asserts that instead.
    What's left worth guarding: the same way id (-10/-20) appears in every
    mesh file, so anything keyed only by raw `way_id` (not `(file_key,
    way_id)`) would silently collide across files. `way_id` fixture ways
    below carry `ref:MLIT_PLATEAU` so they still survive the task-1 filter.
    """

    def _batch(self, importer, specs):
        """Parse each (subdir, lat, lon, name) spec and assemble one batch,
        via the same `_merge_parsed_file` that `run_complete_import` calls per
        file — rather than re-implementing the namespacing here by hand."""
        all_nodes = {}
        all_buildings = []
        for subdir, lat, lon, name in specs:
            d = Path(importer.data_dir) / subdir
            d.mkdir(parents=True, exist_ok=True)
            osm_file = d / 'mesh.osm'
            osm_file.write_text(_COLLIDING_OSM.format(lat=lat, lon=lon, name=name))

            importer._merge_parsed_file(all_nodes, all_buildings, osm_file)
        return all_nodes, all_buildings

    def test_part_links_to_outline_from_its_own_file(self, bare_importer):
        """Renamed in spirit only (kept the original name to avoid churn):
        the `type=building` relation is no longer read (importer
        source-fidelity task 1), so no part->outline link is produced at
        all — not a wrong cross-file one, none. `parts_parent_map` is always
        empty. Both ways still carry `ref:MLIT_PLATEAU`, so all 4 buildings
        (2 per mesh file) are still collected as independent buildings.
        """
        importer = bare_importer(citycode='43100')
        all_nodes, all_buildings = self._batch(importer, [
            ('meshA', '33.0', '133.0', 'A'),
            ('meshB', '34.0', '134.0', 'B'),
        ])

        buildings_data, nodes_data, parts_parent_map = importer.process_buildings_safe(
            all_nodes, all_buildings
        )

        assert len(buildings_data) == 4
        # buildings_data row: (osm_id, ..., name at index 9, ...)
        name_by_osm_id = {row[0]: row[9] for row in buildings_data}
        assert sorted(name_by_osm_id.values()) == [
            'A-outline', 'A-part', 'B-outline', 'B-part'
        ]

        assert parts_parent_map == []

    def test_plateau_id_keeps_the_raw_way_id(self, bare_importer):
        """`plateau_id` にファイルキーの名前空間が漏れないことを守る。

        型の頭文字は付くが、その後ろは変換出力の生の要素 id のままである。
        名前空間付与 (mesh.osm:-10 のような形) が混ざると、DB に保存される
        値が変わってしまう。
        """
        importer = bare_importer(citycode='43100')
        all_nodes, all_buildings = self._batch(importer, [
            ('meshA', '33.0', '133.0', 'A'),
            ('meshB', '34.0', '134.0', 'B'),
        ])

        buildings_data, _, _ = importer.process_buildings_safe(all_nodes, all_buildings)

        # row index 7 = plateau_id
        assert sorted(row[7] for row in buildings_data) == ['w-10', 'w-10', 'w-20', 'w-20']


# ----------------------------------------------------------------------
# ref:MLIT_PLATEAU の保存 (#30)
# ----------------------------------------------------------------------

class TestRefMlitPlateau:
    """CityGML の建物 ID (ref:MLIT_PLATEAU) を DB に保存する。

    変換器は way にこの ID (例 '13206-bldg-11049') を付けるが、タグ変換が
    使うタグだけを拾う方式のため取り出されず、保存されていなかった。
    サーバ側の逆追跡と、1つの relation に何棟分の建物が混ざっているかの
    判定に使う。API 出力には載せない。
    """

    def test_tag_is_extracted(self, bare_importer):
        importer = bare_importer(citycode='13206')
        result = importer.convert_building_tags_enhanced(
            {'building': 'yes', 'ref:MLIT_PLATEAU': '13206-bldg-11049'}, 'mesh.osm'
        )
        assert result['ref_mlit_plateau'] == '13206-bldg-11049'

    def test_absent_tag_is_none(self, bare_importer):
        """合成された外形にはこのタグが無いことがある。欠損は異常ではない。"""
        importer = bare_importer(citycode='13206')
        result = importer.convert_building_tags_enhanced({'building': 'yes'}, 'mesh.osm')
        assert result['ref_mlit_plateau'] is None

    def _osm_with_ref(self):
        return textwrap.dedent("""\
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
            <tag k="ref:MLIT_PLATEAU" v="13206-bldg-11049"/>
          </way>
          <way id="-20">
            <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
            <tag k="building:part" v="yes"/>
            <tag k="ref:MLIT_PLATEAU" v="13206-bldg-11049"/>
          </way>
        </osm>
        """)

    def _rows(self, bare_importer):
        importer = bare_importer(citycode='13206')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(self._osm_with_ref())
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['node_refs'] = [f'{key}:{r}' for r in b['node_refs']]
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        rows, _, _ = importer.process_buildings_safe(all_nodes, buildings)
        return rows

    def test_outline_row_carries_the_id(self, bare_importer):
        rows = self._rows(bare_importer)
        # 行のレイアウトは INSERT の列順。ref_mlit_plateau は building_part の次。
        assert rows[0][24] == '13206-bldg-11049'

    def test_part_row_carries_the_id(self, bare_importer):
        """部分立体にも保存する。relation 内の建物の混在を判定するのに要る。"""
        rows = self._rows(bare_importer)
        assert len(rows) == 2
        assert rows[1][24] == '13206-bldg-11049'

    def test_both_insert_statements_list_the_column(self):
        """INSERT は 2 箇所ある。片方を落とすと静かに NULL が入る。"""
        import re
        src = (Path(__file__).parent.parent / 'plateau_importer2postgis.py').read_text()
        inserts = re.findall(
            r'INSERT INTO plateau_buildings\s*\((.*?)\)\s*VALUES', src, re.S
        )
        assert len(inserts) == 2, f'INSERT の数が変わった: {len(inserts)}'
        for cols in inserts:
            assert 'ref_mlit_plateau' in cols

    def test_insert_column_count_matches_the_row(self, bare_importer):
        """列数・プレースホルダ数・行の長さが揃っていること。"""
        import re
        src = (Path(__file__).parent.parent / 'plateau_importer2postgis.py').read_text()
        cols = re.search(
            r'INSERT INTO plateau_buildings\s*\((.*?)\)\s*VALUES', src, re.S
        ).group(1)
        n_cols = len([c for c in cols.replace('\n', ' ').split(',') if c.strip()])
        template = re.search(r'template="\((.*?)\)",', src, re.S).group(1)
        n_placeholders = template.count('%s')
        assert n_cols == n_placeholders
        assert len(self._rows(bare_importer)[0]) == n_cols


class TestNodeRingId:
    """ノード行が環番号を持つ。内側リングを表現するための土台。

    本タスクでは挙動を変えない。穴の無い建物はすべて 0 になる。
    """

    def test_simple_building_nodes_are_ring_zero(self, bare_importer):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_MIN_OSM.replace(
            '<tag k="building" v="yes"/>',
            '<tag k="building" v="yes"/>\n    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>'
        ))
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['node_refs'] = [f'{key}:{r}' for r in b['node_refs']]
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        _, nodes_data, _ = importer.process_buildings_safe(all_nodes, buildings)

        assert nodes_data, 'ノード行が空'
        # 行のレイアウト: (osm_id, building_id, seq, lat, lon, lon, lat, ring_id)
        assert all(len(row) == 8 for row in nodes_data)
        assert all(row[7] == 0 for row in nodes_data)


# ----------------------------------------------------------------------
# 合成形状を取り込まない (importer source-fidelity task 1)
# ----------------------------------------------------------------------

_SYNTH_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.001" lon="133.0"/>
  <node id="-3" lat="33.001" lon="133.001"/>
  <node id="-4" lat="33.0" lon="133.001"/>
  <node id="-5" lat="33.0002" lon="133.0002"/>
  <node id="-6" lat="33.0004" lon="133.0002"/>
  <node id="-7" lat="33.0004" lon="133.0004"/>
  <node id="-8" lat="33.0002" lon="133.0004"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
    <tag k="building" v="yes"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outline"/>
    <member type="way" ref="-20" role="part"/>
    <tag k="type" v="building"/>
    <tag k="building" v="yes"/>
  </relation>
</osm>
""")


class TestDropSynthesizedShapes:
    """変換器が作った合成形状を取り込まない。

    融合で作られた外形は建物 ID を持たない。10 メッシュ 386 本すべてが
    元データの lod0FootPrint と一致しないことを確認済み。
    """

    def _parse(self, bare_importer, xml):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        return importer.parse_osm_file_safe(osm_file)

    def test_way_without_building_id_is_not_collected(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        way_ids = {b['way_id'] for b in buildings}
        assert '-10' not in way_ids, '合成外形が収集されている'
        assert '-20' in way_ids, '建物 ID を持つ way が落ちている'

    def test_no_parent_link_is_produced(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        assert all(b['parent_outline_way_id'] is None for b in buildings)

    def test_part_with_building_id_becomes_a_building(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        by_id = {b['way_id']: b for b in buildings}
        assert by_id['-20']['is_part'] is False


# ----------------------------------------------------------------------
# 穴のある建物 (importer source-fidelity task 3)
# ----------------------------------------------------------------------

_HOLE_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.001" lon="133.0"/>
  <node id="-3" lat="33.001" lon="133.001"/>
  <node id="-4" lat="33.0" lon="133.001"/>
  <node id="-5" lat="33.0003" lon="133.0003"/>
  <node id="-6" lat="33.0007" lon="133.0003"/>
  <node id="-7" lat="33.0007" lon="133.0007"/>
  <node id="-8" lat="33.0003" lon="133.0007"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outer"/>
    <member type="way" ref="-20" role="inner"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building" v="public"/>
    <tag k="height" v="14.6"/>
  </relation>
</osm>
""")


class TestHoleBuilding:
    """穴のある建物は type=multipolygon で出力される。

    周南市 10 メッシュで 15 個。すべて実在建物で、合成形状は含まれない。
    inner の way には building:part=yes が付くが、これは建物ではなく穴である。
    """

    def _rows(self, bare_importer):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_HOLE_OSM)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_hole_building_is_one_row(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert len(buildings_data) == 1, '穴が別の建物として保存されている'

    def test_geometry_has_an_interior_ring(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        wkt = buildings_data[0][8]   # geometry_wkt
        assert wkt.count('(') >= 3, f'内側リングが無い: {wkt[:80]}'

    def test_outer_way_with_its_own_building_tags_is_not_duplicated(self, bare_importer):
        """brief のフィクスチャは outer way にタグが無いので素通りするが、実データ
        では outer way に building + ref:MLIT_PLATEAU が付くことがある。

        outer を単独の建物としても収集してしまうと、multipolygon 由来の1棟と
        合わせて同じジオメトリの建物が2つでき、さらに悪いことに ---
        way ループが multipolygon 組み立てより先に走るため、タグ付き outer が
        先に「穴の無い」単純建物として登録される。その後 multipolygon 側の穴あき
        建物を処理する際、外側の座標が同一なので重複ジオメトリ判定
        (`processed_geometry_hashes`) に引っかかって *穴あきの方が捨てられる*。
        結果、件数だけを見ると 1 件のままなのに、残るのは穴の無い建物になる
        （中庭が塗りつぶされる = このタスクが解決するはずの不具合そのもの）。
        よって件数だけでなく、残った建物が内側リングを保持しているかも確認する。
        """
        osm_with_tagged_outer = _HOLE_OSM.replace(
            '  <way id="-10">\n    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>\n  </way>',
            '  <way id="-10">\n    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>\n'
            '    <tag k="building" v="public"/>\n'
            '    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-9"/>\n  </way>'
        )
        assert osm_with_tagged_outer != _HOLE_OSM, '置換対象の文字列が一致していない'

        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(osm_with_tagged_outer)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        buildings_data, _, _ = importer.process_buildings_safe(all_nodes, buildings)

        assert len(buildings_data) == 1, 'タグ付き outer way が別建物として重複収集されている'
        wkt = buildings_data[0][8]
        assert wkt.count('(') >= 3, f'穴が塗りつぶされた（重複判定で穴あきの方が捨てられた）: {wkt[:80]}'

    def test_inner_nodes_carry_ring_one(self, bare_importer):
        _, nodes_data, _ = self._rows(bare_importer)
        rings = {row[7] for row in nodes_data}
        assert rings == {0, 1}, f'環番号が {rings}'

    def test_tags_come_from_the_relation(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert buildings_data[0][1] == 'public'   # building
        # height は convert_building_tags_enhanced を通るので float になる
        # (他の建物と同じ扱い。brief の '14.6' 文字列は型が合わないための誤り)。
        assert buildings_data[0][2] == 14.6       # height


_HOLE_MALFORMED_INNER_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.001" lon="133.0"/>
  <node id="-3" lat="33.001" lon="133.001"/>
  <node id="-4" lat="33.0" lon="133.001"/>
  <node id="-5" lat="33.0003" lon="133.0003"/>
  <node id="-6" lat="33.0007" lon="133.0003"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/>
    <tag k="building:part" v="yes"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outer"/>
    <member type="way" ref="-20" role="inner"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building" v="public"/>
  </relation>
</osm>
""")


class TestHoleBuildingMalformedInnerRing:
    """内側リングが不正 (閉鎖後4点未満) でも建物全体は捨てない。外側が有効なら
    保存し、壊れた内側リングだけを落とす。inner way -20 は2点しかなく、閉鎖後も
    3点にしかならないため穴として描けない。
    """

    def test_building_survives_with_outer_ring_only(self, bare_importer):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_HOLE_MALFORMED_INNER_OSM)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]

        buildings_data, nodes_data, _ = importer.process_buildings_safe(all_nodes, buildings)

        assert len(buildings_data) == 1, '不正な内側リングのせいで建物ごと落ちている'
        wkt = buildings_data[0][8]
        # 壊れた内側リングは描かれないので、外側だけの単純な POLYGON になる。
        assert wkt.count('(') == 2, f'壊れた内側リングが混入している: {wkt[:80]}'
        # 内側リングのノードは書き込まれない (座標として無効なため)。
        assert {row[7] for row in nodes_data} == {0}


class TestMultipolygonRingsAreNamespacedInProduction:
    """`run_complete_import` が呼ぶのは `_merge_parsed_file` であり、それが
    `node_refs` と同じファイルキーで `rings` も prefix しなければならない。

    ここを穴埋めしないと、実運用ではリングの各座標が `all_nodes` に一件も
    解決できず、建物ごと静かに消える
    （PR #41 で修正した「node 参照の名前空間バグ」と同種の事故）。

    このテストは prefixing をテスト側で再現するのではなく、production が
    実際に呼ぶ `_merge_parsed_file` をそのまま呼んで確認する。もし
    `_merge_parsed_file` の `rings` prefix 行を消すと、outer/inner の参照が
    どちらも `all_nodes` の中の別キーを指すことになり、座標が1点も解決
    できず building_data が空になってこのテストが落ちる。
    """

    def test_rings_resolve_after_merge_parsed_file(self, bare_importer):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_HOLE_OSM)

        all_nodes, all_buildings = {}, []
        importer._merge_parsed_file(all_nodes, all_buildings, osm_file)

        buildings_data, nodes_data, _ = importer.process_buildings_safe(all_nodes, all_buildings)

        assert len(buildings_data) == 1, 'rings が prefix されておらず建物が消えている'
        wkt = buildings_data[0][8]
        assert wkt.count('(') >= 3, f'内側リングの座標が解決できていない: {wkt[:80]}'
        assert {row[7] for row in nodes_data} == {0, 1}


# ----------------------------------------------------------------------
# plateau_id に元要素の型を記録する (importer source-fidelity task 3)
# ----------------------------------------------------------------------

class TestPlateauIdCarriesTheElementType:
    """`plateau_id` は元要素の型が判る形で記録する。

    OSM は way と relation の id 空間を分けているので、生の数字だけでは
    変換出力のどの要素から来たかを特定できない。`source_dataset` に
    ファイル名が入っているため、型さえ足せば一意に決まる。

    書き方は osmEntity.id.fromOSM と同じで、way -10 なら 'w-10' になる。
    安定した識別子ではない (変換をやり直すと値が変わる)。その役割は
    ref_mlit_plateau が担う。
    """

    def _parse(self, bare_importer, xml):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        return importer.parse_osm_file_safe(osm_file)

    def test_way_derived_building_is_prefixed_with_w(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        by_way = {b['way_id']: b for b in buildings}
        assert by_way['-20']['plateau_id'] == 'w-20'

    def test_way_id_itself_is_not_rewritten(self, bare_importer):
        """`way_id` は親子解決のキーなので、前置してはいけない。"""
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        assert all(not b['way_id'].startswith('w') for b in buildings)

    def test_multipolygon_derived_building_is_prefixed_with_r(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _HOLE_OSM)
        assert len(buildings) == 1
        assert buildings[0]['plateau_id'] == 'r-30'


# ----------------------------------------------------------------------
# 内側リングの最大本数を取り込みレポートに出す (importer source-fidelity task 4)
# ----------------------------------------------------------------------

class TestRingCountIsReported:
    """内側リングの最大本数を取り込みログに出す。

    API の way id 採番は環番号が 1000 未満であることを前提にしている。
    取り込み側は何も捨てないので、本数が上限に近づいていないかを
    人が見られるようにしておく。
    """

    def test_max_inner_ring_count_is_logged(self, bare_importer, caplog):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_HOLE_OSM)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]

        with caplog.at_level(logging.INFO):
            importer.process_buildings_safe(all_nodes, buildings)

        assert any('内側リングの最大本数' in r.message for r in caplog.records)
        assert any('内側リングの最大本数: 1' in r.message for r in caplog.records)

    def _many_ring_osm(self, inner_rings):
        """内側リングを任意の本数だけ持つ multipolygon を組み立てる。"""
        nodes, ways, members = [], [], []
        nid = -1

        def square(lat, lon, d):
            nonlocal nid
            refs = []
            for dlat, dlon in ((0, 0), (d, 0), (d, d), (0, d)):
                nodes.append(f'<node id="{nid}" lat="{lat + dlat}" lon="{lon + dlon}"/>')
                refs.append(nid)
                nid -= 1
            return refs

        outer = square(33.0, 133.0, 0.5)
        ways.append('<way id="-10">'
                    + ''.join(f'<nd ref="{r}"/>' for r in outer + outer[:1])
                    + '</way>')
        members.append('<member type="way" ref="-10" role="outer"/>')
        for r in range(1, inner_rings + 1):
            wid = -1000 - r
            refs = square(33.0 + 0.0001 * r, 133.0 + 0.0001 * r, 0.00002)
            ways.append(f'<way id="{wid}">'
                        + ''.join(f'<nd ref="{x}"/>' for x in refs + refs[:1])
                        + '</way>')
            members.append(f'<member type="way" ref="{wid}" role="inner"/>')
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6">'
            + ''.join(nodes) + ''.join(ways)
            + '<relation id="-30">' + ''.join(members)
            + '<tag k="type" v="multipolygon"/><tag k="building" v="yes"/></relation>'
            + '</osm>'
        )

    def _process(self, bare_importer, xml):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer, importer.process_buildings_safe(all_nodes, buildings)

    def test_building_with_1000_rings_is_still_imported(self, bare_importer):
        """上限は API の制約なので、取り込みでは何も捨てない。

        API が出力できない本数でも DB には入る。式を直せば次のリクエストから
        出るという設計を、取り込み側で担保する。
        外側 1 本 + 内側 1000 本、計 1001 本すべてが ring_id として残ることを
        確認する。途中の環だけが under-4-points 分岐で落ちても、
        最大値だけを見る assert では検出できない。
        """
        xml = self._many_ring_osm(1000)
        _, (buildings_data, nodes_data, _) = self._process(bare_importer, xml)
        assert len(buildings_data) == 1, '環が多い建物が取り込みで捨てられている'
        ring_ids = {row[7] for row in nodes_data}
        assert len(ring_ids) == 1001, '外側+内側1000本のうちどれかの環が欠けている'
        assert max(ring_ids) == 1000, '環番号が 1000 まで無い'

    def test_max_ring_count_reflects_1000_rings(self, bare_importer, caplog):
        xml = self._many_ring_osm(1000)
        with caplog.at_level(logging.INFO):
            self._process(bare_importer, xml)
        assert any('内側リングの最大本数: 1000' in r.message for r in caplog.records)
