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
    <tag k="ref:MLIT_PLATEAU" v="bldg_1f0d1f4e-2a2e-4a6f-9d0c-9a3a2f4c6b11"/>
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
    <tag k="ref:MLIT_PLATEAU" v="bldg_1f0d1f4e-2a2e-4a6f-9d0c-9a3a2f4c6b12"/>
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
            + '<tag k="type" v="multipolygon"/><tag k="building" v="yes"/>'
            + '<tag k="ref:MLIT_PLATEAU" v="35215-bldg-77777"/></relation>'
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


class TestTriangleAreaInSquareMeters:
    """三角形の面積判定を m² で行う。

    従来は shoelace の結果 (度の二乗) を閾値 1e-6 とそのまま比べていた。
    1e-6 度² は緯度 34 度でおよそ 10,190 m² にあたるので、1 万 m² 未満の
    三角形がすべて落ちていた。周南市の 2 メッシュでは実在の建物が 2 棟
    (およそ 670 m² と 16.5 m²) 落ちていた。

    本番 1,489 万棟の実データの最小面積は 0.3 m² なので、閾値 0.1 m² は
    実データに触れない。degenerate に対する番人としては残る。
    """

    def _tri(self, lon, lat, dlon, dlat):
        """(lon, lat) を直角の頂点とする直角三角形。閉鎖点を含めて 4 点。"""
        p0 = (lon, lat)
        p1 = (lon + dlon, lat)
        p2 = (lon, lat + dlat)
        return [p0, p1, p2, p0]

    def test_area_is_returned_in_square_meters(self):
        # 緯度 34 度、経度方向 0.0001 度、緯度方向 0.0001 度の直角三角形。
        # 緯度 1 度 = 111,320 m、経度 1 度 = 111,320 * cos(34°) = 92,296 m。
        # 底辺 9.23 m、高さ 11.13 m、面積 = 9.23 * 11.13 / 2 ≈ 51.4 m²
        from plateau_importer2postgis import _triangle_area_m2
        got = _triangle_area_m2(self._tri(135.0, 34.0, 0.0001, 0.0001))
        assert 50 < got < 53, f'm² になっていない: {got}'

    def test_longitude_shrinks_with_latitude(self):
        """同じ度数の三角形が、緯度によって違う m² になる。"""
        from plateau_importer2postgis import _triangle_area_m2
        south = _triangle_area_m2(self._tri(135.0, 26.0, 0.0001, 0.0001))
        north = _triangle_area_m2(self._tri(135.0, 44.0, 0.0001, 0.0001))
        assert south > north, '経度方向の縮尺補正が効いていない'

    def test_threshold_is_a_tenth_of_a_square_meter(self):
        from plateau_importer2postgis import TINY_AREA_M2
        assert TINY_AREA_M2 == 0.1


_TRIANGLE_OSM_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="{lat0}" lon="{lon0}"/>
  <node id="-2" lat="{lat1}" lon="{lon1}"/>
  <node id="-3" lat="{lat2}" lon="{lon2}"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-1"/>
    <tag k="building" v="yes"/>
    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>
  </way>
</osm>
"""


class TestTriangleImport:
    """三角形の建物が面積で落ちるかどうか。"""

    def _import(self, bare_importer, dlon, dlat, lat=34.0, lon=135.0):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_TRIANGLE_OSM_TEMPLATE.format(
            lat0=lat, lon0=lon,
            lat1=lat, lon1=lon + dlon,
            lat2=lat + dlat, lon2=lon,
        ))
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_small_real_building_is_imported(self, bare_importer):
        # およそ 16 m² の三角形。実測で落ちていた大きさ。
        # 底辺 5.5 m (0.00006 度)、高さ 5.6 m (0.00005 度) で約 15.4 m²
        buildings_data, _, _ = self._import(bare_importer, 0.00006, 0.00005)
        assert len(buildings_data) == 1, '実在の三角形が落ちている'

    def test_larger_real_building_is_imported(self, bare_importer):
        # およそ 620 m² の三角形。実測で落ちていたもう一方 (約 670 m²) に近い大きさ。
        # 底辺 36.9 m (0.0004 度)、高さ 33.4 m (0.0003 度)。
        buildings_data, _, _ = self._import(bare_importer, 0.0004, 0.0003)
        assert len(buildings_data) == 1

    def test_degenerate_triangle_is_dropped(self, bare_importer):
        # 0.1 m² を下回る三角形。ほぼ潰れた形。
        # 底辺 0.9 m (0.00001 度)、高さ 0.11 m (0.000001 度) で約 0.05 m²
        buildings_data, _, _ = self._import(bare_importer, 0.00001, 0.000001)
        assert len(buildings_data) == 0, 'degenerate な三角形が取り込まれている'


# ----------------------------------------------------------------------
# 融合で building:part に降格した建物 (#39 の続き)
# ----------------------------------------------------------------------

_DEMOTED_MP_OSM = textwrap.dedent("""\
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
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outer"/>
    <member type="way" ref="-20" role="inner"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building:part" v="public"/>
    <tag k="name" v="市立秋月小学校"/>
    <tag k="height" v="17.4"/>
    <tag k="ref:MLIT_PLATEAU" v="bldg_1f0d1f4e-2a2e-4a6f-9d0c-9a3a2f4c6b13"/>
  </relation>
</osm>
""")


class TestDemotedMultipolygonIsABuilding:
    """融合は中庭のある建物を building:part に降格させる。

    降格しても実在建物であることは変わらない。`building` キーが無いことを理由に
    落とすと、建物ごと DB から消える。周南市 81 メッシュの実測では
    multipolygon 161 個のうち 44 個が降格しており、そのうち 12 個は形も
    どこにも残らず、6 個は名前も失われていた。学校・病院・特養が含まれる。

    way 側の判定 (`is_building or is_part`) は元から両方を受けている。
    multipolygon 側だけが `building` キーを要求していた非対称を揃える。
    """

    def _rows(self, bare_importer, xml=_DEMOTED_MP_OSM):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_demoted_multipolygon_is_collected(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert len(buildings_data) == 1, '降格した中庭建物が取り込まれていない'

    def test_it_keeps_its_courtyard(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        wkt = buildings_data[0][8]
        assert wkt.count('(') >= 3, f'内側リングが無い: {wkt[:80]}'

    def test_its_name_reaches_the_database(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert '市立秋月小学校' in buildings_data[0], '名前が保存されていない'


class TestDemotedTypeIsTheBuildingType:
    """変換器は融合のときキーだけを building から building:part に変え、値は残す。

    周南市 81 メッシュ・66,319 棟の実測で、`building:part=house` の way は
    元データの用途 411、`building=house` の way も 411 と一致した。
    12 の型すべてで最頻用途が一致する。値の読み替えは推測ではない。
    """

    def _building_value(self, bare_importer, tags_xml):
        importer = bare_importer(citycode='35215')
        original = '    <tag k="building:part" v="public"/>\n'
        assert original in _DEMOTED_MP_OSM, '置換対象の文字列が一致していない'
        xml = _DEMOTED_MP_OSM.replace(original, tags_xml)
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        buildings_data, _, _ = importer.process_buildings_safe(all_nodes, buildings)
        assert buildings_data, '建物が取り込まれていない'
        return buildings_data[0][1]

    def test_demoted_type_becomes_the_building_type(self, bare_importer):
        assert self._building_value(
            bare_importer, '    <tag k="building:part" v="public"/>\n') == 'public'

    def test_demoted_yes_stays_yes(self, bare_importer):
        assert self._building_value(
            bare_importer, '    <tag k="building:part" v="yes"/>\n') == 'yes'

    def test_building_tag_wins_when_both_are_present(self, bare_importer):
        assert self._building_value(
            bare_importer,
            '    <tag k="building" v="school"/>\n'
            '    <tag k="building:part" v="public"/>\n') == 'school'


class TestDemotedWayKeepsItsType:
    """way 側も同じ降格を受ける。周南市 81 メッシュで 5,287 本、
    うち 3,372 本が house / public / industrial などの型を持つ。
    これらは取り込まれてはいるが building=yes に潰されていた。
    """

    def _building_value(self, bare_importer, part_value):
        importer = bare_importer(citycode='35215')
        xml = textwrap.dedent(f"""\
            <?xml version="1.0" encoding="UTF-8"?>
            <osm version="0.6">
              <node id="-1" lat="33.0" lon="133.0"/>
              <node id="-2" lat="33.001" lon="133.0"/>
              <node id="-3" lat="33.001" lon="133.001"/>
              <node id="-4" lat="33.0" lon="133.001"/>
              <way id="-10">
                <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
                <tag k="building:part" v="{part_value}"/>
                <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>
              </way>
            </osm>
            """)
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        buildings_data, _, _ = importer.process_buildings_safe(all_nodes, buildings)
        assert buildings_data, '建物が取り込まれていない'
        return buildings_data[0][1]

    def test_demoted_house_way_is_stored_as_house(self, bare_importer):
        assert self._building_value(bare_importer, 'house') == 'house'

    def test_demoted_yes_way_stays_yes(self, bare_importer):
        assert self._building_value(bare_importer, 'yes') == 'yes'


# ----------------------------------------------------------------------
# 二重出力の統合 (穴を潰した way と 穴のある multipolygon)
# ----------------------------------------------------------------------

_TWIN_OSM = textwrap.dedent("""\
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
    <tag k="building:part" v="public"/>
    <tag k="name" v="白鳩学園育成館"/>
    <tag k="height" v="10.9"/>
    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-25793"/>
  </way>
  <way id="-20">
    <nd ref="-3"/><nd ref="-4"/><nd ref="-1"/><nd ref="-2"/><nd ref="-3"/>
  </way>
  <way id="-30">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
  </way>
  <relation id="-40">
    <member type="way" ref="-20" role="outer"/>
    <member type="way" ref="-30" role="inner"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building:part" v="public"/>
    <tag k="name" v="白鳩学園育成館"/>
    <tag k="height" v="10.9"/>
    <tag k="ref:MLIT_PLATEAU" v="bldg_2621ed43-ee4a-45ff-87b5-3fb42e2f8f05"/>
  </relation>
</osm>
""")


class TestTwinMultipolygonAndWayAreOneBuilding:
    """変換器は穴のある建物を 2 つの要素で出す。

    穴を潰した way は建物 ID を持ち、穴のある multipolygon は gml:id しか持たない。
    **片方ずつしか持っていない。**どちらを捨てても何かを失う。

    周南市 81 メッシュの実測: 降格した multipolygon 40 件のうち 27 件に相手の way がある。
    27 件すべてで、元データの gml:id -> uro:buildingID が相手の way の
    ref:MLIT_PLATEAU と一致した。タグの食い違いも twin の重複も 0 件。
    building タグ付きの multipolygon 117 件には相手の way が 1 つも無い。

    way のループが先に走るので、統合しないと穴無しが先に登録され、multipolygon は
    重複ジオメトリ判定 (外側リングのみでハッシュする) に当たって捨てられる。
    つまり建物としては入るが中庭が塗り潰される。

    フィクスチャの way -10 と way -20 は、始点の位置が違う同じリングにしてある。
    実データでも出力順は揃っていないので、比較は始点と向きの違いを吸収する必要がある。

    撤去条件: 変換器が 1 棟を 1 要素で出すようになり、実データで twin が 0 件になったとき。
    """

    def _rows(self, bare_importer, xml=_TWIN_OSM):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_the_pair_becomes_one_building(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert len(buildings_data) == 1, '二重出力が 2 棟として保存されている'

    def test_the_courtyard_survives(self, bare_importer):
        """ジオメトリは multipolygon 側から取る。"""
        buildings_data, _, _ = self._rows(bare_importer)
        wkt = buildings_data[0][8]
        assert wkt.count('(') >= 3, f'中庭が塗り潰されている: {wkt[:80]}'

    def test_the_building_id_survives(self, bare_importer):
        """識別子は way 側から取る。multipolygon は gml:id しか持たない。"""
        buildings_data, _, _ = self._rows(bare_importer)
        assert '35215-bldg-25793' in buildings_data[0], (
            '建物 ID が失われている (gml:id を保存してしまっている可能性)')

    def test_a_multipolygon_without_a_twin_keeps_its_own_identifier(self, bare_importer):
        """相手の way が無い 13/40 は gml:id のまま。取り込みでは埋められない。"""
        xml = _TWIN_OSM.replace(
            '    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-25793"/>\n', '')
        assert xml != _TWIN_OSM, '置換対象の文字列が一致していない'
        buildings_data, _, _ = self._rows(bare_importer, xml)
        assert len(buildings_data) == 1
        assert 'bldg_2621ed43-ee4a-45ff-87b5-3fb42e2f8f05' in buildings_data[0]

    def test_a_way_with_a_different_ring_is_not_merged(self, bare_importer):
        """外側リングが一致しない way は別の建物。統合してはいけない。"""
        xml = _TWIN_OSM.replace(
            '    <nd ref="-3"/><nd ref="-4"/><nd ref="-1"/><nd ref="-2"/><nd ref="-3"/>\n',
            '    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>\n')
        assert xml != _TWIN_OSM, '置換対象の文字列が一致していない'
        buildings_data, _, _ = self._rows(bare_importer, xml)
        assert len(buildings_data) == 2, 'リングが違うのに統合されている'


class TestCitygmlOsmCourtyardFixtures:
    """中庭のある建物まわりの出力 contract を固定する。

    上流 citygml-osm のコードをそのまま使う方針なので、バージョンを上げると
    出力が変わりうる。**本当の risk は取り込み側の仮定ではなく、変換器の出力が
    黙って変わること。**とくに「合成外形は建物 ID を持たない」は上流 #149 が
    `newOutline.removeTag` を削除しており崩れうる。崩れると実在しない建物が
    DB に入るが、今それを検出する手段が無い。

    fixture が失敗したら、まず変換器の出力が変わっていないかを疑うこと。
    測り直す手順は docs/converter-output.md にある。

    **落ちるのは「こちらが壊れる変化」だけである。**出力を 4 通りに変えて確かめた:
    二重出力がやむ / 型が `building:part` の値から消える / `outer` 無しに outer が付く
    の 3 つは落ちる。降格がやんで `building` キーが戻る変化は落ちない。
    取り込みは `building` と `building:part` の両方を受けるので困らないからである。
    回避策を撤去してよいかの判断は、実データで twin が 0 件になったかで行う。
    """

    FIX = (Path(__file__).parent / 'fixtures' / 'citygml-osm'
           / 'v3_courtyard_shapes.osm')

    def _rows(self, bare_importer):
        importer = bare_importer(citycode='35215')
        nodes, buildings = importer.parse_osm_file_safe(self.FIX)
        key = importer._file_key(self.FIX)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def _by_name(self, buildings_data, name):
        hits = [r for r in buildings_data
                if any(isinstance(x, str) and x == name for x in r)]
        return hits

    def test_the_fixture_parses_and_yields_the_expected_buildings(self, bare_importer):
        """fixture が壊れていると、以下の「無いこと」を見る検査が空振りで通る。

        XML が読めないと parse_osm_file_safe は WARNING を出して空を返すだけなので、
        件数を先に固定しておく。中庭建物 1 棟 + 型つき way 1 棟 = 2 棟。
        """
        rows, _, _ = self._rows(bare_importer)
        assert len(rows) == 2, f'fixture から {len(rows)} 棟。XML が壊れていないか確認'

    def test_demoted_multipolygon_is_a_building(self, bare_importer):
        """A: building キーが無くても建物として扱う。"""
        rows, _, _ = self._rows(bare_importer)
        assert self._by_name(rows, 'テスト中庭建物'), '降格した中庭建物が落ちている'

    def test_the_twin_pair_is_one_building_with_its_hole_and_id(self, bare_importer):
        """B: 形は multipolygon から、識別子は way から。1 棟にまとまる。"""
        rows, _, _ = self._rows(bare_importer)
        hits = self._by_name(rows, 'テスト中庭建物')
        assert len(hits) == 1, f'二重出力が {len(hits)} 棟になっている'
        assert hits[0][8].count('(') >= 3, '中庭が塗り潰されている'
        assert '35215-bldg-25793' in hits[0], '建物 ID が gml:id に置き換わっている'

    def test_the_type_comes_from_the_building_part_value(self, bare_importer):
        """C: 融合はキーだけを変えて値は残す。"""
        rows, _, _ = self._rows(bare_importer)
        hits = [r for r in rows if '35215-bldg-11111' in r]
        assert len(hits) == 1
        assert hits[0][1] == 'house', f'型が {hits[0][1]!r} に潰れている'

    def test_a_multipolygon_without_an_outer_is_skipped(self, bare_importer):
        """D: 外形が無いので保存できない。落ちずにスキップすること。"""
        rows, _, _ = self._rows(bare_importer)
        assert not self._by_name(rows, '外形のない建物'), (
            '外形の無い multipolygon から建物が作られている')

    def test_the_upstream_extensions_are_still_ignored(self, bare_importer):
        """`area` / `fix` / `visible` / `<complete>` は OSM のタグではない。"""
        importer = bare_importer(citycode='35215')
        _, buildings = importer.parse_osm_file_safe(self.FIX)
        for b in buildings:
            for k in ('area', 'fix', 'visible', 'complete'):
                assert k not in b['tags'], f'{k} がタグとして読まれている'


# ----------------------------------------------------------------------
# 識別子を持たない multipolygon は融合外形である
# ----------------------------------------------------------------------

_MERGE_OUTLINE_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.002" lon="133.0"/>
  <node id="-3" lat="33.002" lon="133.002"/>
  <node id="-4" lat="33.0" lon="133.002"/>
  <node id="-5" lat="33.0012" lon="133.0012"/>
  <node id="-6" lat="33.0016" lon="133.0012"/>
  <node id="-7" lat="33.0016" lon="133.0016"/>
  <node id="-8" lat="33.0012" lon="133.0016"/>
  <node id="-11" lat="33.0002" lon="133.0002"/>
  <node id="-12" lat="33.0008" lon="133.0002"/>
  <node id="-13" lat="33.0008" lon="133.0008"/>
  <node id="-14" lat="33.0002" lon="133.0008"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
  </way>
  <way id="-30">
    <nd ref="-11"/><nd ref="-12"/><nd ref="-13"/><nd ref="-14"/><nd ref="-11"/>
    <tag k="building:part" v="school"/>
    <tag k="name" v="市立大津島小学校"/>
    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-22222"/>
  </way>
  <relation id="-40">
    <member type="way" ref="-10" role="outer"/>
    <member type="way" ref="-20" role="inner"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building:part" v="school"/>
    <tag k="name" v="市立大津島小学校"/>
    <tag k="height" v="12.3"/>
  </relation>
  <relation id="-50">
    <member type="relation" ref="-40" role="outline"/>
    <member type="way" ref="-30" role="part"/>
    <tag k="type" v="building"/>
    <tag k="building" v="school"/>
  </relation>
</osm>
""")


class TestIdentifierlessMultipolygonIsAMergeOutline:
    """識別子を持たない multipolygon は、融合された建物群の外形である。

    変換器は融合のとき `ElementRelation.margeTagValue` で outline メンバーから
    `ref:MLIT_PLATEAU` を外す。呼び出しは `RelationMarge.matomeru` の 1 箇所だけで、
    融合の経路にしか無い。よって multipolygon が識別子を失うのは、
    融合された relation の outline になったときに限られる。

    実測でも 2 都市 2 年度で一致した。

    | | 識別子なしで取り込まれる mp | うち他の建物を含む |
    |---|---|---|
    | 宇城市 43213 (2025 V5、57 メッシュ) | 27 | 27 |
    | 周南市 35215 (2024 v4、81 メッシュ) | 28 | 28 |

    周南市の 28 個は 27 個が `type=building` relation の outline メンバーで、
    その relation の part は 2 つ以上の建物 ID を持つ。
    識別子を持つ 129 個には outline メンバーが 1 つも無い。

    落としても名前は失わない。28 個のうち name を持つ 13 個は、
    13 個すべて同じ name が同じメッシュの取り込み対象にも付いていた。
    """

    def _rows(self, bare_importer, xml=_MERGE_OUTLINE_OSM):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_the_merge_outline_is_not_imported(self, bare_importer):
        """飲み込まれた建物だけが残る。外形は実在しない建物である。"""
        rows, _, _ = self._rows(bare_importer)
        assert len(rows) == 1, (
            f'融合外形が建物として取り込まれている (行数 {len(rows)})')
        assert '35215-bldg-22222' in rows[0], '実在建物のほうが落ちている'

    def test_no_building_ends_up_inside_another(self, bare_importer):
        """これが「建物の中に建物」の作られ方だった。"""
        rows, _, _ = self._rows(bare_importer)
        assert len(rows) == 1

    def test_a_multipolygon_with_a_gml_id_is_still_imported(self, bare_importer):
        """識別子を持つ 129 個は実在の中庭建物である。落としてはいけない。"""
        xml = _MERGE_OUTLINE_OSM.replace(
            '    <tag k="height" v="12.3"/>\n',
            '    <tag k="height" v="12.3"/>\n'
            '    <tag k="ref:MLIT_PLATEAU"'
            ' v="bldg_2621ed43-ee4a-45ff-87b5-3fb42e2f8f05"/>\n')
        assert xml != _MERGE_OUTLINE_OSM, '置換対象の文字列が一致していない'
        rows, _, _ = self._rows(bare_importer, xml)
        assert len(rows) == 2, '中庭建物が落とされている'

    def test_an_identifier_from_a_twin_way_still_counts(self, bare_importer):
        """判定は twin の解決より後に置くこと。

        現行の変換出力に、識別子を持たない multipolygon と組になる way は無い
        (融合外形の外側リングは合成物なので、建物 ID を持つ way と一致しない)。
        判定を twin の解決より前に置くと、将来そういう出力が出たとき、
        識別子を持つ実在建物を黙って落とす。順序を固定するための test である。
        """
        xml = _MERGE_OUTLINE_OSM.replace(
            '  <way id="-10">\n'
            '    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>\n'
            '  </way>\n',
            '  <way id="-10">\n'
            '    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>\n'
            '  </way>\n'
            '  <way id="-60">\n'
            '    <nd ref="-3"/><nd ref="-4"/><nd ref="-1"/><nd ref="-2"/><nd ref="-3"/>\n'
            '    <tag k="building" v="school"/>\n'
            '    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-33333"/>\n'
            '  </way>\n')
        assert xml != _MERGE_OUTLINE_OSM, '置換対象の文字列が一致していない'
        rows, _, _ = self._rows(bare_importer, xml)
        hits = [r for r in rows if '35215-bldg-33333' in r]
        assert len(hits) == 1, 'twin から識別子を受け取る建物が落ちている'
        assert hits[0][8].count('(') >= 3, '中庭が塗り潰されている'
