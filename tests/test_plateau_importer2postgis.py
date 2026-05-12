"""
plateau_importer2postgis.py のユニットテスト

主に DB に投入する直前のノード行に対する重複排除ロジック
(`PlateauImporter2PostGIS._dedupe_and_remap_nodes`) を検証する。
"""

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
