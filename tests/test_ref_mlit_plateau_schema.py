"""`_ensure_schema` が ref_mlit_plateau 列を冪等に追加すること (#30)。

本番では 14.9M 行の既存テーブルに列を追加する経路を通る。ここが失敗すると
取り込み自体が止まるので、列が無い状態から始めて追加されること、および
2 回呼んでも壊れないことを実 DB で確認する。

`tests/test_plateau_importer2postgis.py` に置かないのは、あちらが DB 接続を
持たない単体テスト向けだから。
"""
import os

import pytest

from plateau_importer2postgis import PlateauImporter2PostGIS


@pytest.mark.integration
class TestEnsureSchemaAddsRefColumn:
    def _columns(self, conn):
        cur = conn.cursor()
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'plateau_buildings'
        """)
        return {r[0] for r in cur.fetchall()}

    def test_column_is_added_when_missing_and_call_is_idempotent(
        self, fresh_plateau_full_schema, integration_db_url, tmp_path, monkeypatch
    ):
        conn = fresh_plateau_full_schema
        cur = conn.cursor()
        cur.execute("ALTER TABLE plateau_buildings DROP COLUMN IF EXISTS ref_mlit_plateau")
        conn.commit()
        assert 'ref_mlit_plateau' not in self._columns(conn)

        monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
        monkeypatch.setattr(
            PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None
        )
        data_dir = tmp_path / '13206'
        data_dir.mkdir()
        importer = PlateauImporter2PostGIS(
            data_dir=str(data_dir), postgres_url=integration_db_url, citycode='13206'
        )

        importer._ensure_schema()
        assert 'ref_mlit_plateau' in self._columns(conn)

        # 2 回目は何も起きない (既存 import を壊さない)
        importer._ensure_schema()
        assert 'ref_mlit_plateau' in self._columns(conn)


@pytest.mark.integration
class TestInsertActuallyStoresTheId:
    """INSERT を実際に実行して、値が正しい列に入ることを確認する (#30)。

    列数だけ合っていて順序がずれていると、値は別の列に入る。それは文字列を
    見るテストも行の長さを見るテストも通過し、本番の取り込みで初めて壊れる。
    ここだけが実行して確かめる経路。
    """

    def _importer(self, integration_db_url, tmp_path, monkeypatch):
        monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
        monkeypatch.setattr(
            PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None
        )
        monkeypatch.setattr(PlateauImporter2PostGIS, '_ensure_schema', lambda self: None)
        data_dir = tmp_path / '13206'
        data_dir.mkdir()
        return PlateauImporter2PostGIS(
            data_dir=str(data_dir), postgres_url=integration_db_url, citycode='13206'
        )

    def test_row_lands_in_the_right_columns(
        self, fresh_plateau_full_schema, integration_db_url, tmp_path, monkeypatch
    ):
        conn = fresh_plateau_full_schema
        importer = self._importer(integration_db_url, tmp_path, monkeypatch)

        wkt = ('POLYGON((133.0 33.0,133.001 33.0,133.001 33.001,'
               '133.0 33.001,133.0 33.0))')
        # process_buildings_safe が組み立てるのと同じレイアウト
        row = (
            -1,                      # osm_id
            'apartments',            # building
            '12.3',                  # height
            '4.5',                   # ele
            '3',                     # building_levels
            None,                    # building_levels_underground
            'plateau_13206_mesh',    # source_dataset
            '-10',                   # plateau_id
            wkt,                     # geometry_wkt
            'テストビル',              # name
            '1-2-3',                 # addr_full
            '1-2-3',                 # addr_housenumber
            None,                    # addr_street
            '2020',                  # start_date
            'concrete',              # building_material
            None,                    # roof_material
            'flat',                  # roof_shape
            None, None, None, None, None,   # amenity/shop/tourism/leisure/landuse
            '13206',                 # city_code
            None,                    # building_part
            '13206-bldg-11049',      # ref_mlit_plateau
            wkt, wkt,                # geom, centroid
        )

        assert importer.insert_to_database_batch([row], [], None) is True

        cur = conn.cursor()
        cur.execute("""
            SELECT osm_id, building, plateau_id, city_code,
                   building_part, ref_mlit_plateau, name, start_date
            FROM plateau_buildings
        """)
        got = cur.fetchall()
        assert len(got) == 1
        osm_id, building, plateau_id, city, part, ref, name, start = got[0]
        assert ref == '13206-bldg-11049'
        # 隣接する列に染み出していないこと
        assert building == 'apartments'
        assert plateau_id == '-10'
        assert city == '13206'
        assert part is None
        assert name == 'テストビル'
        assert start == '2020'
