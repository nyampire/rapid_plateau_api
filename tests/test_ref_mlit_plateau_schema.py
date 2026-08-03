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
