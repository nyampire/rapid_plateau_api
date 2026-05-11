"""
plateau_purge.py のテスト
"""

from unittest.mock import MagicMock, patch
from io import StringIO

import pytest

import plateau_purge
from plateau_purge import (
    ADVISORY_LOCK_ID,
    AUDIT_TABLE_DDL,
    Purger,
    confirm_execute,
)


# ----------------------------------------------------------------------
# 定数 / DDL
# ----------------------------------------------------------------------

class TestConstants:
    def test_advisory_lock_id_is_int(self):
        assert isinstance(ADVISORY_LOCK_ID, int)
        assert ADVISORY_LOCK_ID > 0

    def test_audit_table_ddl_has_required_columns(self):
        """監査テーブルに必要なカラムが揃っている"""
        for col in [
            'city_code', 'buildings_deleted', 'nodes_deleted',
            'executed_at', 'executed_by', 'hostname', 'duration_seconds'
        ]:
            assert col in AUDIT_TABLE_DDL

    def test_audit_table_uses_if_not_exists(self):
        """CREATE は IF NOT EXISTS で冪等"""
        assert 'CREATE TABLE IF NOT EXISTS plateau_purge_history' in AUDIT_TABLE_DDL


# ----------------------------------------------------------------------
# Purger 基本
# ----------------------------------------------------------------------

class TestPurger:
    def test_citycode_stored(self):
        p = Purger('13112', 'postgresql://x')
        assert p.citycode == '13112'

    def test_postgres_url_from_env(self, monkeypatch):
        monkeypatch.setenv('DATABASE_URL', 'postgresql://env')
        p = Purger('13112')
        assert p.postgres_url == 'postgresql://env'

    def test_check_city_code_column_exists(self, monkeypatch, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('city_code',)
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        assert p.check_city_code_column_exists() is True

    def test_check_city_code_column_missing(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = None
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        assert p.check_city_code_column_exists() is False

    def test_check_audit_table_exists_true(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('plateau_purge_history',)
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        assert p.check_audit_table_exists() is True

    def test_count_target_rows(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        # 2回 fetchone される（buildings, nodes の順）
        cursor.fetchone.side_effect = [(262658,), (2121054,)]
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        buildings, nodes = p.count_target_rows()
        assert buildings == 262658
        assert nodes == 2121054

    def test_count_target_rows_uses_city_code_filter(self, mock_connection):
        """SELECT 文に city_code = %s が含まれる（LIKE ではない）"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.side_effect = [(0,), (0,)]
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        p.count_target_rows()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        for sql in executed_sqls:
            assert 'city_code = %s' in sql
            assert 'LIKE' not in sql

    def test_acquire_lock_success(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (True,)
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        assert p.acquire_lock() is True

    def test_acquire_lock_failure(self, mock_connection):
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (False,)
        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        assert p.acquire_lock() is False


# ----------------------------------------------------------------------
# DRY RUN
# ----------------------------------------------------------------------

class TestDryRun:
    def test_dry_run_reports_no_data_for_zero_count(self, monkeypatch, mock_connection):
        """データなしの city_code は error=no_data"""
        cursor = mock_connection.cursor.return_value
        # 順に: city_code カラムチェック, audit テーブルチェック (in dry_run),
        #       count buildings, count nodes, db_size,
        #       audit テーブルチェック (in get_purge_history)
        cursor.fetchone.side_effect = [
            ('city_code',),         # check_city_code_column_exists
            ('plateau_purge_history',),  # check_audit_table_exists (in dry_run)
            (0,),                   # buildings count
            (0,),                   # nodes count
            ('24 GB', '12 GB', '13 GB'),  # size info
            ('plateau_purge_history',),  # check_audit_table_exists (in get_purge_history)
        ]
        cursor.fetchall.return_value = []  # history empty

        monkeypatch.setattr(
            'plateau_purge.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )

        p = Purger('99999', 'postgresql://x')
        results = p.dry_run()

        assert results['error'] == 'no_data'
        assert results['buildings'] == 0
        assert results['nodes'] == 0

    def test_dry_run_when_city_code_column_missing(self, monkeypatch, mock_connection):
        """city_code カラム未追加なら早期エラー"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = None  # check_city_code_column_exists -> False

        monkeypatch.setattr(
            'plateau_purge.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )

        p = Purger('13112', 'postgresql://x')
        results = p.dry_run()

        assert results['error'] == 'city_code_column_missing'
        assert results['city_code_column_exists'] is False


# ----------------------------------------------------------------------
# 確認プロンプト
# ----------------------------------------------------------------------

class TestConfirmExecute:
    def test_correct_inputs_pass(self, monkeypatch):
        """citycode 一致 + 'DELETE' で承認"""
        inputs = iter(['13112', 'DELETE'])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 262658, 2121054) is True

    def test_wrong_citycode_rejects(self, monkeypatch):
        """citycode 不一致なら拒否"""
        inputs = iter(['13140'])  # typo
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 100, 100) is False

    def test_wrong_final_word_rejects(self, monkeypatch):
        """citycode 一致しても 'DELETE' 以外なら拒否"""
        inputs = iter(['13112', 'delete'])  # lowercase
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 100, 100) is False

    def test_yes_does_not_bypass_confirmation(self, monkeypatch):
        """'yes' という入力では通らない（DELETE 完全一致が必要）"""
        inputs = iter(['13112', 'yes'])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 100, 100) is False


# ----------------------------------------------------------------------
# 削除ロジック (delete_data)
# ----------------------------------------------------------------------

class TestDeleteData:
    def test_delete_returns_zero_when_no_targets(self, mock_connection):
        """対象 building_ids が空ならゼロを返す"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = []  # 対象なし

        p = Purger('99999', 'postgresql://x')
        p.conn = mock_connection
        buildings, nodes = p.delete_data()
        assert buildings == 0
        assert nodes == 0

    def test_delete_runs_nodes_then_buildings(self, mock_connection):
        """FK 制約のためノードを先に削除する順序"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = [(1,), (2,), (3,)]
        cursor.rowcount = 100  # どちらの削除でも 100 件

        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        buildings, nodes = p.delete_data()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        delete_sqls = [s for s in executed_sqls if 'DELETE FROM' in s]
        # 1番目が nodes、2番目が buildings
        assert 'plateau_building_nodes' in delete_sqls[0]
        assert 'plateau_buildings' in delete_sqls[1]
        # ノードと建物の両方が削除されたという結果
        assert buildings == 100
        assert nodes == 100
