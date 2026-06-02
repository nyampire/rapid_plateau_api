"""
plateau_purge.py のテスト
"""

import sys
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

    def test_skip_coverage_refresh_short_circuits(self, monkeypatch):
        """`--skip-coverage-refresh` 相当の flag が立っていれば、
        CoverageManager の import 経路に降りずに早期 return する。

        Rapid#35 part C の re-import バッチで OOM の主因になっていた
        `REFRESH MATERIALIZED VIEW CONCURRENTLY plateau_coverage` を
        per-city ではなくバッチ末尾で 1 回だけ呼ぶ運用のための fallback。
        """
        # plateau_coverage モジュールを取り除いた状態で flag を効かせる。
        # flag が無視されて import まで進めば ModuleNotFoundError が漏れて来るので
        # 早期 return できているかが分かる。
        monkeypatch.setitem(sys.modules, 'plateau_coverage', None)
        p = Purger('13112', 'postgresql://x', skip_coverage_refresh=True)
        p._refresh_coverage_view()  # 例外を出さなければ OK

    def test_default_does_not_skip_coverage_refresh(self):
        """flag 未指定時は従来挙動（refresh が走る経路）"""
        p = Purger('13112', 'postgresql://x')
        assert p.skip_coverage_refresh is False


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
    """
    confirm_execute は誤操作防止のための2段階確認。
    タイポ・大文字小文字違い・空白などをすべて弾く必要がある。
    """

    def test_correct_inputs_pass(self, monkeypatch):
        """citycode 一致 + 'DELETE' で承認"""
        inputs = iter(['13112', 'DELETE'])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 262658, 2121054) is True

    def test_trailing_whitespace_in_citycode_accepted(self, monkeypatch):
        """前後の空白は strip される（タイポ防止）"""
        inputs = iter(['  13112  ', 'DELETE'])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 100, 100) is True

    @pytest.mark.parametrize('typed_citycode', [
        '13140',       # 桁ずれ
        '13111',       # 1桁違い
        '1311',        # 短い
        '131120',      # 長い
        '',            # 空
        '13112x',      # 余分な文字
        'x13112',      # 先頭にゴミ
        '13 112',      # スペース混入
    ])
    def test_wrong_citycode_rejects(self, monkeypatch, typed_citycode):
        """citycode が完全一致でなければ拒否"""
        inputs = iter([typed_citycode])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        assert confirm_execute('13112', 100, 100) is False

    @pytest.mark.parametrize('final_word', [
        'delete',       # 小文字
        'Delete',       # mixed case
        'DELETE ',      # 末尾スペース（strip されればOKだが厳密）
        ' DELETE',      # 先頭スペース
        'DELETE!',      # 余分な文字
        'yes',
        'y',
        '',
        'DROP',
        'REMOVE',
    ])
    def test_wrong_final_word_rejects(self, monkeypatch, final_word):
        """citycode 一致しても DELETE 完全一致でなければ拒否"""
        inputs = iter(['13112', final_word])
        monkeypatch.setattr('builtins.input', lambda prompt='': next(inputs))
        result = confirm_execute('13112', 100, 100)
        # strip 込みで 'DELETE' になるものは True、それ以外は False
        if final_word.strip() == 'DELETE':
            assert result is True
        else:
            assert result is False


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
        cursor.rowcount = 100

        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        p.delete_data()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        delete_sqls = [s for s in executed_sqls if 'DELETE FROM' in s]
        # 1番目が nodes、2番目が buildings
        assert 'plateau_building_nodes' in delete_sqls[0]
        assert 'plateau_buildings' in delete_sqls[1]

    def test_delete_returns_correct_counts_for_each_table(self):
        """
        buildings_deleted と nodes_deleted を取り違えていないか検証。
        SQL内容を見て rowcount を切り替える smart mock cursor を作る。
        """
        from unittest.mock import MagicMock

        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=None)
        cursor.fetchall.return_value = [(1,), (2,), (3,)]

        # 直前に実行された SQL に応じて rowcount を変える
        rowcount_state = {'value': 0}

        def execute_side_effect(sql, params=None):
            sql_upper = (sql or '').upper()
            if 'DELETE FROM PLATEAU_BUILDING_NODES' in sql_upper:
                rowcount_state['value'] = 2_265_185
            elif 'DELETE FROM PLATEAU_BUILDINGS' in sql_upper:
                rowcount_state['value'] = 339_015
            else:
                rowcount_state['value'] = 0

        cursor.execute.side_effect = execute_side_effect

        # rowcount プロパティを動的に読む
        type(cursor).rowcount = property(lambda self: rowcount_state['value'])

        conn = MagicMock()
        conn.cursor.return_value = cursor

        p = Purger('40130', 'postgresql://x')
        p.conn = conn
        buildings_deleted, nodes_deleted = p.delete_data()

        # 戻り値が (buildings, nodes) の順で正しい
        assert buildings_deleted == 339_015
        assert nodes_deleted == 2_265_185

    def test_delete_uses_indexed_city_code_query(self, mock_connection):
        """DELETE FROM plateau_buildings には city_code = %s を使う（LIKE不可）"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = [(1,)]
        cursor.rowcount = 1

        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        p.delete_data()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        delete_buildings = next(s for s in executed_sqls
                                if 'DELETE FROM plateau_buildings' in s)
        assert 'city_code = %s' in delete_buildings
        assert 'LIKE' not in delete_buildings


# ----------------------------------------------------------------------
# execute() 統合フロー
# ----------------------------------------------------------------------

class TestExecuteIntegration:
    """
    Purger.execute() 全体: ロック取得 → 削除 → 監査ログ → ロック解放 → 事後処理
    """

    def test_execute_acquires_and_releases_lock(self, monkeypatch, mock_connection):
        """正常系: ロック取得→処理→ロック解放の順"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchall.return_value = [(1,)]
        cursor.rowcount = 1

        # acquire_lock, audit table 存在チェック等を順に
        cursor.fetchone.side_effect = [
            (True,),     # acquire_lock
            ('plateau_purge_history',),  # check_audit_table_exists for record_audit
        ]

        monkeypatch.setattr(
            'plateau_purge.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        # post_process は重いのでスキップ
        monkeypatch.setattr(
            'plateau_purge.Purger.post_process',
            lambda self: None,
        )

        p = Purger('13112', 'postgresql://x')
        p.execute()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        # pg_try_advisory_lock と pg_advisory_unlock の両方が呼ばれている
        assert any('pg_try_advisory_lock' in s for s in executed_sqls)
        assert any('pg_advisory_unlock' in s for s in executed_sqls)

    def test_execute_releases_lock_on_exception(self, monkeypatch, mock_connection):
        """delete_data が例外を投げてもロックは解放される"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.side_effect = [
            (True,),  # acquire_lock
        ]

        monkeypatch.setattr(
            'plateau_purge.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )
        # delete_data に例外を仕込む
        def raise_exc(self):
            raise RuntimeError('boom')
        monkeypatch.setattr('plateau_purge.Purger.delete_data', raise_exc)

        p = Purger('13112', 'postgresql://x')
        with pytest.raises(SystemExit):  # 例外で sys.exit(1)
            p.execute()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        # ロック解放が呼ばれている
        assert any('pg_advisory_unlock' in s for s in executed_sqls)
        # rollback も呼ばれている
        assert mock_connection.rollback.called

    def test_execute_exits_when_lock_not_acquired(self, monkeypatch, mock_connection):
        """別プロセスがロック保持中なら sys.exit(1) して何もしない"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = (False,)  # acquire_lock returns False

        monkeypatch.setattr(
            'plateau_purge.psycopg2.connect',
            lambda *args, **kwargs: mock_connection,
        )

        p = Purger('13112', 'postgresql://x')
        with pytest.raises(SystemExit):
            p.execute()

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        # DELETE は呼ばれていない
        assert not any('DELETE FROM' in s for s in executed_sqls)


# ----------------------------------------------------------------------
# 監査ログ
# ----------------------------------------------------------------------

class TestRecordAudit:
    def test_record_audit_inserts_with_correct_values(self, monkeypatch, mock_connection):
        """監査ログ INSERT に正しい値が渡される"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = ('plateau_purge_history',)  # audit table exists

        monkeypatch.setenv('USER', 'testuser')
        monkeypatch.setattr('socket.gethostname', lambda: 'testhost')

        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        p.record_audit(
            buildings_deleted=339_015,
            nodes_deleted=2_265_185,
            duration_seconds=54.1,
        )

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        insert_sql = next(s for s in executed_sqls
                          if 'INSERT INTO plateau_purge_history' in s)
        # 全ての必須カラムが含まれる
        assert 'city_code' in insert_sql
        assert 'buildings_deleted' in insert_sql
        assert 'nodes_deleted' in insert_sql
        assert 'executed_by' in insert_sql
        assert 'hostname' in insert_sql
        assert 'duration_seconds' in insert_sql

        # パラメータ確認: 順序とそれぞれの値
        insert_call = next(c for c in cursor.execute.call_args_list
                           if 'INSERT' in c[0][0])
        params = insert_call[0][1]
        assert params[0] == '13112'        # city_code
        assert params[1] == 339_015        # buildings_deleted
        assert params[2] == 2_265_185      # nodes_deleted
        assert params[3] == 'testuser'     # executed_by
        assert params[4] == 'testhost'     # hostname
        assert params[5] == 54.1           # duration_seconds

    def test_record_audit_silently_skips_when_table_missing(self, mock_connection):
        """監査テーブル未作成なら警告のみで例外なし"""
        cursor = mock_connection.cursor.return_value
        cursor.fetchone.return_value = None  # check_audit_table_exists -> False

        p = Purger('13112', 'postgresql://x')
        p.conn = mock_connection
        # 例外を投げない
        p.record_audit(1, 1, 1.0)

        executed_sqls = [call[0][0] for call in cursor.execute.call_args_list]
        # INSERT は呼ばれていない
        assert not any('INSERT INTO plateau_purge_history' in s for s in executed_sqls)
