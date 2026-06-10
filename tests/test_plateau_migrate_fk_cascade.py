"""Unit tests for plateau_migrate_fk_cascade (api#20)."""
import logging
from unittest.mock import MagicMock, patch

import pytest

import plateau_migrate_fk_cascade as mig


def _mock_conn_with_state(state_before: str, state_after: str | None = None):
    """Build a mocked psycopg2 connection whose cursor's fetchone() returns the
    requested confdeltype value. If ``state_after`` is given, the second fetch
    after the DROP+ADD returns that value (used to simulate verification).
    """
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    fetch_returns = [(state_before,)] if state_after is None else [(state_before,), (state_after,)]
    cursor.fetchone.side_effect = fetch_returns
    return conn, cursor


class TestCheckConstraintState:
    def test_returns_confdeltype_char(self):
        cur = MagicMock()
        cur.fetchone.return_value = ("a",)
        assert mig.check_constraint_state(cur) == "a"

    def test_returns_none_when_constraint_missing(self):
        cur = MagicMock()
        cur.fetchone.return_value = None
        assert mig.check_constraint_state(cur) is None


class TestMigrateNoop:
    """Already-CASCADE constraints should exit cleanly without touching DDL."""

    def test_returns_zero_and_skips_ddl(self):
        conn, cursor = _mock_conn_with_state("c")
        with patch.object(mig.psycopg2, "connect", return_value=conn):
            rc = mig.migrate("postgresql://fake", execute=True)
        assert rc == 0
        # Only the initial CHECK_SQL was executed; no DROP or ADD.
        executed_sqls = [call.args[0] for call in cursor.execute.call_args_list]
        assert all("ALTER TABLE" not in s for s in executed_sqls), \
            f"Unexpected DDL when already CASCADE: {executed_sqls!r}"
        conn.commit.assert_not_called()


class TestMigrateMissingConstraint:
    def test_returns_error_exit_code(self):
        conn = MagicMock()
        cursor = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = None  # constraint not found
        with patch.object(mig.psycopg2, "connect", return_value=conn):
            rc = mig.migrate("postgresql://fake", execute=True)
        assert rc == 2
        conn.commit.assert_not_called()


class TestMigrateDryRun:
    def test_no_ddl_emitted_without_execute(self):
        conn, cursor = _mock_conn_with_state("a")
        with patch.object(mig.psycopg2, "connect", return_value=conn):
            rc = mig.migrate("postgresql://fake", execute=False)
        assert rc == 0
        executed_sqls = [call.args[0] for call in cursor.execute.call_args_list]
        # SELECT (check) only — no ALTER TABLE.
        assert all("ALTER TABLE" not in s for s in executed_sqls)
        conn.commit.assert_not_called()


class TestMigrateApply:
    def test_drops_and_adds_with_cascade(self, caplog):
        conn, cursor = _mock_conn_with_state("a", "c")  # before: NO ACTION, after: CASCADE
        with patch.object(mig.psycopg2, "connect", return_value=conn), \
             caplog.at_level(logging.INFO):
            rc = mig.migrate("postgresql://fake", execute=True)
        assert rc == 0
        executed_sqls = [call.args[0] for call in cursor.execute.call_args_list]
        assert any("DROP CONSTRAINT" in s for s in executed_sqls)
        assert any("ADD CONSTRAINT" in s and "ON DELETE CASCADE" in s for s in executed_sqls)
        conn.commit.assert_called_once()

    def test_rolls_back_if_verification_fails(self):
        # Re-add somehow lands with the wrong confdeltype — should rollback and error.
        conn, cursor = _mock_conn_with_state("a", "a")  # never flipped to 'c'
        with patch.object(mig.psycopg2, "connect", return_value=conn):
            rc = mig.migrate("postgresql://fake", execute=True)
        assert rc == 3
        conn.rollback.assert_called()
        conn.commit.assert_not_called()


class TestSqlContents:
    """Defensive — make sure the constants point at the right object."""

    def test_drop_targets_the_node_fk(self):
        assert "plateau_building_nodes" in mig.DROP_SQL
        assert "plateau_building_nodes_building_id_fkey" in mig.DROP_SQL

    def test_add_uses_cascade_on_nodes(self):
        assert "plateau_building_nodes" in mig.ADD_SQL
        assert "REFERENCES plateau_buildings(id)" in mig.ADD_SQL
        assert "ON DELETE CASCADE" in mig.ADD_SQL
