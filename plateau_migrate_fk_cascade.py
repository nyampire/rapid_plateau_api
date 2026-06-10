#!/usr/bin/env python3
"""Switch plateau_building_nodes.building_id FK to ON DELETE CASCADE.

One-shot DDL migration to fix nyampire/rapid_plateau_api#20.

The boundary filter in plateau_importer2postgis.py used a two-step DELETE
(nodes first, then buildings). That pattern is safe when every parent and
its parts get selected together (e.g. plateau_purge.py wipes a whole city).
The boundary filter only selects buildings whose centroid falls outside the
N03 boundary, so a parent can be in the set while its parts are not (or vice
versa). DELETE on the parent fires the existing ``parent_building_id ON DELETE
CASCADE`` and removes those parts — but their node rows weren't in the manual
node-DELETE list, so plateau_building_nodes_building_id_fkey trips.

Adding ON DELETE CASCADE to the nodes FK makes the cleanup atomic: removing
any building drops its nodes implicitly, whether the building was deleted
explicitly or via a cascade from its parent.

Idempotent: if the constraint already cascades, exit without changes.

Usage:
  python3 plateau_migrate_fk_cascade.py --postgres-url "$DATABASE_URL"            # dry-run
  python3 plateau_migrate_fk_cascade.py --postgres-url "$DATABASE_URL" --execute
"""
import argparse
import logging
import sys
from typing import Optional

import psycopg2


CONSTRAINT_NAME = "plateau_building_nodes_building_id_fkey"
TABLE_NAME = "plateau_building_nodes"

# pg_constraint.confdeltype values for FOREIGN KEY ON DELETE behaviour:
#   'a' = NO ACTION (default), 'r' = RESTRICT,
#   'c' = CASCADE,             'n' = SET NULL,  'd' = SET DEFAULT
CHECK_SQL = """
SELECT confdeltype
FROM pg_constraint
WHERE conname = %s
  AND conrelid = %s::regclass
"""

DROP_SQL = f"ALTER TABLE {TABLE_NAME} DROP CONSTRAINT {CONSTRAINT_NAME}"
ADD_SQL = (
    f"ALTER TABLE {TABLE_NAME} "
    f"ADD CONSTRAINT {CONSTRAINT_NAME} "
    f"FOREIGN KEY (building_id) REFERENCES plateau_buildings(id) ON DELETE CASCADE"
)


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("plateau_migrate_fk_cascade")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(h)
    return logger


def check_constraint_state(cursor) -> Optional[str]:
    """Return the confdeltype char for the nodes FK, or None if it doesn't exist."""
    cursor.execute(CHECK_SQL, (CONSTRAINT_NAME, TABLE_NAME))
    row = cursor.fetchone()
    return row[0] if row else None


def migrate(postgres_url: str, execute: bool, logger: Optional[logging.Logger] = None) -> int:
    """Apply (or dry-run) the FK migration. Returns exit code (0 on success)."""
    logger = logger or _setup_logger()
    conn = psycopg2.connect(postgres_url)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            before = check_constraint_state(cur)
            if before is None:
                logger.error(
                    "❌ Constraint %s not found on %s — schema looks unfamiliar; aborting.",
                    CONSTRAINT_NAME, TABLE_NAME,
                )
                return 2
            if before == "c":
                logger.info(
                    "✅ %s already ON DELETE CASCADE; no migration needed.",
                    CONSTRAINT_NAME,
                )
                return 0
            logger.info(
                "🔧 %s currently has confdeltype=%r; will switch to CASCADE.",
                CONSTRAINT_NAME, before,
            )
            if not execute:
                logger.info("Dry-run: would DROP + ADD with ON DELETE CASCADE. Pass --execute to apply.")
                return 0
            cur.execute(DROP_SQL)
            cur.execute(ADD_SQL)
            after = check_constraint_state(cur)
            if after != "c":
                conn.rollback()
                logger.error(
                    "❌ Migration verification failed: confdeltype is %r after re-add; rolled back.",
                    after,
                )
                return 3
            conn.commit()
            logger.info("✅ %s is now ON DELETE CASCADE.", CONSTRAINT_NAME)
            return 0
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Switch plateau_building_nodes.building_id FK to ON DELETE CASCADE (api#20).",
    )
    ap.add_argument("--postgres-url", required=True, help="PostgreSQL connection URL")
    ap.add_argument("--execute", action="store_true",
                    help="apply the migration (default: dry-run only)")
    args = ap.parse_args()
    sys.exit(migrate(args.postgres_url, args.execute))


if __name__ == "__main__":
    main()
