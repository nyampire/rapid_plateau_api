#!/usr/bin/env python3
"""
Plateau データパージツール

指定した市区町村のPlateau建物データをDBから安全に削除する。
Issue #5 (Phase 3) 対応。

機能:
    Dry Run (--dry-run, デフォルト): 削除対象を確認（DB変更なし）
    本番実行 (--execute): 実際にパージを実施

使い方:
    # Dry Run（デフォルト、削除対象の確認のみ）
    python plateau_purge.py --citycode 13112

    # 本番実行（確認プロンプトあり）
    python plateau_purge.py --citycode 13112 --execute

    # 確認プロンプトをスキップ（自動化向け）
    python plateau_purge.py --citycode 13112 --execute --yes

    # 接続URL指定
    python plateau_purge.py --citycode 13112 --postgres-url "postgresql://..."

    # 監査ログテーブルの初期化（初回のみ）
    python plateau_purge.py --init-audit-table

監査ログ:
    すべてのパージは plateau_purge_history テーブルに記録される。
    確認方法:
        SELECT * FROM plateau_purge_history ORDER BY executed_at DESC;
"""

import argparse
import getpass
import json
import logging
import os
import socket
import sys
import time
from typing import Any, Dict, Optional, Tuple

import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# pg_advisory_lock ID（plateau_migrate.py と別の値）
ADVISORY_LOCK_ID = 0x504C5450  # "PLTP" (PLaTeau Purge) の hex表現

# 監査テーブル作成SQL
AUDIT_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS plateau_purge_history (
    id SERIAL PRIMARY KEY,
    city_code VARCHAR(5) NOT NULL,
    buildings_deleted INTEGER NOT NULL,
    nodes_deleted INTEGER NOT NULL,
    executed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    executed_by TEXT,
    hostname TEXT,
    duration_seconds REAL
);
CREATE INDEX IF NOT EXISTS idx_purge_history_city_code
    ON plateau_purge_history(city_code);
CREATE INDEX IF NOT EXISTS idx_purge_history_executed_at
    ON plateau_purge_history(executed_at DESC);
"""


class Purger:
    """Plateau データパージツール"""

    def __init__(self, citycode: str, postgres_url: Optional[str] = None):
        self.citycode = citycode
        if postgres_url is None:
            postgres_url = os.getenv(
                'DATABASE_URL',
                'postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau'
            )
        self.postgres_url = postgres_url
        self.conn: Optional[psycopg2.extensions.connection] = None

    def connect(self, readonly: bool = True) -> None:
        """DB接続"""
        try:
            self.conn = psycopg2.connect(self.postgres_url)
            if readonly:
                self.conn.set_session(readonly=True)
                logger.info("✅ DB接続成功（読み取り専用モード）")
            else:
                self.conn.autocommit = False
                logger.info("✅ DB接続成功（書き込み可能モード）")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ DB接続失敗: {e}")
            sys.exit(1)

    def close(self) -> None:
        if self.conn:
            self.conn.close()

    # ------------------------------------------------------------------
    # 状態確認
    # ------------------------------------------------------------------

    def check_city_code_column_exists(self) -> bool:
        """city_code カラムが存在するか確認（Phase 1 マイグレーション済みか）"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'plateau_buildings'
                  AND column_name = 'city_code'
            """)
            return cur.fetchone() is not None

    def check_audit_table_exists(self) -> bool:
        """plateau_purge_history テーブルが存在するか確認"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = 'plateau_purge_history'
            """)
            return cur.fetchone() is not None

    def count_target_rows(self) -> Tuple[int, int]:
        """削除対象の建物・ノード件数を取得"""
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM plateau_buildings WHERE city_code = %s",
                (self.citycode,)
            )
            buildings = cur.fetchone()[0]

            cur.execute("""
                SELECT count(*) FROM plateau_building_nodes
                WHERE building_id IN (
                    SELECT id FROM plateau_buildings WHERE city_code = %s
                )
            """, (self.citycode,))
            nodes = cur.fetchone()[0]
        return buildings, nodes

    def get_size_info(self) -> Dict[str, Any]:
        """DB容量情報を取得"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    pg_size_pretty(pg_database_size(current_database())) AS db_size,
                    pg_size_pretty(pg_total_relation_size('plateau_buildings')) AS buildings_size,
                    pg_size_pretty(pg_total_relation_size('plateau_building_nodes')) AS nodes_size
            """)
            row = cur.fetchone()
        return {
            'db_size': row[0],
            'buildings_size': row[1],
            'nodes_size': row[2],
        }

    def get_purge_history(self, citycode: str) -> list:
        """過去の同一都市パージ履歴を取得"""
        if not self.check_audit_table_exists():
            return []
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT executed_at, buildings_deleted, nodes_deleted, executed_by
                FROM plateau_purge_history
                WHERE city_code = %s
                ORDER BY executed_at DESC
                LIMIT 5
            """, (citycode,))
            return [
                {
                    'executed_at': row[0].isoformat(),
                    'buildings_deleted': row[1],
                    'nodes_deleted': row[2],
                    'executed_by': row[3],
                }
                for row in cur.fetchall()
            ]

    # ------------------------------------------------------------------
    # Dry Run
    # ------------------------------------------------------------------

    def dry_run(self) -> Dict[str, Any]:
        """Dry Run 実行（読み取り専用）"""
        self.connect(readonly=True)
        try:
            results = {
                'citycode': self.citycode,
                'city_code_column_exists': self.check_city_code_column_exists(),
                'audit_table_exists': self.check_audit_table_exists(),
            }

            if not results['city_code_column_exists']:
                logger.error("❌ city_code カラムが存在しません。先に Phase 1 マイグレーション（plateau_migrate.py）を実行してください")
                results['error'] = 'city_code_column_missing'
                return results

            buildings, nodes = self.count_target_rows()
            results['buildings'] = buildings
            results['nodes'] = nodes

            if buildings == 0:
                logger.warning(f"⚠️ city_code = {self.citycode} のデータは見つかりませんでした")
                results['error'] = 'no_data'

            results['size_info'] = self.get_size_info()
            results['history'] = self.get_purge_history(self.citycode)
            return results
        finally:
            self.close()

    def print_dry_run_summary(self, results: Dict[str, Any]) -> None:
        """Dry Run の人間向けサマリ出力"""
        print()
        print("=" * 60)
        print(f"🔍 Dry Run サマリ: city_code = {results['citycode']} のパージ")
        print("=" * 60)

        if results.get('error') == 'city_code_column_missing':
            print("\n❌ city_code カラムが存在しません")
            print("   先に Phase 1 マイグレーションを実行してください:")
            print("     python plateau_migrate.py --execute")
            return

        if not results.get('audit_table_exists'):
            print("\n⚠️ 監査テーブル plateau_purge_history が存在しません")
            print("   初回実行前に以下を実行してください:")
            print("     python plateau_purge.py --init-audit-table")

        print(f"\n📊 削除対象:")
        print(f"   plateau_buildings:      {results['buildings']:>12,} 行")
        print(f"   plateau_building_nodes: {results['nodes']:>12,} 行 (FK連動)")

        if results.get('error') == 'no_data':
            print(f"\n⚠️ この city_code のデータは存在しません。citycodeを確認してください。")
            return

        size = results['size_info']
        print(f"\n💾 現在のDB容量:")
        print(f"   DB全体:                  {size['db_size']}")
        print(f"   plateau_buildings:       {size['buildings_size']}")
        print(f"   plateau_building_nodes:  {size['nodes_size']}")
        print(f"   ※ VACUUM 後の容量解放は別途確認が必要")

        history = results.get('history', [])
        if history:
            print(f"\n📋 過去のパージ履歴 (city_code={results['citycode']}):")
            for h in history:
                print(f"   {h['executed_at']}  建物 {h['buildings_deleted']:,} / ノード {h['nodes_deleted']:,} (by {h['executed_by']})")

        print()
        print("=" * 60)
        print("⚠️  この操作は取り消せません。")
        print(f"   本番実行: python plateau_purge.py --citycode {results['citycode']} --execute")
        print("=" * 60)

    # ------------------------------------------------------------------
    # 本番実行
    # ------------------------------------------------------------------

    def init_audit_table(self) -> None:
        """監査テーブルを作成"""
        self.connect(readonly=False)
        try:
            logger.info("📋 plateau_purge_history テーブルを初期化中...")
            with self.conn.cursor() as cur:
                cur.execute(AUDIT_TABLE_DDL)
            self.conn.commit()
            logger.info("✅ 監査テーブル初期化完了")
        finally:
            self.close()

    def acquire_lock(self) -> bool:
        """pg_advisory_lock で排他ロック取得"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            acquired = cur.fetchone()[0]
        if not acquired:
            logger.error("❌ 別のパージプロセスが実行中の可能性があります")
        return acquired

    def release_lock(self) -> None:
        """ロック解放"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))

    def delete_data(self) -> Tuple[int, int]:
        """実際のパージ実行。削除件数を返す。"""
        with self.conn.cursor() as cur:
            # 削除対象 building.id をリスト化（後でノード削除に使用）
            logger.info("🔍 削除対象の building.id を取得中...")
            cur.execute(
                "SELECT id FROM plateau_buildings WHERE city_code = %s",
                (self.citycode,)
            )
            building_ids = [row[0] for row in cur.fetchall()]
            logger.info(f"   対象 building.id: {len(building_ids):,} 件")

            if not building_ids:
                logger.warning("削除対象なし")
                return 0, 0

            # ノードを先に削除（FK制約）
            logger.info("🗑️  plateau_building_nodes を削除中...")
            t0 = time.time()
            cur.execute("""
                DELETE FROM plateau_building_nodes
                WHERE building_id = ANY(%s)
            """, (building_ids,))
            nodes_deleted = cur.rowcount
            logger.info(f"   ✅ {nodes_deleted:,} 件削除 ({time.time() - t0:.1f}s)")

            # 建物を削除
            logger.info("🗑️  plateau_buildings を削除中...")
            t0 = time.time()
            cur.execute(
                "DELETE FROM plateau_buildings WHERE city_code = %s",
                (self.citycode,)
            )
            buildings_deleted = cur.rowcount
            logger.info(f"   ✅ {buildings_deleted:,} 件削除 ({time.time() - t0:.1f}s)")

            self.conn.commit()
            return buildings_deleted, nodes_deleted

    def record_audit(
        self,
        buildings_deleted: int,
        nodes_deleted: int,
        duration_seconds: float,
    ) -> None:
        """監査ログに記録"""
        if not self.check_audit_table_exists():
            logger.warning("⚠️ 監査テーブルが存在しないため履歴記録をスキップ")
            return

        try:
            executed_by = os.environ.get('USER') or getpass.getuser()
        except Exception:
            executed_by = 'unknown'
        try:
            hostname = socket.gethostname()
        except Exception:
            hostname = 'unknown'

        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO plateau_purge_history
                (city_code, buildings_deleted, nodes_deleted,
                 executed_by, hostname, duration_seconds)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                self.citycode, buildings_deleted, nodes_deleted,
                executed_by, hostname, duration_seconds
            ))
        self.conn.commit()
        logger.info(f"📋 監査ログ記録完了 (by {executed_by}@{hostname})")

    def post_process(self) -> None:
        """事後処理: VACUUM + ANALYZE"""
        # VACUUM はトランザクション外で実行する必要がある
        # 新規接続で実行（plateau_migrate.py と同様のパターン）
        logger.info("🧹 VACUUM + ANALYZE 実行中...")
        new_conn = psycopg2.connect(self.postgres_url)
        new_conn.autocommit = True
        try:
            with new_conn.cursor() as cur:
                t0 = time.time()
                cur.execute("VACUUM ANALYZE plateau_buildings")
                logger.info(f"   ✅ plateau_buildings ({time.time() - t0:.1f}s)")
                t0 = time.time()
                cur.execute("VACUUM ANALYZE plateau_building_nodes")
                logger.info(f"   ✅ plateau_building_nodes ({time.time() - t0:.1f}s)")
        finally:
            new_conn.close()
        logger.info("✅ 事後処理完了")

    def execute(self) -> None:
        """本番パージ実行"""
        self.connect(readonly=False)
        lock_acquired = False
        start_time = time.time()
        try:
            # 排他ロック取得
            lock_acquired = self.acquire_lock()
            if not lock_acquired:
                sys.exit(1)
            logger.info(f"🔒 排他ロック取得（ID: 0x{ADVISORY_LOCK_ID:08X}）")

            logger.info("=" * 60)
            logger.info(f"🚀 パージ開始: city_code = {self.citycode}")
            logger.info("=" * 60)

            # 実行
            buildings_deleted, nodes_deleted = self.delete_data()

            duration = time.time() - start_time

            # 監査ログ
            self.record_audit(buildings_deleted, nodes_deleted, duration)

            logger.info("=" * 60)
            logger.info(f"🎉 パージ完了")
            logger.info(f"   削除: 建物 {buildings_deleted:,} 件 / ノード {nodes_deleted:,} 件")
            logger.info(f"   所要時間: {duration:.1f}秒")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"❌ パージ失敗: {e}", exc_info=True)
            self.conn.rollback()
            sys.exit(1)
        finally:
            if lock_acquired:
                self.release_lock()
                logger.info("🔓 排他ロック解放")
            self.close()

        # 事後処理（接続を新規に張り直して実行）
        self.post_process()


def confirm_execute(citycode: str, buildings: int, nodes: int) -> bool:
    """ユーザに本番実行の確認を求める（タイポ防止）"""
    print()
    print("=" * 60)
    print("⚠️  本番パージを実行します")
    print("=" * 60)
    print(f"対象 city_code: {citycode}")
    print(f"削除される建物:  {buildings:,} 件")
    print(f"削除されるノード: {nodes:,} 件 (FK連動)")
    print()
    print("⚠️  この操作は取り消せません（再インポートには時間がかかります）")
    print()

    # 1段階目: city_code を再入力（タイポ防止）
    response = input(f"確認のため、city_code をもう一度入力してください ({citycode}): ").strip()
    if response != citycode:
        print(f"❌ city_code が一致しません ({response!r} != {citycode!r})")
        return False

    # 2段階目: DELETE と入力（最終確認）
    response = input("最終確認: 'DELETE' と入力してください: ").strip()
    if response != 'DELETE':
        print("❌ 'DELETE' と入力されませんでした")
        return False

    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Plateau データパージツール',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--citycode',
        help='対象都市コード（例: 13112）',
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='本番実行モード（指定しない場合は Dry Run）',
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='確認プロンプトをスキップ（--execute との併用、自動化向け）',
    )
    parser.add_argument(
        '--init-audit-table',
        action='store_true',
        help='監査テーブル plateau_purge_history を初期化',
    )
    parser.add_argument(
        '--postgres-url',
        help='PostgreSQL接続URL (デフォルト: 環境変数 DATABASE_URL)',
    )
    parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='出力形式 (default: text, --execute では無視)',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細ログ出力',
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # 監査テーブル初期化のみのモード
    if args.init_audit_table:
        runner = Purger('', args.postgres_url)  # citycode 不要
        runner.init_audit_table()
        return

    # citycode は通常実行時には必須
    if not args.citycode:
        parser.error("--citycode が必要です (または --init-audit-table)")

    # citycode 形式チェック
    if not (args.citycode.isdigit() and len(args.citycode) == 5):
        parser.error(f"citycode は5桁の数字である必要があります (指定: {args.citycode!r})")

    runner = Purger(args.citycode, args.postgres_url)

    if args.execute:
        # 本番実行前に Dry Run で確認
        logger.info("📋 本番実行前に Dry Run で状態確認...")
        results = runner.dry_run()
        runner.print_dry_run_summary(results)

        if results.get('error') == 'city_code_column_missing':
            sys.exit(1)
        if results.get('error') == 'no_data':
            logger.error("削除対象データがないため終了します")
            sys.exit(1)

        # 確認プロンプト
        if not args.yes:
            ok = confirm_execute(args.citycode, results['buildings'], results['nodes'])
            if not ok:
                logger.info("実行をキャンセルしました")
                sys.exit(0)
        else:
            logger.info("--yes 指定のため確認プロンプトをスキップ")

        # 本番実行
        runner = Purger(args.citycode, args.postgres_url)  # 新規接続
        runner.execute()
    else:
        # Dry Run のみ
        results = runner.dry_run()
        if args.format == 'json':
            print(json.dumps(results, indent=2, ensure_ascii=False, default=str))
        else:
            runner.print_dry_run_summary(results)


if __name__ == '__main__':
    main()
