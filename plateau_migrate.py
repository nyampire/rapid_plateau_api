#!/usr/bin/env python3
"""
Plateau DB マイグレーションツール

plateau_buildings テーブルへの city_code カラム追加とデータ移行を行う。
Issue #3 (Phase 1) 対応。

機能:
    Dry Run (--dry-run, デフォルト): 読み取り専用で影響範囲を確認
    本番実行 (--execute): 実際にスキーマ変更とデータ移行を実施

使い方:
    # Dry Run（デフォルト、読み取り専用）
    python plateau_migrate.py

    # 本番実行
    python plateau_migrate.py --execute

    # 確認プロンプトをスキップ（自動化向け）
    python plateau_migrate.py --execute --yes

    # 接続URL指定
    python plateau_migrate.py --postgres-url "postgresql://user:pass@host:5432/db"

    # JSON出力（Dry Run のみ）
    python plateau_migrate.py --format json

ロールバック:
    -- city_code カラムを削除して完全に元に戻す
    ALTER TABLE plateau_buildings DROP COLUMN city_code;
"""

import argparse
import json
import logging
import os
import re
import shutil
import sys
import time
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

# CITIES_2024 リストを batch_import_2024 から読み込み
try:
    from batch_import_2024 import ALREADY_IMPORTED, CITIES_2024
except ImportError:
    CITIES_2024 = []
    ALREADY_IMPORTED = set()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# city_code 抽出用の正規表現
# 例: plateau_13112_53393438_bldg_6697_op.osm → 13112
# re.ASCII を付けることで `\d` がASCIIの 0-9 のみにマッチ（全角数字を除外）
CITY_CODE_PATTERN = re.compile(r'plateau_(\d{5})_', re.ASCII)

# Step 2 の見積もり用定数
ROW_SIZE_BYTES = 641  # plateau_buildings の平均行サイズ
BATCH_SIZE = 1_000_000  # 推奨バッチサイズ

# pg_advisory_lock ID（任意のユニークな整数）
ADVISORY_LOCK_ID = 0x504C5444  # "PLTD" の hex表現

# 安全マージン
MIN_DISK_FREE_GB = 2.0  # バッチ方式実行時の最低空き容量


class Migrator:
    """Plateau DB マイグレーションツール"""

    def __init__(self, postgres_url: Optional[str] = None):
        if postgres_url is None:
            postgres_url = os.getenv(
                'DATABASE_URL',
                'postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau'
            )
        self.postgres_url = postgres_url
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.results: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 接続管理
    # ------------------------------------------------------------------

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
    # 共通: 状態確認
    # ------------------------------------------------------------------

    def check_disk_space(self) -> Dict[str, Any]:
        """ディスク空き容量チェック"""
        logger.info("📊 ディスク空き容量を確認中...")
        usage = shutil.disk_usage('/')
        free_gb = usage.free / (1024 ** 3)
        total_gb = usage.total / (1024 ** 3)
        used_gb = usage.used / (1024 ** 3)
        info = {
            'total_gb': round(total_gb, 1),
            'used_gb': round(used_gb, 1),
            'free_gb': round(free_gb, 1),
            'used_percent': round(used_gb / total_gb * 100, 1),
        }
        logger.info(
            f"   合計: {info['total_gb']}GB, 使用済み: {info['used_gb']}GB "
            f"({info['used_percent']}%), 空き: {info['free_gb']}GB"
        )
        return info

    def check_column_exists(self) -> bool:
        """city_code カラムの存在確認"""
        logger.info("🔍 city_code カラムの存在を確認中...")
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'plateau_buildings'
                  AND column_name = 'city_code'
            """)
            exists = cur.fetchone() is not None
        logger.info(f"   city_code カラム: {'存在する' if exists else '未追加'}")
        return exists

    def check_index_exists(self) -> bool:
        """idx_buildings_city_code インデックスの存在確認"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'plateau_buildings'
                  AND indexname = 'idx_buildings_city_code'
            """)
            return cur.fetchone() is not None

    def check_not_null_constraint(self) -> bool:
        """city_code に NOT NULL 制約があるか確認"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT is_nullable
                FROM information_schema.columns
                WHERE table_name = 'plateau_buildings'
                  AND column_name = 'city_code'
            """)
            row = cur.fetchone()
            return row is not None and row[0] == 'NO'

    def count_total_rows(self) -> int:
        """plateau_buildings の総行数"""
        logger.info("📊 plateau_buildings の総行数を確認中...")
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM plateau_buildings")
            total = cur.fetchone()[0]
        logger.info(f"   総行数: {total:,} 行")
        return total

    def count_unmigrated(self) -> int:
        """city_code が未設定の行数"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE city_code IS NULL
            """)
            return cur.fetchone()[0]

    def get_id_range(self) -> Tuple[int, int]:
        """plateau_buildings の id 範囲"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT min(id), max(id) FROM plateau_buildings")
            row = cur.fetchone()
            return (row[0] or 0, row[1] or 0)

    # ------------------------------------------------------------------
    # Dry Run 用: 抽出分析
    # ------------------------------------------------------------------

    def analyze_extraction(self) -> Dict[str, Any]:
        """source_dataset から city_code を抽出した結果を分析"""
        logger.info("🔍 source_dataset から city_code 抽出を試行中...")
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE source_dataset ~ 'plateau_\\d{5}_'
            """)
            extractable = cur.fetchone()[0]

            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE source_dataset IS NULL
            """)
            null_count = cur.fetchone()[0]

            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE source_dataset IS NOT NULL
                  AND source_dataset !~ 'plateau_\\d{5}_'
            """)
            failed = cur.fetchone()[0]

            cur.execute("""
                SELECT source_dataset, count(*) AS cnt
                FROM plateau_buildings
                WHERE source_dataset IS NOT NULL
                  AND source_dataset !~ 'plateau_\\d{5}_'
                GROUP BY source_dataset
                ORDER BY cnt DESC
                LIMIT 10
            """)
            failed_samples = [
                {'source_dataset': row[0], 'count': row[1]}
                for row in cur.fetchall()
            ]

        result = {
            'extractable': extractable,
            'null_count': null_count,
            'failed_count': failed,
            'failed_samples': failed_samples,
        }
        logger.info(f"   ✅ 抽出可能: {extractable:,} 行")
        if null_count > 0:
            logger.warning(f"   ⚠️ source_dataset NULL: {null_count:,} 行")
        if failed > 0:
            logger.warning(f"   ❌ 抽出失敗: {failed:,} 行")
            for sample in failed_samples[:5]:
                logger.warning(f"      例: {sample['source_dataset']!r} ({sample['count']}件)")
        return result

    def city_code_distribution(self) -> List[Dict[str, Any]]:
        """抽出される city_code の分布"""
        logger.info("📍 city_code 分布を集計中...")
        # サブクエリ化して、city_code カラムが追加された後でも alias 衝突を避ける
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT extracted_code, count(*) AS row_count
                FROM (
                    SELECT substring(source_dataset from 'plateau_(\\d{5})_') AS extracted_code
                    FROM plateau_buildings
                    WHERE source_dataset ~ 'plateau_\\d{5}_'
                ) sub
                GROUP BY extracted_code
                ORDER BY row_count DESC
            """)
            distribution = [
                {'city_code': row[0], 'row_count': row[1]}
                for row in cur.fetchall()
            ]
        logger.info(f"   検出された都市数: {len(distribution)} 都市")
        return distribution

    def compare_with_cities_2024(self, distribution: List[Dict[str, Any]]) -> Dict[str, Any]:
        """batch_import_2024.py の CITIES_2024 と比較"""
        logger.info("📋 CITIES_2024 リストとの整合性確認中...")
        if not CITIES_2024:
            logger.warning("   ⚠️ CITIES_2024 リストが読み込めませんでした（スキップ）")
            return {'skipped': True}

        expected = set(CITIES_2024) | set(ALREADY_IMPORTED)
        found = {item['city_code'] for item in distribution}

        unexpected = found - expected
        missing = expected - found

        logger.info(f"   期待される都市: {len(expected)} 都市")
        logger.info(f"   DB内に検出: {len(found)} 都市")
        if unexpected:
            logger.warning(f"   ⚠️ リスト外の都市: {len(unexpected)} 都市 → {sorted(unexpected)[:5]}{'...' if len(unexpected) > 5 else ''}")
        if missing:
            logger.info(f"   ℹ️ 未インポートの都市: {len(missing)} 都市（参考情報）")

        return {
            'expected_count': len(expected),
            'found_count': len(found),
            'unexpected': sorted(unexpected),
            'missing': sorted(missing),
        }

    def estimate_migration(
        self,
        total_rows: int,
        extractable: int,
        free_gb: float,
    ) -> Dict[str, Any]:
        """マイグレーションの所要時間・容量を見積もり"""
        logger.info("⏱️ マイグレーション見積もり計算中...")

        peak_bloat_gb = (BATCH_SIZE * ROW_SIZE_BYTES * 2) / (1024 ** 3)
        full_bloat_gb = (extractable * ROW_SIZE_BYTES * 2) / (1024 ** 3)
        batch_count = (extractable + BATCH_SIZE - 1) // BATCH_SIZE
        batch_time_min = batch_count * 5

        safe_batch = free_gb > peak_bloat_gb + 1.0
        safe_full = free_gb > full_bloat_gb + 2.0

        result = {
            'batch_count': batch_count,
            'batch_size': BATCH_SIZE,
            'estimated_time_min_batch': batch_time_min,
            'estimated_time_min_full': max(15, extractable // 500_000),
            'peak_bloat_gb_batch': round(peak_bloat_gb, 2),
            'peak_bloat_gb_full': round(full_bloat_gb, 2),
            'safe_batch': safe_batch,
            'safe_full': safe_full,
        }

        logger.info(f"   バッチ方式: {batch_count}バッチ × 約5分 = 約{batch_time_min}分")
        logger.info(f"   バッチ方式ピーク容量: 約{result['peak_bloat_gb_batch']}GB")
        logger.info(f"   一括方式ピーク容量: 約{result['peak_bloat_gb_full']}GB")
        logger.info(f"   バッチ方式: {'✅ 安全' if safe_batch else '❌ 危険（容量不足）'}")
        logger.info(f"   一括方式: {'✅ 安全' if safe_full else '❌ 危険（容量不足）'}")
        return result

    def dry_run(self) -> Dict[str, Any]:
        """Dry Run 実行（読み取り専用）"""
        self.connect(readonly=True)
        try:
            self.results['disk'] = self.check_disk_space()
            self.results['city_code_exists'] = self.check_column_exists()

            if self.results['city_code_exists']:
                logger.warning("⚠️ city_code カラムは既に存在します。マイグレーション済みの可能性があります。")

            self.results['total_rows'] = self.count_total_rows()
            self.results['extraction'] = self.analyze_extraction()
            self.results['distribution'] = self.city_code_distribution()
            self.results['cities_check'] = self.compare_with_cities_2024(self.results['distribution'])
            self.results['estimate'] = self.estimate_migration(
                self.results['total_rows'],
                self.results['extraction']['extractable'],
                self.results['disk']['free_gb'],
            )
        finally:
            self.close()
        return self.results

    def print_dry_run_summary(self) -> None:
        """Dry Run の人間向けサマリ出力"""
        print()
        print("=" * 60)
        print("🔍 Dry Run サマリ: city_code カラム追加マイグレーション")
        print("=" * 60)

        disk = self.results['disk']
        print(f"\n💾 ディスク状況:")
        print(f"   合計: {disk['total_gb']}GB / 空き: {disk['free_gb']}GB ({100 - disk['used_percent']:.1f}%)")

        total = self.results['total_rows']
        ext = self.results['extraction']
        print(f"\n📊 plateau_buildings:")
        print(f"   総行数: {total:,}")
        print(f"   抽出可能: {ext['extractable']:,} ({ext['extractable'] / total * 100:.2f}%)")
        if ext['null_count']:
            print(f"   ⚠️ source_dataset NULL: {ext['null_count']:,}")
        if ext['failed_count']:
            print(f"   ❌ 抽出失敗: {ext['failed_count']:,}")

        dist = self.results['distribution']
        print(f"\n📍 city_code 分布（上位10都市 / 全{len(dist)}都市）:")
        for item in dist[:10]:
            print(f"   {item['city_code']}  {item['row_count']:>10,} 行")

        cc = self.results['cities_check']
        if not cc.get('skipped'):
            print(f"\n📋 CITIES_2024 リストとの整合性:")
            print(f"   期待: {cc['expected_count']} 都市 / DB内: {cc['found_count']} 都市")
            if cc['unexpected']:
                print(f"   ⚠️ リスト外: {cc['unexpected']}")

        est = self.results['estimate']
        print(f"\n⏱️ マイグレーション見積もり:")
        print(f"   推奨: バッチ方式（{est['batch_count']}バッチ × 約5分 = 約{est['estimated_time_min_batch']}分）")
        print(f"   ピーク容量: 約{est['peak_bloat_gb_batch']}GB")
        print(f"   判定: {'✅ 実施可能' if est['safe_batch'] else '❌ 容量不足'}")

        print()
        print("=" * 60)
        if self.results['city_code_exists']:
            print("ℹ️ 既にマイグレーション済みです（city_code カラム存在）")
        elif ext['failed_count'] > 0 or ext['null_count'] > 0:
            print("⚠️ 抽出失敗データがあります。原因を調査してから実行してください")
        elif est['safe_batch']:
            print("✅ Dry Run 完了。本番マイグレーション実行可能です（--execute オプション）")
        else:
            print("❌ ディスク容量不足です。空き容量を確保してから再度確認してください")
        print("=" * 60)

    # ------------------------------------------------------------------
    # 本番実行: スキーマ変更とデータ移行
    # ------------------------------------------------------------------

    def acquire_lock(self) -> bool:
        """pg_advisory_lock で排他ロック取得"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
            acquired = cur.fetchone()[0]
        if not acquired:
            logger.error("❌ 別のマイグレーションプロセスが実行中の可能性があります")
        return acquired

    def release_lock(self) -> None:
        """ロック解放"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))

    def step1_add_column(self) -> None:
        """Step 1: city_code カラム追加"""
        if self.check_column_exists():
            logger.info("Step 1: city_code カラムは既に存在します（スキップ）")
            return
        logger.info("Step 1: city_code カラムを追加中...")
        with self.conn.cursor() as cur:
            cur.execute("ALTER TABLE plateau_buildings ADD COLUMN city_code VARCHAR(5)")
        self.conn.commit()
        logger.info("✅ Step 1 完了: city_code カラム追加")

    def step2_populate_data(self) -> None:
        """Step 2: 既存データに city_code を設定（バッチ処理）"""
        unmigrated = self.count_unmigrated()
        if unmigrated == 0:
            logger.info("Step 2: 全行 city_code 設定済み（スキップ）")
            return

        min_id, max_id = self.get_id_range()
        logger.info(f"Step 2: 既存データの city_code 設定（残り {unmigrated:,} 行 / id範囲 {min_id}〜{max_id}）")

        # バッチ実行
        batch_count = (max_id - min_id + BATCH_SIZE) // BATCH_SIZE
        completed_batches = 0
        start_time = time.time()

        for batch_start in range(min_id, max_id + 1, BATCH_SIZE):
            batch_end = batch_start + BATCH_SIZE - 1
            completed_batches += 1

            # ディスク空き再チェック
            disk_free_gb = shutil.disk_usage('/').free / (1024 ** 3)
            if disk_free_gb < MIN_DISK_FREE_GB:
                logger.error(
                    f"❌ ディスク空き容量不足: {disk_free_gb:.2f}GB < {MIN_DISK_FREE_GB}GB"
                )
                logger.error("   バッチ処理を中断します。空き容量確保後に再実行してください。")
                self.conn.rollback()
                sys.exit(1)

            # UPDATE 実行
            batch_start_time = time.time()
            with self.conn.cursor() as cur:
                cur.execute("""
                    UPDATE plateau_buildings
                    SET city_code = substring(source_dataset from 'plateau_(\\d{5})_')
                    WHERE city_code IS NULL
                      AND id BETWEEN %s AND %s
                """, (batch_start, batch_end))
                rows_updated = cur.rowcount
            self.conn.commit()

            # 進捗ログ
            elapsed = time.time() - start_time
            batch_elapsed = time.time() - batch_start_time
            progress_pct = completed_batches / batch_count * 100
            avg_per_batch = elapsed / completed_batches
            remaining_batches = batch_count - completed_batches
            eta_sec = avg_per_batch * remaining_batches
            eta_min = int(eta_sec / 60)

            logger.info(
                f"   [{completed_batches:>2}/{batch_count}] {progress_pct:5.1f}% | "
                f"id {batch_start:>10}-{batch_end:>10} | "
                f"更新 {rows_updated:>8,} 行 | "
                f"バッチ {batch_elapsed:.1f}s | "
                f"ETA 約{eta_min}分 | "
                f"空き {disk_free_gb:.1f}GB"
            )

            # 各バッチ後に VACUUM
            if rows_updated > 0:
                logger.info(f"      🧹 VACUUM 実行中...")
                vacuum_start = time.time()
                # VACUUM は autocommit が必要
                old_autocommit = self.conn.autocommit
                self.conn.autocommit = True
                try:
                    with self.conn.cursor() as cur:
                        cur.execute("VACUUM plateau_buildings")
                finally:
                    self.conn.autocommit = old_autocommit
                logger.info(f"      ✅ VACUUM 完了 ({time.time() - vacuum_start:.1f}s)")

        total_elapsed = time.time() - start_time
        logger.info(f"✅ Step 2 完了: 全 {batch_count} バッチ実行（{total_elapsed / 60:.1f}分）")

    def step3_create_index(self) -> None:
        """Step 3: city_code インデックス作成"""
        if self.check_index_exists():
            logger.info("Step 3: idx_buildings_city_code は既に存在します（スキップ）")
            return
        logger.info("Step 3: idx_buildings_city_code をCONCURRENTLYで作成中...")
        # CREATE INDEX CONCURRENTLY はトランザクション外で実行する必要がある
        # 別接続でautocommitモードを使い、確実にトランザクション外で実行
        new_conn = psycopg2.connect(self.postgres_url)
        new_conn.autocommit = True
        try:
            with new_conn.cursor() as cur:
                cur.execute(
                    "CREATE INDEX CONCURRENTLY idx_buildings_city_code "
                    "ON plateau_buildings(city_code)"
                )
        finally:
            new_conn.close()
        logger.info("✅ Step 3 完了: インデックス作成")

    def step4_set_not_null(self) -> None:
        """Step 4: NOT NULL 制約追加"""
        if self.check_not_null_constraint():
            logger.info("Step 4: NOT NULL 制約は既に設定済み（スキップ）")
            return

        # 全行 city_code 埋まっていることを確認
        unmigrated = self.count_unmigrated()
        if unmigrated > 0:
            logger.error(f"❌ Step 4 スキップ: {unmigrated:,} 行が未設定のため NOT NULL 制約を追加できません")
            return

        logger.info("Step 4: NOT NULL 制約を追加中...")
        with self.conn.cursor() as cur:
            cur.execute(
                "ALTER TABLE plateau_buildings ALTER COLUMN city_code SET NOT NULL"
            )
        self.conn.commit()
        logger.info("✅ Step 4 完了: NOT NULL 制約追加")

    def execute(self) -> None:
        """本番マイグレーション実行"""
        self.connect(readonly=False)
        lock_acquired = False
        try:
            # 排他ロック取得
            lock_acquired = self.acquire_lock()
            if not lock_acquired:
                sys.exit(1)
            logger.info(f"🔒 排他ロック取得（ID: 0x{ADVISORY_LOCK_ID:08X}）")

            start_time = time.time()
            logger.info("=" * 60)
            logger.info("🚀 マイグレーション開始")
            logger.info("=" * 60)

            self.step1_add_column()
            self.step2_populate_data()
            self.step3_create_index()
            self.step4_set_not_null()

            elapsed = time.time() - start_time
            logger.info("=" * 60)
            logger.info(f"🎉 マイグレーション完了（合計 {elapsed / 60:.1f}分）")
            logger.info("=" * 60)
        except Exception as e:
            logger.error(f"❌ マイグレーション失敗: {e}", exc_info=True)
            self.conn.rollback()
            sys.exit(1)
        finally:
            if lock_acquired:
                self.release_lock()
                logger.info("🔓 排他ロック解放")
            self.close()


def confirm_execute() -> bool:
    """ユーザに本番実行の確認を求める"""
    print()
    print("=" * 60)
    print("⚠️  本番マイグレーションを実行します")
    print("=" * 60)
    print("以下の変更が plateau_buildings テーブルに加えられます:")
    print("  Step 1: city_code カラム追加 (VARCHAR(5))")
    print("  Step 2: 既存データから city_code を設定（バッチ処理）")
    print("  Step 3: city_code インデックス作成 (CONCURRENTLY)")
    print("  Step 4: city_code に NOT NULL 制約追加")
    print()
    print("ロールバック方法:")
    print("  ALTER TABLE plateau_buildings DROP COLUMN city_code;")
    print()
    response = input("実行しますか？ [yes/no]: ").strip().lower()
    return response in ('yes', 'y')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Plateau DB マイグレーションツール',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        help='本番実行モード（指定しない場合は Dry Run）',
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='確認プロンプトをスキップ（--execute との併用）',
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

    runner = Migrator(args.postgres_url)

    if args.execute:
        # 本番実行前に Dry Run で確認
        logger.info("📋 本番実行前に Dry Run で状態確認...")
        runner.dry_run()
        runner.print_dry_run_summary()

        # 安全判定
        ext = runner.results['extraction']
        est = runner.results['estimate']
        if ext['failed_count'] > 0:
            logger.error("❌ 抽出失敗データがあるため本番実行を中止します")
            sys.exit(1)
        if not est['safe_batch']:
            logger.error("❌ ディスク容量不足のため本番実行を中止します")
            sys.exit(1)

        # 確認プロンプト
        if not args.yes and not confirm_execute():
            logger.info("実行をキャンセルしました")
            sys.exit(0)

        # 本番実行
        runner = Migrator(args.postgres_url)  # 新しい接続
        runner.execute()
    else:
        # Dry Run のみ
        results = runner.dry_run()
        if args.format == 'json':
            print(json.dumps(results, indent=2, ensure_ascii=False, default=str))
        else:
            runner.print_dry_run_summary()


if __name__ == '__main__':
    main()
