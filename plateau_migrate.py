#!/usr/bin/env python3
"""
Plateau DB マイグレーションツール

plateau_buildings テーブルへの city_code カラム追加とデータ移行を行う。
Issue #3 (Phase 1) 対応。

現状: Dry Run 機能のみ実装。本番マイグレーション機能は後続PRで追加予定。

使い方:
    # Dry Run（デフォルト、読み取り専用）
    python plateau_migrate.py

    # 接続URL指定
    python plateau_migrate.py --postgres-url "postgresql://user:pass@host:5432/db"

    # JSON出力
    python plateau_migrate.py --format json
"""

import argparse
import json
import logging
import os
import re
import shutil
import sys
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
CITY_CODE_PATTERN = re.compile(r'plateau_(\d{5})_')

# Step 2 の見積もり用定数
ROW_SIZE_BYTES = 641  # plateau_buildings の平均行サイズ
BATCH_SIZE = 1_000_000  # 推奨バッチサイズ


class MigrationDryRun:
    """マイグレーション Dry Run ツール"""

    def __init__(self, postgres_url: Optional[str] = None):
        if postgres_url is None:
            postgres_url = os.getenv(
                'DATABASE_URL',
                'postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau'
            )
        self.postgres_url = postgres_url
        self.conn: Optional[psycopg2.extensions.connection] = None
        self.results: Dict[str, Any] = {}

    def connect(self) -> None:
        """DB接続"""
        try:
            self.conn = psycopg2.connect(self.postgres_url)
            self.conn.set_session(readonly=True)  # Dry Run なので読み取り専用
            logger.info("✅ DB接続成功（読み取り専用モード）")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ DB接続失敗: {e}")
            sys.exit(1)

    def close(self) -> None:
        if self.conn:
            self.conn.close()

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

    def count_total_rows(self) -> int:
        """plateau_buildings の総行数"""
        logger.info("📊 plateau_buildings の総行数を確認中...")
        with self.conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM plateau_buildings")
            total = cur.fetchone()[0]
        logger.info(f"   総行数: {total:,} 行")
        return total

    def analyze_extraction(self) -> Dict[str, Any]:
        """source_dataset から city_code を抽出した結果を分析"""
        logger.info("🔍 source_dataset から city_code 抽出を試行中...")
        with self.conn.cursor() as cur:
            # 抽出可能な行数
            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE source_dataset ~ 'plateau_\\d{5}_'
            """)
            extractable = cur.fetchone()[0]

            # source_dataset が NULL の行数
            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE source_dataset IS NULL
            """)
            null_count = cur.fetchone()[0]

            # 抽出失敗（NULL以外でパターンに一致しない）
            cur.execute("""
                SELECT count(*) FROM plateau_buildings
                WHERE source_dataset IS NOT NULL
                  AND source_dataset !~ 'plateau_\\d{5}_'
            """)
            failed = cur.fetchone()[0]

            # 抽出失敗の例（最大10件）
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
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT
                    substring(source_dataset from 'plateau_(\\d{5})_') AS city_code,
                    count(*) AS row_count
                FROM plateau_buildings
                WHERE source_dataset ~ 'plateau_\\d{5}_'
                GROUP BY city_code
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

        unexpected = found - expected  # DBにあるがリストにない
        missing = expected - found  # リストにあるがDBにない

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

        # ピーク容量増（バッチ方式）
        # 1バッチあたりの最悪膨張 = BATCH_SIZE × ROW_SIZE × 2（旧+新タプル）
        peak_bloat_gb = (BATCH_SIZE * ROW_SIZE_BYTES * 2) / (1024 ** 3)

        # 一括方式のピーク容量増
        full_bloat_gb = (extractable * ROW_SIZE_BYTES * 2) / (1024 ** 3)

        # バッチ数
        batch_count = (extractable + BATCH_SIZE - 1) // BATCH_SIZE

        # バッチ方式の所要時間（1バッチ約5分と仮定）
        batch_time_min = batch_count * 5

        # 安全判定
        safe_batch = free_gb > peak_bloat_gb + 1.0  # 1GB安全マージン
        safe_full = free_gb > full_bloat_gb + 2.0  # 2GB安全マージン

        result = {
            'batch_count': batch_count,
            'batch_size': BATCH_SIZE,
            'estimated_time_min_batch': batch_time_min,
            'estimated_time_min_full': max(15, extractable // 500_000),  # 50万行/分と仮定
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

    def run(self) -> Dict[str, Any]:
        """Dry Run 実行"""
        self.connect()
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

    def print_summary(self) -> None:
        """人間向けサマリ出力"""
        print()
        print("=" * 60)
        print("🔍 Dry Run サマリ: city_code カラム追加マイグレーション")
        print("=" * 60)

        # ディスク
        disk = self.results['disk']
        print(f"\n💾 ディスク状況:")
        print(f"   合計: {disk['total_gb']}GB / 空き: {disk['free_gb']}GB ({100 - disk['used_percent']:.1f}%)")

        # データ
        total = self.results['total_rows']
        ext = self.results['extraction']
        print(f"\n📊 plateau_buildings:")
        print(f"   総行数: {total:,}")
        print(f"   抽出可能: {ext['extractable']:,} ({ext['extractable'] / total * 100:.2f}%)")
        if ext['null_count']:
            print(f"   ⚠️ source_dataset NULL: {ext['null_count']:,}")
        if ext['failed_count']:
            print(f"   ❌ 抽出失敗: {ext['failed_count']:,}")

        # 都市分布（上位10）
        dist = self.results['distribution']
        print(f"\n📍 city_code 分布（上位10都市 / 全{len(dist)}都市）:")
        for item in dist[:10]:
            print(f"   {item['city_code']}  {item['row_count']:>10,} 行")

        # 整合性
        cc = self.results['cities_check']
        if not cc.get('skipped'):
            print(f"\n📋 CITIES_2024 リストとの整合性:")
            print(f"   期待: {cc['expected_count']} 都市 / DB内: {cc['found_count']} 都市")
            if cc['unexpected']:
                print(f"   ⚠️ リスト外: {cc['unexpected']}")

        # 見積もり
        est = self.results['estimate']
        print(f"\n⏱️ マイグレーション見積もり:")
        print(f"   推奨: バッチ方式（{est['batch_count']}バッチ × 約5分 = 約{est['estimated_time_min_batch']}分）")
        print(f"   ピーク容量: 約{est['peak_bloat_gb_batch']}GB")
        print(f"   判定: {'✅ 実施可能' if est['safe_batch'] else '❌ 容量不足'}")

        # 最終判定
        print()
        print("=" * 60)
        if self.results['city_code_exists']:
            print("ℹ️ 既にマイグレーション済みです（city_code カラム存在）")
        elif ext['failed_count'] > 0 or ext['null_count'] > 0:
            print("⚠️ 抽出失敗データがあります。原因を調査してから実行してください")
        elif est['safe_batch']:
            print("✅ Dry Run 完了。本番マイグレーション実行可能です（バッチ方式推奨）")
        else:
            print("❌ ディスク容量不足です。空き容量を確保してから再度確認してください")
        print("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Plateau DB マイグレーション Dry Run ツール',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--postgres-url',
        help='PostgreSQL接続URL (デフォルト: 環境変数 DATABASE_URL)',
    )
    parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='出力形式 (default: text)',
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細ログ出力',
    )
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    runner = MigrationDryRun(args.postgres_url)
    results = runner.run()

    if args.format == 'json':
        print(json.dumps(results, indent=2, ensure_ascii=False, default=str))
    else:
        runner.print_summary()


if __name__ == '__main__':
    main()
