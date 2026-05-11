#!/usr/bin/env python3
"""
Plateau カバレッジビュー管理ツール

Plateau対応エリアの GeoJSON 配信用マテリアライズドビュー
`plateau_coverage` の作成・更新を行う。

Issue #9 (rapid側) 対応のサーバ側基盤。

機能:
    - マテリアライズドビューの作成（初回のみ、冪等性あり）
    - REFRESH MATERIALIZED VIEW CONCURRENTLY によるデータ更新
    - カバレッジ情報の取得（API モジュールから利用）

使い方:
    # ビュー作成（初回のみ、再実行は安全）
    python plateau_coverage.py --init

    # データ更新（インポート・パージ後に実行）
    python plateau_coverage.py --refresh

    # 状態確認
    python plateau_coverage.py --status

    # 接続URL指定
    python plateau_coverage.py --refresh --postgres-url "postgresql://..."
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Concave Hull の target_percent (0=最も凹型、1=凸包と同等)
# 0.5 で十分な凹型表現を得つつ頂点数を抑えられる (横浜市 868K建物 → 57頂点)
CONCAVE_HULL_PERCENT = 0.5

# マテリアライズドビュー作成SQL
# 各都市の建物centroidからConcaveHullを生成。
# 点数不足等でConcaveHullが失敗した場合はConvexHullにフォールバック。
MATERIALIZED_VIEW_DDL = f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS plateau_coverage AS
SELECT
    city_code,
    COALESCE(
        ST_ConcaveHull(ST_Collect(centroid), {CONCAVE_HULL_PERCENT}),
        ST_ConvexHull(ST_Collect(centroid))
    ) AS geom,
    count(*) AS building_count
FROM plateau_buildings
WHERE city_code IS NOT NULL
GROUP BY city_code
WITH NO DATA;
"""

# CONCURRENTLY REFRESH に必要な UNIQUE INDEX
UNIQUE_INDEX_DDL = """
CREATE UNIQUE INDEX IF NOT EXISTS plateau_coverage_city_code_idx
    ON plateau_coverage(city_code);
"""

# 空間検索高速化用 GIST インデックス
GEOM_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS plateau_coverage_geom_idx
    ON plateau_coverage USING GIST(geom);
"""


class CoverageManager:
    """カバレッジビュー管理"""

    def __init__(self, postgres_url: Optional[str] = None):
        if postgres_url is None:
            postgres_url = os.getenv(
                'DATABASE_URL',
                'postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau'
            )
        self.postgres_url = postgres_url

    def get_connection(self, autocommit: bool = False) -> psycopg2.extensions.connection:
        """DB接続を取得"""
        conn = psycopg2.connect(self.postgres_url)
        conn.autocommit = autocommit
        return conn

    def view_exists(self, conn: psycopg2.extensions.connection) -> bool:
        """マテリアライズドビューが存在するか確認"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_matviews
                    WHERE schemaname = 'public'
                      AND matviewname = 'plateau_coverage'
                )
            """)
            return cur.fetchone()[0]

    def view_has_data(self, conn: psycopg2.extensions.connection) -> bool:
        """ビューがデータを保持しているか確認（NO DATA直後はFalse）"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ispopulated FROM pg_matviews
                WHERE schemaname = 'public'
                  AND matviewname = 'plateau_coverage'
            """)
            row = cur.fetchone()
            return bool(row and row[0])

    def get_status(self) -> Dict[str, Any]:
        """ビューの状態を返す"""
        conn = self.get_connection()
        try:
            exists = self.view_exists(conn)
            status = {'view_exists': exists}
            if not exists:
                return status

            populated = self.view_has_data(conn)
            status['populated'] = populated

            if populated:
                with conn.cursor() as cur:
                    cur.execute("SELECT count(*) FROM plateau_coverage")
                    status['city_count'] = cur.fetchone()[0]

                    cur.execute("""
                        SELECT
                            pg_size_pretty(pg_total_relation_size('plateau_coverage'))
                    """)
                    status['size'] = cur.fetchone()[0]

                    cur.execute("""
                        SELECT city_code, building_count
                        FROM plateau_coverage
                        ORDER BY building_count DESC
                        LIMIT 5
                    """)
                    status['top_cities'] = [
                        {'city_code': row[0], 'building_count': row[1]}
                        for row in cur.fetchall()
                    ]
            return status
        finally:
            conn.close()

    def init_view(self) -> None:
        """マテリアライズドビューを作成（初回のみ）"""
        logger.info("📋 plateau_coverage マテリアライズドビューを作成中...")
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Step 1: ビュー作成（WITH NO DATA で初期状態は空）
                cur.execute(MATERIALIZED_VIEW_DDL)
                logger.info("   ✅ ビュー定義作成")

                # Step 2: UNIQUE INDEX（CONCURRENTLY REFRESH に必須）
                cur.execute(UNIQUE_INDEX_DDL)
                logger.info("   ✅ UNIQUE INDEX 作成")

                # Step 3: GIST INDEX（空間検索用）
                cur.execute(GEOM_INDEX_DDL)
                logger.info("   ✅ GIST INDEX 作成")

            conn.commit()
            logger.info("✅ ビュー初期化完了")
            logger.info("   次は --refresh でデータを投入してください")
        finally:
            conn.close()

    def drop_view(self) -> None:
        """マテリアライズドビューを削除（定義変更時に使用）"""
        logger.info("🗑️  plateau_coverage マテリアライズドビューを削除中...")
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("DROP MATERIALIZED VIEW IF EXISTS plateau_coverage CASCADE")
            conn.commit()
            logger.info("✅ ビュー削除完了")
        finally:
            conn.close()

    def reinit_view(self) -> None:
        """ビュー定義を更新（DROP → CREATE → REFRESH）"""
        logger.info("🔄 plateau_coverage ビューを再構築中...")
        self.drop_view()
        self.init_view()
        self.refresh(concurrent=False)  # 初回扱い

    def refresh(self, concurrent: bool = True) -> Dict[str, Any]:
        """ビューをリフレッシュ"""
        conn = self.get_connection()
        try:
            if not self.view_exists(conn):
                logger.error("❌ plateau_coverage ビューが存在しません。先に --init を実行してください")
                return {'error': 'view_not_found'}

            populated = self.view_has_data(conn)
        finally:
            conn.close()

        # REFRESH はトランザクション外で実行する必要がある
        new_conn = self.get_connection(autocommit=True)
        try:
            t0 = time.time()
            with new_conn.cursor() as cur:
                if populated and concurrent:
                    logger.info("🔄 REFRESH MATERIALIZED VIEW CONCURRENTLY plateau_coverage ...")
                    cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY plateau_coverage")
                else:
                    if not populated:
                        logger.info("🔄 初回のため通常REFRESH（CONCURRENTLYなし）...")
                    else:
                        logger.info("🔄 REFRESH MATERIALIZED VIEW plateau_coverage ...")
                    cur.execute("REFRESH MATERIALIZED VIEW plateau_coverage")
            elapsed = time.time() - t0
            logger.info(f"✅ REFRESH 完了 ({elapsed:.1f}s)")
            return {'duration_seconds': elapsed}
        finally:
            new_conn.close()

    def get_coverage_geojson(self) -> Dict[str, Any]:
        """カバレッジGeoJSON FeatureCollection を取得 (API用)"""
        conn = self.get_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        city_code,
                        ST_AsGeoJSON(geom)::json AS geom,
                        building_count
                    FROM plateau_coverage
                    ORDER BY city_code
                """)
                features = [
                    {
                        "type": "Feature",
                        "id": row['city_code'],
                        "geometry": row['geom'],
                        "properties": {
                            "city_code": row['city_code'],
                            "building_count": row['building_count'],
                        },
                    }
                    for row in cur.fetchall()
                ]
            return {
                "type": "FeatureCollection",
                "features": features,
            }
        finally:
            conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Plateau カバレッジビュー管理ツール',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--init',
        action='store_true',
        help='マテリアライズドビューを作成（初回のみ、冪等）',
    )
    group.add_argument(
        '--reinit',
        action='store_true',
        help='ビュー定義を更新（DROP → CREATE → REFRESH、定義変更時に使用）',
    )
    group.add_argument(
        '--refresh',
        action='store_true',
        help='ビューをリフレッシュ（インポート・パージ後に実行）',
    )
    group.add_argument(
        '--status',
        action='store_true',
        help='ビューの状態を表示',
    )
    parser.add_argument(
        '--postgres-url',
        help='PostgreSQL接続URL (デフォルト: 環境変数 DATABASE_URL)',
    )
    parser.add_argument(
        '--no-concurrent',
        action='store_true',
        help='--refresh で CONCURRENTLY を使わない（デバッグ用）',
    )
    parser.add_argument(
        '--format',
        choices=['text', 'json'],
        default='text',
        help='--status の出力形式 (default: text)',
    )
    args = parser.parse_args()

    mgr = CoverageManager(args.postgres_url)

    if args.init:
        mgr.init_view()
        logger.info("")
        mgr.refresh(concurrent=False)  # 初回は CONCURRENTLY 不可
    elif args.reinit:
        mgr.reinit_view()
    elif args.refresh:
        mgr.refresh(concurrent=not args.no_concurrent)
    elif args.status:
        status = mgr.get_status()
        if args.format == 'json':
            print(json.dumps(status, indent=2, ensure_ascii=False, default=str))
        else:
            print()
            print("=" * 60)
            print("📊 plateau_coverage ビュー状態")
            print("=" * 60)
            print(f"ビュー存在: {'✅' if status.get('view_exists') else '❌'}")
            if not status.get('view_exists'):
                print("\n初期化するには --init を実行してください")
                return
            print(f"データ投入済: {'✅' if status.get('populated') else '❌ (REFRESH 未実行)'}")
            if status.get('populated'):
                print(f"都市数: {status['city_count']}")
                print(f"ビューサイズ: {status['size']}")
                print(f"\n建物数 Top 5:")
                for c in status.get('top_cities', []):
                    print(f"  {c['city_code']}  {c['building_count']:>10,} 件")


if __name__ == '__main__':
    main()
