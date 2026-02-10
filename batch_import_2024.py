#!/usr/bin/env python3
"""
2024年度Plateau都市データ 一括ダウンロード＆インポートスクリプト

各都市を順番に処理:
  1. ダウンロード（plateau_downloader.py）
  2. PostGISインポート（plateau_importer2postgis.py）
  3. ZIPファイル削除（ストレージ節約）

使用例:
  # 全2024年度都市を処理
  python batch_import_2024.py --postgres-url "postgresql://user:pass@localhost/db"

  # 特定の都市だけ処理
  python batch_import_2024.py --postgres-url "..." --citycodes 21211 16211 33423

  # ドライラン（ダウンロード・インポートせず一覧表示のみ）
  python batch_import_2024.py --dry-run

  # 既にインポート済みの都市をスキップして続きから
  python batch_import_2024.py --postgres-url "..." --skip-imported
"""

import subprocess
import sys
import os
import time
import json
import logging
import shutil
import argparse
from pathlib import Path
from datetime import datetime

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('batch_import_2024.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 2024年度都市コード一覧（surveyor.mydns.jpのfolder名に"2024"を含む都市）
# 既にインポート済みの都市（12221八千代市、33423早島町、16211射水市）は含めない
CITIES_2024 = [
    "03201",  # 盛岡市
    "03202",  # 宮古市
    "04100",  # 仙台市
    "05204",  # 大館市
    "07201",  # 福島市
    "11100",  # さいたま市
    "11202",  # 熊谷市
    "11203",  # 川口市
    "11208",  # 所沢市
    "11217",  # 鴻巣市
    "11223",  # 蕨市
    "11228",  # 志木市
    "11230",  # 新座市
    "11235",  # 富士見市
    "11237",  # 三郷市
    "11240",  # 幸手市
    "11241",  # 鶴ヶ島市
    "11301",  # 伊奈町
    "11324",  # 三芳町
    "11385",  # 上里町
    "12100",  # 千葉市
    "12206",  # 木更津市
    "13106",  # 台東区
    "13107",  # 墨田区
    "13115",  # 杉並区
    "13201",  # 八王子市
    "13202",  # 立川市
    "13203",  # 武蔵野市
    "13204",  # 三鷹市
    "13205",  # 青梅市
    "13206",  # 府中市
    "13207",  # 昭島市
    "13209",  # 町田市
    "13210",  # 小金井市
    "13211",  # 小平市
    "13212",  # 日野市
    "13213",  # 東村山市
    "13214",  # 国分寺市
    "13215",  # 国立市
    "13218",  # 福生市
    "13219",  # 狛江市
    "13220",  # 東大和市
    "13221",  # 清瀬市
    "13223",  # 武蔵村山市
    "13224",  # 多摩市
    "13225",  # 稲城市
    "13228",  # あきる野市
    "13229",  # 西東京市
    "13303",  # 瑞穂町
    "13308",  # 奥多摩町
    "13361",  # 大島町
    "13362",  # 利島村
    "13363",  # 新島村
    "13364",  # 神津島村
    "13381",  # 三宅村
    "13401",  # 八丈町
    "13402",  # 青ヶ島村
    "14100",  # 横浜市
    "14150",  # 相模原市
    "14204",  # 鎌倉市
    "15202",  # 長岡市
    "16202",  # 高岡市
    "17201",  # 金沢市
    "17206",  # 加賀市
    "20220",  # 安曇野市
    "21201",  # 岐阜市
    "21202",  # 大垣市
    "21211",  # 美濃加茂市
    "22100",  # 静岡市
    "22130",  # 浜松市
    "22203",  # 沼津市
    "22205",  # 熱海市
    "22206",  # 三島市
    "22207",  # 富士宮市
    "22208",  # 伊東市
    "22209",  # 島田市
    "22210",  # 富士市
    "22211",  # 磐田市
    "22212",  # 焼津市
    "22213",  # 掛川市
    "22214",  # 藤枝市
    "22215",  # 御殿場市
    "22216",  # 袋井市
    "22219",  # 下田市
    "22220",  # 裾野市
    "22221",  # 湖西市
    "22222",  # 伊豆市
    "22223",  # 御前崎市
    "22224",  # 菊川市
    "22225",  # 伊豆の国市
    "22226",  # 牧之原市
    "22301",  # 東伊豆町
    "22302",  # 河津町
    "22304",  # 南伊豆町
    "22305",  # 松崎町
    "22306",  # 西伊豆町
    "22325",  # 函南町
    "22341",  # 清水町
    "22342",  # 長泉町
    "22344",  # 小山町
    "22424",  # 吉田町
    "22429",  # 川根本町
    "22461",  # 森町
    "23100",  # 名古屋市
    "23201",  # 豊橋市
    "23206",  # 春日井市
    "23211",  # 豊田市
    "23230",  # 日進市
    "24203",  # 伊勢市
    "26100",  # 京都市
    "27100",  # 大阪市
    "27140",  # 堺市
    "27202",  # 岸和田市
    "27216",  # 河内長野市
    "27227",  # 東大阪市
    "28201",  # 姫路市
    "28215",  # 三木市
    "28229",  # たつの市
    "30201",  # 和歌山市
    "30406",  # すさみ町
    "31202",  # 米子市
    "32204",  # 益田市
    "32528",  # 隠岐の島町
    "33202",  # 倉敷市
    "33211",  # 備前市
    "34100",  # 広島市
    "34304",  # 海田町
    "36201",  # 徳島市
    "37206",  # さぬき市
    "39386",  # いの町
    "40130",  # 福岡市
    "40202",  # 大牟田市
    "40223",  # 古賀市
    "41203",  # 鳥栖市
    "42208",  # 松浦市
    "42323",  # 波佐見町
    "43206",  # 玉名市
]

# 既にインポート済みの都市（スキップ対象）
ALREADY_IMPORTED = {"12221", "33423", "16211"}

# 大規模都市（後回しにする）
LARGE_CITIES = {
    "04100",  # 仙台市
    "11100",  # さいたま市
    "13201",  # 八王子市
    "13209",  # 町田市
    "14100",  # 横浜市
    "14150",  # 相模原市
    "22100",  # 静岡市
    "22130",  # 浜松市
    "23100",  # 名古屋市
    "23211",  # 豊田市
    "26100",  # 京都市
    "27100",  # 大阪市
    "27140",  # 堺市
    "34100",  # 広島市
    "40130",  # 福岡市
}


def get_done_dir(base_dir: Path) -> Path:
    """完了記録ディレクトリを取得"""
    done_dir = base_dir / ".done"
    done_dir.mkdir(parents=True, exist_ok=True)
    return done_dir


def mark_city_done(base_dir: Path, citycode: str):
    """都市のインポート完了を記録"""
    done_file = get_done_dir(base_dir) / f"{citycode}.done"
    done_file.write_text(datetime.now().isoformat())
    logger.info(f"📝 [{citycode}] 完了記録: {done_file}")


def get_done_citycodes(base_dir: Path) -> set:
    """完了記録ファイルからインポート済み都市コードを取得"""
    done_dir = get_done_dir(base_dir)
    done_codes = set()
    for f in done_dir.glob("*.done"):
        done_codes.add(f.stem)
    return done_codes


def get_imported_citycodes(postgres_url: str) -> set:
    """DBから既にインポート済みの都市コードを取得（フォールバック）"""
    try:
        import psycopg2
        conn = psycopg2.connect(postgres_url)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT source_dataset
            FROM plateau_buildings
            WHERE source_dataset IS NOT NULL
        """)
        datasets = cursor.fetchall()
        conn.close()

        # source_datasetからcitycodeを抽出（例: "plateau_03201_59413067_bldg_6697_op.osm" → "03201"）
        imported = set()
        for (dataset,) in datasets:
            if dataset:
                import re
                match = re.search(r'(\d{5})', dataset)
                if match:
                    imported.add(match.group(1))
        return imported
    except Exception as e:
        logger.warning(f"インポート済み都市の確認に失敗: {e}")
        return set()


def cleanup_orphan_nodes(postgres_url: str):
    """孤児ノード（対応する建物がないノード）を削除"""
    try:
        import psycopg2 as pg2
        conn = pg2.connect(postgres_url)
        cursor = conn.cursor()

        # 孤児ノード数を確認
        cursor.execute("""
            SELECT COUNT(*) FROM plateau_building_nodes n
            WHERE NOT EXISTS (
                SELECT 1 FROM plateau_buildings b WHERE b.id = n.building_id
            )
        """)
        orphan_count = cursor.fetchone()[0]

        if orphan_count > 0:
            logger.info(f"🧹 孤児ノード検出: {orphan_count}件 — 削除中...")
            cursor.execute("""
                DELETE FROM plateau_building_nodes n
                WHERE NOT EXISTS (
                    SELECT 1 FROM plateau_buildings b WHERE b.id = n.building_id
                )
            """)
            conn.commit()
            logger.info(f"✅ 孤児ノード {orphan_count}件を削除")
        else:
            logger.info(f"✅ 孤児ノードなし")

        conn.close()
    except Exception as e:
        logger.warning(f"⚠️ 孤児ノードクリーンアップ失敗: {e}")


def process_city(citycode: str, base_dir: Path, postgres_url: str, python_cmd: str) -> dict:
    """1都市をダウンロード→インポート→クリーンアップ"""
    data_dir = base_dir / citycode
    result = {
        "citycode": citycode,
        "download_ok": False,
        "import_ok": False,
        "cleanup_ok": False,
        "error": None,
        "start_time": datetime.now().isoformat(),
    }

    try:
        # Phase 0: ディスク残量チェック & 孤児ノードクリーンアップ
        disk_usage = shutil.disk_usage(str(base_dir))
        free_gb = disk_usage.free / (1024**3)
        logger.info(f"💿 ディスク残量: {free_gb:.1f} GB")

        if free_gb < 5.0:
            logger.error(f"❌ [{citycode}] ディスク残量不足 ({free_gb:.1f} GB < 5 GB) — 中断")
            result["error"] = f"disk_full ({free_gb:.1f}GB free)"
            return result

        cleanup_orphan_nodes(postgres_url)

        # Phase 1: ダウンロード
        logger.info(f"📥 [{citycode}] ダウンロード開始...")
        dl_cmd = [
            python_cmd, "plateau_downloader.py",
            "--citycode", citycode,
            "--output-dir", str(data_dir)
        ]
        dl_result = subprocess.run(dl_cmd, text=True, timeout=1800)

        if dl_result.returncode != 0:
            logger.error(f"❌ [{citycode}] ダウンロード失敗")
            result["error"] = "download_failed"
            return result

        # ZIPファイルがあるか確認
        zip_files = list(data_dir.glob("*.zip"))
        if not zip_files:
            logger.warning(f"⚠️ [{citycode}] ZIPファイルなし — スキップ")
            result["error"] = "no_zip_files"
            return result

        result["download_ok"] = True
        logger.info(f"✅ [{citycode}] ダウンロード完了: {len(zip_files)}ファイル")

        # Phase 2: インポート
        logger.info(f"📦 [{citycode}] インポート開始...")
        import_cmd = [
            python_cmd, "plateau_importer2postgis.py",
            "--data-dir", str(data_dir),
            "--postgres-url", postgres_url,
            "--citycode", citycode
        ]
        import_result = subprocess.run(import_cmd, text=True, timeout=3600)

        if import_result.returncode != 0:
            logger.error(f"❌ [{citycode}] インポート失敗")
            result["error"] = "import_failed"
            return result

        # Phase 2.5: DBにデータが実際に入ったか検証
        try:
            import psycopg2 as pg2
            verify_conn = pg2.connect(postgres_url)
            verify_cur = verify_conn.cursor()
            verify_cur.execute(
                "SELECT COUNT(*) FROM plateau_buildings WHERE source_dataset LIKE %s",
                (f"%{citycode}%",)
            )
            db_count = verify_cur.fetchone()[0]
            verify_conn.close()

            if db_count == 0:
                logger.error(f"❌ [{citycode}] DB検証失敗: 建物データ0件")
                result["error"] = "import_no_data_in_db"
                return result

            logger.info(f"✅ [{citycode}] DB検証OK: {db_count}件の建物")
        except Exception as e:
            logger.warning(f"⚠️ [{citycode}] DB検証スキップ: {e}")

        result["import_ok"] = True
        logger.info(f"✅ [{citycode}] インポート完了")

        # インポート完了を記録
        mark_city_done(base_dir, citycode)

    except subprocess.TimeoutExpired:
        result["error"] = "timeout"
        logger.error(f"❌ [{citycode}] タイムアウト")
    except Exception as e:
        result["error"] = str(e)
        logger.error(f"❌ [{citycode}] エラー: {e}")
    finally:
        # ZIPファイル削除（成功・失敗を問わず必ず実行）
        if data_dir.exists():
            logger.info(f"🗑️ [{citycode}] クリーンアップ...")
            try:
                shutil.rmtree(data_dir)
                result["cleanup_ok"] = True
                logger.info(f"✅ [{citycode}] クリーンアップ完了")
            except Exception as e:
                logger.warning(f"⚠️ [{citycode}] クリーンアップ失敗: {e}")

    result["end_time"] = datetime.now().isoformat()
    return result


def main():
    parser = argparse.ArgumentParser(description='2024年度Plateau都市 一括インポート')
    parser.add_argument('--postgres-url', required=False,
                        help='PostgreSQL接続URL（未指定時はDATABASE_URL環境変数）')
    parser.add_argument('--citycodes', nargs='+', help='処理する都市コード（指定しない場合は全2024年度都市）')
    parser.add_argument('--dry-run', action='store_true', help='ドライラン（一覧表示のみ）')
    parser.add_argument('--skip-imported', action='store_true', help='インポート済み都市をスキップ')
    parser.add_argument('--small-first', action='store_true', default=True,
                        help='小規模都市を先に処理（デフォルト: True）')
    parser.add_argument('--base-dir', default='./plateau_data',
                        help='一時データディレクトリ（デフォルト: ./plateau_data）')
    parser.add_argument('--city-interval', type=int, default=10,
                        help='都市間の待機秒数（デフォルト: 10）')

    args = parser.parse_args()

    # PostgreSQL URL
    postgres_url = args.postgres_url or os.environ.get('DATABASE_URL')
    if not postgres_url and not args.dry_run:
        logger.error("❌ --postgres-url または DATABASE_URL 環境変数が必要です")
        sys.exit(1)

    # Python実行パス
    python_cmd = sys.executable

    # 対象都市の決定
    if args.citycodes:
        target_cities = args.citycodes
    else:
        target_cities = list(CITIES_2024)

    # 既にインポート済みの都市を除外
    base_dir = Path(args.base_dir)
    skip_set = set(ALREADY_IMPORTED)
    if args.skip_imported:
        # .doneファイルから完了済み都市を取得（確実）
        done_codes = get_done_citycodes(base_dir)
        skip_set = skip_set | done_codes
        logger.info(f"📊 完了記録済み都市: {len(done_codes)}件")

        # DBからも取得（フォールバック）
        if postgres_url:
            db_imported = get_imported_citycodes(postgres_url)
            new_from_db = db_imported - skip_set
            if new_from_db:
                logger.info(f"📊 DB内追加検出（.doneなし）: {len(new_from_db)}件 {new_from_db}")
                logger.info(f"   ⚠️ これらは不完全インポートの可能性あり。再処理します。")
                # .doneがない都市はスキップしない（不完全の可能性）

    target_cities = [c for c in target_cities if c not in skip_set]

    # 大規模都市を後回しにする
    if args.small_first:
        small_cities = [c for c in target_cities if c not in LARGE_CITIES]
        large_cities = [c for c in target_cities if c in LARGE_CITIES]
        target_cities = small_cities + large_cities
        logger.info(f"📊 処理順序: 小〜中規模 {len(small_cities)}都市 → 大規模 {len(large_cities)}都市")

    logger.info(f"📊 対象都市: {len(target_cities)}件")
    logger.info(f"⏭️ スキップ: {len(skip_set)}件 ({skip_set})")

    if args.dry_run:
        print(f"\n=== ドライラン: {len(target_cities)}都市 ===")
        for i, code in enumerate(target_cities, 1):
            size_label = "🏙️ 大規模" if code in LARGE_CITIES else "🏘️ 小〜中"
            print(f"  {i:3d}. {code} {size_label}")
        print(f"\nスキップ: {skip_set}")
        return

    # 処理開始
    base_dir.mkdir(parents=True, exist_ok=True)

    results = []
    success_count = 0
    fail_count = 0
    total = len(target_cities)

    logger.info("=" * 60)
    logger.info(f"🚀 一括インポート開始: {total}都市")
    logger.info("=" * 60)

    for i, citycode in enumerate(target_cities, 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"🏙️ [{i}/{total}] 都市コード: {citycode}")
        logger.info(f"{'='*60}")

        try:
            result = process_city(citycode, base_dir, postgres_url, python_cmd)
            results.append(result)

            if result["import_ok"]:
                success_count += 1
                logger.info(f"✅ [{i}/{total}] {citycode} 完了 (成功: {success_count}, 失敗: {fail_count})")
            else:
                fail_count += 1
                logger.warning(f"❌ [{i}/{total}] {citycode} 失敗: {result.get('error', 'unknown')}")

            # 都市間インターバル
            if i < total:
                logger.info(f"⏱️ {args.city_interval}秒待機...")
                time.sleep(args.city_interval)

        except KeyboardInterrupt:
            logger.warning(f"\n⚠️ ユーザー中断 ({i}/{total}処理済み)")
            break

    # 最終レポート
    logger.info("\n" + "=" * 60)
    logger.info("📊 最終レポート")
    logger.info("=" * 60)
    logger.info(f"   成功: {success_count}/{total}")
    logger.info(f"   失敗: {fail_count}/{total}")

    if fail_count > 0:
        logger.warning("   失敗都市:")
        for r in results:
            if not r["import_ok"]:
                logger.warning(f"     {r['citycode']}: {r.get('error', 'unknown')}")

    # レポートをJSONに保存
    report_file = f"batch_import_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            "total": total,
            "success": success_count,
            "failed": fail_count,
            "results": results
        }, f, ensure_ascii=False, indent=2)
    logger.info(f"📋 レポート保存: {report_file}")


if __name__ == "__main__":
    main()
