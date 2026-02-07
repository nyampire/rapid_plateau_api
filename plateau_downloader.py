#!/usr/bin/env python3
"""
Plateau建物データ ダウンローダー
surveyor.mydns.jp から指定市区町村のメッシュデータを取得してローカルに保存

データソース: http://surveyor.mydns.jp/task-bldg/
対象: 全国289市区町村のPlateauデータ

使用例:
  # 利用可能な市区町村一覧を表示
  python plateau_downloader.py --list

  # 市区町村コード指定でダウンロード
  python plateau_downloader.py --citycode 31202

  # 市区町村名で検索してダウンロード
  python plateau_downloader.py --cityname 米子

  # 出力先指定
  python plateau_downloader.py --citycode 13101 --output-dir ./chiyoda_data
"""

import requests
import os
import re
import json
import logging
from pathlib import Path
from typing import List, Set, Dict, Tuple, Optional
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('plateau_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class PlateauDownloader:
    """Plateau建物データ汎用ダウンローダー"""

    CITY_LIST_URL = "http://surveyor.mydns.jp/task-bldg/city"
    MESH_URL_TEMPLATE = "http://surveyor.mydns.jp/task-bldg/mesh/{citycode}"
    DATA_BASE_URL = "https://surveyor.mydns.jp/osm-data"

    def __init__(self, citycode: str, output_dir: Optional[str] = None):
        """
        Args:
            citycode: 市区町村コード (例: "31202")
            output_dir: 出力ディレクトリ。Noneの場合は ./plateau_data/{citycode} を使用
        """
        self.citycode = citycode
        self.city_info = None  # fetch_city_info で設定
        self.folder = None     # fetch_city_info で設定

        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = Path(f"./plateau_data/{citycode}")

        self.output_dir.mkdir(parents=True, exist_ok=True)

        # セッション設定
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; OSMFJ-PlateauDownloader/2.0)',
            'Accept': 'application/zip, application/octet-stream, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ja,en;q=0.9',
            'Connection': 'keep-alive'
        })
        self.session.timeout = 60

        # リトライ設定
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    @classmethod
    def fetch_city_list(cls) -> List[Dict]:
        """サイトから市区町村一覧を取得"""
        logger.info("🌐 市区町村一覧を取得中...")

        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; OSMFJ-PlateauDownloader/2.0)'
        })

        response = session.get(cls.CITY_LIST_URL, timeout=30)
        response.raise_for_status()

        # JavaScriptのcities配列を抽出
        content = response.text
        match = re.search(r'const\s+cities\s*=\s*(\[.*?\]);', content, re.DOTALL)
        if not match:
            raise RuntimeError("市区町村データの抽出に失敗しました")

        cities_json = match.group(1)
        # JavaScriptのJSON風データをパース（シングルクォートをダブルクォートに変換等）
        # 実際にはJSONとしてパースを試み、失敗したら正規表現で抽出
        try:
            cities = json.loads(cities_json)
        except json.JSONDecodeError:
            # JavaScript形式の場合、個別に抽出
            cities = cls._parse_cities_js(cities_json)

        logger.info(f"✅ {len(cities)}件の市区町村を取得")
        return cities

    @classmethod
    def _parse_cities_js(cls, js_text: str) -> List[Dict]:
        """JavaScript配列テキストから市区町村データを抽出"""
        cities = []
        # 各オブジェクトを抽出
        pattern = r'\{[^}]*?citycode[^}]*?\}'
        for obj_match in re.finditer(pattern, js_text, re.DOTALL):
            obj_text = obj_match.group()
            city = {}
            for key in ['citycode', 'cityname', 'folder', 'status', 'lng', 'lat']:
                val_match = re.search(rf'"{key}"\s*:\s*"([^"]*)"', obj_text)
                if not val_match:
                    val_match = re.search(rf"'{key}'\s*:\s*'([^']*)'", obj_text)
                if val_match:
                    city[key] = val_match.group(1)
                else:
                    # 数値の場合
                    val_match = re.search(rf'"{key}"\s*:\s*([\d.]+)', obj_text)
                    if val_match:
                        city[key] = val_match.group(1)
            if 'citycode' in city:
                cities.append(city)
        return cities

    def fetch_city_info(self) -> Dict:
        """指定市区町村の情報を取得"""
        cities = self.fetch_city_list()

        for city in cities:
            if city.get('citycode') == self.citycode:
                self.city_info = city
                self.folder = city.get('folder', '')
                logger.info(f"✅ 対象市区町村: {city.get('cityname', '')} ({self.citycode})")
                logger.info(f"   フォルダ: {self.folder}")
                logger.info(f"   ステータス: {city.get('status', 'unknown')}")
                return city

        raise ValueError(f"市区町村コード '{self.citycode}' が見つかりません")

    def fetch_mesh_list(self) -> List[Dict]:
        """メッシュページからメッシュ一覧とダウンロードURLを取得"""
        mesh_url = self.MESH_URL_TEMPLATE.format(citycode=self.citycode)
        logger.info(f"🌐 メッシュ一覧を取得中: {mesh_url}")

        time.sleep(2.0)  # サーバー負荷軽減
        response = self.session.get(mesh_url, timeout=45)
        response.raise_for_status()

        content = response.text

        # メッシュデータをJavaScript配列から抽出
        meshes = []
        match = re.search(r'const\s+meshes\s*=\s*(\[.*?\]);', content, re.DOTALL)
        if match:
            try:
                meshes = json.loads(match.group(1))
            except json.JSONDecodeError:
                meshes = self._parse_meshes_js(match.group(1))

        # ダウンロードURLをHTMLのリンクからも収集
        download_urls = {}
        for link_match in re.finditer(r'href="([^"]*?\.zip)"', content):
            url = link_match.group(1)
            # メッシュコードを抽出
            code_match = re.search(r'/(\d+)_bldg_', url)
            if code_match:
                download_urls[code_match.group(1)] = url

        # メッシュデータにダウンロードURLを付与
        for mesh in meshes:
            meshcode = mesh.get('meshcode', '')
            if meshcode in download_urls:
                mesh['download_url'] = download_urls[meshcode]
            elif self.folder:
                # URLを構築
                mesh['download_url'] = (
                    f"{self.DATA_BASE_URL}/{self.folder}/bldg/"
                    f"{meshcode}_bldg_6697_op.zip"
                )

        logger.info(f"✅ {len(meshes)}件のメッシュを取得")
        if download_urls:
            logger.info(f"   ダウンロードURL確認済み: {len(download_urls)}件")

        return meshes

    def _parse_meshes_js(self, js_text: str) -> List[Dict]:
        """JavaScript配列テキストからメッシュデータを抽出"""
        meshes = []
        pattern = r'\{[^}]*?meshcode[^}]*?\}'
        for obj_match in re.finditer(pattern, js_text, re.DOTALL):
            obj_text = obj_match.group()
            mesh = {}
            for key in ['meshcode', 'status', 'version']:
                val_match = re.search(rf'"{key}"\s*:\s*"([^"]*)"', obj_text)
                if not val_match:
                    val_match = re.search(rf"'{key}'\s*:\s*'([^']*)'", obj_text)
                if val_match:
                    mesh[key] = val_match.group(1)
            if 'meshcode' in mesh:
                meshes.append(mesh)
        return meshes

    def analyze_current_status(self, available_meshes: List[str]) -> Dict:
        """現在のダウンロード状況を分析"""
        logger.info("🔍 現在のダウンロード状況を分析中...")

        existing_files = list(self.output_dir.glob("*.zip"))
        existing_meshes = set()

        for file in existing_files:
            # meshcode_bldg_6697_op.zip 形式
            match = re.match(r'^(\d+)_bldg_', file.name)
            if match:
                existing_meshes.add(match.group(1))
            else:
                # meshcode.zip 形式
                match = re.match(r'^(\d+)\.zip$', file.name)
                if match:
                    existing_meshes.add(match.group(1))

        available_set = set(available_meshes)
        existing_matched = existing_meshes & available_set
        missing_meshes = available_set - existing_meshes

        total = len(available_meshes)
        status = {
            'total_available': total,
            'existing_count': len(existing_matched),
            'missing_count': len(missing_meshes),
            'existing_meshes': sorted(existing_matched),
            'missing_meshes': sorted(missing_meshes),
            'completion_rate': len(existing_matched) / total * 100 if total > 0 else 0
        }

        logger.info(f"✅ ダウンロード状況:")
        logger.info(f"   利用可能総数: {status['total_available']}件")
        logger.info(f"   既存ダウンロード: {status['existing_count']}件")
        logger.info(f"   未取得: {status['missing_count']}件")
        logger.info(f"   完了率: {status['completion_rate']:.1f}%")

        return status

    def download_single_mesh(self, mesh_info: Dict) -> Tuple[str, bool, str, int]:
        """単一メッシュの安全ダウンロード"""
        meshcode = mesh_info.get('meshcode', '')
        download_url = mesh_info.get('download_url', '')
        max_retries = 3
        base_delay = 2.0

        if not download_url:
            return meshcode, False, "no_download_url", 0

        # ファイル名をURLから取得
        filename = download_url.split('/')[-1]
        file_path = self.output_dir / filename

        for attempt in range(max_retries):
            try:
                # 既存ファイルチェック
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    if file_size > 1000:
                        return meshcode, True, "already_exists", file_size
                    else:
                        file_path.unlink()
                        logger.debug(f"🗑️ 不完全ファイル削除: {meshcode}")

                # 接続前の待機
                if attempt > 0:
                    delay = base_delay * (2 ** attempt)
                    logger.debug(f"⏱️ リトライ前待機: {meshcode} - {delay}秒")
                    time.sleep(delay)
                else:
                    time.sleep(0.5)

                # HEADリクエストでファイル存在確認
                logger.debug(f"🔍 [{attempt+1}/{max_retries}] 存在確認: {meshcode}")
                head_response = self.session.head(download_url, timeout=30)

                if head_response.status_code == 404:
                    return meshcode, False, "file_not_found", 0
                elif head_response.status_code != 200:
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return meshcode, False, f"head_error_{head_response.status_code}", 0

                # ダウンロード実行
                logger.debug(f"📥 [{attempt+1}/{max_retries}] ダウンロード開始: {meshcode}")
                response = self.session.get(download_url, timeout=120, stream=True)
                response.raise_for_status()

                downloaded_size = 0
                chunk_size = 4096

                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            if downloaded_size % (chunk_size * 50) == 0:
                                time.sleep(0.01)

                final_size = file_path.stat().st_size

                if final_size > 1000:
                    logger.debug(f"✅ ダウンロード成功: {meshcode} - {final_size:,}bytes")
                    return meshcode, True, "downloaded", final_size
                else:
                    file_path.unlink()
                    if attempt < max_retries - 1:
                        continue
                    else:
                        return meshcode, False, "downloaded_file_too_small", 0

            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    continue
                else:
                    return meshcode, False, "timeout_error", 0

            except requests.exceptions.ConnectionError:
                if attempt < max_retries - 1:
                    time.sleep(base_delay * 2)
                    continue
                else:
                    return meshcode, False, "connection_error", 0

            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    continue
                else:
                    return meshcode, False, f"network_error: {str(e)}", 0

            except Exception as e:
                if attempt < max_retries - 1:
                    continue
                else:
                    return meshcode, False, f"unexpected_error: {str(e)}", 0

        return meshcode, False, "max_retries_exceeded", 0

    def download_missing_meshes(self, mesh_list: List[Dict], missing_codes: List[str]) -> Dict:
        """未取得メッシュの安全ダウンロード"""
        # missing_codesに該当するmesh_infoを抽出
        missing_meshes = [m for m in mesh_list if m.get('meshcode') in missing_codes]

        if not missing_meshes:
            logger.info("📥 未取得メッシュはありません")
            return {'success': [], 'failed': [], 'total_size': 0}

        logger.info(f"📥 未取得メッシュ {len(missing_meshes)}件をダウンロード中...")

        success_list = []
        failed_list = []
        total_size = 0

        for i, mesh_info in enumerate(missing_meshes, 1):
            try:
                meshcode = mesh_info.get('meshcode', '')
                progress = i / len(missing_meshes) * 100
                logger.info(f"📥 [{i:3d}/{len(missing_meshes)}] 処理中: {meshcode} ({progress:.1f}%)")

                if i > 1:
                    time.sleep(1.5)

                meshcode, success, message, size = self.download_single_mesh(mesh_info)

                if success:
                    success_list.append(meshcode)
                    total_size += size
                    if message == "downloaded":
                        logger.info(f"✅ ダウンロード成功: {meshcode} ({size:,}bytes)")
                    else:
                        logger.info(f"⏭️ 既存ファイル確認: {meshcode} ({size:,}bytes)")
                else:
                    failed_list.append((meshcode, message))
                    logger.warning(f"❌ ダウンロード失敗: {meshcode} - {message}")
                    if "404" not in message and "not_found" not in message:
                        time.sleep(3.0)

                if i % 10 == 0:
                    logger.info(f"📊 中間進捗: 成功{len(success_list)}, 失敗{len(failed_list)}, 残り{len(missing_meshes)-i}")

                if i % 20 == 0:
                    logger.info("😴 サーバー保護のための小休止: 5秒")
                    time.sleep(5.0)

            except KeyboardInterrupt:
                logger.warning("⚠️ ユーザー中断 - 現在までの結果を返します")
                break
            except Exception as e:
                failed_list.append((mesh_info.get('meshcode', ''), f"processing_error: {str(e)}"))
                logger.error(f"❌ 処理エラー: {mesh_info.get('meshcode', '')} - {e}")
                continue

        result = {
            'success': success_list,
            'failed': failed_list,
            'total_size': total_size
        }

        total_attempted = len(success_list) + len(failed_list)
        logger.info(f"📊 ダウンロード結果:")
        logger.info(f"   成功: {len(success_list)}件")
        logger.info(f"   失敗: {len(failed_list)}件")
        if total_attempted > 0:
            logger.info(f"   成功率: {len(success_list)/total_attempted*100:.1f}%")
        logger.info(f"   合計サイズ: {total_size:,}bytes ({total_size/1024/1024:.1f}MB)")

        if failed_list:
            failure_types = {}
            for mesh, reason in failed_list:
                failure_type = reason.split(':')[0] if ':' in reason else reason
                failure_types[failure_type] = failure_types.get(failure_type, 0) + 1

            logger.warning(f"   失敗分析:")
            for failure_type, count in sorted(failure_types.items(), key=lambda x: x[1], reverse=True):
                logger.warning(f"     {failure_type}: {count}件")

        return result

    def create_download_report(self, initial_status: Dict, download_result: Dict):
        """ダウンロードレポート作成"""
        report_file = self.output_dir / "download_report.txt"
        cityname = self.city_info.get('cityname', self.citycode) if self.city_info else self.citycode

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"# Plateau建物データ取得レポート: {cityname}\n")
            f.write(f"# 実行日時: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 市区町村コード: {self.citycode}\n")
            if self.folder:
                f.write(f"# フォルダ: {self.folder}\n")
            f.write("\n")

            f.write("## 取得前状況\n")
            f.write(f"利用可能総メッシュ: {initial_status['total_available']}件\n")
            f.write(f"既存ダウンロード: {initial_status['existing_count']}件\n")
            f.write(f"未取得メッシュ: {initial_status['missing_count']}件\n")
            f.write(f"完了率: {initial_status['completion_rate']:.1f}%\n\n")

            f.write("## ダウンロード実行結果\n")
            f.write(f"ダウンロード成功: {len(download_result['success'])}件\n")
            f.write(f"ダウンロード失敗: {len(download_result['failed'])}件\n")
            f.write(f"取得データサイズ: {download_result['total_size']:,}bytes ({download_result['total_size']/1024/1024:.1f}MB)\n\n")

            final_existing = initial_status['existing_count'] + len(download_result['success'])
            total = initial_status['total_available']
            final_completion = final_existing / total * 100 if total > 0 else 0

            f.write("## 最終状況\n")
            f.write(f"総ダウンロード済み: {final_existing}件\n")
            f.write(f"完了率: {final_completion:.1f}%\n")

            if final_completion >= 100.0:
                f.write("✅ 完全カバレッジ達成！\n")
            else:
                remaining = total - final_existing
                f.write(f"⚠️ 残り未取得: {remaining}件\n")

            f.write("\n## 成功メッシュ一覧\n")
            for mesh in sorted(download_result['success']):
                f.write(f"{mesh}\n")

            if download_result['failed']:
                f.write("\n## 失敗メッシュ一覧\n")
                for mesh, reason in download_result['failed']:
                    f.write(f"{mesh}: {reason}\n")

        logger.info(f"📋 レポート作成: {report_file}")

    def run(self):
        """ダウンロード実行"""
        logger.info("🚀 Plateau建物データダウンロード開始")
        logger.info("=" * 60)

        try:
            # Phase 1: 市区町村情報取得
            logger.info("\n📊 Phase 1: 市区町村情報取得")
            self.fetch_city_info()
            cityname = self.city_info.get('cityname', self.citycode)
            logger.info(f"📁 出力ディレクトリ: {self.output_dir.absolute()}")

            # Phase 2: メッシュ一覧取得
            logger.info("\n🌐 Phase 2: メッシュ一覧取得")
            mesh_list = self.fetch_mesh_list()
            if not mesh_list:
                logger.error("❌ メッシュデータが見つかりません")
                return False

            available_codes = [m.get('meshcode') for m in mesh_list if m.get('meshcode')]
            logger.info(f"   対象: {cityname} ({len(available_codes)}メッシュ)")

            # Phase 3: 現状分析
            logger.info("\n📊 Phase 3: ダウンロード状況分析")
            initial_status = self.analyze_current_status(available_codes)

            if initial_status['missing_count'] == 0:
                logger.info("🎉 既に全メッシュダウンロード済みです！")
                self.create_download_report(initial_status, {'success': [], 'failed': [], 'total_size': 0})
                return True

            # Phase 4: ダウンロード
            logger.info(f"\n📥 Phase 4: 未取得メッシュダウンロード ({initial_status['missing_count']}件)")
            download_result = self.download_missing_meshes(mesh_list, initial_status['missing_meshes'])

            # Phase 5: レポート作成
            logger.info(f"\n📋 Phase 5: レポート作成")
            self.create_download_report(initial_status, download_result)

            # Phase 6: 最終確認
            logger.info(f"\n🔍 Phase 6: 最終確認")
            final_status = self.analyze_current_status(available_codes)

            success = final_status['completion_rate'] >= 100.0

            logger.info("=" * 60)
            if success:
                logger.info(f"🎉 {cityname} Plateau建物データ取得成功!")
                logger.info(f"✅ {len(available_codes)}件の全メッシュダウンロード完了")
                logger.info("🚀 次は plateau_importer2postgis.py でDBインポートを実行")
            else:
                logger.warning("⚠️ 一部メッシュの取得に失敗しました")
                logger.info(f"📊 達成率: {final_status['completion_rate']:.1f}%")
                logger.info("🔄 再実行で続きからダウンロード可能です")
            logger.info("=" * 60)

            return success

        except Exception as e:
            logger.error(f"❌ ダウンロード失敗: {e}")
            import traceback
            traceback.print_exc()
            return False


def print_city_list(cities: List[Dict], filter_text: Optional[str] = None):
    """市区町村一覧を表示"""
    if filter_text:
        cities = [c for c in cities if filter_text in c.get('cityname', '') or filter_text in c.get('citycode', '')]

    if not cities:
        print("該当する市区町村が見つかりません")
        return

    print(f"\n{'コード':>6}  {'ステータス':<12}  {'市区町村名'}")
    print("-" * 60)
    for city in sorted(cities, key=lambda c: c.get('citycode', '')):
        code = city.get('citycode', '')
        name = city.get('cityname', '')
        status = city.get('status', '')
        print(f"{code:>6}  {status:<12}  {name}")
    print(f"\n合計: {len(cities)}件")


def run_all_cities(base_output_dir: Optional[str] = None, city_interval: int = 30):
    """全市区町村を順次ダウンロード"""
    logger.info("🌏 全市区町村一括ダウンロード開始")
    logger.info("=" * 60)

    cities = PlateauDownloader.fetch_city_list()
    cities_sorted = sorted(cities, key=lambda c: c.get('citycode', ''))

    total = len(cities_sorted)
    logger.info(f"📊 対象: {total}市区町村")
    logger.info(f"⏱️ 市区町村間インターバル: {city_interval}秒")
    logger.info("=" * 60)

    success_cities = []
    failed_cities = []
    skipped_cities = []

    for i, city in enumerate(cities_sorted, 1):
        citycode = city.get('citycode', '')
        cityname = city.get('cityname', '')

        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"🏙️ [{i}/{total}] {cityname} ({citycode})")
            logger.info(f"{'='*60}")

            if base_output_dir:
                output_dir = str(Path(base_output_dir) / citycode)
            else:
                output_dir = None  # デフォルト (./plateau_data/{citycode})

            downloader = PlateauDownloader(citycode, output_dir)

            try:
                success = downloader.run()
            except ValueError as e:
                logger.warning(f"⏭️ スキップ: {cityname} ({citycode}) - {e}")
                skipped_cities.append((citycode, cityname, str(e)))
                continue

            if success:
                success_cities.append((citycode, cityname))
                logger.info(f"✅ {cityname} 完了")
            else:
                failed_cities.append((citycode, cityname))
                logger.warning(f"⚠️ {cityname} 一部失敗")

            # 市区町村間のインターバル（最後の市区町村以外）
            if i < total:
                logger.info(f"😴 次の市区町村まで {city_interval}秒 待機中...")
                time.sleep(city_interval)

        except KeyboardInterrupt:
            logger.warning(f"\n⚠️ ユーザー中断 ({i}/{total}市区町村処理済み)")
            break
        except Exception as e:
            failed_cities.append((citycode, cityname))
            logger.error(f"❌ {cityname} ({citycode}) エラー: {e}")
            if i < total:
                time.sleep(city_interval)
            continue

    # 最終レポート
    logger.info("\n" + "=" * 60)
    logger.info("📊 全市区町村ダウンロード結果")
    logger.info("=" * 60)
    logger.info(f"   成功: {len(success_cities)}件")
    logger.info(f"   失敗: {len(failed_cities)}件")
    logger.info(f"   スキップ: {len(skipped_cities)}件")
    logger.info(f"   合計: {len(success_cities) + len(failed_cities) + len(skipped_cities)}/{total}件")

    if failed_cities:
        logger.warning("   失敗一覧:")
        for code, name in failed_cities:
            logger.warning(f"     {code} {name}")

    if skipped_cities:
        logger.info("   スキップ一覧:")
        for code, name, reason in skipped_cities:
            logger.info(f"     {code} {name}: {reason}")

    print(f"\n📊 結果: 成功 {len(success_cities)}, 失敗 {len(failed_cities)}, スキップ {len(skipped_cities)}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Plateau建物データ ダウンローダー')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--citycode', help='市区町村コード (例: 31202)')
    group.add_argument('--all', action='store_true', help='全市区町村を一括ダウンロード')
    parser.add_argument('--cityname', help='市区町村名で検索 (部分一致)')
    parser.add_argument('--list', action='store_true', help='利用可能な市区町村一覧を表示')
    parser.add_argument('--output-dir', help='出力ディレクトリ')
    parser.add_argument('--city-interval', type=int, default=30,
                       help='--all 時の市区町村間の待機秒数 (default: 30)')
    parser.add_argument('--verbose', action='store_true', help='詳細ログ出力')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # 一覧表示モード
    if args.list or (not args.citycode and not args.all and args.cityname):
        cities = PlateauDownloader.fetch_city_list()
        print_city_list(cities, args.cityname)
        return

    # 全市区町村一括ダウンロード
    if args.all:
        run_all_cities(args.output_dir, args.city_interval)
        return

    # 単一市区町村ダウンロード
    if not args.citycode:
        parser.print_help()
        print("\n例:")
        print("  python plateau_downloader.py --list                    # 一覧表示")
        print("  python plateau_downloader.py --cityname 米子           # 名前で検索")
        print("  python plateau_downloader.py --citycode 31202          # 単一ダウンロード")
        print("  python plateau_downloader.py --all                     # 全市区町村一括")
        print("  python plateau_downloader.py --all --city-interval 60  # インターバル60秒")
        return

    logger.info("🏗️ Plateau建物データ ダウンローダー起動")

    downloader = PlateauDownloader(args.citycode, args.output_dir)
    success = downloader.run()

    if success:
        print(f"\n🎉 ダウンロード成功!")
        print(f"📁 データ保存場所: {downloader.output_dir}")
        print(f"🚀 次のコマンド: python plateau_importer2postgis.py --data-dir {downloader.output_dir}")
    else:
        print("\n❌ 一部の取得に失敗しました")
        print("📋 詳細: plateau_downloader.log を確認")
        print("🔄 再実行で続きからダウンロード可能")


if __name__ == "__main__":
    main()
