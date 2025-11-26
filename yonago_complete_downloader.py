#!/usr/bin/env python3
"""
米子市Plateau建物データ完全取得スクリプト（ダウンローダー）
153件の全メッシュを取得してローカルに保存

元データソース: http://surveyor.mydns.jp/task-bldg/mesh/31202
対象: 米子市エリアの2次メッシュ（8桁コード）153件
"""

import requests
import os
import re
import logging
from pathlib import Path
from typing import List, Set, Dict, Tuple
import time
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('yonago_complete_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class YonagoCompleteDownloader:
    def __init__(self, output_dir="./yonago_plateau_data"):
        self.output_dir = Path(output_dir)
        self.base_url = "http://surveyor.mydns.jp/task-bldg/mesh/31202"
        
        # 出力ディレクトリ作成
        self.output_dir.mkdir(exist_ok=True)
        logger.info(f"📁 出力ディレクトリ: {self.output_dir.absolute()}")
        
        # 153件の全利用可能メッシュ（確定リスト）
        self.all_available_meshes = [
            "53330256", "53330259", "53330265", "53330266", "53330267", "53330268", "53330269",
            "53330275", "53330276", "53330277", "53330278", "53330279", "53330285", "53330286",
            "53330287", "53330288", "53330289", "53330295", "53330296", "53330297", "53330298",
            "53330299", "53330350", "53330360", "53330363", "53330364", "53330365", "53330370",
            "53330371", "53330372", "53330373", "53330374", "53330375", "53330380", "53330381",
            "53330382", "53330383", "53330384", "53330385", "53330390", "53330391", "53330392",
            "53330393", "53330394", "53330395", "53330396", "53331169", "53331179", "53331189",
            "53331205", "53331206", "53331207", "53331208", "53331209", "53331215", "53331216",
            "53331217", "53331218", "53331219", "53331223", "53331224", "53331225", "53331226",
            "53331227", "53331228", "53331229", "53331231", "53331232", "53331233", "53331234",
            "53331235", "53331236", "53331237", "53331238", "53331239", "53331240", "53331241",
            "53331242", "53331243", "53331244", "53331245", "53331246", "53331247", "53331248",
            "53331249", "53331250", "53331251", "53331252", "53331253", "53331254", "53331255",
            "53331256", "53331257", "53331258", "53331259", "53331260", "53331261", "53331262",
            "53331263", "53331264", "53331265", "53331270", "53331271", "53331272", "53331273",
            "53331280", "53331281", "53331282", "53331290", "53331291", "53331300", "53331301",
            "53331302", "53331303", "53331304", "53331305", "53331306", "53331307", "53331310",
            "53331311", "53331312", "53331313", "53331314", "53331315", "53331316", "53331317",
            "53331318", "53331320", "53331321", "53331322", "53331323", "53331324", "53331325",
            "53331326", "53331327", "53331330", "53331331", "53331332", "53331333", "53331334",
            "53331335", "53331336", "53331341", "53331342", "53331343", "53331344", "53331345",
            "53331346", "53331353", "53331354", "53331355", "53331356", "53331364"
        ]
        
        # セッション設定（安定性重視）
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; OSMFJ-PlateauDownloader/1.0)',
            'Accept': 'application/zip, application/octet-stream, */*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ja,en;q=0.9',
            'Connection': 'keep-alive'
        })
        
        # タイムアウト設定
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
        
    def analyze_current_status(self) -> Dict:
        """現在のダウンロード状況を分析"""
        logger.info("🔍 現在のダウンロード状況を分析中...")
        
        existing_files = list(self.output_dir.glob("*.zip"))
        existing_meshes = set()
        
        # 既存ファイルからメッシュコードを抽出
        for file in existing_files:
            match = re.match(r'^(\d+)\.zip$', file.name)
            if match:
                existing_meshes.add(match.group(1))
            else:
                # ファイル名パターンが異なる場合の対応
                match = re.search(r'(\d{8})', file.name)
                if match:
                    existing_meshes.add(match.group(1))
        
        missing_meshes = set(self.all_available_meshes) - existing_meshes
        
        status = {
            'total_available': len(self.all_available_meshes),
            'existing_count': len(existing_meshes),
            'missing_count': len(missing_meshes),
            'existing_meshes': sorted(existing_meshes),
            'missing_meshes': sorted(missing_meshes),
            'completion_rate': len(existing_meshes) / len(self.all_available_meshes) * 100
        }
        
        logger.info(f"✅ ダウンロード状況:")
        logger.info(f"   利用可能総数: {status['total_available']}件")
        logger.info(f"   既存ダウンロード: {status['existing_count']}件")
        logger.info(f"   未取得: {status['missing_count']}件")
        logger.info(f"   完了率: {status['completion_rate']:.1f}%")
        
        if status['missing_count'] > 0:
            logger.info(f"   未取得メッシュ例: {', '.join(status['missing_meshes'][:10])}")
            if len(status['missing_meshes']) > 10:
                logger.info(f"   ... (他 {len(status['missing_meshes'])-10}件)")
        
        return status
    
    def verify_web_availability(self) -> List[str]:
        """Webサイトから利用可能メッシュを安全確認"""
        logger.info("🌐 Webサイトから利用可能メッシュを安全確認中...")
        
        try:
            # サーバー負荷軽減のための待機
            logger.debug("⏱️ サーバー保護待機: 2秒")
            time.sleep(2.0)
            
            response = self.session.get(self.base_url, timeout=45)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # zipファイルのリンクを抽出
            web_meshes = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and re.match(r'^\d+\.zip$', href):
                    mesh_code = href.replace('.zip', '')
                    web_meshes.append(mesh_code)
            
            # 既知リストと比較
            web_set = set(web_meshes)
            known_set = set(self.all_available_meshes)
            
            only_web = web_set - known_set
            only_known = known_set - web_set
            common = web_set & known_set
            
            logger.info(f"✅ Web確認結果:")
            logger.info(f"   Web発見: {len(web_meshes)}件")
            logger.info(f"   既知リスト: {len(self.all_available_meshes)}件")
            logger.info(f"   共通: {len(common)}件")
            
            if only_web:
                logger.info(f"   Webのみ: {len(only_web)}件")
                if len(only_web) <= 10:
                    logger.info(f"     新発見: {sorted(only_web)}")
                else:
                    logger.info(f"     新発見例: {sorted(only_web)[:5]} (他{len(only_web)-5}件)")
            
            if only_known:
                logger.info(f"   既知のみ: {len(only_known)}件")
                if len(only_known) <= 10:
                    logger.info(f"     Web欠如: {sorted(only_known)}")
                else:
                    logger.info(f"     Web欠如例: {sorted(only_known)[:5]} (他{len(only_known)-5}件)")
            
            # より完全なリストを使用（Web発見 + 既知）
            complete_list = sorted(list(web_set | known_set))
            logger.info(f"   統合後総数: {len(complete_list)}件")
            
            return complete_list
            
        except Exception as e:
            logger.warning(f"⚠️ Web確認失敗、既知リストを使用: {e}")
            logger.info(f"🔧 フォールバック: 既知の{len(self.all_available_meshes)}件を使用")
            return self.all_available_meshes
    
    def download_single_mesh(self, mesh_code: str) -> Tuple[str, bool, str, int]:
        """単一メッシュの安全ダウンロード（リトライ・接続確認付き）"""
        max_retries = 3
        base_delay = 2.0  # 基本待機時間
        
        for attempt in range(max_retries):
            try:
                url = f"{self.base_url}/{mesh_code}.zip"
                file_path = self.output_dir / f"{mesh_code}.zip"
                
                # 既存ファイルチェック
                if file_path.exists():
                    file_size = file_path.stat().st_size
                    if file_size > 1000:  # 1KB以上なら有効とみなす
                        return mesh_code, True, "already_exists", file_size
                    else:
                        # サイズが小さすぎる場合は削除して再試行
                        file_path.unlink()
                        logger.debug(f"🗑️ 不完全ファイル削除: {mesh_code}")
                
                # 接続前の待機（サーバー負荷軽減）
                if attempt > 0:
                    delay = base_delay * (2 ** attempt)  # 指数バックオフ
                    logger.debug(f"⏱️ リトライ前待機: {mesh_code} - {delay}秒")
                    time.sleep(delay)
                else:
                    # 初回でも少し待機
                    time.sleep(0.5)
                
                # まずHEADリクエストでファイル存在確認
                logger.debug(f"🔍 [{attempt+1}/{max_retries}] 存在確認: {mesh_code}")
                head_response = self.session.head(url, timeout=30)
                
                if head_response.status_code == 404:
                    return mesh_code, False, "file_not_found", 0
                elif head_response.status_code != 200:
                    # 200以外の場合は次の試行へ
                    if attempt < max_retries - 1:
                        logger.debug(f"⚠️ HEAD応答 {head_response.status_code}: {mesh_code} - リトライします")
                        continue
                    else:
                        return mesh_code, False, f"head_error_{head_response.status_code}", 0
                
                # ファイルサイズ確認
                content_length = head_response.headers.get('content-length')
                if content_length:
                    expected_size = int(content_length)
                    if expected_size < 1000:  # 1KB未満は異常
                        return mesh_code, False, "file_too_small_on_server", 0
                    logger.debug(f"📏 予想サイズ: {mesh_code} - {expected_size:,}bytes")
                
                # 実際のダウンロード実行
                logger.debug(f"📥 [{attempt+1}/{max_retries}] ダウンロード開始: {mesh_code}")
                response = self.session.get(url, timeout=120, stream=True)
                response.raise_for_status()
                
                # ストリーミングダウンロード（チャンクサイズ小さめ）
                downloaded_size = 0
                chunk_size = 4096  # 4KB（小さめで安定性重視）
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # 大きなファイルの場合は途中で小休止
                            if downloaded_size % (chunk_size * 50) == 0:  # 200KB毎
                                time.sleep(0.01)
                
                final_size = file_path.stat().st_size
                
                # ダウンロード成功確認
                if final_size > 1000:  # 最小サイズチェック
                    logger.debug(f"✅ ダウンロード成功: {mesh_code} - {final_size:,}bytes")
                    return mesh_code, True, "downloaded", final_size
                else:
                    file_path.unlink()  # 不完全ファイル削除
                    if attempt < max_retries - 1:
                        logger.debug(f"⚠️ ファイルサイズ異常: {mesh_code} - リトライします")
                        continue
                    else:
                        return mesh_code, False, "downloaded_file_too_small", 0
                        
            except requests.exceptions.Timeout:
                if attempt < max_retries - 1:
                    logger.debug(f"⏱️ タイムアウト: {mesh_code} - リトライします")
                    continue
                else:
                    return mesh_code, False, "timeout_error", 0
                    
            except requests.exceptions.ConnectionError:
                if attempt < max_retries - 1:
                    logger.debug(f"🔌 接続エラー: {mesh_code} - リトライします")
                    time.sleep(base_delay * 2)  # 接続エラーは長めに待機
                    continue
                else:
                    return mesh_code, False, "connection_error", 0
                    
            except requests.RequestException as e:
                if attempt < max_retries - 1:
                    logger.debug(f"🌐 ネットワークエラー: {mesh_code} - {str(e)} - リトライします")
                    continue
                else:
                    return mesh_code, False, f"network_error: {str(e)}", 0
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.debug(f"❌ 予期しないエラー: {mesh_code} - {str(e)} - リトライします")
                    continue
                else:
                    return mesh_code, False, f"unexpected_error: {str(e)}", 0
        
        # ここに到達することはないはずだが、安全のため
        return mesh_code, False, "max_retries_exceeded", 0
    
    def download_missing_meshes(self, missing_meshes: List[str]) -> Dict:
        """未取得メッシュの安全ダウンロード（シーケンシャル処理）"""
        if not missing_meshes:
            logger.info("📥 未取得メッシュはありません")
            return {'success': [], 'failed': [], 'total_size': 0}
        
        logger.info(f"📥 未取得メッシュ {len(missing_meshes)}件を安全ダウンロード中...")
        logger.info("🐌 サーバー安定性を考慮してシーケンシャル処理を実行")
        
        success_list = []
        failed_list = []
        total_size = 0
        
        # シーケンシャル処理（並列処理は停止）
        for i, mesh_code in enumerate(missing_meshes, 1):
            try:
                # 進捗表示
                progress = i / len(missing_meshes) * 100
                logger.info(f"📥 [{i:3d}/{len(missing_meshes)}] 処理中: {mesh_code} ({progress:.1f}%)")
                
                # サーバー負荷軽減のための待機
                if i > 1:  # 2番目以降
                    wait_time = 1.5  # 1.5秒間隔
                    logger.debug(f"⏱️ サーバー保護待機: {wait_time}秒")
                    time.sleep(wait_time)
                
                # ダウンロード実行
                mesh_code, success, message, size = self.download_single_mesh(mesh_code)
                
                if success:
                    success_list.append(mesh_code)
                    total_size += size
                    if message == "downloaded":
                        logger.info(f"✅ ダウンロード成功: {mesh_code} ({size:,}bytes)")
                    else:
                        logger.info(f"⏭️ 既存ファイル確認: {mesh_code} ({size:,}bytes)")
                else:
                    failed_list.append((mesh_code, message))
                    logger.warning(f"❌ ダウンロード失敗: {mesh_code} - {message}")
                    
                    # 失敗時は少し長めに待機
                    if "404" not in message and "not_found" not in message:
                        logger.debug("⏱️ 失敗後追加待機: 3秒")
                        time.sleep(3.0)
                
                # 10件毎に中間レポート
                if i % 10 == 0:
                    logger.info(f"📊 中間進捗: 成功{len(success_list)}, 失敗{len(failed_list)}, 残り{len(missing_meshes)-i}")
                
                # 20件毎に小休止
                if i % 20 == 0:
                    logger.info("😴 サーバー保護のための小休止: 5秒")
                    time.sleep(5.0)
                    
            except KeyboardInterrupt:
                logger.warning("⚠️ ユーザー中断 - 現在までの結果を返します")
                break
            except Exception as e:
                failed_list.append((mesh_code, f"processing_error: {str(e)}"))
                logger.error(f"❌ 処理エラー: {mesh_code} - {e}")
                continue
        
        result = {
            'success': success_list,
            'failed': failed_list,
            'total_size': total_size
        }
        
        logger.info(f"📊 ダウンロード結果:")
        logger.info(f"   成功: {len(success_list)}件")
        logger.info(f"   失敗: {len(failed_list)}件")
        logger.info(f"   成功率: {len(success_list)/(len(success_list)+len(failed_list))*100:.1f}%")
        logger.info(f"   合計サイズ: {total_size:,}bytes ({total_size/1024/1024:.1f}MB)")
        
        # 失敗詳細の分析
        if failed_list:
            failure_types = {}
            for mesh, reason in failed_list:
                failure_type = reason.split(':')[0] if ':' in reason else reason
                failure_types[failure_type] = failure_types.get(failure_type, 0) + 1
            
            logger.warning(f"   失敗分析:")
            for failure_type, count in sorted(failure_types.items(), key=lambda x: x[1], reverse=True):
                logger.warning(f"     {failure_type}: {count}件")
            
            # 失敗メッシュの詳細表示（最初の5件）
            logger.warning(f"   失敗メッシュ例:")
            for mesh, reason in failed_list[:5]:
                logger.warning(f"     {mesh}: {reason}")
            if len(failed_list) > 5:
                logger.warning(f"     ... (他 {len(failed_list)-5}件)")
        
        return result
    
    def create_download_report(self, initial_status: Dict, download_result: Dict):
        """ダウンロードレポート作成"""
        report_file = self.output_dir / "download_report.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# 米子市Plateau建物データ完全取得レポート\n")
            f.write(f"# 実行日時: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# 元データソース: {self.base_url}\n\n")
            
            f.write("## 取得前状況\n")
            f.write(f"利用可能総メッシュ: {initial_status['total_available']}件\n")
            f.write(f"既存ダウンロード: {initial_status['existing_count']}件\n")
            f.write(f"未取得メッシュ: {initial_status['missing_count']}件\n")
            f.write(f"完了率: {initial_status['completion_rate']:.1f}%\n\n")
            
            f.write("## ダウンロード実行結果\n")
            f.write(f"ダウンロード成功: {len(download_result['success'])}件\n")
            f.write(f"ダウンロード失敗: {len(download_result['failed'])}件\n")
            f.write(f"取得データサイズ: {download_result['total_size']:,}bytes ({download_result['total_size']/1024/1024:.1f}MB)\n\n")
            
            # 最終状況
            final_existing = initial_status['existing_count'] + len(download_result['success'])
            final_completion = final_existing / initial_status['total_available'] * 100
            
            f.write("## 最終状況\n")
            f.write(f"総ダウンロード済み: {final_existing}件\n")
            f.write(f"完了率: {final_completion:.1f}%\n")
            
            if final_completion >= 100.0:
                f.write("✅ 完全カバレッジ達成！\n")
            else:
                remaining = initial_status['total_available'] - final_existing
                f.write(f"⚠️ 残り未取得: {remaining}件\n")
            
            f.write("\n## 成功メッシュ一覧\n")
            for mesh in sorted(download_result['success']):
                f.write(f"{mesh}.zip\n")
            
            if download_result['failed']:
                f.write("\n## 失敗メッシュ一覧\n")
                for mesh, reason in download_result['failed']:
                    f.write(f"{mesh}: {reason}\n")
        
        logger.info(f"📋 レポート作成: {report_file}")
    
    def run_complete_download(self):
        """完全ダウンロード実行"""
        logger.info("🚀 米子市Plateau建物データ完全取得開始")
        logger.info("=" * 60)
        logger.info("📊 対象: 153件の2次メッシュ（米子市全域）")
        logger.info("🎯 目標: 全メッシュの完全取得")
        logger.info("=" * 60)
        
        try:
            # Phase 1: 現状分析
            logger.info("\n📊 Phase 1: 現在のダウンロード状況分析")
            initial_status = self.analyze_current_status()
            
            # Phase 2: Web可用性確認
            logger.info("\n🌐 Phase 2: Web可用性確認")
            verified_meshes = self.verify_web_availability()
            
            # 最新のメッシュリストで再分析
            existing_files = list(self.output_dir.glob("*.zip"))
            existing_meshes = set()
            for file in existing_files:
                match = re.match(r'^(\d+)\.zip$', file.name)
                if match and match.group(1) in verified_meshes:
                    existing_meshes.add(match.group(1))
            
            missing_meshes = list(set(verified_meshes) - existing_meshes)
            
            if not missing_meshes:
                logger.info("🎉 既に全メッシュダウンロード済みです！")
                self.create_download_report(initial_status, {'success': [], 'failed': [], 'total_size': 0})
                return True
            
            # Phase 3: 未取得メッシュダウンロード
            logger.info(f"\n📥 Phase 3: 未取得メッシュダウンロード ({len(missing_meshes)}件)")
            download_result = self.download_missing_meshes(missing_meshes)
            
            # Phase 4: レポート作成
            logger.info(f"\n📋 Phase 4: ダウンロードレポート作成")
            self.create_download_report(initial_status, download_result)
            
            # Phase 5: 最終確認
            logger.info(f"\n🔍 Phase 5: 最終確認")
            final_status = self.analyze_current_status()
            
            # 結果判定
            success = final_status['completion_rate'] >= 100.0
            
            logger.info("=" * 60)
            if success:
                logger.info("🎉 米子市Plateau建物データ完全取得成功!")
                logger.info("✅ 153件の全メッシュダウンロード完了")
                logger.info("🚀 次は yonago_complete_importer.py でDBインポートを実行")
            else:
                logger.warning("⚠️ 一部メッシュの取得に失敗しました")
                logger.info(f"📊 達成率: {final_status['completion_rate']:.1f}%")
                logger.info("🔄 失敗分は後で再実行可能です")
            
            logger.info("=" * 60)
            return success
            
        except Exception as e:
            logger.error(f"❌ 完全取得失敗: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='米子市Plateau建物データ完全取得')
    parser.add_argument('--output-dir', default='./yonago_plateau_data',
                       help='出力ディレクトリ (default: ./yonago_plateau_data)')
    parser.add_argument('--verbose', action='store_true',
                       help='詳細ログ出力')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("📥 米子市Plateau建物データ完全ダウンローダー起動")
    
    downloader = YonagoCompleteDownloader(args.output_dir)
    success = downloader.run_complete_download()
    
    if success:
        logger.info("✅ 完全取得成功！次は yonago_complete_importer.py でDBインポートを実行してください")
        print("\n🎉 完全取得成功!")
        print("📁 データ保存場所: ./yonago_plateau_data/")
        print("🚀 次のコマンド: python3.9 yonago_complete_importer.py")
    else:
        logger.error("❌ 完全取得に問題が発生しました")
        print("\n❌ 一部の取得に失敗しました")
        print("📋 詳細: yonago_complete_downloader.log を確認")
        print("🔄 再実行で続きからダウンロード可能")

if __name__ == "__main__":
    main()