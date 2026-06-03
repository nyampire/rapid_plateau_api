#!/usr/bin/env python3
"""
Plateau建物データ PostGISインポーター
ローカルのzipファイルからPostgreSQLに安全にインポート

前提条件:
- Plateau建物データのzipファイルを取得済み
- PostgreSQL/PostGISデータベースが準備済み
"""

import os
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
import logging
from typing import List, Dict, Tuple, Set, Optional
import time
import hashlib
import re
from collections import defaultdict

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('plateau_importer2postgis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PlateauImporter2PostGIS:
    def __init__(self,
                 data_dir="./plateau_data",
                 postgres_url="postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau",
                 coord_bounds=None,
                 citycode=None):
        """
        Args:
            data_dir: zipファイルが格納されたディレクトリ
            postgres_url: PostgreSQL接続URL
            coord_bounds: 座標範囲チェック用 (min_lat, max_lat, min_lon, max_lon)。Noneで無効化
            citycode: 市区町村コード (例: "31202")。Noneの場合はdata_dirのディレクトリ名から推定
        """
        self.data_dir = Path(data_dir)
        self.postgres_url = postgres_url
        self.coord_bounds = coord_bounds

        # 市区町村コードの決定
        if citycode:
            self.citycode = citycode
        else:
            # data_dirのディレクトリ名から推定 (例: ./plateau_data/31202 → "31202")
            dirname = self.data_dir.name
            match = re.match(r'^(\d{5})', dirname)
            self.citycode = match.group(1) if match else "unknown"
        logger.info(f"🏙️ 市区町村コード: {self.citycode}")
        self.extracted_dir = self.data_dir / "extracted"

        # 一時ディレクトリ作成
        self.extracted_dir.mkdir(exist_ok=True)

        # ID管理（DBから既存最大値を取得して継続）
        self.building_id_counter = 1
        self.node_id_counter = -1  # 負の値でノードID管理

        # 重複除去用
        self.processed_geometry_hashes = set()
        self.node_coordinate_map = {}  # 座標 -> ユニークID のマッピング

        self._test_connection()
        self._initialize_id_counters()  # DBから既存IDを取得

    def _initialize_id_counters(self):
        """DBから既存の最大IDを取得してカウンターを初期化"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()

            # 建物の最大IDを取得
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM plateau_buildings")
            max_building_id = cursor.fetchone()[0]
            self.building_id_counter = max_building_id + 1

            # ノードの最小ID（負の値）を取得
            cursor.execute("SELECT COALESCE(MIN(osm_id), 0) FROM plateau_building_nodes")
            min_node_id = cursor.fetchone()[0]
            # 既存の最小値よりさらに小さい値から開始
            self.node_id_counter = min(min_node_id - 1, -1)

            # 既存ノード座標マップは読み込まない（OOM対策）
            # 都市ごとにIDが独立しているため全テーブルのマップは不要
            # node_coordinate_map はインポート中にインクリメンタルに構築される

            conn.close()

            logger.info(f"🔢 ID初期化完了:")
            logger.info(f"   建物IDカウンター: {self.building_id_counter} から開始")
            logger.info(f"   ノードIDカウンター: {self.node_id_counter} から開始")

        except Exception as e:
            logger.warning(f"⚠️ ID初期化でエラー（デフォルト値を使用）: {e}")

        # building:part 対応のスキーマ拡張をべき等に適用
        self._ensure_schema()

    def _ensure_schema(self):
        """plateau_buildings に building:part 対応カラムを idempotent に追加。

        - building_part TEXT: building:part タグの値 (typically 'yes')、それ以外は NULL
        - parent_building_id INTEGER: part の場合の outline 親 building.id (ON DELETE CASCADE)
        """
        try:
            conn = psycopg2.connect(self.postgres_url)
            try:
                cur = conn.cursor()
                cur.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='plateau_buildings'
                      AND column_name IN ('building_part', 'parent_building_id')
                """)
                existing = {row[0] for row in cur.fetchall()}
                added = []
                if 'building_part' not in existing:
                    cur.execute("ALTER TABLE plateau_buildings ADD COLUMN building_part TEXT")
                    added.append('building_part')
                if 'parent_building_id' not in existing:
                    cur.execute(
                        "ALTER TABLE plateau_buildings "
                        "ADD COLUMN parent_building_id INTEGER "
                        "REFERENCES plateau_buildings(id) ON DELETE CASCADE"
                    )
                    cur.execute(
                        "CREATE INDEX IF NOT EXISTS idx_buildings_parent_building_id "
                        "ON plateau_buildings(parent_building_id) "
                        "WHERE parent_building_id IS NOT NULL"
                    )
                    added.append('parent_building_id')
                if added:
                    conn.commit()
                    logger.info(f"🗂️ スキーマ拡張: {', '.join(added)} を追加")
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"⚠️ スキーマ拡張でエラー（既存のままで継続）: {e}")

    def _test_connection(self):
        """PostgreSQL接続テスト"""
        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()

            # 現在のデータ確認
            cursor.execute("SELECT COUNT(*) FROM plateau_buildings")
            building_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM plateau_building_nodes")
            node_count = cursor.fetchone()[0]

            logger.info(f"✅ PostgreSQL接続成功")
            logger.info(f"📊 現在のデータ: 建物{building_count:,}件, ノード{node_count:,}件")

            # ID範囲確認
            if building_count > 0:
                cursor.execute("SELECT MIN(osm_id), MAX(osm_id) FROM plateau_buildings")
                building_range = cursor.fetchone()
                logger.info(f"🏢 建物ID範囲: {building_range[0]} ~ {building_range[1]}")

            if node_count > 0:
                cursor.execute("SELECT MIN(osm_id), MAX(osm_id) FROM plateau_building_nodes")
                node_range = cursor.fetchone()
                logger.info(f"📍 ノードID範囲: {node_range[0]} ~ {node_range[1]}")

            conn.close()

        except Exception as e:
            logger.error(f"❌ PostgreSQL接続失敗: {e}")
            raise

    def analyze_existing_data(self) -> Dict:
        """既存データの詳細分析"""
        logger.info("🔍 既存データを詳細分析中...")

        try:
            conn = psycopg2.connect(self.postgres_url)
            cursor = conn.cursor()

            # 基本統計
            cursor.execute("SELECT COUNT(*) FROM plateau_buildings WHERE ST_IsValid(geom)")
            valid_buildings = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM plateau_buildings WHERE NOT ST_IsValid(geom)")
            invalid_buildings = cursor.fetchone()[0]

            # データセット別統計
            cursor.execute("""
                SELECT
                    source_dataset,
                    COUNT(*) as count
                FROM plateau_buildings
                GROUP BY source_dataset
                ORDER BY count DESC
            """)
            dataset_stats = cursor.fetchall()

            # ID利用状況
            cursor.execute("SELECT MIN(osm_id), MAX(osm_id) FROM plateau_buildings WHERE osm_id > 0")
            building_id_range = cursor.fetchone()

            cursor.execute("SELECT MIN(osm_id), MAX(osm_id) FROM plateau_building_nodes WHERE osm_id < 0")
            node_id_range = cursor.fetchone()

            conn.close()

            analysis = {
                'valid_buildings': valid_buildings,
                'invalid_buildings': invalid_buildings,
                'total_buildings': valid_buildings + invalid_buildings,
                'validity_rate': valid_buildings / (valid_buildings + invalid_buildings) * 100 if (valid_buildings + invalid_buildings) > 0 else 0,
                'dataset_stats': dataset_stats,
                'building_id_range': building_id_range,
                'node_id_range': node_id_range
            }

            logger.info(f"✅ 既存データ分析結果:")
            logger.info(f"   有効建物: {analysis['valid_buildings']:,}件")
            logger.info(f"   無効建物: {analysis['invalid_buildings']:,}件")
            logger.info(f"   有効性率: {analysis['validity_rate']:.1f}%")
            logger.info(f"   建物ID範囲: {analysis['building_id_range']}")
            logger.info(f"   ノードID範囲: {analysis['node_id_range']}")

            if dataset_stats:
                logger.info(f"   データセット別:")
                for dataset, count in dataset_stats[:5]:
                    logger.info(f"     {dataset}: {count:,}件")

            # 次のID設定
            if building_id_range and building_id_range[1]:
                self.building_id_counter = building_id_range[1] + 1
            if node_id_range and node_id_range[0]:
                self.node_id_counter = node_id_range[0] - 1

            logger.info(f"🆔 次回使用ID: 建物={self.building_id_counter}, ノード={self.node_id_counter}")

            return analysis

        except Exception as e:
            logger.error(f"❌ 既存データ分析エラー: {e}")
            return {}

    def find_zip_files(self) -> List[Path]:
        """zipファイル検索と分析"""
        logger.info(f"📁 zipファイル検索: {self.data_dir}")

        zip_files = list(self.data_dir.glob("*.zip"))
        zip_files.sort()

        total_size = 0
        mesh_codes = []

        for zip_file in zip_files:
            file_size = zip_file.stat().st_size
            total_size += file_size

            # メッシュコード抽出
            match = re.match(r'^(\d+)', zip_file.name)
            if match:
                mesh_codes.append(match.group(1))

        logger.info(f"📦 発見したzipファイル: {len(zip_files)}件")
        logger.info(f"📊 合計サイズ: {total_size:,}bytes ({total_size/1024/1024:.1f}MB)")
        logger.info(f"🗂️ メッシュコード: {len(mesh_codes)}件")

        if mesh_codes:
            logger.info(f"   メッシュ例: {', '.join(sorted(mesh_codes)[:10])}")
            if len(mesh_codes) > 10:
                logger.info(f"   ... (他 {len(mesh_codes)-10}件)")

        return zip_files

    def extract_zip_files(self, zip_files: List[Path]) -> List[Path]:
        """zipファイル展開（重複回避）"""
        logger.info(f"📂 {len(zip_files)}件のzipファイルを展開中...")

        osm_files = []
        processed_count = 0

        for i, zip_path in enumerate(zip_files, 1):
            try:
                # 展開先ディレクトリ
                extract_subdir = self.extracted_dir / zip_path.stem
                extract_subdir.mkdir(exist_ok=True)

                # 既に展開済みかチェック
                existing_osm = list(extract_subdir.glob("*.osm"))
                if existing_osm:
                    logger.info(f"⏭️ [{i:3d}/{len(zip_files)}] スキップ（既存）: {zip_path.name}")
                    osm_files.extend(existing_osm)
                    continue

                logger.info(f"📂 [{i:3d}/{len(zip_files)}] 展開中: {zip_path.name}")

                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # ファイル一覧確認
                    file_list = zip_ref.namelist()
                    osm_count = len([f for f in file_list if f.endswith('.osm')])

                    if osm_count == 0:
                        logger.warning(f"     ⚠️ OSMファイルなし")
                        continue

                    # 展開実行
                    zip_ref.extractall(extract_subdir)
                    processed_count += 1

                # OSMファイルを収集
                for osm_path in extract_subdir.rglob("*.osm"):
                    osm_files.append(osm_path)
                    logger.info(f"     📄 OSM発見: {osm_path.name}")

            except zipfile.BadZipFile:
                logger.warning(f"❌ 不正なzipファイル: {zip_path.name}")
                continue
            except Exception as e:
                logger.warning(f"❌ 展開失敗: {zip_path.name} - {e}")
                continue

        logger.info(f"✅ 展開完了: {processed_count}件処理, {len(osm_files)}個のOSMファイル")
        return osm_files

    def parse_osm_file_safe(self, osm_file: Path) -> Tuple[Dict, List]:
        """安全なOSMファイル解析（修復済み技術）

        building:part 対応:
        - <relation type=building> をパースし、role=outline/role=part の way を識別
        - building タグを持つ way は単純な building、または relation の outline
        - building:part タグを持つ way は part (relation 経由でも単独でも対応)
        - 各 building/part に対し building_part フラグと parent_outline_way_id を付与
        """
        try:
            tree = ET.parse(osm_file)
            root = tree.getroot()
        except ET.ParseError as e:
            logger.warning(f"❌ XMLパースエラー {osm_file.name}: {e}")
            return {}, []

        file_prefix = osm_file.stem
        nodes = {}
        buildings = []

        # relation 解析: type=building の relation から
        # part_way_id → parent_outline_way_id のマップを構築
        part_to_outline = {}
        for rel_elem in root.findall('relation'):
            rel_tags = {}
            for tag_elem in rel_elem.findall('tag'):
                k = tag_elem.get('k')
                v = tag_elem.get('v')
                if k and v:
                    rel_tags[k] = v
            if rel_tags.get('type') != 'building':
                continue
            outline_way_id = None
            part_way_ids = []
            for m in rel_elem.findall('member'):
                if m.get('type') != 'way':
                    continue
                role = m.get('role')
                ref = m.get('ref')
                if role == 'outline':
                    outline_way_id = ref
                elif role == 'part':
                    part_way_ids.append(ref)
            if outline_way_id:
                for pwid in part_way_ids:
                    part_to_outline[pwid] = outline_way_id

        # ノード収集（座標検証付き）
        for node_elem in root.findall('node'):
            original_id = node_elem.get('id')
            try:
                lat = float(node_elem.get('lat'))
                lon = float(node_elem.get('lon'))

                # 座標範囲チェック（指定がなければ日本全域）
                if self.coord_bounds:
                    min_lat, max_lat, min_lon, max_lon = self.coord_bounds
                    in_bounds = min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
                else:
                    in_bounds = 20.0 <= lat <= 46.0 and 122.0 <= lon <= 154.0
                if in_bounds:
                    # 座標ベースのユニークID生成（修復済み技術）
                    coord_key = f"{lat:.7f},{lon:.7f}"

                    if coord_key in self.node_coordinate_map:
                        # 既存座標の場合は既存IDを使用
                        unique_id = self.node_coordinate_map[coord_key]
                    else:
                        # 新座標の場合は新IDを割り当て
                        unique_id = self.node_id_counter
                        self.node_coordinate_map[coord_key] = unique_id
                        self.node_id_counter -= 1

                    nodes[original_id] = {
                        'unique_id': unique_id,
                        'lat': lat,
                        'lon': lon,
                        'coord_key': coord_key
                    }
            except (ValueError, TypeError):
                continue

        # 建物ウェイ収集 (building または building:part を持つ way が対象)
        for way_elem in root.findall('way'):
            tags = {}
            for tag_elem in way_elem.findall('tag'):
                key = tag_elem.get('k')
                value = tag_elem.get('v')
                if key and value:
                    tags[key] = value

            # 建物判定: building または building:part のいずれかがあれば対象
            is_building = bool(tags.get('building'))
            is_part = bool(tags.get('building:part'))
            if not (is_building or is_part):
                continue

            way_id = way_elem.get('id')
            nd_refs = []

            for nd_elem in way_elem.findall('nd'):
                nd_ref = nd_elem.get('ref')
                if nd_ref in nodes:
                    nd_refs.append(nd_ref)

            # 最低3点でポリゴン形成
            if len(nd_refs) >= 3:
                # part の場合は parent_outline_way_id を解決
                parent_outline_way_id = part_to_outline.get(way_id) if is_part else None
                buildings.append({
                    'way_id': way_id,
                    'tags': tags,
                    'node_refs': nd_refs,
                    'source_file': osm_file.name,
                    'file_prefix': file_prefix,
                    'is_part': is_part and not is_building,  # building:part のみで building タグ無し
                    'parent_outline_way_id': parent_outline_way_id,
                })

        return nodes, buildings

    def convert_building_tags_enhanced(self, tags: Dict, source_info: str) -> Dict:
        """建物タグ変換（品質向上版）"""
        result = {
            'building': 'yes',
            'height': None,
            'ele': None,
            'building_levels': None,
            'name': None,
            'addr_housenumber': None,
            'addr_street': None,
            'building_material': None,
            'roof_material': None,
            'roof_shape': None,
            'start_date': None,
            'amenity': None,
            'shop': None,
            'tourism': None,
            'leisure': None,
            'landuse': None,
            'source_dataset': f"plateau_{self.citycode}_{source_info}",
            'city_code': self.citycode if self.citycode and self.citycode != "unknown" else None,
        }

        # 基本建物タイプ
        building_type = tags.get('building', 'yes')
        if building_type and building_type != 'no':
            result['building'] = building_type

        # 高さ情報（厳格検証）
        height_raw = tags.get('height')
        if height_raw:
            try:
                height_val = float(height_raw)
                if 0.5 <= height_val <= 300:  # 現実的な建物高さ
                    result['height'] = height_val
            except (ValueError, TypeError):
                pass

        # 階数
        levels_raw = tags.get('building:levels')
        if levels_raw:
            try:
                levels = int(float(levels_raw))
                if 1 <= levels <= 50:
                    result['building_levels'] = levels
            except (ValueError, TypeError):
                pass

        # 建物名称
        name = tags.get('name') or tags.get('name:ja')
        if name:
            result['name'] = name[:100]

        # 住所情報
        addr_housenumber = tags.get('addr:housenumber')
        if addr_housenumber:
            result['addr_housenumber'] = addr_housenumber[:20]

        addr_street = tags.get('addr:street')
        if addr_street:
            result['addr_street'] = addr_street[:100]

        # 建材・屋根情報
        building_material = tags.get('building:material')
        if building_material:
            result['building_material'] = building_material[:50]

        roof_material = tags.get('roof:material')
        if roof_material:
            result['roof_material'] = roof_material[:50]

        roof_shape = tags.get('roof:shape')
        if roof_shape:
            result['roof_shape'] = roof_shape[:50]

        # 標高情報
        ele_raw = tags.get('ele')
        if ele_raw:
            try:
                ele_val = float(ele_raw)
                if -100 <= ele_val <= 9000:  # 現実的な標高範囲
                    result['ele'] = ele_val
            except (ValueError, TypeError):
                pass

        # 建設年
        start_date = tags.get('start_date')
        if start_date:
            result['start_date'] = start_date[:10]

        # 用途・施設情報
        for key in ['amenity', 'shop', 'tourism', 'leisure', 'landuse']:
            value = tags.get(key)
            if value:
                result[key] = value[:50]

        return result

    def create_geometry_hash(self, coords: List[Tuple[float, float]]) -> str:
        """ジオメトリハッシュ生成（重複除去用）"""
        # 座標を正規化してハッシュ化
        normalized_coords = []
        for lon, lat in coords:
            normalized_coords.append((round(lon, 7), round(lat, 7)))

        # ソートして向き統一
        normalized_coords.sort()
        coord_str = '|'.join([f"{lon},{lat}" for lon, lat in normalized_coords])

        return hashlib.md5(coord_str.encode()).hexdigest()

    def process_buildings_safe(self, all_nodes: Dict, all_buildings: List) -> Tuple[List, List, List]:
        """建物処理（安全版・重複除去付き）

        Returns:
            (buildings_data, nodes_data, parts_parent_map)
            parts_parent_map: List[Tuple[part_osm_id, parent_outline_osm_id]]
        """
        logger.info(f"🏗️ {len(all_buildings):,}建物を安全処理中...")

        # outline / simple を先に処理して、part の parent_osm_id 解決を容易にする
        all_buildings = sorted(all_buildings, key=lambda b: 1 if b.get('is_part') else 0)

        buildings_data = []
        nodes_data = []
        parts_parent_map = []  # [(part_osm_id, parent_outline_osm_id), ...]
        way_id_to_osm_id = {}  # source way_id → assigned building_id_counter
        processed_count = 0
        skipped_count = 0
        duplicate_count = 0
        skip_reasons = {
            "too_few_coords": 0,    # 座標3点未満（ポリゴン形成不可）
            "too_few_points": 0,    # 閉鎖後4点未満
            "tiny_area": 0,         # 極小面積
            "error": 0,             # 例外発生
        }
        skipped_buildings = []  # スキップした建物の詳細記録

        for i, building in enumerate(all_buildings, 1):
            try:
                # 進捗表示
                if i % 1000 == 0:
                    progress = (i / len(all_buildings)) * 100
                    logger.info(f"🔄 処理中: {i:,}/{len(all_buildings):,} ({progress:.1f}%) - 成功:{processed_count}, 重複:{duplicate_count}, スキップ:{skipped_count}")

                tags = building['tags']
                node_refs = building['node_refs']
                source_file = building['source_file']

                # 座標収集・ユニークID使用
                coords = []
                building_nodes = []

                for seq, original_node_ref in enumerate(node_refs):
                    if original_node_ref in all_nodes:
                        node_data = all_nodes[original_node_ref]
                        unique_node_id = node_data['unique_id']
                        lat = node_data['lat']
                        lon = node_data['lon']

                        coords.append((lon, lat))

                        # ノードデータ（ユニークID使用）
                        building_nodes.append((
                            unique_node_id,        # id（負の値）
                            self.building_id_counter,  # building_id
                            seq,                   # sequence_id
                            lat,                   # lat
                            lon,                   # lon
                            lon,                   # ST_Point用 lon
                            lat                    # ST_Point用 lat
                        ))

                # ポリゴン形成チェック
                if len(coords) >= 3:
                    # ポリゴン閉鎖
                    if coords[0] != coords[-1]:
                        coords.append(coords[0])

                    # 重複チェック
                    geom_hash = self.create_geometry_hash(coords[:-1])  # 閉鎖点除外でハッシュ

                    if geom_hash in self.processed_geometry_hashes:
                        duplicate_count += 1
                        continue

                    self.processed_geometry_hashes.add(geom_hash)

                    # 面積チェック（極小ポリゴン除外）
                    if len(coords) >= 4:
                        # 簡易面積計算
                        area_check = True
                        if len(coords) == 4:  # 三角形
                            x1, y1 = coords[0]
                            x2, y2 = coords[1]
                            x3, y3 = coords[2]
                            area = abs((x1*(y2-y3) + x2*(y3-y1) + x3*(y1-y2))/2)
                            if area < 0.000001:  # 極小面積
                                area_check = False

                        if area_check:
                            # タグ変換
                            converted_tags = self.convert_building_tags_enhanced(tags, source_file)

                            # WKT作成
                            coords_str = ','.join([f"{lon} {lat}" for lon, lat in coords])
                            polygon_wkt = f"POLYGON(({coords_str}))"

                            # 住所を結合
                            addr_parts = []
                            if converted_tags.get('addr_street'):
                                addr_parts.append(converted_tags['addr_street'])
                            if converted_tags.get('addr_housenumber'):
                                addr_parts.append(converted_tags['addr_housenumber'])
                            addr_full = ' '.join(addr_parts) if addr_parts else None

                            # building:part 判定 (parse_osm_file_safe 由来)
                            is_part = bool(building.get('is_part'))
                            building_part_value = 'yes' if is_part else None
                            # building タグ: part の場合は building タグ無しなので None
                            building_value = (
                                converted_tags.get('building', 'yes')
                                if not is_part
                                else tags.get('building')  # 通常 None
                            )

                            # 建物データ（plateau_buildingsテーブル構造に合わせる）
                            buildings_data.append((
                                self.building_id_counter,           # osm_id
                                building_value,                     # building (part の場合 None)
                                converted_tags.get('height'),       # height
                                converted_tags.get('ele'),          # ele
                                converted_tags.get('building_levels'),  # building_levels
                                None,                               # building_levels_underground
                                converted_tags.get('source_dataset'),   # source_dataset
                                building['way_id'],                 # plateau_id
                                polygon_wkt,                        # geometry_wkt
                                converted_tags.get('name'),         # name
                                addr_full,                          # addr_full
                                converted_tags.get('addr_housenumber'), # addr_housenumber
                                converted_tags.get('addr_street'),  # addr_street
                                converted_tags.get('start_date'),   # start_date
                                converted_tags.get('building_material'), # building_material
                                converted_tags.get('roof_material'),    # roof_material
                                converted_tags.get('roof_shape'),       # roof_shape
                                converted_tags.get('amenity'),      # amenity
                                converted_tags.get('shop'),         # shop
                                converted_tags.get('tourism'),      # tourism
                                converted_tags.get('leisure'),      # leisure
                                converted_tags.get('landuse'),      # landuse
                                converted_tags.get('city_code'),    # city_code
                                building_part_value,                # building_part
                                polygon_wkt,                        # geom用WKT
                                polygon_wkt                         # centroid用WKT
                            ))

                            # way_id → osm_id を記録 (part の parent 解決に使う)
                            way_id_to_osm_id[building['way_id']] = self.building_id_counter

                            # part の場合は parent_outline_way_id 経由で parent_osm_id を解決
                            if is_part and building.get('parent_outline_way_id'):
                                parent_way_id = building['parent_outline_way_id']
                                parent_osm_id = way_id_to_osm_id.get(parent_way_id)
                                if parent_osm_id is not None:
                                    parts_parent_map.append(
                                        (self.building_id_counter, parent_osm_id)
                                    )

                            nodes_data.extend(building_nodes)
                            self.building_id_counter += 1
                            processed_count += 1
                        else:
                            skipped_count += 1
                            skip_reasons["tiny_area"] += 1
                            skipped_buildings.append({
                                "reason": "tiny_area",
                                "way_id": building.get('way_id'),
                                "source_file": source_file,
                                "num_coords": len(coords),
                                "area": area,
                                "coords": coords[:5],
                                "tags": {k: v for k, v in tags.items() if k in ('building', 'height', 'name', 'addr:full')},
                                "probable_cause": "CityGML",
                                "probable_cause_detail": "元CityGMLのgml:posList座標値の問題。"
                                    "座標が重複・近接しているか、EPSG変換時の精度損失により"
                                    "面積がほぼゼロになっている。OSM変換ツールは座標をそのまま"
                                    "変換するため、.osmファイル側の問題ではない。",
                                "diagnosis": "ポリゴンの面積が極小 (< 0.000001度^2, 約0.01m^2)。"
                                    "頂点座標が同一地点に集中しているか、CityGMLのLOD0フットプリントが正しく生成されていない可能性。",
                                "citygml_check": "元CityGMLのbldg:lod0FootPrint (またはbldg:lod0RoofEdge) 内の"
                                    "gml:posList座標値に重複や極端な近接がないか確認。"
                                    "EPSG変換時の精度損失も要確認。",
                                "qgis_check": "QGISで当該座標付近を表示し、ジオメトリの妥当性チェック"
                                    " (ベクタ > ジオメトリツール > 妥当性チェック) を実行。",
                            })
                    else:
                        skipped_count += 1
                        skip_reasons["too_few_points"] += 1
                        skipped_buildings.append({
                            "reason": "too_few_points",
                            "way_id": building.get('way_id'),
                            "source_file": source_file,
                            "num_coords": len(coords),
                            "coords": coords,
                            "tags": {k: v for k, v in tags.items() if k in ('building', 'height', 'name', 'addr:full')},
                            "probable_cause": "CityGML or OSM変換",
                            "probable_cause_detail": "元CityGMLの座標数不足、またはCityGML→OSM変換時の"
                                "頂点欠落のいずれか。CityGMLのgml:posListに十分な座標ペアがあるなら"
                                ".osm変換ツール側の問題、なければCityGML側の問題。",
                            "diagnosis": f"ポリゴン閉鎖後の頂点数が{len(coords)}点。"
                                "有効なポリゴンには最低4点 (3頂点+閉鎖点) が必要。",
                            "citygml_check": "元CityGMLで当該gml:idのbldg:lod0FootPrint内gml:posListの"
                                "座標ペア数が3組以上あるか確認。",
                            "qgis_check": "元CityGMLをQGISで読み込み、当該建物のジオメトリが"
                                "正常にレンダリングされるか確認。表示されない場合はCityGML側のデータ不備。",
                        })
                else:
                    skipped_count += 1
                    skip_reasons["too_few_coords"] += 1
                    skipped_buildings.append({
                        "reason": "too_few_coords",
                        "way_id": building.get('way_id'),
                        "source_file": source_file,
                        "num_coords": len(coords),
                        "num_node_refs": len(node_refs),
                        "node_refs_sample": node_refs[:10],
                        "tags": {k: v for k, v in tags.items() if k in ('building', 'height', 'name', 'addr:full')},
                        "probable_cause": "OSM変換",
                        "probable_cause_detail": "OSM変換ツール側の問題の可能性が高い。"
                            "way要素がnd refでノードを参照しているが、対応するnode要素が"
                            ".osmファイル内に出力されていない。CityGML側には座標データが"
                            "存在するはずなので、変換時のノード出力漏れが原因。"
                            "ファイル分割境界でのノード欠落が典型的なパターン。",
                        "diagnosis": f"way要素はnd refを{len(node_refs)}件参照していますが、"
                            f"座標解決できたノードは{len(coords)}点のみ。"
                            "OSMファイル内のnode定義が欠落している。",
                        "citygml_check": "元CityGMLで当該gml:idの建物に座標データが存在するか確認。"
                            "存在する場合は変換ツール側の不具合。",
                        "qgis_check": "OSMファイルをQGISで読み込み (QuickOSMプラグイン等)、"
                            "当該way_idがポリゴンとして表示されるか確認。"
                            "表示されない場合はノード参照の不整合が原因。",
                    })

            except Exception as e:
                logger.warning(f"⚠️ 建物処理エラー {i}: {e}")
                skipped_count += 1
                skip_reasons["error"] += 1
                skipped_buildings.append({
                    "reason": "error",
                    "way_id": building.get('way_id', 'unknown'),
                    "source_file": building.get('source_file', 'unknown'),
                    "error_message": str(e),
                    "probable_cause": "OSM変換 or インポーター",
                    "probable_cause_detail": "タグ値や座標値に不正なデータが含まれている場合は"
                        ".osmファイル (変換ツール) 側の問題。パース処理自体のバグであれば"
                        "本インポーターの問題。error_messageの内容で切り分け可能。",
                    "diagnosis": "建物データの解析中に予期しないエラーが発生。"
                        "タグ値に不正な文字列が含まれているか、座標値が数値として解析できない可能性。",
                    "citygml_check": "エラーメッセージを元に、当該建物のタグや座標値に"
                        "不正な値 (NaN, 空文字, 不正なUTF-8等) が含まれていないか確認。",
                    "qgis_check": "元CityGMLをQGISで読み込み、当該gml:idの建物が"
                        "正常にレンダリングされるか確認。属性テーブルで不正値の有無も確認。",
                })
                continue

        logger.info(f"📊 建物処理結果:")
        logger.info(f"   成功: {processed_count:,}件")
        logger.info(f"   うち building:part: {len(parts_parent_map):,}件 (parent 解決済)")
        logger.info(f"   重複除去: {duplicate_count:,}件")
        logger.info(f"   スキップ: {skipped_count:,}件")
        if skipped_count > 0:
            for reason, count in skip_reasons.items():
                if count > 0:
                    reason_labels = {
                        "too_few_coords": "座標3点未満（ポリゴン形成不可）",
                        "too_few_points": "閉鎖後4点未満",
                        "tiny_area": "極小面積",
                        "error": "処理エラー",
                    }
                    logger.info(f"     - {reason_labels.get(reason, reason)}: {count:,}件")
        logger.info(f"   総ノード: {len(nodes_data):,}件")

        # スキップした建物の詳細をJSONファイルに出力
        if skipped_buildings:
            skip_report_file = f"skipped_buildings_{self.citycode}.json"
            try:
                import json
                report_data = {
                    "citycode": self.citycode,
                    "total_buildings": len(all_buildings),
                    "processed": processed_count,
                    "skipped": skipped_count,
                    "duplicates": duplicate_count,
                    "skip_summary": {k: v for k, v in skip_reasons.items() if v > 0},
                    "skipped_buildings": skipped_buildings,
                }
                with open(skip_report_file, 'w', encoding='utf-8') as f:
                    json.dump(report_data, f, ensure_ascii=False, indent=2)
                logger.info(f"📋 スキップ詳細レポート: {skip_report_file}")
            except Exception as e:
                logger.warning(f"⚠️ スキップレポート保存失敗: {e}")

        return buildings_data, nodes_data, parts_parent_map

    @staticmethod
    def _dedupe_and_remap_nodes(nodes_data: List, osm_id_to_db_id: Dict) -> Tuple[List, int, int]:
        """投入用ノード行に対し、building内クロージャ重複の除去とbuilding_idの差し替えを行う。

        重複排除キーは ``(osm_building_id, osm_id)`` のペア。
        - 同一buildingで refs[0] == refs[-1] の閉路重複は1件にまとめる
        - 異なるbuildingが共有するコーナーノードは双方に保持する
          （``plateau_building_nodes`` には osm_id への UNIQUE 制約がなく、
           同じ osm_id が複数の building_id に紐付くのが正しい設計）

        Returns:
            (mapped_nodes_data, skipped_count, orphan_count)
        """
        mapped = []
        seen = set()
        skipped = 0
        orphan = 0
        for node_data in nodes_data:
            node_osm_id = node_data[0]
            osm_building_id = node_data[1]
            if osm_building_id not in osm_id_to_db_id:
                orphan += 1
                continue
            key = (osm_building_id, node_osm_id)
            if key in seen:
                skipped += 1
                continue
            seen.add(key)
            db_building_id = osm_id_to_db_id[osm_building_id]
            mapped.append((node_data[0], db_building_id, node_data[2],
                           node_data[3], node_data[4], node_data[5], node_data[6]))
        return mapped, skipped, orphan

    def insert_to_database_batch(self, buildings_data: List, nodes_data: List,
                                  parts_parent_map: Optional[List[Tuple[int, int]]] = None) -> bool:
        """バッチ単位のDB投入（事前削除なし・run_complete_importのバッチ処理用）

        parts_parent_map: building:part の (part_osm_id, parent_outline_osm_id) リスト。
            INSERT 完了後、osm_id → db_id を解決して parent_building_id を UPDATE する。
        """
        logger.info(f"💾 バッチDB投入中...")
        logger.info(f"   建物: {len(buildings_data):,}件")
        logger.info(f"   ノード: {len(nodes_data):,}件")
        if parts_parent_map:
            logger.info(f"   building:part: {len(parts_parent_map):,}件")

        conn = psycopg2.connect(self.postgres_url)

        try:
            cursor = conn.cursor()

            if buildings_data:
                logger.info("🏢 建物データ投入中...")
                execute_values(
                    cursor,
                    """
                    INSERT INTO plateau_buildings
                    (osm_id, building, height, ele, building_levels, building_levels_underground,
                     source_dataset, plateau_id, geometry_wkt,
                     name, addr_full, addr_housenumber, addr_street,
                     start_date, building_material, roof_material, roof_shape,
                     amenity, shop, tourism, leisure, landuse,
                     city_code,
                     building_part,
                     geom, centroid)
                    VALUES %s
                    """,
                    buildings_data,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), ST_Centroid(ST_GeomFromText(%s, 4326)))",
                    page_size=1000
                )
                logger.info("✅ 建物投入完了")

            if nodes_data:
                logger.info("📍 ノードデータ投入中...")

                # 今回のバッチで投入した建物の osm_id → DB id マッピング
                # バッチ内の建物のosm_idリストを作成
                batch_osm_ids = [b[0] for b in buildings_data]  # buildings_data[0] = osm_id
                # IN句で一括取得（バッチ単位なので件数は限定的）
                cursor.execute(
                    "SELECT osm_id, id FROM plateau_buildings WHERE osm_id = ANY(%s)",
                    (batch_osm_ids,)
                )
                osm_id_to_db_id = dict(cursor.fetchall())
                logger.info(f"   建物IDマッピング: {len(osm_id_to_db_id):,}件")

                mapped_nodes_data, skipped_count, orphan_count = self._dedupe_and_remap_nodes(
                    nodes_data, osm_id_to_db_id
                )

                if orphan_count > 0:
                    logger.warning(f"   ⚠️ 建物なしノード除外: {orphan_count:,}件")
                logger.info(f"   投入ノード: {len(mapped_nodes_data):,}件")
                if skipped_count > 0:
                    logger.info(f"   重複スキップ: {skipped_count:,}件")

                if mapped_nodes_data:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO plateau_building_nodes (osm_id, building_id, sequence_id, lat, lon, geom)
                        VALUES %s
                        """,
                        mapped_nodes_data,
                        template="(%s, %s, %s, %s, %s, ST_Point(%s, %s))",
                        page_size=5000
                    )
                logger.info("✅ ノード投入完了")

            # building:part の parent_building_id 解決
            if parts_parent_map:
                self._resolve_part_parents(cursor, parts_parent_map)

            # 行政界 N03 フィルタ (Rapid#35 part C)
            self._apply_city_boundary_filter(cursor)

            conn.commit()
            logger.info(f"✅ バッチコミット完了")
            return True

        except Exception as e:
            logger.error(f"❌ バッチDB投入失敗: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _build_boundary_filter_select_sql() -> str:
        """source city の N03 行政界の外にある建物 id を選ぶ SQL。

        Part A (osmfj_plateau_api 側) と同じ semantics を共有する:
        - dash_city_master.boundary_geom IS NULL の都市 (特殊データセット
          13999 / 27999 など) は無視 → 結果が空 → pass-through。
        - master に行が無い city_code も pass-through (JOIN で落ちる)。
        - boundary が登録されていて centroid が外側の行のみ返す。

        引数は city_code 1 個 (%s)。
        """
        return (
            "SELECT b.id "
            "FROM plateau_buildings b "
            "JOIN dash_city_master m ON m.city_code = b.city_code "
            "WHERE b.city_code = %s "
            "  AND m.boundary_geom IS NOT NULL "
            "  AND NOT ST_Contains(m.boundary_geom, b.centroid)"
        )

    def _apply_city_boundary_filter(self, cursor) -> Tuple[int, int]:
        """source city の N03 行政界の外にある建物・ノードを削除する。

        PLATEAU は都市別配布だが標準地域メッシュは複数 city にまたがる。
        共有メッシュ内の建物は両方の都市の bundle で別レコードとして取り込まれ、
        結果として ~13% の cross-city duplicate を生んでいた (Rapid#35)。
        本フィルタはその根本対策として、本来 source city の行政界に属さない
        建物 (= 重複側) を import の最終段で削除する。

        plateau_building_nodes は ON DELETE CASCADE を持たないので、
        plateau_purge.py と同じ「ノード → 建物」の順で 2 段階削除する。

        Returns:
            (buildings_deleted, nodes_deleted)
        """
        if not self.citycode or self.citycode == "unknown":
            return 0, 0
        try:
            cursor.execute(
                self._build_boundary_filter_select_sql(), (self.citycode,)
            )
            outside_ids = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            # dash_city_master が存在しない / 接続喪失などの場合は素通り。
            # 行政界フィルタは恒久対策の補助層であり、欠落しても import 全体を
            # 失敗させない方が安全 (Part A の API 側フィルタが残る)。
            logger.warning(f"⚠️ 行政界フィルタの SELECT 失敗: {e}（pass-through）")
            return 0, 0

        if not outside_ids:
            logger.info(
                "🌐 行政界 N03 フィルタ: 削除対象なし"
                " (境界未登録 or 全件境界内)"
            )
            return 0, 0

        # SAVEPOINT で囲んで DELETE が FK 違反などで失敗しても import 本体は通す。
        # 行政界フィルタは恒久対策の補助層なので、ここで例外を出すより重複を残して
        # Part A の API filter で隠す方を選ぶ。
        cursor.execute("SAVEPOINT boundary_filter")
        try:
            cursor.execute(
                "DELETE FROM plateau_building_nodes WHERE building_id = ANY(%s)",
                (outside_ids,),
            )
            nodes_deleted = cursor.rowcount or 0
            cursor.execute(
                "DELETE FROM plateau_buildings WHERE id = ANY(%s)",
                (outside_ids,),
            )
            buildings_deleted = cursor.rowcount or 0
            cursor.execute("RELEASE SAVEPOINT boundary_filter")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT boundary_filter")
            logger.warning(
                f"⚠️ 行政界フィルタの DELETE で例外、フィルタを skip "
                f"(outside_ids={len(outside_ids):,}件): {e}"
            )
            return 0, 0

        logger.info(
            f"🌐 行政界 N03 フィルタ: {buildings_deleted:,} 建物 / "
            f"{nodes_deleted:,} ノードを境界外として削除"
        )
        return buildings_deleted, nodes_deleted

    @staticmethod
    def _build_part_parent_updates(parts_parent_map: List[Tuple[int, int]],
                                   osm_to_db: Dict[int, int]) -> Tuple[List[Tuple[int, int]], int]:
        """parts_parent_map と osm_id→db_id マッピングから UPDATE 用ペアを構築する pure 関数。

        Args:
            parts_parent_map: List[(part_osm_id, parent_outline_osm_id)]
            osm_to_db: {osm_id: db_id} の辞書

        Returns:
            (updates, unresolved_count)
            updates: List[(child_db_id, parent_db_id)] - UPDATE 対象
            unresolved_count: child または parent が未解決でスキップされた数
        """
        updates = []
        unresolved = 0
        for part_osm, parent_osm in parts_parent_map:
            child_db = osm_to_db.get(part_osm)
            parent_db = osm_to_db.get(parent_osm)
            if child_db is None or parent_db is None:
                unresolved += 1
                continue
            updates.append((child_db, parent_db))
        return updates, unresolved

    @staticmethod
    def _resolve_part_parents(cursor, parts_parent_map: List[Tuple[int, int]]) -> int:
        """building:part の parent_building_id を解決する。

        Args:
            cursor: psycopg2 cursor (オープン中のトランザクション)
            parts_parent_map: List[(part_osm_id, parent_outline_osm_id)]

        Returns:
            UPDATE 成功した part 行数
        """
        if not parts_parent_map:
            return 0
        # 必要な osm_id 一式 (part と parent 両方)
        all_osm_ids = list({osm_id for pair in parts_parent_map for osm_id in pair})
        cursor.execute(
            "SELECT osm_id, id FROM plateau_buildings WHERE osm_id = ANY(%s)",
            (all_osm_ids,)
        )
        osm_to_db = dict(cursor.fetchall())

        updates, unresolved = PlateauImporter2PostGIS._build_part_parent_updates(
            parts_parent_map, osm_to_db
        )

        if updates:
            execute_values(
                cursor,
                """
                UPDATE plateau_buildings AS pb
                SET parent_building_id = data.parent_id
                FROM (VALUES %s) AS data(child_id, parent_id)
                WHERE pb.id = data.child_id
                """,
                updates,
                template="(%s, %s)",
                page_size=5000
            )

        logger.info(f"🔗 building:part parent 解決: {len(updates):,}件 (未解決 {unresolved})")
        return len(updates)

    def insert_to_database_safe(self, buildings_data: List, nodes_data: List,
                                 parts_parent_map: Optional[List[Tuple[int, int]]] = None) -> bool:
        """データベース安全投入（トランザクション管理・重複回避）"""
        logger.info(f"💾 データベースに安全投入中...")
        logger.info(f"   建物: {len(buildings_data):,}件")
        logger.info(f"   ノード: {len(nodes_data):,}件")

        conn = psycopg2.connect(self.postgres_url)

        try:
            cursor = conn.cursor()

            # 建物投入
            # 不完全インポートの既存データを先に削除（citycode指定時）
            if self.citycode and self.citycode != "unknown":
                cursor.execute(
                    "SELECT COUNT(*) FROM plateau_buildings WHERE city_code = %s",
                    (self.citycode,)
                )
                existing_count = cursor.fetchone()[0]
                if existing_count > 0:
                    logger.info(f"🧹 既存データ検出: {self.citycode} ({existing_count}件) — 削除して再インポート")
                    # ノードを先に削除（foreign key制約）
                    cursor.execute("""
                        DELETE FROM plateau_building_nodes
                        WHERE building_id IN (
                            SELECT id FROM plateau_buildings WHERE city_code = %s
                        )
                    """, (self.citycode,))
                    cursor.execute(
                        "DELETE FROM plateau_buildings WHERE city_code = %s",
                        (self.citycode,)
                    )
                    conn.commit()
                    logger.info(f"✅ 既存データ削除完了")

            if buildings_data:
                logger.info("🏢 建物データ投入中...")

                execute_values(
                    cursor,
                    """
                    INSERT INTO plateau_buildings
                    (osm_id, building, height, ele, building_levels, building_levels_underground,
                     source_dataset, plateau_id, geometry_wkt,
                     name, addr_full, addr_housenumber, addr_street,
                     start_date, building_material, roof_material, roof_shape,
                     amenity, shop, tourism, leisure, landuse,
                     city_code,
                     building_part,
                     geom, centroid)
                    VALUES %s
                    """,
                    buildings_data,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), ST_Centroid(ST_GeomFromText(%s, 4326)))",
                    page_size=1000
                )
                logger.info("✅ 建物投入完了")

            # ノード投入
            if nodes_data:
                logger.info("📍 ノードデータ投入中...")

                # osm_id → auto increment id のマッピングを取得
                # ノードのbuilding_idにはosm_id（building_id_counter）が入っているが、
                # foreign keyはplateau_buildings.id（auto increment）を参照する
                cursor.execute(
                    "SELECT osm_id, id FROM plateau_buildings WHERE city_code = %s",
                    (self.citycode,)
                )
                osm_id_to_db_id = dict(cursor.fetchall())
                logger.info(f"   建物IDマッピング: {len(osm_id_to_db_id):,}件")

                mapped_nodes_data, skipped_count, orphan_count = self._dedupe_and_remap_nodes(
                    nodes_data, osm_id_to_db_id
                )

                if orphan_count > 0:
                    logger.warning(f"   ⚠️ 建物なしノード除外: {orphan_count:,}件")

                logger.info(f"   投入ノード: {len(mapped_nodes_data):,}件")
                logger.info(f"   重複スキップ: {skipped_count:,}件")

                if mapped_nodes_data:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO plateau_building_nodes (osm_id, building_id, sequence_id, lat, lon, geom)
                        VALUES %s
                        """,
                        mapped_nodes_data,
                        template="(%s, %s, %s, %s, %s, ST_Point(%s, %s))",
                        page_size=5000
                    )
                logger.info("✅ ノード投入完了")

            # building:part の parent_building_id 解決
            if parts_parent_map:
                self._resolve_part_parents(cursor, parts_parent_map)

            # 行政界 N03 フィルタ (Rapid#35 part C)
            self._apply_city_boundary_filter(cursor)

            # コミット
            conn.commit()

            # 最終確認
            cursor.execute("SELECT COUNT(*) FROM plateau_buildings")
            final_buildings = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM plateau_building_nodes")
            final_nodes = cursor.fetchone()[0]

            # 有効性確認
            cursor.execute("SELECT COUNT(*) FROM plateau_buildings WHERE ST_IsValid(geom)")
            valid_buildings = cursor.fetchone()[0]

            validity_rate = valid_buildings / final_buildings * 100 if final_buildings > 0 else 0

            logger.info(f"🎉 投入完了!")
            logger.info(f"📊 最終データ: 建物{final_buildings:,}件, ノード{final_nodes:,}件")
            logger.info(f"✅ ジオメトリ有効性: {validity_rate:.1f}% ({valid_buildings:,}/{final_buildings:,})")

            return True

        except Exception as e:
            logger.error(f"❌ データベース投入失敗: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

    def create_import_report(self, start_analysis: Dict, zip_count: int, osm_count: int,
                           building_processed: int, node_processed: int):
        """インポートレポート作成"""
        report_file = self.data_dir / "import_report.txt"

        # 最終分析
        final_analysis = self.analyze_existing_data()

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# Plateau建物データ インポートレポート\n")
            f.write(f"# 実行日時: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("## インポート前状況\n")
            f.write(f"建物数: {start_analysis.get('total_buildings', 0):,}件\n")
            f.write(f"有効性率: {start_analysis.get('validity_rate', 0):.1f}%\n")

            f.write("\n## 処理データ\n")
            f.write(f"ZIPファイル: {zip_count}件\n")
            f.write(f"OSMファイル: {osm_count}件\n")
            f.write(f"新規建物: {building_processed:,}件\n")
            f.write(f"新規ノード: {node_processed:,}件\n")

            f.write("\n## インポート後状況\n")
            f.write(f"総建物数: {final_analysis.get('total_buildings', 0):,}件\n")
            f.write(f"有効性率: {final_analysis.get('validity_rate', 0):.1f}%\n")

            building_increase = final_analysis.get('total_buildings', 0) - start_analysis.get('total_buildings', 0)
            f.write(f"建物増加: +{building_increase:,}件\n")

            if final_analysis.get('validity_rate', 0) >= 99.9:
                f.write("\n✅ 高品質インポート成功\n")

        logger.info(f"📋 インポートレポート作成: {report_file}")

    def run_complete_import(self):
        """完全インポート実行（バッチ分割対応・大規模都市OOM対策）"""
        logger.info("🚀 Plateau建物データ PostGISインポート開始")
        logger.info("=" * 60)

        start_time = time.time()

        try:
            # Phase 1: 事前分析
            logger.info("\n📊 Phase 1: 既存データ分析")
            start_analysis = self.analyze_existing_data()

            # Phase 2: zipファイル確認
            logger.info("\n📁 Phase 2: zipファイル確認")
            zip_files = self.find_zip_files()
            if not zip_files:
                logger.error("❌ zipファイルが見つかりません")
                logger.info("💡 ヒント: データディレクトリにzipファイルを配置してください")
                return False

            # Phase 3: OSM抽出
            logger.info("\n📂 Phase 3: OSM展開・抽出")
            osm_files = self.extract_zip_files(zip_files)
            if not osm_files:
                logger.error("❌ OSMファイルが見つかりません")
                return False

            # 既存データの事前削除（バッチ処理前に1回だけ実行）
            if self.citycode and self.citycode != "unknown":
                import psycopg2 as pg2
                conn = pg2.connect(self.postgres_url)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT COUNT(*) FROM plateau_buildings WHERE city_code = %s",
                    (self.citycode,)
                )
                existing_count = cursor.fetchone()[0]
                if existing_count > 0:
                    logger.info(f"🧹 既存データ検出: {self.citycode} ({existing_count}件) — 削除して再インポート")
                    cursor.execute("""
                        DELETE FROM plateau_building_nodes
                        WHERE building_id IN (
                            SELECT id FROM plateau_buildings WHERE city_code = %s
                        )
                    """, (self.citycode,))
                    cursor.execute(
                        "DELETE FROM plateau_buildings WHERE city_code = %s",
                        (self.citycode,)
                    )
                    conn.commit()
                    logger.info(f"✅ 既存データ削除完了")
                conn.close()

            # バッチサイズ決定（ファイル数に応じて分割）
            BATCH_SIZE = 10  # 10ファイルずつ処理
            num_batches = (len(osm_files) + BATCH_SIZE - 1) // BATCH_SIZE
            logger.info(f"📦 バッチ分割: {len(osm_files)}ファイル → {num_batches}バッチ (各{BATCH_SIZE}ファイル)")

            total_buildings_count = 0
            total_nodes_count = 0

            for batch_idx in range(num_batches):
                batch_start = batch_idx * BATCH_SIZE
                batch_end = min(batch_start + BATCH_SIZE, len(osm_files))
                batch_files = osm_files[batch_start:batch_end]

                logger.info(f"\n{'='*40}")
                logger.info(f"📦 バッチ {batch_idx+1}/{num_batches} ({len(batch_files)}ファイル)")
                logger.info(f"{'='*40}")

                # Phase 4: OSM解析（バッチ単位）
                all_nodes = {}
                all_buildings = []

                for i, osm_file in enumerate(batch_files, 1):
                    file_num = batch_start + i
                    logger.info(f"📖 [{file_num:3d}/{len(osm_files)}] 解析中: {osm_file.name}")

                    nodes, buildings = self.parse_osm_file_safe(osm_file)

                    for original_id, node_data in nodes.items():
                        file_specific_key = f"{osm_file.name}:{original_id}"
                        all_nodes[file_specific_key] = node_data

                    for building in buildings:
                        building['node_refs'] = [f"{osm_file.name}:{ref}" for ref in building['node_refs']]
                        all_buildings.append(building)

                    logger.info(f"     結果: {len(nodes):,}ノード, {len(buildings):,}建物")

                logger.info(f"📊 バッチ統合: {len(all_nodes):,}ノード, {len(all_buildings):,}建物")

                # Phase 5: 建物処理（バッチ単位）
                buildings_data, nodes_data, parts_parent_map = self.process_buildings_safe(all_nodes, all_buildings)

                if buildings_data:
                    # Phase 6: データベース投入（バッチ単位、事前削除なし）
                    logger.info(f"💾 バッチ {batch_idx+1} DB投入中...")
                    success = self.insert_to_database_batch(buildings_data, nodes_data, parts_parent_map)
                    if not success:
                        logger.error(f"❌ バッチ {batch_idx+1} DB投入失敗")
                        return False

                    total_buildings_count += len(buildings_data)
                    total_nodes_count += len(nodes_data)

                # メモリ解放
                del all_nodes, all_buildings, buildings_data, nodes_data, parts_parent_map
                import gc
                gc.collect()
                logger.info(f"🧹 メモリ解放完了")

            # Phase 7: レポート作成
            logger.info("\n📋 Phase 7: インポートレポート作成")
            self.create_import_report(
                start_analysis, len(zip_files), len(osm_files),
                total_buildings_count, total_nodes_count
            )

            elapsed_time = time.time() - start_time

            logger.info("=" * 60)
            logger.info("🎉 Plateau建物データ PostGISインポート成功!")
            logger.info(f"⏱️ 処理時間: {elapsed_time/60:.1f}分")
            logger.info(f"🏢 新規建物: {total_buildings_count:,}件")
            logger.info(f"📍 新規ノード: {total_nodes_count:,}件")
            logger.info("✅ 次のステップ:")
            logger.info("   1. API動作確認")
            logger.info("   2. RapiD Editor表示テスト")
            logger.info("   3. カバレッジ検証")
            logger.info("=" * 60)

            return True

        except Exception as e:
            logger.error(f"❌ インポート失敗: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Plateau建物データ PostGISインポーター')
    parser.add_argument('--data-dir', default='./plateau_data',
                       help='データディレクトリ (default: ./plateau_data)')
    parser.add_argument('--postgres-url',
                       default='postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau',
                       help='PostgreSQL接続URL')
    parser.add_argument('--citycode',
                       help='市区町村コード (例: "31202")。未指定時はdata-dirのディレクトリ名から推定')
    parser.add_argument('--coord-bounds',
                       help='座標範囲チェック: "min_lat,max_lat,min_lon,max_lon" (例: "35.2,35.6,133.0,133.5")')
    parser.add_argument('--verbose', action='store_true',
                       help='詳細ログ出力')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    coord_bounds = None
    if args.coord_bounds:
        coord_bounds = tuple(float(x) for x in args.coord_bounds.split(','))

    logger.info("🏗️ Plateau建物データ PostGISインポーター起動")

    importer = PlateauImporter2PostGIS(args.data_dir, args.postgres_url, coord_bounds, args.citycode)
    success = importer.run_complete_import()

    if success:
        logger.info("✅ インポート成功！APIテストを実行してください")
        print("\n🎉 インポート成功!")
        print("🚀 次は API動作確認とRapiD Editorテスト")
    else:
        logger.error("❌ インポートに失敗しました")
        print("\n❌ インポートに問題が発生しました")
        print("📋 詳細: plateau_importer2postgis.log を確認")
        sys.exit(1)

if __name__ == "__main__":
    main()
