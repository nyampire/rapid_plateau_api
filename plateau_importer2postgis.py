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
from typing import List, Dict, Tuple, Set
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

            # 既存のノード座標をマップに読み込み（同一座標は同一IDを保証）
            cursor.execute("""
                SELECT osm_id, lat, lon 
                FROM plateau_building_nodes 
                WHERE osm_id IS NOT NULL
            """)
            existing_nodes = cursor.fetchall()
            for osm_id, lat, lon in existing_nodes:
                coord_key = f"{lat:.7f},{lon:.7f}"
                self.node_coordinate_map[coord_key] = osm_id

            conn.close()

            logger.info(f"🔢 ID初期化完了:")
            logger.info(f"   建物IDカウンター: {self.building_id_counter} から開始")
            logger.info(f"   ノードIDカウンター: {self.node_id_counter} から開始")
            logger.info(f"   既存ノード座標マップ: {len(self.node_coordinate_map):,} 件読み込み")

        except Exception as e:
            logger.warning(f"⚠️ ID初期化でエラー（デフォルト値を使用）: {e}")

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
        """安全なOSMファイル解析（修復済み技術）"""
        try:
            tree = ET.parse(osm_file)
            root = tree.getroot()
        except ET.ParseError as e:
            logger.warning(f"❌ XMLパースエラー {osm_file.name}: {e}")
            return {}, []

        file_prefix = osm_file.stem
        nodes = {}
        buildings = []

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

        # 建物ウェイ収集
        for way_elem in root.findall('way'):
            tags = {}
            for tag_elem in way_elem.findall('tag'):
                key = tag_elem.get('k')
                value = tag_elem.get('v')
                if key and value:
                    tags[key] = value

            # 建物判定
            if tags.get('building'):
                way_id = way_elem.get('id')
                nd_refs = []

                for nd_elem in way_elem.findall('nd'):
                    nd_ref = nd_elem.get('ref')
                    if nd_ref in nodes:
                        nd_refs.append(nd_ref)

                # 最低3点でポリゴン形成
                if len(nd_refs) >= 3:
                    buildings.append({
                        'way_id': way_id,
                        'tags': tags,
                        'node_refs': nd_refs,
                        'source_file': osm_file.name,
                        'file_prefix': file_prefix
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
            'source_dataset': f"plateau_{self.citycode}_{source_info}"
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

    def process_buildings_safe(self, all_nodes: Dict, all_buildings: List) -> Tuple[List, List]:
        """建物処理（安全版・重複除去付き）"""
        logger.info(f"🏗️ {len(all_buildings):,}建物を安全処理中...")

        buildings_data = []
        nodes_data = []
        processed_count = 0
        skipped_count = 0
        duplicate_count = 0

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

                            # 建物データ（plateau_buildingsテーブル構造に合わせる）
                            buildings_data.append((
                                self.building_id_counter,           # osm_id
                                converted_tags.get('building', 'yes'),  # building
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
                                polygon_wkt,                        # geom用WKT
                                polygon_wkt                         # centroid用WKT
                            ))

                            nodes_data.extend(building_nodes)
                            self.building_id_counter += 1
                            processed_count += 1
                        else:
                            skipped_count += 1
                    else:
                        skipped_count += 1
                else:
                    skipped_count += 1

            except Exception as e:
                logger.warning(f"⚠️ 建物処理エラー {i}: {e}")
                skipped_count += 1
                continue

        logger.info(f"📊 建物処理結果:")
        logger.info(f"   成功: {processed_count:,}件")
        logger.info(f"   重複除去: {duplicate_count:,}件")
        logger.info(f"   スキップ: {skipped_count:,}件")
        logger.info(f"   総ノード: {len(nodes_data):,}件")

        return buildings_data, nodes_data

    def insert_to_database_safe(self, buildings_data: List, nodes_data: List) -> bool:
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
                    "SELECT COUNT(*) FROM plateau_buildings WHERE source_dataset LIKE %s",
                    (f"%{self.citycode}%",)
                )
                existing_count = cursor.fetchone()[0]
                if existing_count > 0:
                    logger.info(f"🧹 既存データ検出: {self.citycode} ({existing_count}件) — 削除して再インポート")
                    # ノードを先に削除（foreign key制約）
                    cursor.execute("""
                        DELETE FROM plateau_building_nodes
                        WHERE building_id IN (
                            SELECT id FROM plateau_buildings WHERE source_dataset LIKE %s
                        )
                    """, (f"%{self.citycode}%",))
                    cursor.execute(
                        "DELETE FROM plateau_buildings WHERE source_dataset LIKE %s",
                        (f"%{self.citycode}%",)
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
                     geom, centroid)
                    VALUES %s
                    """,
                    buildings_data,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), ST_Centroid(ST_GeomFromText(%s, 4326)))",
                    page_size=1000
                )
                logger.info("✅ 建物投入完了")

            # ノード投入
            if nodes_data:
                logger.info("📍 ノードデータ投入中...")

                # 今回投入するbuilding_idの集合を取得（直前にINSERTした建物のみ）
                cursor.execute(
                    "SELECT id FROM plateau_buildings WHERE source_dataset LIKE %s",
                    (f"%{self.citycode}%",)
                )
                current_building_ids = set(row[0] for row in cursor.fetchall())
                logger.info(f"   今回の建物ID: {len(current_building_ids):,}件")

                # 今回の建物に属するノードのみフィルタ & データ内重複除去
                unique_nodes_data = []
                seen_node_ids = set()
                skipped_count = 0
                orphan_count = 0

                for node_data in nodes_data:
                    node_id = node_data[0]  # osm_id
                    building_id = node_data[1]  # building_id
                    if node_id in seen_node_ids:
                        skipped_count += 1
                    elif building_id not in current_building_ids:
                        orphan_count += 1
                    else:
                        unique_nodes_data.append(node_data)
                        seen_node_ids.add(node_id)

                if orphan_count > 0:
                    logger.warning(f"   ⚠️ 建物なしノード除外: {orphan_count:,}件")

                logger.info(f"   投入ノード: {len(unique_nodes_data):,}件")
                logger.info(f"   重複スキップ: {skipped_count:,}件")

                if unique_nodes_data:
                    execute_values(
                        cursor,
                        """
                        INSERT INTO plateau_building_nodes (osm_id, building_id, sequence_id, lat, lon, geom)
                        VALUES %s
                        """,
                        unique_nodes_data,
                        template="(%s, %s, %s, %s, %s, ST_Point(%s, %s))",
                        page_size=5000
                    )
                logger.info("✅ ノード投入完了")

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
        """完全インポート実行"""
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

            # Phase 4: OSM解析
            logger.info("\n📖 Phase 4: OSM解析・統合")
            all_nodes = {}
            all_buildings = []

            for i, osm_file in enumerate(osm_files, 1):
                logger.info(f"📖 [{i:3d}/{len(osm_files)}] 解析中: {osm_file.name}")

                nodes, buildings = self.parse_osm_file_safe(osm_file)

                # ノード統合（重複座標は同一IDに）
                for original_id, node_data in nodes.items():
                    file_specific_key = f"{osm_file.name}:{original_id}"
                    all_nodes[file_specific_key] = node_data

                # 建物統合
                for building in buildings:
                    # ノード参照をファイル固有キーに変更
                    building['node_refs'] = [f"{osm_file.name}:{ref}" for ref in building['node_refs']]
                    all_buildings.append(building)

                logger.info(f"     結果: {len(nodes):,}ノード, {len(buildings):,}建物")

            logger.info(f"📊 統合結果: {len(all_nodes):,}ノード, {len(all_buildings):,}建物")
            logger.info(f"🆔 ユニーク座標: {len(self.node_coordinate_map):,}箇所")

            # Phase 5: 建物処理
            logger.info("\n🏗️ Phase 5: 建物データ処理")
            buildings_data, nodes_data = self.process_buildings_safe(all_nodes, all_buildings)

            if not buildings_data:
                logger.error("❌ 処理可能な建物データがありません")
                return False

            # Phase 6: データベース投入
            logger.info("\n💾 Phase 6: データベース投入")
            success = self.insert_to_database_safe(buildings_data, nodes_data)

            if not success:
                return False

            # Phase 7: レポート作成
            logger.info("\n📋 Phase 7: インポートレポート作成")
            self.create_import_report(
                start_analysis, len(zip_files), len(osm_files),
                len(buildings_data), len(nodes_data)
            )

            # 完了時間
            elapsed_time = time.time() - start_time

            logger.info("=" * 60)
            logger.info("🎉 Plateau建物データ PostGISインポート成功!")
            logger.info(f"⏱️ 処理時間: {elapsed_time/60:.1f}分")
            logger.info(f"🏢 新規建物: {len(buildings_data):,}件")
            logger.info(f"📍 新規ノード: {len(nodes_data):,}件")
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
