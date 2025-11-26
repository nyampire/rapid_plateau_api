#!/usr/bin/env python3
"""
OSMFJ Plateau MapWithAI API - PostgreSQL/PostGIS版（タイル境界問題修正版）
日本のPlateau都市データをMapWithAI形式で配信
修正: ST_Intersects/ST_Contains切り替え可能、キャッシュ制御改善
"""

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
import json
import logging
import os
import uvicorn
from typing import List, Dict, Any, Optional
from datetime import datetime
import re
import hashlib

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OSMFJPlateauAPI:
    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
            # 環境変数または デフォルト接続文字列
            database_url = os.getenv('DATABASE_URL',
                'postgresql://osmfj_user:secure_plateau_password@localhost:5432/osmfj_plateau')

        self.database_url = database_url
        self._test_connection()

    def _test_connection(self):
        """データベース接続テスト"""
        try:
            conn = psycopg2.connect(self.database_url)
            cursor = conn.cursor()
            cursor.execute("SELECT PostGIS_Version()")
            version = cursor.fetchone()[0]
            logger.info(f"✅ PostgreSQL/PostGIS接続成功: {version}")

            # テーブル存在確認
            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('plateau_buildings', 'plateau_building_nodes')
            """)
            tables = cursor.fetchall()
            logger.info(f"📋 利用可能テーブル: {[table[0] for table in tables]}")

            conn.close()
        except Exception as e:
            logger.error(f"❌ PostgreSQL接続失敗: {e}")
            raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

    def get_connection(self):
        """データベース接続を取得"""
        try:
            return psycopg2.connect(self.database_url, cursor_factory=RealDictCursor)
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise HTTPException(status_code=500, detail="Database connection failed")

    def get_buildings_in_bbox(self, min_lon: float, min_lat: float,
                             max_lon: float, max_lat: float,
                             limit: int = 1000, city: Optional[str] = None,
                             use_intersects: bool = True) -> List[Dict]:
        """
        PostGIS spatial queryを使用した建物検索（切り替え可能な空間判定方式）

        Args:
            use_intersects: True = ST_Intersects（建物全体）、False = ST_Contains（重心のみ）
        """

        conn = self.get_connection()
        cursor = conn.cursor()

        # デバッグログ
        query_type = "INTERSECTS" if use_intersects else "CENTROID-BASED"
        logger.info(f"🔍 Executing spatial query ({query_type}) with bbox: {min_lon}, {min_lat}, {max_lon}, {max_lat}")

        try:
            # 空間判定条件を動的に構築
            if use_intersects:
                # 建物ジオメトリ全体がbboxと交差する場合を含める
                spatial_condition = """
                    ST_Intersects(
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326),
                        b.geom
                    )
                """
                distinct_key = "b.osm_id"  # ST_Intersectsの場合はosm_idで重複除去
            else:
                # 重心ベース判定（元の実装）
                spatial_condition = """
                    ST_Contains(
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326),
                        b.centroid
                    )
                """
                distinct_key = "MD5(ST_AsText(b.geom))"  # 重心ベースの場合はgeomハッシュで重複除去

            query = f"""
                WITH unique_buildings AS (
                    SELECT DISTINCT ON ({distinct_key})
                        b.id,  -- 追加：idカラムを選択
                        b.osm_id,
                        b.building,
                        b.height,
                        b.ele,
                        b.building_levels,
                        b.building_levels_underground,
                        b.source_dataset,
                        b.plateau_id,
                        b.ref_mlit_plateau,
                        b.name,
                        b.addr_full,
                        b.start_date,
                        b.survey_date,
                        b.building_class,
                        b.building_usage,
                        b.geom,
                        b.centroid,
                        ST_AsText(b.geom) as geometry_wkt,
                        ST_Distance(b.centroid, ST_SetSRID(ST_Point(%s, %s), 4326)) as distance,
                        ST_X(b.centroid) as centroid_lon,
                        ST_Y(b.centroid) as centroid_lat
                    FROM plateau_buildings b
                    WHERE {spatial_condition}
                    ORDER BY {distinct_key}, b.osm_id
                )
                SELECT
                    ub.id,  -- 追加：idを選択
                    ub.osm_id,
                    ub.building,
                    ub.height,
                    ub.ele,
                    ub.building_levels,
                    ub.building_levels_underground,
                    ub.source_dataset,
                    ub.plateau_id,
                    ub.ref_mlit_plateau,
                    ub.name,
                    ub.addr_full,
                    ub.start_date,
                    ub.survey_date,
                    ub.building_class,
                    ub.building_usage,
                    ub.geometry_wkt,
                    ub.distance,
                    ub.centroid_lon,
                    ub.centroid_lat,
                    ARRAY_AGG(
                        json_build_object(
                            'id', n.id,
                            'osm_id', n.osm_id,
                            'lat', n.lat,
                            'lon', n.lon,
                            'sequence_id', n.sequence_id
                        ) ORDER BY n.sequence_id
                    ) as nodes
                FROM unique_buildings ub
                LEFT JOIN plateau_building_nodes n ON ub.id = n.building_id  -- 修正：ub.idを使用
                GROUP BY ub.id, ub.osm_id, ub.building, ub.height, ub.ele, ub.building_levels,
                         ub.building_levels_underground, ub.source_dataset, ub.plateau_id,
                         ub.ref_mlit_plateau, ub.name, ub.addr_full, ub.start_date,
                         ub.survey_date, ub.building_class, ub.building_usage, ub.geom,
                         ub.centroid, ub.geometry_wkt, ub.distance, ub.centroid_lon, ub.centroid_lat
                ORDER BY ub.distance, ub.osm_id
                LIMIT %s
            """

            # 中心点計算してパラメータ設定
            center_lon = (min_lon + max_lon) / 2
            center_lat = (min_lat + max_lat) / 2
            params = [center_lon, center_lat, min_lon, min_lat, max_lon, max_lat, limit]

            cursor.execute(query, params)
            buildings = cursor.fetchall()

            # 結果を辞書リストに変換
            result = [dict(building) for building in buildings]

            logger.info(f"🏢 検索結果（{query_type}）: {len(result)}件の建物 (bbox: {min_lon:.6f},{min_lat:.6f},{max_lon:.6f},{max_lat:.6f})")

            return result

        except Exception as e:
            logger.error(f"PostGIS query error: {e}")
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
        finally:
            conn.close()

    # osmfj_plateau_api.py の buildings_to_osm_xml メソッドを修正

# buildings_to_osm_xmlメソッドの修正版（デバッグ強化）
    def buildings_to_osm_xml(self, buildings: List[Dict]) -> str:
        """OSM標準準拠のXML出力（OSM APIと完全互換）"""

        # OSM XMLルート要素（OSM API v0.6準拠）
        osm = ET.Element('osm')
        osm.set('version', '0.6')
        osm.set('generator', 'osmfj-plateau-api-v2.5-osm-compatible')
        osm.set('copyright', 'Plateau Japan')
        osm.set('attribution', 'https://www.mlit.go.jp/plateau/')
        osm.set('license', 'https://www.mlit.go.jp/plateau/')

        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # DBのIDを使用するため、カウンターは不要だがフォールバック用に残す
        way_id_counter = -1000001  # フォールバック用

        logger.info(f"🔍 OSM標準XML生成: {len(buildings)}件の建物を処理中")

        # 最初にすべてのノードとwayを準備（後で正しい順序で出力）
        all_nodes = []
        all_ways = []
        processed_buildings = 0
        created_ways = 0
        total_nodes_created = 0

        for building_index, building in enumerate(buildings):
            try:
                nodes = building.get('nodes', [])
                if not nodes or nodes == [None] or not any(nodes):
                    continue

                # 有効なノードのみをフィルタ（DBのidを保持）
                valid_nodes = []
                for node in nodes:
                    if node and 'lat' in node and 'lon' in node and 'id' in node:
                        try:
                            lat = float(node['lat'])
                            lon = float(node['lon'])
                            node_db_id = int(node['id'])
                            if -90 <= lat <= 90 and -180 <= lon <= 180:
                                valid_nodes.append({'lat': lat, 'lon': lon, 'id': node_db_id})
                        except (ValueError, TypeError):
                            continue

                if len(valid_nodes) < 3:
                    continue

                # ポリゴンの閉鎖チェック
                first_node = valid_nodes[0]
                last_node = valid_nodes[-1]
                is_closed = (abs(first_node['lat'] - last_node['lat']) < 1e-7 and
                            abs(first_node['lon'] - last_node['lon']) < 1e-7)

                if is_closed:
                    valid_nodes = valid_nodes[:-1]

                # Way要素を準備（建物のDB IDを使用）
                building_db_id = building.get('id', way_id_counter)
                way_elem = ET.Element('way')
                way_elem.set('id', str(-building_db_id))  # 負の値にしてOSM新規ID形式に
                way_elem.set('visible', 'true')
                way_elem.set('version', '1')
                way_elem.set('changeset', '1')
                way_elem.set('timestamp', timestamp)
                way_elem.set('user', 'osmfj-plateau')
                way_elem.set('uid', '1')

                # ノードを作成（DBのIDを使用）
                way_node_refs = []
                first_node_id = None

                for i, node_data in enumerate(valid_nodes):
                    lat = node_data['lat']
                    lon = node_data['lon']
                    node_db_id = -node_data['id']  # 負の値にしてOSM新規ID形式に

                    # ノード要素を作成
                    node_elem = ET.Element('node')
                    node_elem.set('id', str(node_db_id))
                    node_elem.set('visible', 'true')
                    node_elem.set('version', '1')
                    node_elem.set('changeset', '1')
                    node_elem.set('timestamp', timestamp)
                    node_elem.set('user', 'osmfj-plateau')
                    node_elem.set('uid', '1')
                    node_elem.set('lat', f"{lat:.7f}")
                    node_elem.set('lon', f"{lon:.7f}")

                    all_nodes.append(node_elem)

                    # wayにノード参照を追加
                    nd_elem = ET.SubElement(way_elem, 'nd')
                    nd_elem.set('ref', str(node_db_id))

                    if i == 0:
                        first_node_id = node_db_id

                    total_nodes_created += 1

                # ポリゴンを閉じる（最初のノードを参照）
                nd_elem = ET.SubElement(way_elem, 'nd')
                nd_elem.set('ref', str(first_node_id))

                # OSM標準タグを追加
                def add_tag(parent, key, value):
                    if value is not None and str(value).strip():
                        tag_elem = ET.SubElement(parent, 'tag')
                        tag_elem.set('k', key)
                        tag_elem.set('v', str(value))

                # 必須タグ（OSM標準）
                add_tag(way_elem, 'building', building.get('building', 'yes'))

                # 高さ情報（OSM標準）
                if building.get('height'):
                    add_tag(way_elem, 'height', str(building['height']))

                if building.get('building_levels'):
                    add_tag(way_elem, 'building:levels', str(building['building_levels']))

                # ソース情報（OSM標準）
                add_tag(way_elem, 'source', 'Plateau Japan (MLIT)')

                # Plateau固有の情報（独自タグはネームスペースを使用）
                if building.get('plateau_id'):
                    add_tag(way_elem, 'ref:plateau', building['plateau_id'])

                if building.get('ele'):
                    add_tag(way_elem, 'ele', str(building['ele']))

                # デバッグ情報（必要に応じて削除可能）
                if building.get('source_dataset'):
                    add_tag(way_elem, 'source:dataset', building['source_dataset'])

                all_ways.append(way_elem)
                created_ways += 1
                processed_buildings += 1

                # デバッグログ
                if building.get('plateau_id') in ['2929', '85025']:
                    logger.info(f"🎯 Plateau {building.get('plateau_id')}: way_id={-building_db_id}, nodes={len(valid_nodes)+1}")

            except Exception as e:
                logger.warning(f"⚠️ 建物処理エラー {building.get('id', 'unknown')}: {e}")
                continue

        # OSM標準の順序で要素を追加：ノード → ウェイ
        for node in all_nodes:
            osm.append(node)
        for way in all_ways:
            osm.append(way)

        logger.info(f"✅ OSM標準XML生成完了: {processed_buildings}件処理, {created_ways}件のWay作成, {total_nodes_created}個のノード作成")

        # OSM標準準拠のXML生成
        try:
            # XML宣言付きで出力
            xml_string = ET.tostring(osm, encoding='unicode', method='xml')

            # OSM標準のXML宣言
            xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>'

            # タグの間に改行を追加してフォーマット
            # ><を>\n<に置換して読みやすくする
            formatted_xml = xml_string.replace('><', '>\n<')

            # 最終的なXMLを構築
            final_xml = xml_declaration + '\n' + formatted_xml

            return final_xml

        except Exception as e:
            logger.error(f"❌ XML生成エラー: {e}")
            # 最小限のフォールバック
            return '<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6" generator="osmfj-plateau-api-fallback"></osm>'

    def get_statistics(self) -> Dict[str, Any]:
        """データベース統計情報取得"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # 直接クエリで基本統計を取得
            cursor.execute("""
                SELECT
                    COUNT(*) as building_count,
                    COUNT(CASE WHEN height IS NOT NULL THEN 1 END) as buildings_with_height,
                    AVG(height) as avg_height,
                    MAX(height) as max_height,
                    COUNT(CASE WHEN ref_mlit_plateau IS NOT NULL THEN 1 END) as buildings_with_plateau_id,
                    pg_size_pretty(pg_total_relation_size('plateau_buildings')) as buildings_table_size
                FROM plateau_buildings
            """)
            building_stats = cursor.fetchone()

            cursor.execute("""
                SELECT
                    COUNT(*) as node_count,
                    pg_size_pretty(pg_total_relation_size('plateau_building_nodes')) as nodes_table_size
                FROM plateau_building_nodes
            """)
            node_stats = cursor.fetchone()

            # 空間範囲取得
            cursor.execute("""
                SELECT
                    MIN(lon) as min_lon,
                    MIN(lat) as min_lat,
                    MAX(lon) as max_lon,
                    MAX(lat) as max_lat
                FROM plateau_building_nodes
            """)
            bbox = cursor.fetchone()

            # データセット別統計
            cursor.execute("""
                SELECT
                    source_dataset,
                    COUNT(*) as count
                FROM plateau_buildings
                WHERE source_dataset IS NOT NULL
                GROUP BY source_dataset
                ORDER BY count DESC
            """)
            datasets = cursor.fetchall()

            return {
                'database': 'PostgreSQL/PostGIS',
                'database_name': 'osmfj_plateau',
                'buildings': dict(building_stats) if building_stats else {},
                'nodes': dict(node_stats) if node_stats else {},
                'bbox': dict(bbox) if bbox else None,
                'datasets': [dict(ds) for ds in datasets],
                'api_version': '2.1.0-flexible',
                'timestamp': datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Statistics query error: {e}")
            raise HTTPException(status_code=500, detail=f"Statistics query failed: {str(e)}")
        finally:
            conn.close()

    def get_cities(self) -> List[Dict[str, Any]]:
        """利用可能な都市一覧取得"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # データセット別の統計を都市として扱う
            cursor.execute("""
                SELECT
                    source_dataset as dataset_name,
                    COUNT(*) as building_count,
                    AVG(height) as avg_height,
                    MIN(lon) as min_lon,
                    MIN(lat) as min_lat,
                    MAX(lon) as max_lon,
                    MAX(lat) as max_lat
                FROM plateau_buildings b
                LEFT JOIN plateau_building_nodes n ON b.osm_id = n.building_id
                WHERE source_dataset IS NOT NULL
                GROUP BY source_dataset
                ORDER BY building_count DESC
            """)
            datasets = cursor.fetchall()
            return [dict(ds) for ds in datasets]
        except Exception as e:
            logger.error(f"Cities query error: {e}")
            raise HTTPException(status_code=500, detail=f"Cities query failed: {str(e)}")
        finally:
            conn.close()

# FastAPI アプリケーション初期化
app = FastAPI(
    title="OSMFJ Plateau MapWithAI API (Flexible)",
    description="日本のPlateau都市データをMapWithAI形式で配信するAPI（柔軟な空間判定版）",
    version="2.1.0-flexible",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS設定（RapiD Editor対応強化）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 本番では適切に制限
    allow_credentials=True,
    allow_methods=["GET", "HEAD", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# API インスタンス
api = OSMFJPlateauAPI()

@app.get("/")
async def root():
    """API情報"""
    return {
        "name": "OSMFJ Plateau MapWithAI API (Flexible)",
        "version": "2.1.0-flexible",
        "description": "日本のPlateau都市データのMapWithAI配信API（柔軟な空間判定版）",
        "database": "osmfj_plateau",
        "features": [
            "切り替え可能な空間判定（ST_Intersects/ST_Contains）",
            "改善されたキャッシュ制御",
            "タイル境界問題の解決"
        ],
        "endpoints": {
            "buildings": "/api/mapwithai/buildings?bbox=min_lon,min_lat,max_lon,max_lat&use_intersects=true",
            "statistics": "/api/stats",
            "cities": "/api/cities",
            "health": "/health",
            "debug": "/debug/xml?bbox=min_lon,min_lat,max_lon,max_lat",
            "debug_plateau": "/debug/plateau/{plateau_id}",
            "compare_plateaus": "/debug/compare-plateaus?ids=id1,id2"
        },
        "docs": "/docs"
    }

@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    try:
        stats = api.get_statistics()
        return {
            "status": "healthy",
            "database": "PostgreSQL/PostGIS",
            "database_name": "osmfj_plateau",
            "buildings_count": stats['buildings'].get('building_count', 0),
            "nodes_count": stats['nodes'].get('node_count', 0),
            "api_version": "2.1.0-flexible",
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Service unhealthy: {str(e)}")

@app.get("/api/stats")
async def get_statistics():
    """データベース統計情報"""
    return api.get_statistics()

@app.get("/api/cities")
async def get_cities():
    """利用可能な都市一覧"""
    return api.get_cities()

@app.get("/debug/xml")
async def debug_xml(
    bbox: str = "133.368,35.380,133.369,35.381",
    limit: int = 3,
    use_intersects: bool = True
):
    """XMLデバッグ用エンドポイント - プレーンテキストでXMLを返す"""
    try:
        coords = [float(x.strip()) for x in bbox.split(',')]
        min_lon, min_lat, max_lon, max_lat = coords

        buildings = api.get_buildings_in_bbox(
            min_lon, min_lat, max_lon, max_lat, limit,
            use_intersects=use_intersects
        )
        osm_xml = api.buildings_to_osm_xml(buildings)

        return Response(
            content=osm_xml,
            media_type="text/plain",
            headers={"Content-Type": "text/plain; charset=utf-8"}
        )
    except Exception as e:
        return Response(
            content=f"Error: {str(e)}",
            media_type="text/plain"
        )

@app.get("/debug/plateau/{plateau_id}")
async def debug_plateau_id(
    plateau_id: str,
    format: str = "xml"
):
    """特定のPlateau IDを持つ建物をデバッグ用に取得"""
    conn = api.get_connection()
    cursor = conn.cursor()

    try:
        # Plateau IDで検索
        cursor.execute("""
            SELECT DISTINCT ON (b.id)
                b.id,
                b.osm_id,
                b.building,
                b.height,
                b.plateau_id,
                b.source_dataset,
                ST_AsText(b.geom) as geometry_wkt,
                ARRAY_AGG(
                    json_build_object(
                        'osm_id', n.osm_id,
                        'lat', n.lat,
                        'lon', n.lon,
                        'sequence_id', n.sequence_id
                    ) ORDER BY n.sequence_id
                ) as nodes
            FROM plateau_buildings b
            LEFT JOIN plateau_building_nodes n ON b.id = n.building_id
            WHERE b.plateau_id = %s
            GROUP BY b.id, b.osm_id, b.building, b.height, b.plateau_id,
                     b.source_dataset, b.geom
        """, [plateau_id])

        building = cursor.fetchone()

        if not building:
            raise HTTPException(status_code=404, detail=f"Plateau ID {plateau_id} not found")

        building_dict = dict(building)

        if format == "json":
            # JSON形式で詳細情報を返す
            return {
                "plateau_id": plateau_id,
                "building": building_dict,
                "node_count": len(building_dict.get('nodes', [])),
                "nodes": building_dict.get('nodes', [])
            }
        else:
            # XML形式
            buildings = [building_dict]
            osm_xml = api.buildings_to_osm_xml(buildings)
            return Response(
                content=osm_xml,
                media_type="application/xml",
                headers={"Content-Type": "application/xml; charset=utf-8"}
            )

    except Exception as e:
        logger.error(f"Debug plateau error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/debug/compare-plateaus")
async def compare_plateaus(
    ids: str,  # カンマ区切りのPlateau ID
    format: str = "json"
):
    """複数のPlateau IDの建物を比較"""
    plateau_ids = [id.strip() for id in ids.split(',')]
    conn = api.get_connection()
    cursor = conn.cursor()

    try:
        results = []
        node_usage = {}  # ノードIDの使用状況を追跡

        for plateau_id in plateau_ids:
            cursor.execute("""
                SELECT DISTINCT ON (b.id)
                    b.id,
                    b.osm_id,
                    b.plateau_id,
                    b.source_dataset,
                    ARRAY_AGG(
                        json_build_object(
                            'node_id', n.osm_id,
                            'lat', n.lat,
                            'lon', n.lon,
                            'sequence_id', n.sequence_id
                        ) ORDER BY n.sequence_id
                    ) as nodes
                FROM plateau_buildings b
                LEFT JOIN plateau_building_nodes n ON b.id = n.building_id
                WHERE b.plateau_id = %s
                GROUP BY b.id, b.osm_id, b.plateau_id, b.source_dataset
            """, [plateau_id])

            building = cursor.fetchone()
            if building:
                building_dict = dict(building)

                # ノードIDの使用状況を記録
                for node in building_dict.get('nodes', []):
                    node_id = node.get('node_id')
                    if node_id:
                        if node_id not in node_usage:
                            node_usage[node_id] = []
                        node_usage[node_id].append({
                            'plateau_id': plateau_id,
                            'building_id': building_dict['id'],
                            'sequence': node.get('sequence_id')
                        })

                results.append({
                    'plateau_id': plateau_id,
                    'building_id': building_dict['id'],
                    'osm_id': building_dict['osm_id'],
                    'dataset': building_dict['source_dataset'],
                    'node_count': len(building_dict.get('nodes', [])),
                    'nodes': building_dict.get('nodes', [])
                })

        # 共有されているノードを特定
        shared_nodes = {
            node_id: usage
            for node_id, usage in node_usage.items()
            if len(usage) > 1
        }

        if format == "xml":
            # 比較用にXMLを出力 - すべての建物を一度に処理して重複を防ぐ
            all_buildings = []
            for plateau_id in plateau_ids:
                cursor.execute("""
                    SELECT b.*,
                           ARRAY_AGG(
                               json_build_object(
                                   'osm_id', n.osm_id,
                                   'lat', n.lat,
                                   'lon', n.lon,
                                   'sequence_id', n.sequence_id
                               ) ORDER BY n.sequence_id
                           ) as nodes
                    FROM plateau_buildings b
                    LEFT JOIN plateau_building_nodes n ON b.id = n.building_id
                    WHERE b.plateau_id = %s
                    GROUP BY b.id
                """, [plateau_id])
                building = cursor.fetchone()
                if building:
                    all_buildings.append(dict(building))

            # すべての建物を一度にXMLに変換（ノードIDの重複を防ぐ）
            combined_xml = api.buildings_to_osm_xml(all_buildings)
            return Response(
                content=combined_xml,
                media_type="text/plain",
                headers={"Content-Type": "text/plain; charset=utf-8"}
            )
        else:
            return {
                "buildings": results,
                "shared_nodes": shared_nodes,
                "summary": {
                    "total_buildings": len(results),
                    "total_unique_nodes": len(node_usage),
                    "shared_node_count": len(shared_nodes)
                }
            }

    except Exception as e:
        logger.error(f"Compare plateaus error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

# HEADメソッド対応
@app.head("/api/mapwithai/buildings")
async def head_buildings(
    bbox: str,
    limit: int = 1000,
    city: Optional[str] = None,
    use_intersects: bool = True
):
    """
    RapiD Editor用HEADリクエスト対応 - Content-Type事前確認用
    """
    try:
        # パラメータの簡単な検証のみ
        coords = [float(x.strip()) for x in bbox.split(',')]
        if len(coords) != 4:
            raise ValueError("bbox must have 4 coordinates")

        logger.info(f"🔍 HEADリクエスト - bbox: {bbox}, use_intersects: {use_intersects}")

        # bboxベースのETag生成
        etag_content = f"{bbox}-{use_intersects}-{limit}"
        etag = f'"{hashlib.md5(etag_content.encode()).hexdigest()}"'

        return Response(
            content="",  # HEADリクエストはボディなし
            status_code=200,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",

                # 改善されたキャッシュ制御
                "Cache-Control": "public, max-age=300",  # 5分間キャッシュ
                "ETag": etag,

                "X-API-Version": "2.1.0-flexible",
                "X-Data-Source": "Plateau Japan (MLIT)",
                "X-Content-Type-Options": "nosniff",
                "Accept-Ranges": "none"
            }
        )

    except ValueError as e:
        logger.error(f"❌ HEADリクエスト パラメータエラー: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid parameters: {str(e)}")
    except Exception as e:
        logger.error(f"❌ HEADリクエスト 内部エラー: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# OPTIONSリクエスト対応（CORS preflight）
@app.options("/api/mapwithai/buildings")
async def options_buildings():
    """CORS preflight リクエスト対応"""
    return Response(
        content="",
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
            "Access-Control-Max-Age": "86400",
            "Content-Type": "application/xml; charset=utf-8",
        }
    )

@app.get("/api/mapwithai/buildings")
async def get_buildings(
    bbox: str,
    limit: int = 1000,
    city: Optional[str] = None,
    use_intersects: bool = True
):
    """
    RapiD Editor互換の建物データ取得（OSM XML形式）- 柔軟な空間判定版

    Parameters:
        bbox: "min_lon,min_lat,max_lon,max_lat"形式の境界ボックス
        limit: 返す建物の最大数
        use_intersects: True=ST_Intersects使用（建物全体）、False=ST_Contains使用（重心のみ）
    """

    try:
        # bboxパラメータをパース
        coords = [float(x.strip()) for x in bbox.split(',')]
        if len(coords) != 4:
            raise ValueError("bbox must have 4 coordinates: min_lon,min_lat,max_lon,max_lat")

        min_lon, min_lat, max_lon, max_lat = coords

        # デバッグログ
        query_type = "ST_Intersects" if use_intersects else "ST_Contains(centroid)"
        logger.info(f"🎯 RapiD互換リクエスト（{query_type}） - bbox: {min_lon}, {min_lat}, {max_lon}, {max_lat}")

        # 境界値チェック
        if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
            raise ValueError("Longitude must be between -180 and 180")
        if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
            raise ValueError("Latitude must be between -90 and 90")
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ValueError("Invalid bbox: min values must be less than max values")

        # 建物データ取得
        buildings = api.get_buildings_in_bbox(
            min_lon, min_lat, max_lon, max_lat, limit, city,
            use_intersects=use_intersects
        )
        logger.info(f"🏢 データベースから取得（{query_type}）: {len(buildings)}件の建物")

        # 空データの場合の処理
        if not buildings:
            logger.info("📭 該当範囲に建物データなし - 空のOSMを返却")
            empty_osm = '''<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="osmfj-plateau-api-v2.1-flexible"></osm>'''

            return Response(
                content=empty_osm,
                media_type="application/xml",
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
                    "Cache-Control": "public, max-age=300",
                    "Content-Length": str(len(empty_osm.encode('utf-8'))),
                    "X-API-Version": "2.1.0-flexible",
                    "X-Data-Source": "Plateau Japan (MLIT)"
                }
            )

        # RapiD互換のOSM XML形式に変換
        osm_xml = api.buildings_to_osm_xml(buildings)

        # XMLの最終検証と修正
        try:
            # XML宣言の確認
            if not osm_xml.startswith('<?xml'):
                logger.warning("⚠️ XML宣言が不正 - 修正中")
                osm_xml = '<?xml version="1.0" encoding="UTF-8"?>\n' + osm_xml

            # BOMの除去
            if osm_xml.startswith('\ufeff'):
                logger.warning("⚠️ BOM検出 - 除去中")
                osm_xml = osm_xml[1:]

            # 制御文字の除去
            original_length = len(osm_xml)
            osm_xml = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', osm_xml)
            if len(osm_xml) != original_length:
                logger.warning(f"⚠️ 制御文字除去: {original_length - len(osm_xml)}文字")

            # XMLパースの最終確認
            ET.fromstring(osm_xml)
            logger.info("✅ XML構文チェック: 正常")

        except ET.ParseError as e:
            logger.error(f"❌ XML構文エラー: {e}")
            logger.error(f"❌ 問題のあるXML（最初の200文字）:\n{repr(osm_xml[:200])}")

            # フォールバック: 最小限のXMLを返す
            fallback_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6" generator="osmfj-plateau-api-fallback">
  <!-- XML生成エラーのため空のデータを返却 -->
</osm>'''

            return Response(
                content=fallback_xml,
                media_type="application/xml",
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "Access-Control-Allow-Origin": "*",
                    "X-API-Error": "XML generation failed",
                }
            )

        logger.info(f"🚀 RapiD互換API応答（{query_type}）: {len(buildings)}件の建物データをXML形式で配信 ({len(osm_xml)}文字)")

        # レスポンスヘッダーの最適化
        xml_bytes = osm_xml.encode('utf-8')

        # ETag生成（bbox + 設定ベース）
        etag_content = f"{bbox}-{use_intersects}-{limit}-{len(buildings)}"
        etag = f'"{hashlib.md5(etag_content.encode()).hexdigest()}"'

        return Response(
            content=xml_bytes,  # バイト形式で返却
            media_type="application/xml",
            headers={
                # XMLパース問題対策
                "Content-Type": "application/xml; charset=utf-8",

                # CORS ヘッダー
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",

                # 改善されたキャッシュ制御
                "Cache-Control": "public, max-age=300",  # 5分間キャッシュ
                "ETag": etag,

                # 条件付きリクエスト対応
                "Last-Modified": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),

                # Content-Length（バイト数）
                "Content-Length": str(len(xml_bytes)),

                # API識別用
                "X-API-Version": "2.1.0-flexible",
                "X-Data-Source": "Plateau Japan (MLIT)",
                "X-Query-Type": query_type,
                "X-Buildings-Count": str(len(buildings)),

                # XMLパース用ヘッダー
                "X-Content-Type-Options": "nosniff",
            }
        )

    except ValueError as e:
        logger.error(f"❌ パラメータエラー: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid parameters: {str(e)}")
    except Exception as e:
        logger.error(f"❌ API内部エラー: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    logger.info("🚀 OSMFJ Plateau API (Flexible) サーバー起動中...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
