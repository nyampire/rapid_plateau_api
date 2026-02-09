#!/usr/bin/env python3
"""
OSMFJ Plateau MapWithAI API - PostgreSQL/PostGIS版
日本のPlateau都市データをMapWithAI/RapiD形式で配信

機能:
- PostGISによる空間検索（ST_Intersects/ST_Contains切り替え可能）
- OSM XML形式でのデータ配信
- RapiDエディタとの互換性確保
"""

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import xml.etree.ElementTree as ET
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
    """Plateau建物データAPI"""

    def __init__(self, database_url: Optional[str] = None):
        if database_url is None:
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
            logger.info(f"PostgreSQL/PostGIS接続成功: {version}")

            cursor.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('plateau_buildings', 'plateau_building_nodes')
            """)
            tables = cursor.fetchall()
            logger.info(f"利用可能テーブル: {[table[0] for table in tables]}")
            conn.close()
        except Exception as e:
            logger.error(f"PostgreSQL接続失敗: {e}")
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
        PostGIS空間検索による建物取得

        Args:
            min_lon, min_lat, max_lon, max_lat: バウンディングボックス
            limit: 最大取得件数
            city: 市区町村フィルタ（未実装）
            use_intersects: True=ST_Intersects, False=ST_Contains(centroid)
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            if use_intersects:
                spatial_condition = """
                    ST_Intersects(
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326),
                        b.geom
                    )
                """
                distinct_key = "b.osm_id"
            else:
                spatial_condition = """
                    ST_Contains(
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326),
                        b.centroid
                    )
                """
                distinct_key = "MD5(ST_AsText(b.geom))"

            # LATERAL JOINで各建物のノードを個別に集約（GROUP BY不要）
            query = f"""
                WITH unique_buildings AS (
                    SELECT DISTINCT ON ({distinct_key})
                        b.id,
                        b.osm_id,
                        b.building,
                        b.height,
                        b.ele,
                        b.building_levels,
                        b.name,
                        b.addr_housenumber,
                        b.addr_street,
                        b.start_date,
                        b.building_material,
                        b.roof_material,
                        b.roof_shape,
                        b.amenity,
                        b.shop,
                        b.tourism,
                        b.leisure,
                        b.landuse
                    FROM plateau_buildings b
                    WHERE {spatial_condition}
                    ORDER BY {distinct_key}
                )
                SELECT
                    ub.id,
                    ub.osm_id,
                    ub.building,
                    ub.height,
                    ub.ele,
                    ub.building_levels,
                    ub.name,
                    ub.addr_housenumber,
                    ub.addr_street,
                    ub.start_date,
                    ub.building_material,
                    ub.roof_material,
                    ub.roof_shape,
                    ub.amenity,
                    ub.shop,
                    ub.tourism,
                    ub.leisure,
                    ub.landuse,
                    bn.nodes
                FROM unique_buildings ub
                LEFT JOIN LATERAL (
                    SELECT ARRAY_AGG(
                        json_build_object(
                            'id', n.id,
                            'osm_id', n.osm_id,
                            'lat', n.lat,
                            'lon', n.lon,
                            'sequence_id', n.sequence_id
                        ) ORDER BY n.sequence_id
                    ) as nodes
                    FROM plateau_building_nodes n
                    WHERE n.building_id = ub.id
                ) bn ON true
                ORDER BY ub.osm_id
                LIMIT %s
            """

            params = [min_lon, min_lat, max_lon, max_lat, limit]

            cursor.execute(query, params)
            buildings = cursor.fetchall()
            result = [dict(building) for building in buildings]

            logger.info(f"検索結果: {len(result)}件 (bbox: {min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f})")
            return result

        except Exception as e:
            logger.error(f"PostGIS query error: {e}")
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
        finally:
            conn.close()

    def buildings_to_osm_xml(self, buildings: List[Dict]) -> str:
        """建物データをOSM XML形式に変換"""
        osm = ET.Element('osm')
        osm.set('version', '0.6')
        osm.set('generator', 'osmfj-plateau-api')
        osm.set('copyright', 'Plateau Japan')
        osm.set('attribution', 'https://www.mlit.go.jp/plateau/')
        osm.set('license', 'https://www.mlit.go.jp/plateau/')

        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        all_nodes = []
        all_ways = []
        processed_buildings = 0
        total_nodes_created = 0

        for building in buildings:
            try:
                nodes = building.get('nodes', [])
                if not nodes or nodes == [None] or not any(nodes):
                    continue

                # 有効なノードをフィルタ（DBのIDを保持）
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

                # ポリゴン閉鎖チェック
                first_node = valid_nodes[0]
                last_node = valid_nodes[-1]
                is_closed = (abs(first_node['lat'] - last_node['lat']) < 1e-7 and
                            abs(first_node['lon'] - last_node['lon']) < 1e-7)
                if is_closed:
                    valid_nodes = valid_nodes[:-1]

                # Way要素作成（DBのIDを使用）
                building_db_id = building.get('id')
                way_elem = ET.Element('way')
                way_elem.set('id', str(-building_db_id))
                way_elem.set('visible', 'true')
                way_elem.set('version', '1')
                way_elem.set('changeset', '1')
                way_elem.set('timestamp', timestamp)
                way_elem.set('user', 'osmfj-plateau')
                way_elem.set('uid', '1')

                first_node_id = None

                for i, node_data in enumerate(valid_nodes):
                    node_db_id = -node_data['id']

                    # ノード要素作成
                    node_elem = ET.Element('node')
                    node_elem.set('id', str(node_db_id))
                    node_elem.set('visible', 'true')
                    node_elem.set('version', '1')
                    node_elem.set('changeset', '1')
                    node_elem.set('timestamp', timestamp)
                    node_elem.set('user', 'osmfj-plateau')
                    node_elem.set('uid', '1')
                    node_elem.set('lat', f"{node_data['lat']:.7f}")
                    node_elem.set('lon', f"{node_data['lon']:.7f}")

                    all_nodes.append(node_elem)

                    nd_elem = ET.SubElement(way_elem, 'nd')
                    nd_elem.set('ref', str(node_db_id))

                    if i == 0:
                        first_node_id = node_db_id

                    total_nodes_created += 1

                # ポリゴンを閉じる
                nd_elem = ET.SubElement(way_elem, 'nd')
                nd_elem.set('ref', str(first_node_id))

                # タグ追加
                def add_tag(parent, key, value):
                    if value is not None and str(value).strip():
                        tag_elem = ET.SubElement(parent, 'tag')
                        tag_elem.set('k', key)
                        tag_elem.set('v', str(value))

                add_tag(way_elem, 'building', building.get('building', 'yes'))
                if building.get('height'):
                    add_tag(way_elem, 'height', str(building['height']))
                if building.get('ele'):
                    add_tag(way_elem, 'ele', str(building['ele']))
                if building.get('building_levels'):
                    add_tag(way_elem, 'building:levels', str(building['building_levels']))
                if building.get('name'):
                    add_tag(way_elem, 'name', building['name'])
                if building.get('addr_housenumber'):
                    add_tag(way_elem, 'addr:housenumber', building['addr_housenumber'])
                if building.get('addr_street'):
                    add_tag(way_elem, 'addr:street', building['addr_street'])
                if building.get('start_date'):
                    add_tag(way_elem, 'start_date', building['start_date'])
                if building.get('building_material'):
                    add_tag(way_elem, 'building:material', building['building_material'])
                if building.get('roof_material'):
                    add_tag(way_elem, 'roof:material', building['roof_material'])
                if building.get('roof_shape'):
                    add_tag(way_elem, 'roof:shape', building['roof_shape'])
                if building.get('amenity'):
                    add_tag(way_elem, 'amenity', building['amenity'])
                if building.get('shop'):
                    add_tag(way_elem, 'shop', building['shop'])
                if building.get('tourism'):
                    add_tag(way_elem, 'tourism', building['tourism'])
                if building.get('leisure'):
                    add_tag(way_elem, 'leisure', building['leisure'])
                if building.get('landuse'):
                    add_tag(way_elem, 'landuse', building['landuse'])

                all_ways.append(way_elem)
                processed_buildings += 1

            except Exception as e:
                logger.warning(f"建物処理エラー {building.get('id', 'unknown')}: {e}")
                continue

        # OSM順序で要素追加: ノード → ウェイ
        for node in all_nodes:
            osm.append(node)
        for way in all_ways:
            osm.append(way)

        logger.info(f"XML生成完了: {processed_buildings}件, {total_nodes_created}ノード")

        try:
            xml_string = ET.tostring(osm, encoding='unicode', method='xml')
            xml_declaration = '<?xml version="1.0" encoding="UTF-8"?>'
            formatted_xml = xml_string.replace('><', '>\n<')
            return xml_declaration + '\n' + formatted_xml
        except Exception as e:
            logger.error(f"XML生成エラー: {e}")
            return '<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6" generator="osmfj-plateau-api-fallback"></osm>'

    def get_statistics(self) -> Dict[str, Any]:
        """データベース統計情報取得"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT
                    COUNT(*) as building_count,
                    COUNT(CASE WHEN height IS NOT NULL THEN 1 END) as buildings_with_height,
                    AVG(height) as avg_height,
                    MAX(height) as max_height
                FROM plateau_buildings
            """)
            building_stats = cursor.fetchone()

            cursor.execute("SELECT COUNT(*) as node_count FROM plateau_building_nodes")
            node_stats = cursor.fetchone()

            return {
                "buildings": {
                    "total": building_stats['building_count'],
                    "with_height": building_stats['buildings_with_height'],
                    "avg_height": float(building_stats['avg_height']) if building_stats['avg_height'] else None,
                    "max_height": float(building_stats['max_height']) if building_stats['max_height'] else None
                },
                "nodes": {
                    "total": node_stats['node_count']
                }
            }
        except Exception as e:
            logger.error(f"Statistics query error: {e}")
            return {"error": str(e)}
        finally:
            conn.close()


# FastAPIアプリケーション
app = FastAPI(
    title="OSMFJ Plateau API",
    description="日本のPlateau都市データをMapWithAI/RapiD形式で配信",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# APIインスタンス
api = OSMFJPlateauAPI()


@app.get("/")
async def root():
    """API情報"""
    return {
        "name": "OSMFJ Plateau API",
        "version": "3.0.0",
        "description": "Plateau建物データをOSM XML形式で配信",
        "endpoints": {
            "buildings": "/api/mapwithai/buildings?bbox=min_lon,min_lat,max_lon,max_lat",
            "statistics": "/api/stats"
        }
    }


@app.get("/health")
async def health_check():
    """ヘルスチェック"""
    return {"status": "healthy"}


@app.get("/api/stats")
async def get_statistics():
    """統計情報取得"""
    return api.get_statistics()


@app.options("/api/mapwithai/buildings")
async def options_buildings():
    """CORS preflight対応"""
    return Response(
        content="",
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With",
            "Access-Control-Max-Age": "86400",
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
    RapiD互換の建物データ取得（OSM XML形式）

    Parameters:
        bbox: "min_lon,min_lat,max_lon,max_lat"形式
        limit: 最大取得件数
        use_intersects: True=ST_Intersects, False=ST_Contains(centroid)
    """
    try:
        coords = [float(x.strip()) for x in bbox.split(',')]
        if len(coords) != 4:
            raise ValueError("bbox must have 4 coordinates")

        min_lon, min_lat, max_lon, max_lat = coords

        # バリデーション
        if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
            raise ValueError("Longitude must be between -180 and 180")
        if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
            raise ValueError("Latitude must be between -90 and 90")
        if min_lon >= max_lon or min_lat >= max_lat:
            raise ValueError("Invalid bbox: min values must be less than max values")

        buildings = api.get_buildings_in_bbox(
            min_lon, min_lat, max_lon, max_lat, limit, city,
            use_intersects=use_intersects
        )

        # 空データ
        if not buildings:
            empty_osm = '<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6" generator="osmfj-plateau-api"></osm>'
            return Response(
                content=empty_osm,
                media_type="application/xml",
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "Access-Control-Allow-Origin": "*",
                    "Cache-Control": "public, max-age=300",
                }
            )

        osm_xml = api.buildings_to_osm_xml(buildings)

        # XML検証
        try:
            if osm_xml.startswith('\ufeff'):
                osm_xml = osm_xml[1:]
            osm_xml = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', osm_xml)
            ET.fromstring(osm_xml)
        except ET.ParseError as e:
            logger.error(f"XML構文エラー: {e}")
            fallback_xml = '<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6" generator="osmfj-plateau-api-fallback"></osm>'
            return Response(
                content=fallback_xml,
                media_type="application/xml",
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "Access-Control-Allow-Origin": "*",
                }
            )

        xml_bytes = osm_xml.encode('utf-8')
        etag = f'"{hashlib.md5(f"{bbox}-{limit}-{len(buildings)}".encode()).hexdigest()}"'

        return Response(
            content=xml_bytes,
            media_type="application/xml",
            headers={
                "Content-Type": "application/xml; charset=utf-8",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                "Cache-Control": "public, max-age=300",
                "ETag": etag,
                "Content-Length": str(len(xml_bytes)),
                "X-Buildings-Count": str(len(buildings)),
            }
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid parameters: {str(e)}")
    except Exception as e:
        logger.error(f"API error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


if __name__ == "__main__":
    logger.info("OSMFJ Plateau API サーバー起動...")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
