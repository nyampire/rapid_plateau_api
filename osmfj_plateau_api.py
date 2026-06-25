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

from plateau_coverage import CoverageManager

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
            else:
                spatial_condition = """
                    ST_Contains(
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326),
                        b.centroid
                    )
                """

            # Cross-city mesh duplicate guard (Rapid#35):
            # PLATEAU は都市別配布だが標準地域メッシュは複数 city にまたがる。
            # 共有メッシュ内の建物は両方の都市の bundle で別レコードとして取り込まれて
            # おり、bbox クエリでそのまま両方返してしまうと、ユーザ画面に同じ建物が
            # 微妙に違う形状・属性で 2 重に出る。
            # ここでは「建物の centroid が source city の N03 行政界
            # (dash_city_master.boundary_geom) に含まれるレコードだけ通す」フィルタ
            # を CTE の WHERE 句に重ねる。
            #   - boundary_geom IS NULL の city（特殊データセット 13999 / 27999 など）
            #     はフィルタ対象外、従来通り全件残す。
            #   - dash_city_master に行が無い city_code もフィルタしない（LEFT JOIN）。
            # 根本的な dedup は importer 修正 + 再 import (#35) で別途実施するが、
            # 当面は本フィルタが defense-in-depth として残る想定。
            city_boundary_filter = """
                AND NOT EXISTS (
                    SELECT 1 FROM dash_city_master m
                    WHERE m.city_code = b.city_code
                      AND m.boundary_geom IS NOT NULL
                      AND NOT ST_Contains(m.boundary_geom, b.centroid)
                )
            """

            # Cross-city duplicate dedup at API output (#31).
            # 入口の city_boundary_filter を通り抜けた重複（=両 city とも boundary
            # が centroid を含む / どちらも dash_city_master 行が無い等）を出口で
            # 1 件に畳む。dedup key は同一建物の判定に必要十分な 4 タプル。
            # tiebreaker は (1) N03 boundary に centroid を含む city を優先、
            # (2) smallest city_code (deterministic) の順。
            dedup_key = """
                ROUND(ST_X(b.centroid)::numeric, 6),
                ROUND(ST_Y(b.centroid)::numeric, 6),
                COALESCE(b.height::text, ''),
                COALESCE(b.building_levels::text, ''),
                COALESCE(b.building_part, '')
            """
            dedup_tiebreaker = """
                (CASE WHEN EXISTS (
                    SELECT 1 FROM dash_city_master m
                    WHERE m.city_code = b.city_code
                      AND m.boundary_geom IS NOT NULL
                      AND ST_Contains(m.boundary_geom, b.centroid)
                ) THEN 0 ELSE 1 END),
                b.city_code
            """

            # Phase 2: bbox 内の outline / simple を取得後、それらの parts も追加で取得。
            # さらに bbox 内の orphan part (relation 無しの building:part) も併せて返す。
            # LATERAL JOIN で各 building のノードを個別に集約（GROUP BY 不要）。
            query = f"""
                WITH bbox_outlines AS (
                    -- bbox 内の outline / simple (building_part IS NULL)
                    SELECT DISTINCT ON ({dedup_key})
                        b.id, b.osm_id, b.building, b.height, b.ele,
                        b.building_levels, b.name, b.addr_housenumber,
                        b.addr_street, b.start_date, b.building_material,
                        b.roof_material, b.roof_shape, b.amenity, b.shop,
                        b.tourism, b.leisure, b.landuse, b.building_part,
                        b.parent_building_id
                    FROM plateau_buildings b
                    WHERE {spatial_condition}
                      AND b.building_part IS NULL
                      {city_boundary_filter}
                    ORDER BY {dedup_key}, {dedup_tiebreaker}
                    LIMIT %s
                ),
                related_parts AS (
                    -- 上記 outline に紐づく part (bbox 内外を問わず全て)
                    SELECT
                        b.id, b.osm_id, b.building, b.height, b.ele,
                        b.building_levels, b.name, b.addr_housenumber,
                        b.addr_street, b.start_date, b.building_material,
                        b.roof_material, b.roof_shape, b.amenity, b.shop,
                        b.tourism, b.leisure, b.landuse, b.building_part,
                        b.parent_building_id
                    FROM plateau_buildings b
                    WHERE b.parent_building_id IN (SELECT id FROM bbox_outlines)
                ),
                orphan_parts AS (
                    -- bbox 内で relation 無しの building:part
                    SELECT
                        b.id, b.osm_id, b.building, b.height, b.ele,
                        b.building_levels, b.name, b.addr_housenumber,
                        b.addr_street, b.start_date, b.building_material,
                        b.roof_material, b.roof_shape, b.amenity, b.shop,
                        b.tourism, b.leisure, b.landuse, b.building_part,
                        b.parent_building_id
                    FROM plateau_buildings b
                    WHERE b.building_part = 'yes'
                      AND b.parent_building_id IS NULL
                      AND {spatial_condition}
                      {city_boundary_filter}
                ),
                all_buildings AS (
                    SELECT * FROM bbox_outlines
                    UNION
                    SELECT * FROM related_parts
                    UNION
                    SELECT * FROM orphan_parts
                )
                SELECT
                    ub.id, ub.osm_id, ub.building, ub.height, ub.ele,
                    ub.building_levels, ub.name, ub.addr_housenumber,
                    ub.addr_street, ub.start_date, ub.building_material,
                    ub.roof_material, ub.roof_shape, ub.amenity, ub.shop,
                    ub.tourism, ub.leisure, ub.landuse, ub.building_part,
                    ub.parent_building_id,
                    bn.nodes
                FROM all_buildings ub
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
            """

            params = [min_lon, min_lat, max_lon, max_lat, limit,
                      min_lon, min_lat, max_lon, max_lat]

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

    # OSM XML 上での relation ID の生成オフセット。
    # building の DB id は正値、way の出力 id は -building_db_id なので、
    # relation の id は更に -1_000_000 でオフセットして衝突を避ける。
    RELATION_ID_OFFSET = -1_000_000

    def _emit_building_tags(self, parent_elem, building: Dict, is_part: bool):
        """way / relation 共通のタグを追加するヘルパー。

        is_part=True の場合は `building:part=yes`、それ以外は `building=*`。
        どちらでも height / ele / building:levels / name / addr / 等を出力。
        """
        def add_tag(key, value):
            if value is not None and str(value).strip():
                tag_elem = ET.SubElement(parent_elem, 'tag')
                tag_elem.set('k', key)
                tag_elem.set('v', str(value))

        if is_part:
            add_tag('building:part', 'yes')
        else:
            add_tag('building', building.get('building', 'yes'))

        if building.get('height'):
            add_tag('height', str(building['height']))
        if building.get('ele'):
            add_tag('ele', str(building['ele']))
        if building.get('building_levels'):
            add_tag('building:levels', str(building['building_levels']))
        if building.get('name'):
            add_tag('name', building['name'])
        if building.get('addr_housenumber'):
            add_tag('addr:housenumber', building['addr_housenumber'])
        if building.get('addr_street'):
            add_tag('addr:street', building['addr_street'])
        if building.get('start_date'):
            add_tag('start_date', building['start_date'])
        if building.get('building_material'):
            add_tag('building:material', building['building_material'])
        if building.get('roof_material'):
            add_tag('roof:material', building['roof_material'])
        if building.get('roof_shape'):
            add_tag('roof:shape', building['roof_shape'])
        if building.get('amenity'):
            add_tag('amenity', building['amenity'])
        if building.get('shop'):
            add_tag('shop', building['shop'])
        if building.get('tourism'):
            add_tag('tourism', building['tourism'])
        if building.get('leisure'):
            add_tag('leisure', building['leisure'])
        if building.get('landuse'):
            add_tag('landuse', building['landuse'])

    def buildings_to_osm_xml(self, buildings: List[Dict]) -> str:
        """建物データを OSM XML 形式に変換 (Phase 2: relation 出力対応)

        - outline / simple building: <way building=*> を従来通り出力
        - part: <way building:part=yes> を出力 (PLATEAU LOD2 慣習)
        - outline が parts を持つ場合: <relation type=building> を生成し、
          outline のタグを duplicate (OSM Simple 3D Buildings 慣習・流派 A)
        - orphan part (parent 無し): <way> 単独で出力 (relation 無し)
        """
        osm = ET.Element('osm')
        osm.set('version', '0.6')
        osm.set('generator', 'osmfj-plateau-api')
        osm.set('copyright', 'Plateau Japan')
        osm.set('attribution', 'https://www.mlit.go.jp/plateau/')
        osm.set('license', 'https://www.mlit.go.jp/plateau/')

        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        all_nodes = []
        all_ways = []
        all_relations = []
        # parent_db_id → list of (way_id of part) — 後で relation を組むのに使う
        parts_by_parent_db_id: Dict[int, List[int]] = {}
        # parent_db_id → outline の building dict (タグ duplicate のため)
        outline_by_db_id: Dict[int, Dict] = {}
        # DB id of buildings that successfully emitted a way (失敗除外)
        emitted_db_ids = set()

        # Within a relation group (outline + its parts) we MUST emit a single
        # node element for each unique (lat, lon) and reuse its id from every
        # member way. Before this dedup, the importer stored separate rows in
        # plateau_building_nodes for outline and each part even at identical
        # coordinates, so the API used to emit 2-3 distinct nodes per corner —
        # editors then saw the corners as unshared and reported it as a bug
        # (Rapid#33). Scoping the dedup to *one relation* keeps cross-building
        # corner sharing (Phase 1, fixed at the importer level) untouched.
        # Key: relation-group id (outline's DB id for outlines/parts-with-parent,
        # the building's own id for orphan parts).
        # Value: { (lat, lon) → canonical node id (negative, the first one seen) }
        group_coord_to_nid: Dict[int, Dict[tuple, int]] = {}
        # Tracks which canonical node ids have already produced a <node> element
        # so duplicates from later ways simply reference the existing one.
        emitted_node_ids: set = set()

        def _coord_key(lat: float, lon: float) -> tuple:
            # Match the 7-decimal precision used in the output below so float
            # representation jitter never makes "same coordinate" look distinct.
            return (round(lat, 7), round(lon, 7))

        def _group_id_for(b: Dict) -> int:
            """Relation group: outline+parts share, orphan parts are their own group."""
            if b.get('building_part') == 'yes' and b.get('parent_building_id') is not None:
                return b['parent_building_id']
            return b.get('id')

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
                way_id = -building_db_id
                is_part = (building.get('building_part') == 'yes')
                parent_id = building.get('parent_building_id')

                way_elem = ET.Element('way')
                way_elem.set('id', str(way_id))
                way_elem.set('visible', 'true')
                way_elem.set('version', '1')
                way_elem.set('changeset', '1')
                way_elem.set('timestamp', timestamp)
                way_elem.set('user', 'osmfj-plateau')
                way_elem.set('uid', '1')

                first_node_id = None

                # Look up / register canonical node ids inside this relation
                # group. If outline was processed first, parts reuse its ids;
                # if a part was processed first, the outline reuses the part's
                # id — either way every member way at this coordinate ends up
                # pointing at the same <node>.
                group_id = _group_id_for(building)
                coord_map = group_coord_to_nid.setdefault(group_id, {})

                for i, node_data in enumerate(valid_nodes):
                    key = _coord_key(node_data['lat'], node_data['lon'])
                    canonical_id = coord_map.get(key)
                    if canonical_id is None:
                        canonical_id = -node_data['id']
                        coord_map[key] = canonical_id

                    if canonical_id not in emitted_node_ids:
                        emitted_node_ids.add(canonical_id)
                        node_elem = ET.Element('node')
                        node_elem.set('id', str(canonical_id))
                        node_elem.set('visible', 'true')
                        node_elem.set('version', '1')
                        node_elem.set('changeset', '1')
                        node_elem.set('timestamp', timestamp)
                        node_elem.set('user', 'osmfj-plateau')
                        node_elem.set('uid', '1')
                        node_elem.set('lat', f"{node_data['lat']:.7f}")
                        node_elem.set('lon', f"{node_data['lon']:.7f}")
                        all_nodes.append(node_elem)
                        total_nodes_created += 1

                    nd_elem = ET.SubElement(way_elem, 'nd')
                    nd_elem.set('ref', str(canonical_id))

                    if i == 0:
                        first_node_id = canonical_id

                # ポリゴンを閉じる
                nd_elem = ET.SubElement(way_elem, 'nd')
                nd_elem.set('ref', str(first_node_id))

                # タグ追加 (outline/simple vs part で異なる)
                self._emit_building_tags(way_elem, building, is_part)

                all_ways.append(way_elem)
                emitted_db_ids.add(building_db_id)
                # relation 構築の準備
                if is_part and parent_id is not None:
                    parts_by_parent_db_id.setdefault(parent_id, []).append(way_id)
                elif not is_part:
                    outline_by_db_id[building_db_id] = building

                processed_buildings += 1

            except Exception as e:
                logger.warning(f"建物処理エラー {building.get('id', 'unknown')}: {e}")
                continue

        # relation 生成: parts を持つ outline 1件 = 1 relation
        for parent_db_id, part_way_ids in parts_by_parent_db_id.items():
            outline = outline_by_db_id.get(parent_db_id)
            if outline is None:
                # outline が同じバッチに含まれていない (bbox 外などで除外された場合)
                # → relation を作らず、parts は単独 way として残す
                continue
            rel_elem = ET.Element('relation')
            rel_elem.set('id', str(self.RELATION_ID_OFFSET - parent_db_id))
            rel_elem.set('visible', 'true')
            rel_elem.set('version', '1')
            rel_elem.set('changeset', '1')
            rel_elem.set('timestamp', timestamp)
            rel_elem.set('user', 'osmfj-plateau')
            rel_elem.set('uid', '1')
            # outline メンバー
            outline_way_id = -parent_db_id
            m = ET.SubElement(rel_elem, 'member')
            m.set('type', 'way')
            m.set('ref', str(outline_way_id))
            m.set('role', 'outline')
            # part メンバー
            for part_way_id in sorted(part_way_ids, reverse=True):  # 出力安定化のためソート
                m = ET.SubElement(rel_elem, 'member')
                m.set('type', 'way')
                m.set('ref', str(part_way_id))
                m.set('role', 'part')
            # タグ: type=building + outline のタグを duplicate
            type_tag = ET.SubElement(rel_elem, 'tag')
            type_tag.set('k', 'type')
            type_tag.set('v', 'building')
            self._emit_building_tags(rel_elem, outline, is_part=False)

            all_relations.append(rel_elem)

        # OSM 順序で要素追加: node → way → relation
        for node in all_nodes:
            osm.append(node)
        for way in all_ways:
            osm.append(way)
        for rel in all_relations:
            osm.append(rel)

        logger.info(
            f"XML生成完了: {processed_buildings}件 (way: {len(all_ways)}, "
            f"relation: {len(all_relations)}), {total_nodes_created}ノード"
        )

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
                WHERE building_part IS NULL  -- outline/simple のみ集計
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

# --- Plateau 進捗ダッシュボード API (read-only /api/dashboard/*) を相乗りマウント ---
# 実体は rapid_plateau_dashboard の api/dashboard_api.py（同ディレクトリに配置してデプロイ）。
# 未配置でも本体APIは起動できるよう try/except。
try:
    from dashboard_api import router as dashboard_router
    app.include_router(dashboard_router)
    logging.info("mounted dashboard router at /api/dashboard")
except Exception as e:
    logging.warning(f"dashboard router not mounted: {e}")

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


@app.get("/api/mapwithai/coverage")
async def get_coverage():
    """
    Plateau対応エリアのGeoJSON FeatureCollection を返す

    マテリアライズドビュー plateau_coverage から
    都市単位の凸包ポリゴンを取得して返却。

    Returns:
        FeatureCollection (各Featureは1都市の凸包ポリゴン)
            properties: city_code, building_count

    キャッシュ:
        データはマテリアライズドビュー化済みのため高速。
        コンテンツは都市の追加・パージ時のみ変化する。
    """
    try:
        mgr = CoverageManager(api.database_url)
        geojson = mgr.get_coverage_geojson()
        return Response(
            content=__import__('json').dumps(geojson, ensure_ascii=False),
            media_type="application/json",
            headers={
                # クライアント側で長期キャッシュ（コンテンツ変化はまれ）
                "Cache-Control": "public, max-age=3600",
                "Access-Control-Allow-Origin": "*",
            },
        )
    except psycopg2.errors.UndefinedTable:
        raise HTTPException(
            status_code=503,
            detail="plateau_coverage view not initialized. Run: python plateau_coverage.py --init"
        )
    except Exception as e:
        logger.error(f"Coverage endpoint error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


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
