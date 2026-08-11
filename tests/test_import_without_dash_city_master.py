"""Regression: import must actually land rows when `dash_city_master` is absent.

行政界 N03 フィルタ (`_apply_city_boundary_filter`) は「テーブルが無ければ
素通り」する設計だが、SELECT の失敗は PostgreSQL のトランザクションを
abort 状態にするため、素通りしたつもりでも直後の `conn.commit()` が
実質 ROLLBACK になり、**「成功ログを出して 0 件」** という最悪の失敗の仕方を
していた (2026-08-11 に 周南市 81 メッシュの取り込みで再現)。

ダッシュボード由来の `dash_city_master` が無い DB — 新規デプロイや
ダッシュボード無しの環境 — がその条件に該当する。

These tests require a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. They are skipped by default; run with
``pytest --run-integration``.
"""

import pytest


CITY_CODE = '13203'


def _square_wkt(lat: float, lon: float, size_deg: float = 0.0001) -> str:
    """Tiny square polygon WKT centered on (lat, lon)."""
    return (
        f"POLYGON(("
        f"{lon - size_deg} {lat - size_deg},"
        f"{lon + size_deg} {lat - size_deg},"
        f"{lon + size_deg} {lat + size_deg},"
        f"{lon - size_deg} {lat + size_deg},"
        f"{lon - size_deg} {lat - size_deg}"
        f"))"
    )


def _building_row(osm_id: int, lat: float, lon: float, city_code: str = CITY_CODE):
    """`insert_to_database_batch` が期待する 27 要素の buildings_data 行。

    レイアウトは `process_buildings_safe` の append と同じ順序
    (末尾 2 要素は geom / centroid 用の WKT)。
    """
    wkt = _square_wkt(lat, lon)
    return (
        osm_id,          # osm_id
        'yes',           # building
        10.0,            # height
        None,            # ele
        3,               # building_levels
        None,            # building_levels_underground
        'plateau',       # source_dataset
        f'plateau-{osm_id}',  # plateau_id
        wkt,             # geometry_wkt
        None, None, None, None,   # name, addr_full, addr_housenumber, addr_street
        None, None, None, None,   # start_date, building_material, roof_material, roof_shape
        None, None, None, None, None,  # amenity, shop, tourism, leisure, landuse
        city_code,       # city_code
        None,            # building_part
        None,            # ref_mlit_plateau
        wkt,             # geom 用 WKT
        wkt,             # centroid 用 WKT
    )


def _node_rows(building_osm_id: int, lat: float, lon: float, base_node_id: int):
    """building 1 件ぶんの nodes_data (osm_id, building_id, seq, lat, lon, lon, lat, ring_id)。

    `building_id` にはこの時点では building の osm_id が入り、
    importer 側で DB の id に differ 解決される。
    """
    d = 0.0001
    corners = [
        (lat - d, lon - d),
        (lat - d, lon + d),
        (lat + d, lon + d),
        (lat + d, lon - d),
    ]
    return [
        (base_node_id - i, building_osm_id, i, clat, clon, clon, clat, 0)
        for i, (clat, clon) in enumerate(corners)
    ]


def _count_buildings(conn, city_code=CITY_CODE) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM plateau_buildings WHERE city_code = %s",
            (city_code,),
        )
        return cur.fetchone()[0]


def _seed_boundary(conn, city_code: str, polygon_wkt: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dash_city_master (city_code, boundary_geom)
            VALUES (%s, ST_Multi(ST_GeomFromText(%s, 4326)))
            """,
            (city_code, polygon_wkt),
        )


@pytest.mark.integration
def test_batch_insert_persists_rows_without_dash_city_master(
    fresh_plateau_full_schema, integration_db_url, bare_importer, caplog
):
    """`dash_city_master` が無い DB でも投入行が commit される (成功ログ ≠ 0 件)。

    これが本ファイルの主眼: フィルタの SELECT 失敗がトランザクションを巻き添えに
    せず、pass-through の意図どおり建物が残ること。
    """
    conn = fresh_plateau_full_schema
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS dash_city_master CASCADE')

    lat, lon = 35.6890, 139.4855
    importer = bare_importer(citycode=CITY_CODE, postgres_url=integration_db_url)

    with caplog.at_level('WARNING'):
        ok = importer.insert_to_database_batch(
            [_building_row(-1001, lat, lon)],
            _node_rows(-1001, lat, lon, base_node_id=-2001),
        )

    assert ok is True
    # 素通りしたことがログに残る (握りつぶしの黙認ではない)
    assert any('行政界フィルタ' in r.message for r in caplog.records)
    # そして肝心の行が実在する
    assert _count_buildings(conn) == 1
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM plateau_building_nodes")
        assert cur.fetchone()[0] == 4


@pytest.mark.integration
def test_batch_insert_persists_rows_when_boundary_column_missing(
    fresh_plateau_full_schema, integration_db_url, bare_importer
):
    """テーブルはあるがカラムが欠けている DB でも投入行が残る。

    SAVEPOINT 方式を `to_regclass` によるテーブル存在チェックより優先した根拠が
    これ。存在チェックはテーブル不在しか救えず、スキーマのずれ
    (plateau_migrate.py が追加する city_code の欠落、PostGIS 未導入など) では
    存在チェックを通過した後で SELECT が落ち、同じ silent data loss になる。
    このテストが無いと、後日「素直に存在チェックへ簡略化」しても全テストが
    通ってしまい、設計判断ごと退行する。
    """
    conn = fresh_plateau_full_schema
    with conn.cursor() as cur:
        cur.execute('ALTER TABLE dash_city_master DROP COLUMN boundary_geom')

    lat, lon = 35.6890, 139.4855
    importer = bare_importer(citycode=CITY_CODE, postgres_url=integration_db_url)

    ok = importer.insert_to_database_batch(
        [_building_row(-1001, lat, lon)],
        _node_rows(-1001, lat, lon, base_node_id=-2001),
    )

    assert ok is True
    assert _count_buildings(conn) == 1


@pytest.mark.integration
def test_batch_insert_still_filters_when_master_present(
    fresh_plateau_full_schema, integration_db_url, bare_importer
):
    """テーブルがある通常環境では従来どおり境界外を削除する。

    pass-through を直したついでにフィルタ本体を無効化していないことの担保。
    """
    conn = fresh_plateau_full_schema
    inside_lat, inside_lon = 35.6890, 139.4855
    outside_lat, outside_lon = 35.9000, 139.9000
    # inside 側だけを含む行政界
    _seed_boundary(conn, CITY_CODE, _square_wkt(inside_lat, inside_lon, size_deg=0.01))

    importer = bare_importer(citycode=CITY_CODE, postgres_url=integration_db_url)
    ok = importer.insert_to_database_batch(
        [
            _building_row(-1001, inside_lat, inside_lon),
            _building_row(-1002, outside_lat, outside_lon),
        ],
        _node_rows(-1001, inside_lat, inside_lon, base_node_id=-2001)
        + _node_rows(-1002, outside_lat, outside_lon, base_node_id=-3001),
    )

    assert ok is True
    assert _count_buildings(conn) == 1
    with conn.cursor() as cur:
        cur.execute("SELECT osm_id FROM plateau_buildings")
        assert [r[0] for r in cur.fetchall()] == [-1001]
        # 境界外建物のノードは CASCADE で消える
        cur.execute("SELECT COUNT(*) FROM plateau_building_nodes")
        assert cur.fetchone()[0] == 4
