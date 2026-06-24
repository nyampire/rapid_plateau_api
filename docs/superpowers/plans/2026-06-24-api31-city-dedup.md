# API output dedup for cross-city duplicate buildings (#31) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When two adjacent municipalities both import the same building, return only one row from `/api/mapwithai/buildings`, deterministically picking the geographically correct city.

**Architecture:** SQL-level dedup inside the existing CTE in `PlateauAPI.get_buildings_in_bbox`. The `bbox_outlines` and `orphan_parts` CTEs gain a new `DISTINCT ON` key (centroid + height + levels + building_part) and an `ORDER BY` tiebreaker (N03-contained `city_code` first, then smallest `city_code`).

**Tech Stack:** Python 3, psycopg2, PostgreSQL 16 + PostGIS 3, FastAPI, pytest.

## Global Constraints

- Dedup key tuple (verbatim): `(ROUND(ST_X(centroid)::numeric, 6), ROUND(ST_Y(centroid)::numeric, 6), COALESCE(height::text, ''), COALESCE(building_levels::text, ''), COALESCE(building_part, ''))`
- Tiebreaker order: (1) row whose `city_code` has `dash_city_master.boundary_geom` containing the centroid; (2) smallest `city_code`
- Layer: SQL CTE only — no Python-side post-fetch filtering
- Tests: integration tests using `PLATEAU_TEST_DATABASE_URL` + new `fresh_plateau_full_schema` fixture; default `pytest` (no `--run-integration`) must continue to pass without a DB
- Single production file modified: `osmfj_plateau_api.py` (no migration, no API response shape change). Test files and `tests/conftest.py` may be added or extended.
- Log line shape (verbatim): `f"検索結果: {len(result)}件 (bbox: {min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f}, deduped: {raw_count - len(result)}件)"`

---

### Task 1: Test infrastructure — `fresh_plateau_full_schema` fixture + smoke test

**Files:**
- Modify: `tests/conftest.py` — add new fixture
- Create: `tests/test_dedup_city_duplicates.py` — new test file containing helpers + smoke test

**Interfaces:**
- Produces:
  - `fresh_plateau_full_schema` pytest fixture, yields an autocommit `psycopg2` connection with PostGIS extension enabled and `plateau_buildings`, `plateau_building_nodes`, `dash_city_master` tables present
  - `_square_wkt(lat, lon, size_deg=0.0001) -> str` returning a tiny square polygon WKT
  - `_seed_building(conn, *, osm_id, city_code, lat, lon, height=None, building_levels=None, building_part=None, parent_building_id=None) -> int` inserting one row and returning DB id
  - `_seed_city_boundary(conn, *, city_code, polygon_wkt: str) -> None` inserting one `dash_city_master` row

- [ ] **Step 1: Add the `fresh_plateau_full_schema` fixture**

Append to `tests/conftest.py` (after `fresh_plateau_schema`, before `mock_connection`):

```python
@pytest.fixture
def fresh_plateau_full_schema(integration_db_url):
    """Full schema with PostGIS for dedup integration tests.

    Drops and recreates ``plateau_buildings`` (full column set incl. ``geom``,
    ``centroid``, ``osm_id``, ``city_code``, ``height``, ``building_levels``,
    ``building_part``, ``parent_building_id``) plus ``plateau_building_nodes``
    and ``dash_city_master``. Requires PostGIS — unlike ``fresh_plateau_schema``
    which is intentionally PostGIS-free.

    Usage::

        @pytest.mark.integration
        def test_x(fresh_plateau_full_schema, integration_db_url):
            conn = fresh_plateau_full_schema
            # seed via _seed_building from test_dedup_city_duplicates.py
            api = PlateauAPI(database_url=integration_db_url)
            ...
    """
    import psycopg2
    conn = psycopg2.connect(integration_db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute('CREATE EXTENSION IF NOT EXISTS postgis')
        cur.execute('DROP TABLE IF EXISTS plateau_building_nodes CASCADE')
        cur.execute('DROP TABLE IF EXISTS plateau_buildings CASCADE')
        cur.execute('DROP TABLE IF EXISTS dash_city_master CASCADE')
        cur.execute('''
            CREATE TABLE plateau_buildings (
                id SERIAL PRIMARY KEY,
                osm_id BIGINT,
                building TEXT,
                height DOUBLE PRECISION,
                ele DOUBLE PRECISION,
                building_levels INTEGER,
                name TEXT,
                addr_housenumber TEXT,
                addr_street TEXT,
                start_date TEXT,
                building_material TEXT,
                roof_material TEXT,
                roof_shape TEXT,
                amenity TEXT,
                shop TEXT,
                tourism TEXT,
                leisure TEXT,
                landuse TEXT,
                city_code TEXT,
                building_part TEXT,
                parent_building_id INTEGER
                    REFERENCES plateau_buildings(id) ON DELETE CASCADE,
                geom geometry(Polygon, 4326),
                centroid geometry(Point, 4326)
            )
        ''')
        cur.execute('CREATE INDEX ON plateau_buildings USING GIST (geom)')
        cur.execute('CREATE INDEX ON plateau_buildings USING GIST (centroid)')
        cur.execute('''
            CREATE TABLE plateau_building_nodes (
                id SERIAL PRIMARY KEY,
                osm_id BIGINT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                sequence_id INTEGER,
                building_id INTEGER
                    REFERENCES plateau_buildings(id) ON DELETE CASCADE
            )
        ''')
        cur.execute('''
            CREATE TABLE dash_city_master (
                city_code TEXT PRIMARY KEY,
                boundary_geom geometry(MultiPolygon, 4326)
            )
        ''')
    yield conn
    conn.close()
```

- [ ] **Step 2: Create `tests/test_dedup_city_duplicates.py` with helpers and a smoke test**

```python
"""Integration tests for cross-city building dedup at API output (#31).

These tests require a real PostgreSQL + PostGIS instance reachable via
``PLATEAU_TEST_DATABASE_URL``. They are skipped by default; run with
``pytest --run-integration``.
"""
import logging

import pytest

from osmfj_plateau_api import PlateauAPI


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


def _seed_building(conn, *, osm_id, city_code, lat, lon,
                   height=None, building_levels=None,
                   building_part=None, parent_building_id=None):
    """Insert one plateau_buildings row, return its DB id."""
    wkt = _square_wkt(lat, lon)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO plateau_buildings
                (osm_id, building, height, building_levels, city_code,
                 building_part, parent_building_id, geom, centroid)
            VALUES (%s, 'yes', %s, %s, %s, %s, %s,
                    ST_GeomFromText(%s, 4326),
                    ST_Centroid(ST_GeomFromText(%s, 4326)))
            RETURNING id
            """,
            (osm_id, height, building_levels, city_code, building_part,
             parent_building_id, wkt, wkt),
        )
        return cur.fetchone()[0]


def _seed_city_boundary(conn, *, city_code, polygon_wkt):
    """Insert one dash_city_master row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dash_city_master (city_code, boundary_geom)
            VALUES (%s, ST_Multi(ST_GeomFromText(%s, 4326)))
            """,
            (city_code, polygon_wkt),
        )


@pytest.mark.integration
def test_fixture_round_trips_a_single_building(
    fresh_plateau_full_schema, integration_db_url
):
    """Smoke test: fixture and seed helpers wired up; PlateauAPI can read."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=1, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 1
```

- [ ] **Step 3: Verify the default test run still skips integration tests**

Run: `cd /Users/nyampire/git/rapid_plateau_api && pytest tests/test_dedup_city_duplicates.py -v`

Expected: `1 skipped` (because `--run-integration` is not set).

- [ ] **Step 4: Run the smoke test with the integration flag**

Prerequisite: `PLATEAU_TEST_DATABASE_URL` env var set to a writable PostGIS-enabled PostgreSQL. A local dev database (e.g. `createdb plateau_api_test && psql plateau_api_test -c 'CREATE EXTENSION postgis'`) is sufficient — the fixture drops and recreates tables on each test.

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py -v --run-integration`

Expected: `1 passed`. If it fails because the database is unreachable, fix the env var or DB, not the test.

- [ ] **Step 5: Commit**

```bash
cd /Users/nyampire/git/rapid_plateau_api
git add tests/conftest.py tests/test_dedup_city_duplicates.py
git commit -m "Add fresh_plateau_full_schema fixture for dedup integration tests (#31)"
```

---

### Task 2: Implement outline dedup in `bbox_outlines` CTE

**Files:**
- Modify: `osmfj_plateau_api.py:91-149` (the `bbox_outlines` CTE definition inside `get_buildings_in_bbox`)
- Modify: `tests/test_dedup_city_duplicates.py` — add 5 new behavioral tests

**Interfaces:**
- Consumes: `fresh_plateau_full_schema`, `_seed_building`, `_seed_city_boundary` from Task 1
- Produces: nothing new beyond modified SQL — same `PlateauAPI.get_buildings_in_bbox(...) -> List[Dict]` signature

- [ ] **Step 1: Write the 5 failing behavioral tests**

Append to `tests/test_dedup_city_duplicates.py`:

```python
@pytest.mark.integration
def test_dedup_picks_n03_contained_city(
    fresh_plateau_full_schema, integration_db_url
):
    """When city A's N03 contains the centroid and city B has no boundary row,
    both pass the input filter; dedup must keep city A."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    # 13206 boundary contains the centroid → passes city_boundary_filter
    _seed_city_boundary(conn, city_code='13206',
                        polygon_wkt=_square_wkt(lat, lon, size_deg=0.01))
    # 13214 has no dash_city_master row → also passes (LEFT-JOIN behavior)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 101  # 13206 wins via N03 priority


@pytest.mark.integration
def test_dedup_picks_smallest_city_code_when_neither_n03_contains(
    fresh_plateau_full_schema, integration_db_url
):
    """Neither city has a dash_city_master row → smallest city_code wins."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 102  # 13206 wins via smallest city_code


@pytest.mark.integration
def test_dedup_picks_smallest_city_code_when_both_n03_contain(
    fresh_plateau_full_schema, integration_db_url
):
    """Both boundaries overlap and contain the centroid → smallest city_code wins."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)
    inside = _square_wkt(lat, lon, size_deg=0.01)
    _seed_city_boundary(conn, city_code='13206', polygon_wkt=inside)
    _seed_city_boundary(conn, city_code='13214', polygon_wkt=inside)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 102


@pytest.mark.integration
def test_height_difference_preserves_both(
    fresh_plateau_full_schema, integration_db_url
):
    """Same centroid but different height → two distinct buildings → both kept."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13214',
                   lat=lat, lon=lon, height=23, building_levels=5)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert {r['osm_id'] for r in results} == {101, 102}


@pytest.mark.integration
def test_related_parts_follow_surviving_outline(
    fresh_plateau_full_schema, integration_db_url
):
    """When outline A is deduped out, A's children parts must also disappear."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    outline_a = _seed_building(conn, osm_id=101, city_code='13214',
                               lat=lat, lon=lon, height=22, building_levels=5)
    outline_b = _seed_building(conn, osm_id=102, city_code='13206',
                               lat=lat, lon=lon, height=22, building_levels=5)
    # Children: same shape per parent, offset slightly so they don't collide
    _seed_building(conn, osm_id=201, city_code='13214',
                   lat=lat + 0.00002, lon=lon, height=10, building_levels=3,
                   building_part='yes', parent_building_id=outline_a)
    _seed_building(conn, osm_id=202, city_code='13206',
                   lat=lat + 0.00002, lon=lon, height=10, building_levels=3,
                   building_part='yes', parent_building_id=outline_b)

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    # Surviving outline = 102 (13206 wins via smallest city_code).
    # 102's child = 202. Outline 101 and its child 201 are gone.
    osm_ids = {r['osm_id'] for r in results}
    assert osm_ids == {102, 202}
```

- [ ] **Step 2: Run the new tests, confirm they fail**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py -v --run-integration -k "dedup or height_difference or related_parts"`

Expected: 4 of 5 fail (`test_dedup_picks_n03_contained_city`, `test_dedup_picks_smallest_city_code_when_neither_n03_contains`, `test_dedup_picks_smallest_city_code_when_both_n03_contain`, `test_related_parts_follow_surviving_outline`) because the existing `DISTINCT ON ({distinct_key})` is by `b.osm_id` or `MD5(...)`, neither of which collapses two distinct osm_ids with identical geometry. `test_height_difference_preserves_both` should already pass under both existing and new logic.

If a test fails for an unexpected reason (e.g. schema mismatch), fix the test / fixture before changing production code.

- [ ] **Step 3: Modify `bbox_outlines` CTE in `osmfj_plateau_api.py`**

Open `osmfj_plateau_api.py`. Replace lines 91-149 (the block from `if use_intersects:` through the `LIMIT %s\n            ),`) with:

```python
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
```

(Leave `related_parts`, `orphan_parts`, `all_buildings`, and the final SELECT/LATERAL block from line 150 onward UNCHANGED for this task — orphan_parts is handled in Task 3.)

- [ ] **Step 4: Run the failing tests, confirm they now pass**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py -v --run-integration`

Expected: all 6 currently-defined integration tests pass (smoke + 5 new).

- [ ] **Step 5: Run the full test suite (no integration) to confirm no regressions**

Run: `cd /Users/nyampire/git/rapid_plateau_api && pytest -v`

Expected: all unit tests pass; integration tests skipped.

If `tests/test_buildings_xml.py` complains about the changed SQL shape (it asserts substrings like `'dash_city_master' in sql`), inspect the assertion. The reference to `dash_city_master` should still be present in the new query (it appears in both `city_boundary_filter` and `dedup_tiebreaker`), so the substring test should keep passing. If a more specific count assertion (e.g. `sql.count('FROM dash_city_master m') == 2`) breaks because the count is now 4 (2 filter + 2 dedup tiebreaker — one for bbox_outlines, one to be added in Task 3 for orphan_parts), update the assertion to match. Document the change in the commit message.

- [ ] **Step 6: Commit**

```bash
cd /Users/nyampire/git/rapid_plateau_api
git add osmfj_plateau_api.py tests/test_dedup_city_duplicates.py
git commit -m "Dedup cross-city duplicate outlines at API output (#31)"
```

---

### Task 3: Implement orphan_parts dedup

**Files:**
- Modify: `osmfj_plateau_api.py` — `orphan_parts` CTE inside the same `query = f"""..."""` block
- Modify: `tests/test_dedup_city_duplicates.py` — append 1 new test

**Interfaces:**
- Consumes: helpers and fixture from Task 1; `dedup_key` and `dedup_tiebreaker` Python f-string variables introduced in Task 2
- Produces: nothing new beyond modified SQL

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dedup_city_duplicates.py`:

```python
@pytest.mark.integration
def test_orphan_parts_dedup(
    fresh_plateau_full_schema, integration_db_url
):
    """Orphan building:part rows (parent_building_id IS NULL) must also be
    deduped across cities by the same key + tiebreaker as outlines."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=301, city_code='13214',
                   lat=lat, lon=lon, height=10, building_levels=3,
                   building_part='yes')
    _seed_building(conn, osm_id=302, city_code='13206',
                   lat=lat, lon=lon, height=10, building_levels=3,
                   building_part='yes')

    api = PlateauAPI(database_url=integration_db_url)
    results = api.get_buildings_in_bbox(
        lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
    )

    assert len(results) == 1
    assert results[0]['osm_id'] == 302  # 13206 wins via smallest city_code
```

- [ ] **Step 2: Run, confirm it fails**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py::test_orphan_parts_dedup -v --run-integration`

Expected: FAIL with `assert len(results) == 1` (got 2).

- [ ] **Step 3: Modify the `orphan_parts` CTE**

In `osmfj_plateau_api.py`, locate the `orphan_parts` CTE inside `query` (it currently reads):

```python
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
```

Replace with:

```python
                orphan_parts AS (
                    -- bbox 内で relation 無しの building:part
                    -- outline と同じ dedup key + tiebreaker で出口防御 (#31)
                    SELECT DISTINCT ON ({dedup_key})
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
                    ORDER BY {dedup_key}, {dedup_tiebreaker}
                ),
```

- [ ] **Step 4: Run the test, confirm it now passes**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py::test_orphan_parts_dedup -v --run-integration`

Expected: PASS.

- [ ] **Step 5: Run all dedup tests + full suite**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py -v --run-integration && pytest -v`

Expected: all dedup tests pass; full no-integration suite passes.

If `tests/test_buildings_xml.py` has a `sql.count('FROM dash_city_master m')` assertion, it must now expect `4` (was `2` before Task 2: city_boundary_filter for bbox_outlines + orphan_parts; Task 2 added `dedup_tiebreaker` to bbox_outlines = +1; Task 3 added it to orphan_parts = +1). Update if needed.

- [ ] **Step 6: Commit**

```bash
cd /Users/nyampire/git/rapid_plateau_api
git add osmfj_plateau_api.py tests/test_dedup_city_duplicates.py
git commit -m "Dedup cross-city duplicate orphan building:part rows at API output (#31)"
```

---

### Task 4: Log deduped count for observability

**Files:**
- Modify: `osmfj_plateau_api.py` — extend the existing `logger.info("検索結果: ...")` line at ~line 216
- Modify: `tests/test_dedup_city_duplicates.py` — append 1 test using `caplog`

**Interfaces:**
- Produces: log line shape (verbatim from Global Constraints):
  `f"検索結果: {len(result)}件 (bbox: {min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f}, deduped: {raw_count - len(result)}件)"`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_dedup_city_duplicates.py`:

```python
@pytest.mark.integration
def test_logs_deduped_count(
    fresh_plateau_full_schema, integration_db_url, caplog
):
    """The info log line must include the deduped count, derived from
    COUNT(*) OVER () minus the returned row count."""
    conn = fresh_plateau_full_schema
    lat, lon = 35.6890, 139.4855
    _seed_building(conn, osm_id=101, city_code='13214',
                   lat=lat, lon=lon, height=22, building_levels=5)
    _seed_building(conn, osm_id=102, city_code='13206',
                   lat=lat, lon=lon, height=22, building_levels=5)

    api = PlateauAPI(database_url=integration_db_url)
    with caplog.at_level(logging.INFO, logger='osmfj_plateau_api'):
        api.get_buildings_in_bbox(
            lon - 0.005, lat - 0.005, lon + 0.005, lat + 0.005, limit=100,
        )

    info_msgs = [r.message for r in caplog.records if r.levelno == logging.INFO]
    matched = [m for m in info_msgs if 'deduped' in m and '1件' in m]
    assert matched, f"expected a 'deduped: 1件' info log, got: {info_msgs!r}"
```

- [ ] **Step 2: Run, confirm it fails**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py::test_logs_deduped_count -v --run-integration`

Expected: FAIL (`deduped` substring not in log).

- [ ] **Step 3: Add `raw_count` window column to the inner CTE and the outer SELECT**

In `osmfj_plateau_api.py`, change the `bbox_outlines` SELECT column list (after the change from Task 2) to also include the window count. The `bbox_outlines` CTE's SELECT list becomes:

```python
                bbox_outlines AS (
                    -- bbox 内の outline / simple (building_part IS NULL)
                    SELECT DISTINCT ON ({dedup_key})
                        b.id, b.osm_id, b.building, b.height, b.ele,
                        b.building_levels, b.name, b.addr_housenumber,
                        b.addr_street, b.start_date, b.building_material,
                        b.roof_material, b.roof_shape, b.amenity, b.shop,
                        b.tourism, b.leisure, b.landuse, b.building_part,
                        b.parent_building_id,
                        COUNT(*) OVER () AS pre_dedup_count
                    FROM plateau_buildings b
                    WHERE {spatial_condition}
                      AND b.building_part IS NULL
                      {city_boundary_filter}
                    ORDER BY {dedup_key}, {dedup_tiebreaker}
                    LIMIT %s
                ),
```

Mirror the change in `related_parts` and `orphan_parts` so the UNION column shapes match — add `0 AS pre_dedup_count` to both (we only count outline candidates for the log; parts are out of scope for the metric):

```python
                related_parts AS (
                    SELECT
                        b.id, b.osm_id, b.building, b.height, b.ele,
                        b.building_levels, b.name, b.addr_housenumber,
                        b.addr_street, b.start_date, b.building_material,
                        b.roof_material, b.roof_shape, b.amenity, b.shop,
                        b.tourism, b.leisure, b.landuse, b.building_part,
                        b.parent_building_id,
                        0 AS pre_dedup_count
                    FROM plateau_buildings b
                    WHERE b.parent_building_id IN (SELECT id FROM bbox_outlines)
                ),
                orphan_parts AS (
                    SELECT DISTINCT ON ({dedup_key})
                        b.id, b.osm_id, b.building, b.height, b.ele,
                        b.building_levels, b.name, b.addr_housenumber,
                        b.addr_street, b.start_date, b.building_material,
                        b.roof_material, b.roof_shape, b.amenity, b.shop,
                        b.tourism, b.leisure, b.landuse, b.building_part,
                        b.parent_building_id,
                        0 AS pre_dedup_count
                    FROM plateau_buildings b
                    WHERE b.building_part = 'yes'
                      AND b.parent_building_id IS NULL
                      AND {spatial_condition}
                      {city_boundary_filter}
                    ORDER BY {dedup_key}, {dedup_tiebreaker}
                ),
```

In the `all_buildings AS (...)` and the outer SELECT, propagate `pre_dedup_count`:

```python
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
                    ub.pre_dedup_count,
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
```

- [ ] **Step 4: Compute and log the deduped count in Python**

In the same method, change the result-processing block (currently lines ~213-217) to:

```python
            cursor.execute(query, params)
            buildings = cursor.fetchall()
            result = [dict(building) for building in buildings]

            # Compute deduped count from the window column.
            # The max() picks any non-zero outline row's count; if there were
            # zero outlines (only orphan parts or empty), default to 0.
            raw_count = max(
                (r.get('pre_dedup_count', 0) or 0) for r in result
            ) if result else 0
            # The window count includes both DISTINCT collapsing and any LIMIT
            # truncation; in practice LIMIT rarely fires for typical bboxes.
            deduped = max(0, raw_count - len(result))
            # Strip the internal column from the response so the API output
            # shape is unchanged.
            for r in result:
                r.pop('pre_dedup_count', None)

            logger.info(
                f"検索結果: {len(result)}件 "
                f"(bbox: {min_lon:.4f},{min_lat:.4f},{max_lon:.4f},{max_lat:.4f}, "
                f"deduped: {deduped}件)"
            )
            return result
```

- [ ] **Step 5: Run the log test, confirm it passes**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py::test_logs_deduped_count -v --run-integration`

Expected: PASS — log contains `deduped: 1件`.

- [ ] **Step 6: Run all dedup tests + full suite**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest tests/test_dedup_city_duplicates.py -v --run-integration && pytest -v`

Expected: all dedup tests pass (8 total: 1 smoke + 5 outline + 1 orphan + 1 log); full no-integration suite passes.

Pay attention to `tests/test_osmfj_plateau_api.py` and `tests/test_buildings_xml.py`: if any test inspects the response dict keys, the API response shape is unchanged (we strip `pre_dedup_count`) — those tests should still pass. If they fail, the strip step is the place to debug.

- [ ] **Step 7: Commit**

```bash
cd /Users/nyampire/git/rapid_plateau_api
git add osmfj_plateau_api.py tests/test_dedup_city_duplicates.py
git commit -m "Log deduped count from bbox query window function (#31)"
```

---

### Task 5: Manual pre-deploy validation

**Files:** none modified.

**Interfaces:** none.

- [ ] **Step 1: Run the full suite one more time, locally**

Run: `cd /Users/nyampire/git/rapid_plateau_api && pytest -v && PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest -v --run-integration`

Expected: both runs green.

- [ ] **Step 2: Push the branch and open a PR**

```bash
cd /Users/nyampire/git/rapid_plateau_api
git push origin HEAD
gh pr create --title "Dedup cross-city duplicate buildings at API output (#31)" \
    --body-file - <<'EOF'
Closes #31.

Adds an out-the-door dedup layer to `/api/mapwithai/buildings` so the same
building never appears twice when adjacent municipalities both imported it.

- Key: `(round(centroid_x, 6), round(centroid_y, 6), height, building_levels, building_part)`
- Tiebreaker: the `city_code` whose N03 boundary contains the centroid; smallest `city_code` otherwise
- Applied to both `bbox_outlines` and `orphan_parts` CTEs
- New `deduped: N件` field on the info log line for observability

New integration tests (require `PLATEAU_TEST_DATABASE_URL`) cover all four
tiebreaker branches plus the part-rides-along-with-surviving-outline invariant
and the log shape. CI continues to skip integration tests.
EOF
```

- [ ] **Step 3: Verify against an affected production bbox**

The issue references coordinates `35.689363, 139.485579` near a 13206 / 13214 boundary. After deploy:

```bash
curl -sG 'https://rapid.nyampire.info/api/mapwithai/buildings' \
    --data-urlencode 'bbox=139.4853,35.6891,139.4859,35.6896' \
    | grep -c 'osm_id="-' # count emitted ways
```

Before the change this returns the duplicate (two ways with identical geometry); after, only one. If duplicates remain, capture the response and reopen the issue with the failing case.

- [ ] **Step 4: Inspect a production log line for the new `deduped:` field**

After the deploy, exercise the same bbox once and confirm the API log records a `deduped: N件` value (N ≥ 1 if duplicates were present, 0 otherwise). Log access is environment-specific and out of scope for this plan.

---

## Self-review

**Spec coverage:**

- Dedup key tuple — Task 2 Step 3 (`dedup_key` block) ✓
- Tiebreaker (N03 then smallest city_code) — Task 2 Step 3 (`dedup_tiebreaker` block) ✓
- SQL layer only — all SQL changes live in `osmfj_plateau_api.py` CTEs; no Python post-fetch filtering ✓
- Defense in depth (do not replace `city_boundary_filter`) — `city_boundary_filter` is preserved verbatim; dedup runs in addition ✓
- Apply to orphan_parts — Task 3 ✓
- Logging shape — Task 4 Step 4 matches Global Constraints log line verbatim ✓
- Edge cases (NULL height, NULL boundary_geom, etc.) — covered by `COALESCE(...::text, '')` in `dedup_key` and by `boundary_geom IS NOT NULL AND ST_Contains(...)` in tiebreaker; tested via `test_dedup_picks_smallest_city_code_when_neither_n03_contains` (no row at all) and the `height_difference_preserves_both` test ✓
- Integration tests via `PLATEAU_TEST_DATABASE_URL` + fresh schema fixture — Task 1 ✓
- CI continues to pass without DB — Task 1 Step 3 verifies this explicitly ✓
- API response shape unchanged — Task 4 Step 4 strips `pre_dedup_count` from results before returning ✓
- Single file production change — confirmed: only `osmfj_plateau_api.py` ✓
- Rollback via `git revert` — implicit (no migration, no schema change) ✓

**Type consistency check:**

- `dedup_key` and `dedup_tiebreaker` Python f-string snippets are defined once in Task 2 and reused by name in Task 3 — names match ✓
- `pre_dedup_count` column added in Task 4 — referenced as `r.get('pre_dedup_count', 0)` in the Python and `pop('pre_dedup_count', None)` to strip — same name ✓
- `_seed_building` parameter names (`osm_id`, `city_code`, `lat`, `lon`, `height`, `building_levels`, `building_part`, `parent_building_id`) match column names used in the SQL ✓

**Placeholder scan:** no TBD / TODO / "implement appropriately" / "similar to" strings.

**Gaps:** none — every spec requirement maps to a task and step.
