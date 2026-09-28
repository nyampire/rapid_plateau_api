# 高さの警告（API 側）実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** API が建物を返すときに高さの怪しさを判定し、該当した建物に目印のタグを添えます。

**Architecture:** 判定は新しいモジュール `plateau_height_warning.py` の純粋な関数で行います。
`get_buildings_in_bbox` の問い合わせに面積の列を 2 つ足し、取り出した行に判定の結果を書き込みます。
`_emit_building_tags` が結果をタグとして出力します。

**Tech Stack:** Python 3、FastAPI、psycopg2、PostgreSQL 17 + PostGIS 3.6（試験用）、pytest。

設計文書: `docs/superpowers/specs/2026-09-28-height-warning-design.md`

## Global Constraints

- 閾値は `plateau_height_outliers.py` の `DEFAULT_` 定数を読み込んで使います。値を書き写しません。
- 警告にする検査は次のとおりです。`degenerate-area`、`part-over-outline`、`needle` はすべて、`absolute` は高さ 1.0 m 未満だけ、`floor-height` は `building` が `house`、`apartments`、`residential` のものだけです。
- 目印のタグは `plateau:height_warning` です。値は検査の名前を `CHECKS` の並びで `;` でつないだものです。
- `needle` に該当したときだけ、`plateau:footprint_m2` に底面積（平方メートル、小数 4 桁）を添えます。
- 球面上の面積は、平面での面積が正のときだけ計算します。
- 本番への反映は、エディタの変更を公開した後に行います。この計画には本番への反映を含めません。
- コメント、docstring、コミットメッセージは日本語のですます調で、1 行に 1 文です。
- `git add -A` は使いません。ファイルを明示して `git add` します。
- 統合試験は `LC_ALL=C PLATEAU_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/plateau_test python3 -m pytest --run-integration` で実行します。手元の PostgreSQL 17 と PostGIS で、空のデータベース `plateau_test` を作り、`CREATE EXTENSION postgis;` を済ませておきます。

---

### Task 1: 判定の関数

**Files:**
- Create: `plateau_height_warning.py`
- Test: `tests/test_plateau_height_warning.py`

**Interfaces:**
- Consumes: `plateau_height_outliers.CHECKS` と `DEFAULT_` 定数。
- Produces:
  - `height_warnings(row: Dict, outline: Optional[Dict] = None) -> List[str]`
  - `annotate_height_warnings(rows: Iterable[Dict]) -> None`（各行に `row['height_warnings']` を書き込みます）
  - `RESIDENTIAL_BUILDING_VALUES: frozenset`
  - 行の辞書で使うキー: `id`、`parent_building_id`、`building`、`height`、`building_levels`、`planar_area`、`footprint_m2`

- [ ] **Step 1: 失敗する試験を書く**

`tests/test_plateau_height_warning.py`:

```python
"""plateau_height_warning.py の試験

API が返す行 1 件ごとの判定を確かめる。
DB を使わない。
"""

import pytest

from plateau_height_warning import (
    annotate_height_warnings, height_warnings,
)


def _row(**kwargs):
    """判定に関係しない列を既定値で埋めた行。"""
    row = {
        'id': 1, 'parent_building_id': None, 'building': 'yes',
        'height': 7.0, 'building_levels': None,
        'planar_area': 1e-8, 'footprint_m2': None,
    }
    row.update(kwargs)
    return row


def test_ordinary_building_has_no_warning():
    assert height_warnings(_row()) == []


@pytest.mark.parametrize('area, expected', [
    (0.0, ['degenerate-area']),
    (-2.4e-08, ['degenerate-area']),
    (1e-9, []),
    (None, []),
])
def test_degenerate_area_is_zero_or_negative_planar_area(area, expected):
    assert height_warnings(_row(planar_area=area)) == expected


def test_part_over_outline_needs_more_than_the_tolerance():
    outline = _row(id=1, height=9.0)
    over = _row(id=2, parent_building_id=1, height=19.5)
    at_limit = _row(id=3, parent_building_id=1, height=19.0)

    assert height_warnings(over, outline) == ['part-over-outline']
    assert height_warnings(at_limit, outline) == []


def test_part_over_outline_is_skipped_without_both_heights():
    outline = _row(id=1, height=None)
    part = _row(id=2, parent_building_id=1, height=150.0)
    assert height_warnings(part, outline) == []
    assert height_warnings(part, None) == []


@pytest.mark.parametrize('height, area, expected', [
    (18.4, 0.0036, ['needle']),
    (15.0, 1.0, []),       # 高さはちょうど 15 m では対象外
    (18.4, 20.0, []),      # 底面積はちょうど 20 m² では対象外
    (18.4, None, []),      # 底面積を計算していない行
])
def test_needle(height, area, expected):
    assert height_warnings(_row(height=height, footprint_m2=area)) == expected


@pytest.mark.parametrize('height, expected', [
    (0.5, ['absolute']),
    (0.99, ['absolute']),
    (1.0, []),
    (250.0, []),           # 上側は警告にしない
    (None, []),
])
def test_absolute_warns_only_on_the_low_side(height, expected):
    assert height_warnings(_row(height=height)) == expected


@pytest.mark.parametrize('building, height, levels, expected', [
    ('house', 24.6, 2, ['floor-height']),       # 12.3 m / 階
    ('apartments', 2.97, 29, ['floor-height']),  # 0.10 m / 階
    ('residential', 20.4, 2, []),                # ちょうど 10.2 m / 階
    ('house', 3.0, 2, []),                       # ちょうど 1.5 m / 階
    ('industrial', 24.6, 2, []),                 # 住宅以外は警告にしない
    ('house', 24.6, 0, []),
    ('house', 24.6, None, []),
])
def test_floor_height_warns_only_for_residential(
    building, height, levels, expected
):
    row = _row(building=building, height=height, building_levels=levels)
    assert height_warnings(row) == expected


def test_warnings_follow_the_order_of_the_survey_checks():
    row = _row(
        planar_area=0.0, height=0.5, building='house', building_levels=3,
    )
    assert height_warnings(row) == [
        'degenerate-area', 'absolute', 'floor-height'
    ]


def test_height_given_as_a_string_is_read_as_a_number():
    assert height_warnings(_row(height='0.5')) == ['absolute']


def test_annotate_finds_the_outline_among_the_rows():
    outline = _row(id=10, height=9.1)
    part = _row(id=11, parent_building_id=10, height=149.2)
    orphan = _row(id=12, parent_building_id=99, height=149.2)

    annotate_height_warnings([outline, part, orphan])

    assert outline['height_warnings'] == []
    assert part['height_warnings'] == ['part-over-outline']
    assert orphan['height_warnings'] == []
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `python3 -m pytest -q tests/test_plateau_height_warning.py`
Expected: FAIL（`ModuleNotFoundError: No module named 'plateau_height_warning'`）

- [ ] **Step 3: 最小の実装を書く**

`plateau_height_warning.py`:

```python
#!/usr/bin/env python3
"""PLATEAU の建物の高さが怪しいかを、API の応答 1 件ごとに判定します。

判定の条件の値は、plateau_height_outliers.py の DEFAULT_ 定数を使います。
調査と警告で閾値がずれないようにするためです。

調査は SQL で全件を数えます。
ここでは、API が取り出した行を Python で判定します。

警告にするのは、調査の検査のうち誤りの可能性が高いものだけです。
degenerate-area、part-over-outline、needle はすべて警告にします。
absolute は、高さが低すぎる側だけを警告にします。
floor-height は、住宅だけを警告にします。
"""

from typing import Dict, Iterable, List, Optional

from plateau_height_outliers import (
    CHECKS,
    DEFAULT_ABSOLUTE_MIN_M,
    DEFAULT_FLOOR_HEIGHT_MAX_M,
    DEFAULT_FLOOR_HEIGHT_MIN_M,
    DEFAULT_NEEDLE_AREA_M2,
    DEFAULT_NEEDLE_HEIGHT_M,
    DEFAULT_PART_TOLERANCE_M,
)

# floor-height を警告にする building の値です。
# 工場や倉庫は 1 階が高いことが多く、正常な建物に警告が出てしまうため外します。
RESIDENTIAL_BUILDING_VALUES = frozenset({'house', 'apartments', 'residential'})


def _num(value) -> Optional[float]:
    """数として読めない値を None にします。"""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def height_warnings(row: Dict, outline: Optional[Dict] = None) -> List[str]:
    """1 件の建物について、該当した検査の名前を返します。

    row は get_buildings_in_bbox が返す行です。
    planar_area は平面での面積、footprint_m2 は平方メートルでの底面積です。
    outline は、row が部分立体のときの外形の行です。
    名前は CHECKS の並びで返します。
    """
    found = set()
    height = _num(row.get('height'))

    planar_area = _num(row.get('planar_area'))
    if planar_area is not None and planar_area <= 0:
        found.add('degenerate-area')

    if outline is not None:
        outline_height = _num(outline.get('height'))
        if (height is not None and outline_height is not None
                and height > outline_height + DEFAULT_PART_TOLERANCE_M):
            found.add('part-over-outline')

    footprint = _num(row.get('footprint_m2'))
    if (height is not None and footprint is not None
            and height > DEFAULT_NEEDLE_HEIGHT_M
            and footprint < DEFAULT_NEEDLE_AREA_M2):
        found.add('needle')

    if height is not None and height < DEFAULT_ABSOLUTE_MIN_M:
        found.add('absolute')

    levels = _num(row.get('building_levels'))
    if (row.get('building') in RESIDENTIAL_BUILDING_VALUES
            and height is not None and levels is not None and levels > 0):
        per_floor = height / levels
        if (per_floor < DEFAULT_FLOOR_HEIGHT_MIN_M
                or per_floor > DEFAULT_FLOOR_HEIGHT_MAX_M):
            found.add('floor-height')

    return [name for name in CHECKS if name in found]


def annotate_height_warnings(rows: Iterable[Dict]) -> None:
    """各行に height_warnings を書き込みます。

    部分立体の外形は、同じ応答の行から探します。
    外形が応答に含まれない部分立体は、part-over-outline を判定しません。
    """
    rows = list(rows)
    by_id = {r.get('id'): r for r in rows}
    for r in rows:
        parent_id = r.get('parent_building_id')
        outline = by_id.get(parent_id) if parent_id is not None else None
        r['height_warnings'] = height_warnings(r, outline)
```

- [ ] **Step 4: 試験が通ることを確かめる**

Run: `python3 -m pytest -q tests/test_plateau_height_warning.py`
Expected: すべて PASS

- [ ] **Step 5: コミット**

```bash
git add plateau_height_warning.py tests/test_plateau_height_warning.py
git commit -m "feat: 建物の高さが怪しいかを 1 件ずつ判定する関数を足す"
```

---

### Task 2: 問い合わせに面積の列を足し、行に判定を書き込む

**Files:**
- Modify: `osmfj_plateau_api.py`（`get_buildings_in_bbox` の問い合わせ 3 か所と最後の SELECT、取り出した後の処理）
- Test: `tests/test_height_warning_api.py`（新規、統合試験）

**Interfaces:**
- Consumes: `annotate_height_warnings`（Task 1）
- Produces: `get_buildings_in_bbox` が返す各行に `height_warnings: List[str]` と `footprint_m2: Optional[float]` が入ります。`planar_area` は返す前に取り除きます。

- [ ] **Step 1: 失敗する統合試験を書く**

`tests/test_height_warning_api.py`:

```python
"""get_buildings_in_bbox が行に高さの警告を書き込むことの統合試験

PostGIS が要るので、--run-integration のときだけ動く。
"""

import pytest

from tests.test_dedup_city_duplicates import _seed_building, _square_wkt
from tests.test_plateau_height_outliers import (
    _bowtie_with_hole_wkt, _set_wkt,
)

pytestmark = pytest.mark.integration

LAT, LON = 36.70, 137.20
BBOX = (137.19, 36.69, 137.21, 36.71)

# API は、重心、高さ、階数、部分立体かどうかがすべて同じ建物を 1 つにまとめる。
# 同じ高さの建物を並べる試験では、経度をずらして別の建物にする。
STEP = 0.001


def _set_building(conn, building_id, value):
    with conn.cursor() as cur:
        cur.execute(
            'UPDATE plateau_buildings SET building = %s WHERE id = %s',
            (value, building_id),
        )


def _rows_by_id(plateau_api_class, integration_db_url):
    api = plateau_api_class(database_url=integration_db_url)
    rows = api.get_buildings_in_bbox(*BBOX)
    return {r['id']: r for r in rows}


def test_rows_carry_warnings_and_drop_the_planar_area(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    ok = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=7.0,
    )
    low = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON, height=0.5,
    )

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[ok]['height_warnings'] == []
    assert rows[low]['height_warnings'] == ['absolute']
    assert 'planar_area' not in rows[ok]


def test_needle_gets_a_footprint_in_square_metres(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    needle = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=18.4,
    )
    _set_wkt(conn, needle, _square_wkt(LAT, LON, 0.00001))
    short = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON, height=7.0,
    )

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[needle]['height_warnings'] == ['needle']
    assert 0 < rows[needle]['footprint_m2'] < 20
    # 高さ 15 m 以下の建物は、球面上の面積を計算しない。
    assert rows[short]['footprint_m2'] is None


def test_negative_area_does_not_stop_the_query(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    broken = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=20.0,
    )
    _set_wkt(conn, broken, _bowtie_with_hole_wkt(LAT, LON))

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[broken]['height_warnings'] == ['degenerate-area']
    assert rows[broken]['footprint_m2'] is None


def test_part_is_compared_with_its_outline_in_the_same_response(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    outline = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON, height=9.1,
    )
    part = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON, height=149.2,
        building_part='yes', parent_building_id=outline,
    )

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[part]['height_warnings'] == ['part-over-outline']
    assert rows[outline]['height_warnings'] == []


def test_floor_height_warns_only_for_residential(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    conn = fresh_plateau_full_schema
    house = _seed_building(
        conn, osm_id=1, city_code='16201', lat=LAT, lon=LON,
        height=24.6, building_levels=2,
    )
    _set_building(conn, house, 'house')
    factory = _seed_building(
        conn, osm_id=2, city_code='16201', lat=LAT, lon=LON + STEP,
        height=24.6, building_levels=2,
    )
    _set_building(conn, factory, 'industrial')

    rows = _rows_by_id(plateau_api_class, integration_db_url)

    assert rows[house]['height_warnings'] == ['floor-height']
    assert rows[factory]['height_warnings'] == []
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `LC_ALL=C PLATEAU_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/plateau_test python3 -m pytest -q --run-integration tests/test_height_warning_api.py`
Expected: FAIL（`KeyError: 'height_warnings'`）

- [ ] **Step 3: 問い合わせに列を足す**

`osmfj_plateau_api.py` の先頭の import に足します。

```python
from plateau_height_outliers import DEFAULT_NEEDLE_HEIGHT_M
from plateau_height_warning import annotate_height_warnings
```

`get_buildings_in_bbox` の問い合わせでは、`bbox_outlines`、`related_parts`、`orphan_parts` の 3 つの CTE に、同じ位置で同じ 2 列を足します。
3 つとも、次の行の直後です。

```sql
                        ST_AsGeoJSON(ST_PointOnSurface(b.geom))::jsonb -> 'coordinates'
                            AS representative_point,
```

足す 2 列です。

```sql
                        ST_Area(b.geom) AS planar_area,
                        CASE WHEN b.height > {DEFAULT_NEEDLE_HEIGHT_M}
                                  AND ST_Area(b.geom) > 0
                             THEN ST_Area(b.geom::geography)
                        END AS footprint_m2,
```

UNION ALL で列の並びをそろえるため、3 か所とも同じ位置に置きます。
問い合わせは f-string なので、`{DEFAULT_NEEDLE_HEIGHT_M}` はコードの定数で置き換わります。
利用者の入力は入りません。

CTE の上に、次のコメントを置きます。

```sql
                -- planar_area と footprint_m2 は高さの警告の判定に使う。
                -- 球面上の面積は、平面での面積が正で、高さが needle の下限を超えるときだけ計算する。
                -- 面積が 0 以下の輪郭では、球面上の計算が内部エラーで止まるため。
```

最後の SELECT では、`ub.representative_point,` の直後に 2 列を足します。

```sql
                    ub.planar_area,
                    ub.footprint_m2,
```

- [ ] **Step 4: 取り出した行に判定を書き込む**

`representative_point` を整える for 文の直後に、次を足します。

```python
            # 高さの警告を判定して、各行に書き込みます。
            # 部分立体は、同じ応答に含まれる外形と比べます。
            annotate_height_warnings(result)
```

`pre_dedup_count` を取り除いている for 文を、次のように変えます。

```python
            for r in result:
                r.pop('pre_dedup_count', None)
                # planar_area は判定にだけ使う列で、応答には含めません。
                r.pop('planar_area', None)
```

- [ ] **Step 5: 試験が通ることを確かめる**

Run: `LC_ALL=C PLATEAU_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/plateau_test python3 -m pytest -q --run-integration tests/test_height_warning_api.py tests/test_representative_point.py tests/test_intersects_parent_flag.py tests/test_dedup_city_duplicates.py`
Expected: すべて PASS

- [ ] **Step 6: コミット**

```bash
git add osmfj_plateau_api.py tests/test_height_warning_api.py
git commit -m "feat: 建物を返すときに高さの警告を判定する"
```

---

### Task 3: 判定の結果をタグとして出力する

**Files:**
- Modify: `osmfj_plateau_api.py`（`_emit_building_tags` の末尾）
- Test: `tests/test_buildings_xml.py`（試験を足す）

**Interfaces:**
- Consumes: 行の `height_warnings`、`footprint_m2`（Task 2）
- Produces: way と、外形のタグを複製する relation に、`plateau:height_warning` と `plateau:footprint_m2` のタグが付きます。エディタの計画がこの 2 つのタグ名を使います。

- [ ] **Step 1: 失敗する試験を書く**

`tests/test_buildings_xml.py` の末尾に足します。

```python
# ----------------------------------------------------------------------
# 高さの警告のタグ
# ----------------------------------------------------------------------
def _square_nodes(base_id, lat=35.68, lon=139.76, size=0.0001):
    """外側の環だけを持つ正方形のノード列。"""
    corners = [(lat, lon), (lat, lon + size),
               (lat + size, lon + size), (lat + size, lon)]
    return [
        {'id': base_id + i, 'osm_id': base_id + i, 'lat': la, 'lon': lo,
         'sequence_id': i, 'ring_id': 0}
        for i, (la, lo) in enumerate(corners)
    ]


def _tags_of(elem):
    return {t.get('k'): t.get('v') for t in elem.findall('tag')}


class TestHeightWarningTags:
    """判定の結果を、OSM に無い補助のタグとして出す"""

    def test_way_carries_the_warning_and_the_footprint(self, api):
        building = {
            'id': 1, 'building': 'yes', 'height': 18.4,
            'building_part': None, 'parent_building_id': None,
            'height_warnings': ['needle'], 'footprint_m2': 0.0036,
            'nodes': _square_nodes(100),
        }
        root = ET.fromstring(api.buildings_to_osm_xml([building]))
        tags = _tags_of(root.find('way'))

        assert tags['plateau:height_warning'] == 'needle'
        assert tags['plateau:footprint_m2'] == '0.0036'

    def test_several_warnings_are_joined_with_semicolons(self, api):
        building = {
            'id': 1, 'building': 'house', 'height': 0.5,
            'building_levels': 3,
            'building_part': None, 'parent_building_id': None,
            'height_warnings': ['absolute', 'floor-height'],
            'footprint_m2': None,
            'nodes': _square_nodes(100),
        }
        root = ET.fromstring(api.buildings_to_osm_xml([building]))
        tags = _tags_of(root.find('way'))

        assert tags['plateau:height_warning'] == 'absolute;floor-height'
        # needle でなければ底面積は出さない。
        assert 'plateau:footprint_m2' not in tags

    def test_no_warning_no_tag(self, api):
        building = {
            'id': 1, 'building': 'yes', 'height': 7.0,
            'building_part': None, 'parent_building_id': None,
            'height_warnings': [], 'footprint_m2': None,
            'nodes': _square_nodes(100),
        }
        root = ET.fromstring(api.buildings_to_osm_xml([building]))
        tags = _tags_of(root.find('way'))

        assert 'plateau:height_warning' not in tags

    def test_relation_copies_the_outline_warning(self, api):
        outline = {
            'id': 1, 'building': 'yes', 'height': 0.5,
            'building_part': None, 'parent_building_id': None,
            'height_warnings': ['absolute'], 'footprint_m2': None,
            'nodes': _square_nodes(100),
        }
        part = {
            'id': 2, 'building': 'yes', 'height': 3.0,
            'building_part': 'yes', 'parent_building_id': 1,
            'height_warnings': [], 'footprint_m2': None,
            'nodes': _square_nodes(200),
        }
        root = ET.fromstring(api.buildings_to_osm_xml([outline, part]))
        relation = root.find('relation')

        assert _tags_of(relation)['plateau:height_warning'] == 'absolute'
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `python3 -m pytest -q tests/test_buildings_xml.py -k HeightWarning`
Expected: 3 件が FAIL（`KeyError: 'plateau:height_warning'`）、`test_no_warning_no_tag` は PASS

- [ ] **Step 3: タグを出力する**

`_emit_building_tags` の末尾（`representative_point` のタグの後）に足します。

```python
        # 高さの警告です。
        # OSM のタグではないので、エディタが受け取った時点で取り除きます。
        # needle の警告の文に値を入れるため、底面積を添えます。
        warnings = building.get('height_warnings') or []
        if warnings:
            add_tag('plateau:height_warning', ';'.join(warnings))
            footprint = building.get('footprint_m2')
            if 'needle' in warnings and footprint is not None:
                add_tag('plateau:footprint_m2', f'{float(footprint):.4f}')
```

- [ ] **Step 4: 試験が通ることを確かめる**

Run: `python3 -m pytest -q tests/test_buildings_xml.py`
Expected: すべて PASS

- [ ] **Step 5: コミット**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "feat: 高さの警告を補助のタグとして出力する"
```

---

### Task 4: 調査スクリプトと API の判定が一致することを確かめる

**Files:**
- Test: `tests/test_height_warning_api.py`（試験を足す）

**Interfaces:**
- Consumes: `HeightOutlierSurvey.run_check`（既存）、`get_buildings_in_bbox`（Task 2）、`RESIDENTIAL_BUILDING_VALUES`（Task 1）

- [ ] **Step 1: 試験を書く**

`tests/test_height_warning_api.py` の末尾に足します。

```python
def test_api_agrees_with_the_survey_on_the_same_buildings(
    fresh_plateau_full_schema, integration_db_url, plateau_api_class
):
    """同じ建物に対して、調査と API が同じ建物を選ぶ。

    判定を SQL と Python の 2 か所で持つので、食い違いをここで捕まえる。
    警告にしない条件（absolute の上側、住宅以外の floor-height）は、
    調査の結果から除いて比べる。
    """
    from plateau_height_outliers import HeightOutlierSurvey
    from plateau_height_warning import RESIDENTIAL_BUILDING_VALUES

    conn = fresh_plateau_full_schema
    seeds = [
        dict(height=7.0),
        dict(height=0.5),
        dict(height=250.0),
        dict(height=18.4, tiny=True),
        dict(height=24.6, levels=2, building='house'),
        dict(height=24.6, levels=2, building='warehouse'),
        dict(height=20.0, broken=True),
    ]
    for osm_id, s in enumerate(seeds, start=1):
        lon = LON + STEP * osm_id
        bid = _seed_building(
            conn, osm_id=osm_id, city_code='16201', lat=LAT, lon=lon,
            height=s['height'], building_levels=s.get('levels'),
        )
        if 'building' in s:
            _set_building(conn, bid, s['building'])
        if s.get('tiny'):
            _set_wkt(conn, bid, _square_wkt(LAT, lon, 0.00001))
        if s.get('broken'):
            _set_wkt(conn, bid, _bowtie_with_hole_wkt(LAT, lon))
    outline = _seed_building(
        conn, osm_id=100, city_code='16201', lat=LAT, lon=LON, height=9.1,
    )
    _seed_building(
        conn, osm_id=101, city_code='16201', lat=LAT, lon=LON, height=149.2,
        building_part='yes', parent_building_id=outline,
    )

    survey = HeightOutlierSurvey(postgres_url=integration_db_url)
    rows = _rows_by_id(plateau_api_class, integration_db_url)

    for check in ('degenerate-area', 'part-over-outline', 'needle',
                  'absolute', 'floor-height'):
        result = survey.run_check(check, samples=100)
        expected = set()
        for s in result.samples:
            if check == 'absolute' and s['height'] >= 1.0:
                continue
            if (check == 'floor-height'
                    and s['building'] not in RESIDENTIAL_BUILDING_VALUES):
                continue
            expected.add(s['id'])
        actual = {
            bid for bid, r in rows.items() if check in r['height_warnings']
        }
        assert actual == expected, check
```

- [ ] **Step 2: 試験が通ることを確かめる**

判定は Task 1 から Task 3 で実装済みなので、この試験は最初から通る見込みです。
食い違いを捕まえられることを確かめるため、`plateau_height_warning.py` の needle の条件にある `footprint < DEFAULT_NEEDLE_AREA_M2` を、一時的に `footprint < DEFAULT_NEEDLE_AREA_M2 / 1000` に書き換えて実行し、失敗することを見ます。
確かめたら元に戻します。

Run: `LC_ALL=C PLATEAU_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/plateau_test python3 -m pytest -q --run-integration tests/test_height_warning_api.py`
Expected: 書き換えた状態で `needle` の assert が FAIL、戻した状態で PASS

- [ ] **Step 3: コミット**

```bash
git add tests/test_height_warning_api.py
git commit -m "test: 調査スクリプトと API の判定が一致することを確かめる"
```

---

### Task 5: 応答時間を測り、Pull Request を作る

**Files:**
- なし（測定のみ）

- [ ] **Step 1: 全体の試験を回す**

Run: `python3 -m pytest -q`
Expected: 失敗なし

Run: `LC_ALL=C PLATEAU_TEST_DATABASE_URL=postgresql://postgres@127.0.0.1:55432/plateau_test python3 -m pytest -q --run-integration`
Expected: 失敗なし

- [ ] **Step 2: 本番のデータベースで応答時間を測る**

本番のデータベースに対して、変更前（`main`）と変更後の `get_buildings_in_bbox` を、同じ範囲で 5 回ずつ呼び、所要時間の中央値を比べます。
範囲は建物の多い 2 か所にします（大阪市の中心部 `135.49,34.69,135.51,34.71`、新宿区の中心部 `139.69,35.68,139.71,35.70`）。
データベースは読むだけで、サーバの API は止めません。

変更前との差が 1 回あたり 20 % を超える場合は、ここで止めて相談します。
面積の計算を絞る案は、設計文書の「応答時間」の節にあります。

- [ ] **Step 3: Pull Request を作る**

ファイル、コミットメッセージ、PR 本文の 3 つで機微情報を確かめてから出します。
PR 本文には、応答時間の測定結果と、本番への反映はエディタの公開後に行うことを書きます。
