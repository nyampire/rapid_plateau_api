# 極小面積の判定を m² で行う 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 三角形の面積判定を m² で行い、閾値を 0.1 m² にして、実在の三角形が落ちないようにする。

**Architecture:** 面積の計算を module-level の関数 `_triangle_area_m2` に切り出し、緯度補正した shoelace で m² を返す。新しい依存は足さない。判定の適用範囲（三角形のみ）は変えない。

**Tech Stack:** Python 3, psycopg2, pytest

## Global Constraints

- 面積は **m²** で計算する。緯度補正した shoelace を使う。
- **新しい依存を足さない。** importer は標準ライブラリと psycopg2 だけで動いている。shapely も pyproj も使わない。
- PostGIS は使わない。この判定は INSERT の前に走る。
- 閾値は **0.1 m²**。本番 1,489 万棟の実データの最小面積が 0.3 m² なので、実データに触れない。
- **判定の適用範囲は三角形（`len(coords) == 4`）のまま。**全多角形には広げない。実測の結果、広げると落ちるのは junk ではなく実在の極小建物だと判っている。
- `coords` は `(lon, lat)` のタプルの列である。shoelace の x が経度、y が緯度になる。
- 除外の記録（`skip_reasons["tiny_area"]` と `skipped_buildings`）の仕組みは変えない。
- spec: `docs/superpowers/specs/2026-08-06-tiny-area-check-design.ja.md`

## テストの実行

```bash
python -m pytest -q
PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration
```

現在の baseline は unit 261 passed / 19 skipped、integration 280 passed。

---

### Task 1: 面積を m² で測り、閾値を 0.1 m² にする

**Files:**
- Modify: `plateau_importer2postgis.py`（import の追加、module-level 定数と関数、面積判定の差し替え）
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Produces: `_triangle_area_m2(coords)` — module-level 関数。`coords` の先頭 3 点から三角形の面積を m² で返す。テストから直接呼べる。
- Produces: `TINY_AREA_M2 = 0.1` — module-level 定数。
- 四角形以上の挙動は変わらない。判定を通らないまま。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の末尾に追加する。

```python
class TestTriangleAreaInSquareMeters:
    """三角形の面積判定を m² で行う。

    従来は shoelace の結果 (度の二乗) を閾値 1e-6 とそのまま比べていた。
    1e-6 度² は緯度 34 度でおよそ 10,190 m² にあたるので、1 万 m² 未満の
    三角形がすべて落ちていた。周南市の 2 メッシュでは実在の建物が 2 棟
    (およそ 670 m² と 16.5 m²) 落ちていた。

    本番 1,489 万棟の実データの最小面積は 0.3 m² なので、閾値 0.1 m² は
    実データに触れない。degenerate に対する番人としては残る。
    """

    def _tri(self, lon, lat, dlon, dlat):
        """(lon, lat) を直角の頂点とする直角三角形。閉鎖点を含めて 4 点。"""
        p0 = (lon, lat)
        p1 = (lon + dlon, lat)
        p2 = (lon, lat + dlat)
        return [p0, p1, p2, p0]

    def test_area_is_returned_in_square_meters(self):
        # 緯度 34 度、経度方向 0.0001 度、緯度方向 0.0001 度の直角三角形。
        # 緯度 1 度 = 111,320 m、経度 1 度 = 111,320 * cos(34°) = 92,296 m。
        # 底辺 9.23 m、高さ 11.13 m、面積 = 9.23 * 11.13 / 2 ≈ 51.4 m²
        from plateau_importer2postgis import _triangle_area_m2
        got = _triangle_area_m2(self._tri(135.0, 34.0, 0.0001, 0.0001))
        assert 50 < got < 53, f'm² になっていない: {got}'

    def test_longitude_shrinks_with_latitude(self):
        """同じ度数の三角形が、緯度によって違う m² になる。"""
        from plateau_importer2postgis import _triangle_area_m2
        south = _triangle_area_m2(self._tri(135.0, 26.0, 0.0001, 0.0001))
        north = _triangle_area_m2(self._tri(135.0, 44.0, 0.0001, 0.0001))
        assert south > north, '経度方向の縮尺補正が効いていない'

    def test_threshold_is_a_tenth_of_a_square_meter(self):
        from plateau_importer2postgis import TINY_AREA_M2
        assert TINY_AREA_M2 == 0.1


_TRIANGLE_OSM_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="{lat0}" lon="{lon0}"/>
  <node id="-2" lat="{lat1}" lon="{lon1}"/>
  <node id="-3" lat="{lat2}" lon="{lon2}"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-1"/>
    <tag k="building" v="yes"/>
    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>
  </way>
</osm>
"""


class TestTriangleImport:
    """三角形の建物が面積で落ちるかどうか。"""

    def _import(self, bare_importer, dlon, dlat, lat=34.0, lon=135.0):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_TRIANGLE_OSM_TEMPLATE.format(
            lat0=lat, lon0=lon,
            lat1=lat, lon1=lon + dlon,
            lat2=lat + dlat, lon2=lon,
        ))
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_small_real_building_is_imported(self, bare_importer):
        # およそ 16 m² の三角形。実測で落ちていた大きさ。
        # 底辺 5.5 m (0.00006 度)、高さ 5.6 m (0.00005 度) で約 15.4 m²
        buildings_data, _, _ = self._import(bare_importer, 0.00006, 0.00005)
        assert len(buildings_data) == 1, '実在の三角形が落ちている'

    def test_larger_real_building_is_imported(self, bare_importer):
        # およそ 620 m² の三角形。実測で落ちていたもう一方 (約 670 m²) に近い大きさ。
        # 底辺 36.9 m (0.0004 度)、高さ 33.4 m (0.0003 度)。
        buildings_data, _, _ = self._import(bare_importer, 0.0004, 0.0003)
        assert len(buildings_data) == 1

    def test_degenerate_triangle_is_dropped(self, bare_importer):
        # 0.1 m² を下回る三角形。ほぼ潰れた形。
        # 底辺 0.9 m (0.00001 度)、高さ 0.11 m (0.000001 度) で約 0.05 m²
        buildings_data, _, _ = self._import(bare_importer, 0.00001, 0.000001)
        assert len(buildings_data) == 0, 'degenerate な三角形が取り込まれている'
```

`tests/test_plateau_importer2postgis.py` の import に `Path` が無ければ足す（既にあるはず）。

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestTriangleAreaInSquareMeters tests/test_plateau_importer2postgis.py::TestTriangleImport -v`
Expected: `TestTriangleAreaInSquareMeters` の 3 件が ImportError で FAIL（`_triangle_area_m2` と `TINY_AREA_M2` が無い）。
`test_small_real_building_is_imported` と `test_larger_real_building_is_imported` が FAIL（現行の閾値で落ちる）。
`test_degenerate_triangle_is_dropped` は PASS（現行でも落ちる）。

- [ ] **Step 3: `math` を import する**

`plateau_importer2postgis.py` の import の並びに追加する。`import hashlib` の隣が位置になる。

```python
import math
```

- [ ] **Step 4: 定数と面積計算の関数を足す**

`plateau_importer2postgis.py` の import の後、クラス定義より前に追加する。

```python
# 極小ポリゴンとみなす面積の上限 (m²)。
#
# 本番 1,489 万棟の実データの最小面積は 0.3 m² なので、この値は実データに触れない。
# 1 m² 未満の 2,264 件を無作為に抽出して細長さ (周長 ÷ √面積) を測ると 4.0〜8.1 で、
# 正方形の 4.0 に近い。degenerate な細片なら数十から数百になる。
# つまり本番に degenerate な多角形は無く、この番人が実際に捕まえるものは存在しない。
# それでも面積ゼロに近い多角形は PostGIS 上も無効になるので、番人としては残す。
TINY_AREA_M2 = 0.1

# 緯度 1 度あたりの距離 (m)。建物規模では緯度による変化を無視できる。
_METERS_PER_DEGREE_LAT = 111320.0


def _triangle_area_m2(coords):
    """三角形の面積を m² で返す。

    `coords` は (lon, lat) のタプルの列で、先頭 3 点を使う。

    座標が経緯度なので、shoelace をそのまま使うと単位が度の二乗になる。
    緯度 1 度はおよそ 111,320 m、経度 1 度は緯度によって縮む (111,320 × cos(緯度))。
    建物規模の多角形なら、この補正で誤差は 1% を大きく下回る。

    PostGIS を使う案は、この判定が INSERT の前に走るので合わない。
    shapely や pyproj を足す案は、importer が標準ライブラリと psycopg2 だけで
    動いている構成を崩す。
    """
    (x1, y1), (x2, y2), (x3, y3) = coords[0], coords[1], coords[2]
    area_deg2 = abs((x1 * (y2 - y3) + x2 * (y3 - y1) + x3 * (y1 - y2)) / 2)
    lat_rad = math.radians((y1 + y2 + y3) / 3)
    meters_per_degree_lon = _METERS_PER_DEGREE_LAT * math.cos(lat_rad)
    return area_deg2 * _METERS_PER_DEGREE_LAT * meters_per_degree_lon
```

- [ ] **Step 5: 面積判定を差し替える**

`plateau_importer2postgis.py` の面積チェックを差し替える。

```python
                    # 面積チェック（極小ポリゴン除外）
                    if len(coords) >= 4:
                        area_check = True
                        area = None
                        if len(coords) == 4:  # 三角形
                            # 従来はここで度の二乗のまま 1e-6 と比べていた。
                            # 1e-6 度² は緯度 34 度でおよそ 10,190 m² にあたり、
                            # 1 万 m² 未満の三角形がすべて落ちていた (#44)。
                            area = _triangle_area_m2(coords)
                            if area < TINY_AREA_M2:
                                area_check = False
```

`skipped_buildings` に積んでいる `"area": area` はそのままでよい。
記録される値の単位が度の二乗から m² に変わる。読める値になるので改善である。

- [ ] **Step 6: テストが通ることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestTriangleAreaInSquareMeters tests/test_plateau_importer2postgis.py::TestTriangleImport -v`
Expected: 6 passed

- [ ] **Step 7: 閾値が本当に効いていることを確かめる**

`TINY_AREA_M2` を一時的に `0.0` にし、`test_degenerate_triangle_is_dropped` が落ちることを確認してから戻す。

前後で必ずバイトコードを消す。同じ大きさの改変を同じ秒に戻すと古い `.pyc` が使われ、確認が無意味になる。

```bash
find . -name "__pycache__" -type d -exec rm -rf {} +
```

戻したあと `git diff` が想定どおりであることを確認する。

- [ ] **Step 8: 全テストを実行する**

Run: `python -m pytest -q`
Expected: baseline の 261 passed / 19 skipped に新規 6 件が加わる

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration`
Expected: 失敗なし

- [ ] **Step 9: 実データで確認する**

周南 2 メッシュを取り込み直し、**除外が 0 件になり建物が 3,837 行になる**ことを確認する。
これまでは三角形 2 件が落ちて 3,835 行だった。

素の変換出力を使うこと。`imp_rule3` を含むパスの下のものは規則適用後にフィルタ済みで、検証にならない。
staged copy が
`/private/tmp/claude-501/-Users-nyampire-git-Rapid/f1440411-3615-4d28-b29d-9772815bc402/scratchpad/task5/35215/`
にある（識別子無し way が 51310655 で 63、51310636 で 54 あることを確かめてから使う）。

検証用 DB `plateau_task5` には `plateau_buildings`、`plateau_building_nodes`、`dash_city_master` の
3 つが要る。`dash_city_master` が無いと行政界フィルタの SELECT が失敗してトランザクションが中断し、
**1 行も入らないままログは「インポート成功」と出る**。

前回の実行のデータが残っているので、取り込み前に TRUNCATE すること。

```bash
psql -d plateau_task5 -c "TRUNCATE plateau_building_nodes, plateau_buildings RESTART IDENTITY CASCADE"
python plateau_importer2postgis.py \
  --data-dir <作業ディレクトリ>/35215 \
  --postgres-url "postgresql:///plateau_task5" --no-zip
psql -d plateau_task5 -c "SELECT count(*) FROM plateau_buildings"
```

`skipped_buildings_35215.json` が生成されないこと（除外 0 件）も確認する。
生成された場合は中身を確認し、報告に貼る。

- [ ] **Step 10: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "fix(#44): compare the tiny-area threshold in square metres"
```
