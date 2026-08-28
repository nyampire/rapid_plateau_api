# 面積の条件で type=building リレーションを選ぶ 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 最大の部材の面積が 300 平方メートル以上の `type=building` リレーションだけを取り込み、配信でメンバーの建物の型を落とさないようにする。

**Architecture:** 取り込み器の `parse_osm_file_safe` で、`4cd2b96` が外したリレーション解析を戻し、そこに面積の条件を足す。条件を満たしたリレーションだけが `part_to_outline` に入り、その外形は建物 ID を持たなくても取り込まれる。配信側は共通のタグ出力を、固定値ではなく DB の `building` 列を使う形に変える。あわせて取り込みで部材の型を `building` 列に保存する。

**Tech Stack:** Python 3、標準ライブラリの `xml.etree.ElementTree` と `math`、psycopg2、pytest。

## Global Constraints

- 設計文書: `docs/superpowers/specs/2026-08-28-building-relation-area-gate-design.md`
- 閾値は 300 平方メートル。判定は「最大の部材が 300 以上」。300 ちょうどは取り込む
- 条件は 1 つだけ。建物区分も用途も高さも部材の数も使わない
- 面積は `.osm` の座標から計算する。`buildingRoofEdgeArea` の属性は使わない
- 取り込み器に依存を足さない。標準ライブラリと psycopg2 だけで動く構成を崩さない
- 変換器 (`scripts/reimport/` 配下と外部の jar) には手を入れない
- 既存のテスト 480 件は全部通ったままにする
- 公開リポジトリなので、ファイルとコミットメッセージに機微情報を書かない

---

### Task 1: 多角形の面積を求める関数

**Files:**
- Modify: `plateau_importer2postgis.py:50-72`（`_triangle_area_m2` の直後に追加）
- Test: `tests/test_plateau_importer2postgis.py`（末尾に追加）

**Interfaces:**
- Consumes: 既存の `_METERS_PER_DEGREE_LAT`
- Produces: `_polygon_area_m2(coords) -> float`。`coords` は `(lon, lat)` のタプルの列で、
  閉じていてもいなくてもよい。3 点未満なら `0.0` を返す。
  `RELATION_MIN_LARGEST_PART_AREA_M2 = 300.0`（モジュール直下の定数）。
  `_relation_passes_area_gate(part_areas) -> bool`。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の import に `math` を足し、
`from plateau_importer2postgis import PlateauImporter2PostGIS` を次に変えます。

```python
from plateau_importer2postgis import (
    PlateauImporter2PostGIS,
    RELATION_MIN_LARGEST_PART_AREA_M2,
    _polygon_area_m2,
    _relation_passes_area_gate,
)
```

そのうえで、ファイルの末尾に次を足します。

```python
# ----------------------------------------------------------------------
# 面積の条件で type=building relation を選ぶ
# ----------------------------------------------------------------------


def _square_coords(side_m, lat=33.0, lon=133.0):
    """1 辺 `side_m` メートルのおよそ正方形の環を (lon, lat) の列で返す。

    緯度 1 度を 111,320 m、経度 1 度をその cos(緯度) 倍として辺の長さを度に直す。
    面積の判定の境界をぎりぎりで跨がせる用途には使わない（丸めで裏返るため）。
    境界そのものは `_relation_passes_area_gate` の単体テストで見る。
    """
    d_lat = side_m / 111320.0
    d_lon = side_m / (111320.0 * math.cos(math.radians(lat)))
    return [(lon, lat), (lon + d_lon, lat), (lon + d_lon, lat + d_lat),
            (lon, lat + d_lat), (lon, lat)]


class TestPolygonAreaM2:
    """既知の矩形に対して期待値を返すこと。"""

    def test_known_rectangle_matches_the_metric_estimate(self):
        lat, lon, d = 35.0, 139.0, 0.0001
        coords = [(lon, lat), (lon + d, lat), (lon + d, lat + d),
                  (lon, lat + d), (lon, lat)]
        # 実装と同じ近似で手計算した値。平均緯度で経度側を縮める。
        expected = (d * 111320.0) * (
            d * 111320.0 * math.cos(math.radians(lat + d / 2)))
        assert _polygon_area_m2(coords) == pytest.approx(expected, rel=1e-9)
        # 近似そのものが妥当な桁であることも押さえる (11.1 m x 9.1 m)。
        assert _polygon_area_m2(coords) == pytest.approx(101.5, rel=0.01)

    def test_square_helper_yields_the_requested_area(self):
        assert _polygon_area_m2(_square_coords(20.0)) == pytest.approx(400.0, rel=1e-3)

    def test_unclosed_ring_gives_the_same_area_as_closed(self):
        closed = _square_coords(20.0)
        assert _polygon_area_m2(closed[:-1]) == pytest.approx(
            _polygon_area_m2(closed), rel=1e-12)

    def test_fewer_than_three_points_is_zero(self):
        assert _polygon_area_m2([]) == 0.0
        assert _polygon_area_m2([(133.0, 33.0), (133.001, 33.0)]) == 0.0
        # 閉じた 2 点 (同じ点に戻る) も面積を持たない。
        assert _polygon_area_m2([(133.0, 33.0), (133.001, 33.0), (133.0, 33.0)]) == 0.0


class TestRelationAreaGate:
    """最大の部材の面積だけで relation を選ぶ。"""

    def test_threshold_is_300(self):
        assert RELATION_MIN_LARGEST_PART_AREA_M2 == 300.0

    def test_just_below_the_threshold_is_rejected(self):
        assert _relation_passes_area_gate([299.0]) is False

    def test_exactly_the_threshold_is_accepted(self):
        assert _relation_passes_area_gate([300.0]) is True

    def test_a_single_large_part_is_enough(self):
        assert _relation_passes_area_gate([301.0]) is True

    def test_the_largest_part_decides(self):
        assert _relation_passes_area_gate([1.0, 2.0, 301.0]) is True
        assert _relation_passes_area_gate([299.0, 10.0, 5.0]) is False

    def test_no_parts_is_rejected(self):
        assert _relation_passes_area_gate([]) is False
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -k "PolygonAreaM2 or RelationAreaGate" -q`
Expected: 収集の時点で `ImportError: cannot import name '_polygon_area_m2'` になります。

- [ ] **Step 3: 実装を書く**

`plateau_importer2postgis.py` の `_triangle_area_m2` の定義の直後（`class PlateauImporter2PostGIS:` の前）に足します。

```python
# type=building relation を取り込むかどうかを決める、最大の部材の面積 (m²)。
# 変換出力のこの relation には、複数の棟を持つ工場や学校のほかに、戸建てと
# カーポート、住宅密集地の戸建ての並びが混ざっている。残したいのは前者だけである。
# PLATEAU の建築面積は中央値 67 m²、上位 5% で 255 m² なので、300 m² 以上の部材を
# 持つ集合には戸建てだけの組み合わせが入りにくい。
# 詳細は docs/superpowers/specs/2026-08-28-building-relation-area-gate-design.md を参照。
RELATION_MIN_LARGEST_PART_AREA_M2 = 300.0


def _polygon_area_m2(coords):
    """多角形の面積を m² で返す。

    `coords` は (lon, lat) のタプルの列。閉じていてもいなくてもよい。
    3 点未満なら 0.0 を返す。

    近似は `_triangle_area_m2` と同じで、緯度 1 度を 111,320 m、経度 1 度を
    その cos(平均緯度) 倍とみなす。1 メッシュの内部なら緯度差が小さく、
    300 m² の判定に要る精度は十分に出る。
    """
    ring = list(coords)
    if len(ring) >= 2 and ring[0] == ring[-1]:
        ring = ring[:-1]
    if len(ring) < 3:
        return 0.0
    total = 0.0
    for i in range(len(ring)):
        x1, y1 = ring[i]
        x2, y2 = ring[(i + 1) % len(ring)]
        total += x1 * y2 - x2 * y1
    area_deg2 = abs(total) / 2
    lat_rad = math.radians(sum(y for _, y in ring) / len(ring))
    meters_per_degree_lon = _METERS_PER_DEGREE_LAT * math.cos(lat_rad)
    return area_deg2 * _METERS_PER_DEGREE_LAT * meters_per_degree_lon


def _relation_passes_area_gate(part_areas):
    """部材の面積の列から、その relation を取り込むかどうかを返す。

    条件は「最大の部材が RELATION_MIN_LARGEST_PART_AREA_M2 以上」の 1 つだけ。
    2 番目の部材に条件を足す案は、大きい建物に庇が 1 つ付いた形を巻き添えに
    するだけで、落としたい形には効かなかったので採らなかった。
    """
    if not part_areas:
        return False
    return max(part_areas) >= RELATION_MIN_LARGEST_PART_AREA_M2
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -k "PolygonAreaM2 or RelationAreaGate" -q`
Expected: 12 passed

- [ ] **Step 5: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "feat(importer): 多角形の面積と、部材の面積で relation を選ぶ判定を足す"
```

---

### Task 2: 面積の条件で relation を読み直す

**Files:**
- Modify: `plateau_importer2postgis.py:446-462`（`parse_osm_file_safe` の docstring）
- Modify: `plateau_importer2postgis.py:475-480`（`part_to_outline` の宣言とコメント）
- Modify: `plateau_importer2postgis.py:579` の直後（ノード収集の後、way ループの前）に relation の解析を追加
- Modify: `plateau_importer2postgis.py:615-620`（建物 ID 無しの way を落とす条件）
- Modify: `plateau_importer2postgis.py:630-650`（`is_part` と `parent_outline_way_id` の決定）
- Test: `tests/test_plateau_importer2postgis.py`（Task 1 で足した節の続き）

**Interfaces:**
- Consumes: Task 1 の `_polygon_area_m2` と `_relation_passes_area_gate`
- Produces: `parse_osm_file_safe` が返す building の辞書で、
  条件を満たした relation の `role=part` メンバーは `is_part=True` と
  `parent_outline_way_id=<outline の way id 文字列>` を持つ。
  `role=outline` メンバーは `ref:MLIT_PLATEAU` が無くても収集される。
  条件を満たさない relation のメンバーは、これまでどおり `is_part=False`、
  `parent_outline_way_id=None` の独立した建物になる。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の末尾に足します。

```python
def _relation_osm(part_side_m, outline_side_m=None, part_tag='building:part',
                  part_value='yes', outline_ref=None, extra_part_side_m=None):
    """outline 1 本と part 1〜2 本を持つ type=building relation の .osm を返す。

    outline は建物 ID を持たない合成外形にしてある (`outline_ref=None`)。
    融合が outline メンバーから `ref:MLIT_PLATEAU` を外すためで、実データと同じ形。
    """
    if outline_side_m is None:
        outline_side_m = part_side_m * 2
    lat, lon = 33.0, 133.0

    def _ring(side_m, offset_lat=0.0, offset_lon=0.0):
        d_lat = side_m / 111320.0
        d_lon = side_m / (111320.0 * math.cos(math.radians(lat)))
        y, x = lat + offset_lat, lon + offset_lon
        return [(x, y), (x + d_lon, y), (x + d_lon, y + d_lat), (x, y + d_lat)]

    rings = [('-100', _ring(outline_side_m)), ('-200', _ring(part_side_m))]
    if extra_part_side_m is not None:
        rings.append(('-300', _ring(extra_part_side_m, offset_lat=0.01)))

    nodes, ways, node_id = [], [], -1
    for way_id, ring in rings:
        refs = []
        for x, y in ring:
            nodes.append(f'  <node id="{node_id}" lat="{y:.8f}" lon="{x:.8f}"/>')
            refs.append(node_id)
            node_id -= 1
        nd = ''.join(f'<nd ref="{r}"/>' for r in refs + [refs[0]])
        if way_id == '-100':
            ref_tag = (f'\n    <tag k="ref:MLIT_PLATEAU" v="{outline_ref}"/>'
                       if outline_ref else '')
            ways.append(f'  <way id="{way_id}">\n    {nd}\n'
                        f'    <tag k="building" v="yes"/>{ref_tag}\n  </way>')
        else:
            ways.append(f'  <way id="{way_id}">\n    {nd}\n'
                        f'    <tag k="{part_tag}" v="{part_value}"/>\n'
                        f'    <tag k="ref:MLIT_PLATEAU" v="39999-bldg{way_id}"/>\n'
                        f'  </way>')

    members = '\n'.join(
        ['    <member type="way" ref="-100" role="outline"/>']
        + [f'    <member type="way" ref="{w}" role="part"/>'
           for w, _ in rings[1:]])
    return ('<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6">\n'
            + '\n'.join(nodes) + '\n' + '\n'.join(ways) + '\n'
            + '  <relation id="-900">\n' + members + '\n'
            + '    <tag k="type" v="building"/>\n'
            + '    <tag k="building" v="yes"/>\n  </relation>\n</osm>\n')


class TestRelationAreaGateOnOsmFiles:
    """`.osm` を通した挙動。閾値の境界そのものは
    `TestRelationAreaGate` で見ているので、ここは十分に大きい / 小さい形で見る。
    """

    def _parse(self, bare_importer, xml):
        importer = bare_importer(citycode='39999')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        _nodes, buildings = importer.parse_osm_file_safe(osm_file)
        return {b['way_id']: b for b in buildings}

    def test_large_part_keeps_the_relation(self, bare_importer):
        """部材が 400 m² なら、外形が取り込まれ、部材に親が付く。"""
        by_way = self._parse(bare_importer, _relation_osm(part_side_m=20.0))
        assert '-100' in by_way, '条件を満たした relation の外形が落ちている'
        assert by_way['-100']['is_part'] is False
        assert by_way['-100']['parent_outline_way_id'] is None
        assert by_way['-200']['is_part'] is True
        assert by_way['-200']['parent_outline_way_id'] == '-100'

    def test_a_single_large_part_is_enough(self, bare_importer):
        """部材が 1 本しかなくても、それが大きければ通る。"""
        by_way = self._parse(bare_importer, _relation_osm(part_side_m=25.0))
        assert len(by_way) == 2
        assert by_way['-200']['is_part'] is True

    def test_small_parts_drop_the_relation(self, bare_importer):
        """部材が小さい relation は読まれず、外形も取り込まれない。"""
        by_way = self._parse(bare_importer, _relation_osm(part_side_m=8.0))
        assert '-100' not in by_way, '建物 ID の無い外形が取り込まれている'
        assert by_way['-200']['is_part'] is False
        assert by_way['-200']['parent_outline_way_id'] is None

    def test_members_of_a_dropped_relation_stay_independent(self, bare_importer):
        """条件を満たさなかった relation のメンバーは独立した建物として残る。"""
        by_way = self._parse(
            bare_importer, _relation_osm(part_side_m=8.0, extra_part_side_m=6.0))
        assert set(by_way) == {'-200', '-300'}
        assert all(b['is_part'] is False for b in by_way.values())

    def test_the_largest_part_decides(self, bare_importer):
        """小さい部材が混ざっていても、最大が大きければ全部が部材になる。"""
        by_way = self._parse(
            bare_importer, _relation_osm(part_side_m=20.0, extra_part_side_m=4.0))
        assert by_way['-200']['is_part'] is True
        assert by_way['-300']['is_part'] is True
        assert by_way['-300']['parent_outline_way_id'] == '-100'

    def test_outline_exception_does_not_leak_to_other_ways(self, bare_importer):
        """relation に属さない建物 ID 無しの way は、これまでどおり落ちる。"""
        xml = _relation_osm(part_side_m=20.0).replace(
            '</osm>',
            '  <node id="-90" lat="33.05" lon="133.05"/>\n'
            '  <node id="-91" lat="33.0505" lon="133.05"/>\n'
            '  <node id="-92" lat="33.0505" lon="133.0505"/>\n'
            '  <node id="-93" lat="33.05" lon="133.0505"/>\n'
            '  <way id="-400">\n'
            '    <nd ref="-90"/><nd ref="-91"/><nd ref="-92"/>'
            '<nd ref="-93"/><nd ref="-90"/>\n'
            '    <tag k="building" v="yes"/>\n  </way>\n</osm>')
        by_way = self._parse(bare_importer, xml)
        assert '-400' not in by_way, 'relation に属さない合成形状が通っている'
        assert '-100' in by_way, '条件を満たした外形まで落ちている'
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -k RelationAreaGateOnOsmFiles -q`
Expected: 6 件のうち少なくとも `test_large_part_keeps_the_relation` が
`assert '-100' in by_way` で FAIL します（いまは外形が落ち、部材にも親が付かないため）。

- [ ] **Step 3: docstring と `part_to_outline` のコメントを直す**

`plateau_importer2postgis.py` の `parse_osm_file_safe` の docstring のうち、
`4cd2b96` で書いた 3 行を次に差し替えます。

差し替え前:

```
        - <relation type=building> は読まない。融合で作られた合成 outline と
          その親子関係を取り込まないため。よって parent_outline_way_id は
          常に None、is_part は常に False になる。
        - `ref:MLIT_PLATEAU` タグを持たない building/building:part way は
          融合で作られた合成形状とみなし、取り込まない。
```

差し替え後:

```
        - <relation type=building> は、最大の部材が 300 m² 以上のものだけを読む。
          条件を満たしたものは outline を親、part を子として取り込む。
          満たさないものは読まなかったことにし、メンバーは独立した建物になる。
        - `ref:MLIT_PLATEAU` タグを持たない building/building:part way は
          融合で作られた合成形状とみなし、取り込まない。ただし条件を満たした
          relation の outline メンバーだけは例外として取り込む。
```

続けて、`part_to_outline = {}` の直前にある 5 行のコメントを次に差し替えます。

差し替え前:

```
        # type=building の relation は読まない。
        # この relation は「接触する建物を融合したまとまり」であり、outline は
        # 融合で作られた形状、part は取り込まれた実在建物である。元データの
        # BuildingPart に由来するものではない（変換器はそれを読んでいない）。
        # 親子関係を作ると実在しない建物を親にすることになるため、作らない。
        part_to_outline = {}
```

差し替え後:

```
        # type=building の relation の親子関係。面積の条件を通ったものだけが入る。
        # 中身はノード収集の後で埋める（部材の面積に座標が要るため）。
        part_to_outline = {}
        gated_outline_way_ids = set()
```

- [ ] **Step 4: relation の解析を足す**

ノード収集のループの直後、`# 建物ウェイ収集` のコメントの直前に足します。

```python
        # type=building の relation を、最大の部材の面積で選ぶ。
        # この relation は「接触する建物を融合したまとまり」で、複数の棟を持つ
        # 工場や学校のほかに、戸建てとカーポート、戸建ての並びが混ざっている。
        # 残したいのは前者だけなので、最大の部材の面積 1 つで分ける。
        # 建物区分は都市によって入っていないことがあるが、面積は図形から計算できる。
        # 条件を満たさなかった relation は読まなかったことにする。メンバーは
        # 下の way ループでこれまでどおり独立した建物として取り込まれる。
        for rel_elem in root.findall('relation'):
            rel_tags = {t.get('k'): t.get('v') for t in rel_elem.findall('tag')
                        if t.get('k') and t.get('v')}
            if rel_tags.get('type') != 'building':
                continue
            outline_way_id = None
            part_way_ids = []
            for m in rel_elem.findall('member'):
                if m.get('type') != 'way':
                    continue
                role = m.get('role')
                if role == 'outline':
                    outline_way_id = m.get('ref')
                elif role == 'part':
                    part_way_ids.append(m.get('ref'))
            if not outline_way_id or not part_way_ids:
                continue
            part_areas = []
            for pwid in part_way_ids:
                coords = [(nodes[r]['lon'], nodes[r]['lat'])
                          for r in raw_way_nd_refs.get(pwid, []) if r in nodes]
                part_areas.append(_polygon_area_m2(coords))
            if not _relation_passes_area_gate(part_areas):
                continue
            gated_outline_way_ids.add(outline_way_id)
            for pwid in part_way_ids:
                part_to_outline[pwid] = outline_way_id
```

- [ ] **Step 5: 建物 ID 無しの way を落とす条件に例外を作る**

差し替え前:

```python
            # 建物 ID を持たない建物 way は、融合で作られた合成形状である。
            # 元データに対応する形状が無いので取り込まない (10 メッシュ 386 本で確認)。
            ref_mlit = tags.get('ref:MLIT_PLATEAU')
            if (is_building or is_part) and not ref_mlit:
                continue
```

差し替え後:

```python
            # 建物 ID を持たない建物 way は、融合で作られた合成形状である。
            # 元データに対応する形状が無いので取り込まない (10 メッシュ 386 本で確認)。
            # 例外は、面積の条件を通った relation の outline メンバーだけ。融合は
            # outline から ref:MLIT_PLATEAU を外すが、この合成外形は残したい。
            ref_mlit = tags.get('ref:MLIT_PLATEAU')
            if not ref_mlit and way_id not in gated_outline_way_ids:
                continue
```

- [ ] **Step 6: `is_part` と親を relation から決める**

差し替え前:

```python
            if len(nd_refs) >= 3:
                # part の場合は parent_outline_way_id を解決
                parent_outline_way_id = part_to_outline.get(way_id) if is_part else None
```

差し替え後:

```python
            if len(nd_refs) >= 3:
                # 面積の条件を通った relation の part メンバーだけが親を持つ。
                parent_outline_way_id = part_to_outline.get(way_id)
```

同じ `buildings.append({...})` の中の `is_part` を差し替えます。

差し替え前:

```python
                    # 建物 ID を持つ way は、building:part に降格されていても
                    # 元は独立した建物である。変換器は CityGML の BuildingPart を
                    # 読まないので、真の部分立体は出力に存在しない。
                    'is_part': False,
```

差し替え後:

```python
                    # 面積の条件を通った relation の part メンバーだけを部材とする。
                    # それ以外の way は、building:part に降格されていても独立した
                    # 建物として扱う。変換器は CityGML の BuildingPart を読まない
                    # ので、降格は融合の副産物であって部分立体ではない。
                    'is_part': parent_outline_way_id is not None,
```

- [ ] **Step 7: 新しいテストが通ることを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -k RelationAreaGateOnOsmFiles -q`
Expected: 6 passed

- [ ] **Step 8: 既存のテストが通ったままであることを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -q`
Expected: すべて PASS。

`TestParseOsmFileRelations` と `TestCitygmlOsmFixtures` の既存の期待値は変わりません。
どちらの標本も部材が 10 m² から 34 m² で、300 m² に届かないためです。
もし FAIL したら、その fixture の部材の面積を `_polygon_area_m2` で測ってから
期待値を直すか、fixture を小さくするかを判断します。期待値を先に直さないこと。

- [ ] **Step 9: 全体のテストを走らせる**

Run: `python3 -m pytest -q`
Expected: 480 passed, 26 skipped

- [ ] **Step 10: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "feat(importer): 最大の部材が 300 m 平方以上の type=building relation を取り込む"
```

---

### Task 3: 部材の型を DB に保存する

**Files:**
- Modify: `plateau_importer2postgis.py:962-971`（`building_value` の決定）
- Test: `tests/test_plateau_importer2postgis.py`（Task 2 で足した節の続き）

**Interfaces:**
- Consumes: Task 2 の `is_part` と `parent_outline_way_id`
- Produces: `process_buildings_safe` が返す `buildings_data` の各行で、
  添字 1 (`building`) が部材でも型を持つ。添字 23 (`building_part`) はこれまでどおり
  部材なら `'yes'`、それ以外は `None`。

**なぜ要るか:** 配信で `building:part=<型>` を出すには、型が DB に無いといけません。
いまは部材の行の `building` 列が空です。`building_part` 列は `'yes'` という値で
判定に使われているので、型はそちらではなく `building` 列に入れます。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の末尾に足します。

```python
class TestPartKeepsItsTypeInTheDatabase:
    """部材の行も建物の型を持つ。配信が `building:part=<型>` を出すのに要る。

    無壁舎の後処理は way のキーを見て値を書き換えるので、部材には
    `building:part=roof` が付く。これを `building` 列に保存する。
    """

    def _rows(self, bare_importer, xml):
        importer = bare_importer(citycode='39999')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        buildings_data, _, _ = importer.process_buildings_safe(all_nodes, buildings)
        # 添字 7 が plateau_id ('w-100' のような形)。way ごとに引けるようにする。
        return {row[7]: row for row in buildings_data}

    def test_part_row_keeps_its_type(self, bare_importer):
        rows = self._rows(bare_importer, _relation_osm(
            part_side_m=20.0, part_value='roof'))
        assert rows['w-200'][1] == 'roof', '部材の型が保存されていない'
        assert rows['w-200'][23] == 'yes', '部材の印が消えている'

    def test_part_without_a_type_falls_back_to_yes(self, bare_importer):
        rows = self._rows(bare_importer, _relation_osm(part_side_m=20.0))
        assert rows['w-200'][1] == 'yes'
        assert rows['w-200'][23] == 'yes'

    def test_outline_row_is_not_marked_as_a_part(self, bare_importer):
        rows = self._rows(bare_importer, _relation_osm(part_side_m=20.0))
        assert rows['w-100'][1] == 'yes'
        assert rows['w-100'][23] is None
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -k PartKeepsItsType -q`
Expected: `test_part_row_keeps_its_type` が `assert None == 'roof'` で FAIL します。

- [ ] **Step 3: 実装を書く**

差し替え前:

```python
                            # building:part 判定 (parse_osm_file_safe 由来)
                            is_part = bool(building.get('is_part'))
                            building_part_value = 'yes' if is_part else None
                            # building タグ: part の場合は building タグ無しなので None
                            building_value = (
                                converted_tags.get('building', 'yes')
                                if not is_part
                                else tags.get('building')  # 通常 None
                            )
```

差し替え後:

```python
                            # building:part 判定 (parse_osm_file_safe 由来)
                            is_part = bool(building.get('is_part'))
                            building_part_value = 'yes' if is_part else None
                            # 型は部材でも保存する。配信が building:part=<型> を
                            # 出すのに要るため。convert_building_tags_enhanced は
                            # building が無ければ building:part の値を型として読む。
                            building_value = converted_tags.get('building', 'yes')
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_plateau_importer2postgis.py -q`
Expected: すべて PASS

- [ ] **Step 5: 全体のテストを走らせる**

Run: `python3 -m pytest -q`
Expected: 483 passed, 26 skipped（Task 3 の 3 件が増えます）

- [ ] **Step 6: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "feat(importer): 部材の行にも建物の型を保存する"
```

---

### Task 4: 配信で部材の型を出す

**Files:**
- Modify: `osmfj_plateau_api.py:447-462`（`_emit_building_tags` の docstring と `building:part` の出力）
- Test: `tests/test_buildings_xml.py`（`TestBuildingsToOsmXmlRelations` に追加）

**Interfaces:**
- Consumes: Task 3 が保存した `building` 列（API の SELECT に `b.building` として既に入っています）
- Produces: `buildings_to_osm_xml` の出力で、部材の way が `building:part=<型>` を持つ。
  `building` 列が空の行はこれまでどおり `building:part=yes` になる。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_buildings_xml.py` の `TestBuildingsToOsmXmlRelations` の中、
`test_part_emits_building_part_tag_not_building` の直後に足します。

```python
    def test_part_emits_its_building_type(self, api):
        """部材の型を building:part の値として出す。DB の型を捨てない。"""
        part = _make_part(part_id=11, parent_id=None, building='industrial')
        xml_str = api.buildings_to_osm_xml([part])
        root = ET.fromstring(xml_str)
        tags = {t.get('k'): t.get('v') for t in root.find('way').findall('tag')}
        assert tags.get('building:part') == 'industrial'
        assert 'building' not in tags

    def test_walless_part_emits_building_part_roof(self, api):
        """無壁舎が部材になった場合は building:part=roof になる。"""
        part = _make_part(part_id=12, parent_id=None, building='roof')
        xml_str = api.buildings_to_osm_xml([part])
        root = ET.fromstring(xml_str)
        tags = {t.get('k'): t.get('v') for t in root.find('way').findall('tag')}
        assert tags.get('building:part') == 'roof'

    def test_part_without_a_type_stays_yes(self, api):
        """型が空の行はこれまでどおり building:part=yes を出す。"""
        part = _make_part(part_id=13, parent_id=None, building=None)
        xml_str = api.buildings_to_osm_xml([part])
        root = ET.fromstring(xml_str)
        tags = {t.get('k'): t.get('v') for t in root.find('way').findall('tag')}
        assert tags.get('building:part') == 'yes'

    def test_typed_part_inside_a_relation_keeps_its_type(self, api):
        """relation の中でも部材の型は落ちず、外形の型とは別に出る。"""
        outline = _make_building(building_id=1, building='school', height=10)
        p1 = _make_part(part_id=2, parent_id=1, building='roof', height=4)
        xml_str = api.buildings_to_osm_xml([outline, p1])
        root = ET.fromstring(xml_str)
        by_id = {w.get('id'): {t.get('k'): t.get('v') for t in w.findall('tag')}
                 for w in root.findall('way')}
        assert by_id[str(-(1 * 1000))].get('building') == 'school'
        assert by_id[str(-(2 * 1000))].get('building:part') == 'roof'
        # relation 側は外形のタグを写すので building=school のまま。
        rel_tags = {t.get('k'): t.get('v')
                    for t in root.find('relation').findall('tag')}
        assert rel_tags.get('building') == 'school'
        assert 'building:part' not in rel_tags
```

- [ ] **Step 2: 失敗することを確かめる**

Run: `python3 -m pytest tests/test_buildings_xml.py -k "building_type or roof or without_a_type or typed_part" -q`
Expected: `test_part_emits_its_building_type` が `assert 'yes' == 'industrial'` で FAIL します。

- [ ] **Step 3: 実装を書く**

`osmfj_plateau_api.py` の `_emit_building_tags` の docstring の 1 行を差し替えます。

差し替え前:

```
        is_part=True の場合は `building:part=yes`、それ以外は `building=*`。
```

差し替え後:

```
        is_part=True の場合は `building:part=<型>`、それ以外は `building=<型>`。
        型は DB の building 列から取る。取り込みは部材の行にも型を保存している。
        型が空の行 (2026-08-28 より前に取り込まれた部材) は yes に落とす。
```

続けて出力の分岐を差し替えます。

差し替え前:

```python
        if is_part:
            add_tag('building:part', 'yes')
        else:
            add_tag('building', building.get('building', 'yes'))
```

差し替え後:

```python
        if is_part:
            add_tag('building:part', building.get('building') or 'yes')
        else:
            add_tag('building', building.get('building') or 'yes')
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_buildings_xml.py -q`
Expected: すべて PASS

- [ ] **Step 5: 全体のテストを走らせる**

Run: `python3 -m pytest -q`
Expected: 487 passed, 26 skipped

- [ ] **Step 6: コミット**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "feat(api): 部材の建物の型を building:part の値として出す"
```

---

### Task 5: 手元の `.osm` で通しの確認をする

**Files:**
- Create: なし（一時ディレクトリで作業し、リポジトリには入れない）

**Interfaces:**
- Consumes: Task 2 と Task 3 の取り込み側の変更

**なぜ要るか:** テストは合成した `.osm` で見ています。
実データの `.osm` に対して、条件を通る relation の割合が設計の見積り
（143 都市で 16.00%）とかけ離れていないことを 1 度は確かめておきます。

- [ ] **Step 1: 手元に `.osm` があるかを確かめる**

```bash
ls -d /tmp/*/[0-9]*/ 2>/dev/null | head; find ~ -name '*.osm' -path '*plateau*' 2>/dev/null | head -3
```

見つからない場合はこの作業をとばし、Task 6 に進みます。
（見つからないこと自体は異常ではありません。前回の変換で退避したものが
消えているだけです。実データでの確認はサーバでの取り込みログで代替します。）

- [ ] **Step 2: 割合を数える**

`.osm` が見つかった場合、その 1 メッシュに対して次を走らせます。
`<OSM_FILE>` は見つかったファイルの絶対パスに置き換えます。

```bash
python3 -c "
import sys, xml.etree.ElementTree as ET
sys.path.insert(0, '.')
from plateau_importer2postgis import _polygon_area_m2, _relation_passes_area_gate
root = ET.parse(sys.argv[1]).getroot()
nodes = {n.get('id'): (float(n.get('lon')), float(n.get('lat'))) for n in root.findall('node')}
refs = {w.get('id'): [nd.get('ref') for nd in w.findall('nd')] for w in root.findall('way')}
total = kept = 0
for r in root.findall('relation'):
    tags = {t.get('k'): t.get('v') for t in r.findall('tag')}
    if tags.get('type') != 'building':
        continue
    parts = [m.get('ref') for m in r.findall('member') if m.get('role') == 'part']
    if not parts:
        continue
    total += 1
    areas = [_polygon_area_m2([nodes[x] for x in refs.get(p, []) if x in nodes]) for p in parts]
    if _relation_passes_area_gate(areas):
        kept += 1
print(f'relation {total} 件のうち {kept} 件 ({kept/total*100:.1f}%) が条件を満たす' if total else 'relation なし')
" <OSM_FILE>
```

Expected: 数パーセントから 20 パーセント程度。
0% または 100% になった場合は、面積の計算か member の読み取りが壊れています。
その場合は先に進まず、`_polygon_area_m2` の入力を 1 件表示して原因を見ます。

- [ ] **Step 3: 結果を記録する（コミットしない）**

数えた値をこのあとのまとめに使います。ファイルには残しません。

---

### Task 6: 公開に出す前の確認

**Files:**
- 変更したすべてのファイル、コミットメッセージ、PR 本文

- [ ] **Step 1: 差分に機微情報が無いことを確かめる**

検査に使う文字列そのものを残さないため、形で見ます。

```bash
git diff main --name-only | tr '\n' ' '
git diff main -U0 | grep -nE '(^\+.*([0-9]{1,3}\.){3}[0-9]{1,3})|(^\+.*/(opt|var|etc|home)/)|(^\+.*sudo )|(^\+.*postgresql://)|(^\+.*(mydns|ik1-))' || echo "当たりなし"
```

Expected: 「当たりなし」。当たった場合はその行を読み、実際に機微かを判断して直します。

- [ ] **Step 2: コミットメッセージを確かめる**

```bash
git log main..HEAD --format='%s%n%b' | grep -nE '(([0-9]{1,3}\.){3}[0-9]{1,3})|(/(opt|var|etc|home)/)|(sudo )|(postgresql://)|((mydns|ik1-))' || echo "当たりなし"
```

Expected: 「当たりなし」

- [ ] **Step 3: 全体のテストと変更の一覧を確かめる**

```bash
python3 -m pytest -q 2>&1 | tail -3 && git log --oneline main..HEAD
```

Expected: 487 passed, 26 skipped と、Task 1 から Task 4 の 4 コミット。

- [ ] **Step 4: PR を作る**

```bash
git push -u origin design/building-relation-area-gate
```

PR 本文には、設計文書へのリンク、変更した 2 箇所、Task 5 で数えた割合を書きます。
サーバのホスト名、パス、接続文字列、操作手順は書きません。
作成後に本文へ同じ検査をかけます。

```bash
gh pr view --json body --jq '.body' | grep -nE '(([0-9]{1,3}\.){3}[0-9]{1,3})|(/(opt|var|etc|home)/)|(sudo )|(postgresql://)|((mydns|ik1-))' || echo "当たりなし"
```

Expected: 「当たりなし」

---

## この計画が扱わないこと

- サーバへの反映と 145 都市の取り込み。実装が入って PR がマージされてから別途行います
- 既に取り込んだ 148 都市の変換のやり直し。3 から 4 時間かかるので別の作業にします
- 閾値 300 が写真と合っているかの抜き取り確認。設計文書の「測っていないこと」に残っています
- 2026-08-28 より前に取り込まれた部材の行は `building` 列が空のままです。
  配信は `yes` に落とすので壊れませんが、型は取り込み直すまで戻りません
