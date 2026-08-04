# 取り込みを元データに忠実にする 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 変換出力のうち元データに対応する形状だけを取り込み、内側リング（中庭）を保持する。

**Architecture:** 取り込み側だけを変更する。判定は「建物 ID を持つ way」「`building` タグ付き `type=multipolygon`」の 2 経路に絞り、合成形状と `type=building` の relation は使わない。内側リングはノード表に環番号を持たせて表現し、API はそれを `type=multipolygon` として出力する。

**Tech Stack:** Python 3, FastAPI, psycopg2, PostgreSQL 16 + PostGIS 3.4, pytest

## Global Constraints

- 比較対象は**変換出力**である。元データと変換出力の差は変換器の責務で、本計画では扱わない。
- 建物 ID を持たない建物 way は保存しない。10 メッシュ 386 本すべてが元データに対応しないことを確認済み。
- 識別子の欠落を理由に実在建物を捨てない。`type=multipolygon` は識別子が無くても取り込む。
- `inner` メンバーの way は独立した建物として保存しない。親の建物の内側リングとして保存する。付いている `building:part=yes` は使わない。
- `parent_building_id` は新規取り込みで NULL のままにする。列は削除しない。
- `plateau_buildings.geom` は `geometry(Polygon,4326)` のままでよい。内側リングを保持できることを実 DB で確認済み。
- API の出力に `ref:MLIT_PLATEAU` を含めない（#30 の方針）。
- 三角形の面積判定の不具合は本計画の対象外。別 Issue。
- spec: `docs/superpowers/specs/2026-08-04-importer-source-fidelity-design.md`

---

### Task 1: 合成形状と type=building の relation を使わない

**Files:**
- Modify: `plateau_importer2postgis.py:395-420`（relation の解析）
- Modify: `plateau_importer2postgis.py:450-490`（way の収集）
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Produces: `parse_osm_file_safe` が返す `buildings` に、建物 ID を持たない建物 way が含まれなくなる。
- Produces: 同じく `parent_outline_way_id` が常に `None` になり、`process_buildings_safe` が返す `parts_parent_map` が空になる。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の末尾に追加する。

```python
_SYNTH_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.001" lon="133.0"/>
  <node id="-3" lat="33.001" lon="133.001"/>
  <node id="-4" lat="33.0" lon="133.001"/>
  <node id="-5" lat="33.0002" lon="133.0002"/>
  <node id="-6" lat="33.0004" lon="133.0002"/>
  <node id="-7" lat="33.0004" lon="133.0004"/>
  <node id="-8" lat="33.0002" lon="133.0004"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
    <tag k="building" v="yes"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outline"/>
    <member type="way" ref="-20" role="part"/>
    <tag k="type" v="building"/>
    <tag k="building" v="yes"/>
  </relation>
</osm>
""")


class TestDropSynthesizedShapes:
    """変換器が作った合成形状を取り込まない。

    融合で作られた外形は建物 ID を持たない。10 メッシュ 386 本すべてが
    元データの lod0FootPrint と一致しないことを確認済み。
    """

    def _parse(self, bare_importer, xml):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        return importer.parse_osm_file_safe(osm_file)

    def test_way_without_building_id_is_not_collected(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        way_ids = {b['way_id'] for b in buildings}
        assert '-10' not in way_ids, '合成外形が収集されている'
        assert '-20' in way_ids, '建物 ID を持つ way が落ちている'

    def test_no_parent_link_is_produced(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        assert all(b['parent_outline_way_id'] is None for b in buildings)
```

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestDropSynthesizedShapes -v`
Expected: `test_way_without_building_id_is_not_collected` が FAIL（`-10` が収集される）。`test_no_parent_link_is_produced` も FAIL（`-20` の親が `-10` になる）。

- [ ] **Step 3: relation の解析をやめる**

`plateau_importer2postgis.py` の `part_to_outline` を組み立てるループ（395 行付近）を、コメントごと次で置き換える。

```python
        # type=building の relation は読まない。
        # この relation は「接触する建物を融合したまとまり」であり、outline は
        # 融合で作られた形状、part は取り込まれた実在建物である。元データの
        # BuildingPart に由来するものではない（変換器はそれを読んでいない）。
        # 親子関係を作ると実在しない建物を親にすることになるため、作らない。
        part_to_outline = {}
```

- [ ] **Step 4: 建物 ID を持たない way を収集しない**

`plateau_importer2postgis.py:450` 付近の way 収集で、`is_building` / `is_part` の判定の直後に挿入する。

```python
            # 建物 ID を持たない建物 way は、融合で作られた合成形状である。
            # 元データに対応する形状が無いので取り込まない (10 メッシュ 386 本で確認)。
            ref_mlit = tags.get('ref:MLIT_PLATEAU')
            if (is_building or is_part) and not ref_mlit:
                continue
```

- [ ] **Step 5: 建物 ID を持つ part を建物として扱う**

融合で `building:part` に降格された way も、建物 ID を持つなら元は独立した建物である。
`parse_osm_file_safe` の `buildings.append` で `is_part` を渡している箇所を差し替える。

```python
                    # 建物 ID を持つ way は、building:part に降格されていても
                    # 元は独立した建物である。変換器は CityGML の BuildingPart を
                    # 読まないので、真の部分立体は出力に存在しない。
                    'is_part': False,
```

`convert_building_tags_enhanced` に渡るタグはそのままでよい。`building` タグが無い場合は
`building_value` が `converted_tags.get('building', 'yes')` で `'yes'` になる。

テストを `TestDropSynthesizedShapes` に足す。

```python
    def test_part_with_building_id_becomes_a_building(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        by_id = {b['way_id']: b for b in buildings}
        assert by_id['-20']['is_part'] is False
```

- [ ] **Step 6: テストが通ることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestDropSynthesizedShapes -v`
Expected: 3 passed

- [ ] **Step 7: 全テストを実行する**

Run: `python -m pytest -q`
Expected: 既存の 228 passed が維持され、新規 3 件が加わって 231 passed

既存テストで `parent_outline_way_id` を期待するものがあれば、`type=building` の relation を読まなくなった旨をコメントに書いたうえで期待値を修正する。テスト自体を削除しない。

- [ ] **Step 8: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "fix: stop importing shapes the source does not contain"
```

---

### Task 2: ノード表に環番号を持たせる

**Files:**
- Modify: `plateau_importer2postgis.py:114-152`（`_ensure_schema`）
- Modify: `plateau_importer2postgis.py:667`（`building_nodes.append`）
- Modify: `plateau_importer2postgis.py:906-940`（`_dedupe_and_remap_nodes`）
- Modify: `plateau_importer2postgis.py:1012` と `:1267`（ノードの INSERT、2 箇所）
- Modify: `tests/conftest.py`（テスト用スキーマ 2 箇所）
- Test: `tests/test_plateau_importer2postgis.py`, `tests/test_ref_mlit_plateau_schema.py`

**Interfaces:**
- Produces: `plateau_building_nodes.ring_id INTEGER NOT NULL DEFAULT 0`。0 が外側、1 以降が内側。
- Produces: `nodes_data` の行が 8 要素になる。`(osm_id, building_id, sequence_id, lat, lon, lon, lat, ring_id)`。
- Consumes: Task 1 の変更。本タスクでは挙動を変えない。すべて `ring_id=0` になる。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の `TestRefMlitPlateau` の下に追加する。

```python
class TestNodeRingId:
    """ノード行が環番号を持つ。内側リングを表現するための土台。

    本タスクでは挙動を変えない。穴の無い建物はすべて 0 になる。
    """

    def test_simple_building_nodes_are_ring_zero(self, bare_importer):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_MIN_OSM.replace(
            '<tag k="building" v="yes"/>',
            '<tag k="building" v="yes"/>\n    <tag k="ref:MLIT_PLATEAU" v="35215-bldg-1"/>'
        ))
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['node_refs'] = [f'{key}:{r}' for r in b['node_refs']]
        _, nodes_data, _ = importer.process_buildings_safe(all_nodes, buildings)

        assert nodes_data, 'ノード行が空'
        # 行のレイアウト: (osm_id, building_id, seq, lat, lon, lon, lat, ring_id)
        assert all(len(row) == 8 for row in nodes_data)
        assert all(row[7] == 0 for row in nodes_data)
```

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestNodeRingId -v`
Expected: FAIL（行が 7 要素）

- [ ] **Step 3: 列を冪等に追加する**

`_ensure_schema` の `SELECT column_name` を差し替える。

```python
                cur.execute("""
                    SELECT table_name, column_name FROM information_schema.columns
                    WHERE (table_name='plateau_buildings'
                           AND column_name IN ('building_part', 'parent_building_id',
                                               'ref_mlit_plateau'))
                       OR (table_name='plateau_building_nodes'
                           AND column_name = 'ring_id')
                """)
                existing = {(row[0], row[1]) for row in cur.fetchall()}
```

これに合わせて既存の判定を `('plateau_buildings', 'building_part') not in existing` の形に直し、末尾に追加する。

```python
                if ('plateau_building_nodes', 'ring_id') not in existing:
                    cur.execute(
                        "ALTER TABLE plateau_building_nodes "
                        "ADD COLUMN ring_id INTEGER NOT NULL DEFAULT 0"
                    )
                    added.append('ring_id')
```

- [ ] **Step 4: 行に環番号を足す**

`plateau_importer2postgis.py:667` の `building_nodes.append` を差し替える。

```python
                        building_nodes.append((
                            unique_node_id,        # id（負の値）
                            self.building_id_counter,  # building_id
                            seq,                   # sequence_id
                            lat,                   # lat
                            lon,                   # lon
                            lon,                   # ST_Point用 lon
                            lat,                   # ST_Point用 lat
                            0,                     # ring_id (0=外側)
                        ))
```

- [ ] **Step 5: 重複排除と INSERT を合わせる**

`_dedupe_and_remap_nodes` の `mapped.append` を差し替える。

```python
            mapped.append((node_data[0], db_building_id, node_data[2],
                           node_data[3], node_data[4], node_data[5], node_data[6],
                           node_data[7]))
```

ノードの INSERT を 2 箇所とも差し替える（1012 行と 1267 行）。

```sql
                        INSERT INTO plateau_building_nodes (osm_id, building_id, sequence_id, lat, lon, geom, ring_id)
                        VALUES %s
```

```python
                        template="(%s, %s, %s, %s, %s, ST_Point(%s, %s), %s)",
```

`tests/conftest.py` の `plateau_building_nodes` の定義 2 箇所に `ring_id INTEGER NOT NULL DEFAULT 0` を足す。

- [ ] **Step 6: テストが通ることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestNodeRingId -v`
Expected: 1 passed

- [ ] **Step 7: 実 DB で列が追加されることを確認するテストを足す**

`tests/test_ref_mlit_plateau_schema.py` の `TestEnsureSchemaAddsRefColumn` に追加する。

```python
    def test_ring_id_is_added_to_nodes(
        self, fresh_plateau_full_schema, integration_db_url, tmp_path, monkeypatch
    ):
        conn = fresh_plateau_full_schema
        cur = conn.cursor()
        cur.execute("ALTER TABLE plateau_building_nodes DROP COLUMN IF EXISTS ring_id")
        conn.commit()

        monkeypatch.setattr(PlateauImporter2PostGIS, '_test_connection', lambda self: None)
        monkeypatch.setattr(
            PlateauImporter2PostGIS, '_initialize_id_counters', lambda self: None
        )
        data_dir = tmp_path / '35215'
        data_dir.mkdir()
        importer = PlateauImporter2PostGIS(
            data_dir=str(data_dir), postgres_url=integration_db_url, citycode='35215'
        )
        importer._ensure_schema()

        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'plateau_building_nodes'
        """)
        assert 'ring_id' in {r[0] for r in cur.fetchall()}
```

- [ ] **Step 8: 全テストを実行する**

Run: `python -m pytest -q`
Expected: 232 passed

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration`
Expected: 失敗なし。環境変数が無い場合は skip でよい。

キャッシュに注意する。実装を一時的に壊して確認するときは、その前後で
`find . -name "__pycache__" -type d -exec rm -rf {} +` を実行する。
同サイズの改変を同じ秒に戻すと古いバイトコードが使われ、確認が無意味になる。

- [ ] **Step 9: コミット**

```bash
git add plateau_importer2postgis.py tests/
git commit -m "feat: give node rows a ring index"
```

---

### Task 3: 穴のある建物を取り込む

**Files:**
- Modify: `plateau_importer2postgis.py:395-420`（relation の解析。Task 1 で空にした箇所）
- Modify: `plateau_importer2postgis.py:450-490`（way の収集）
- Modify: `plateau_importer2postgis.py:640-700`（`process_buildings_safe` の多角形組み立て）
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Consumes: Task 2 の `ring_id`
- Produces: `buildings` の要素が `rings` を持つ。`[[外側の node_refs], [内側の node_refs], ...]`。単純な建物は 1 要素。
- Produces: `process_buildings_safe` が内側リングを持つ建物に `POLYGON((外側),(内側))` を作る。

- [ ] **Step 1: 失敗するテストを書く**

```python
_HOLE_OSM = textwrap.dedent("""\
<?xml version="1.0" encoding="UTF-8"?>
<osm version="0.6">
  <node id="-1" lat="33.0" lon="133.0"/>
  <node id="-2" lat="33.001" lon="133.0"/>
  <node id="-3" lat="33.001" lon="133.001"/>
  <node id="-4" lat="33.0" lon="133.001"/>
  <node id="-5" lat="33.0003" lon="133.0003"/>
  <node id="-6" lat="33.0007" lon="133.0003"/>
  <node id="-7" lat="33.0007" lon="133.0007"/>
  <node id="-8" lat="33.0003" lon="133.0007"/>
  <way id="-10">
    <nd ref="-1"/><nd ref="-2"/><nd ref="-3"/><nd ref="-4"/><nd ref="-1"/>
  </way>
  <way id="-20">
    <nd ref="-5"/><nd ref="-6"/><nd ref="-7"/><nd ref="-8"/><nd ref="-5"/>
    <tag k="building:part" v="yes"/>
  </way>
  <relation id="-30">
    <member type="way" ref="-10" role="outer"/>
    <member type="way" ref="-20" role="inner"/>
    <tag k="type" v="multipolygon"/>
    <tag k="building" v="public"/>
    <tag k="height" v="14.6"/>
  </relation>
</osm>
""")


class TestHoleBuilding:
    """穴のある建物は type=multipolygon で出力される。

    周南市 10 メッシュで 15 個。すべて実在建物で、合成形状は含まれない。
    inner の way には building:part=yes が付くが、これは建物ではなく穴である。
    """

    def _rows(self, bare_importer):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_HOLE_OSM)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer.process_buildings_safe(all_nodes, buildings)

    def test_hole_building_is_one_row(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert len(buildings_data) == 1, '穴が別の建物として保存されている'

    def test_geometry_has_an_interior_ring(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        wkt = buildings_data[0][8]   # geometry_wkt
        assert wkt.count('(') >= 3, f'内側リングが無い: {wkt[:80]}'

    def test_inner_nodes_carry_ring_one(self, bare_importer):
        _, nodes_data, _ = self._rows(bare_importer)
        rings = {row[7] for row in nodes_data}
        assert rings == {0, 1}, f'環番号が {rings}'

    def test_tags_come_from_the_relation(self, bare_importer):
        buildings_data, _, _ = self._rows(bare_importer)
        assert buildings_data[0][1] == 'public'   # building
        assert buildings_data[0][2] == '14.6'     # height
```

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestHoleBuilding -v`
Expected: 4 件とも FAIL（`b['rings']` が無いため KeyError）

- [ ] **Step 3: way を環の一覧として持つ**

`parse_osm_file_safe` の way 収集で、`buildings.append` の辞書に `'rings': [nd_refs]` を足す。
`node_refs` は残す（既存のコードが参照している）。

- [ ] **Step 4: multipolygon を読む**

Task 1 で空にした relation 解析の位置に挿入する。

```python
        # building タグを持つ type=multipolygon は、穴のある建物である。
        # outer を外側、inner を内側のリングとして 1 棟にまとめる。
        # inner の way には building:part=yes が付くが、これは建物ではなく穴なので
        # 独立した建物として収集しない。
        mp_inner_way_ids = set()
        mp_buildings = []
        for rel_elem in root.findall('relation'):
            rel_tags = {t.get('k'): t.get('v') for t in rel_elem.findall('tag')
                        if t.get('k') and t.get('v')}
            if rel_tags.get('type') != 'multipolygon' or 'building' not in rel_tags:
                continue
            outer, inners = [], []
            for m in rel_elem.findall('member'):
                if m.get('type') != 'way':
                    continue
                if m.get('role') == 'outer':
                    outer.append(m.get('ref'))
                elif m.get('role') == 'inner':
                    inners.append(m.get('ref'))
                    mp_inner_way_ids.add(m.get('ref'))
            if len(outer) == 1:
                mp_buildings.append((rel_elem.get('id'), rel_tags, outer[0], inners))
```

way の収集ループで、`mp_inner_way_ids` と outer の way を通常の建物として収集しないようにする。
そのうえで `mp_buildings` を、環の一覧を持つ建物として `buildings` に加える。

- [ ] **Step 5: 多角形の組み立てを環に対応させる**

`process_buildings_safe` の座標収集（653-680 行付近）を差し替える。
`node_refs` を 1 本読むかわりに `rings` を環ごとに読む。

```python
                tags = building['tags']
                source_file = building['source_file']

                # 環ごとに座標を集める。rings[0] が外側、以降が内側。
                ring_coords = []
                building_nodes = []
                for ring_no, refs in enumerate(building['rings']):
                    coords = []
                    for seq, original_node_ref in enumerate(refs):
                        if original_node_ref not in all_nodes:
                            continue
                        node_data = all_nodes[original_node_ref]
                        lat, lon = node_data['lat'], node_data['lon']
                        coords.append((lon, lat))
                        building_nodes.append((
                            node_data['unique_id'],
                            self.building_id_counter,
                            seq,
                            lat, lon, lon, lat,
                            ring_no,
                        ))
                    if coords:
                        ring_coords.append(coords)

                coords = ring_coords[0] if ring_coords else []
```

多角形の WKT を作る箇所（`polygon_wkt = f"POLYGON(({coords_str}))"`）を差し替える。

```python
                            # 外側と内側をまとめて 1 つの POLYGON にする。
                            # 内側リングは中庭であり、塗りつぶさない。
                            ring_wkts = []
                            for ring in ring_coords:
                                r = list(ring)
                                if r[0] != r[-1]:
                                    r.append(r[0])
                                if len(r) < 4:
                                    continue
                                ring_wkts.append(
                                    '(' + ','.join(f"{lon} {lat}" for lon, lat in r) + ')'
                                )
                            polygon_wkt = f"POLYGON({','.join(ring_wkts)})"
```

面積の検算と重複の判定は外側の環（`coords`）に対して行う。既存のコードのままでよい。

内側リングだけが不正な場合に建物全体を捨てないこと。外側が有効なら保存する。

- [ ] **Step 6: テストが通ることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestHoleBuilding -v`
Expected: 4 passed

- [ ] **Step 7: 全テストを実行する**

Run: `python -m pytest -q`
Expected: 236 passed

- [ ] **Step 8: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "feat: keep the courtyards the source has"
```

---

### Task 4: API が穴のある建物をマルチポリゴンで返す

**Files:**
- Modify: `osmfj_plateau_api.py:173-265`（クエリ。`ring_id` を返す）
- Modify: `osmfj_plateau_api.py:444-590`（XML 生成）
- Test: `tests/test_buildings_xml.py`

**Interfaces:**
- Consumes: Task 3 が保存した `ring_id`
- Produces: 内側リングを持つ建物が `type=multipolygon` の relation として出力される。outer と inner の way を伴う。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_buildings_xml.py` の末尾に追加する。

```python
class TestMultipolygonOutput:
    """穴のある建物は type=multipolygon で返す。

    タグは relation に付ける。変換器の出力形式と同じにする。
    """

    def _hole_building(self):
        def ring(base, lat, lon, d, r):
            return [{'id': base + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': r}
                    for i, (la, lo) in enumerate([
                        (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)])]
        return {
            'id': 1, 'building': 'public', 'height': 14.6, 'building_part': None,
            'parent_building_id': None,
            'nodes': ring(100, 33.0, 133.0, 0.001, 0) + ring(200, 33.0003, 133.0003, 0.0004, 1),
        }

    def test_relation_is_multipolygon(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building()]))
        rel = root.find('relation')
        assert rel is not None, 'relation が出ていない'
        tags = {t.get('k'): t.get('v') for t in rel.findall('tag')}
        assert tags.get('type') == 'multipolygon'
        assert tags.get('building') == 'public'

    def test_roles_are_outer_and_inner(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building()]))
        roles = sorted(m.get('role') for m in root.find('relation').findall('member'))
        assert roles == ['inner', 'outer']

    def test_ways_carry_no_building_tag(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building()]))
        for w in root.findall('way'):
            tags = {t.get('k') for t in w.findall('tag')}
            assert 'building' not in tags, 'タグは relation に付ける'

    def test_building_without_hole_is_still_one_way(self, api):
        b = self._hole_building()
        b['nodes'] = [n for n in b['nodes'] if n['ring_id'] == 0]
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.find('relation') is None
        assert len(root.findall('way')) == 1
```

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_buildings_xml.py::TestMultipolygonOutput -v`
Expected: 最初の 3 件が FAIL。`test_building_without_hole_is_still_one_way` は PASS。

- [ ] **Step 3: クエリに環番号を足す**

`osmfj_plateau_api.py` の LATERAL でノードを集める箇所（251-263 行）の `json_build_object` に `'ring_id', n.ring_id` を足し、`ORDER BY n.ring_id, n.sequence_id` に変える。

- [ ] **Step 4: 環ごとに way を作る**

XML 生成のループで、ノードを環ごとに分ける。`valid_nodes` を作る直前に挿入する。

```python
                # 環ごとに分ける。ring_id が無い行は 0 とみなす (移行中のデータ)。
                rings = {}
                for node in nodes:
                    if not node:
                        continue
                    rings.setdefault(node.get('ring_id', 0) or 0, []).append(node)
                if not rings:
                    continue
```

環が 1 つのときは既存の処理をそのまま使う（`nodes = rings[0]` として続ける）。

環が 2 つ以上のときは、環ごとにタグの無い way を作り、relation でまとめる。
way の id は建物の id から導く。外側を `-building_db_id`、内側を
`RING_ID_OFFSET - building_db_id * 100 - ring_no` のように衝突しない値にする。
`RELATION_ID_OFFSET` と同じ考え方で定数を 1 つ足す。

```python
                    rel_elem = ET.Element('relation')
                    rel_elem.set('id', str(self.RELATION_ID_OFFSET - building_db_id))
                    for attr, val in (('visible', 'true'), ('version', '1'),
                                      ('changeset', '1'), ('timestamp', timestamp),
                                      ('user', 'osmfj-plateau'), ('uid', '1')):
                        rel_elem.set(attr, val)
                    for ring_no in sorted(rings):
                        m = ET.SubElement(rel_elem, 'member')
                        m.set('type', 'way')
                        m.set('ref', str(ring_way_ids[ring_no]))
                        m.set('role', 'outer' if ring_no == 0 else 'inner')
                    type_tag = ET.SubElement(rel_elem, 'tag')
                    type_tag.set('k', 'type')
                    type_tag.set('v', 'multipolygon')
                    self._emit_building_tags(rel_elem, building, is_part=False)
                    all_relations.append(rel_elem)
```

タグは relation にだけ付ける。way には付けない。
`_emit_building_tags` はそのまま使う。

- [ ] **Step 5: テストが通ることを確認する**

Run: `python -m pytest tests/test_buildings_xml.py::TestMultipolygonOutput -v`
Expected: 4 passed

- [ ] **Step 6: 全テストを実行する**

Run: `python -m pytest -q`
Expected: 240 passed

- [ ] **Step 7: コミット**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "feat: serve buildings with courtyards as multipolygons"
```

---

### Task 5: 実データで検証する

**Files:**
- 変更なし。測定のみ。

**Interfaces:**
- Consumes: Task 1〜4

- [ ] **Step 1: 変換済みの周南市データを用意する**

`51310655` と `51310636` の変換済み `.osm` を作業ディレクトリに置く。
未作成なら citygml-osm 3.0.6 で変換する。conversion.json を作業ディレクトリに置き、Java 17 で `1st` を渡す。
引数はモード名であり、ファイル名ではない。

- [ ] **Step 2: テスト DB に取り込む**

Run: `python plateau_importer2postgis.py --data-dir <dir> --postgres-url "postgresql:///plateau_api_test" --no-zip`

- [ ] **Step 3: 建物数を確認する**

変換出力の「建物 ID を持つ way」＋「`building` タグ付き multipolygon」の数と、DB の行数を比べる。
三角形の不具合の分だけ DB が少なくなる。その差が三角形の建物だけであることを確認する。

- [ ] **Step 4: 「建物の中に建物」を確認する**

```sql
SELECT count(*) FROM plateau_buildings a JOIN plateau_buildings b ON a.id <> b.id
WHERE ST_Contains(ST_MakeValid(a.geom), ST_MakeValid(b.geom));
```

Expected: 0（現在の取り込みでは 2 メッシュで 155）

- [ ] **Step 5: 内側リングを確認する**

```sql
SELECT count(*) FROM plateau_buildings WHERE ST_NumInteriorRings(geom) > 0;
```

Expected: 6（51310655 で 1、51310636 で 5）

- [ ] **Step 6: 親子リンクを確認する**

```sql
SELECT count(*) FROM plateau_buildings WHERE parent_building_id IS NOT NULL;
```

Expected: 0

- [ ] **Step 7: API の応答を確認する**

該当の bbox でエンドポイントを叩き、穴のある建物が `type=multipolygon` として返り、その way にタグが付いていないことを確認する。

- [ ] **Step 8: 測定結果を spec に追記してコミット**

```bash
git add docs/superpowers/specs/2026-08-04-importer-source-fidelity-design.md
git commit -m "docs: record the measurements after implementation"
```
