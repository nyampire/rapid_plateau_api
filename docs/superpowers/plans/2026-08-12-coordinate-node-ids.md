# 座標から決まるノード id 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** API が返すノード id を、応答に含まれる建物ではなく座標だけから決まる値にして、取得する範囲をまたいでも同じ角が同じノードになるようにする。

**Architecture:** `osmfj_plateau_api.py` の応答組み立てにある `coord_to_nid`（応答の中で最初に来た建物の DB ノード行 id を採用する辞書）を廃止し、座標を引数に取る純粋な関数 `_node_id` に置き換える。DB のスキーマ、取り込み、Rapid のコードはいずれも変更しない。

**Tech Stack:** Python 3、FastAPI、`xml.etree.ElementTree`、pytest

設計: `docs/superpowers/specs/2026-08-12-coordinate-node-ids-design.md`
Issue: [#51](https://github.com/nyampire/rapid_plateau_api/issues/51)

## Global Constraints

- 対象ファイルは `osmfj_plateau_api.py` と `tests/test_buildings_xml.py` の 2 つだけ。DB のスキーマ、`plateau_importer2postgis.py`、Rapid 側には一切触らない
- 採番の定数は `PlateauAPI` クラスの定数として置く。既存の `WAY_ID_RING_MULTIPLIER` と同じ場所、同じ書き方に合わせる
- `lat_i` と `lon_i` は **先に `round(lat, 7)` で 7 桁に丸め、その値を `1e7` 倍して丸め、あとから原点を引く**。`(lat - 20.0) * 1e7` の順で書かない（引き算の誤差が入る）。また `round(lat * 1e7)` のように丸めずに掛けない（掛け算の時点の浮動小数点誤差で、出力の `f"{lat:.7f}"` と丸めが食い違う半端値がある）。両方を満たして初めて `f"{lat:.7f}"` と同じ丸めになる
- ノード id は負で 0 以外。範囲外の座標だけ、警告を出して `-node_data['id']` に落ちる
- コミットは既存の履歴に合わせ、`Co-Authored-By` の trailer を付けない
- テストの実行は `python3 -m pytest`

---

### Task 1: 座標から id を返す関数と定数

**Files:**
- Modify: `osmfj_plateau_api.py`（`WAY_ID_RING_MULTIPLIER` の定義の直後、現在 367-378 行あたり）
- Test: `tests/test_buildings_xml.py`（末尾に新しいクラスを追加）

**Interfaces:**
- Produces: `PlateauAPI._node_id(self, lat: float, lon: float, db_node_id: int) -> int`。座標から決まる負の id を返す。範囲外のときだけ `-db_node_id` を返し `logger.warning` を出す
- Produces: 定数 `NODE_COORD_SCALE`、`NODE_LAT_OFFSET`、`NODE_LON_OFFSET`、`NODE_LAT_STEPS`、`NODE_LON_STEPS`

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_buildings_xml.py` の末尾に追加する。

```python
# ----------------------------------------------------------------------
# 座標から決まるノード id (#51)
# ----------------------------------------------------------------------

class TestNodeIdFromCoordinate:
    """ノード id が座標だけで決まること。

    応答に含まれる建物で id が変わると、同じ角が取得のたびに別のノードとして
    Rapid に届き、承認すると重複ノードとして OSM に上がる (#51)。
    """

    def test_id_is_negative_and_nonzero(self, api):
        nid = api._node_id(35.7000000, 139.7000000, 12345)
        assert nid < 0

    def test_id_fits_in_int64(self, api):
        # 範囲の隅がもっとも大きな値になる
        nid = api._node_id(46.0 - 1e-7, 154.0 - 1e-7, 1)
        assert abs(nid) < 2 ** 63

    def test_same_coordinate_gives_same_id_regardless_of_db_row(self, api):
        # 同じ角に属する DB の行は建物ごとに違う id を持つ
        assert api._node_id(32.6445365, 130.6984598, 272155034) == \
               api._node_id(32.6445365, 130.6984598, 272163761)

    def test_distinct_coordinates_never_collide(self, api):
        seen = set()
        for i in range(200):
            for j in range(200):
                nid = api._node_id(32.6445365 + i * 1e-7,
                                   130.6984598 + j * 1e-7, 1)
                seen.add(nid)
        assert len(seen) == 200 * 200

    def test_id_matches_printed_coordinate(self, api):
        # 出力は f"{lat:.7f}" で丸めるので、丸めた値と生の値で id が割れてはならない
        lat, lon = 35.70000004999, 139.70000004999
        assert api._node_id(lat, lon, 1) == \
               api._node_id(float(f'{lat:.7f}'), float(f'{lon:.7f}'), 1)

    def test_out_of_range_falls_back_to_db_id(self, api, caplog):
        with caplog.at_level(logging.WARNING):
            nid = api._node_id(60.0, 139.7, 4242)   # 緯度が範囲外
        assert nid == -4242
        assert any('範囲外' in r.message for r in caplog.records)
```

- [ ] **Step 2: テストが失敗することを確認する**

Run: `python3 -m pytest tests/test_buildings_xml.py::TestNodeIdFromCoordinate -v`
Expected: FAIL。`AttributeError: 'PlateauAPI' object has no attribute '_node_id'`

- [ ] **Step 3: 定数と関数を実装する**

`osmfj_plateau_api.py` の `WAY_ID_RING_MULTIPLIER = 1000` から始まる定数群の直後、
`_way_id` の定義の前に追加する。

```python
    # ノードの id は建物ではなく座標から決める。
    #
    #   node : -(1 + lat_i * NODE_LON_STEPS + lon_i)
    #
    # way と relation は建物 id から決めているが、ノードだけは違う規則を使う。
    # 同じ角が複数の建物に属し、DB にはその建物の数だけ行があるためで、
    # 行の id から決めると「その応答で先に来た建物」で値が変わる (#51)。
    #
    # 範囲は緯度 20..46 度、経度 122..154 度。沖ノ鳥島から択捉島、
    # 与那国島から南鳥島までを含む。
    #
    # 単射である理由: lon_i の最大値が NODE_LON_STEPS より小さいので、
    # 商と余りが一意に決まり、lat_i と lon_i の両方が一致するしかない。
    #
    # 桁: 最大 83,200,000,580,000,001 で int64 の上限の約 111 分の 1。
    # way id の側にある「JavaScript の安全整数 9e15」の見積もりはこちらには
    # 当てはまらない。Rapid は entityID を文字列として持ち、数値に変換するのは
    # relation を並べ替える osmRelation.creationOrder だけで、ノードは通らない。
    # アップロードを受ける cgimap は仮 id を int64 で読み、条件は「負」と
    # 「0 でない」の 2 つだけである。
    NODE_COORD_SCALE = 10_000_000        # 7 桁
    NODE_LAT_OFFSET = 200_000_000        # 20.0 * NODE_COORD_SCALE
    NODE_LON_OFFSET = 1_220_000_000      # 122.0 * NODE_COORD_SCALE
    NODE_LAT_STEPS = 260_000_001         # 緯度 20..46 度の刻み数
    NODE_LON_STEPS = 320_000_001         # 経度 122..154 度の刻み数

    def _node_id(self, lat: float, lon: float, db_node_id: int) -> int:
        """座標から決まるノードの合成 OSM id。

        範囲外の座標だけ、警告を出して DB の行 id に落ちる。丸めた値が範囲を
        外れると他の座標と id が衝突し、Rapid が後から来た角を捨てて way の形が
        壊れるため、id の安定性より出力の完全性を優先する。
        """
        # 先に 1e7 倍してから原点を引く。原点を先に引いて掛けると引き算の
        # 誤差でずれることがあるため、掛け算を先にするのはそれを避けるためで、
        # これだけでは出力の f"{lat:.7f}" と丸めが一致する保証にはならない。
        # 1e7 倍した時点の lat は既に浮動小数点の丸め誤差を含んでいるので、
        # その積を丸めても "35.00000015" のような half-way 値では
        # f"{lat:.7f}" の丸めと食い違うことがある。round(lat, 7) で先に
        # 7 桁に丸めてから 1e7 倍することで、f"{lat:.7f}" と同じ丸めが得られる。
        lat_i = round(round(lat, 7) * self.NODE_COORD_SCALE) - self.NODE_LAT_OFFSET
        lon_i = round(round(lon, 7) * self.NODE_COORD_SCALE) - self.NODE_LON_OFFSET

        if not (0 <= lat_i < self.NODE_LAT_STEPS and 0 <= lon_i < self.NODE_LON_STEPS):
            logger.warning(
                f"⚠️ ノード座標が範囲外: ({lat}, {lon})。"
                f"DB の行 id {db_node_id} に落とす (#51 の症状が残る)"
            )
            return -db_node_id

        return -(1 + lat_i * self.NODE_LON_STEPS + lon_i)
```

- [ ] **Step 4: テストが通ることを確認する**

Run: `python3 -m pytest tests/test_buildings_xml.py::TestNodeIdFromCoordinate -v`
Expected: PASS (6 passed)

- [ ] **Step 5: コミットする**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "feat(api): 座標から決まるノード id の採番を足す

way と relation は建物 id から決めているが、ノードは同じ角が複数の建物に
属するため、行の id から決めると応答ごとに値が変わる (#51)。

この commit では採番の関数だけを足し、応答の組み立てはまだ差し替えない。"
```

---

### Task 2: 応答の組み立てを差し替える

**Files:**
- Modify: `osmfj_plateau_api.py`（`buildings_to_osm_xml` の中。現在 476-499 行の `coord_to_nid` まわりと、577-582 行の `_make_way_elem` の中）
- Test: `tests/test_buildings_xml.py`（`TestNodeIdFromCoordinate` に追加）

**Interfaces:**
- Consumes: Task 1 の `PlateauAPI._node_id`
- Produces: `coord_to_nid` と `_coord_key` が消える。`emitted_node_ids` は残る

- [ ] **Step 1: 失敗するテストを書く**

`TestNodeIdFromCoordinate` クラスの末尾に追加する。

```python
    def test_shared_corner_id_does_not_depend_on_which_buildings_are_present(self, api):
        """#51 の再現。隣の建物が応答に入るかどうかで角の id が変わってはならない。"""
        corner = {'lat': 32.6445365, 'lon': 130.6984598}
        b1 = _make_building(building_id=1, building='yes', nodes=[
            {'id': 272155033, 'lat': 32.6444162, 'lon': 130.6988233},
            {'id': 272155034, **corner},
            {'id': 272155035, 'lat': 32.6444965, 'lon': 130.6984413},
        ])
        # 隣の建物。同じ角を自分の行 id で持つ
        b2 = _make_building(building_id=2, building='yes', nodes=[
            {'id': 272163761, **corner},
            {'id': 272163762, 'lat': 32.6446000, 'lon': 130.6984598},
            {'id': 272163763, 'lat': 32.6446000, 'lon': 130.6985000},
        ])

        def corner_id(xml_str):
            root = ET.fromstring(xml_str)
            hit = [n for n in root.findall('node')
                   if (n.get('lat'), n.get('lon')) == ('32.6445365', '130.6984598')]
            assert len(hit) == 1
            return hit[0].get('id')

        both = corner_id(api.buildings_to_osm_xml([b1, b2]))
        only_b2 = corner_id(api.buildings_to_osm_xml([b2]))
        reversed_order = corner_id(api.buildings_to_osm_xml([b2, b1]))

        assert both == only_b2 == reversed_order

    def test_adjacent_buildings_share_the_corner_node(self, api):
        corner = {'lat': 32.6445365, 'lon': 130.6984598}
        b1 = _make_building(building_id=1, building='yes', nodes=[
            {'id': 272155033, 'lat': 32.6444162, 'lon': 130.6988233},
            {'id': 272155034, **corner},
            {'id': 272155035, 'lat': 32.6444965, 'lon': 130.6984413},
        ])
        b2 = _make_building(building_id=2, building='yes', nodes=[
            {'id': 272163761, **corner},
            {'id': 272163762, 'lat': 32.6446000, 'lon': 130.6984598},
            {'id': 272163763, 'lat': 32.6446000, 'lon': 130.6985000},
        ])

        root = ET.fromstring(api.buildings_to_osm_xml([b1, b2]))

        # 応答の中の重複は 0 件のまま (#38 の退行が無いこと)
        assert _josm_duplicate_node_coords(root) == []

        at_corner = [n for n in root.findall('node')
                     if (n.get('lat'), n.get('lon')) == ('32.6445365', '130.6984598')]
        assert len(at_corner) == 1
        corner_nid = at_corner[0].get('id')

        ways = root.findall('way')
        assert len(ways) == 2
        for way in ways:
            refs = [nd.get('ref') for nd in way.findall('nd')]
            assert corner_nid in refs
```

- [ ] **Step 2: テストが失敗することを確認する**

Run: `python3 -m pytest tests/test_buildings_xml.py::TestNodeIdFromCoordinate -v`
Expected: `test_shared_corner_id_does_not_depend_on_which_buildings_are_present` が FAIL。
`both` は `-272155034`、`only_b2` は `-272163761` になり、assert が落ちる。
`test_adjacent_buildings_share_the_corner_node` は現行コードでも PASS する（#38 が応答の中では効いているため）。

- [ ] **Step 3: `coord_to_nid` を廃止する**

`osmfj_plateau_api.py` の 476-499 行にある次のブロックをすべて削除する。
`# Emit a single <node> for each unique (lat, lon)` で始まるコメント、
`coord_to_nid` の宣言、`_coord_key` の定義が対象で、`emitted_node_ids` は残す。

削除するのは次の範囲。

```python
        # Emit a single <node> for each unique (lat, lon) across the WHOLE
        # response and reuse its id from every way that touches that coordinate.
        # ... (コメント全体)
        # Value: { (lat, lon) → canonical node id (negative, the first one seen) }
        coord_to_nid: Dict[tuple, int] = {}
        # Tracks which canonical node ids have already produced a <node> element
        # so duplicates from later ways simply reference the existing one.
        emitted_node_ids: set = set()

        def _coord_key(lat: float, lon: float) -> tuple:
            # Match the 7-decimal precision used in the output below so float
            # representation jitter never makes "same coordinate" look distinct.
            return (round(lat, 7), round(lon, 7))
```

置き換える内容。

```python
        # ノードの id は座標から決まる (_node_id)。同じ座標には必ず同じ id が
        # 付くので、どの建物が先に来たかを覚える必要がない。応答をまたいでも
        # 同じ角が同じノードになる (#51)。
        # 同じ <node> を二度出さないために、出した id だけを記録する。
        emitted_node_ids: set = set()
```

- [ ] **Step 4: `_make_way_elem` の中を差し替える**

現在 577-582 行の次のブロックを置き換える。

```python
            for i, node_data in enumerate(valid_nodes):
                key = _coord_key(node_data['lat'], node_data['lon'])
                canonical_id = coord_to_nid.get(key)
                if canonical_id is None:
                    canonical_id = -node_data['id']
                    coord_to_nid[key] = canonical_id

                if canonical_id not in emitted_node_ids:
```

置き換える内容。

```python
            for i, node_data in enumerate(valid_nodes):
                canonical_id = self._node_id(node_data['lat'], node_data['lon'],
                                             node_data['id'])

                if canonical_id not in emitted_node_ids:
```

その直前にある次のコメント（572-576 行）も、内容が古くなるので置き換える。

```python
            # Look up / register the canonical node id for each coordinate
            # in the response-wide map. The first way to touch a coordinate
            # registers its id; every later way — whether a part of the same
            # building or a separate neighbouring building — reuses it, so
            # coincident corners resolve to a single shared <node>.
```

置き換える内容。

```python
            # 座標から id を決める。同じ角に触れる way は、同じ建物の部分立体
            # でも隣の建物でも同じ id を得るので、<node> は 1 つに集まる。
```

- [ ] **Step 5: テストが通ることを確認する**

Run: `python3 -m pytest tests/test_buildings_xml.py -v`
Expected: PASS。`TestNodeIdFromCoordinate` が 8 件通り、既存のテストも落ちない。

既存のテストは 1 つも直さなくてよい。確認済みの理由は 2 つある。

ノード id の具体値を assert している既存テストは無い。
`test_way_id_derived_from_building_id`（180 行）は `node_id < 0` しか見ておらず、
共有ノードのテスト（652 行）は出力された id を読み取ってから way の参照と突き合わせている。

既存テストの座標はすべて緯度 35.7 台と経度 139.7 台で、採番の範囲に入る。
`999` を使うテストがあるが、これは `_valid_nodes_from_ring` の
緯度経度チェックで `_node_id` に届く前に落ちる。

- [ ] **Step 6: すべてのテストを走らせる**

Run: `python3 -m pytest`
Expected: PASS。integration を除いた既存の件数が維持されること。

- [ ] **Step 7: コミットする**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "fix(api): ノード id を座標から決めて取得範囲に依存させない

隣り合う建物が共有する角が、その応答に隣の建物が入るかどうかで別の id に
なっていた。Rapid は id で要素を区別するので、同じ角が別のノードとして
承認され、JOSM が重複ノードとして報告していた (#51)。

応答の中で最初に来た建物の行 id を採用する coord_to_nid を廃止し、
座標から決まる _node_id に置き換えた。応答の中の重複が 0 件のままであること
(#38 の退行が無いこと) もテストで固定した。"
```

---

### Task 3: アップロードの仮 id が満たすべき条件を固定する

**Files:**
- Test: `tests/test_buildings_xml.py`（`TestNodeIdFromCoordinate` の後に新しいクラスを追加）

**Interfaces:**
- Consumes: Task 2 で差し替え済みの `buildings_to_osm_xml`

このタスクは実アップロードの代わりではない。実アップロードはユーザが本番の Rapid から行う。
ここでは、受け取る側の実装から読み取った条件を、こちら側の出力に対して固定する。

- [ ] **Step 1: 失敗しないことを先に疑うテストを書く**

`tests/test_buildings_xml.py` に追加する。

```python
class TestOsmChangePlaceholderContract:
    """アップロードを受ける側が仮 id に課す条件を、応答の側で満たしているか。

    条件の出どころは 2 つ。
    cgimap は id を int64 で読み、作成する要素には「0 でない」と「負である」を
    課す (osmobject.hpp)。Rails は加えて「作成する要素の仮 id は型ごとに
    一意であること」を課す (lib/diff_reader.rb)。
    """

    def _response(self, api):
        corner = {'lat': 32.6445365, 'lon': 130.6984598}
        b1 = _make_building(building_id=1, building='yes', nodes=[
            {'id': 272155033, 'lat': 32.6444162, 'lon': 130.6988233},
            {'id': 272155034, **corner},
            {'id': 272155035, 'lat': 32.6444965, 'lon': 130.6984413},
        ])
        b2 = _make_building(building_id=2, building='yes', nodes=[
            {'id': 272163761, **corner},
            {'id': 272163762, 'lat': 32.6446000, 'lon': 130.6984598},
            {'id': 272163763, 'lat': 32.6446000, 'lon': 130.6985000},
        ])
        return ET.fromstring(api.buildings_to_osm_xml([b1, b2]))

    def test_all_ids_are_negative_and_nonzero(self, api):
        root = self._response(api)
        for kind in ('node', 'way', 'relation'):
            for elem in root.findall(kind):
                value = int(elem.get('id'))
                assert value < 0, f'{kind} id {value} が負でない'

    def test_all_ids_fit_in_int64(self, api):
        root = self._response(api)
        for kind in ('node', 'way', 'relation'):
            for elem in root.findall(kind):
                assert abs(int(elem.get('id'))) < 2 ** 63

    def test_ids_are_unique_within_each_type(self, api):
        root = self._response(api)
        for kind in ('node', 'way', 'relation'):
            ids = [elem.get('id') for elem in root.findall(kind)]
            assert len(ids) == len(set(ids)), f'{kind} の id が重複している'

    def test_every_nd_ref_resolves_to_an_emitted_node(self, api):
        root = self._response(api)
        emitted = {n.get('id') for n in root.findall('node')}
        for way in root.findall('way'):
            for nd in way.findall('nd'):
                assert nd.get('ref') in emitted, \
                    f"way {way.get('id')} が未出力のノード {nd.get('ref')} を参照している"
```

- [ ] **Step 2: テストを走らせる**

Run: `python3 -m pytest tests/test_buildings_xml.py::TestOsmChangePlaceholderContract -v`
Expected: PASS (4 passed)

すべて通るのが期待どおりである。Task 2 の実装が条件を満たしていることの確認であり、
将来この条件を崩す変更が入ったときに落ちる網として置く。

- [ ] **Step 3: 全体を走らせる**

Run: `python3 -m pytest`
Expected: PASS

- [ ] **Step 4: コミットする**

```bash
git add tests/test_buildings_xml.py
git commit -m "test(api): アップロードの仮 id が満たすべき条件を固定する

cgimap と Rails の実装から読み取った条件 (負、0 でない、int64 に収まる、
型ごとに一意) を応答に対して確認する。nd の参照が必ず出力済みのノードに
解決することもあわせて見る。"
```

---

## 反映後の確認（実装の外、ユーザが行う）

1. API を再起動する（sudo が要る）
2. 報告された 2 か所を、隣の建物が入る範囲と入らない範囲の両方で取得し、同じ id が返ること

```bash
curl -s 'https://rapid.nyampire.info/api/mapwithai/buildings?bbox=130.698400,32.644380,130.698830,32.644550&use_intersects=true&limit=5000' | grep -o 'lat="32.6445365" lon="130.6984598"' -B2 | head
```

3. Rapid から実際に建物を承認してアップロードし、JOSM が重複ノードを出さないこと
4. Rapid のキャッシュに古い id が残るため、確認は再読み込みかシークレットウィンドウの後に行う
