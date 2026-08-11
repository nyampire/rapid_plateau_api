# 合成 OSM id の採番と `plateau_id` の規約 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 合成 OSM id を、型ごとに `(建物id, 役割)` から一意に決まる掛け算方式に置き換え、`plateau_id` に元要素の型を記録する。

**Architecture:** API の id 生成を定数 offset から掛け算に変える。way は `-(建物id × 1000 + 環番号)`、relation は `-(建物id × 10 + 種別)`。唯一の前提「環番号 < 1000」は API の出力側だけで守り、取り込み側では何も捨てない。`plateau_id` は importer 側で `w` / `r` を前置する。

**Tech Stack:** Python 3, FastAPI, psycopg2, PostgreSQL 16 + PostGIS 3.4, pytest

## Global Constraints

- 採番式は `way = -(建物id × 1000 + 環番号)`、`relation = -(建物id × 10 + 種別)`。環番号は外側が 0、内側が 1 から。種別は `type=building` が 1、`type=multipolygon` が 2。
- 既存の 3 定数 `RELATION_ID_OFFSET`、`MULTIPOLYGON_RELATION_ID_OFFSET`、`INNER_RING_WAY_ID_OFFSET` は廃止する。
- **環番号の上限は API の制約であって、データの制約ではない。取り込み側では何も捨てない。**内側リングが何本あってもそのまま DB に保存する。
- 環番号が 1000 に達する建物は、API がそのレスポンスから外して警告を出す。DB には残るので、式を直せば次のリクエストから出る。
- タグは relation にだけ付ける。メンバーの way には付けない。
- 穴の無い建物は way 1 本で出力する。この経路の出力は id の値以外変えない。
- API の出力に `ref:MLIT_PLATEAU` を含めない（#30 の方針）。
- 既存の `type=building` relation の出力を消さない。本番の 1,489 万行は再取り込みまで親子リンクを持ったままである。
- スキーマ変更は無い。
- `plateau_id` は `w{元のway id}` / `r{元のrelation id}` の形にする。`osmEntity.id.fromOSM` と同じ書き方で、way `-10` なら `w-10` になる。
- `plateau_id` を変えるために `way_id` を書き換えない。`way_id` は親子解決のキーとして使われている。
- 三角形の面積判定（#44）には触れない。
- 建物 id の詰め直しは対象外。`osm_id` 列にも触れない。
- spec: `docs/superpowers/specs/2026-08-04-synthetic-id-scheme-design.md`

## spec からの逸脱（1 点、意図的）

spec の「検知」の表は、事前確認を**変換直後に別途行う**と書いている。
本計画では、それを**取り込みログに最大本数を出す**形（Task 4）に置き換えた。

理由は 2 つ。
取り込み側が何も捨てなくなったので、人が知るのが変換直後でも取り込み直後でも失うものが無い。
別途スクリプトを置くと、API 側の上限 1000 を importer にも書くことになり数字が二重管理になる。

Task 4 は閾値を持たず観測値だけを出す。上限との比較は人がする。

---

### Task 1: 採番を掛け算方式に置き換える

**Files:**
- Modify: `osmfj_plateau_api.py:333-362`（定数）
- Modify: `osmfj_plateau_api.py:580`、`:637-641`、`:675`、`:707`、`:715`（id 生成の 5 箇所）
- Test: `tests/test_buildings_xml.py`

**Interfaces:**
- Produces: `OSMFJPlateauAPI._way_id(building_db_id, ring_no=0) -> int`
- Produces: `OSMFJPlateauAPI._relation_id(building_db_id, kind) -> int`
- Produces: 定数 `WAY_ID_RING_MULTIPLIER = 1000`、`RELATION_ID_KIND_MULTIPLIER = 10`、`RELATION_KIND_BUILDING = 1`、`RELATION_KIND_MULTIPOLYGON = 2`

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_buildings_xml.py` の末尾に追加する。

```python
class TestSyntheticIdScheme:
    """合成 OSM id は (建物id, 役割) から型ごとに一意に決まる。

    way      : -(建物id * 1000 + 環番号)
    relation : -(建物id * 10   + 種別)

    定数 offset 方式は建物 id が offset の桁に達すると族の帯が重なる。
    本番の建物 id は約 1,490 万で、旧 RELATION_ID_OFFSET (-1,000,000) を
    既に越えていた。
    """

    def _hole_building(self, db_id=1, inner_rings=1):
        def ring(base, lat, lon, d, r):
            return [{'id': base + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': r}
                    for i, (la, lo) in enumerate([
                        (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)])]
        nodes = ring(1000, 33.0, 133.0, 0.01, 0)
        for r in range(1, inner_rings + 1):
            nodes += ring(1000 + r * 10, 33.0 + 0.0005 * r, 133.0 + 0.0005 * r, 0.0002, r)
        return {
            'id': db_id, 'building': 'public', 'height': 14.6, 'building_part': None,
            'parent_building_id': None, 'nodes': nodes,
        }

    def _plain_building(self, db_id):
        return {
            'id': db_id, 'building': 'yes', 'height': 5.0, 'building_part': None,
            'parent_building_id': None,
            'nodes': [{'id': db_id * 100 + i, 'lat': la, 'lon': lo,
                       'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (34.0, 134.0), (34.01, 134.0), (34.01, 134.01), (34.0, 134.01)])],
        }

    def test_plain_way_id_uses_the_ring_multiplier(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._plain_building(7)]))
        assert root.find('way').get('id') == str(-(7 * 1000))

    def test_inner_ring_way_ids_follow_the_ring_number(self, api):
        b = self._hole_building(db_id=7, inner_rings=2)
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        members = {m.get('role'): [] for m in root.find('relation').findall('member')}
        for m in root.find('relation').findall('member'):
            members[m.get('role')].append(m.get('ref'))
        assert members['outer'] == [str(-(7 * 1000 + 0))]
        assert sorted(members['inner']) == sorted(
            [str(-(7 * 1000 + 1)), str(-(7 * 1000 + 2))]
        )

    def test_multipolygon_relation_id_uses_kind_two(self, api):
        root = ET.fromstring(api.buildings_to_osm_xml([self._hole_building(db_id=7)]))
        assert root.find('relation').get('id') == str(-(7 * 10 + 2))

    def test_building_relation_id_uses_kind_one(self, api):
        outline = self._plain_building(7)
        part = self._plain_building(8)
        part['building_part'] = 'yes'
        part['parent_building_id'] = 7
        root = ET.fromstring(api.buildings_to_osm_xml([outline, part]))
        rel = root.find('relation')
        assert rel.get('id') == str(-(7 * 10 + 1))
        members = {m.get('role'): m.get('ref') for m in rel.findall('member')}
        assert members['outline'] == str(-(7 * 1000))
        assert members['part'] == str(-(8 * 1000))

    def test_ids_are_unique_within_each_type(self, api):
        """旧方式で衝突していた組を含めても、型の中で重複しない。

        旧方式では type=building relation の id (-1_000_000 - id_C) と
        type=multipolygon relation の id (-3_000_000 - id_A) が
        id_C == id_A + 2_000_000 のときに一致した。
        """
        buildings = [self._hole_building(db_id=5)]
        for db_id in (1, 2, 2_000_005, 3, 100):
            buildings.append(self._plain_building(db_id))
        outline = self._plain_building(9)
        part = self._plain_building(10)
        part['building_part'] = 'yes'
        part['parent_building_id'] = 9
        buildings += [outline, part]

        root = ET.fromstring(api.buildings_to_osm_xml(buildings))
        way_ids = [w.get('id') for w in root.findall('way')]
        rel_ids = [r.get('id') for r in root.findall('relation')]
        assert len(way_ids) == len(set(way_ids)), f'way id が重複: {way_ids}'
        assert len(rel_ids) == len(set(rel_ids)), f'relation id が重複: {rel_ids}'
```

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_buildings_xml.py::TestSyntheticIdScheme -v`
Expected: `test_plain_way_id_uses_the_ring_multiplier` は `-7` が返って FAIL。他の id 系も旧 offset の値で FAIL。`test_ids_are_unique_within_each_type` は PASS するかもしれない（旧方式でも今回の組み合わせでたまたま衝突しない場合がある）。PASS でも構わない。

- [ ] **Step 3: 定数を置き換える**

`osmfj_plateau_api.py:333-362` の 3 定数とその説明コメントを、まとめて次で置き換える。

```python
    # 合成 OSM id の採番。型ごとに (建物id, 役割) から一意に決める。
    #
    #   way      : -(建物id * WAY_ID_RING_MULTIPLIER + 環番号)
    #   relation : -(建物id * RELATION_ID_KIND_MULTIPLIER + 種別)
    #
    # 環番号は外側リングが 0、内側リングが 1 から。種別は下の 2 定数。
    #
    # 単射である理由: 環番号が乗数未満なら商と余りが一意に決まるので、
    # 値が一致するには建物 id と役割の両方が一致するしかない。
    # 種別は {1, 2} の 2 値で乗数 10 未満なので、relation 側は前提なしに
    # 成立する。way 側の前提は「環番号 < WAY_ID_RING_MULTIPLIER」の一つだけで、
    # これを越えると建物 N の環が建物 N+1 の通常 way id を奪う。
    # 上限は出力側で守る (buildings_to_osm_xml)。
    #
    # OSM の id は型ごとに独立しており、Rapid も entityID を型の頭文字付きで
    # 作る (osmEntity.id.fromOSM) ので、way と relation の間では衝突しない。
    # 確認が要るのは同じ型の中だけである。
    #
    # 桁: 建物 id 1,490 万 × 1000 で約 150 億。JavaScript の安全整数
    # 9e15 の約 60 万分の 1 で、全都市の再取り込み 1 周あたり 1,490 万の
    # 増加なら約 60 万周分の余裕がある。
    WAY_ID_RING_MULTIPLIER = 1000
    RELATION_ID_KIND_MULTIPLIER = 10
    RELATION_KIND_BUILDING = 1
    RELATION_KIND_MULTIPOLYGON = 2

    def _way_id(self, building_db_id: int, ring_no: int = 0) -> int:
        """way の合成 OSM id。ring_no 0 が外側リング (穴の無い建物もこれ)。"""
        return -(building_db_id * self.WAY_ID_RING_MULTIPLIER + ring_no)

    def _relation_id(self, building_db_id: int, kind: int) -> int:
        """relation の合成 OSM id。kind は RELATION_KIND_* のいずれか。"""
        return -(building_db_id * self.RELATION_ID_KIND_MULTIPLIER + kind)
```

- [ ] **Step 4: id 生成の 5 箇所を差し替える**

`osmfj_plateau_api.py:580`（穴の無い建物の way）

```python
                    way_id = self._way_id(building_db_id)
```

`osmfj_plateau_api.py:637-641`（環ごとの way）

```python
                    ring_way_ids: Dict[int, int] = {
                        ring_no: self._way_id(building_db_id, ring_no)
                        for ring_no in rings
                    }
```

`osmfj_plateau_api.py:675`（`type=multipolygon` の relation）

```python
                    rel_elem.set('id', str(self._relation_id(
                        building_db_id, self.RELATION_KIND_MULTIPOLYGON)))
```

`osmfj_plateau_api.py:707`（`type=building` の relation）

```python
            rel_elem.set('id', str(self._relation_id(
                parent_db_id, self.RELATION_KIND_BUILDING)))
```

`osmfj_plateau_api.py:715`（outline メンバーの way id）

```python
            outline_way_id = self._way_id(parent_db_id)
```

- [ ] **Step 5: 既存テストの期待値を直す**

`tests/test_buildings_xml.py:734` を差し替える。

```python
        # outer 側の way id は単一 way のときと同じ規則 (ring 0) であるはず。
        # これは ring 0 (外形) の way にしか成り立たない。
        assert members['outer'] == str(-(building['id'] * 1000))
```

`test_multipolygon_relation_id_does_not_collide_with_parts_relation` の docstring から旧定数名（`RELATION_ID_OFFSET` / `MULTIPOLYGON_RELATION_ID_OFFSET`）への言及を消し、次の趣旨に書き換える。テストの assert 自体は変えない。

```python
        """1 建物が「穴を持つ outline」かつ「part の親」のとき、2 つの
        relation の id が別であることを確認する。

        採番は type=building が -(建物id * 10 + 1)、type=multipolygon が
        -(建物id * 10 + 2) なので、同じ建物でも種別の桁で分かれる。

        なお、この入力の組み合わせは DB には存在しえない。
        parent_building_id を埋める親マップは取り込み側で常に空であり、
        ring_id >= 1 は multipolygon 経路からしか出ない。両者は同じ
        取り込み実行で決まるので、1 行が両方を持つことはない。
        この assert は 2 つの relation が出る形を正しいと認めるものではなく、
        id が別であることだけを固定している。
        """
```

- [ ] **Step 6: テストが通ることを確認する**

Run: `python -m pytest tests/test_buildings_xml.py -v`
Expected: `TestSyntheticIdScheme` の 5 件が PASS。既存の `TestMultipolygonOutput` も全件 PASS。

- [ ] **Step 7: 旧定数が残っていないことを確認する**

Run: `grep -n "RELATION_ID_OFFSET\|MULTIPOLYGON_RELATION_ID_OFFSET\|INNER_RING_WAY_ID_OFFSET" osmfj_plateau_api.py tests/`
Expected: 出力なし

- [ ] **Step 8: 全テストを実行する**

Run: `python -m pytest -q`
Expected: 既存の 246 passed / 19 skipped が維持され、新規 5 件が加わる

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration`
Expected: 失敗なし

- [ ] **Step 9: コミット**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "fix: 合成 OSM id を掛け算方式にして族の帯の重なりをなくす"
```

---

### Task 2: 環番号の上限とレスポンス内の一意性を API で守る

**Files:**
- Modify: `osmfj_plateau_api.py:448`（`emitted_db_ids` の置き換え）
- Modify: `osmfj_plateau_api.py:580-600`、`:625-660`、`:665-690`、`:700-720`（登録の呼び出し）
- Test: `tests/test_buildings_xml.py`

**Interfaces:**
- Consumes: Task 1 の `_way_id` / `_relation_id` と `WAY_ID_RING_MULTIPLIER`
- Produces: 環番号が `WAY_ID_RING_MULTIPLIER` 以上の建物はレスポンスに出ない。DB の内容は変わらない。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_buildings_xml.py` の `TestSyntheticIdScheme` の下に追加する。

```python
class TestRingLimitAndIdUniqueness:
    """環番号の上限は API の出力側だけで守る。

    上限は id の採番式の制約であって、データの制約ではない。
    取り込み側は何も捨てないので、式を直せば次のリクエストから出る。
    """

    def _hole_building(self, db_id=1, inner_rings=1):
        def ring(base, lat, lon, d, r):
            return [{'id': base + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': r}
                    for i, (la, lo) in enumerate([
                        (lat, lon), (lat + d, lon), (lat + d, lon + d), (lat, lon + d)])]
        nodes = ring(10 ** 7, 33.0, 133.0, 0.5, 0)
        for r in range(1, inner_rings + 1):
            base = 10 ** 7 + r * 10
            nodes += ring(base, 33.0 + 0.0001 * r, 133.0 + 0.0001 * r, 0.00002, r)
        return {
            'id': db_id, 'building': 'public', 'height': 14.6, 'building_part': None,
            'parent_building_id': None, 'nodes': nodes,
        }

    def test_ring_999_is_served(self, api):
        b = self._hole_building(db_id=3, inner_rings=999)
        root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.find('relation') is not None, '環 999 本の建物が出ていない'
        way_ids = {w.get('id') for w in root.findall('way')}
        assert str(-(3 * 1000 + 999)) in way_ids

    def test_ring_1000_is_dropped_from_the_response(self, api, caplog):
        b = self._hole_building(db_id=3, inner_rings=1000)
        with caplog.at_level(logging.WARNING):
            root = ET.fromstring(api.buildings_to_osm_xml([b]))
        assert root.find('relation') is None, '環 1000 本の建物が出力されている'
        assert root.findall('way') == [], '一部の way だけ残っている'
        assert root.findall('node') == [], '参照されないノードが残っている'
        assert any('1000' in r.message for r in caplog.records), '警告が出ていない'

    def test_other_buildings_survive_when_one_is_dropped(self, api):
        big = self._hole_building(db_id=3, inner_rings=1000)
        ok = {
            'id': 4, 'building': 'yes', 'height': 5.0, 'building_part': None,
            'parent_building_id': None,
            'nodes': [{'id': 900 + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (35.0, 135.0), (35.01, 135.0), (35.01, 135.01), (35.0, 135.01)])],
        }
        root = ET.fromstring(api.buildings_to_osm_xml([big, ok]))
        assert [w.get('id') for w in root.findall('way')] == [str(-(4 * 1000))]

    def test_duplicate_id_is_reported(self, api, caplog, monkeypatch):
        """採番式が壊れたときの網。式を潰して衝突を作り、警告が出ることを見る。

        現在の式では同型の id が衝突しないので、衝突そのものは作れない。
        検知の経路が生きていることだけを確かめる。
        """
        monkeypatch.setattr(type(api), '_way_id',
                            lambda self, building_db_id, ring_no=0: -1)
        b1 = {
            'id': 1, 'building': 'yes', 'height': 5.0, 'building_part': None,
            'parent_building_id': None,
            'nodes': [{'id': 10 + i, 'lat': la, 'lon': lo, 'sequence_id': i, 'ring_id': 0}
                      for i, (la, lo) in enumerate([
                          (36.0, 136.0), (36.01, 136.0), (36.01, 136.01), (36.0, 136.01)])],
        }
        b2 = dict(b1, id=2)
        with caplog.at_level(logging.WARNING):
            api.buildings_to_osm_xml([b1, b2])
        assert any('衝突' in r.message for r in caplog.records), '重複が報告されていない'
```

`tests/test_buildings_xml.py` の import に `logging` が無ければ足す。

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_buildings_xml.py::TestRingLimitAndIdUniqueness -v`
Expected: `test_ring_1000_is_dropped_from_the_response` が FAIL（建物が出力される）。`test_other_buildings_survive_when_one_is_dropped` も FAIL。`test_duplicate_id_is_reported` も FAIL（検知が無い）。`test_ring_999_is_served` は PASS。

- [ ] **Step 3: 一意性の登録を用意する**

`osmfj_plateau_api.py:448` の `emitted_db_ids = set()` を差し替える。

```python
        # 発行済みの (型, id) を記録する。同じ型で同じ id が二度出たら、
        # 採番式の単射性が壊れている。式の証明が将来の変更で崩れたときの網。
        emitted_element_ids = set()

        def _register_element_id(kind, element_id):
            key = (kind, element_id)
            if key in emitted_element_ids:
                logger.warning(
                    f"⚠️ 合成 id の衝突: {kind} id {element_id} が同じレスポンスに"
                    f"二度出た。採番式を確認すること"
                )
            emitted_element_ids.add(key)
```

`emitted_db_ids.add(building_db_id)` の 2 箇所（`:587` と `:649` 付近）を削除する。この変数は書かれるだけで読まれていなかった。

- [ ] **Step 4: 環番号の上限を守る**

`osmfj_plateau_api.py` の穴のある建物の分岐で、環の検証ループより前に挿入する。`rings` を組み立てた直後、`ring_failed` の初期化のあたりが位置になる。

```python
                    # 環番号が乗数に達すると、この建物の環が次の建物の通常
                    # way id を奪う。奪われた側は編集画面から黙って消えるので、
                    # 出力しないほうを選ぶ。DB には保存されているので、
                    # 採番式を直せば次のリクエストから出る。
                    max_ring_no = max(rings)
                    if max_ring_no >= self.WAY_ID_RING_MULTIPLIER:
                        logger.warning(
                            f"⚠️ 建物をレスポンスから除外 {building_db_id}: "
                            f"内側リングが環番号 {max_ring_no} まであり、way id の"
                            f"採番範囲 ({self.WAY_ID_RING_MULTIPLIER}) を超える。"
                            f"DB には保存されている"
                        )
                        continue
```

- [ ] **Step 5: 登録を呼ぶ**

穴の無い建物の way を追加する箇所（`all_ways.append(way_elem)` の直後）。

```python
                    _register_element_id('way', way_id)
```

環ごとの way を追加する箇所（`all_ways.extend(ring_way_elems)` の直後）。

```python
                    for ring_no in sorted(rings):
                        _register_element_id('way', ring_way_ids[ring_no])
```

`type=multipolygon` の relation を追加する箇所（`all_relations.append(rel_elem)` の直前）。

```python
                    _register_element_id(
                        'relation',
                        self._relation_id(building_db_id, self.RELATION_KIND_MULTIPOLYGON))
```

`type=building` の relation を追加する箇所（`all_relations.append(rel_elem)` の直前）。

```python
            _register_element_id(
                'relation', self._relation_id(parent_db_id, self.RELATION_KIND_BUILDING))
```

- [ ] **Step 6: テストが通ることを確認する**

Run: `python -m pytest tests/test_buildings_xml.py::TestRingLimitAndIdUniqueness -v`
Expected: 4 passed

- [ ] **Step 7: 上限の検査が本当に効いていることを確かめる**

`max_ring_no >= self.WAY_ID_RING_MULTIPLIER` を `> self.WAY_ID_RING_MULTIPLIER` に一時的に変え、`test_ring_1000_is_dropped_from_the_response` が落ちることを確認してから戻す。

前後で必ずバイトコードを消す。同じ大きさの改変を同じ秒に戻すと古い `.pyc` が使われ、確認が無意味になる。

```bash
find . -name "__pycache__" -type d -exec rm -rf {} +
```

戻したあと `git diff` が空であることを確認する。

- [ ] **Step 8: 全テストを実行する**

Run: `python -m pytest -q`
Expected: Task 1 の結果に新規 4 件が加わる

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration`
Expected: 失敗なし

- [ ] **Step 9: コミット**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py
git commit -m "feat: 環番号の上限と合成 id の重複を API で検知する"
```

---

### Task 3: `plateau_id` に元要素の型を記録する

**Files:**
- Modify: `plateau_importer2postgis.py:521-537`（way 由来の建物 dict）
- Modify: `plateau_importer2postgis.py:551-560`（multipolygon 由来の建物 dict）
- Modify: `plateau_importer2postgis.py:851`（INSERT に渡す値）
- Modify: `README.md:141`
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Produces: 建物 dict が `plateau_id` を持つ。way 由来は `w{way_id}`、multipolygon 由来は `r{rel_id}`。
- Produces: `way_id` は従来どおり生の要素 id のまま。親子解決のキーに使われているので変えない。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の末尾に追加する。

```python
class TestPlateauIdCarriesTheElementType:
    """`plateau_id` は元要素の型が判る形で記録する。

    OSM は way と relation の id 空間を分けているので、生の数字だけでは
    変換出力のどの要素から来たかを特定できない。`source_dataset` に
    ファイル名が入っているため、型さえ足せば一意に決まる。

    書き方は osmEntity.id.fromOSM と同じで、way -10 なら 'w-10' になる。
    安定した識別子ではない (変換をやり直すと値が変わる)。その役割は
    ref_mlit_plateau が担う。
    """

    def _parse(self, bare_importer, xml):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        return importer.parse_osm_file_safe(osm_file)

    def test_way_derived_building_is_prefixed_with_w(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        by_way = {b['way_id']: b for b in buildings}
        assert by_way['-20']['plateau_id'] == 'w-20'

    def test_way_id_itself_is_not_rewritten(self, bare_importer):
        """`way_id` は親子解決のキーなので、前置してはいけない。"""
        nodes, buildings = self._parse(bare_importer, _SYNTH_OSM)
        assert all(not b['way_id'].startswith('w') for b in buildings)

    def test_multipolygon_derived_building_is_prefixed_with_r(self, bare_importer):
        nodes, buildings = self._parse(bare_importer, _HOLE_OSM)
        assert len(buildings) == 1
        assert buildings[0]['plateau_id'] == 'r-30'
```

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestPlateauIdCarriesTheElementType -v`
Expected: 3 件中 2 件が KeyError で FAIL（`plateau_id` が無い）。`test_way_id_itself_is_not_rewritten` は PASS。

- [ ] **Step 3: way 由来の建物に `plateau_id` を足す**

`plateau_importer2postgis.py` の way 収集の `buildings.append` の辞書に、`'way_id': way_id,` の直後に挿入する。

```python
                    # 変換出力のどの要素から来たかの記録。osmEntity.id.fromOSM と
                    # 同じ書き方で、way -10 なら 'w-10'。way_id 自体は親子解決の
                    # キーなので生のまま残す。
                    'plateau_id': f'w{way_id}',
```

- [ ] **Step 4: multipolygon 由来の建物に `plateau_id` を足す**

`plateau_importer2postgis.py` の multipolygon の `buildings.append` の辞書に、`'way_id': rel_id,` の直後に挿入する。

```python
                'plateau_id': f'r{rel_id}',
```

- [ ] **Step 5: INSERT に渡す値を差し替える**

`plateau_importer2postgis.py:851` を差し替える。

```python
                                building['plateau_id'],             # plateau_id
```

- [ ] **Step 6: PR #41 のガードテストを新しい形に合わせる**

`tests/test_plateau_importer2postgis.py` の `test_plateau_id_keeps_the_raw_way_id` は、ファイルキーの名前空間付与が `plateau_id` に漏れないことを守っている。前置後もその趣旨を保つ形に直す。

```python
    def test_plateau_id_keeps_the_raw_way_id(self, bare_importer):
        """`plateau_id` にファイルキーの名前空間が漏れないことを守る。

        型の頭文字は付くが、その後ろは変換出力の生の要素 id のままである。
        名前空間付与 (mesh.osm:-10 のような形) が混ざると、DB に保存される
        値が変わってしまう。
        """
```

続く assert の期待値を `'-10'` から `'w-10'` に直す。テストの構造は変えない。

- [ ] **Step 7: README を直す**

`README.md:141` の行を差し替える。

```markdown
| plateau_id | text | 変換出力の元要素。way 由来は `w-123`、multipolygon 由来は `r-456`。変換をやり直すと値が変わるため安定した識別子ではない（それは `ref_mlit_plateau`） |
```

- [ ] **Step 8: テストが通ることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py -v`
Expected: 新規 3 件が PASS、既存も全件 PASS

- [ ] **Step 9: 全テストを実行する**

Run: `python -m pytest -q`
Expected: Task 2 の結果に新規 3 件が加わる

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration`
Expected: 失敗なし

- [ ] **Step 10: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py README.md
git commit -m "feat: plateau_id に元要素の型を記録する"
```

---

### Task 4: 内側リングの最大本数を取り込みレポートに出す

**Files:**
- Modify: `plateau_importer2postgis.py`（`process_buildings_safe` の集計とログ）
- Test: `tests/test_plateau_importer2postgis.py`

**Interfaces:**
- Consumes: Task 3 の変更なし（独立）
- Produces: 取り込みログに `内側リングの最大本数: N` が出る。取り込みの内容は変えない。

閾値は持たない。API 側の上限（1000）は API の定数であり、importer に同じ数字を持たせると二重管理になる。観測値だけ出し、人が比べる。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_plateau_importer2postgis.py` の末尾に追加する。

```python
class TestRingCountIsReported:
    """内側リングの最大本数を取り込みログに出す。

    API の way id 採番は環番号が 1000 未満であることを前提にしている。
    取り込み側は何も捨てないので、本数が上限に近づいていないかを
    人が見られるようにしておく。
    """

    def test_max_inner_ring_count_is_logged(self, bare_importer, caplog):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(_HOLE_OSM)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]

        with caplog.at_level(logging.INFO):
            importer.process_buildings_safe(all_nodes, buildings)

        assert any('内側リングの最大本数' in r.message for r in caplog.records)
        assert any('内側リングの最大本数: 1' in r.message for r in caplog.records)

    def _many_ring_osm(self, inner_rings):
        """内側リングを任意の本数だけ持つ multipolygon を組み立てる。"""
        nodes, ways, members = [], [], []
        nid = -1

        def square(lat, lon, d):
            nonlocal nid
            refs = []
            for dlat, dlon in ((0, 0), (d, 0), (d, d), (0, d)):
                nodes.append(f'<node id="{nid}" lat="{lat + dlat}" lon="{lon + dlon}"/>')
                refs.append(nid)
                nid -= 1
            return refs

        outer = square(33.0, 133.0, 0.5)
        ways.append('<way id="-10">'
                    + ''.join(f'<nd ref="{r}"/>' for r in outer + outer[:1])
                    + '</way>')
        members.append('<member type="way" ref="-10" role="outer"/>')
        for r in range(1, inner_rings + 1):
            wid = -1000 - r
            refs = square(33.0 + 0.0001 * r, 133.0 + 0.0001 * r, 0.00002)
            ways.append(f'<way id="{wid}">'
                        + ''.join(f'<nd ref="{x}"/>' for x in refs + refs[:1])
                        + '</way>')
            members.append(f'<member type="way" ref="{wid}" role="inner"/>')
        return (
            '<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6">'
            + ''.join(nodes) + ''.join(ways)
            + '<relation id="-30">' + ''.join(members)
            + '<tag k="type" v="multipolygon"/><tag k="building" v="yes"/></relation>'
            + '</osm>'
        )

    def _process(self, bare_importer, xml):
        importer = bare_importer(citycode='35215')
        osm_file = Path(importer.data_dir) / 'mesh.osm'
        osm_file.write_text(xml)
        nodes, buildings = importer.parse_osm_file_safe(osm_file)
        key = importer._file_key(osm_file)
        all_nodes = {f'{key}:{k}': v for k, v in nodes.items()}
        for b in buildings:
            b['rings'] = [[f'{key}:{r}' for r in ring] for ring in b['rings']]
        return importer, importer.process_buildings_safe(all_nodes, buildings)

    def test_building_with_1000_rings_is_still_imported(self, bare_importer):
        """上限は API の制約なので、取り込みでは何も捨てない。

        API が出力できない本数でも DB には入る。式を直せば次のリクエストから
        出るという設計を、取り込み側で担保する。
        """
        xml = self._many_ring_osm(1000)
        _, (buildings_data, nodes_data, _) = self._process(bare_importer, xml)
        assert len(buildings_data) == 1, '環が多い建物が取り込みで捨てられている'
        assert max(row[7] for row in nodes_data) == 1000, '環番号が 1000 まで無い'

    def test_max_ring_count_reflects_1000_rings(self, bare_importer, caplog):
        xml = self._many_ring_osm(1000)
        with caplog.at_level(logging.INFO):
            self._process(bare_importer, xml)
        assert any('内側リングの最大本数: 1000' in r.message for r in caplog.records)
```

`tests/test_plateau_importer2postgis.py` の import に `logging` が無ければ足す。

- [ ] **Step 2: テストが落ちることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestRingCountIsReported -v`
Expected: `test_max_inner_ring_count_is_logged` と `test_max_ring_count_reflects_1000_rings` が FAIL（そのログが無い）。`test_building_with_1000_rings_is_still_imported` は PASS（取り込みは元から何も捨てないため）。

- [ ] **Step 3: 最大本数を集計する**

`process_buildings_safe` の建物ループの前に初期化する。`skipped_buildings = []` の隣が位置になる。

```python
        max_inner_rings = 0        # 観測した内側リングの最大本数
```

環ごとの座標を集めるループの直前で更新する。`for ring_no, refs in enumerate(building['rings']):` の直前に挿入する。

```python
                max_inner_rings = max(max_inner_rings, len(building['rings']) - 1)
```

- [ ] **Step 4: 集計結果をログに出す**

建物ループの後、スキップ内訳を出しているログの並びに追加する。

```python
        logger.info(f"   内側リングの最大本数: {max_inner_rings}")
```

- [ ] **Step 5: テストが通ることを確認する**

Run: `python -m pytest tests/test_plateau_importer2postgis.py::TestRingCountIsReported -v`
Expected: 3 passed

- [ ] **Step 6: 全テストを実行する**

Run: `python -m pytest -q`
Expected: Task 3 の結果に新規 2 件が加わる

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test python -m pytest -q --run-integration`
Expected: 失敗なし

- [ ] **Step 7: 実データで確認する**

周南 2 メッシュを取り込み直し、ログに `内側リングの最大本数: 1` が出ることと、建物数が 3,835 のまま変わらないことを確認する。

素の変換出力は次の 2 本である。**フィルタ済みの中間生成物を使わないこと。**
`imp_rule3/` 以下のものは規則適用後の出力で、建物 ID を持たない way が 0 件になっているため検証にならない。

```
.../scratchpad/sn_merge/51310655_bldg_6697_op.osm
.../scratchpad/scale/51310636/51310636_bldg_6697_op.osm
```

この 2 本を作業ディレクトリの `35215/` に置いて実行する。

```bash
python plateau_importer2postgis.py \
  --data-dir <作業ディレクトリ>/35215 \
  --postgres-url "postgresql:///plateau_task5" --no-zip
```

検証用 DB には `plateau_buildings`、`plateau_building_nodes`、`dash_city_master` の 3 つを作る。
`dash_city_master` が無いと行政界フィルタの SELECT が失敗してトランザクションが中断し、
**1 行も入らないままログは「インポート成功」と出る**。

> **訂正 (2026-08-11)。**`dash_city_master` は不要になった。
> 失敗した SELECT を `SAVEPOINT` で囲み、宣言どおりの pass-through にしたので、
> このテーブルが無くても行が入る。作るのは `plateau_buildings` と
> `plateau_building_nodes` の 2 つでよい。
取り込み後に `SELECT count(*) FROM plateau_buildings` で 3,835 を確かめること。

API 応答も確認する。穴のある建物の relation id が `-(建物id × 10 + 2)`、outer way が `-(建物id × 1000)`、inner way が `-(建物id × 1000 + 1)` になっていること。

- [ ] **Step 8: コミット**

```bash
git add plateau_importer2postgis.py tests/test_plateau_importer2postgis.py
git commit -m "feat: 内側リングの最大本数を取り込みレポートに出す"
```
