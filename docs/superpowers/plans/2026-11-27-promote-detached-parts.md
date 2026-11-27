# リレーションに入らない部分立体を独立した建物として出力する 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 親の外形と重ならない部分立体と、親が記録されていない部分立体を、API の応答で `building` の way として出力する。

**Architecture:** `get_buildings_in_bbox` の問い合わせに、外形と重ならない部分立体を自分の位置で取り出す CTE を足す。
この CTE と `orphan_parts` は、行の `building_part` と `parent_building_id` を NULL にして返し、`promoted_part` を TRUE にする。
こうすると、`buildings_to_osm_xml` のタグ付けと `annotate_height_warnings` は、手を入れずに建物として扱う。
`buildings_to_osm_xml` では、同じ建物が `related_parts` からも返ったときの重複を外し、ログの数え方を直す。

**Tech Stack:** Python 3、FastAPI、psycopg2、PostgreSQL 16 と PostGIS 3.4、pytest

**Spec:** `docs/superpowers/specs/2026-11-27-promote-detached-parts-design.md`

## Global Constraints

- 外したままにする条件は2つ。兄弟（同じ `parent_building_id` を持つ別の行）と `ref_mlit_plateau` が同じもの。`ref_mlit_plateau` が文字列の `'Null'` のもの。
- 親の外形と重なる部分立体の出力は変えない。リレーションの `part` のまま。
- 独立した建物として出す行は、`building=<building 列の値、空なら yes>` の単独の way にし、どのリレーションにも入れない。
- 新しい CTE には、`orphan_parts` と同じ `spatial_condition`、`city_boundary_filter`、`DISTINCT ON ({dedup_key})` と `dedup_tiebreaker` を当てる。
- 1つの DB の行は、応答の中で way を1本だけ作る。
- Rapid、取り込み、DB の構造は変えない。
- コミットメッセージに `Co-Authored-By` の行を付けない。サブエージェントにも同じ決まりを伝える。
- 公開のリポジトリなので、コードのコメントとコミットメッセージに、サーバの名前、内部のパス、本番への接続の手順を書かない。

## Review Focus

1. `ref_mlit_plateau` が SQL の NULL の部分立体。本番には無いが、判別できないものとして外したままにするのが自然。Task 1 の試験で固定する。
2. 部分立体の `geom` が NULL や不正な形のとき。`ST_Intersects` が NULL を返すので、独立させず、今までどおり部分立体として扱われるのが自然。Task 1 の試験で固定する。
3. 外形と、外形と重ならない部分立体の両方が取得範囲に入るとき。way が2本出ると、同じ建物が重なって表示される。Task 2 の試験で固定する。
4. 兄弟と建物ID を共有するのが、外形と重なる兄弟だけのとき。重なる兄弟と同じ ID の離れた部分立体は、1棟の一部と考えられるので外したままにする。Task 1 の試験で固定する。
5. 独立した建物に `building=roof` が入っているとき。外形と同じく `layer=1` が付くのが自然。Task 2 の試験で固定する。

---

### Task 1: 問い合わせで独立した建物の行を返す

**Files:**
- Modify: `osmfj_plateau_api.py`（`get_buildings_in_bbox` の 170〜300 行付近の問い合わせと、302 行付近の `params`）
- Modify: `tests/test_buildings_xml.py`（`TestGetBuildingsInBboxQuery` の `params` の期待）
- Create: `tests/test_promote_detached_parts.py`

**Interfaces:**
- Produces: `get_buildings_in_bbox` の各行に `promoted_part`（bool）が加わる。`related_parts` と `bbox_outlines` の行は FALSE。新しい CTE と `orphan_parts` の行は TRUE で、`building_part` と `parent_building_id` は None。
- Produces: 新しい CTE の名前は `detached_parts`。
- Produces: `params` は 13 個になる。並びは、外形の範囲4個、`limit`、`orphan_parts` の範囲4個、`detached_parts` の範囲4個。

- [ ] **Step 1: 失敗する試験を書く**

`tests/test_promote_detached_parts.py` に、`@pytest.mark.integration` の試験を書く。
`fresh_plateau_full_schema` と `plateau_api_class` を使い、行は `tests/test_intersects_parent_flag.py` と同じく INSERT で入れる。
`ref_mlit_plateau` を入れるので、`tests/test_dedup_city_duplicates.py` の `_seed_building` は使わない。

外形 O（id 1）を (133.0, 33.0) の 0.001 度四方に置く。
部分立体はすべて `parent_building_id=1`、`building_part='yes'` にする。
取得範囲は `132.99, 32.99, 133.01, 33.01`。

| id | 位置 | `ref_mlit_plateau` | 期待 |
|---|---|---|---|
| 2 | O の内側 | `'A-2'` | `promoted_part` が FALSE、`building_part='yes'`、`parent_building_id=1` |
| 3 | O の外、範囲の内（133.003, 33.003） | `'A-3'` | `promoted_part` が TRUE、`building_part` と `parent_building_id` が None |
| 4 | O の外、範囲の内（133.005, 33.005） | `'Null'` | `promoted_part` が TRUE の行が無い |
| 5 | O の外、範囲の内（133.007, 33.007） | `'A-2'`（id 2 と共有） | `promoted_part` が TRUE の行が無い |
| 6 | O の外、範囲の内（133.009, 33.003） | NULL | `promoted_part` が TRUE の行が無い |
| 7 | O の外、範囲の内（133.003, 33.009）、`geom` は NULL、`centroid` は入れる | `'A-7'` | `promoted_part` が TRUE の行が無い |

```python
def test_detached_part_is_returned_as_a_building(...):
    promoted = [r for r in rows if r['promoted_part']]
    assert [r['id'] for r in promoted] == [3]
    assert promoted[0]['building_part'] is None
    assert promoted[0]['parent_building_id'] is None

def test_part_inside_outline_is_unchanged(...):
    row = next(r for r in rows if r['id'] == 2)
    assert row['promoted_part'] is False
    assert row['building_part'] == 'yes'
    assert row['parent_building_id'] == 1
```

次の2つの試験も同じファイルに書く。

- `test_detached_part_is_returned_when_its_outline_is_out_of_range`: O を (134.0, 34.0) に置き、id 3 だけを範囲の内に置く。`promoted_part` が TRUE の id 3 の行が1件返る。
- `test_orphan_part_is_returned_as_a_building`: `parent_building_id` が NULL の部分立体を範囲の内に置く。`promoted_part` が TRUE で、`building_part` が None の行が返る。

高さの警告の試験も同じファイルに書く。

- `test_detached_part_gets_needle_and_not_part_over_outline`: O の高さを 9.1、id 3 の高さを 149.2、id 3 を底面積の小さい形（`DEFAULT_NEEDLE_AREA_M2` より小さい）にする。id 3 の `promoted_part` が TRUE の行の `height_warnings` に `'needle'` が入り、`'part-over-outline'` が入らない。

`tests/test_buildings_xml.py` の `TestGetBuildingsInBboxQuery` では、`params` の期待を次のように直す。

```python
assert len(params) == 13
assert params[9:13] == [139.7, 35.7, 139.8, 35.8]
```

`detached_parts` が SQL に含まれることも確かめる。
`test_city_boundary_filter_applies_to_outlines_and_orphans` の `FROM dash_city_master m` の回数の期待は、4 から 6 に直す（`detached_parts` のフィルタと `dedup_tiebreaker` の2回が増える）。

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest --run-integration tests/test_promote_detached_parts.py tests/test_buildings_xml.py -v`
Expected: FAIL。`KeyError: 'promoted_part'` と `assert 9 == 13`。

- [ ] **Step 3: 問い合わせを変える**

`bbox_outlines` と `related_parts` に `FALSE AS promoted_part` を足す。
`orphan_parts` は、`NULL::text AS building_part`、`NULL::bigint AS parent_building_id`、`TRUE AS promoted_part` を返すように変える（`WHERE` と `dedup_key` は元の列のまま）。
`detached_parts` を `orphan_parts` の後ろに足し、`all_buildings` の `UNION ALL` と最後の `SELECT` に加える。
`detached_parts` の列は `orphan_parts` と同じ並びにし、`0 AS pre_dedup_count` と `NULL::boolean AS intersects_parent` を返す。
`detached_parts` の条件は次のとおり。

```sql
FROM plateau_buildings b
JOIN plateau_buildings o ON o.id = b.parent_building_id
WHERE b.building_part = 'yes'
  AND {spatial_condition}
  AND NOT ST_Intersects(ST_MakeValid(b.geom), ST_MakeValid(o.geom))
  AND b.ref_mlit_plateau IS NOT NULL
  AND b.ref_mlit_plateau <> 'Null'
  AND NOT EXISTS (
      SELECT 1 FROM plateau_buildings s
      WHERE s.parent_building_id = b.parent_building_id
        AND s.id <> b.id
        AND s.ref_mlit_plateau = b.ref_mlit_plateau
  )
  {city_boundary_filter}
ORDER BY {dedup_key}, {dedup_tiebreaker}
```

`all_buildings` の上の、3つの CTE が互いに重ならないと書いたコメントを直す。
`detached_parts` の行は `related_parts` の行と同じ建物になりうるので、重なりは Task 2 で外すと書く。
`params` の末尾に範囲の4個を足す。

- [ ] **Step 4: 試験が通ることを確かめる**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest --run-integration tests/ -q`
Expected: すべて PASS。`tests/test_intersects_parent_flag.py` と `tests/test_drop_far_parts_endpoint.py` は、離れた部分立体が範囲の外にあるので、直さずに通る。

- [ ] **Step 5: コミットする**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py tests/test_promote_detached_parts.py
git commit -m "feat: 外形と重ならない部分立体と親の無い部分立体を、建物の行として返す"
```

### Task 2: 出力で重複を外し、件数をログに出す

**Files:**
- Modify: `osmfj_plateau_api.py`（`buildings_to_osm_xml` の 590〜640 行付近と 920〜930 行付近）
- Modify: `tests/test_buildings_xml.py`（`TestBuildingsToOsmXml` に試験を足す）
- Modify: `tests/test_drop_far_parts_endpoint.py`（試験を足す）

**Interfaces:**
- Consumes: Task 1 の `promoted_part`。
- Produces: ログの行 `出口補正: 親と交差しない部分立体を除外 {n}件` と `出口補正: 部分立体を独立した建物として出力 {n}件`。

- [ ] **Step 1: 失敗する試験を書く**

`tests/test_buildings_xml.py` の `TestBuildingsToOsmXml` に、モックの行で試験を足す。
行は `_make_building` で作り、`promoted_part` と `intersects_parent` を与える。

- `test_promoted_row_wins_over_its_related_duplicate`: 同じ `id` の2行を渡す。1つは `building_part='yes'`、`parent_building_id=1`、`intersects_parent=False`、`promoted_part=False`。もう1つは `building_part=None`、`parent_building_id=None`、`promoted_part=True`。way が1本だけ出て、`building=yes` のタグを持ち、`building:part` のタグを持たない。`relation` は出ない。
- `test_promoted_roof_gets_layer`: `building='roof'`、`promoted_part=True` の行で、`layer=1` が付く。
- `test_drop_count_excludes_promoted_duplicates`: 上の2行に加えて、`intersects_parent=False` で重複の無い行を1つ渡す。`caplog` で、除外の件数が 1、独立した建物の件数が 1 と出る。

`tests/test_drop_far_parts_endpoint.py` に、外形と、外形の外にある部分立体の両方が範囲に入る場合の通しの試験を足す。

- `test_detached_part_in_range_is_served_once_as_building`: 部分立体（`ref_mlit_plateau='A-4'`）の way が1本だけあり、`building` のタグを持ち、`relation` の `member` に入らない。

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest --run-integration tests/test_buildings_xml.py tests/test_drop_far_parts_endpoint.py -v`
Expected: FAIL。way が2本出るか、除外の件数が 2 になる。

- [ ] **Step 3: `buildings_to_osm_xml` の除外を変える**

`promoted_part` が TRUE の行の `id` を集める。
`intersects_parent is False` の行は今までどおり外し、その `id` が集めた中に無いときだけ `dropped_far_parts` に数える。
`promoted_part` が TRUE の行の件数を `promoted_parts` として数え、`dropped_far_parts` のログの次に1行出す。
612 行付近の撤去条件のコメントを書き直す。
外す処理が今は、兄弟と建物ID を共有するものと建物ID が `Null` のものを外すために残っていること、案 B（取り込みの変更）が入ったら撤去を考えることを書く。

- [ ] **Step 4: 試験が通ることを確かめる**

Run: `PLATEAU_TEST_DATABASE_URL=postgresql:///plateau_api_test pytest --run-integration tests/ -q`
Expected: すべて PASS。

- [ ] **Step 5: コミットする**

```bash
git add osmfj_plateau_api.py tests/test_buildings_xml.py tests/test_drop_far_parts_endpoint.py
git commit -m "feat: 独立した建物として出す部分立体の重複を外し、件数をログに出す"
```

### Task 3: 応答の時間を本番の規模で比べる

この作業は本番の DB に読み取りだけで問い合わせるので、コントローラが利用者の承認を得てから行う。
サブエージェントには渡さない。

- [ ] **Step 1: 比べる範囲を選ぶ**

部分立体の多い都市（さいたま市 11100、大阪市 27100）と、外形と重ならない部分立体の多い都市から、それぞれ 200m 四方と 1km 四方を選ぶ。

- [ ] **Step 2: 変更の前と後の問い合わせを、読み取りだけの設定で `EXPLAIN (ANALYZE, BUFFERS)` にかける**

Expected: 変更の後の実行時間と、`detached_parts` の計画の部分を記録する。
変更の後の時間が前の2倍を超えたら、実装を止めて利用者に報告する。
2倍は私の判断で決めた目安で、設計書には値が無い。

- [ ] **Step 3: 結果を PR の本文に書く**

都市コード、範囲の大きさ、前と後の時間だけを書く。
問い合わせの手順とサーバの情報は書かない。

## 範囲外

本番への反映、Issue へのコメント、#29 と #43 の訂正は、この計画に入れない。
それぞれ利用者の承認を得てから行う。
