# 無壁舎を building=roof にする後処理 実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** PLATEAU の建物区分が無壁舎 (`bldg:class` 3003 / 3004) の建物を、手元の変換直後に `building=roof` へ書き換える。

**Architecture:** `.gml` と `.osm` が同じ作業ディレクトリに並ぶのは変換直後だけである。そこに Python の後処理を 1 本挟み、`uro:buildingID` と `ref:MLIT_PLATEAU` を鍵に区分を引いて `.osm` を書き戻す。取り込み器と変換器には触れない。

**Tech Stack:** Python 3 標準ライブラリのみ (`xml.etree.ElementTree`)、bash、pytest

**設計:** [2026-08-26-walless-building-roof-design.md](../specs/2026-08-26-walless-building-roof-design.md)

## Global Constraints

- 依存を増やさない。標準ライブラリだけで書く。`requirements.txt` は変えない
- **名前空間 URI で要素を引かない。**`uro` の URI はデータセットごとに違う (`.../iur/uro/1.4`、`https://www.geospatial.jp/iur/uro/3.1`、`.../3.2` を実データで確認)。要素名で引く
- `.gml` は最大 125MB ある。全文を文字列に載せない。`ET.iterparse` で読み、要素を `clear()` する
- `.osm` の書き戻しは `.part` に書いてから `os.replace` する。`extract_city.py` と同じ形にする。切り詰めた `.osm` が残ると、枚数の門を通り抜けて建物が欠けたまま取り込まれる
- 無壁舎が 1 件も無くても失敗にしない。件数を報告して正常終了する
- 区分のコード表は 3000 / 3001 / 3002 / 3003 / 3004 の 5 つ。9999 と `Null` は表に無い
- テストは通信しない。`tests/conftest.py` が `scripts/reimport` を `sys.path` に通しているので `import apply_walless_roof` で引ける

## 所要の見当

Task 1 の `parse_gml_classes` を実データで先に測ってある。
豊中市の 114MB のメッシュを 1.9 秒で読み、建物 4,054 棟を取り出した。
別に書いた正規表現版と件数も区分の分布も完全に一致した。

毎秒 60MB で読める計算になる。
全国の `.gml` は展開後で 168GB 前後なので、この工程の追加は 50 分ほどにあたる。
第 1 段の変換が 148 都市で 40 時間 18 分かかっているので、その 2% 程度である。

`.osm` の書き戻しはこれとは別に乗るが、`.osm` は `.gml` の 13% の大きさなので支配的にはならない。

---

### Task 1: GML から建物区分を読む

**Files:**
- Create: `scripts/reimport/apply_walless_roof.py`
- Test: `tests/test_reimport_walless_roof.py`

**Interfaces:**
- Produces: `parse_gml_classes(path) -> tuple[dict[str, str], bool]`
  第 1 要素は建物 ID から区分コードへの辞書。区分を持たない建物は載せない。
  第 2 要素は、そのメッシュに `bldg:class` が 1 つでもあったかどうか。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reimport_walless_roof.py` を新規に作る。

```python
"""無壁舎 (bldg:class 3003/3004) を building=roof にする後処理を固定する。

区分は元データにしか無く、変換器は出力しない。
`.gml` と `.osm` が並んでいる変換直後にしか当てられない。

通信はしない。すべてローカルの一時ファイルで組む。
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

import apply_walless_roof

REPO = Path(__file__).resolve().parent.parent
TOOL = REPO / 'scripts' / 'reimport' / 'apply_walless_roof.py'

# uro の名前空間はデータセットごとに違う。実データで 3 つ確認している。
URO_31 = 'https://www.geospatial.jp/iur/uro/3.1'
URO_14 = 'http://www.kantei.go.jp/jp/singi/tiiki/toshisaisei/itoshisaisei/iur/uro/1.4'


def _gml(buildings, uro_ns=URO_31):
    """buildings は (建物ID, 区分) の並び。区分が None なら class 要素を出さない。

    建物 ID を出さない建物は (None, 区分) で表す。
    """
    parts = []
    for bid, cls in buildings:
        body = ''
        if cls is not None:
            body += ('<bldg:class codeSpace="../../codelists/Building_class.xml">'
                     '%s</bldg:class>' % cls)
        if bid is not None:
            body += ('<uro:buildingIDAttribute><uro:BuildingIDAttribute>'
                     '<uro:buildingID>%s</uro:buildingID>'
                     '</uro:BuildingIDAttribute></uro:buildingIDAttribute>' % bid)
        parts.append('<core:cityObjectMember><bldg:Building gml:id="x">'
                     '%s</bldg:Building></core:cityObjectMember>' % body)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<core:CityModel xmlns:core="http://www.opengis.net/citygml/2.0" '
        'xmlns:bldg="http://www.opengis.net/citygml/building/2.0" '
        'xmlns:gml="http://www.opengis.net/gml" '
        'xmlns:gen="http://www.opengis.net/citygml/generics/2.0" '
        'xmlns:uro="%s">%s</core:CityModel>' % (uro_ns, ''.join(parts))
    )


def test_parse_gml_classes_reads_id_and_class(tmp_path):
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('27203-bldg-1', '3003'), ('27203-bldg-2', '3001')]),
                 encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {'27203-bldg-1': '3003', '27203-bldg-2': '3001'}
    assert saw is True
```

- [ ] **Step 2: 落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_parse_gml_classes_reads_id_and_class -v`
Expected: FAIL. `ModuleNotFoundError: No module named 'apply_walless_roof'` で collection が落ちる。

- [ ] **Step 3: 最小の実装を書く**

`scripts/reimport/apply_walless_roof.py` を新規に作る。

```python
#!/usr/bin/env python3
"""無壁舎 (bldg:class 3003/3004) の建物を `building=roof` にする。

    python3 apply_walless_roof.py <作業ディレクトリ>

変換の直後、`manifest.txt` を作る前に走らせる。
区分は元データ (`.gml`) にしかなく、変換器は出力しない。
`.gml` と `.osm` が同じディレクトリに並ぶのはこの時点だけである。

`uro:buildingID` と `.osm` の `ref:MLIT_PLATEAU` を鍵に突き合わせる。
"""
import json
import os
import sys
import xml.etree.ElementTree as ET

# 標準製品仕様書 表 4-28 の Building_class。9999 と Null はこの表に無い。
WALLLESS = {'3003', '3004'}
KNOWN_CLASSES = {'3000', '3001', '3002', '3003', '3004'}


def _local(tag):
    """名前空間を落として要素名だけ返す。"""
    return tag.rsplit('}', 1)[-1]


def parse_gml_classes(path):
    """建物 ID から区分コードへの辞書と、区分を 1 つでも見たかを返す。

    uro の名前空間 URI はデータセットごとに違う (1.4 / 3.1 / 3.2 を実データで
    確認) ので、URI では引かず要素名で引く。
    `bldg:class` だけは名前空間で絞る。他のスキーマにも `class` があるため。
    """
    classes = {}
    saw_class = False
    cur = None
    for event, elem in ET.iterparse(path, events=('start', 'end')):
        name = _local(elem.tag)
        if event == 'start':
            if name == 'Building':
                cur = {'id': None, 'class': None}
            continue
        if cur is not None:
            if name == 'class' and '/building/' in elem.tag and cur['class'] is None:
                cur['class'] = (elem.text or '').strip()
                saw_class = True
            elif name == 'buildingID' and cur['id'] is None:
                cur['id'] = (elem.text or '').strip()
        if name == 'Building':
            if cur and cur['id'] and cur['class']:
                classes[cur['id']] = cur['class']
            cur = None
            elem.clear()
    return classes, saw_class
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_parse_gml_classes_reads_id_and_class -v`
Expected: PASS

- [ ] **Step 5: 名前空間の版違いと、欠けている場合のテストを足す**

`tests/test_reimport_walless_roof.py` に足す。

```python
def test_parse_gml_classes_survives_other_uro_namespace(tmp_path):
    """uro の URI は 1.4 / 3.1 / 3.2 と版で変わる。URI で引くと空になる。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('53391550-bldg-1', '3004')], uro_ns=URO_14),
                 encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {'53391550-bldg-1': '3004'}
    assert saw is True


def test_parse_gml_classes_skips_building_without_class(tmp_path):
    """区分の無い建物は辞書に載せない。載せると 9999 と見分けが付かない。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('27209-bldg-1', None)]), encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {}
    assert saw is False


def test_parse_gml_classes_keeps_out_of_codelist_value(tmp_path):
    """9999 も Null も表に無いが、判定は呼び出し側でする。ここでは落とさない。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([('27203-bldg-9', '9999'), ('27203-bldg-8', 'Null')]),
                 encoding='utf-8')
    classes, _ = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {'27203-bldg-9': '9999', '27203-bldg-8': 'Null'}


def test_parse_gml_classes_ignores_building_without_id(tmp_path):
    """建物 ID を持たない建物は突き合わせようがない。"""
    p = tmp_path / 'a.gml'
    p.write_text(_gml([(None, '3003')]), encoding='utf-8')
    classes, saw = apply_walless_roof.parse_gml_classes(str(p))
    assert classes == {}
    assert saw is True
```

- [ ] **Step 6: 4 件とも通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py -v`
Expected: 5 passed

- [ ] **Step 7: 名前空間で引く実装に戻して赤くなることを確かめる**

`parse_gml_classes` の `buildingID` の行を、URI を決め打ちする形へ一時的に変える。

```python
            elif elem.tag == '{https://www.geospatial.jp/iur/uro/3.1}buildingID' and cur['id'] is None:
```

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_parse_gml_classes_survives_other_uro_namespace -v`
Expected: FAIL (`assert {} == {'53391550-bldg-1': '3004'}`)

確かめたら元に戻す。

Run: `python3 -m pytest tests/test_reimport_walless_roof.py -v`
Expected: 5 passed

- [ ] **Step 8: コミット**

```bash
git add scripts/reimport/apply_walless_roof.py tests/test_reimport_walless_roof.py
git commit -m "feat(reimport): GML から建物区分を読む"
```

---

### Task 2: .osm の way を書き換える

**Files:**
- Modify: `scripts/reimport/apply_walless_roof.py`
- Test: `tests/test_reimport_walless_roof.py`

**Interfaces:**
- Consumes: `parse_gml_classes(path) -> tuple[dict[str, str], bool]`
- Produces: `rewrite_osm(path, classes) -> dict`
  返す辞書のキーは `buildings` / `joined` / `rewritten` / `unknown_code` / `no_class` / `unjoinable` の 6 つで、値は int。

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_reimport_walless_roof.py` に足す。

```python
def _osm(ways):
    """ways は (way id, タグの辞書) の並び。"""
    parts = []
    for wid, tags in ways:
        t = ''.join('<tag k="%s" v="%s"/>' % (k, v) for k, v in tags.items())
        parts.append('<way id="%s">%s</way>' % (wid, t))
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<osm version="0.6" generator="test">%s</osm>' % ''.join(parts))


def _tags_of(path, way_id):
    import xml.etree.ElementTree as ET
    root = ET.parse(path).getroot()
    for w in root.findall('way'):
        if w.get('id') == way_id:
            return {t.get('k'): t.get('v') for t in w.findall('tag')}
    raise AssertionError('way %s が無い' % way_id)


def test_rewrite_osm_rewrites_building_key(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-1', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    assert _tags_of(p, '-1')['building'] == 'roof'
    assert stat['rewritten'] == 1
```

- [ ] **Step 2: 落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_rewrite_osm_rewrites_building_key -v`
Expected: FAIL with `AttributeError: module 'apply_walless_roof' has no attribute 'rewrite_osm'`

- [ ] **Step 3: 最小の実装を書く**

`scripts/reimport/apply_walless_roof.py` に足す。

```python
def _atomic_write(tree, path):
    """書き終えてから名前を付ける。切り詰めた .osm を残さない。"""
    part = path + '.part'
    tree.write(part, encoding='utf-8', xml_declaration=True)
    os.replace(part, path)


def rewrite_osm(path, classes):
    """無壁舎の way を `roof` にして書き戻す。要約を返す。

    `building` と `building:part` の両方を見る。取り込み器は `building` を
    優先し、無ければ `building:part` を採る。融合は取り込まれる側の way の
    キーを `building:part` へ降格させるので、無壁舎の過半は降格側に載る。
    """
    stat = {'buildings': 0, 'joined': 0, 'rewritten': 0,
            'unknown_code': 0, 'no_class': 0, 'unjoinable': 0}
    tree = ET.parse(path)
    root = tree.getroot()
    changed = False
    for way in root.findall('way'):
        tags = {t.get('k'): t for t in way.findall('tag')}
        if 'building' in tags:
            key = 'building'
        elif 'building:part' in tags:
            key = 'building:part'
        else:
            continue
        stat['buildings'] += 1
        ref = tags.get('ref:MLIT_PLATEAU')
        if ref is None:
            # 融合が作る合成形状。取り込み器も取り込まないので扱わない。
            stat['unjoinable'] += 1
            continue
        code = classes.get(ref.get('v'))
        if code is None:
            stat['no_class'] += 1
            continue
        stat['joined'] += 1
        if code in WALLLESS:
            tags[key].set('v', 'roof')
            stat['rewritten'] += 1
            changed = True
        elif code not in KNOWN_CLASSES:
            stat['unknown_code'] += 1
    if changed:
        _atomic_write(tree, path)
    return stat
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_rewrite_osm_rewrites_building_key -v`
Expected: PASS

- [ ] **Step 5: 残りの経路のテストを足す**

```python
def test_rewrite_osm_rewrites_demoted_key(tmp_path):
    """融合で building:part に降格した無壁舎も書き換える。

    豊中市 50 メッシュでは無壁舎 26,842 本のうち 15,507 本が降格側だった。
    building だけを見る実装は、ここで 58% を取りこぼす。
    """
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-2', {'building:part': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-2'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {'c-bldg-2': '3004'})
    assert _tags_of(p, '-2')['building:part'] == 'roof'
    assert stat['rewritten'] == 1


def test_rewrite_osm_leaves_ordinary_buildings(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-3', {'building': 'house',
                               'ref:MLIT_PLATEAU': 'c-bldg-3'}),
                       ('-4', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-4'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(
        str(p), {'c-bldg-3': '3001', 'c-bldg-4': '3002'})
    assert _tags_of(p, '-3')['building'] == 'house'
    assert _tags_of(p, '-4')['building'] == 'yes'
    assert stat['rewritten'] == 0
    assert stat['joined'] == 2


def test_rewrite_osm_counts_out_of_codelist_code(tmp_path):
    """9999 と Null は表に無い。書き換えず、件数だけ数える。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-5', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-5'}),
                       ('-6', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-6'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(
        str(p), {'c-bldg-5': '9999', 'c-bldg-6': 'Null'})
    assert _tags_of(p, '-5')['building'] == 'yes'
    assert stat['rewritten'] == 0
    assert stat['unknown_code'] == 2


def test_rewrite_osm_skips_way_without_identifier(tmp_path):
    """識別子の無い way は融合の合成形状で、取り込み器も取り込まない。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-7', {'building': 'yes'})]), encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {})
    assert _tags_of(p, '-7')['building'] == 'yes'
    assert stat['unjoinable'] == 1
    assert stat['joined'] == 0


def test_rewrite_osm_counts_building_without_class(tmp_path):
    """区分を持たない都市がある。突き合わせは通るが直らない。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-8', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-8'})]),
                 encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {})
    assert stat['no_class'] == 1
    assert stat['joined'] == 0


def test_rewrite_osm_leaves_non_building_ways_alone(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-9', {'highway': 'residential'})]), encoding='utf-8')
    stat = apply_walless_roof.rewrite_osm(str(p), {})
    assert stat['buildings'] == 0


def test_rewrite_osm_is_idempotent(tmp_path):
    """再実行しても区分は変わらないので結果も変わらない。"""
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-1', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
                 encoding='utf-8')
    apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    stat = apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    assert _tags_of(p, '-1')['building'] == 'roof'
    assert stat['rewritten'] == 1


def test_rewrite_osm_leaves_no_part_file(tmp_path):
    p = tmp_path / 'a.osm'
    p.write_text(_osm([('-1', {'building': 'yes',
                               'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
                 encoding='utf-8')
    apply_walless_roof.rewrite_osm(str(p), {'c-bldg-1': '3003'})
    assert not (tmp_path / 'a.osm.part').exists()
```

- [ ] **Step 6: 全部通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py -v`
Expected: 14 passed

- [ ] **Step 7: 降格側を見ない実装に戻して赤くなることを確かめる**

`rewrite_osm` の分岐を一時的に次へ変える。

```python
        if 'building' in tags:
            key = 'building'
        else:
            continue
```

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_rewrite_osm_rewrites_demoted_key -v`
Expected: FAIL (`KeyError` か `assert 0 == 1`)

確かめたら元に戻す。

Run: `python3 -m pytest tests/test_reimport_walless_roof.py -v`
Expected: 14 passed

- [ ] **Step 8: コミット**

```bash
git add scripts/reimport/apply_walless_roof.py tests/test_reimport_walless_roof.py
git commit -m "feat(reimport): 無壁舎の way を building=roof にする"
```

---

### Task 3: ディレクトリをまとめて処理して要約を出す

**Files:**
- Modify: `scripts/reimport/apply_walless_roof.py`
- Test: `tests/test_reimport_walless_roof.py`

**Interfaces:**
- Consumes: `parse_gml_classes`, `rewrite_osm`
- Produces: `apply_dir(work_dir) -> dict`
  キーは `meshes` / `meshes_without_class` と、`rewrite_osm` の 6 つの合計。
  CLI は要約を JSON 1 行で標準出力に出す。

- [ ] **Step 1: 失敗するテストを書く**

```python
def test_apply_dir_reports_mesh_without_class(tmp_path):
    """区分を持たない都市を黙って通さない。

    守口市 27209 は標本の 12 メッシュで bldg:class が 1 件も無かった。
    建物 ID はあるので突き合わせは通り、1 件も直らないまま完走する。
    """
    (tmp_path / 'm1.gml').write_text(_gml([('c-bldg-1', None)]), encoding='utf-8')
    (tmp_path / 'm1.osm').write_text(
        _osm([('-1', {'building': 'yes', 'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
        encoding='utf-8')
    out = apply_walless_roof.apply_dir(str(tmp_path))
    assert out['meshes'] == 1
    assert out['meshes_without_class'] == 1
    assert out['rewritten'] == 0
```

- [ ] **Step 2: 落ちることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_apply_dir_reports_mesh_without_class -v`
Expected: FAIL with `AttributeError: module 'apply_walless_roof' has no attribute 'apply_dir'`

- [ ] **Step 3: 最小の実装を書く**

`scripts/reimport/apply_walless_roof.py` に足す。

```python
def apply_dir(work_dir):
    """作業ディレクトリの `.osm` を全部処理して合計を返す。"""
    total = {'meshes': 0, 'meshes_without_class': 0, 'buildings': 0,
             'joined': 0, 'rewritten': 0, 'unknown_code': 0,
             'no_class': 0, 'unjoinable': 0}
    names = sorted(n for n in os.listdir(work_dir) if n.endswith('.osm'))
    for name in names:
        osm = os.path.join(work_dir, name)
        gml = osm[:-4] + '.gml'
        # 対の .gml が無いと区分を引けない。黙って素通りさせると、
        # 直っていないことに気づけないまま転送まで進む。
        if not os.path.exists(gml):
            raise SystemExit('対の .gml が無い: %s' % os.path.basename(gml))
        classes, saw_class = parse_gml_classes(gml)
        if not saw_class:
            total['meshes_without_class'] += 1
        stat = rewrite_osm(osm, classes)
        total['meshes'] += 1
        for k, v in stat.items():
            total[k] += v
    return total


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    print(json.dumps(apply_dir(sys.argv[1]), ensure_ascii=False))
```

- [ ] **Step 4: 通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_apply_dir_reports_mesh_without_class -v`
Expected: PASS

- [ ] **Step 5: 残りのテストを足す**

```python
def test_apply_dir_sums_across_meshes(tmp_path):
    for n, code in (('m1', '3003'), ('m2', '3001')):
        (tmp_path / (n + '.gml')).write_text(
            _gml([('c-bldg-' + n, code)]), encoding='utf-8')
        (tmp_path / (n + '.osm')).write_text(
            _osm([('-1', {'building': 'yes',
                          'ref:MLIT_PLATEAU': 'c-bldg-' + n})]),
            encoding='utf-8')
    out = apply_walless_roof.apply_dir(str(tmp_path))
    assert out['meshes'] == 2
    assert out['meshes_without_class'] == 0
    assert out['rewritten'] == 1
    assert out['joined'] == 2


def test_apply_dir_fails_when_gml_missing(tmp_path):
    (tmp_path / 'm1.osm').write_text(_osm([]), encoding='utf-8')
    with pytest.raises(SystemExit) as e:
        apply_walless_roof.apply_dir(str(tmp_path))
    assert 'm1.gml' in str(e.value)


def test_cli_prints_json_summary(tmp_path):
    (tmp_path / 'm1.gml').write_text(_gml([('c-bldg-1', '3003')]),
                                     encoding='utf-8')
    (tmp_path / 'm1.osm').write_text(
        _osm([('-1', {'building': 'yes', 'ref:MLIT_PLATEAU': 'c-bldg-1'})]),
        encoding='utf-8')
    r = subprocess.run([sys.executable, str(TOOL), str(tmp_path)],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert out['rewritten'] == 1


def test_cli_fails_without_argument():
    r = subprocess.run([sys.executable, str(TOOL)],
                       capture_output=True, text=True)
    assert r.returncode != 0
```

- [ ] **Step 6: 全部通ることを確かめる**

Run: `python3 -m pytest tests/test_reimport_walless_roof.py -v`
Expected: 19 passed

- [ ] **Step 7: `.gml` が無いときに素通りする実装に戻して赤くなることを確かめる**

`apply_dir` の存在検査を一時的に次へ変える。

```python
        if not os.path.exists(gml):
            continue
```

Run: `python3 -m pytest tests/test_reimport_walless_roof.py::test_apply_dir_fails_when_gml_missing -v`
Expected: FAIL (`DID NOT RAISE <class 'SystemExit'>`)

確かめたら元に戻す。

Run: `python3 -m pytest tests/test_reimport_walless_roof.py -v`
Expected: 19 passed

- [ ] **Step 8: コミット**

```bash
git add scripts/reimport/apply_walless_roof.py tests/test_reimport_walless_roof.py
git commit -m "feat(reimport): メッシュをまとめて処理して要約を出す"
```

---

### Task 4: ship_city.sh に組み込む

**Files:**
- Modify: `scripts/reimport/ship_city.sh:58-60` (終了コードの定義)
- Modify: `scripts/reimport/ship_city.sh:206,235,269,272,305` (段の番号)
- Modify: `scripts/reimport/ship_city.sh:269` の直前 (工程の挿入)
- Test: `tests/test_ship_city.py`

**Interfaces:**
- Consumes: `apply_walless_roof.py` の CLI (`python3 apply_walless_roof.py <作業ディレクトリ>`、成功時 exit 0)

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_ship_city.py` の末尾に足す。
既存の `env` フィクスチャと `_run` / `_good_extract` / `_good_java` / `_good_transfer` をそのまま使う。
`env` にメソッドを足す必要は無い。

既存のテストはこの工程を足しても赤くならない。
偽 extract が書く `.gml` は `<x/>`、偽 java が書く `.osm` は `<osm><node/></osm>` で、
どちらも解析できる XML である。
建物が 1 つも無いので、書き換えは 0 件で正常終了する。

```python
def test_walless_step_runs_before_manifest(env):
    """無壁舎の書き換えは manifest より前に走る。

    転送は .osm と manifest.txt だけを送るので、.gml が手元にある
    この時点でしか区分を当てられない。
    """
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert '無壁舎' in r.stdout
    assert r.stdout.index('無壁舎') < r.stdout.index('manifest')


def test_walless_step_failure_stops_before_transfer(env):
    """書き換えが落ちたら転送しない。直っていない .osm を送らない。

    取り出しは extract_stub なので、python3 を潰してもこの工程だけが落ちる。
    """
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=2)
    _stub(env.bin, 'python3', 'exit 1')

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr
    assert '転送' not in r.stdout
```

- [ ] **Step 2: 落ちることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py::test_walless_step_runs_before_manifest -v`
Expected: FAIL. `無壁舎` が標準出力に出ないので `assert '無壁舎' in r.stdout` で落ちる。

- [ ] **Step 3: 終了コードを足す**

`scripts/reimport/ship_city.sh` の 58 行目から 60 行目を次にする。

```bash
EXIT_EXTRACT=10
EXIT_CONVERT=11
EXIT_TRANSFER=12
EXIT_WALLESS=13
```

- [ ] **Step 4: 工程を挿入して段の番号を直す**

`say "3/5 manifest"` の直前に足す。

```bash
say "3/6 無壁舎"
# 区分は .gml にしかなく、変換器は出力しない。転送は .osm と manifest.txt
# だけを送るので、.gml が並んでいるこの時点でしか当てられない。
WALLESS_OUT=$(python3 "$(dirname "$0")/apply_walless_roof.py" "$WORK")
WALLESS_EXIT=$?
if [ "$WALLESS_EXIT" -ne 0 ]; then
  bail "$EXIT_WALLESS" "無壁舎の書き換えが exit $WALLESS_EXIT"
fi
say "無壁舎 $WALLESS_OUT"
```

段の番号を 5 から 6 に直す。

```bash
say "1/6 取り出し"      # 206 行目
say "2/6 変換"          # 235 行目
say "4/6 manifest"      # もとの 269 行目
say "5/6 転送"          # もとの 272 行目
say "6/6 記録して掃除"  # もとの 305 行目
```

- [ ] **Step 5: 通ることを確かめる**

Run: `python3 -m pytest tests/test_ship_city.py -v`
Expected: 既存のテストを含めて全部 PASS

- [ ] **Step 6: 工程を manifest の後ろへ動かして赤くなることを確かめる**

挿入した塊を `echo "$OSM_N" > "$WORK/manifest.txt"` の後ろへ一時的に動かす。

Run: `python3 -m pytest tests/test_ship_city.py::test_walless_step_runs_before_manifest -v`
Expected: FAIL (`assert 位置 < 位置` が偽)

確かめたら元に戻す。

- [ ] **Step 7: 全体のテストを走らせる**

Run: `python3 -m pytest -q`
Expected: 既存の 459 件に今回の分が足された数で、失敗 0

- [ ] **Step 8: 手元の実データで 1 メッシュだけ当てて確かめる**

退避してある豊中市の作業ディレクトリから 1 対だけを一時ディレクトリへ複製し、実データで走らせる。
書き換え前後の件数が設計の実測と合うことを見る。

`SRC` には、失敗して退避された作業ディレクトリを指定する。
場所は `ship.env` の `WORK_ROOT` の下で、名前は `<都市コード>.failed.<日時>` である。
`ship.env` は公開しないので、ここにはパスを書かない。

```bash
TMP=$(mktemp -d)
SRC=<退避された作業ディレクトリ>          # WORK_ROOT/<都市コード>.failed.<日時>
STEM=$(basename "$(ls "$SRC"/*.gml | head -1)" .gml)
cp "$SRC/$STEM.gml" "$SRC/$STEM.osm" "$TMP/"
python3 scripts/reimport/apply_walless_roof.py "$TMP"
grep -c 'v="roof"' "$TMP"/*.osm
rm -rf "$TMP"
```

Expected: JSON の `rewritten` と `grep -c` の数が一致する。`meshes` は 1。

- [ ] **Step 9: 機微情報を検査してコミット**

公開リポジトリなので、サーバの識別子と内部パスが入っていないことを確かめる。
検査する文字列そのものを計画や差分に書くと、検査が検査対象を持ち込む形になる。
名指しはせず、形で見る。

```bash
# 絶対パスの形、IP の形、ホスト名らしき形
git diff --cached | grep -nE "(^|[^a-zA-Z])/[a-z]+/[a-z]+/|[0-9]{1,3}(\.[0-9]{1,3}){3}|[a-z]+[0-9]+-[0-9]+-[0-9]+"
# 昇格を伴う操作の形
git diff --cached | grep -niE "\bsu(do)?\b|OWNER TO"
```

出たものが、テストの一時ディレクトリや `scripts/reimport/` のような
リポジトリ内の相対パスだけであることを目で確かめてからコミットする。

```bash
git add scripts/reimport/ship_city.sh tests/test_ship_city.py
git commit -m "feat(reimport): 無壁舎の書き換えを変換と manifest のあいだに挟む"
```

---

### Task 5: 手順書を更新する

**Files:**
- Modify: `scripts/reimport/README.md`

`deploy/README.md` はサーバ側の手順書で、この工程は手元で完結するため触らない。

- [ ] **Step 1: 手元側の手順書に足す**

`scripts/reimport/README.md` の第 1 段の説明に、工程が 1 つ増えたことを書く。

```markdown
## 無壁舎の書き換え

変換の直後、`manifest.txt` を作る前に `apply_walless_roof.py` が走る。
PLATEAU の建物区分が無壁舎 (`bldg:class` 3003 / 3004) の建物を
`building=roof` にする。カーポートと庇がここに入る。

区分は元データ (`.gml`) にしかなく、変換器は出力しない。
転送は `.osm` と `manifest.txt` だけを送るので、`.gml` が手元に並んでいる
この時点でしか当てられない。

`ship_city.sh` が要約を 1 行で出す。

- `rewritten` が書き換えた件数
- `meshes_without_class` が区分を 1 件も持たなかったメッシュの数

**`meshes_without_class` がメッシュ数と同じなら、その都市は 1 件も直っていない。**
区分を持たない都市が実在する (守口市 27209 の標本 12 メッシュで確認)。
突き合わせ自体は通るので、この数を見ないと気づけない。
```

- [ ] **Step 2: コミット**

```bash
git add scripts/reimport/README.md
git commit -m "docs(reimport): 無壁舎の書き換えを手順書に書く"
```

---

## 自己確認

**設計の網羅:** 設計の「決めたこと」の 6 項目を追う。
位置は Task 4、突き合わせは Task 1、書き換えの規則は Task 2、触らないものは Task 2 のテスト 3 件、報告は Task 3、べき等性は Task 2 の `test_rewrite_osm_is_idempotent`、テストは各タスクに入っている。
「a の判定は別途検討する」は実装を伴わないので、タスクを立てない。

**型の一致:** `parse_gml_classes` は `(dict, bool)` を返し、Task 3 の `apply_dir` が `classes, saw_class` で受ける。
`rewrite_osm` が返す 6 つのキーは Task 3 の `total` の初期値に同じ名前で並んでいる。

**置き場所:** `apply_walless_roof.py` は `scripts/reimport/` に置く。
`tests/conftest.py` がこのディレクトリを `sys.path` に通しているので、テストから `import apply_walless_roof` で引ける。
