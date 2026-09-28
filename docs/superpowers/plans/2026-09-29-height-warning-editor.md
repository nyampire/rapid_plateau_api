# 高さの警告（エディタ側）実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** API が添えた高さの警告を、Rapid のサイドバー、地図の塗り、タグ転記の印と欄に表示します。

**Architecture:** `PlateauService` が API の応答を読むときに目印のタグを取り除き、建物のデータの属性 `heightWarnings` と `footprintM2` に移します。
警告の文は 1 つの util が作り、サイドバーとタグ転記の欄の両方がそれを使います。
地図は `PixiLayerRapid` と `PixiLayerHeightTransfer` で描き方を切り替えます。

**Tech Stack:** JavaScript（ES modules）、d3、PixiJS、mocha + chai（karma で実行）。

作業するリポジトリ: `Rapid`（`main` から枝を作ります）
設計文書: `rapid_plateau_api` の `docs/superpowers/specs/2026-09-28-height-warning-design.md`

## Global Constraints

- API は way と relation に `plateau:height_warning`（検査の名前を `;` でつないだ値）と `plateau:footprint_m2`（needle のときだけ）を添えます。
- この 2 つのタグは OSM に送りません。`PlateauService` で必ず取り除きます。
- 警告のある PLATEAU の建物は、塗りを `dots` の模様にします。追加できないとき（`construction`）はそちらを優先します。
- タグ転記の印は、状態が `CANDIDATE` で転記元に警告があるときだけ、マゼンタの丸に赤い縁（`0xE53935`）と「!」にします。
- 追加と適用の操作は止めません。
- 文言は `data/core.yaml`（英語）と `data/l10n/core.ja.json`（日本語）の両方に足します。
- コメントとコミットメッセージは日本語で、1 行に 1 文です。
- `git add -A` は使いません。
- 試験は `npm run test:browser` で実行します。1 つのファイルだけ試すときも、この計画では全体を実行して結果を確かめます。
- lint は `npm run lint` です。

---

### Task 1: PlateauService で目印のタグを取り除く

**Files:**
- Modify: `modules/services/PlateauService.js`（`_extractRepresentativePoint` の後に関数を足し、`_parseWay` と `_parseRelation` から呼ぶ）
- Test: `test/browser/services/PlateauService.test.js`（`#representativePoint parsing` の describe の後に足す）

**Interfaces:**
- Produces: PLATEAU の way と relation に、`heightWarnings: string[]`（警告が無ければ属性を付けない）と `footprintM2: number`（値があるときだけ）が付きます。

- [ ] **Step 1: 失敗する試験を書く**

```js
  describe('#height warning parsing', () => {
    function _makeService() {
      return new Rapid.PlateauService(new MockContext());
    }
    function _fakeDataset() {
      return { id: 'plateauJapan', cache: { seen: new Set() } };
    }
    function _parseXMLAsync(service, xml) {
      const doc = new window.DOMParser().parseFromString(xml, 'application/xml');
      return new Promise((resolve, reject) => {
        service._parseXML(_fakeDataset(), doc, { id: 'fake-tile' }, (err, result) => {
          if (err) reject(err);
          else resolve(result);
        });
      });
    }
    const nodes = `
      <node id="1" lat="35.6795" lon="139.7560"/>
      <node id="2" lat="35.6795" lon="139.7566"/>
      <node id="3" lat="35.6800" lon="139.7566"/>
      <node id="4" lat="35.6800" lon="139.7560"/>`;

    it('lifts the warning tags onto entity properties and removes them from tags', async () => {
      const xml = `<?xml version="1.0"?><osm version="0.6">${nodes}
        <way id="10">
          <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
          <tag k="building" v="yes"/>
          <tag k="height" v="18.4"/>
          <tag k="plateau:height_warning" v="needle;floor-height"/>
          <tag k="plateau:footprint_m2" v="0.0036"/>
        </way></osm>`;

      const result = await _parseXMLAsync(_makeService(), xml);
      const way = result.find(e => e.id === 'w10');

      expect(way.heightWarnings).to.eql(['needle', 'floor-height']);
      expect(way.footprintM2).to.eql(0.0036);
      expect(way.tags['plateau:height_warning']).to.be.undefined;
      expect(way.tags['plateau:footprint_m2']).to.be.undefined;
      expect(way.tags.height).to.eql('18.4');
    });

    it('leaves the properties off when there is no warning', async () => {
      const xml = `<?xml version="1.0"?><osm version="0.6">${nodes}
        <way id="10">
          <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
          <tag k="building" v="yes"/>
        </way></osm>`;

      const result = await _parseXMLAsync(_makeService(), xml);
      const way = result.find(e => e.id === 'w10');

      expect(way.heightWarnings).to.be.undefined;
      expect(way.footprintM2).to.be.undefined;
    });

    it('lifts the warning off a relation too', async () => {
      const xml = `<?xml version="1.0"?><osm version="0.6">${nodes}
        <way id="10">
          <nd ref="1"/><nd ref="2"/><nd ref="3"/><nd ref="4"/><nd ref="1"/>
        </way>
        <relation id="20">
          <member type="way" ref="10" role="outline"/>
          <tag k="type" v="building"/>
          <tag k="building" v="yes"/>
          <tag k="plateau:height_warning" v="absolute"/>
        </relation></osm>`;

      const result = await _parseXMLAsync(_makeService(), xml);
      const relation = result.find(e => e.id === 'r20');

      expect(relation.heightWarnings).to.eql(['absolute']);
      expect(relation.tags['plateau:height_warning']).to.be.undefined;
    });
  });
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `npm run test:browser`
Expected: 追加した 3 件のうち 2 件が FAIL（`heightWarnings` が undefined）

- [ ] **Step 3: 実装する**

`_extractRepresentativePoint` の直後に足します。

```js
  /**
   * _extractHeightWarning
   * API が添えた高さの警告のタグを、tags から取り除いて返す。
   * `plateau:height_warning` と `plateau:footprint_m2` は OSM のタグではない。
   * 追加した建物と一緒に OSM へ送られないよう、読めるかどうかに関わらず必ず消す。
   *
   * @param   {Object} tags  変更してよい tags（`_getTags` が作ったもの）
   * @return  {{ checks: string[], footprintM2: number|null }}
   */
  _extractHeightWarning(tags) {
    const raw = tags['plateau:height_warning'];
    const rawArea = tags['plateau:footprint_m2'];
    delete tags['plateau:height_warning'];
    delete tags['plateau:footprint_m2'];

    const checks = raw ? raw.split(';').map(s => s.trim()).filter(Boolean) : [];
    const area = parseFloat(rawArea);
    return { checks, footprintM2: Number.isFinite(area) ? area : null };
  }


  /**
   * _applyHeightWarning
   * 取り出した警告を、entity の属性に移す。警告が無ければ何も付けない。
   */
  _applyHeightWarning(entity, warning) {
    if (warning.checks.length) entity.heightWarnings = warning.checks;
    if (warning.footprintM2 !== null) entity.footprintM2 = warning.footprintM2;
  }
```

`_parseWay` を次のようにします。

```js
  _parseWay(obj, uid) {
    const attrs = obj.attributes;
    const tags = this._getTags(obj);
    const representativePoint = this._extractRepresentativePoint(tags);
    const heightWarning = this._extractHeightWarning(tags);
    const way = new osmWay({
      id: uid,
      visible: this._getVisible(attrs),
      tags: tags,
      nodes: this._getNodes(obj),
    });
    if (representativePoint) way.representativePoint = representativePoint;
    this._applyHeightWarning(way, heightWarning);
    return way;
  }
```

`_parseRelation` にも同じ 2 行を足します。
`const representativePoint = this._extractRepresentativePoint(tags);` の直後に `const heightWarning = this._extractHeightWarning(tags);` を置きます。
`if (representativePoint) relation.representativePoint = representativePoint;` の直後に `this._applyHeightWarning(relation, heightWarning);` を置きます。

- [ ] **Step 4: 試験が通ることを確かめる**

Run: `npm run test:browser`
Expected: すべて PASS

- [ ] **Step 5: コミット**

```bash
git add modules/services/PlateauService.js test/browser/services/PlateauService.test.js
git commit -m "feat(plateau): 高さの警告のタグを取り除き、建物の属性に移す"
```

---

### Task 2: 警告の文を作る util と文言

**Files:**
- Create: `modules/util/plateau_height_warning.js`
- Modify: `modules/util/index.js`（export を足す）
- Modify: `data/core.yaml`（`height_transfer:` の塊の直後に `plateau_height_warning:` を足す）
- Modify: `data/l10n/core.ja.json`（`"height_transfer": { ... },` の直後に足す）
- Test: `test/browser/util/plateau_height_warning.js`

**Interfaces:**
- Consumes: `heightWarnings`、`footprintM2`（Task 1）、`utilBuildingRelationInfo`（既存）
- Produces:
  - `utilPlateauHeightWarningMessages(entity, graph, l10n) -> string[]`
    entity が建物 relation に属していれば、relation とそのメンバーの警告もまとめ、同じ文は 1 回だけ返します。
    graph が無いときは entity だけを見ます。
  - 文言のキー: `plateau_height_warning.title`、`.advice`、`.degenerate_area`、`.part_over_outline`、`.part_over_outline_generic`、`.needle`、`.needle_generic`、`.absolute`、`.floor_height_high`、`.floor_height_low`

- [ ] **Step 1: 失敗する試験を書く**

`test/browser/util/plateau_height_warning.js`:

```js
describe('utilPlateauHeightWarningMessages', () => {
  // t は、キーと値を読める形の文字列にして返す。
  // 値の書式まで確かめるため。
  const l10n = {
    t: (key, params) => params ? `${key} ${JSON.stringify(params)}` : key
  };

  function way(id, tags, extra = {}) {
    return Object.assign(Rapid.osmWay({ id, tags, nodes: [] }), extra);
  }

  it('returns nothing for a building without a warning', () => {
    const w = way('w1', { building: 'yes', height: '7' });
    expect(Rapid.utilPlateauHeightWarningMessages(w, null, l10n)).to.eql([]);
  });

  it('writes the height and footprint into the needle message', () => {
    const w = way('w1', { building: 'yes', height: '18.4' },
      { heightWarnings: ['needle'], footprintM2: 0.0036 });
    expect(Rapid.utilPlateauHeightWarningMessages(w, null, l10n)).to.eql([
      'plateau_height_warning.needle {"height":"18.4","area":"0.0036"}'
    ]);
  });

  it('falls back to a message without numbers when the footprint is missing', () => {
    const w = way('w1', { building: 'yes', height: '18.4' },
      { heightWarnings: ['needle'] });
    expect(Rapid.utilPlateauHeightWarningMessages(w, null, l10n)).to.eql([
      'plateau_height_warning.needle_generic'
    ]);
  });

  it('tells the high side from the low side for floor-height', () => {
    const high = way('w1', { building: 'house', height: '24.6', 'building:levels': '2' },
      { heightWarnings: ['floor-height'] });
    const low = way('w2', { building: 'apartments', height: '2.97', 'building:levels': '29' },
      { heightWarnings: ['floor-height'] });
    expect(Rapid.utilPlateauHeightWarningMessages(high, null, l10n)).to.eql([
      'plateau_height_warning.floor_height_high {"per_floor":"12.3"}'
    ]);
    expect(Rapid.utilPlateauHeightWarningMessages(low, null, l10n)).to.eql([
      'plateau_height_warning.floor_height_low {"per_floor":"0.1"}'
    ]);
  });

  it('writes the height into the absolute and degenerate messages', () => {
    const w = way('w1', { building: 'yes', height: '0.5' },
      { heightWarnings: ['degenerate-area', 'absolute'] });
    expect(Rapid.utilPlateauHeightWarningMessages(w, null, l10n)).to.eql([
      'plateau_height_warning.degenerate_area',
      'plateau_height_warning.absolute {"height":"0.5"}'
    ]);
  });

  it('compares a part with its outline through the building relation', () => {
    const outline = way('w1', { building: 'yes', height: '9.1' });
    const part = way('w2', { 'building:part': 'yes', height: '149.2' },
      { heightWarnings: ['part-over-outline'] });
    const relation = Rapid.osmRelation({
      id: 'r1', tags: { type: 'building', building: 'yes', height: '9.1' },
      members: [{ id: 'w1', type: 'way', role: 'outline' },
                { id: 'w2', type: 'way', role: 'part' }]
    });
    const graph = new Rapid.Graph([outline, part, relation]);

    expect(Rapid.utilPlateauHeightWarningMessages(part, graph, l10n)).to.eql([
      'plateau_height_warning.part_over_outline {"part":"149.2","outline":"9.1","diff":"140.1"}'
    ]);
  });

  it('collects the warnings of every member when the outline is selected', () => {
    // 1 つを選んで追加すると建物全体が追加されるので、全体の警告を出す。
    const outline = way('w1', { building: 'yes', height: '9.1' });
    const part = way('w2', { 'building:part': 'yes', height: '149.2' },
      { heightWarnings: ['part-over-outline'] });
    const relation = Rapid.osmRelation({
      id: 'r1', tags: { type: 'building', building: 'yes', height: '9.1' },
      members: [{ id: 'w1', type: 'way', role: 'outline' },
                { id: 'w2', type: 'way', role: 'part' }]
    });
    const graph = new Rapid.Graph([outline, part, relation]);

    expect(Rapid.utilPlateauHeightWarningMessages(outline, graph, l10n)).to.have.length(1);
  });

  it('says the same thing only once', () => {
    // relation は外形の警告を複製して持つので、外形と relation で同じ文になる。
    const outline = way('w1', { building: 'yes', height: '0.5' },
      { heightWarnings: ['absolute'] });
    const relation = Object.assign(Rapid.osmRelation({
      id: 'r1', tags: { type: 'building', building: 'yes', height: '0.5' },
      members: [{ id: 'w1', type: 'way', role: 'outline' }]
    }), { heightWarnings: ['absolute'] });
    const graph = new Rapid.Graph([outline, relation]);

    expect(Rapid.utilPlateauHeightWarningMessages(outline, graph, l10n)).to.eql([
      'plateau_height_warning.absolute {"height":"0.5"}'
    ]);
  });
});
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `npm run test:browser`
Expected: FAIL（`Rapid.utilPlateauHeightWarningMessages is not a function`）

- [ ] **Step 3: util を実装する**

`modules/util/plateau_height_warning.js`:

```js
import { utilBuildingRelationInfo } from './building_relation.js';


// 警告は範囲の外に出たときだけ届く。
// 範囲の内側の値を 1 つ決めれば、上側と下側を分けられる。
// 3 m は一般的な階高で、範囲（1.5 m から 10.2 m）の内側にある。
const FLOOR_HEIGHT_SIDE_M = 3;


/**
 * utilPlateauHeightWarningMessages
 * PLATEAU の建物に付いた高さの警告を、利用者に見せる文にする。
 * entity が建物 relation に属していれば、relation と全メンバーの警告をまとめる。
 * 1 つを選んで追加すると、建物全体が一緒に追加されるためである。
 * 同じ文は 1 回だけ返す。
 *
 * @param   {osmEntity} entity  PLATEAU の way か relation
 * @param   {Graph?}    graph   PLATEAU の graph。無ければ entity だけを見る
 * @param   {Object}    l10n    `t(key, params)` を持つ翻訳の仕組み
 * @return  {string[]}
 */
export function utilPlateauHeightWarningMessages(entity, graph, l10n) {
  if (!entity) return [];

  const entities = [entity];
  const info = graph ? utilBuildingRelationInfo(entity, graph) : null;
  if (info) {
    entities.push(info.relation);
    for (const m of info.relation.members ?? []) {
      const member = graph.hasEntity(m.id);
      if (member) entities.push(member);
    }
  }

  const messages = [];
  for (const e of entities) {
    for (const [key, params] of _warningItems(e, graph)) {
      const text = params ? l10n.t(key, params) : l10n.t(key);
      if (!messages.includes(text)) messages.push(text);
    }
  }
  return messages;
}


function _fmt(value, digits = 1) {
  return Number(value).toFixed(digits);
}


function _findOutline(entity, graph) {
  if (!graph) return null;
  const info = utilBuildingRelationInfo(entity, graph);
  if (!info) return null;
  const member = (info.relation.members ?? []).find(m => m.role === 'outline');
  return member ? graph.hasEntity(member.id) : null;
}


/**
 * _warningItems
 * 1 つの entity の警告を、[文言のキー, 値] の組にする。
 * 値を入れられないときは、値の無い文言を使う。
 */
function _warningItems(entity, graph) {
  const items = [];
  const tags = entity.tags ?? {};
  const height = parseFloat(tags.height);

  for (const check of entity.heightWarnings ?? []) {
    if (check === 'degenerate-area') {
      items.push(['plateau_height_warning.degenerate_area', null]);

    } else if (check === 'part-over-outline') {
      const outlineHeight = parseFloat(_findOutline(entity, graph)?.tags?.height);
      if (Number.isFinite(height) && Number.isFinite(outlineHeight)) {
        items.push(['plateau_height_warning.part_over_outline', {
          part: _fmt(height), outline: _fmt(outlineHeight), diff: _fmt(height - outlineHeight)
        }]);
      } else {
        items.push(['plateau_height_warning.part_over_outline_generic', null]);
      }

    } else if (check === 'needle') {
      if (Number.isFinite(height) && Number.isFinite(entity.footprintM2)) {
        items.push(['plateau_height_warning.needle', {
          height: _fmt(height), area: String(Number(entity.footprintM2.toPrecision(2)))
        }]);
      } else {
        items.push(['plateau_height_warning.needle_generic', null]);
      }

    } else if (check === 'absolute') {
      items.push(['plateau_height_warning.absolute', { height: _fmt(height) }]);

    } else if (check === 'floor-height') {
      const levels = parseFloat(tags['building:levels']);
      const perFloor = height / levels;
      const key = (perFloor > FLOOR_HEIGHT_SIDE_M)
        ? 'plateau_height_warning.floor_height_high'
        : 'plateau_height_warning.floor_height_low';
      items.push([key, { per_floor: _fmt(perFloor) }]);
    }
  }
  return items;
}
```

`modules/util/index.js` の `utilApplyPlateauSourceTags` の行の直後に足します。

```js
export { utilPlateauHeightWarningMessages } from './plateau_height_warning.js';
```

- [ ] **Step 4: 文言を足す**

`data/core.yaml` の `height_transfer:` の塊（`area_mismatch_note:` の行まで）の直後、`  title:` の前に足します。

```yaml
  plateau_height_warning:
    title: This height may be wrong
    advice: Check the height against aerial imagery or street-level photos before adding it.
    degenerate_area: The outline is broken and its area is zero or less.
    part_over_outline: "A part of this building ({part} m) is {diff} m taller than the whole building ({outline} m)."
    part_over_outline_generic: A part of this building is more than 10 m taller than the whole building.
    needle: "The footprint is only {area} m² for a height of {height} m."
    needle_generic: The footprint is too small for the height.
    absolute: "The height is {height} m, too low for a building."
    floor_height_high: "Each floor is {per_floor} m tall, too tall for a residential building."
    floor_height_low: "Each floor is {per_floor} m tall, too low for a residential building."
```

`data/l10n/core.ja.json` の `"height_transfer": { ... },` の閉じ括弧の行の直後に、同じ字下げで足します。

```json
    "plateau_height_warning": {
      "title": "高さが誤っている可能性があります",
      "advice": "追加する前に、航空写真や街路の写真で高さを確かめてください。",
      "degenerate_area": "輪郭が壊れていて、面積が 0 以下です。",
      "part_over_outline": "建物の一部（{part} m）が、建物全体（{outline} m）より {diff} m 高くなっています。",
      "part_over_outline_generic": "建物の一部が、建物全体より 10 m を超えて高くなっています。",
      "needle": "高さ {height} m に対して、底面積が {area} m² しかありません。",
      "needle_generic": "高さに対して、底面積が小さすぎます。",
      "absolute": "高さが {height} m で、建物としては低すぎます。",
      "floor_height_high": "1 階あたりの高さが {per_floor} m で、住宅としては高すぎます。",
      "floor_height_low": "1 階あたりの高さが {per_floor} m で、住宅としては低すぎます。"
    },
```

- [ ] **Step 5: 試験が通ることを確かめる**

Run: `npm run lint && npm run test:browser`
Expected: lint のエラーなし、すべて PASS

- [ ] **Step 6: コミット**

```bash
git add modules/util/plateau_height_warning.js modules/util/index.js data/core.yaml data/l10n/core.ja.json test/browser/util/plateau_height_warning.js
git commit -m "feat(plateau): 高さの警告を利用者に見せる文にする util を足す"
```

---

### Task 3: 警告の欄を描く部品と、サイドバーへの表示

**Files:**
- Create: `modules/ui/plateau_height_warning.js`
- Modify: `modules/ui/index.js`（export を足す）
- Modify: `modules/ui/UiRapidInspector.js`（`renderHeightWarning` を足し、`render` から呼ぶ）
- Modify: `css/80_app.css`（`.plateau-tags-note` の定義の後に足す）
- Test: `test/browser/ui/UiRapidInspector.js`（試験を足す）

**Interfaces:**
- Consumes: `utilPlateauHeightWarningMessages`（Task 2）
- Produces: `uiPlateauHeightWarning($parent, messages, l10n, before?)`
  `$parent` の中に `.plateau-height-warning` を 1 つ作ります（既存のものは作り直します）。
  `before` に CSS セレクタを渡すと、その要素の前に置きます。
  messages が空なら何も作りません。
  Task 5 もこの関数を使います。

- [ ] **Step 1: 失敗する試験を書く**

`test/browser/ui/UiRapidInspector.js` の末尾の `});` の前に足します。

```js
  describe('#renderHeightWarning', () => {
    function plateauDatum(extra = {}) {
      return Object.assign(
        Rapid.osmWay({ id: 'w1', tags: { building: 'yes', height: '0.5' }, nodes: [] }),
        { __service__: 'plateau', __datasetid__: 'plateauJapan' },
        extra
      );
    }

    afterEach(() => d3.selectAll('.test-body').remove());

    it('shows the warning above the tags for a building with a warning', () => {
      const datum = plateauDatum({ heightWarnings: ['absolute'] });
      inspector.context.services.plateau.graph = () => new Rapid.Graph([datum]);
      inspector.datum = datum;

      const $body = d3.select('body').append('div').attr('class', 'test-body');
      $body.append('div').attr('class', 'tag-info');
      inspector.renderHeightWarning($body);

      const $warning = $body.select('.plateau-height-warning');
      expect($warning.empty()).to.be.false;
      expect($warning.text()).to.contain('plateau_height_warning.title');
      expect($warning.text()).to.contain('plateau_height_warning.absolute');
      expect($warning.text()).to.contain('plateau_height_warning.advice');
      // タグ一覧より前に置く。
      expect($body.node().firstChild.classList.contains('plateau-height-warning')).to.be.true;
    });

    it('shows nothing for a building without a warning', () => {
      const datum = plateauDatum();
      inspector.context.services.plateau.graph = () => new Rapid.Graph([datum]);
      inspector.datum = datum;

      const $body = d3.select('body').append('div').attr('class', 'test-body');
      inspector.renderHeightWarning($body);

      expect($body.select('.plateau-height-warning').empty()).to.be.true;
    });

    it('does not stop the accept button', () => {
      inspector.datum = plateauDatum({ heightWarnings: ['absolute'] });
      expect(inspector.isAcceptFeatureDisabled()).to.be.null;
    });
  });
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `npm run test:browser`
Expected: FAIL（`inspector.renderHeightWarning is not a function`）

- [ ] **Step 3: 部品を実装する**

`modules/ui/plateau_height_warning.js`:

```js
/**
 * uiPlateauHeightWarning
 * PLATEAU の建物の高さの警告を、見出し、理由の一覧、確かめ方の一文として描く。
 * Rapid の追加の欄とタグ転記の欄の両方で使う。
 * 描き直しのたびに作り直すので、古い欄は残らない。
 *
 * @param {d3-selection} $parent   描く先
 * @param {string[]}     messages  `utilPlateauHeightWarningMessages` の結果
 * @param {Object}       l10n      翻訳の仕組み
 * @param {string?}      before    この CSS セレクタの要素の前に置く
 */
export function uiPlateauHeightWarning($parent, messages, l10n, before) {
  $parent.selectAll('.plateau-height-warning').remove();
  if (!messages?.length) return;

  const $warning = before
    ? $parent.insert('div', before)
    : $parent.append('div');

  $warning
    .attr('class', 'plateau-height-warning')
    .attr('role', 'alert');

  $warning.append('p')
    .attr('class', 'plateau-height-warning-title')
    .text(l10n.t('plateau_height_warning.title'));

  const $list = $warning.append('ul');
  for (const message of messages) {
    $list.append('li').text(message);
  }

  $warning.append('p')
    .attr('class', 'plateau-height-warning-advice')
    .text(l10n.t('plateau_height_warning.advice'));
}
```

`modules/ui/index.js` の `UiRapidInspector` の行の直前に足します。

```js
export { uiPlateauHeightWarning } from './plateau_height_warning.js';
```

- [ ] **Step 4: サイドバーから呼ぶ**

`modules/ui/UiRapidInspector.js` の import に足します。

```js
import { uiPlateauHeightWarning } from './plateau_height_warning.js';
import { utilPlateauHeightWarningMessages } from '../util/plateau_height_warning.js';
```

コンストラクタで `this.renderTagInfo = this.renderTagInfo.bind(this);` の直後に足します。

```js
    this.renderHeightWarning = this.renderHeightWarning.bind(this);
```

`render()` の `.call(this.renderFeatureInfo)` と `.call(this.renderTagInfo)` のあいだに `.call(this.renderHeightWarning)` を足します。

`renderTagInfo` の直前にメソッドを足します。

```js
  /**
   * renderHeightWarning
   * PLATEAU の建物の高さが怪しいとき、タグ一覧の上に警告を出す。
   * 追加の操作は止めない。
   * 警告を読んだうえで追加し、あとから高さを直すこともできるためである。
   * @param {d3-selection} $selection - 描く先
   */
  renderHeightWarning($selection) {
    const datum = this.datum;
    const context = this.context;
    const l10n = context.systems.l10n;

    const service = datum ? context.services[datum.__service__] : null;
    const graph = service?.graph?.(datum.__datasetid__) ?? null;
    const messages = utilPlateauHeightWarningMessages(datum, graph, l10n);

    uiPlateauHeightWarning($selection, messages, l10n, '.tag-info');
  }
```

`$parent.insert('div', '.tag-info')` は、`.tag-info` がまだ無い初回の描画では末尾に追加します。
`render()` は警告の後で `renderTagInfo` を呼ぶので、初回でも警告がタグ一覧の上に来ます。

- [ ] **Step 5: 見た目を足す**

`css/80_app.css` の `.plateau-tags-note { ... }` の定義の後に足します。

```css
/* PLATEAU の高さの警告。
   追加を止めない注意なので、赤ではなく橙の帯にする。 */
.plateau-height-warning {
    margin: 0 0 8px;
    padding: 6px 8px;
    border-left: 4px solid #e65100;
    background: #fff3e0;
    color: #4e342e;
    font-size: 12px;
}
.plateau-height-warning-title {
    margin: 0 0 4px;
    font-weight: bold;
}
.plateau-height-warning ul {
    margin: 0 0 4px;
    padding-left: 16px;
}
.plateau-height-warning-advice {
    margin: 0;
}
```

- [ ] **Step 6: 試験が通ることを確かめる**

Run: `npm run lint && npm run test:browser`
Expected: すべて PASS

- [ ] **Step 7: コミット**

```bash
git add modules/ui/plateau_height_warning.js modules/ui/index.js modules/ui/UiRapidInspector.js css/80_app.css test/browser/ui/UiRapidInspector.js
git commit -m "feat(plateau): 高さが怪しい建物を選んだとき、サイドバーに警告を出す"
```

---

### Task 4: 地図の塗りを点の模様にする

**Files:**
- Modify: `modules/pixi/PixiLayerRapid.js`（`renderPolygons` の塗りの決め方をメソッドに分ける）
- Test: `test/browser/pixi/PixiLayerRapid.test.js`（試験を足す）

**Interfaces:**
- Consumes: `heightWarnings`（Task 1）
- Produces: `PixiLayerRapid#_polygonFill(color, addBlocked, entity) -> Object`

- [ ] **Step 1: 失敗する試験を書く**

`test/browser/pixi/PixiLayerRapid.test.js` の最後の `});` の前に足します。

```js
  describe('#_polygonFill', () => {
    function layer() {
      const scene = makeScene();
      scene.context.services.plateau = {
        isAddBlocked: () => false, startAsync: () => Promise.resolve()
      };
      return new Rapid.PixiLayerRapid(scene, 'rapid');
    }

    it('fills a building without a warning plainly', () => {
      const fill = layer()._polygonFill(0xD500F9, false, { tags: {} });
      expect(fill.pattern).to.be.undefined;
    });

    it('fills a building with a height warning with dots', () => {
      const fill = layer()._polygonFill(0xD500F9, false, { heightWarnings: ['needle'] });
      expect(fill.pattern).to.eql('dots');
    });

    it('prefers the construction stripes while adding is blocked', () => {
      const fill = layer()._polygonFill(0xD500F9, true, { heightWarnings: ['needle'] });
      expect(fill.pattern).to.eql('construction');
    });
  });
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `npm run test:browser`
Expected: FAIL（`_polygonFill is not a function`）

- [ ] **Step 3: 実装する**

`renderPolygons` の `if (feature.dirty) {` の中の `fill:` を次に置き換えます。

```js
            fill: this._polygonFill(color, addBlocked, entity)
```

`renderPolygons` の直前にメソッドを足します。

```js
  /**
   * _polygonFill
   * Rapid の候補の塗り方を決める。
   * 追加できないあいだは斜線、高さが怪しい PLATEAU の建物は点の模様にする。
   * 両方に当てはまるときは斜線を優先する。
   * 追加できないことを先に伝える必要があるためである。
   * 色はデータセットの色のままにして、どのデータセットの候補かを見分けられるようにする。
   */
  _polygonFill(color, addBlocked, entity) {
    if (addBlocked) return { width: 2, color: color, alpha: 0.3, pattern: 'construction' };
    if (entity?.heightWarnings?.length) return { width: 2, color: color, alpha: 0.3, pattern: 'dots' };
    return { width: 2, color: color, alpha: 0.3 };
  }
```

- [ ] **Step 4: 試験が通ることを確かめる**

Run: `npm run lint && npm run test:browser`
Expected: すべて PASS

- [ ] **Step 5: コミット**

```bash
git add modules/pixi/PixiLayerRapid.js test/browser/pixi/PixiLayerRapid.test.js
git commit -m "feat(plateau): 高さが怪しい建物を点の模様で塗る"
```

---

### Task 5: タグ転記の印と欄に警告を出す

**Files:**
- Modify: `modules/pixi/PixiLayerHeightTransfer.js`（印の描き方）
- Modify: `modules/ui/sections/plateau_tags.js`（欄に警告を出す）
- Test: `test/browser/pixi/PixiLayerHeightTransfer.test.js`、`test/browser/ui/sections/plateau_tags.js`

**Interfaces:**
- Consumes: `heightWarnings`（Task 1）、`utilPlateauHeightWarningMessages`（Task 2）、`uiPlateauHeightWarning`（Task 3）
- Produces: `PixiLayerHeightTransfer#_styleFor(candidate) -> Object`（`STATE_STYLE` の値か `WARNING_CANDIDATE_STYLE`）

- [ ] **Step 1: 失敗する試験を書く**

`test/browser/pixi/PixiLayerHeightTransfer.test.js` の `describe('#render', ...)` の中に足します。

```js
    it('marks a CANDIDATE with a height warning with a red ring and "!"', () => {
      const c1 = {
        plateauFeature: { id: 'p1', representativePoint: [139.755, 35.679], heightWarnings: ['absolute'] },
        state: 'CANDIDATE'
      };
      const layer = new Rapid.PixiLayerHeightTransfer(makeScene(makeMode()), 'height-transfer');

      const style = layer._styleFor(c1);
      expect(style.ring).to.eql(0xE53935);
      expect(style.glyph).to.eql('!');
      expect(style.color).to.eql(0xD500F9);
    });

    it('keeps the plain CANDIDATE mark without a warning', () => {
      const c1 = { plateauFeature: { id: 'p1', representativePoint: [139.755, 35.679] }, state: 'CANDIDATE' };
      const layer = new Rapid.PixiLayerHeightTransfer(makeScene(makeMode()), 'height-transfer');

      const style = layer._styleFor(c1);
      expect(style.ring).to.be.undefined;
      expect(style.glyph).to.be.null;
    });

    it('keeps the AREA_MISMATCH mark even with a height warning', () => {
      const c1 = {
        plateauFeature: { id: 'p1', representativePoint: [139.755, 35.679], heightWarnings: ['absolute'] },
        state: 'AREA_MISMATCH'
      };
      const layer = new Rapid.PixiLayerHeightTransfer(makeScene(makeMode()), 'height-transfer');

      expect(layer._styleFor(c1).glyph).to.eql('!?');
    });

    it('draws the "!" on the icon of a CANDIDATE with a height warning', () => {
      const c1 = {
        plateauFeature: { id: 'p1', representativePoint: [139.755, 35.679], heightWarnings: ['absolute'] },
        state: 'CANDIDATE'
      };
      const layer = new Rapid.PixiLayerHeightTransfer(makeScene(makeMode({ candidates: [c1] })), 'height-transfer');
      layer._container = makeFakeContainer();

      layer.render(0, projectIdentity, 17);

      const icon = layer._container.children[0];
      expect(icon.children[0].text).to.eql('!');
    });
```

`test/browser/ui/sections/plateau_tags.js` の末尾の `});` の前に足します。

```js
  it('shows the height warning of the Plateau building above the proposal', () => {
    const cand = candidate('CANDIDATE', {
      missingTags: ['height'],
      plateauFeature: Object.assign(
        Rapid.osmWay({ id: 'w9', tags: { building: 'yes', height: '0.5' }, nodes: [] }),
        { heightWarnings: ['absolute'] }
      )
    });
    render(new MockContext(cand));

    const $warning = wrap.select('.plateau-height-warning');
    expect($warning.empty()).to.be.false;
    expect($warning.text()).to.contain('plateau_height_warning.absolute');
    // 適用のボタンは残す。
    expect(wrap.select('.plateau-apply').empty()).to.be.false;
  });
```

- [ ] **Step 2: 試験が失敗することを確かめる**

Run: `npm run test:browser`
Expected: 追加した 5 件がすべて FAIL（`_styleFor is not a function`、印に子要素が無い、欄に `.plateau-height-warning` が無い）

- [ ] **Step 3: 印を実装する**

`modules/pixi/PixiLayerHeightTransfer.js` の `STATE_STYLE` の直後に足します。

```js
// 転記元の PLATEAU の建物に高さの警告があるときの CANDIDATE の印。
// マゼンタは「転記できる候補」、赤い縁と「!」は「注意が要る」を表す。
// 「!?」は OSM の値との食い違いと面積の不一致に使っているので、別の印にする。
const WARNING_CANDIDATE_STYLE = {
  color: 0xD500F9, radius: 7, glyph: '!', ring: 0xE53935, minZoom: MIN_CANDIDATE_ZOOM
};
```

`render` の for 文の `const style = STATE_STYLE[candidate.state];` を次に置き換えます。

```js
      const style = this._styleFor(candidate);
```

`_makeIcon` の直前にメソッドを足します。

```js
  /**
   * _styleFor
   * 候補の印の描き方を選ぶ。
   * 高さの警告で印を変えるのは CANDIDATE だけにする。
   * ほかの状態はすでに色と「!?」で注意を示しており、警告の内容は転記の欄に出るためである。
   * @param  candidate  MatchCandidate
   * @return {Object?}  STATE_STYLE の値か WARNING_CANDIDATE_STYLE
   */
  _styleFor(candidate) {
    if (candidate.state === 'CANDIDATE' && candidate.plateauFeature?.heightWarnings?.length) {
      return WARNING_CANDIDATE_STYLE;
    }
    return STATE_STYLE[candidate.state];
  }
```

`_makeIcon` の `.stroke(...)` を次に置き換えます。

```js
      .stroke({ width: style.ring ? 2.5 : 1.5, color: style.ring ?? 0xFFFFFF, alpha: 1.0 });
```

- [ ] **Step 4: 転記の欄を実装する**

`modules/ui/sections/plateau_tags.js` の先頭の import に足します。

```js
import { uiPlateauHeightWarning } from '../plateau_height_warning.js';
import { utilPlateauHeightWarningMessages } from '../../util/plateau_height_warning.js';
```

`renderContent` の中で、`const noteKey = NOTE_KEYS[cand.state];` の直前に足します。

```js
    // 転記元の PLATEAU の建物の高さが怪しいときは、提案の前に警告を出す。
    // 適用の操作は止めない。
    const plateauFeature = cand.plateauFeature;
    const plateauGraph = context.services?.plateau?.graph?.(plateauFeature?.__datasetid__) ?? null;
    const warnings = utilPlateauHeightWarningMessages(plateauFeature, plateauGraph, l10n);
    uiPlateauHeightWarning($panel, warnings, l10n);
```

- [ ] **Step 5: 試験が通ることを確かめる**

Run: `npm run lint && npm run test:browser`
Expected: すべて PASS

- [ ] **Step 6: コミット**

```bash
git add modules/pixi/PixiLayerHeightTransfer.js modules/ui/sections/plateau_tags.js test/browser/pixi/PixiLayerHeightTransfer.test.js test/browser/ui/sections/plateau_tags.js
git commit -m "feat(plateau): タグ転記の印と欄で、高さが怪しい転記元を知らせる"
```

---

### Task 6: 手元で動かして確かめ、Pull Request を作る

**Files:**
- なし（確認のみ）

- [ ] **Step 1: 全体の試験と build を回す**

Run: `npm run lint && npm run build && npm run test:unit && npm run test:browser`
Expected: 失敗なし

- [ ] **Step 2: 手元のエディタで見た目を確かめる**

API 側の変更を手元で動かし、エディタの接続先をその API にして開きます。
次の建物で、塗り、サイドバー、転記の印と欄を確かめ、スクリーンショットを撮ります。

- 住宅で 1 階あたりの高さが大きい建物（岡山市）
- 高さ 0.5 m の建物（柏市）
- 部分立体が外形より大きく高い建物（大阪市）

追加した建物のタグに、`plateau:height_warning` と `plateau:footprint_m2` が含まれないことも確かめます。

- [ ] **Step 3: Pull Request を作る**

ファイル、コミットメッセージ、PR 本文の 3 つで機微情報を確かめてから出します。
PR 本文には、スクリーンショットと、API より先に本番へ出す必要があることを書きます。
