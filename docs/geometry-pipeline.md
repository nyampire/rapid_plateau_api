# 建物の図形がたどる経路

CityGML の建物が Rapid の編集画面に出るまでに、図形は 3 か所で作り替えられる。
変換器 [citygml-osm](https://github.com/yuuhayashi/citygml-osm)、取り込み器 `plateau_importer2postgis.py`、
配信 API `osmfj_plateau_api.py` である。

この文書は**どの段が図形に何をするか**を通しで並べる。
「この形はどこで決まったのか」を追うときの地図として読む。

## 番号の読み方

段に `01`〜`06`、その中の工程に `02-1`、個々の処理に `02-1-3` の番号を振ってある。
話すときのポインタに使う。

変換器の枝番 `02-1`〜`02-6` は、`GmlLoadRoute` のルート定義に書かれた (1)〜(6) と同じ番号である。

他の文書との棲み分けは次のとおり。

| 文書 | 扱うもの |
|---|---|
| この文書 | 段ごとの処理と、その順序 |
| [converter-output.md](converter-output.md) | 変換出力の性質と、取り込みでどこまで対処できるか。実測値 |
| [upstream-consultation.md](upstream-consultation.md) | 上流に何を相談するか |
| [../ARCHITECTURE.md](../ARCHITECTURE.md) | リポジトリの構造と実装の経緯 |

---

## 全体図

```
  CityGML (.gml)                        配信元の .osm
       |                                     |
       | 01 extract_city.py                  | plateau_downloader.py
       |    (Range で bldg だけ取り出す)      |  (zip を落として展開)
       v                                     |
  02 citygml-osm 1st                         |
       |  ★ 3D を平面に潰す                   |
       |  ★ 接する建物を融合する               |
       |  ★ 合成外形を作る                    |
       v                                     |
  03 apply_walless_roof.py                   |
       |    (無壁舎に building=roof)          |
       v                                     v
       +----------------> .osm <-------------+
                           |
                           v
           04 plateau_importer2postgis.py
                           |  ★ 合成物を落とす
                           |  ★ 穴つきポリゴンを組む
                           |  ★ 重複と極小を捨てる
                           v
                    PostgreSQL/PostGIS
                           |
           05 plateau_coverage.py (都市の範囲を別に作る)
                           |
                           v
               06 osmfj_plateau_api.py
                           |  ★ 重複を畳む
                           |  ★ way と relation を組む
                           v
                     Rapid エディタ
```

★ が付いた段だけが図形を作り替える。

入力には 2 つの経路がある。
配信元から変換済みの `.osm` を落とす経路と、CityGML を自前で変換する経路である。
本番の 137 都市は前者で入っており、**変換に使われたバージョンは特定できていない**。
再取り込みは後者で行う。

---

## 01 取り出し (`scripts/reimport/extract_city.py`)

**図形は素通りする。**

配信元の zip は全国で 1,356 GB あるが、要るのは `udx/bldg/*.gml` だけで 8.84 GB しかない。
zip 末尾の中央ディレクトリだけ読んで位置を割り出し、HTTP Range でそこだけ取り出す。

手順は [../scripts/reimport/README.md](../scripts/reimport/README.md) にある。

---

## 02 変換 (citygml-osm の `1st`)

`GmlLoadRoute` が 6 段のルートを組む。
図形の処理は `02-1` `02-2` `02-4` に集まっている。

### 02-1 パース — `CityModelParser`

| # | すること | 場所 | 内容 |
|---|---|---|---|
| 02-1-1 | 平面化 | `endGmlPosList` | `gml:posList` は「緯度 経度 標高」で来る。ノードには緯度経度しか残さず、標高はリング単位で最小を `ele`、最大を `maxele` のタグにする |
| 02-1-2 | ノードの共有 | `putNode` | 緯度経度の文字列をキーに、同じ位置のノードを 1 つにする |
| 02-1-3 | 壊れたリングの排除 | `isOverlapped` | 3 点未満、閉じていない、同じ位置のノードを 2 度含むリングを捨てる |
| 02-1-4 | リングの正規化 | `ElementWay.toArea` | 末尾のノードが先頭と同位置なら先頭ノードに付け替えて閉じる。同時に boxcel インデックスに登録する |
| 02-1-5 | LOD の選択 | `solidWay` フラグ | `lod0RoofEdge` / `lod0FootPrint` があればそれを外形にする。無ければ `lod1Solid` / `lod2Solid` の面から作る |
| 02-1-6 | 高さの算出 | `endBldgLod1Solid` | 全ての面の `ele` の最小と `maxele` の最大を取り、差が 1 m を超えるときだけ `height` を出す |
| 02-1-7 | 重複外形の破棄 | Issue #137 の分岐 | 外形リングが立体の面と同位置なら way を消す |
| 02-1-8 | 穴の取り込み | `endGmlInterior` | `gml:interior` を `inner` として足す。inner の way からは `height` / `ele` / `ref:MLIT_PLATEAU` を外す |

**3D が 2D になるのは 02-1-1 である。**
標高はノードから消え、リングのタグとして残る。

**`outer` を持たない multipolygon の起点は 02-1-7 である。**
外形が Issue #137 の重複判定で捨てられると、穴だけを持つ relation が残る。
経緯と修正は [converter-output.md](converter-output.md) にある。

02-1-5 では、`lod1Solid` を読んだ後は `lod2Solid` と `bldg:boundedBy` を無視する (上流 #149)。

### 02-2 融合 — `RelationMarge`

**出力の性質のほとんどはこの段に由来する。**
javadoc に明記された意図的な設計で、v2.3.1 から変わっていない。

```
入力                          出力
+-------+                     +===============+   合成外形の way (新規)
| 建物A |--+                  |  +----+ +---+ |   building=* / 建物 ID なし
+-------+  | 外壁の線分を      |  | A  | | B | |
        +-------+  共有        |  +----+ +---+ |   実在建物 2 本
        | 建物B |     ==>      +===============+   building:part=* / 建物 ID あり
        +-------+
                                type=building の relation が両者を束ねる
```

| # | すること | 場所 | 内容 |
|---|---|---|---|
| 02-2-1 | 接触の判定 | `checkParts` → `MargeFactory.isDuplicateSegment` | boxcel インデックスで候補を絞り、2 本の way が同じ線分を持つかを見る。角が 1 点触れているだけでは融合しない |
| 02-2-2 | 降格 | `matomeru` | 取り込まれた側の `building` を `building:part` に付け替える。値 (建物の型) は残る |
| 02-2-3 | 識別子の除去 | `margeTagValue` | 融合された relation の outline から `ref:MLIT_PLATEAU` を消す |
| 02-2-4 | 重なる inner の始末 | `DuplicateInnerProcessor` (上流 #138) | 穴の位置に建物があるとき、inner を消して建物側を part として扱い直す |

**元データに `bldg:BuildingPart` は存在しない。**
出力の `building:part` はすべて 02-2-2 の産物で、真の部分立体ではない。

**識別子を持たない multipolygon は 02-2-3 でしか生まれない。**

### 02-3 メンバーの少ない relation の解体 — `BuildingGarbage`

メンバーが 1 個以下の `type=building` と `type=multipolygon` を解体する。

### 02-4 合成外形の生成 — `OutlineFactory`

`MargeFactory.marge` が合成外形を作る。

| # | すること | 内容 |
|---|---|---|
| 02-4-1 | 線分を集める | part の全ての線分を 1 つのリストにする |
| 02-4-2 | 接する辺を消す | 2 度出てくる線分を消す |
| 02-4-3 | つなぎ直す | 残った線分を端点でつないでリングにする |
| 02-4-4 | outer を選ぶ | 最大緯度が一番大きいリングを `outer`、残りを `inner` にする |
| 02-4-5 | 高さを決める | relation の `height` は member の最大、`ele` は最小を取る |

02-4-4 は包含関係ではなく緯度で決めている。

02-4-5 で付く高さは、実在の建物のものではない。

### 02-5 重複する part の除去 — `OsmMargeWay`

outline と同じ形の part を消す。
続けて relation に属さない way を消し、relation の `name` を決める。

### 02-6 出力

`.osm` に書き出す。
その前に way に属さない node を消す。

### 変換器を直すときに崩してはいけない前提

取り込みは次の 4 つに依存している。

1. 合成外形は建物 ID を持たない
2. 実在建物は建物 ID を持つ
3. `building:part` の値は元の建物の型である
4. multipolygon の `outer` はちょうど 1 本である

とくに 1 が崩れると、実在しない建物が DB に入る。

---

## 03 無壁舎の書き換え (`scripts/reimport/apply_walless_roof.py`)

**図形は素通りする。**

建物区分が無壁舎 (`bldg:class` 3003 / 3004) の建物を `building=roof` にする。
カーポートと庇がここに入る。

区分は元データにしかなく、変換器は出力しない。
`.gml` と `.osm` が並ぶのは変換の直後だけなので、当てられるのはこの時点しかない。
`uro:buildingID` と `ref:MLIT_PLATEAU` を鍵に突き合わせる。

`building` と `building:part` の両方を見る。
02-2-2 の降格があるため、無壁舎の過半は降格側に載る。

---

## 04 取り込み (`plateau_importer2postgis.py`)

変換出力の性質を承知の上で実在建物だけを選び直し、PostGIS のポリゴンに組み直す段である。
`parse_osm_file_safe` が 04-1 と 04-2、`process_buildings_safe` が 04-3 を受け持つ。

### 04-1 選別

| # | すること | 内容 |
|---|---|---|
| 04-1-1 | `type=building` の relation を読まない | outline は合成形状なので、親子関係を作らない。`parent_building_id` が新規取り込みで常に NULL なのはこのため |
| 04-1-2 | 建物 ID の無い建物要素を落とす | `ref:MLIT_PLATEAU` の無い `building` / `building:part` は合成物とみなす。way にも multipolygon にも同じ規則を当てる |
| 04-1-3 | 降格した建物を建物として扱う | 建物 ID があれば `building` として保存する。`building` が無ければ `building:part` の値を型として読む |
| 04-1-4 | 範囲外のノードを捨てる | 既定は緯度 20〜46 度、経度 122〜154 度。範囲内のノードだけを 7 桁の座標をキーに共有する |

判定の基準は一貫して「建物 ID を持つか」の 1 つである。

### 04-2 穴の組み立てと二重出力の統合

穴のある建物は 2 つの要素で出てくることがある。

| | 形 | 識別子 |
|---|---|---|
| way | 穴が潰れている | 建物 ID |
| multipolygon | 穴がある | `gml:id` |

**片方ずつしか持っていない。**

| # | すること | 内容 |
|---|---|---|
| 04-2-1 | 相方を照合する | 外側リングのノード列を、始点と向きの違いを吸収して照合する (`_ring_key`) |
| 04-2-2 | 1 棟にまとめる | ジオメトリは multipolygon から、識別子は way から取る |
| 04-2-3 | 組める multipolygon を選ぶ | `outer` がちょうど 1 本のものだけを組む。0 本または複数本は WARNING を出してスキップする |
| 04-2-4 | 壊れた穴を落とす | 閉じた後に 4 点未満の inner は、その環だけ捨てて建物は残す |

04-2-2 をしないと way が先に登録され、multipolygon は 04-3-1 の重複判定に当たって捨てられる。
建物としては入るが、中庭が塗り潰される。

### 04-3 組み立てと投入

| # | すること | 内容 |
|---|---|---|
| 04-3-1 | 重複の判定 | 外側リングの座標を 7 桁に丸め、ソートして md5 を取る。ソートするので始点と向きの違いを吸収する |
| 04-3-2 | 極小の排除 | 三角形の面積を緯度で経度を補正した shoelace で m² に直し、`TINY_AREA_M2` (0.1 m²) 未満を捨てる |
| 04-3-3 | ジオメトリの投入 | `POLYGON(外側, 内側…)` を `ST_GeomFromText`、重心を `ST_Centroid` で入れる。ノードは `ring_id` と `sequence_id` を持って別表に入る |
| 04-3-4 | 行政界フィルタ | 重心が配布元の N03 行政界の外にある行を、投入の最後に削除する |

04-3-4 は、標準地域メッシュが複数の市区町村にまたがることへの対処である。
共有メッシュの建物は両方の都市の配布に入るので、片方を落とす。
境界が未登録の都市は素通しする。

---

## 05 対応エリアの再計算 (`plateau_coverage.py`)

**建物の形は変えない。**

都市ごとに建物の重心を集め、`ST_ConcaveHull` で輪郭を作ってマテリアライズドビューに持つ。
Rapid の低ズームで対応エリアを描くためのもので、建物のジオメトリには触らない。

取り込み器は自動で REFRESH しないので、都市を入れ替えたら別に走らせる。
再取り込みでは都市ごとに走らせず、全都市が終わってから 1 回だけにする。

---

## 06 配信 (`osmfj_plateau_api.py`)

DB の行を Rapid が読める OSM XML に組み直す段である。

| # | すること | 内容 |
|---|---|---|
| 06-1 | bbox で引く | 既定は `ST_Intersects(envelope, geom)`。`use_intersects=false` なら `ST_Contains(envelope, centroid)` |
| 06-2 | 行政界フィルタ | 04-3-4 と同じ判定を WHERE 句でもう一度掛ける。再取り込みが済んでいない都市のための防御層 |
| 06-3 | 重複を畳む | 重心 6 桁・`height`・階数・part かどうかの 5 つを鍵に 1 件にする。行政界に重心が入る都市を優先し、次に city_code の小さい方を残す |
| 06-4 | 遠い part を落とす | 記録上の親と交差しない part を出さない。判定できない NULL は落とさない |
| 06-5 | ノードの id を決める | 緯度経度を 7 桁の格子に落として単射な id を作る。同じ角に触れる way は同じ node を参照する |
| 06-6 | 要素を組み立てる | 穴の無い建物は 1 本の way。穴のある建物は環ごとにタグ無しの way を作り、`type=multipolygon` の relation にタグを付ける |
| 06-7 | 代表点を付ける | `ST_PointOnSurface` の座標をタグとして出す。重心と違って必ずポリゴンの内側に入る |

06-5 が座標から id を決めるのは、同じ角が複数の建物に属し、DB にはその数だけ行があるためである。
行 id から決めると、その応答で先に来た建物によって値が変わる。

---

## どの性質が、どこで生まれ、どこで扱われるか

| 性質 | 生まれる場所 | 扱う場所 |
|---|---|---|
| 合成外形の way が作られる | 02-2 / 02-4 | 04-1-2 建物 ID の無い建物 way を落とす |
| 実在建物が `building:part` に降格する | 02-2-2 | 04-1-3 建物 ID があれば建物として保存する |
| 建物の型が `building:part` の値側に移る | 02-2-2 | 04-1-3 値を型として読む |
| 識別子を持たない multipolygon が出る | 02-2-3 | 04-1-2 way と同じ規則で落とす |
| 同じ建物を way と multipolygon で二重に出す | 02-1 + 02-2 | 04-2-2 外側リングの一致で 1 行に統合する |
| multipolygon の識別子が `gml:id` になる | 02-1-8 付近 | 04-2-1 で相方のある分だけ回復できる |
| `outer` を持たない multipolygon が出る | 02-1-7 | 保存する外形が無い。変換器側で一部を修正済み |
| `lod1Solid` 由来の巨大 part が独立して出る | 02-1-5 | 06 で落とす予定 |
| 余分な合成 part | 02-2 | 04-1-2 で自動的に落ちる |

件数と実測、それぞれの詳細は [converter-output.md](converter-output.md) にある。

---

## 追いかけるときの手順

**変換器の挙動を確かめる。**

```bash
cp <citygml-osm>/conversion.json <作業dir>/     # 無いと FileNotFoundException
cd <作業dir>                                    # .gml を置く
java -jar <citygml-osm>/target/citygml-osm-<version>-jar-with-dependencies.jar 1st
```

引数はモード名 (`1st` / `2nd` / `3rd` / `4th` / `pack` / `unpack`) である。
ファイル名を渡すとどの分岐にも入らず、何もせず exit 0 する。
Java 17 が要る。

**取り込みの挙動を確かめる。**

DB 接続なしで `parse_osm_file_safe` を直接呼べばよい。
`tests/conftest.py` の `bare_importer` fixture が同じことをしている。

**変換器の jar は clean な checkout から作る。**
作業ツリーが実験用のブランチに置かれていることがあり、そこで作った jar は master のものと中身が違う。
再取り込みで使っている jar には手元の修正が入っている。
経緯は [upstream-consultation.md](upstream-consultation.md) にある。
