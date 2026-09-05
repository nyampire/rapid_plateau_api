# 再取り込みの下ごしらえ

CityGML を自前で変換して都市を取り込み直すときに使う道具。
上流の配信元が落ちているあいだの代替手段で、[#21](https://github.com/nyampire/rapid_plateau_api/issues/21) の Phase 1 を人手でなぞるためのものである。

以下のコマンドはすべて `scripts/reimport/` で実行する。

## zip 全体は落とさない

148 都市の CityGML zip は合計 **1,356 GB** ある。1 都市で 269 GB のものもある。
必要なのは `udx/bldg/*.gml` だけで、そこは全国合わせて **8.84 GB**（展開 168 GB、18,352 メッシュ）しかない。

配信元は `Accept-Ranges: bytes` を返す。
zip 末尾の中央ディレクトリだけ読めば、必要なメンバの位置が判り、そこだけ Range で取り出せる。
1 都市あたりの通信は数 MB で済む。

Python の `zipfile` は seek できる file-like を渡せば ZIP64 も含めて解釈するので、
Range で読む薄い層を 1 つ用意すれば足りる（`httpzip.py`）。
`io.RawIOBase` を継承する場合は **`readinto` の実装が要る**。`read` だけでは `zipfile` が `NotImplementedError` を投げる。

## 使い方

通常の運用では下の「第 1 段 (手元) の流し方」を使う。
ここに書く手順は、都市を計画に追加するとき、または個別に確かめたいときに使う。

`ckan_download_plan.csv` は 148 都市ぶんをコミットしてあるので、
既に載っている都市を取り出すだけなら手順 1 は要らない。

```bash
# 1. 都市を足す / URL を取り直す (CKAN に問い合わせる)
#    既存の CSV は残り、渡した都市の行だけが入れ替わる
python3 build_download_plan.py 30406

#    引数なしで、載っている都市すべての URL を取り直す
python3 build_download_plan.py

#    zip の大きさを CKAN に問い合わせない (--no-size で省略できる)
python3 build_download_plan.py --no-size

# 2. 取得量とメッシュ数を見積もる (中身は落とさない)
#    出力先を省略すると ./bldg_scan.json に書く
python3 scan_bldg.py

# 3. 建物データだけ取り出す
python3 extract_city.py 30406 ./work/30406
```

出力した `.gml` は citygml-osm にそのまま渡せる。
変換したあと `.osm` をサーバの `<SHIP_PATH>/.incoming/<都市>/` へ送り、`--no-zip` を付けて取り込む。
取り込み側は開始時にこれを `<SHIP_PATH>/<都市>` へ rename してから読む。

**取り込みの前に `plateau_purge.py` を呼ぶ必要はない。**
`plateau_importer2postgis.py` は `--citycode` が渡っていれば、開始前にその都市の
既存データを自分で消す。
purge を挟むと対応エリアの再計算まで走り、メモリの小さいサーバでは OOM する。

## 第 1 段 (手元) の流し方

`ship_city.sh` は 1 都市の取り出し、変換、転送を 1 本でまとめて行う。
`ship_all.sh` は計画の一覧に載った都市を順に `ship_city.sh` へ渡す。
第 1 段はこの 2 本を使うので、`extract_city.py` を直接叩く上の手順は普段は要らない。

設定は `ship.env` から読む。

```bash
cp ship.env.example ship.env
```

`ship.env` は `JAVA_BIN`、`CITYGML_OSM_JAR`、`SHIP_HOST`、`SHIP_PATH` など、
手元の環境と転送先に合わせて書き換える値を持つ。
`KEEP_RETAINED_DIRS` は退避した作業ディレクトリを何件残すかで、既定は 3、`0` で掃除しない。
各項目の意味は `ship.env.example` のコメントに書いてある。
`ship.env` 自体は `.gitignore` に入っているので、書き換えてもコミットされない。

`CITYGML_OSM_JAR` が指す jar には、手元で当てた修正が入っている。
上流の citygml-osm 3.0.6 (`699d933`) そのままではない。
計画の 298 都市のうち 5 都市が変換で異常終了する不具合を回避するためのもので、
修正前の jar は同じディレクトリに `.orig-699d933` を付けた名前で控えてある。

経緯と、修正が既存の出力を変えないことの検証は
[upstream-consultation.md](../../docs/upstream-consultation.md) の
「4. WAY を消すときに他のリレーションの参照を外さない」にある。
上流に受け入れられたら、この段落は消す。

失敗した都市の作業ディレクトリは `<都市>.failed.<日時>` として残る。
再実行のときに前回の残骸を退かしたものは `<都市>.stale.<日時>` になる。
どちらも `WORK_ROOT` 全体で新しい順に `KEEP_RETAINED_DIRS` 件だけ残り、古いものから消える。
`ship_all.sh` は起動時に残っている件数と合計サイズを 1 行出す。

全都市を送るには `ship_all.sh` を起動する。

```bash
bash ship_all.sh
```

`shipped.txt` にある都市は飛ばすので、途中で止めても同じコマンドを再実行すれば続きから進む。
1 都市の失敗では全体を止めない。
ディスクが `DISK_MIN_KB` を割ったときだけ全体を止める。

失敗した都市だけをやり直すときも、同じ `ship_all.sh` を再実行する。
成功済みの都市は `shipped.txt` にあるので飛ばされ、失敗した都市だけが対象になる。

`ship_all.sh` は最後に `reimport_targets_<日時>.txt` を作り、`SHIP_PATH` の親へ送る。
ここまでが第 1 段で、この先はサーバ側の `deploy/README.md` に従う。

転送先をサーバ側で確かめるときは `SHIP_PATH` ではなく `SHIP_PATH/.incoming/` を見る。
`ship_city.sh` はここへ送り、確定 (rename) はサーバ側の取り込みが開始時に行うので、届いた直後の入力は `SHIP_PATH` 直下には無い。

## 全都市をやり直す

規則を変えたあと、既に送った都市にもそれを適用したいときに使う。

    bash scripts/reimport/reship_all.sh

`shipped.txt` を控えに移してから空にするので、計画の全都市が対象に戻る。
空にするのは初回だけで、`WORK_ROOT/reship_in_progress` が残っているあいだは
`ship_all.sh` の通常の飛ばしで続きから進む。

途中で止まったら、同じコマンドをもう一度実行する。

実行中は macOS のスリープを抑止する。
`caffeinate` が無い環境では警告を出して抑止せずに続ける。

1 周目で一部の都市が失敗したときは、もう 1 度だけ実行して失敗した都市を処理する。
それでも失敗した都市は一覧に出す。

終わったら、サーバで取り込みを起動する手順を表示する。
サーバの済みの記録 (`done.txt`) を改名する必要があるので、その手順も含める。
場所は `ship.env` の `REIMPORT_DONE_PATH` から読み、未設定なら伏せた案内だけを出す。

画面と同じ内容を `WORK_ROOT/reship_<日時>.log` に残す。

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

`building` と `building:part` の両方を見る。融合は取り込まれる側の way の
キーを `building:part` へ降格させるため、無壁舎の過半は降格側に載る
(豊中市の 111MB のメッシュでは 405 件のうち 252 件が降格側だった)。

## 終了コード

`ship_all.sh` はどの都市が失敗したかを最後にまとめて出すが、個別の理由は終了コードで区別する。
2 本で終了コードの意味が違うので、スクリプトごとに分けて示す。

### `ship_city.sh`

| コード | 意味 |
|---|---|
| 0 | 成功 |
| 1 | `ship.env` が無い、必須の項目が未設定、`CONVERSION_JSON` / `CITYGML_OSM_JAR` の実体が無い、`DISK_MIN_KB` / `KEEP_RETAINED_DIRS` が数字でない、または citycode が 5 桁の数字でない |
| 2 | ディスク不足、または空き容量を読めない |
| 3 | 取り出し前の準備の失敗 (`WORK_ROOT` を作れない、前回の作業ディレクトリを退避または再作成できない) |
| 10 | 取り出し (`extract_city.py`) の失敗。`.gml` が 1 つも無い場合を含む |
| 11 | 変換 (`java`) の失敗、`conversion.json` を複製できない、`.osm` の枚数不一致、空ファイル、閉じタグの欠落 |
| 12 | 転送先を作れない、転送 (`rsync` / `ssh`) の失敗、または転送先の枚数を数えられないか一致しない |
| 13 | 無壁舎の書き換え (`apply_walless_roof.py`) の失敗。対の `.gml` が無い、`.osm` を XML として読めない場合を含む |

### `ship_all.sh`

| コード | 意味 |
|---|---|
| 0 | 全都市成功 |
| 1 | `ship.env` が無い、必須の項目が未設定、または失敗した都市が 1 つ以上ある (最終結果) |
| 2 | ディスク不足、または空き容量を読めない |
| 3 | 設定検査の失敗 (`DISK_MIN_KB` / `EXPECTED_CITIES` / `KEEP_RETAINED_DIRS` が数字でない)、`SHIPPED_TXT` に書けない、`WORK_ROOT` を作れない、計画ファイル (`PLAN_CSV`) が無い、計画の件数が `EXPECTED_CITIES` と合わない、書き出した一覧が 0 都市 |
| 4 | 一覧 (`reimport_targets_<日時>.txt`) のサーバへの転送に失敗 |

`ship_city.sh` の exit 2 (ディスク不足) は `ship_all.sh` にそのまま伝わり、全体を止める。

## ファイル

| ファイル | 用途 |
|---|---|
| `httpzip.py` | HTTP Range で読む file-like。`zipfile.ZipFile` に渡す |
| `build_download_plan.py` | CKAN から都市ごとの CityGML zip の URL を集めて CSV に書く |
| `scan_bldg.py` | 各 zip の中央ディレクトリを読み、建物データの量を数える |
| `extract_city.py` | `udx/bldg/*.gml` だけを Range で取り出す |
| `apply_walless_roof.py` | 無壁舎 (`bldg:class` 3003 / 3004) の建物を `building=roof` にする |
| `ckan_download_plan.csv` | 148 都市の URL 一覧。`build_download_plan.py` が作る |

CSV は 148 都市 (宇城市 43213 を含む) を全部カバーしている。
都市を新しく足す、または URL を取り直すときは上の手順 1 を使う。

## CKAN の読み方で引っかかるところ

package 名は `plateau-<citycode>-<romaji>-<種別>-<年度>` で、1 都市に複数年度ある。

resource は name が `CityGML（vN）` に**完全一致**するものだけを見る。
部分一致にすると `【uc25-…】…のCityGMLデータ` のような、ユースケース実証用の別データを拾う。
同じ package に複数の版が並ぶので、`vN` が最大のものを採る。

zip の中のパスは `udx/bldg/...` で始まる。**先頭に `/` は付かない。**

## 実測 (2026-08-12)

147 都市すべてに CKAN の package があり、選んだ版は DB の `spec_versions` とほぼ一致した
（v4 が 103、v5 が 43、v3 が 1）。

年度が上がるぶんメッシュは増える。43 都市で増え、全体で 17,726 から 18,352 になる。
福島市は 59 から 212、安曇野市は 94 から 212、すさみ町は 19 から 103 に増える。
**再取り込みは作り直しだけでなく範囲の拡張でもある**ので、建物数が大きく増えても異常ではない。

すさみ町 30406 で全段を通したときの所要は、取得 2.5 秒、変換 45 秒、転送 12MB、取り込み 4.9 分だった。

## 注意

**変換器の jar は clean な checkout から作る。**
作業ツリーが実験用のブランチに置かれていることがあり、そこで作った jar は master のものと中身が違う。

**`plateau_purge.py --execute` は本番実行の前に dry run のサマリを出す。**
出力を `tail` で切ると実行していないように見えるが、そのときには既に削除されている。
実行したかどうかは出力ではなく DB の行数で確かめる。

**元データの識別子の数と DB の行数は一致しない。**
穴を潰した way と穴のある multipolygon が同じ建物の二重出力になっていることがあり、
取り込みはこれを 1 行にまとめる。識別子 2 つが 1 行になるぶん、源データより少なくなる。

**対応エリアの再計算は全都市が終わってから 1 回だけ。**
都市ごとに走らせない。
