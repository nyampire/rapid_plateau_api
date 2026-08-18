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
変換したあと `.osm` をサーバへ送り、`--no-zip` を付けて取り込む。

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
各項目の意味は `ship.env.example` のコメントに書いてある。
`ship.env` 自体は `.gitignore` に入っているので、書き換えてもコミットされない。

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

## 終了コード

`ship_all.sh` はどの都市が失敗したかを最後にまとめて出すが、個別の理由は終了コードで区別する。
2 本で終了コードの意味が違うので、スクリプトごとに分けて示す。

### `ship_city.sh`

| コード | 意味 |
|---|---|
| 0 | 成功 |
| 1 | `ship.env` が無い、または必須の項目が未設定 |
| 2 | ディスク不足、または空き容量を読めない |
| 10 | 取り出し (`extract_city.py`) の失敗。`.gml` が 1 つも無い場合を含む |
| 11 | 変換 (`java`) の失敗、`.osm` の枚数不一致、空ファイル、閉じタグの欠落 |
| 12 | 転送 (`rsync` / `ssh`) の失敗、または転送先の枚数を数えられないか一致しない |

### `ship_all.sh`

| コード | 意味 |
|---|---|
| 0 | 全都市成功 |
| 1 | `ship.env` が無い、必須の項目が未設定、または失敗した都市が 1 つ以上ある (最終結果) |
| 2 | ディスク不足、または空き容量を読めない |
| 3 | 設定検査の失敗 (`DISK_MIN_KB` / `EXPECTED_CITIES` が数字でない)、`WORK_ROOT` を作れない、計画の件数が `EXPECTED_CITIES` と合わない |
| 4 | 一覧 (`reimport_targets_<日時>.txt`) のサーバへの転送に失敗 |

`ship_city.sh` の exit 2 (ディスク不足) は `ship_all.sh` にそのまま伝わり、全体を止める。

## ファイル

| ファイル | 用途 |
|---|---|
| `httpzip.py` | HTTP Range で読む file-like。`zipfile.ZipFile` に渡す |
| `build_download_plan.py` | CKAN から都市ごとの CityGML zip の URL を集めて CSV に書く |
| `scan_bldg.py` | 各 zip の中央ディレクトリを読み、建物データの量を数える |
| `extract_city.py` | `udx/bldg/*.gml` だけを Range で取り出す |
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
