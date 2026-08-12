# 再取り込みの下ごしらえ

CityGML を自前で変換して都市を取り込み直すときに使う道具。
上流の配信元が落ちているあいだの代替手段で、[#21](https://github.com/nyampire/rapid_plateau_api/issues/21) の Phase 1 を人手でなぞるためのものである。

## zip 全体は落とさない

147 都市の CityGML zip は合計 **1,356 GB** ある。1 都市で 269 GB のものもある。
必要なのは `udx/bldg/*.gml` だけで、そこは全国合わせて **8.84 GB**（展開 168 GB、18,352 メッシュ）しかない。

配信元は `Accept-Ranges: bytes` を返す。
zip 末尾の中央ディレクトリだけ読めば、必要なメンバの位置が判り、そこだけ Range で取り出せる。
1 都市あたりの通信は数 MB で済む。

Python の `zipfile` は seek できる file-like を渡せば ZIP64 も含めて解釈するので、
Range で読む薄い層を 1 つ用意すれば足りる（`httpzip.py`）。
`io.RawIOBase` を継承する場合は **`readinto` の実装が要る**。`read` だけでは `zipfile` が `NotImplementedError` を投げる。

## 使い方

```bash
# 1. 対象都市の CityGML の URL を集める (CKAN に問い合わせる)
python3 build_download_plan.py 30406

# 2. 取得量とメッシュ数を見積もる (中身は落とさない)
python3 scan_bldg.py

# 3. 建物データだけ取り出す
python3 extract_city.py 30406 ./work/30406
```

出力した `.gml` は citygml-osm にそのまま渡せる。
変換したあと `.osm` をサーバへ送り、purge してから `--no-zip` で取り込む。

## ファイル

| ファイル | 用途 |
|---|---|
| `httpzip.py` | HTTP Range で読む file-like。`zipfile.ZipFile` に渡す |
| `build_download_plan.py` | CKAN から都市ごとの CityGML zip の URL を集めて CSV に書く |
| `scan_bldg.py` | 各 zip の中央ディレクトリを読み、建物データの量を数える |
| `extract_city.py` | `udx/bldg/*.gml` だけを Range で取り出す |
| `ckan_download_plan.csv` | 147 都市の URL 一覧。`build_download_plan.py` が作る |

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
