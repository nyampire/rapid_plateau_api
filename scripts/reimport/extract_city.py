#!/usr/bin/env python3
"""CityGML zip から `udx/bldg/*.gml` だけを HTTP Range で取り出す。

    python3 extract_city.py <citycode> <出力ディレクトリ>

zip 全体は落とさない。147 都市の zip は合計 1,356 GB あるが、建物データは
その 0.09〜18% で合計 8.84 GB しかない (2026-08-12 実測)。配信元は
`Accept-Ranges: bytes` を返すので、末尾の中央ディレクトリだけ読んで
必要なメンバを Range で取り出せる。

実測: すさみ町 103 メッシュ = 通信 5.3MB・2.5 秒。

取り出した .gml は citygml-osm にそのまま渡せる。手順は
`docs/superpowers/` の再取り込みの記述を参照。
"""
import csv
import io
import json
import os
import sys
import time
import zipfile

from httpzip import HttpFile

HERE = os.path.dirname(os.path.abspath(__file__))
PLAN = os.path.join(HERE, 'ckan_download_plan.csv')
MEMBER_PREFIX = 'udx/bldg/'          # zip の直下が udx/。先頭に / は付かない


def extract(city_code, dest):
    with open(PLAN) as f:
        rows = [r for r in csv.DictReader(f) if r['city_code'] == city_code]
    if not rows:
        raise SystemExit('%s は %s に無い' % (city_code, PLAN))
    row = rows[0]

    os.makedirs(dest, exist_ok=True)
    started = time.time()
    remote = HttpFile(row['url'])
    zf = zipfile.ZipFile(io.BufferedReader(remote, buffer_size=1 << 20))
    members = [i for i in zf.infolist()
               if i.filename.startswith(MEMBER_PREFIX) and i.filename.endswith('.gml')]

    # 出力先を basename に潰すので、zip 内のパスが違っても衝突しうる。
    # 黙って上書きするとファイル数だけが減り、原因は取り出しの外から判らない。
    seen = {}
    for info in members:
        base = os.path.basename(info.filename)
        if base in seen:
            raise SystemExit('basename が衝突している: %s (%s と %s)'
                             % (base, seen[base], info.filename))
        seen[base] = info.filename

    written = 0
    for info in members:
        out = os.path.join(dest, os.path.basename(info.filename))
        # 書き終えてから名前を付ける。切り詰められた `.gml` が残ると、
        # 枚数の照合を通り抜けて建物が欠けたまま取り込まれる。
        part = out + '.part'
        with zf.open(info) as src, open(part, 'wb') as dst:
            while True:
                chunk = src.read(1 << 22)
                if not chunk:
                    break
                dst.write(chunk)
        size = os.path.getsize(part)
        if size != info.file_size:
            os.unlink(part)
            raise SystemExit('%s の大きさが zip の申告と違う (%d != %d)'
                             % (info.filename, size, info.file_size))
        os.replace(part, out)
        written += size

    return {
        'city_code': city_code,
        'package': row['package'],
        'citygml_v': row['citygml_v'],
        'meshes': len(members),
        'raw_bytes': written,
        'fetched_bytes': remote.bytes_fetched,
        'zip_bytes': remote.size,
        'seconds': round(time.time() - started, 1),
    }


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit(__doc__)
    print(json.dumps(extract(sys.argv[1], sys.argv[2]), ensure_ascii=False))
