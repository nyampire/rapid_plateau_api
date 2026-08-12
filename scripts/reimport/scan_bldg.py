#!/usr/bin/env python3
"""各都市の zip の中央ディレクトリだけ読んで、建物データの量を数える。

    python3 scan_bldg.py [出力先.json]

zip の中身は落とさない。1 都市あたり通信 ~1MB・2 秒で、147 都市が 45 秒で終わる
(2026-08-12 実測)。再取り込みの前に、取得量とメッシュ数を見積もるために使う。

出力の JSON には都市ごとのメッシュコード一覧も入るので、本番 DB の
`source_dataset` と突き合わせれば、年度が上がってメッシュがいくつ増えるかが判る。
"""
import csv
import io
import json
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor

from httpzip import HttpFile

HERE = os.path.dirname(os.path.abspath(__file__))
PLAN = os.path.join(HERE, 'ckan_download_plan.csv')
MEMBER_PREFIX = 'udx/bldg/'


def scan(row):
    try:
        remote = HttpFile(row['url'])
        zf = zipfile.ZipFile(io.BufferedReader(remote, buffer_size=1 << 20))
        bldg = [i for i in zf.infolist()
                if i.filename.startswith(MEMBER_PREFIX) and i.filename.endswith('.gml')]
        return {
            'city_code': row['city_code'],
            'citygml_v': row['citygml_v'],
            'zip_bytes': remote.size,
            'meshes': len(bldg),
            'bldg_compressed': sum(i.compress_size for i in bldg),
            'bldg_raw': sum(i.file_size for i in bldg),
            'mesh_codes': sorted(i.filename.split('/')[-1].split('_')[0] for i in bldg),
        }
    except Exception as e:
        return {'city_code': row['city_code'], 'error': repr(e)}


def main():
    out_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, 'bldg_scan.json')
    with open(PLAN) as f:
        rows = list(csv.DictReader(f))

    started = time.time()
    results = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        for i, r in enumerate(pool.map(scan, rows), 1):
            results.append(r)
            if i % 20 == 0:
                print('...%d/%d  %.0fs' % (i, len(rows), time.time() - started), flush=True)

    with open(out_path, 'w') as f:
        json.dump(results, f, ensure_ascii=False)

    ok = [r for r in results if 'error' not in r]
    print('%d 都市を走査、失敗 %d、%.0f 秒' % (len(ok), len(results) - len(ok), time.time() - started))
    print('建物データ 圧縮 %.2f GB / 展開 %.0f GB / メッシュ %d'
          % (sum(r['bldg_compressed'] for r in ok) / 1e9,
             sum(r['bldg_raw'] for r in ok) / 1e9,
             sum(r['meshes'] for r in ok)))
    for r in results:
        if 'error' in r:
            print('失敗 %s %s' % (r['city_code'], r['error']), file=sys.stderr)
    return 1 if len(ok) != len(results) else 0


if __name__ == '__main__':
    sys.exit(main())
