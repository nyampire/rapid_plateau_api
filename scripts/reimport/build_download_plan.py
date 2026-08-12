#!/usr/bin/env python3
"""G空間情報センターの CKAN から、PLATEAU 各都市の CityGML zip の URL を集める。

`ckan_download_plan.csv` を書き出す。`extract_city.py` はこの CSV を読む。

対象の都市コードは引数で渡す。渡さなければ、同じディレクトリの
`ckan_download_plan.csv` に載っている都市を対象にして作り直す。

    python3 build_download_plan.py                 # 既存の一覧を最新の URL で更新
    python3 build_download_plan.py 30406 43213     # 都市を指定

CKAN の package 名は `plateau-<citycode>-<romaji>-<種別>-<年度>` の形で、
1 都市に複数年度ある。最新年度を採る。

resource は name が `CityGML（vN）` に**完全一致**するものだけを見る。
部分一致にすると `【uc25-…】…のCityGMLデータ` のような、ユースケース実証用の
別データを拾う。同じ package に複数の版があるので、vN が最大のものを採る。
"""
import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

CKAN = 'https://www.geospatial.jp/ckan/api/3/action'
HERE = os.path.dirname(os.path.abspath(__file__))
PLAN = os.path.join(HERE, 'ckan_download_plan.csv')
RESOURCE_NAME = re.compile(r'^CityGML（v(\d+)）$')
PACKAGE_NAME = re.compile(r'^plateau-(\d{5})-(.+)-(\d{4})$')


def _get(action, **params):
    url = CKAN + '/' + action + '?' + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=120) as r:
        return json.load(r)['result']


def list_packages():
    """PLATEAU の package 名をすべて集める。1 リクエストで足りる件数しかない。"""
    names, start = [], 0
    while True:
        d = _get('package_search', q='PLATEAU', rows=1000, start=start)
        got = [p['name'] for p in d['results']]
        names += got
        if not got or len(names) >= d['count']:
            break
        start = len(names)
        time.sleep(1)
    return sorted(n for n in set(names) if n.startswith('plateau-'))


def latest_by_citycode(names):
    """citycode → 最新年度の package 名。"""
    best = {}
    for n in names:
        m = PACKAGE_NAME.match(n)
        if not m:
            continue
        code, year = m.group(1), int(m.group(3))
        if code not in best or year > best[code][0]:
            best[code] = (year, n)
    return best


def citygml_resource(package):
    """package の resources から、版が最大の `CityGML（vN）` を返す。"""
    best = None
    for res in _get('package_show', id=package).get('resources', []):
        m = RESOURCE_NAME.match((res.get('name') or '').strip())
        if not m:
            continue
        v = int(m.group(1))
        if best is None or v > best[0]:
            best = (v, res.get('url'))
    return best


def content_length(url):
    req = urllib.request.Request(url, method='HEAD')
    with urllib.request.urlopen(req, timeout=90) as r:
        return int(r.headers.get('Content-Length') or 0)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('citycodes', nargs='*', help='省略時は既存の CSV の都市を対象にする')
    ap.add_argument('--no-size', action='store_true', help='zip の大きさを問い合わせない')
    args = ap.parse_args()

    wanted = args.citycodes
    if not wanted:
        if not os.path.exists(PLAN):
            ap.error('都市コードを渡すか、先に一度都市を指定して %s を作ること' % PLAN)
        with open(PLAN) as f:
            wanted = [row['city_code'] for row in csv.DictReader(f)]

    best = latest_by_citycode(list_packages())
    rows, missing = [], []
    for i, code in enumerate(sorted(set(wanted)), 1):
        if code not in best:
            missing.append(code)
            continue
        year, package = best[code]
        found = citygml_resource(package)
        if not found:
            missing.append(code)
            continue
        v, url = found
        size = 0 if args.no_size else content_length(url)
        rows.append([code, package, year, v, size, url])
        if i % 25 == 0:
            print('...%d/%d' % (i, len(set(wanted))), flush=True)
        time.sleep(0.3)

    with open(PLAN, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['city_code', 'package', 'year', 'citygml_v', 'bytes', 'url'])
        w.writerows(rows)

    print('%d 都市を %s に書き出した' % (len(rows), PLAN))
    if missing:
        print('CityGML が見つからない都市: %s' % ', '.join(missing), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
