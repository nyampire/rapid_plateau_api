#!/usr/bin/env python3
"""G空間情報センターの CKAN から、PLATEAU 各都市の CityGML zip の URL を集める。

`ckan_download_plan.csv` を書き出す。`extract_city.py` はこの CSV を読む。

対象の都市コードは引数で渡す。渡さなければ、既存の CSV に載っている都市を
すべて対象にして取り直す。

    python3 build_download_plan.py                 # 既存の一覧を最新の URL で更新
    python3 build_download_plan.py 30406 43213     # この 2 都市を足す / 取り直す

**書き出しは追加である。**渡した都市の行だけが入れ替わり、載っていない都市は
そのまま残る。解決できなかった都市も既存の行を残す。消してしまうと、引数なしの
モードが CSV 自身を読むので二度と戻らない。

CKAN の package 名は `plateau-<citycode>-<romaji>-<種別>-<年度>` の形で、
1 都市に複数年度ある。新しい年度から順に見て、CityGML を持つ最初の年度を採る。
最新年度が 3D Tiles だけということがあるので、1 年度で諦めない。

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


def by_citycode(names):
    """citycode → (年度, package 名) の新しい順のリスト。

    最新年度に CityGML が無い package もあるので、1 件に絞らず順に試せる形で返す。
    3D Tiles だけを収めた年度が 1 つあるだけで都市ごと落ちるのを避ける。
    """
    found = {}
    for n in names:
        m = PACKAGE_NAME.match(n)
        if not m:
            continue
        code, year = m.group(1), int(m.group(3))
        found.setdefault(code, []).append((year, n))
    return {code: sorted(v, reverse=True) for code, v in found.items()}


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


FIELDS = ['city_code', 'package', 'year', 'citygml_v', 'bytes', 'url']


def load_plan(path):
    """既存の計画を citycode → 行 の dict で読む。無ければ空。"""
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return {row['city_code']: row for row in csv.DictReader(f)}


def write_plan(path, by_code):
    """計画を書き出す。書き終えてから差し替えるので、途中で落ちても既存が残る。"""
    tmp = path + '.tmp'
    with open(tmp, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(FIELDS)
        for code in sorted(by_code):
            row = by_code[code]
            w.writerow([row[k] for k in FIELDS])
    os.replace(tmp, path)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('citycodes', nargs='*', help='省略時は既存の CSV の都市を対象にする')
    ap.add_argument('--no-size', action='store_true', help='zip の大きさを問い合わせない')
    args = ap.parse_args(argv)

    # 既存の計画は残したまま、対象の都市の行だけ入れ替える。
    # 置き換えにすると、都市を 1 つ足すつもりの実行で残りが消える。
    plan = load_plan(PLAN)

    wanted = args.citycodes
    if not wanted:
        if not plan:
            ap.error('都市コードを渡すか、先に一度都市を指定して %s を作ること' % PLAN)
        wanted = list(plan)

    catalog = by_citycode(list_packages())
    wanted = sorted(set(wanted))
    updated, missing = 0, []
    for i, code in enumerate(wanted, 1):
        found = None
        for year, package in catalog.get(code, []):
            got = citygml_resource(package)
            if got:
                found = (year, package, got[0], got[1])
                break
            time.sleep(0.3)
        if not found:
            # 解決できなかった都市は既存の行を残す。消すと、引数なしのモードが
            # 計画自身を読むので二度と戻らない。
            missing.append(code)
            continue
        year, package, v, url = found
        size = 0 if args.no_size else content_length(url)
        plan[code] = dict(zip(FIELDS, [code, package, year, v, size, url]))
        updated += 1
        if i % 25 == 0:
            print('...%d/%d' % (i, len(wanted)), flush=True)
        time.sleep(0.3)

    write_plan(PLAN, plan)

    print('%d 都市を更新、計 %d 都市を %s に書き出した' % (updated, len(plan), PLAN))
    if missing:
        print('CityGML が見つからない都市 (既存の行はそのまま): %s'
              % ', '.join(missing), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
