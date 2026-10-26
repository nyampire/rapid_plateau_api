#!/usr/bin/env python3
"""1 都市の建物外形と building:part を CSV に書き出す。

`analyze.py` の入力を作る。ジオメトリは WKB の 16 進で持つ。

接続先は `--postgres-url` か環境変数 `DATABASE_URL` から取る。
どちらも渡さないと終了コード 2 で止まる。

    python3 export_city.py 39201 --postgres-url "$DATABASE_URL" -o plateau_39201.csv.gz
"""

import argparse
import gzip
import os
import sys


def build_copy_sql():
    """都市 1 つぶんを書き出す COPY 文を返す。都市コードは引数で渡す。

    API の入口フィルタと同じ条件を掛ける。
    行政界の外に centroid が落ちる建物は、隣接市の配布に含まれる重複なので除く。
    """
    return """
        COPY (
          SELECT b.id,
                 b.parent_building_id,
                 COALESCE(b.building_part, '') AS building_part,
                 b.height,
                 b.ele,
                 b.building_levels,
                 encode(ST_AsBinary(b.geom), 'hex') AS wkb
          FROM plateau_buildings b
          WHERE b.city_code = %s
            AND NOT EXISTS (
              SELECT 1 FROM dash_city_master m
              WHERE m.city_code = b.city_code
                AND m.boundary_geom IS NOT NULL
                AND NOT ST_Contains(m.boundary_geom, b.centroid))
        ) TO STDOUT WITH CSV HEADER
    """


def export(citycode, postgres_url, out_path):
    import psycopg2

    conn = psycopg2.connect(postgres_url)
    try:
        with conn.cursor() as cur, gzip.open(out_path, 'wt') as fh:
            cur.copy_expert(cur.mogrify(build_copy_sql(), (citycode,)).decode(), fh)
    finally:
        conn.close()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument('citycode', help='市区町村コード (例 39201)')
    p.add_argument('--postgres-url', default=None,
                   help='接続文字列。省略すると環境変数 DATABASE_URL を使う')
    p.add_argument('-o', '--output', default=None,
                   help='出力先。省略すると plateau_<citycode>.csv.gz')
    args = p.parse_args(argv)

    url = args.postgres_url or os.environ.get('DATABASE_URL')
    if not url:
        print('接続先がありません。--postgres-url か DATABASE_URL を渡してください。',
              file=sys.stderr)
        return 2

    out = args.output or f'plateau_{args.citycode}.csv.gz'
    export(args.citycode, url, out)
    print(f'{out} に書き出しました。')
    return 0


if __name__ == '__main__':
    sys.exit(main())
