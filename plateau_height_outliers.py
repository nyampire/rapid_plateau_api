#!/usr/bin/env python3
"""PLATEAU 建物の高さのはずれ値を数える調査スクリプト。

データベースは読むだけです。
列の追加もビューの更新も行いません。

## 検査と既定の閾値

各検査は「該当する行を返す SELECT」を組み立てます。
件数、都市ごとの集計、代表例は、その結果から導きます。

### degenerate-area

平面での面積が正でない多角形を探します。
PostGIS は外環の面積から内環の面積を引いて面積を出します。
輪郭が自己交差すると外環の面積が打ち消し合って小さくなり、0 か、内環を引いて負になります。
建物として成り立ちません。

球面の面積計算はこの多角形で内部エラーになるため、needle は対象から外します。
外したものがどこにも出ないと見落とすので、この検査で報告します。

### part-over-outline

部分立体が親の外形より、既定で 10 m 以上高いものを探します。

部分立体が外形より高いこと自体は誤りではありません。
屋上の塔屋や階段室は、外形の高さを主屋根までで取っていればそれより高くなります。
実データでも、超過 0.5 m 超の 180 件のうち中央値は 3.85 m でした。

超過の帯ごとに、外形に対する部分立体の面積比を見ると性質が分かれます。

| 超過 | 件数 | 面積比の中央値 |
|---|---:|---:|
| 2 m 以下 | 45 | 0.004 |
| 2 〜 5 m | 66 | 0.145 |
| 5 〜 10 m | 32 | 0.065 |
| 10 m 超 | 37 | 4.481 |

10 m 以下の帯は面積比の中央値が 0.004 から 0.145 で、屋上の構造物の平面にあたります。
10 m 超の帯だけ中央値が 4.481 で、建物の一部が全体より広い状態です。

既定値 10 m は、この中央値が分かれる位置にあります。
ただし、2 つの集団は重なっています。
10 m 以下の 143 件のうち 21 件は面積比が 0.7 以上で、既定値では報告されません。
10 m 超の 37 件のうち 12 件は面積比が 0.7 未満です。
代表例には面積比を添えるので、判断はその列で行えます。

### absolute

高さ自体が範囲の外にあるものを探します。
既定の範囲は 1.0 m から 200 m です。
floor-height が使えない、階数の無い建物を対象に含めるために置いています。

### needle

高さの割に底面積が小さいものを探します。
既定は高さ 15 m 超かつ底面積 20 m² 未満です。

### floor-height

1 階あたりの高さが範囲の外にあるものを探します。
既定の範囲は 1.5 m から 10.2 m です。
階数が入っているのは全体の約 51 パーセントで、残りはこの検査の対象になりません。
体育館、工場、倉庫、寺社では 1 階あたりが大きい値を正常に取るため、
この検査は建物の種別を区別できないまま数えます。
結果には building の値ごとの件数を添えるので、種別ごとの偏りはそこで見ます。

## 閾値を決めた根拠（2026-09-28 時点の実測）

対象は 27,873,959 件で、高さは 27,872,247 件、階数は 14,220,532 件に入っています。
部分立体は 887,286 件です。

高さの分布は、中央値 7.00 m、99 パーセント点 22.20 m、最大 276.50 m でした。
1 階あたりの高さは、中央値 3.75 m、四分位が 3.30 から 4.30、
99 パーセント点 10.20 m、99.9 パーセント点 20.76 m でした。

既定の閾値で全件を実行した件数は次のとおりです。

| 検査 | 件数 |
|---|---:|
| degenerate-area | 7 |
| part-over-outline | 37 |
| absolute | 3,460 |
| needle | 29,587 |
| floor-height | 262,009 |

part-over-outline の 37 件は、超過 10 m 超のものです。
許容差を 0.5 m まで下げると 180 件になりますが、143 件は屋上の構造物です。

自己交差する多角形は約 2,500 件あります。
そのうち符号付き面積まで 0 以下になったものが 7 件です。
ST_MakeValid を通すと MultiPolygon として修復できますが、
このスクリプトは読むだけなので修復はしません。
"""

import argparse
import csv
import os
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

DEFAULT_SAMPLES = 10
DEFAULT_PART_TOLERANCE_M = 10.0
DEFAULT_NEEDLE_HEIGHT_M = 15.0
DEFAULT_NEEDLE_AREA_M2 = 20.0
DEFAULT_FLOOR_HEIGHT_MIN_M = 1.5
DEFAULT_FLOOR_HEIGHT_MAX_M = 10.2
DEFAULT_ABSOLUTE_MIN_M = 1.0
DEFAULT_ABSOLUTE_MAX_M = 200.0

# 検査の名前から、それを実行するメソッドの名前への対応。
# 並びは確からしさの高いものからにしてあり、報告の読み順になる。
CHECKS = {
    'degenerate-area': '_degenerate_area',
    'part-over-outline': '_part_over_outline',
    'absolute': '_absolute',
    'needle': '_needle',
    'floor-height': '_floor_height',
}


def resolve_checks(names: Optional[List[str]]) -> List[str]:
    """--check の指定を、実行する検査の並びに直します。

    省略したときは登録してあるすべてを返します。
    """
    if not names:
        return list(CHECKS)
    unknown = [n for n in names if n not in CHECKS]
    if unknown:
        known = ', '.join(CHECKS)
        raise ValueError(
            f"不明な検査です: {', '.join(unknown)} (使えるのは {known})"
        )
    return [n for n in CHECKS if n in names]


@dataclass
class MatchQuery:
    """該当する行を返す SELECT と、代表例を並べる順序。

    sql は id、city_code、building を必ず含めます。
    order_by は sql が返す列の名前で書きます。
    """

    sql: str
    params: Tuple
    order_by: str


@dataclass
class CheckResult:
    """1 つの検査の結果。

    total は該当した件数です。
    by_city は都市ごとの件数を多い順に並べたものです。
    by_building は building の値ごとの件数を、同じく多い順に並べたものです。
    samples は確認用に取り出した代表例です。
    """

    name: str
    total: int
    by_city: List[Tuple[str, int]] = field(default_factory=list)
    by_building: List[Tuple[Optional[str], int]] = field(
        default_factory=list
    )
    samples: List[Dict[str, Any]] = field(default_factory=list)


class HeightOutlierSurvey:
    """高さのはずれ値を数えます。"""

    def __init__(self, postgres_url: Optional[str] = None):
        if postgres_url is None:
            postgres_url = os.getenv('DATABASE_URL')
        if not postgres_url:
            raise ValueError(
                '接続先が指定されていません。'
                '--postgres-url か環境変数 DATABASE_URL を設定してください。'
            )
        self.postgres_url = postgres_url

    def run_check(self, name: str,
                  samples: int = DEFAULT_SAMPLES,
                  city: Optional[str] = None) -> CheckResult:
        """検査を 1 つ実行します。

        city を渡すと、その市町村コードの建物だけを数えます。
        """
        if name not in CHECKS:
            known = ', '.join(CHECKS)
            raise ValueError(f'不明な検査です: {name} (使えるのは {known})')
        match = getattr(self, CHECKS[name])()
        return self._run(name, match, samples, city)

    def _run(self, name: str, match: MatchQuery, samples: int,
             city: Optional[str] = None) -> CheckResult:
        """1 つの検査から、件数、都市ごとの集計、代表例を取ります。"""
        # 絞り込みは、検査ごとの SQL を副問い合わせにして外側で条件を足します。
        # すべての検査に同じ書き方で適用できます。
        inner = f'({match.sql}) AS m'
        params = match.params
        if city is not None:
            inner = f'(SELECT * FROM {inner} WHERE city_code = %s) AS mc'
            params = params + (city,)
        # 読み取り専用のトランザクションにして、書き込みをデータベースの側で拒ませます。
        # 検査を足したときの誤りも、ここで止まります。
        conn = psycopg2.connect(self.postgres_url)
        try:
            conn.set_session(readonly=True)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f'SELECT city_code, COUNT(*) AS n FROM {inner} '
                    'GROUP BY city_code ORDER BY n DESC, city_code',
                    params,
                )
                by_city = [(r['city_code'], r['n']) for r in cur.fetchall()]
                # 件数は都市ごとの件数の合計から出します。
                # 全件の走査を 1 回減らせます。
                total = sum(n for _, n in by_city)

                cur.execute(
                    f'SELECT building, COUNT(*) AS n FROM {inner} '
                    'GROUP BY building ORDER BY n DESC, building',
                    params,
                )
                by_building = [
                    (r['building'], r['n']) for r in cur.fetchall()
                ]

                cur.execute(
                    f'SELECT * FROM {inner} ORDER BY {match.order_by} '
                    'LIMIT %s',
                    params + (samples,),
                )
                rows = [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()
        return CheckResult(
            name=name, total=total, by_city=by_city,
            by_building=by_building, samples=rows,
        )

    def _degenerate_area(self) -> MatchQuery:
        """平面での面積が正でない多角形を探します。

        輪郭が自己交差して外環の面積が打ち消し合い、0 か、内環を引いて負になった形です。
        建物として成り立たないので、それ自体がデータの誤りです。
        """
        return MatchQuery(
            sql="""
                SELECT id, city_code, building, height,
                       ST_Area(geom) AS planar_area,
                       ST_NPoints(geom) AS n_points
                FROM plateau_buildings
                WHERE geom IS NOT NULL
                  AND ST_Area(geom) <= 0
            """,
            params=(),
            order_by='planar_area',
        )

    def _part_over_outline(
        self, tolerance: float = DEFAULT_PART_TOLERANCE_M
    ) -> MatchQuery:
        # area_ratio は外形に対する部分立体の面積の比です。
        # 屋上の構造物は平面が小さく 0.1 前後になります。
        # 外形より広い部分立体は構造が壊れており 1 を超えます。
        # 比は平面のまま取ります。外形と部分立体は同じ緯度にあるためです。
        return MatchQuery(
            sql="""
                SELECT c.id, c.city_code, c.building,
                       c.height AS part_height,
                       o.height AS outline_height,
                       ST_Area(c.geom) / NULLIF(ST_Area(o.geom), 0)
                           AS area_ratio
                FROM plateau_buildings c
                JOIN plateau_buildings o ON o.id = c.parent_building_id
                WHERE c.height IS NOT NULL
                  AND o.height IS NOT NULL
                  AND c.height > o.height + %s
            """,
            params=(tolerance,),
            order_by='part_height - outline_height DESC',
        )

    def _needle(
        self, min_height: float = DEFAULT_NEEDLE_HEIGHT_M,
        max_area: float = DEFAULT_NEEDLE_AREA_M2
    ) -> MatchQuery:
        # 面積は geography に変換して平方メートルで測る。
        # 緯度によって経度 1 度の長さが変わるため。
        #
        # 平面での面積が正でない多角形は、球面の計算が内部エラーになる。
        # 該当するものは degenerate-area で別に報告するので、ここでは外す。
        # CASE を使うのは、WHERE の条件の評価順が保証されないため。
        # CASE は選ばれた枝だけを評価することが保証されている。
        return MatchQuery(
            sql="""
                SELECT id, city_code, building, height,
                       ST_Area(geom::geography) AS area_m2
                FROM plateau_buildings
                WHERE height IS NOT NULL
                  AND geom IS NOT NULL
                  AND height > %s
                  AND CASE WHEN ST_Area(geom) > 0
                           THEN ST_Area(geom::geography) < %s
                           ELSE false
                      END
            """,
            params=(min_height, max_area),
            order_by='height / NULLIF(area_m2, 0) DESC',
        )

    def _floor_height(
        self, min_m: float = DEFAULT_FLOOR_HEIGHT_MIN_M,
        max_m: float = DEFAULT_FLOOR_HEIGHT_MAX_M
    ) -> MatchQuery:
        # outside_factor は「範囲の外に何倍出ているか」です。
        # 低い側は 0 で頭打ちになり、差で比べると高い側だけが代表例を占めます。
        # 倍率で比べると、両端が同じ尺度に乗ります。
        return MatchQuery(
            sql="""
                SELECT id, city_code, building, height, building_levels,
                       height / building_levels AS floor_height,
                       GREATEST(
                           %s / NULLIF(height / building_levels, 0),
                           (height / building_levels) / %s
                       ) AS outside_factor
                FROM plateau_buildings
                WHERE height IS NOT NULL
                  AND building_levels > 0
                  AND (height / building_levels < %s
                       OR height / building_levels > %s)
            """,
            params=(min_m, max_m, min_m, max_m),
            order_by='outside_factor DESC',
        )

    def _absolute(
        self, min_m: float = DEFAULT_ABSOLUTE_MIN_M,
        max_m: float = DEFAULT_ABSOLUTE_MAX_M
    ) -> MatchQuery:
        # 上限の 200 m は実データの最大 276.5 m より低く取っている。
        # 超高層建築は実在するので、ここに出たものが誤りとは限らない。
        # outside_factor の考え方は floor-height と同じです。
        return MatchQuery(
            sql="""
                SELECT id, city_code, building, height,
                       GREATEST(%s / NULLIF(height, 0), height / %s)
                           AS outside_factor
                FROM plateau_buildings
                WHERE height IS NOT NULL
                  AND (height < %s OR height > %s)
            """,
            params=(min_m, max_m, min_m, max_m),
            order_by='outside_factor DESC',
        )


def format_result(result: CheckResult, top_cities: int = 5,
                  top_buildings: int = 10) -> str:
    """1 つの検査の結果を、読める形の文字列にします。"""
    lines = [f'## {result.name}: {result.total} 件']
    if result.total == 0:
        return '\n'.join(lines)

    lines.append('')
    lines.append('件数の多い都市:')
    for city, n in result.by_city[:top_cities]:
        lines.append(f'  {city}  {n} 件')

    lines.append('')
    lines.append('件数の多い building の値:')
    for building, n in result.by_building[:top_buildings]:
        lines.append(f'  {building}  {n} 件')

    lines.append('')
    lines.append('代表例:')
    for row in result.samples:
        parts = [f"{k}={v}" for k, v in row.items() if k != 'id']
        lines.append(f"  id={row['id']}  " + '  '.join(parts))
    return '\n'.join(lines)


def write_csv(path: str, results: List[CheckResult]) -> None:
    """代表例を CSV に書き出します。

    列は検査ごとに違うので、check と id 以外は文字列にして 1 列にまとめます。
    """
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['check', 'id', 'city_code', 'detail'])
        for result in results:
            for row in result.samples:
                detail = ' '.join(
                    f'{k}={v}' for k, v in row.items()
                    if k not in ('id', 'city_code')
                )
                writer.writerow(
                    [result.name, row['id'], row['city_code'], detail]
                )


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description='PLATEAU 建物の高さのはずれ値を数えます'
    )
    parser.add_argument(
        '--postgres-url',
        help='PostgreSQL接続URL (デフォルト: 環境変数 DATABASE_URL)',
    )
    parser.add_argument(
        '--check',
        action='append',
        choices=list(CHECKS),
        help='実行する検査 (繰り返し指定可、デフォルト: すべて)',
    )
    parser.add_argument(
        '--samples',
        type=int,
        default=DEFAULT_SAMPLES,
        help=f'代表例の件数 (デフォルト: {DEFAULT_SAMPLES})',
    )
    parser.add_argument(
        '--city',
        help='市町村コードで絞る (試すとき用)',
    )
    parser.add_argument(
        '--csv',
        help='代表例を CSV に書き出す先',
    )
    args = parser.parse_args(argv)

    try:
        names = resolve_checks(args.check)
        survey = HeightOutlierSurvey(postgres_url=args.postgres_url)
    except ValueError as exc:
        print(f'エラー: {exc}', file=sys.stderr)
        return 1

    results = []
    for name in names:
        result = survey.run_check(
            name, samples=args.samples, city=args.city
        )
        results.append(result)
        print(format_result(result))
        print()

    if args.csv:
        write_csv(args.csv, results)
        print(f'CSV を書き出しました: {args.csv}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
