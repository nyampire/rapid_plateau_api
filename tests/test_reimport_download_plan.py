"""build_download_plan.py が既存の取得計画を壊さないことを固定する。

`ckan_download_plan.csv` は 147 都市ぶんをコミットしてある成果物で、
再取り込みの対象一覧を兼ねる。都市を 1 つ足すつもりで実行したときに
残り 146 都市が消えると、消えたことに気づかないまま先へ進む。

CKAN には問い合わせない。`_get` と `content_length` を差し替える。
"""

import csv

import pytest

import build_download_plan as bdp


# 都市コード → その都市の package 名と resources
CATALOG = {
    'plateau-30406-susami-cho-2025': [
        {'name': 'CityGML（v5）', 'url': 'https://example.invalid/30406_2025.zip'},
    ],
    'plateau-43213-uki-shi-2024': [
        {'name': 'CityGML（v4）', 'url': 'https://example.invalid/43213_2024.zip'},
    ],
    # 最新年度に CityGML が無く、前年度にはある都市
    'plateau-07201-fukushima-shi-2025': [
        {'name': '3D Tiles', 'url': 'https://example.invalid/07201_3dtiles.zip'},
    ],
    'plateau-07201-fukushima-shi-2024': [
        {'name': 'CityGML（v4）', 'url': 'https://example.invalid/07201_2024.zip'},
    ],
}


@pytest.fixture
def plan(tmp_path, monkeypatch):
    """空の計画ファイルを指す。CKAN は差し替える。"""
    path = tmp_path / 'ckan_download_plan.csv'
    monkeypatch.setattr(bdp, 'PLAN', str(path))

    def fake_get(action, **params):
        if action == 'package_search':
            names = sorted(CATALOG)
            return {'results': [{'name': n} for n in names], 'count': len(names)}
        if action == 'package_show':
            return {'resources': CATALOG.get(params['id'], [])}
        raise AssertionError('想定外の action: %s' % action)

    monkeypatch.setattr(bdp, '_get', fake_get)
    monkeypatch.setattr(bdp, 'content_length', lambda url: 12345)
    monkeypatch.setattr(bdp.time, 'sleep', lambda s: None)
    return path


def _rows(path):
    with open(path) as f:
        return {r['city_code']: r for r in csv.DictReader(f)}


def _seed(path, city_codes):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['city_code', 'package', 'year', 'citygml_v', 'bytes', 'url'])
        for code in city_codes:
            w.writerow([code, 'old-package-%s' % code, '2020', '3', '1',
                        'https://example.invalid/old_%s.zip' % code])


def test_adding_a_city_keeps_the_existing_ones(plan):
    """都市を 1 つ渡しても、既にある都市が消えない。

    README と設計文書はこの呼び方を「追加」として書いている。
    置換だと 147 都市の計画が 1 行に潰れる。
    """
    _seed(plan, ['30406', '43213'])

    bdp.main(['07201'])

    rows = _rows(plan)
    assert set(rows) == {'30406', '43213', '07201'}


def test_named_city_is_refreshed(plan):
    """渡した都市の行は、CKAN から取り直した内容に入れ替わる。"""
    _seed(plan, ['30406'])

    bdp.main(['30406'])

    row = _rows(plan)['30406']
    assert row['package'] == 'plateau-30406-susami-cho-2025'
    assert row['year'] == '2025'
    assert row['url'] == 'https://example.invalid/30406_2025.zip'


def test_city_that_cannot_be_resolved_keeps_its_old_row(plan):
    """CKAN に無い都市を渡しても、その都市の既存の行を消さない。

    一時的な不調で落ちた都市が計画から消えると、引数なしのモードは
    計画自身を読むので二度と戻らない。
    """
    _seed(plan, ['30406', '99999'])

    bdp.main(['99999'])

    rows = _rows(plan)
    assert '99999' in rows
    assert rows['99999']['url'] == 'https://example.invalid/old_99999.zip'


def test_falls_back_to_an_older_year_that_has_citygml(plan):
    """最新年度に CityGML が無ければ、ある年度まで遡る。

    3D Tiles だけの年度が 1 つあるだけで都市ごと落ちると、
    148 都市を覆うという前提が黙って崩れる。
    """
    bdp.main(['07201'])

    row = _rows(plan)['07201']
    assert row['package'] == 'plateau-07201-fukushima-shi-2024'
    assert row['url'] == 'https://example.invalid/07201_2024.zip'


def test_refresh_mode_covers_every_city_in_the_plan(plan):
    """引数なしのモードは、計画にある都市をすべて残す。"""
    _seed(plan, ['30406', '43213'])

    bdp.main([])

    assert set(_rows(plan)) == {'30406', '43213'}


def test_existing_plan_survives_a_failure_midway(plan, monkeypatch):
    """書き出しの途中で落ちても、既存の計画が壊れない。"""
    _seed(plan, ['30406', '43213'])
    before = plan.read_text()

    def explode(url):
        raise OSError('CKAN が落ちている')

    monkeypatch.setattr(bdp, 'content_length', explode)

    with pytest.raises(OSError):
        bdp.main(['30406'])

    assert plan.read_text() == before
