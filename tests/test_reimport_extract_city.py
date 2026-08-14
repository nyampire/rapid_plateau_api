"""extract_city.py が壊れた取り出し結果を残さないことを固定する。

取り出しの関門は `.gml` の数と報告値の一致だけを見る。
数が合ってしまう壊れ方は、変換も転送も通り抜けて取り込みまで届く。

通信はしない。ローカルの zip を読ませる。
"""

import io
import os
import zipfile

import pytest

import extract_city


class _LocalRaw(io.RawIOBase):
    """HttpFile と同じ形でローカルの bytes を読ませる。"""

    def __init__(self, data):
        self._buf = io.BytesIO(data)
        self.size = len(data)
        self.bytes_fetched = 0

    def seekable(self): return True
    def readable(self): return True
    def seek(self, off, whence=0): return self._buf.seek(off, whence)
    def tell(self): return self._buf.tell()

    def readinto(self, b):
        n = self._buf.readinto(b)
        self.bytes_fetched += n or 0
        return n


def _zip_bytes(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


@pytest.fixture
def city(tmp_path, monkeypatch):
    """都市 30406 の計画を 1 行だけ用意し、zip の中身を差し替えられるようにする。"""
    plan = tmp_path / 'plan.csv'
    plan.write_text(
        'city_code,package,year,citygml_v,bytes,url\n'
        '30406,plateau-30406-susami-cho-2025,2025,5,1,https://example.invalid/x.zip\n')
    monkeypatch.setattr(extract_city, 'PLAN', str(plan))

    def use(members):
        data = _zip_bytes(members)
        monkeypatch.setattr(extract_city, 'HttpFile', lambda url: _LocalRaw(data))
    return use


def test_extracts_the_building_gml(city, tmp_path):
    """`udx/bldg/*.gml` だけを取り出し、メッシュ数を報告する。"""
    city({
        'udx/bldg/53394500_bldg_6697_op.gml': b'<CityModel>a</CityModel>',
        'udx/bldg/53394501_bldg_6697_op.gml': b'<CityModel>bb</CityModel>',
        'udx/tran/53394500_tran_6697_op.gml': b'<CityModel>road</CityModel>',
    })
    dest = tmp_path / 'out'

    got = extract_city.extract('30406', str(dest))

    assert got['meshes'] == 2
    assert sorted(os.listdir(dest)) == [
        '53394500_bldg_6697_op.gml', '53394501_bldg_6697_op.gml']
    assert (dest / '53394500_bldg_6697_op.gml').read_bytes() == b'<CityModel>a</CityModel>'


def test_rejects_duplicate_basenames(city, tmp_path):
    """basename が衝突するメンバがあれば落ちる。

    出力先を basename に潰すので、後のメンバが前のメンバを黙って上書きする。
    報告値は 2 なのにファイルは 1 個になり、原因は取り出しの外からは判らない。
    """
    city({
        'udx/bldg/a/53394500_bldg_6697_op.gml': b'<CityModel>a</CityModel>',
        'udx/bldg/b/53394500_bldg_6697_op.gml': b'<CityModel>b</CityModel>',
    })
    dest = tmp_path / 'out'

    with pytest.raises(SystemExit) as excinfo:
        extract_city.extract('30406', str(dest))
    assert '53394500_bldg_6697_op.gml' in str(excinfo.value)


def test_leaves_no_partial_gml_when_a_member_fails(city, tmp_path, monkeypatch):
    """途中で落ちたとき、切り詰められた `.gml` を残さない。

    残すと数が合ってしまい、変換も転送も通って建物が欠けたまま取り込まれる。
    """
    city({
        'udx/bldg/53394500_bldg_6697_op.gml': b'<CityModel>' + b'x' * 5000,
        'udx/bldg/53394501_bldg_6697_op.gml': b'<CityModel>' + b'y' * 5000,
    })
    dest = tmp_path / 'out'

    real_open = extract_city.zipfile.ZipFile.open
    calls = []

    class _DiesMidStream:
        """先頭だけ返してから落ちる。書きかけのファイルが残る状況を作る。"""

        def __init__(self, src):
            self._src = src
            self._reads = 0

        def read(self, n=-1):
            self._reads += 1
            if self._reads == 1:
                return self._src.read(64)
            raise OSError('接続が切れた')

        def __enter__(self): return self
        def __exit__(self, *exc): return False

    def flaky(self, member, *a, **kw):
        calls.append(member)
        src = real_open(self, member, *a, **kw)
        return _DiesMidStream(src) if len(calls) == 2 else src

    monkeypatch.setattr(extract_city.zipfile.ZipFile, 'open', flaky)

    with pytest.raises(OSError):
        extract_city.extract('30406', str(dest))

    leftover = [p for p in os.listdir(dest)] if dest.exists() else []
    assert [p for p in leftover if p.endswith('.gml')] == [
        '53394500_bldg_6697_op.gml']
    assert (dest / '53394500_bldg_6697_op.gml').read_bytes().endswith(b'x' * 5000)


def test_written_size_matches_the_zip_entry(city, tmp_path):
    """書き出した大きさが zip の申告と一致する。"""
    body = b'<CityModel>' + b'z' * 10000 + b'</CityModel>'
    city({'udx/bldg/53394500_bldg_6697_op.gml': body})
    dest = tmp_path / 'out'

    got = extract_city.extract('30406', str(dest))

    assert got['raw_bytes'] == len(body)
    assert (dest / '53394500_bldg_6697_op.gml').stat().st_size == len(body)
