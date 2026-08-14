"""HttpFile が Range 応答を検証することを固定する。

配信元が Range を無視して 200 と本文全体を返したとき、`HttpFile` は
要求した範囲ではなくファイル先頭を返す。長さも例外も呼び出し側から
見て正常なので、`zipfile` は壊れたデータを黙って読む。

通信はしない。`urlopen` を差し替えて確かめる。
"""

import io

import pytest

import httpzip

BODY = bytes(range(256)) * 4          # 1024 バイト。中身から位置が判る


class _Resp(io.BytesIO):
    def __init__(self, data, headers, status):
        super().__init__(data)
        self.headers = headers
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _stub(range_status=206, range_body=None, on_get=None):
    """HEAD には素直に答え、Range 要求には指定した応答を返す urlopen。"""
    def fake(req, timeout=None):
        if req.get_method() == 'HEAD':
            return _Resp(b'', {'Content-Length': str(len(BODY)),
                               'Accept-Ranges': 'bytes'}, 200)
        if on_get is not None:
            on_get()
        if range_body is not None:
            return _Resp(range_body, {}, range_status)
        start, end = req.headers['Range'].split('=')[1].split('-')
        chunk = BODY[int(start):int(end) + 1]
        return _Resp(chunk, {}, range_status)
    return fake


@pytest.fixture
def patched(monkeypatch):
    def apply(fake):
        monkeypatch.setattr(httpzip.urllib.request, 'urlopen', fake)
        return httpzip.HttpFile('http://example.invalid/x.zip')
    return apply


def test_range_response_returns_requested_bytes(patched):
    """まっとうな 206 応答では、要求した範囲がそのまま返る。"""
    f = patched(_stub())
    f.seek(1000)
    assert f.read(10) == BODY[1000:1010]
    assert f.tell() == 1010


def test_rejects_response_that_ignored_the_range(patched):
    """Range を無視して 200 で全文を返す応答は拒否する。

    受け入れると、要求した 1000 番地ではなくファイル先頭が返る。
    BufferedReader を通すと長さは要求どおりになるので、
    呼び出し側から異常を検出する手がかりが残らない。
    """
    f = patched(_stub(range_status=200, range_body=BODY))
    f.seek(1000)
    with pytest.raises(Exception) as excinfo:
        f.read(10)
    assert '200' in str(excinfo.value)


def test_rejects_response_longer_than_requested(patched):
    """206 を名乗っていても、要求より長い本文は拒否する。"""
    f = patched(_stub(range_status=206, range_body=BODY))
    f.seek(1000)
    with pytest.raises(Exception):
        f.read(10)


def test_position_does_not_advance_past_eof_on_bad_response(patched):
    """拒否したあと、読み位置がファイル長を越えていない。

    越えると後続の read が空を返し続け、切り詰められたファイルに見える。
    """
    f = patched(_stub(range_status=200, range_body=BODY))
    f.seek(1000)
    with pytest.raises(Exception):
        f.read(10)
    assert f.tell() <= f.size


def test_buffered_reader_sees_the_error(patched):
    """BufferedReader 越しでも握りつぶさない。実際の呼び出し経路はこちら。"""
    f = patched(_stub(range_status=200, range_body=BODY))
    br = io.BufferedReader(f, buffer_size=1 << 20)
    br.seek(1000)
    with pytest.raises(Exception):
        br.read(10)


def test_retry_waits_between_attempts(patched, monkeypatch):
    """再試行のあいだに待つ。待たないと数ミリ秒で 5 回叩いて諦める。"""
    slept = []
    monkeypatch.setattr(httpzip.time, 'sleep', slept.append)

    attempts = []

    def boom():
        attempts.append(1)
        if len(attempts) < 3:
            raise OSError('transient')

    f = patched(_stub(on_get=boom))
    f.seek(0)
    assert f.read(4) == BODY[:4]
    assert len(attempts) == 3
    assert len(slept) == 2
    assert all(s > 0 for s in slept)


def test_zero_retries_still_makes_one_attempt(patched):
    """retries=0 でも 1 回は取りに行く。

    以前は range(0) でループが回らず、未定義の data に触れて
    UnboundLocalError になっていた。
    """
    f = patched(_stub())
    f.retries = 0
    f.seek(0)
    assert f.read(4) == BODY[:4]
