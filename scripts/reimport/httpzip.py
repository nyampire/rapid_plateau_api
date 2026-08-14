"""HTTP Range-backed file object so zipfile can read a remote zip without downloading it."""
import io, time, urllib.request


class RangeNotHonored(RuntimeError):
    """配信元が Range 要求を尊重しなかった。受け入れると別の場所のデータを読む。"""


class HttpFile(io.RawIOBase):
    def __init__(self, url, timeout=120, retries=5):
        self.url, self.timeout, self.retries = url, timeout, retries
        self._pos = 0
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=timeout) as r:
            length = r.headers.get('Content-Length')
            if length is None:
                raise RuntimeError('server did not report a content length')
            self.size = int(length)
            if r.headers.get('Accept-Ranges') != 'bytes':
                raise RuntimeError('server does not advertise byte ranges')
        self.bytes_fetched = 0

    def seekable(self): return True
    def readable(self): return True

    def seek(self, off, whence=0):
        if whence == 0: self._pos = off
        elif whence == 1: self._pos += off
        else: self._pos = self.size + off
        return self._pos

    def tell(self): return self._pos

    def _fetch(self, start, end):
        """[start, end] を 1 回取りに行く。Range が効いていなければ例外。"""
        req = urllib.request.Request(
            self.url, headers={'Range': 'bytes=%d-%d' % (start, end)})
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            status = getattr(r, 'status', None)
            if status != 206:
                # 200 は Range を無視して全文を返している。長さも例外も
                # 正常に見えるので、ここで落とさないと呼び出し側は
                # 別の場所のデータを正しいものとして読む。
                raise RangeNotHonored(
                    'range request answered with %s, not 206' % status)
            data = r.read()
        if len(data) > end - start + 1:
            raise RangeNotHonored(
                'range response longer than requested (%d > %d)'
                % (len(data), end - start + 1))
        return data

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self._pos
        n = min(n, self.size - self._pos)
        if n <= 0: return b''
        end = self._pos + n - 1
        for attempt in range(max(1, self.retries)):
            try:
                data = self._fetch(self._pos, end)
                break
            except RangeNotHonored:
                raise                                    # 再試行しても直らない
            except Exception:                            # transient CDN hiccups
                if attempt == max(1, self.retries) - 1:
                    raise
                time.sleep(2 ** attempt)
        self._pos += len(data)
        self.bytes_fetched += len(data)
        return data

    def readinto(self, b):
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)
