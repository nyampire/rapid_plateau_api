"""HTTP Range-backed file object so zipfile can read a remote zip without downloading it."""
import io, urllib.request


class HttpFile(io.RawIOBase):
    def __init__(self, url, timeout=120, retries=5):
        self.url, self.timeout, self.retries = url, timeout, retries
        self._pos = 0
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=timeout) as r:
            self.size = int(r.headers['Content-Length'])
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

    def read(self, n=-1):
        if n is None or n < 0:
            n = self.size - self._pos
        n = min(n, self.size - self._pos)
        if n <= 0: return b''
        end = self._pos + n - 1
        last = None
        for attempt in range(self.retries):
            try:
                req = urllib.request.Request(
                    self.url, headers={'Range': 'bytes=%d-%d' % (self._pos, end)})
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    data = r.read()
                break
            except Exception as e:                       # transient CDN hiccups
                last = e
                if attempt == self.retries - 1:
                    raise
        self._pos += len(data)
        self.bytes_fetched += len(data)
        return data

    def readinto(self, b):
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)
