"""reship_all.sh の初回準備と再実行を固定する。"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
RESHIP_ALL = REPO / 'scripts' / 'reimport' / 'reship_all.sh'


def _stub(bin_dir: Path, name: str, body: str):
    p = bin_dir / name
    p.write_text('#!/usr/bin/env bash\n' + body + '\n')
    p.chmod(0o755)
    return p


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    work = tmp_path / 'work'
    work.mkdir()
    shipped = tmp_path / 'shipped.txt'
    called = tmp_path / 'called.txt'

    # 既定は 1 周で全部成功する ship_all.sh。呼ばれた回数を called.txt に積む。
    _stub(bin_dir, 'ship_all_stub', 'echo call >> "%s"\nexit 0' % called)

    ship_env = tmp_path / 'ship.env'
    ship_env.write_text(
        'WORK_ROOT="%s"\n'
        'SHIPPED_TXT="%s"\n'
        'SHIP_HOST="stubhost"\n'
        'SHIP_PATH="/stub/import"\n'
        % (work, shipped)
    )

    run_env = dict(os.environ)
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])
    run_env['SHIP_ENV'] = str(ship_env)
    run_env['SHIP_ALL_CMD'] = str(bin_dir / 'ship_all_stub')

    class Env:
        pass

    e = Env()
    e.bin = bin_dir
    e.work = work
    e.shipped = shipped
    e.called = called
    e.ship_env = ship_env
    e.run_env = run_env
    e.tmp = tmp_path
    e.marker = work / 'reship_in_progress'
    return e


def _run(e):
    return subprocess.run(['bash', str(RESHIP_ALL)],
                          env=e.run_env, capture_output=True, text=True,
                          timeout=60)


def _calls(e):
    if not e.called.exists():
        return 0
    return len([x for x in e.called.read_text().splitlines() if x])


class TestConfigCheck:
    """設定が無い、または必須の値が欠けていれば、何もせずに 3 で止まる。"""

    def test_stops_when_ship_env_is_missing(self, env):
        env.ship_env.unlink()
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 0

    def test_stops_when_work_root_is_missing(self, env):
        env.ship_env.write_text('SHIPPED_TXT="%s"\nSHIP_HOST="h"\n' % env.shipped)
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 0

    def test_stops_when_shipped_txt_is_missing(self, env):
        env.ship_env.write_text('WORK_ROOT="%s"\nSHIP_HOST="h"\n' % env.work)
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 0
