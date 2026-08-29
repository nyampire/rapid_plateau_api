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


class TestFirstRun:
    """印が無ければ初回。shipped.txt を改名して空にし、印を置く。"""

    def test_renames_shipped_and_leaves_it_empty(self, env):
        env.shipped.write_text('01100 50\n01202 30\n')
        r = _run(env)
        assert r.returncode == 0
        assert env.shipped.read_text() == ''
        backups = sorted(env.tmp.glob('shipped.txt.*'))
        assert len(backups) == 1, '改名した控えが 1 つだけ残る'
        assert backups[0].read_text() == '01100 50\n01202 30\n'

    def test_writes_the_marker(self, env):
        env.shipped.write_text('01100 50\n')
        _run(env)
        assert env.marker.exists()
        lines = env.marker.read_text().splitlines()
        assert lines[0].startswith('開始 ')
        assert 'shipped.txt.' in lines[1]

    def test_works_when_shipped_does_not_exist(self, env):
        # shipped.txt がまだ無い状態でも止まらない。控えは作らない。
        assert not env.shipped.exists()
        r = _run(env)
        assert r.returncode == 0
        assert env.shipped.read_text() == ''
        assert sorted(env.tmp.glob('shipped.txt.*')) == []


class TestSecondRun:
    """印があれば再実行。shipped.txt に触らない。"""

    def test_keeps_shipped_when_marker_exists(self, env):
        env.marker.write_text('開始 2026-08-29 00:00:00\n退避 shipped.txt.old\n')
        env.shipped.write_text('01100 50\n01202 30\n')
        r = _run(env)
        assert r.returncode == 0
        assert env.shipped.read_text() == '01100 50\n01202 30\n'
        assert sorted(env.tmp.glob('shipped.txt.*')) == []

    def test_does_not_overwrite_the_marker(self, env):
        original = '開始 2026-08-29 00:00:00\n退避 shipped.txt.old\n'
        env.marker.write_text(original)
        _run(env)
        assert env.marker.read_text() == original


def _stub_exits(env, *codes):
    """呼ばれるたびに codes の順で終了コードを返す ship_all.sh に差し替える。"""
    seq = env.tmp / 'seq.txt'
    seq.write_text('\n'.join(str(c) for c in codes) + '\n')
    _stub(env.bin, 'ship_all_stub',
          'echo call >> "%s"\n'
          'N=$(wc -l < "%s" | tr -d " ")\n'
          'I=$(wc -l < "%s" | tr -d " ")\n'
          'CODE=$(sed -n "${I}p" "%s")\n'
          '[ -z "$CODE" ] && CODE=$(sed -n "${N}p" "%s")\n'
          'exit "$CODE"' % (env.called, seq, env.called, seq, seq))


class TestRetry:
    """1 周目が一部失敗 (1) のときだけ、もう 1 度だけ実行する。"""

    def test_runs_once_when_everything_succeeds(self, env):
        _stub_exits(env, 0)
        r = _run(env)
        assert r.returncode == 0
        assert _calls(env) == 1

    def test_runs_twice_when_some_cities_fail(self, env):
        _stub_exits(env, 1, 0)
        r = _run(env)
        assert r.returncode == 0, '2 周目で全部成功したので 0 になる'
        assert _calls(env) == 2

    def test_stops_after_the_second_round(self, env):
        _stub_exits(env, 1, 1)
        r = _run(env)
        assert r.returncode == 1
        assert _calls(env) == 2, '3 周目は実行しない'

    def test_does_not_retry_on_disk_shortage(self, env):
        _stub_exits(env, 2)
        r = _run(env)
        assert r.returncode == 2
        assert _calls(env) == 1, 'ディスク不足はやり直しても直らない'

    def test_does_not_retry_on_config_error(self, env):
        _stub_exits(env, 3)
        r = _run(env)
        assert r.returncode == 3
        assert _calls(env) == 1

    def test_does_not_retry_on_transfer_failure(self, env):
        _stub_exits(env, 4)
        r = _run(env)
        assert r.returncode == 4
        assert _calls(env) == 1
