"""reimport_batch.sh の一覧の受け取りと done.txt の照合を固定する。"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
BATCH = REPO / 'deploy' / 'reimport_batch.sh'


@pytest.fixture
def batch_env(tmp_path):
    logs = tmp_path / 'logs'
    logs.mkdir()
    called = tmp_path / 'called.txt'
    one = tmp_path / 'one_stub.sh'
    one.write_text(
        '#!/usr/bin/env bash\n'
        'echo "$1" >> "%s"\n'
        'exit 0\n' % called
    )
    one.chmod(0o755)

    run_env = dict(os.environ)
    run_env.update({
        'HOME': str(tmp_path),
        'REIMPORT_LOG_DIR': str(logs),
        'REIMPORT_ONE': str(one),
    })

    class Env:
        pass

    e = Env()
    e.tmp = tmp_path
    e.logs = logs
    e.called = called
    e.run_env = run_env
    return e


def test_batch_reads_the_list_given_as_an_argument(batch_env):
    """一覧のパスを引数で受け取る。固定名だけだと新しい一覧を渡せない。"""
    targets = batch_env.tmp / 'reimport_targets_20260814-120000.txt'
    targets.write_text('13402\n30406\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['13402', '30406']


def test_batch_skips_cities_listed_in_done(batch_env):
    """done.txt にある都市は飛ばす。照合は行全体との一致。"""
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n30406\n')
    (batch_env.logs / 'done.txt').write_text('13402\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['30406']
