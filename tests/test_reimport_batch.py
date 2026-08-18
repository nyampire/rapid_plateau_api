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


def test_missing_list_exits_6_with_no_list_status(batch_env):
    """一覧ファイルが無ければ、握りつぶさずに exit 6 で止まる。

    deploy/README.md の手順は日時付きの一覧名を打たせる。打ち間違えると
    148 都市が 1 件も走らないまま exit 0 で終わり、5 秒後に上げる watchdog も
    batch_status を見て即座に終了するので、監視も一緒に消える。
    """
    missing = batch_env.tmp / 'does_not_exist.txt'

    r = subprocess.run(['bash', str(BATCH), str(missing)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 6, r.stdout + r.stderr
    assert (batch_env.logs / 'batch_status').read_text().strip() == 'NO_LIST'
    assert not batch_env.called.exists()


def test_empty_list_exits_6_with_empty_list_status(batch_env):
    """一覧ファイルはあるが 0 件なら、握りつぶさずに exit 6 で止まる。

    grep -c . が壊れた値を返す、あるいは空の一覧をそのまま流すと、
    ループが 0 回で完走して「成功」に見える。NO_LIST とは別の合図にして
    どちらの事情で止まったかログから区別できるようにする。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 6, r.stdout + r.stderr
    assert (batch_env.logs / 'batch_status').read_text().strip() == 'EMPTY_LIST'
    assert not batch_env.called.exists()


def test_batch_does_not_leak_stdin_to_the_child(batch_env):
    """bash "$REIMPORT_ONE" "$CITY" は while の stdin (一覧ファイル) を継がない。

    いまの取り込み器は stdin を読まないので無害だが、読む処理が入ると
    親の while が読んでいる一覧ファイルの次の行を子が横取りし、都市が
    黙って抜け落ちる。stdin を読む偽 REIMPORT_ONE でその横取りを再現する。
    3 都市のうち 30406 だけが子に食われて called.txt から消えれば、
    < /dev/null が抜けている合図になる。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n30406\n43213\n')
    one = batch_env.tmp / 'one_stub.sh'
    one.write_text(
        '#!/usr/bin/env bash\n'
        'read -r _stolen\n'
        'echo "$1" >> "%s"\n'
        'exit 0\n' % batch_env.called
    )
    one.chmod(0o755)

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['13402', '30406', '43213']
