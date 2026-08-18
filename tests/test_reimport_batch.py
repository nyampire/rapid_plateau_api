"""reimport_batch.sh の一覧の受け取りと done.txt の照合を固定する。"""

import os
import signal
import subprocess
import time
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
        # 既定の plateau_importer2postgis.py のままだと、機械上の無関係な
        # プロセスに引きずられて exit 5 になる。
        # tmp_path 配下の存在しない名前にして pgrep が一致しないようにする。
        # pgrep -f はこの値を拡張正規表現として扱うが、tmp_path にメタ文字は出ない。
        'STRAY_IMPORT_PATTERN': str(tmp_path / 'no_stray_import_marker'),
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
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['13402', '30406']


def test_batch_skips_cities_listed_in_done(batch_env):
    """done.txt にある都市は飛ばす。照合は行全体との一致。"""
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n30406\n')
    (batch_env.logs / 'done.txt').write_text('13402\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

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
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

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
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

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
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.read_text().split() == ['13402', '30406', '43213']


def test_wrapper_exit_2_aborts_the_whole_batch(batch_env):
    """wrapper が exit 2 (ディスク不足) を返したら、その場で全体を止める。

    exit 2 でバッチ全体を止める分岐は安全装置の中心にある。
    ここを消すと、失敗が failed.txt に記録されるだけで次の都市へ進んでしまう。
    returncode と batch_status に加えて、後続の都市が呼ばれていないことも確かめる。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n30406\n')
    one = batch_env.tmp / 'one_stub.sh'
    one.write_text(
        '#!/usr/bin/env bash\n'
        'if [ "$1" = "13402" ]; then\n'
        '  exit 2\n'
        'fi\n'
        'echo "$1" >> "%s"\n'
        'exit 0\n' % batch_env.called
    )
    one.chmod(0o755)

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 2, r.stdout + r.stderr
    assert (batch_env.logs / 'batch_status').read_text().strip() == 'DISK_ABORT'
    assert (batch_env.logs / 'failed.txt').read_text().splitlines() == ['13402 2']
    assert not batch_env.called.exists(), 'exit 2 の後、次の都市 (30406) が呼ばれてしまった'


def test_pause_file_stops_the_batch_with_paused_status(batch_env):
    """$HOME/reimport_pause があれば、都市を 1 つも処理せずに PAUSED で止まる。

    pause の検出を無効化した実装でも、都市が正常に呼ばれて DONE で終わってしまう。
    都市が呼ばれていないことまで見て、検出漏れを塞ぐ。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n30406\n')
    (batch_env.tmp / 'reimport_pause').write_text('')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr
    assert (batch_env.logs / 'batch_status').read_text().strip() == 'PAUSED'
    assert not batch_env.called.exists(), 'pause 中なのに都市が呼ばれた'


def test_successful_city_is_appended_to_done_txt(batch_env):
    """成功した都市の番号が done.txt に 1 行増える。

    既存のテストは事前に置いた done.txt を読むだけで、バッチ自身が書く側は固定されていなかった。
    再開機能の半分しか固定されていない。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr
    assert (batch_env.logs / 'done.txt').read_text().splitlines() == ['13402']


def test_failed_city_is_appended_to_failed_txt_with_its_exit_code(batch_env):
    """失敗した都市が「<都市> <終了コード>」の形で failed.txt に増える。

    exit 2 は全体を止める予約番号なので、続行できる別の値 (9) を使う。
    上の test_wrapper_exit_2_aborts_the_whole_batch と合わせて、2 種類の終了コードで確かめる。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n')
    one = batch_env.tmp / 'one_stub.sh'
    one.write_text('#!/usr/bin/env bash\nexit 9\n')
    one.chmod(0o755)

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr
    assert (batch_env.logs / 'failed.txt').read_text().splitlines() == ['13402 9']
    assert (batch_env.logs / 'done.txt').read_text().strip() == ''


def test_stray_import_process_aborts_before_any_city_runs(batch_env):
    """既に取り込みが走っていれば、都市を 1 つも触らずに exit 5 で止まる。

    既定の検出対象は機械全体を見る plateau_importer2postgis.py という一般的な名前になっている。
    このテストでは STRAY_IMPORT_PATTERN でテスト専用の目印に絞り、
    機械上の無関係なプロセスに引きずられないようにする。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n')
    marker_script = batch_env.tmp / 'stray_marker_for_test.sh'
    marker_script.write_text('#!/usr/bin/env bash\nsleep 30\n')
    marker_script.chmod(0o755)
    batch_env.run_env['STRAY_IMPORT_PATTERN'] = str(marker_script)

    # start_new_session=True で bash を新しいプロセスグループの
    # リーダーにする。sleep 30 はその子として同じグループに入るので、
    # 後始末は bash 単体ではなくグループごと殺す。
    proc = subprocess.Popen(['bash', str(marker_script)], start_new_session=True)
    try:
        deadline = time.time() + 10
        found = False
        while time.time() < deadline:
            out = subprocess.run(['pgrep', '-f', str(marker_script)],
                                 capture_output=True, text=True)
            if out.stdout.strip():
                found = True
                break
            time.sleep(0.1)
        assert found, '見張り対象のプロセスが起動しなかった'

        r = subprocess.run(['bash', str(BATCH), str(targets)],
                           env=batch_env.run_env, capture_output=True, text=True, timeout=15)

        assert r.returncode == 5, r.stdout + r.stderr
        assert (batch_env.logs / 'batch_status').read_text().strip() == 'STRAY_IMPORT'
        assert not batch_env.called.exists()
    finally:
        # bash だけを kill すると、その下の sleep 30 が孫として残る。
        # プロセスグループごと SIGKILL して sleep まで確実に片付ける。
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=10)


def test_done_txt_match_is_the_whole_line_not_a_substring(batch_env):
    """done.txt との照合は行全体との一致で、部分一致ではない。

    done.txt に別の都市 113402 (13402 を部分文字列として含む) しか無ければ、13402 は「済み」ではない。
    grep -qx を grep -q に変えた実装は部分一致で誤って skip してしまう。
    13402 が実際に呼ばれることまで確かめる。
    """
    targets = batch_env.tmp / 'targets.txt'
    targets.write_text('13402\n')
    (batch_env.logs / 'done.txt').write_text('113402\n')

    r = subprocess.run(['bash', str(BATCH), str(targets)],
                       env=batch_env.run_env, capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr
    assert batch_env.called.exists(), '13402 が誤って skip された'
    assert batch_env.called.read_text().split() == ['13402']
