"""reimport_watchdog.sh の is_real_wrapper を固定する。

置き場所を変えると判定が常に false に倒れ、90 分の打ち切りも kill も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

ループ本体は回さない。実物から is_real_wrapper の定義を取り出して呼ぶ。
テストの中に書き写すと、写しのほうを試すことになって実物の変更を捕まえられない。

macOS に /proc は無いので、読み先のパスだけ差し替える。
判定の case は実物のままなので、そこが変われば落ちる。
"""

import os
import platform
import re
import signal
import subprocess
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
WATCHDOG = REPO / 'deploy' / 'reimport_watchdog.sh'


def _config_env(tmp_path, **overrides):
    """検査だけを試す実行環境。ループへ入る手前で落ちる想定なので、
    バッチのログ類は用意しない。
    """
    logs = tmp_path / 'logs'
    logs.mkdir()
    run_env = dict(os.environ)
    run_env.update({
        'HOME': str(tmp_path),
        'REIMPORT_LOG_DIR': str(logs),
    })
    run_env.update(overrides)
    return run_env


def _existing_wrapper(tmp_path, name='reimport_one.sh'):
    """存在するだけの WRAPPER_PATH ファイルを用意する。

    1-7 の検査 ([ -f "$WRAPPER_PATH" ]) を通すためだけに要る。
    """
    p = tmp_path / name
    p.write_text('#!/usr/bin/env bash\n')
    p.chmod(0o755)
    return p


def _fake_df(bin_dir, avail_field):
    """df -k / の Available 列 (4 列目) を差し替える偽 df。

    -h など他の呼び出しにも同じ行を返すが、trigger_pause の診断表示にしか
    使わないのでテストの成否には関わらない。
    """
    p = bin_dir / 'df'
    p.write_text(
        '#!/usr/bin/env bash\n'
        'echo "Filesystem 1K-blocks Used Available Capacity Mounted"\n'
        'echo "dummy 1 1 %s 1%% /"\n' % avail_field
    )
    p.chmod(0o755)
    return p


def _terminate(proc):
    """テスト終了時に watchdog の子プロセスを確実に片付ける。"""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def _is_real_wrapper_source():
    """実物から is_real_wrapper の定義をそのまま取り出す。"""
    body = WATCHDOG.read_text()
    m = re.search(r'^is_real_wrapper\(\) \{.*?^\}', body, re.S | re.M)
    assert m, 'is_real_wrapper の定義が見つからない'
    return m.group(0)


def _wrapper_path_default_source():
    """実物から WRAPPER_PATH の既定値の定義をそのまま取り出す。"""
    body = WATCHDOG.read_text()
    m = re.search(r'^: "\$\{WRAPPER_PATH:=.*\}"$', body, re.M)
    assert m, 'WRAPPER_PATH の既定値の定義が見つからない'
    return m.group(0)


def _ask(wrapper_path, cmdline, tmp_path):
    """実物の is_real_wrapper に cmdline を判定させ、終了コードを返す。"""
    cmdline_file = tmp_path / 'cmdline'
    cmdline_file.write_text(cmdline)

    fn = _is_real_wrapper_source().replace('/proc/$pid/cmdline', '"$CMDLINE_FILE"')
    script = (
        'set -uo pipefail\n'
        'WRAPPER_PATH=%s\n'
        'CMDLINE_FILE=%s\n'
        '%s\n'
        'is_real_wrapper 1\n'
    ) % (wrapper_path, cmdline_file, fn)

    return subprocess.run(['bash', '-c', script],
                          capture_output=True, text=True).returncode


def test_watchdog_reads_wrapper_path_from_the_environment():
    """判定に使うパスが環境変数から来ている。

    絶対パスが直接書き込まれていると、置き場所を変えたときに一致しなくなる。
    """
    body = WATCHDOG.read_text()
    assert 'WRAPPER_PATH' in body
    assert '"bash $WRAPPER_PATH "*)' in body


def test_watchdog_does_not_hardcode_an_absolute_wrapper_path():
    """判定に使うパスが直に埋め込まれていない。

    実サーバのパスを名指しで書くと、この公開リポジトリにそれを残すことになる。
    絶対パスの形だけを見て、どこを指しているかは問わない。
    """
    body = WATCHDOG.read_text()
    hard = re.findall(r'"bash /[^"$]*reimport_one\.sh', body)
    assert not hard, hard


def test_matches_when_the_cmdline_uses_the_configured_path(tmp_path):
    assert _ask('/somewhere/reimport_one.sh',
                'bash /somewhere/reimport_one.sh 30406 ', tmp_path) == 0


def test_does_not_match_when_the_wrapper_moved(tmp_path):
    """WRAPPER_PATH がずれていれば一致しない。これが黙って起きる失敗の形。"""
    assert _ask('/elsewhere/reimport_one.sh',
                'bash /somewhere/reimport_one.sh 30406 ', tmp_path) == 1


def test_wrapper_path_defaults_to_reimport_one(tmp_path):
    """WRAPPER_PATH を省略すると REIMPORT_ONE に揃う。

    reimport_batch.sh は bash "$REIMPORT_ONE" "$CITY" で wrapper を起動する。
    WRAPPER_PATH の既定がそこからずれていると、判定は永久に false のままで、
    90 分の打ち切りも kill も一致するプロセスを見つけられない。
    """
    line = _wrapper_path_default_source()
    script = (
        'set -uo pipefail\n'
        'unset WRAPPER_PATH\n'
        'REIMPORT_ONE=/srv/plateau/reimport_one.sh\n'
        '%s\n'
        'echo "$WRAPPER_PATH"\n'
    ) % line

    r = subprocess.run(['bash', '-c', script], capture_output=True, text=True)

    assert r.stdout.strip() == '/srv/plateau/reimport_one.sh', r.stdout + r.stderr


def test_non_numeric_disk_halt_kb_aborts_before_the_loop(tmp_path):
    """DISK_HALT_KB が数字でなければ、ループへ入る前に exit 3 で落ちる。

    [ "$AVAIL_KB" -lt "$DISK_HALT_KB" ] は DISK_HALT_KB が数字でないと
    エラー終了し、if がそれを偽として扱う。ディスクの門が黙って消えるので、
    起動時に検査して落とす。ループを回さないよう timeout を付ける。
    """
    run_env = _config_env(tmp_path, DISK_HALT_KB='not-a-number')

    r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                       capture_output=True, text=True, timeout=15)

    assert r.returncode == 3, r.stdout + r.stderr


def test_empty_max_city_min_aborts_before_the_loop(tmp_path):
    """MAX_CITY_MIN が空だと、ループへ入る前に exit 3 で落ちる。

    空のまま $(( MAX_CITY_MIN * 60 )) を評価すると 0 になり、最初の都市が
    0 秒で打ち切られて全体が pause する。起動時に検査して落とす。
    ループを回さないよう timeout を付ける。
    """
    run_env = _config_env(tmp_path, MAX_CITY_MIN='')

    r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                       capture_output=True, text=True, timeout=15)

    assert r.returncode == 3, r.stdout + r.stderr


def test_unreadable_avail_kb_pauses_and_exits_2(tmp_path):
    """df の Available が数字でなければ、比較の手前で pause して exit 2 になる。

    AVAIL_KB を need_int に通さずに [ "$AVAIL_KB" -lt "$DISK_HALT_KB" ] を
    評価すると integer expression expected でエラー終了し、if がそれを
    偽として扱う。メモリ逼迫で df が壊れたときに限ってディスクの門が消える
    ので、ここで確かめて trigger_pause に倒す。
    """
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    _fake_df(bin_dir, 'not-a-number')
    wrapper = _existing_wrapper(tmp_path)

    run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper))
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])

    r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                       capture_output=True, text=True, timeout=15)

    assert r.returncode == 2, r.stdout + r.stderr
    assert (tmp_path / 'reimport_pause').exists()
    assert 'AVAIL_KB を読めない' in r.stdout + r.stderr


def test_readable_avail_kb_does_not_halt_for_that_reason(tmp_path):
    """df が数字を返す限り、その理由では止まらない。

    このテストが無いと「AVAIL_KB を見た瞬間に必ず exit 2 する」実装でも
    上のテストが通ってしまう。十分に大きい空きを返す df を使い、ループが
    1 巡してもまだ動いていることを確かめる。
    """
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    _fake_df(bin_dir, '99999999')
    wrapper = _existing_wrapper(tmp_path)

    run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper))
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])

    proc = subprocess.Popen(['bash', str(WATCHDOG)], env=run_env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    try:
        time.sleep(1.5)
        assert proc.poll() is None, (
            '読める AVAIL_KB なのに終了した: ' + (proc.stdout.read() if proc.stdout else ''))
    finally:
        _terminate(proc)


def test_missing_wrapper_path_halts_before_start_log(tmp_path):
    """WRAPPER_PATH の実体が無ければ、START ログの手前で exit 3 になる。

    pgrep -f は basename で当たるので候補は見つかり、is_real_wrapper の
    前方一致だけが永久に false になる。ログは正常に出続けるので、90 分の
    打ち切りが 1 度も発動しないことに誰も気づけない。起動時に確かめて落とす。
    """
    missing = tmp_path / 'does_not_exist.sh'
    run_env = _config_env(tmp_path, WRAPPER_PATH=str(missing))

    r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                       capture_output=True, text=True, timeout=15)

    assert r.returncode == 3, r.stdout + r.stderr
    assert 'WATCHDOG START' not in (r.stdout + r.stderr)


def test_existing_wrapper_path_reaches_the_start_log(tmp_path):
    """WRAPPER_PATH の実体があれば、検査を通って START ログまで進む。

    このテストが無いと「WRAPPER_PATH を見た瞬間に必ず exit 3 する」実装でも
    上のテストが通ってしまう。
    """
    wrapper = _existing_wrapper(tmp_path)
    run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper))

    proc = subprocess.Popen(['bash', str(WATCHDOG)], env=run_env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    try:
        time.sleep(1.5)
        assert proc.poll() is None, (
            '実在する WRAPPER_PATH なのに終了した: ' + (proc.stdout.read() if proc.stdout else ''))
        log = (tmp_path / 'logs' / 'watchdog.log').read_text()
        assert 'WATCHDOG START' in log
    finally:
        _terminate(proc)


def test_continuous_failures_pause_with_exit_7_not_3(tmp_path):
    """連続 FAIL による pause は exit 7。設定検査の失敗 (exit 3) と混ざらない。

    どちらも exit 3 のままだと、watchdog が止まった理由を終了コードから
    切り分けられない。5 は reimport_batch.sh の二重取り込み検出と衝突する
    ため避ける。他の設定検査系のテストは exit 3 のままであることを固定して
    いるので、この 2 種類が揃って初めてコードが割れていることを確かめられる。
    """
    wrapper = _existing_wrapper(tmp_path)
    run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper))
    logs = tmp_path / 'logs'

    proc = subprocess.Popen(['bash', str(WATCHDOG)], env=run_env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    try:
        time.sleep(0.5)  # baseline (START_LINES) を確定させる
        with open(logs / 'summary.log', 'a') as f:
            for i in range(3):
                f.write('[2026-08-18 00:00:0%d] [1/1] city: FAIL exit=11\n' % i)

        rc = proc.wait(timeout=15)

        assert rc == 7, proc.stdout.read() if proc.stdout else ''
        assert (tmp_path / 'reimport_pause').exists()
    finally:
        _terminate(proc)


def test_valid_config_enters_the_loop_and_exits_when_batch_status_appears(tmp_path):
    """正しい設定で起動すると、ループへ入って batch_status を検出し exit 0 で終わる。

    「起動して即 exit 3」の実装でも、設定検査だけを見るテストは全部通ってしまう。
    batch_status の検出はループの中でしか起きない。
    ここが exit 0 になること自体が、起動時の検査
    (need_int や WRAPPER_PATH の実在確認) を通り抜けた証拠になる。
    """
    wrapper = _existing_wrapper(tmp_path)
    run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper))
    (tmp_path / 'logs' / 'batch_status').write_text('DONE\n')

    r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                       capture_output=True, text=True, timeout=15)

    assert r.returncode == 0, r.stdout + r.stderr


def test_disk_halt_kb_breach_pauses_and_exits_2(tmp_path):
    """DISK_HALT_KB を実機の空き容量よりずっと大きくすると、pause を立てて exit 2 になる。

    df は差し替えない。
    テストを動かす機械の実際の空き容量が、この極端な閾値を必ず下回ることを利用する。
    ディスク HALT の分岐そのものを消した実装や、
    閾値を無視して回し続ける実装はタイムアウトで検出する。

    returncode と pause ファイルだけでは、AVAIL_KB が読めないときの門
    (test_unreadable_avail_kb_pauses_and_exits_2) と区別できない。
    どちらも trigger_pause 経由で同じ exit 2 になるため、
    watchdog.log に残る理由の文字列まで見て門を取り違えていないか確かめる。
    """
    wrapper = _existing_wrapper(tmp_path)
    run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper),
                          DISK_HALT_KB='999999999999999')

    r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                       capture_output=True, text=True, timeout=15)

    assert r.returncode == 2, r.stdout + r.stderr
    assert (tmp_path / 'reimport_pause').exists()
    log = (tmp_path / 'logs' / 'watchdog.log').read_text()
    assert 'disk HALT' in log, log


@pytest.mark.skipif(
    platform.system() != 'Linux',
    reason='is_real_wrapper は /proc/$pid/cmdline を読むので Linux でしか動かない',
)
def test_max_city_min_zero_kills_a_real_stuck_wrapper(tmp_path):
    """MAX_CITY_MIN を 0 にすると、90 分の打ち切りが実物の子プロセスに対して発動する。

    is_real_wrapper は /proc/$pid/cmdline を読むので、macOS では動かせない。
    ここだけは書き写しではなく、実物の watchdog を実物の子プロセスに対して走らせて確かめる。
    """
    wrapper = tmp_path / 'wrapper.sh'
    wrapper.write_text('#!/usr/bin/env bash\nsleep 30\n')
    wrapper.chmod(0o755)

    # start_new_session=True で bash を新しいプロセスグループの
    # リーダーにする。sleep 30 はその子として同じグループに入るので、
    # 後始末は bash 単体ではなくグループごと殺す。
    child = subprocess.Popen(['bash', str(wrapper), 'stuckcity'], start_new_session=True)
    try:
        run_env = _config_env(tmp_path, INTERVAL='1', WRAPPER_PATH=str(wrapper),
                              MAX_CITY_MIN='0')

        r = subprocess.run(['bash', str(WATCHDOG)], env=run_env,
                           capture_output=True, text=True, timeout=20)

        assert r.returncode == 4, r.stdout + r.stderr
        assert (tmp_path / 'reimport_pause').exists()

        deadline = time.time() + 10
        alive = True
        while time.time() < deadline:
            if child.poll() is not None:
                alive = False
                break
            time.sleep(0.2)
        assert not alive, '打ち切りの kill -TERM が実物の子プロセスに届かなかった'
    finally:
        # bash 自体は watchdog の kill -TERM で落ちても、その下の sleep 30 は
        # 別プロセスとして残り得る。プロセスグループごと SIGKILL して片付ける。
        try:
            os.killpg(os.getpgid(child.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=10)
