"""reimport_watchdog.sh の is_real_wrapper を固定する。

置き場所を変えると判定が常に false に倒れ、90 分の打ち切りも kill も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

ループ本体は回さない。実物から is_real_wrapper の定義を取り出して呼ぶ。
テストの中に書き写すと、写しのほうを試すことになって実物の変更を捕まえられない。

macOS に /proc は無いので、読み先のパスだけ差し替える。
判定の case は実物のままなので、そこが変われば落ちる。
"""

import os
import re
import subprocess
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
