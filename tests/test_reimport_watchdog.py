"""reimport_watchdog.sh の is_real_wrapper を固定する。

置き場所を変えると判定が常に false に倒れ、90 分の打ち切りも kill も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

ループ本体は回さない。実物から is_real_wrapper の定義を取り出して呼ぶ。
テストの中に書き写すと、写しのほうを試すことになって実物の変更を捕まえられない。

macOS に /proc は無いので、読み先のパスだけ差し替える。
判定の case は実物のままなので、そこが変われば落ちる。
"""

import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
WATCHDOG = REPO / 'deploy' / 'reimport_watchdog.sh'


def _is_real_wrapper_source():
    """実物から is_real_wrapper の定義をそのまま取り出す。"""
    body = WATCHDOG.read_text()
    m = re.search(r'^is_real_wrapper\(\) \{.*?^\}', body, re.S | re.M)
    assert m, 'is_real_wrapper の定義が見つからない'
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
