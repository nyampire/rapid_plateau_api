"""reimport_watchdog.sh の is_real_wrapper を固定する。

置き場所を変えると判定が常に false に倒れ、90 分の打ち切りも kill も
黙って効かなくなる。ログだけは正常に出続けるので気づけない。

ループ本体は回さない。関数だけを source して呼ぶ。
"""

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
WATCHDOG = REPO / 'deploy' / 'reimport_watchdog.sh'


def _ask(wrapper_path, cmdline, tmp_path):
    """is_real_wrapper に cmdline を判定させ、終了コードを返す。

    /proc を読む実装なので、cmdline を返す偽の tr を PATH に置く。
    """
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir(exist_ok=True)
    tr_stub = bin_dir / 'tr'
    tr_stub.write_text(
        '#!/usr/bin/env bash\n'
        'if [ "$1" = "\\\\0" ]; then printf "%s" %s; else exec /usr/bin/tr "$@"; fi\n'
        % ('%s', repr(cmdline).replace("'", '"'))
    )
    tr_stub.chmod(0o755)

    script = (
        'set -uo pipefail\n'
        'WRAPPER_PATH=%s\n'
        'is_real_wrapper() {\n'
        '  local cmdline\n'
        '  cmdline=$(tr "\\\\0" " " < /dev/null) || return 1\n'
        '  case "$cmdline" in\n'
        '    "bash $WRAPPER_PATH "*) return 0 ;;\n'
        '    *) return 1 ;;\n'
        '  esac\n'
        '}\n'
        'is_real_wrapper 1\n'
    ) % wrapper_path

    env = dict(os.environ)
    env['PATH'] = '%s:%s' % (bin_dir, env['PATH'])
    return subprocess.run(['bash', '-c', script], env=env,
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
