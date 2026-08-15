"""ship_city.sh の門と失敗時の振る舞いを固定する。

外部コマンドは PATH に置いた偽物に差し替える。通信も変換もしない。
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SHIP_CITY = REPO / 'scripts' / 'reimport' / 'ship_city.sh'


def _stub(bin_dir: Path, name: str, body: str):
    """PATH に置く偽コマンドを作る。"""
    p = bin_dir / name
    p.write_text('#!/usr/bin/env bash\n' + body + '\n')
    p.chmod(0o755)
    return p


@pytest.fixture
def env(tmp_path):
    """ship_city.sh を走らせるための一式を用意する。"""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    work_root = tmp_path / 'work'
    work_root.mkdir()
    conversion = tmp_path / 'conversion.json'
    conversion.write_text('{}')
    jar = tmp_path / 'fake.jar'
    jar.write_text('')
    shipped = tmp_path / 'shipped.txt'

    ship_env = tmp_path / 'ship.env'
    ship_env.write_text(
        'EXTRACT_CMD="%s/extract_stub"\n'
        'JAVA_BIN="%s/java"\n'
        'CITYGML_OSM_JAR="%s"\n'
        'CONVERSION_JSON="%s"\n'
        'WORK_ROOT="%s"\n'
        'SHIP_HOST="stubhost"\n'
        'SHIP_PATH="/stub/import"\n'
        'SHIPPED_TXT="%s"\n'
        'DISK_MIN_KB=0\n'
        % (bin_dir, bin_dir, jar, conversion, work_root, shipped)
    )

    run_env = dict(os.environ)
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])
    run_env['SHIP_ENV'] = str(ship_env)

    class Env:
        pass

    e = Env()
    e.bin = bin_dir
    e.work_root = work_root
    e.shipped = shipped
    e.run_env = run_env
    e.tmp = tmp_path
    return e


def _run(e, city='30406'):
    return subprocess.run(
        ['bash', str(SHIP_CITY), city],
        env=e.run_env, capture_output=True, text=True)


def test_extract_gate_fails_when_file_count_differs(env):
    """報告された meshes より少ない .gml しか出なければ、取り出しの門で落ちる。"""
    # meshes は 2 と報告するが、書き出すのは 1 個だけ
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":2,"raw_bytes":5}\'')

    r = _run(env)

    assert r.returncode == 10, r.stderr
    assert not env.shipped.exists()


def test_extract_gate_passes_when_file_count_matches(env):
    """報告と実ファイル数が合えば、取り出しの門を通る。

    これが無いと「常に exit 10 で落ちる」実装でも上のテストが通ってしまい、
    門が比較していることを何も固定できない。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'printf "<x/>" > "$2/53394501_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":2,"raw_bytes":10}\'')

    r = _run(env)

    assert r.returncode != 10, r.stdout + r.stderr
    assert '報告 2 メッシュ、実ファイル 2' in r.stdout


def test_extract_gate_fails_when_the_report_is_unreadable(env):
    """meshes を読めなければ落ちる。

    MESHES が空のまま比較すると [ は integer expression expected で
    エラー終了し、if がそれを偽として扱うので門をすり抜ける。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo "これは JSON ではない"')

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
