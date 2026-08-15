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


def test_extract_gate_uses_the_number_from_the_report(env):
    """比較に使う数が、報告された meshes から来ている。

    他の 3 件はどれも meshes が 2 なので、比較対象を 2 に固定して
    $EXTRACT_JSON を読まない実装でも全部通ってしまう。
    3 メッシュの報告に 3 ファイルを添えると、2 固定の実装はここで落ちる。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'printf "<x/>" > "$2/53394501_bldg_6697_op.gml"\n'
          'printf "<x/>" > "$2/53394502_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":3,"raw_bytes":15}\'')

    r = _run(env)

    assert r.returncode != 10, r.stdout + r.stderr
    assert '報告 3 メッシュ、実ファイル 3' in r.stdout


def _good_extract(e, n=2):
    """n 個の .gml を書き出し、meshes=n を報告する偽 extract。"""
    body = 'mkdir -p "$2"\n'
    for i in range(n):
        body += 'printf "<x/>" > "$2/5339450%d_bldg_6697_op.gml"\n' % i
    body += 'echo \'{"city_code":"30406","meshes":%d,"raw_bytes":5}\'' % n
    _stub(e.bin, 'extract_stub', body)


def _good_java(e):
    """cwd の .gml と同じ数だけ、閉じタグ付きの .osm を書く偽 java。"""
    _stub(e.bin, 'java',
          'for f in *.gml; do\n'
          '  printf "<osm><node/></osm>" > "${f%.gml}.osm"\n'
          'done\n'
          'exit 0')


def _good_transfer(e, remote_count=None):
    """rsync と ssh の偽物。ssh は転送先の枚数を答える。"""
    _stub(e.bin, 'rsync', 'exit 0')
    n = 'echo "$REMOTE_N"' if remote_count is None else 'echo %d' % remote_count
    _stub(e.bin, 'ssh', n)


def test_convert_gate_fails_on_truncated_osm(env):
    """閉じタグの無い .osm があれば、変換の門で落ちる。"""
    _good_extract(env, n=2)
    _stub(env.bin, 'java',
          'printf "<osm><node/></osm>" > 53394500_bldg_6697_op.osm\n'
          'printf "<osm><node" > 53394501_bldg_6697_op.osm\n'
          'exit 0')
    _good_transfer(env)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr


def test_convert_gate_fails_when_java_exits_nonzero(env):
    """java が 0 以外で終われば、変換の門で落ちる。"""
    _good_extract(env, n=2)
    _stub(env.bin, 'java',
          'printf "<osm><node/></osm>" > 53394500_bldg_6697_op.osm\n'
          'printf "<osm><node/></osm>" > 53394501_bldg_6697_op.osm\n'
          'exit 3')
    _good_transfer(env)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr


def test_transfer_gate_fails_when_remote_count_differs(env):
    """転送先の枚数が手元と違えば、転送の門で落ちる。"""
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=1)

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr


def test_records_city_and_count_when_everything_passes(env):
    """全部通れば shipped.txt に <citycode> <osm数> が入り、作業を消す。"""
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.shipped.read_text().strip() == '30406 2'
    assert not (env.work_root / '30406').exists()


def test_transfer_gate_fails_when_the_remote_count_is_unreadable(env):
    """転送先の枚数を数えられなければ落ちる。

    ssh が失敗すると REMOTE_N が空になる。そのまま比較すると
    integer expression expected でエラー終了し、if がそれを偽として扱う。
    門が消え、転送を確かめないまま shipped.txt に記録して作業を消す。
    記録された都市は ship_all.sh が永久に飛ばす。
    """
    _good_extract(env, n=2)
    _good_java(env)
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh', 'echo "ssh: connect failed" >&2\nexit 255')

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_gates_use_computed_counts_not_constants(env):
    """門が数える値を使っている。定数と比べていない。

    他のテストがどれも 2 メッシュなので、.osm 数も転送先の枚数も 2 に
    固定した実装で通ってしまう。3 メッシュで一通り流して区別する。
    """
    _good_extract(env, n=3)
    _good_java(env)
    _good_transfer(env, remote_count=3)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.shipped.read_text().strip() == '30406 3'
