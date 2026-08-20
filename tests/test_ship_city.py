"""ship_city.sh の門と失敗時の振る舞いを固定する。

外部コマンドは PATH に置いた偽物に差し替える。通信も変換もしない。
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SHIP_CITY = REPO / 'scripts' / 'reimport' / 'ship_city.sh'
SHIP_ENV_EXAMPLE = REPO / 'scripts' / 'reimport' / 'ship.env.example'

# 空の .osm 検査 (ship_city.sh:139) が出す文言。
# この文言を変えるとテストは赤くなる。
# 赤くなったら、まずここと ship_city.sh:139 を見比べて門が消えていないか確かめる。
EMPTY_OSM_MESSAGE = '空のファイル'


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
        env=e.run_env, capture_output=True, text=True, timeout=60)


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


def test_extract_gate_fails_when_meshes_is_not_a_number(env):
    """meshes が数字でなければ落ちる。

    上の 1 件は python3 が非 0 で終わる経路しか通らないので、
    終了コードの判定だけの実装でも通ってしまう。
    JSON として正しく python3 も成功するが値が数字でない場合は、
    need_int でしか捕まらない。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":"たくさん","raw_bytes":5}\'')

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


def test_extract_gate_fails_when_meshes_is_zero(env):
    """meshes が 0 なら、.gml の数と一致していても取り出しの門で落ちる。

    件数の一致だけを見る門は 0 == 0 を素通りさせる。extract_city.py は
    udx/bldg/ で始まる .gml だけを拾うので、zip の内部配置が想定と違う版
    では members が空になり、meshes: 0 で正常終了してしまう。shipped.txt に
    成功として記録されると、ship_all.sh がその都市を以後永久に飛ばす。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'echo \'{"city_code":"30406","meshes":0,"raw_bytes":0}\'')

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    assert not env.shipped.exists()
    assert '.gml が 1 つも無い' in r.stdout


def test_extract_gate_passes_when_meshes_is_exactly_one(env):
    """meshes が 1 なら、下限の門はここでは落ちない。

    下限を 2 以上と書き違えた実装が紛れ込んでいないか、境界値で確かめる。
    上のテストの 0 と対にして、下限がちょうど 1 であることを固定する。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":1,"raw_bytes":5}\'')

    r = _run(env)

    assert r.returncode != 10, r.stdout + r.stderr


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


def _transfer_copies_for_real(e, remote_root):
    """SHIP_PATH を書き込める tmp のディレクトリに差し替え、
    偽 rsync に $WORK から実際にコピーさせ、偽 ssh にその場所を
    find させる。ssh の宛先ホストは stubhost のまま変えない。
    """
    remote_root.mkdir()
    ship_env2 = e.tmp / 'ship_copy.env'
    ship_env2.write_text(
        (e.tmp / 'ship.env').read_text().replace(
            'SHIP_PATH="/stub/import"', 'SHIP_PATH="%s"' % remote_root))
    e.run_env['SHIP_ENV'] = str(ship_env2)
    _stub(e.bin, 'rsync',
          'argv=("$@")\n'
          'n=${#argv[@]}\n'
          'src="${argv[$((n-2))]}"\n'
          'dst="${argv[$((n-1))]}"\n'
          'dstpath="${dst#*:}"\n'
          'mkdir -p "$dstpath"\n'
          'cp "$src"*.osm "$dstpath"/ 2>/dev/null\n'
          'cp "$src"manifest.txt "$dstpath"/\n'
          'exit 0')
    _stub(e.bin, 'ssh',
          'cmd="$2"\n'
          'bash -c "$cmd"')


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


def test_convert_gate_fails_when_osm_count_is_less_than_gml_count(env):
    """.osm の数が .gml より少なければ、変換の門で落ちる。

    java が exit 0 で終わりつつ一部の .gml だけ変換して残りを書き出さない
    失敗を模す。終了コードだけを見る実装ではこの不一致に気づけない。
    """
    _good_extract(env, n=3)
    _stub(env.bin, 'java',
          'printf "<osm><node/></osm>" > 53394500_bldg_6697_op.osm\n'
          'printf "<osm><node/></osm>" > 53394501_bldg_6697_op.osm\n'
          'exit 0')
    _good_transfer(env)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr


def test_convert_gate_fails_on_empty_osm(env):
    """空の .osm ファイルがあれば、専用のメッセージで変換の門に落ちる。

    JVM が書き出しの途中で落ちると、0 バイトの .osm が 1 個として
    数えられてしまう。件数の一致だけでは切り詰めも空ファイルも検出できない。
    空ファイルの検査 (138-140 行) を削除しても、閉じタグの検査が
    同じ exit 11 を返してしまい終了コードだけでは区別できない (brief 実測)。
    メッセージまで確かめて、この検査自体が生きていることを固定する。
    """
    _good_extract(env, n=2)
    _stub(env.bin, 'java',
          'printf "<osm><node/></osm>" > 53394500_bldg_6697_op.osm\n'
          'printf "" > 53394501_bldg_6697_op.osm\n'
          'exit 0')
    _good_transfer(env)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr
    assert EMPTY_OSM_MESSAGE in r.stdout, r.stdout


def test_disk_gate_fails_when_free_space_is_below_the_minimum(env):
    """空き容量が DISK_MIN_KB を下回れば、ディスクの門で exit 2 になる。

    fixture の ship.env は常に DISK_MIN_KB=0 なので、この門はこれまで
    一度も実際の df の値と比べられていなかった。現実的にあり得ない
    下限を設定して、比較そのものが生きていることを固定する。
    """
    ship_tight = env.tmp / 'ship_tight.env'
    ship_tight.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'DISK_MIN_KB=0\n', 'DISK_MIN_KB=999999999999\n'))
    env.run_env['SHIP_ENV'] = str(ship_tight)

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr
    # 「空き容量を読めない」ときも exit 2 なので、終了コードだけでは
    # 下限との比較そのものを固定できない。メッセージまで確かめる。
    assert 'ディスクが足りない' in r.stdout, r.stdout


def test_bail_moves_the_failed_work_dir_aside(env):
    """落ちたとき、作業ディレクトリを消さずに <citycode>.failed.* へ退避する。

    残したままだと、次の実行が前回の .gml や .osm を数えて誤って通ってしまう。
    """
    _stub(env.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'echo \'{"city_code":"30406","meshes":0,"raw_bytes":0}\'')

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    assert not (env.work_root / '30406').exists()
    failed_dirs = list(env.work_root.glob('30406.failed.*'))
    assert len(failed_dirs) == 1, failed_dirs


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


def test_transfer_gate_fails_when_ssh_succeeds_with_junk_output(env):
    """ssh が成功しても、出力が数字でなければ落ちる。

    上の 1 件は ssh が非 0 で終わる経路しか通らないので、
    終了コードの判定だけの実装でも通ってしまう。
    リモートの find がエラーを吐いて標準出力に紛れる形は need_int でしか捕まらない。
    """
    _good_extract(env, n=2)
    _good_java(env)
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh', 'echo "find: No such file or directory"\nexit 0')

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


def test_transfer_invokes_rsync_and_ssh_with_expected_host_and_flags(env):
    """rsync と ssh に渡る引数を実測する。

    これまでの偽 rsync/ssh は何を渡されても exit 0 しか返さなかったので、
    宛先を wronghost:/wrong/path/ に変えても、--include/--exclude を
    全部消しても、ssh の問い合わせ先を別ホストにしても素通りしていた
    (brief 実測)。--delete を外す変異も同様に素通りしていた。
    --exclude='*' があっても --delete は転送先の余分な .osm や
    manifest.txt を消す役目を持ち、メッシュ数が前回より減った都市を
    送り直したときに古い .osm が残るのを防ぐ。偽 rsync は実コピーを
    しないのでコピー側のテストでも捕まらない。ここでは受け取った
    引数をファイルへ記録させて、宛先の形と主要なフラグ、ssh の
    問い合わせ先を直接確かめる。
    """
    _good_extract(env, n=2)
    _good_java(env)
    rsync_args = env.tmp / 'rsync_args.txt'
    ssh_args = env.tmp / 'ssh_args.txt'
    _stub(env.bin, 'rsync',
          'printf \'%%s\\n\' "$@" > "%s"\n'
          'exit 0' % rsync_args)
    _stub(env.bin, 'ssh',
          'printf \'%%s\\n\' "$@" > "%s"\n'
          'echo 2' % ssh_args)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    rsync_argv = rsync_args.read_text().splitlines()
    assert '--delete' in rsync_argv, rsync_argv
    assert "--include=*.osm" in rsync_argv, rsync_argv
    assert "--include=manifest.txt" in rsync_argv, rsync_argv
    assert "--exclude=*" in rsync_argv, rsync_argv
    assert rsync_argv[-1] == 'stubhost:/stub/import/.incoming/30406/', rsync_argv
    ssh_argv = ssh_args.read_text().splitlines()
    assert ssh_argv[0] == 'stubhost', ssh_argv


def test_transfer_actually_copies_files_and_manifest_matches_the_osm_count(env):
    """偽 rsync に実際にコピーさせ、manifest.txt の中身を確かめる。

    宛先を wronghost:/wrong/path/ に変える変異も、manifest.txt に
    99 のような誤った数を書く変異も、コピー先で実際に検査しないと
    気づけない (brief 実測)。SHIP_PATH を書き込める tmp のディレクトリに
    差し替え、偽 rsync に $WORK から実際にコピーさせたうえで、
    コピー先の manifest.txt が .osm の実枚数と一致することを確かめる。
    """
    _transfer_copies_for_real(env, remote_root=env.tmp / 'remote')
    _good_extract(env, n=2)
    _good_java(env)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    dst = env.tmp / 'remote' / '.incoming' / '30406'
    assert sorted(p.name for p in dst.glob('*.osm')) == [
        '53394500_bldg_6697_op.osm', '53394501_bldg_6697_op.osm']
    assert (dst / 'manifest.txt').read_text().strip() == '2'


def test_transfer_manifest_matches_a_different_osm_count(env):
    """manifest.txt の数値が .osm の実枚数から来ている。定数ではない。

    上の 1 件だけだと、manifest.txt へ常に '2' を書く実装でも通ってしまう。
    5 メッシュで一通り流して区別する。
    """
    _transfer_copies_for_real(env, remote_root=env.tmp / 'remote')
    _good_extract(env, n=5)
    _good_java(env)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    dst = env.tmp / 'remote' / '.incoming' / '30406'
    assert len(list(dst.glob('*.osm'))) == 5
    assert (dst / 'manifest.txt').read_text().strip() == '5'


def test_missing_conversion_json_exits_1_before_running_anything(env):
    """CONVERSION_JSON の実体が無ければ、何も走らせずに exit 1 で止まる。

    ship.env.example の既定は /path/to/citygml-osm/conversion.json という
    プレースホルダなので、書き換え漏れが起きる。: "${CONVERSION_JSON:?...}"
    が保証するのは変数が設定されていることだけで実体の有無ではないので、
    未検査だと変換の cp まで気づけない。
    """
    missing = env.tmp / 'does_not_exist.json'
    ship_bad = env.tmp / 'ship_missing_conversion.env'
    ship_bad.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'CONVERSION_JSON="%s"' % (env.tmp / 'conversion.json'),
            'CONVERSION_JSON="%s"' % missing))
    env.run_env['SHIP_ENV'] = str(ship_bad)

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_missing_citygml_osm_jar_exits_1_before_running_anything(env):
    """CITYGML_OSM_JAR の実体が無ければ、何も走らせずに exit 1 で止まる。

    CONVERSION_JSON と同じ理由で、未設定かどうかしか見ていないと
    書き換え漏れに java の実行まで気づけない。
    """
    missing = env.tmp / 'does_not_exist.jar'
    ship_bad = env.tmp / 'ship_missing_jar.env'
    ship_bad.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'CITYGML_OSM_JAR="%s"' % (env.tmp / 'fake.jar'),
            'CITYGML_OSM_JAR="%s"' % missing))
    env.run_env['SHIP_ENV'] = str(ship_bad)

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_disk_min_kb_not_a_number_exits_1_before_the_disk_gate(env):
    """DISK_MIN_KB が数字でなければ、比較へ進まず exit 1 で止まる。

    [ "$AVAIL" -lt "$DISK_MIN_KB" ] は DISK_MIN_KB が数字でないと
    integer expression expected でエラー終了し、if がそれを偽として扱う。
    ディスクの門が「設定を書き損じたときに限って」消えるので、起動時に
    need_int で検査して落とす。ship.env に DISK_MIN_KB="5GB" と書く
    書き損じを模す。
    """
    ship_bad = env.tmp / 'ship_bad_disk.env'
    ship_bad.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'DISK_MIN_KB=0\n', 'DISK_MIN_KB=5GB\n'))
    env.run_env['SHIP_ENV'] = str(ship_bad)

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_work_root_mkdir_failure_exits_3_not_extract_failure(env):
    """WORK_ROOT を作れないときは exit 3 で止まり、取り出しの失敗 (10) に化けない。

    WORK_ROOT と同名の通常ファイルがあると mkdir -p は失敗する。結果を
    見ずに進むと、この先の disk_kb や extract_stub がその場所へ書けずに
    失敗し、「取り出しの失敗」に見せかけてしまう。ship_all.sh が同じ
    mkdir の失敗に付けた exit 3 と揃える。
    """
    blocked_root = env.tmp / 'blocked_work_root'
    blocked_root.write_text('mkdir -p を邪魔する通常ファイル\n')
    ship_blocked = env.tmp / 'ship_blocked_root.env'
    ship_blocked.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'WORK_ROOT="%s"' % env.work_root, 'WORK_ROOT="%s"' % blocked_root))
    env.run_env['SHIP_ENV'] = str(ship_blocked)

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_work_dir_creation_failure_exits_3(env):
    """作業ディレクトリを作れない (退避も含む) ときは exit 3 で止まる。

    WORK_ROOT を書き込み不可にすると、再試行のための mv や新規の mkdir が
    失敗する。結果を見ずに進むと、前回の .gml や .osm を抱えたまま先へ
    進む形に戻ってしまう。bail が退避で使う mv とは別物なので、ここでは
    WORK_ROOT 自体の権限で再現する。
    """
    os.chmod(env.work_root, 0o555)
    try:
        r = _run(env)
    finally:
        os.chmod(env.work_root, 0o755)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_convert_gate_fails_when_conversion_json_cannot_be_copied(env):
    """conversion.json の複製に失敗すれば、変換の門で落ちる。

    これまでの cp "$CONVERSION_JSON" "$WORK/conversion.json" は結果を
    見ていなかったので、複製に失敗しても変換から記録まで全部通り、
    既定の設定のまま走った変換器の出力が shipped.txt に成功として
    記録されていた (brief 実測)。cp を失敗する偽物に差し替えて確かめる。
    """
    _good_extract(env, n=2)
    _stub(env.bin, 'cp', 'exit 1')
    _good_java(env)  # 呼ばれない想定。もし呼ばれたら bail が抜けている
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 11, r.stdout + r.stderr
    assert 'conversion.json を複製できない' in r.stdout, r.stdout
    assert not env.shipped.exists()


def test_convert_invokes_java_with_the_jar_and_records_conversion_json(env):
    """java に渡る引数と、実行時の cwd に conversion.json があることを実測する。

    これまでの偽 java は引数を一切見ずに *.gml から *.osm を書くだけ
    だったので、-jar を消しても、1st を落としても、conversion.json の
    複製を消しても緑のままだった (brief 実測)。既にある「偽 rsync に
    引数を記録させる」テストと同じ形で、偽 java に argv と cwd の
    conversion.json の有無を記録させる。
    """
    _good_extract(env, n=2)
    java_args = env.tmp / 'java_args.txt'
    conversion_seen = env.tmp / 'conversion_seen.txt'
    body = (
        "printf '%s\\n' \"$@\" > '" + str(java_args) + "'\n"
        "if [ -f conversion.json ]; then\n"
        "  echo yes > '" + str(conversion_seen) + "'\n"
        "else\n"
        "  echo no > '" + str(conversion_seen) + "'\n"
        "fi\n"
        "for f in *.gml; do\n"
        "  printf \"<osm><node/></osm>\" > \"${f%.gml}.osm\"\n"
        "done\n"
        "exit 0"
    )
    _stub(env.bin, 'java', body)
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    java_argv = java_args.read_text().splitlines()
    assert '-jar' in java_argv, java_argv
    assert str(env.tmp / 'fake.jar') in java_argv, java_argv
    assert java_argv[-1] == '1st', java_argv
    assert conversion_seen.read_text().strip() == 'yes'


def test_ship_env_example_ship_path_does_not_expand_the_local_home():
    """ship.env.example の SHIP_PATH は手元の $HOME に展開されない形にする。

    SHIP_PATH は転送先 (サーバ) のパスとして使われる。ship.env は手元の
    bash が source するので $HOME を使うと手元のホームに展開されてしまい、
    「SHIP_PATH はサーバの PLATEAU_IMPORT_DIR と同じ絶対パスにする」という
    同じファイルの説明と矛盾する。
    """
    body = SHIP_ENV_EXAMPLE.read_text()
    m = re.search(r'^SHIP_PATH="([^"]*)"$', body, re.M)
    assert m, 'SHIP_PATH の既定値が見つからない'
    assert 'HOME' not in m.group(1), m.group(1)


def test_ship_env_example_ship_path_matches_the_other_placeholders():
    """SHIP_PATH の既定値が、他の項目と同じ /path/to/... の形になっている。

    JAVA_BIN や WORK_ROOT など、このファイルの他の絶対パスは全て
    /path/to/... のプレースホルダで揃っている。SHIP_PATH だけ実行時に
    展開される値のままだと、この形式に一致しない。
    """
    body = SHIP_ENV_EXAMPLE.read_text()
    m = re.search(r'^SHIP_PATH="([^"]*)"$', body, re.M)
    assert m, 'SHIP_PATH の既定値が見つからない'
    assert m.group(1).startswith('/path/to/'), m.group(1)


def _retained(e, name):
    """退避ディレクトリを 1 件作る。中身も置いて du が 0 にならないようにする。"""
    d = e.work_root / name
    d.mkdir()
    (d / 'dummy.osm').write_text('<osm/>')
    return d


def _fails_at_extract(e):
    """取り出しで落ちる偽 extract。作業ディレクトリは作るので退避が起きる。"""
    _stub(e.bin, 'extract_stub',
          'mkdir -p "$2"\n'
          'printf "<x/>" > "$2/53394500_bldg_6697_op.gml"\n'
          'echo \'{"city_code":"30406","meshes":2,"raw_bytes":5}\'')


def test_retained_dirs_are_pruned_to_the_cap(env):
    """上限 3、退避 5 件のところへ 1 件増えると、古い 3 件が消えて 3 件残る。

    掃除が走るのは退避を作った瞬間だけなので、失敗経路を通す。
    """
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000',
               '20260813-000000', '20260814-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 3, kept
    # 新しい 2 件と、いま作られた 30406 の退避が残る
    assert '11111.stale.20260813-000000' in kept, kept
    assert '11111.stale.20260814-000000' in kept, kept
    assert any(n.startswith('30406.failed.') for n in kept), kept
    assert '11111.stale.20260810-000000' not in kept, kept


def test_prune_cap_of_one_keeps_only_the_newest(env):
    """上限 1 のとき、いま作った 1 件だけが残る。

    件数を 2 種類 (3 と 1) 使う。上限 3 に固定した実装を通さないため。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=1\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 1, kept
    assert kept[0].startswith('30406.failed.'), kept


def test_prune_disabled_by_zero(env):
    """上限 0 では 1 件も消さない。3 件 + 新しい 1 件で 4 件残る。"""
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=0\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 4, kept


def test_success_does_not_prune(env):
    """成功した回は退避を作らないので、掃除も走らない。

    上限 1 で退避 3 件のまま成功させても消えない。
    「起動時に無条件で掃除する」実装をここで落とす。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=1\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _good_extract(env, n=2)
    _good_java(env)
    _good_transfer(env, remote_count=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert len(list(env.work_root.glob('*.stale.*'))) == 3


def test_prune_refuses_when_the_cap_is_not_a_number(env):
    """上限が数字でなければ、掃除を飛ばさずに止める。

    数値でないまま [ "$n" -le "$KEEP_RETAINED_DIRS" ] を評価すると
    integer expression expected でエラー終了し、if がそれを偽として扱う。
    掃除が黙って消えるので、ここは止める側でなければならない。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=three\n')

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert 'KEEP_RETAINED_DIRS' in r.stdout + r.stderr


def test_prune_cap_with_leading_zero_08_keeps_eight(env):
    """KEEP_RETAINED_DIRS=08 は 8 進ではなく 10 進の 8 として扱う。

    need_int は 08 を通すが、正規化なしで $((08 + 1)) を評価すると
    value too great for base で算術エラーになり、tail の引数が壊れて
    掃除が 1 件も走らない (退避 11 件全部が残ってしまう)。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=08\n')
    for d in range(10, 20):  # 10 件
        _retained(env, '11111.stale.202608%02d-000000' % d)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 8, kept


def test_prune_cap_with_leading_zero_010_keeps_ten(env):
    """KEEP_RETAINED_DIRS=010 は 8 進ではなく 10 進の 10 として扱う。

    正規化なしだと [ n -le 010 ] は 10 進の 10 と比べて掃除に入るのに、
    tail の切り出しに使う $((010 + 1)) は 8 進で 9 になり、上限が黙って
    2 件ずれて 8 件しか残らない (ちょうど 10 件残ることを見る)。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=010\n')
    for d in range(10, 23):  # 13 件
        _retained(env, '11111.stale.202608%02d-000000' % d)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 10, kept


def test_prune_orders_by_name_not_mtime(env):
    """並べ替えは名前の末尾の日時で行う。

    mv はディレクトリの mtime を退避した時刻に更新しないので、
    mtime 順に消すと実際の退避の順と食い違う。
    名前と mtime の順を逆にしておき、名前順で消えることを見る。

    上限 2、退避 2 件 + 新しい 1 件 = 3 件なので、消えるのは 1 件だけ。
    名前順なら 20260810 が、mtime 順なら 20260812 が消える。
    """
    import os
    import time
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=2\n')
    old_name = _retained(env, '11111.stale.20260810-000000')
    new_name = _retained(env, '11111.stale.20260812-000000')
    now = time.time()
    os.utime(new_name, (now - 100000, now - 100000))  # 名前は新しいが mtime は古い
    os.utime(old_name, (now, now))                    # 名前は古いが mtime は新しい
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert '11111.stale.20260812-000000' in kept, kept
    assert '11111.stale.20260810-000000' not in kept, kept


def test_prune_is_quiet_when_there_is_nothing_else_to_remove(env):
    """他に退避が無くてもエラーにならない。

    グロブが展開されないと "$WORK_ROOT"/*.failed.* のようなリテラルの
    文字列が for に渡る。ここは nullglob と直後の [ -d "$d" ] ガードの
    二重防御になっていて、どちらか一方だけを外しても崩れない
    (実測: [ -d ] だけ外す/nullglob だけ外す、どちらも 60 passed)。
    両方を同時に外したときだけ赤くなる (実測: 2 failed)。
    このテストが固定できるのは、両方が同時には失われないことだけであり、
    どちらか一方が守っている、という切り分けはできない。
    """
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert len(kept) == 1, kept


def test_non_timestamp_named_retained_dir_is_never_pruned_and_never_counted(env):
    """日時でない名前の退避は掃除の対象から外れ、上限の枠も奪わない。

    ts="${d##*.}" が末尾を無検査で取ると、11111.stale.backup のような
    手作りの名前は sort -r で数字より前に来て「最新」扱いされ、
    永久に残ったうえで上限の枠を占有してしまう。上限 1 のところに
    手作りの名前を 1 件、日時形式を 2 件置く。手作りの名前は消えず、
    かつ日時形式の枠 (今回の失敗退避 1 件) を奪わないことを確かめる。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=1\n')
    _retained(env, '11111.stale.backup')
    for ts in ['20260810-000000', '20260811-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    kept = sorted(p.name for p in env.work_root.glob('*.*.*'))
    assert '11111.stale.backup' in kept, kept
    assert any(n.startswith('30406.failed.') for n in kept), kept
    assert '11111.stale.20260810-000000' not in kept, kept
    assert '11111.stale.20260811-000000' not in kept, kept
    # 手作りの名前 1 件 + 日時形式の中で最新 (今回の失敗退避) 1 件
    assert len(kept) == 2, kept
    assert '日時の形でない退避が 1 件ある' in r.stdout, r.stdout


def test_bail_does_not_prune_when_the_evacuation_mv_fails(env):
    """退避の mv 自体が失敗したときは、古い退避を消さない。

    これまでの bail() は mv の結果を見ずに「退避した」と言って
    prune_retained を呼んでいた。掃除を足したことで「退避に失敗したのに
    古い退避が消える」経路が新しくできた。

    以前は WORK_ROOT を chmod 555 にして mv を失敗させていたが、同じ
    chmod が prune_retained 内の rm -rf も失敗させてしまい、「3 件残る」の
    表明が修正の有無によらず常に真になっていた (prune_retained を if の外に
    出す変異でも 41 passed で全緑になることを実測)。ここでは PATH に mv の
    偽物を置いて mv だけを失敗させ、rm -rf は生きたままにする。
    """
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=1\n')
    for ts in ['20260810-000000', '20260811-000000', '20260812-000000']:
        _retained(env, '11111.stale.' + ts)
    _fails_at_extract(env)
    _stub(env.bin, 'mv', 'exit 1')

    r = _run(env)

    assert r.returncode == 10, r.stdout + r.stderr
    assert len(list(env.work_root.glob('11111.stale.*'))) == 3
    assert '退避できない' in r.stdout + r.stderr, r.stdout + r.stderr


def test_transfer_targets_the_incoming_directory(env):
    """rsync の宛先が .incoming/<都市>/ を指す。"""
    _good_extract(env, n=2)
    _good_java(env)
    seen = env.tmp / 'rsync_args.txt'
    _stub(env.bin, 'rsync', 'echo "$@" >> %s\nexit 0' % seen)
    _stub(env.bin, 'ssh', 'echo "$@" >> %s.ssh\necho 2' % seen)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert 'stubhost:/stub/import/.incoming/30406/' in seen.read_text()


def test_transfer_creates_the_incoming_directory_first(env):
    """rsync の前に mkdir -p が走る。

    rsync は宛先の最後の 1 段しか作らない。.incoming と <都市> の
    2 段を作る必要があるので、先に作っておかないと転送が落ちる。
    """
    _good_extract(env, n=2)
    _good_java(env)
    order = env.tmp / 'order.txt'
    _stub(env.bin, 'ssh', 'echo "ssh $2" >> %s\ncase "$2" in *mkdir*) ;; *) echo 2 ;; esac' % order)
    _stub(env.bin, 'rsync', 'echo rsync >> %s\nexit 0' % order)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    lines = order.read_text().splitlines()
    mkdir_at = next(i for i, l in enumerate(lines) if 'mkdir' in l)
    rsync_at = next(i for i, l in enumerate(lines) if l == 'rsync')
    assert mkdir_at < rsync_at, lines
    assert '/stub/import/.incoming/30406' in lines[mkdir_at]


def test_transfer_fails_when_the_destination_cannot_be_created(env):
    """転送先を作れなければ、転送の門で落ちる。"""
    _good_extract(env, n=2)
    _good_java(env)
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh',
          'case "$2" in *mkdir*) echo "mkdir: 権限がない" >&2; exit 1 ;; esac\necho 2')

    r = _run(env)

    assert r.returncode == 12, r.stdout + r.stderr
    assert not env.shipped.exists()


def test_remote_count_is_read_from_incoming(env):
    """枚数を数える先も .incoming/<都市> になる。

    宛先だけ変えて数える先を元のままにすると、常に 0 件を数えて
    転送の門が落ち続ける。逆に数える先だけ変えても素通りする。
    """
    _good_extract(env, n=2)
    _good_java(env)
    seen = env.tmp / 'ssh_args.txt'
    _stub(env.bin, 'rsync', 'exit 0')
    _stub(env.bin, 'ssh',
          'echo "$2" >> %s\ncase "$2" in *mkdir*) ;; *) echo 2 ;; esac' % seen)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    find_lines = [l for l in seen.read_text().splitlines() if 'find' in l]
    assert find_lines, seen.read_text()
    assert '/stub/import/.incoming/30406' in find_lines[0]


def test_end_to_end_transfer_lands_in_incoming(env):
    """偽 rsync に実際にコピーさせ、.incoming/<都市>/ に届くことを見る。"""
    _good_extract(env, n=2)
    _good_java(env)
    remote = env.tmp / 'remote'
    _transfer_copies_for_real(env, remote)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    landed = remote / '.incoming' / '30406'
    assert landed.is_dir(), sorted(p.name for p in remote.rglob('*'))
    assert len(list(landed.glob('*.osm'))) == 2
    assert (landed / 'manifest.txt').read_text().strip() == '2'
