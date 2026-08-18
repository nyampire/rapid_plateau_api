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
    assert rsync_argv[-1] == 'stubhost:/stub/import/30406/', rsync_argv
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
    dst = env.tmp / 'remote' / '30406'
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
    dst = env.tmp / 'remote' / '30406'
    assert len(list(dst.glob('*.osm'))) == 5
    assert (dst / 'manifest.txt').read_text().strip() == '5'


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
