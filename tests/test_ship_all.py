"""ship_all.sh の件数確認と再開を固定する。"""

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SHIP_ALL = REPO / 'scripts' / 'reimport' / 'ship_all.sh'


def _stub(bin_dir: Path, name: str, body: str):
    p = bin_dir / name
    p.write_text('#!/usr/bin/env bash\n' + body + '\n')
    p.chmod(0o755)
    return p


@pytest.fixture
def env(tmp_path):
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    plan = tmp_path / 'plan.csv'
    shipped = tmp_path / 'shipped.txt'
    called = tmp_path / 'called.txt'

    _stub(bin_dir, 'ship_city_stub',
          'echo "$1" >> "%s"\n'
          'echo "$1 3" >> "%s"\n'
          'exit 0' % (called, shipped))
    _stub(bin_dir, 'rsync', 'exit 0')
    _stub(bin_dir, 'ssh', 'exit 0')

    ship_env = tmp_path / 'ship.env'
    ship_env.write_text(
        'SHIP_CITY_CMD="%s/ship_city_stub"\n'
        'PLAN_CSV="%s"\n'
        'SHIPPED_TXT="%s"\n'
        'SHIP_HOST="stubhost"\n'
        'SHIP_PATH="/stub/import"\n'
        'WORK_ROOT="%s"\n'
        'DISK_MIN_KB=0\n'
        'EXPECTED_CITIES=3\n'
        % (bin_dir, plan, shipped, tmp_path)
    )

    run_env = dict(os.environ)
    run_env['PATH'] = '%s:%s' % (bin_dir, run_env['PATH'])
    run_env['SHIP_ENV'] = str(ship_env)

    class Env:
        pass

    e = Env()
    e.bin = bin_dir
    e.plan = plan
    e.shipped = shipped
    e.called = called
    e.run_env = run_env
    e.tmp = tmp_path
    return e


def _write_plan(e, codes):
    lines = ['city_code,package,year,citygml_v,bytes,url']
    for c in codes:
        lines.append('%s,plateau-%s-x-2025,2025,5,1,https://example.invalid/x.zip' % (c, c))
    e.plan.write_text('\n'.join(lines) + '\n')


def _run(e):
    return subprocess.run(['bash', str(SHIP_ALL)],
                          env=e.run_env, capture_output=True, text=True,
                          timeout=60)


def test_stops_when_plan_has_wrong_city_count(env):
    """計画の件数が EXPECTED_CITIES と違えば、1 都市も処理せずに止まる。

    147 件のまま流すと targets も 147 件になり、20 時間後の最終確認まで
    誰も気づかない。
    """
    _write_plan(env, ['30406', '43213'])   # 2 件。EXPECTED_CITIES は 3

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.called.exists()


def test_expected_cities_reads_from_config_not_hardcoded(env):
    """EXPECTED_CITIES は ship.env の値を読む。fixture の 3 に固定されていない。

    fixture が常に EXPECTED_CITIES=3 なので、比較を
    `[ "$CODES" -ne 3 ]` とリテラルに置き換えても他のテストは全部通って
    しまう (brief 実測)。ship.env で 2 に変えた計画が 2 都市でも
    通ることを確かめて、設定ファイルを実際に読んでいることを固定する。
    """
    _write_plan(env, ['13402', '30406'])  # 2 都市
    ship_two = env.tmp / 'ship_two.env'
    ship_two.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'EXPECTED_CITIES=3\n', 'EXPECTED_CITIES=2\n'))
    env.run_env['SHIP_ENV'] = str(ship_two)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406']


def test_processes_every_city_in_the_plan(env):
    _write_plan(env, ['13402', '30406', '43213'])

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']


def test_skips_cities_already_in_shipped(env):
    """shipped.txt にある都市は飛ばす。途中で止めても続きから走る。"""
    _write_plan(env, ['13402', '30406', '43213'])
    env.shipped.write_text('13402 8\n30406 103\n')

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['43213']


def test_one_failure_does_not_stop_the_rest(env):
    """1 都市が失敗しても続ける。終了コードは 1 になる。"""
    _write_plan(env, ['13402', '30406', '43213'])
    _stub(env.bin, 'ship_city_stub',
          'echo "$1" >> "%s"\n'
          'if [ "$1" = "30406" ]; then exit 11; fi\n'
          'echo "$1 3" >> "%s"\n'
          'exit 0' % (env.called, env.shipped))

    r = _run(env)

    assert r.returncode == 1, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']
    assert '30406' in r.stdout


def test_disk_shortage_aborts_the_whole_run(env):
    """空きが下限を割ったら、その場で全体を止める。

    このテストが無いと、ディスクチェックを丸ごと消した実装でも通る。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    env.run_env['SHIP_ENV'] = str(env.tmp / 'ship_tight.env')
    (env.tmp / 'ship_tight.env').write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'DISK_MIN_KB=0', 'DISK_MIN_KB=999999999999'))

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr
    assert not env.called.exists()


def test_ship_city_disk_exit_aborts_the_whole_run(env):
    """ship_city.sh が exit 2 を返したら、次の都市へ進まずに止める。

    2 はディスク不足の合図で、他の失敗と混ぜてはいけない。
    このテストが無いと、exit 2 の分岐を消した実装でも通る。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    _stub(env.bin, 'ship_city_stub',
          'echo "$1" >> "%s"\n'
          'if [ "$1" = "30406" ]; then exit 2; fi\n'
          'echo "$1 3" >> "%s"\n'
          'exit 0' % (env.called, env.shipped))

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406']


def test_target_list_holds_only_city_codes(env):
    """サーバへ渡す一覧は都市コード 1 列だけにする。

    shipped.txt は <citycode> <osm数> の 2 列。第 2 段のバッチは行から
    空白を全部除くので、2 列のまま渡すと 43213 103 が 43213103 になり、
    その都市は永久に取り込まれない。
    """
    # ship.env の EXPECTED_CITIES は 3 (fixture 参照)。ブリーフ本文は 2 都市の
    # 計画だったが、それだと件数ゲートで exit 3 になり、この関数が確かめたい
    # 列の切り出しまで到達しない。他の新規テストと同じ 3 都市に揃える。
    _write_plan(env, ['13402', '30406', '43213'])

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    written = sorted(env.tmp.glob('reimport_targets_*.txt'))
    assert written, '一覧が作られていない'
    lines = [ln for ln in written[-1].read_text().splitlines() if ln]
    assert lines == ['13402', '30406', '43213'], lines


def test_resume_does_not_skip_on_a_prefix_match(env):
    """都市コードの前方一致で誤って飛ばさない。

    照合が行頭と末尾の空白で挟まれていないと、1340 が 13402 に一致する。
    """
    # 同上: EXPECTED_CITIES=3 に合わせて 3 都市の計画にする。
    _write_plan(env, ['13402', '30406', '43213'])
    env.shipped.write_text('1340 8\n')

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']


def test_creates_work_root_before_checking_disk(env):
    """WORK_ROOT が無くても、作ってから df を掛けるので初回で落ちない。

    mkdir -p が無いと、初回の disk_kb "$WORK_ROOT" は「ディレクトリが無い」
    ことを「空き容量を読めない」に見せかけて exit 2 で落ちる。exit 2 は
    ディスク不足の予約番号なので、運用者はディスクを疑って調べ始めるが、
    実際にはディレクトリが無いだけ。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    missing_root = env.tmp / 'fresh_work_root'
    assert not missing_root.exists()
    env.run_env['SHIP_ENV'] = str(env.tmp / 'ship_missing_root.env')
    (env.tmp / 'ship_missing_root.env').write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'WORK_ROOT="%s"' % env.tmp, 'WORK_ROOT="%s"' % missing_root))

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']
    assert missing_root.is_dir()


def test_mkdir_failure_aborts_with_exit_3_not_disk_exit_2(env):
    """WORK_ROOT を作れないときは exit 3 で止まり、exit 2 にはならない。

    WORK_ROOT と同名の通常ファイルがあると mkdir -p は失敗する。
    set -e が無いこのスクリプトでは、失敗を捕まえないと mkdir の失敗が
    無視され、後段の disk_kb "$WORK_ROOT" が exit 2 で落ちる。それは
    「ディレクトリを作れない」を「ディスク不足」に見せかける、
    同じ誤誘導を条件だけ変えて再現してしまう。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    blocked_root = env.tmp / 'blocked_work_root'
    blocked_root.write_text('mkdir -p を邪魔する通常ファイル\n')
    env.run_env['SHIP_ENV'] = str(env.tmp / 'ship_blocked_root.env')
    (env.tmp / 'ship_blocked_root.env').write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'WORK_ROOT="%s"' % env.tmp, 'WORK_ROOT="%s"' % blocked_root))

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.called.exists()


def test_target_list_transfer_uses_the_expected_destination(env):
    """一覧の転送元と転送先を実測する。

    fixture の偽 rsync は何を渡されても exit 0 を返すだけだったので、
    一覧の宛先を別ホストにする変異も、この行そのものを `true` に
    置き換える変異も素通りしていた (brief 実測)。宛先 (argv[-1]) だけを
    見ていると、送信元を shipped.txt そのものにすり替える変異
    (`rsync -az "$SHIPPED_TXT" "$SHIP_HOST:..."`) も宛先の形が同じなら
    通ってしまう。shipped.txt は 2 列 (都市コードと件数) なので、
    それをそのまま送ると第 2 段が `43213 103` を `43213103` として読み、
    その都市は永久に取り込まれない (ship_all.sh のコメント参照)。
    引数を記録させて、転送元 (argv[-2]) と転送先 (argv[-1]) の両方を
    確かめる。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    rsync_args = env.tmp / 'rsync_args.txt'
    _stub(env.bin, 'rsync',
          'printf \'%%s\\n\' "$@" >> "%s"\n'
          'exit 0' % rsync_args)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert rsync_args.exists(), '一覧の転送で rsync が呼ばれていない'
    argv = rsync_args.read_text().splitlines()
    assert argv, '一覧の転送で rsync に引数が渡っていない'
    assert re.match(
        r'^%s/reimport_targets_\d{8}-\d{6}\.txt$' % re.escape(str(env.tmp)),
        argv[-2]), argv
    assert re.match(
        r'^stubhost:/stub/import/\.\./reimport_targets_\d{8}-\d{6}\.txt$',
        argv[-1]), argv


def test_target_list_transfer_failure_aborts_with_exit_4(env):
    """一覧の転送が失敗したら exit 4 で止まる。

    fixture の偽 rsync は常に exit 0 を返すだけなので、失敗検出 (exit 4) を
    丸ごと削除しても他のテストは全部通っていた (brief 実測)。
    偽 rsync を呼び出し回数で数え、この 1 回きりの呼び出しを非 0 にして
    確かめる。

    このカウンタは現状では効いていない。
    fixture の SHIP_CITY_CMD (ship_city_stub) は rsync を呼ばない偽物なので、
    ship_all.sh 内で rsync が呼ばれるのは一覧の転送 1 回きりであり、
    この仕掛けは「無条件に exit 1 を返す偽物」と等価になっている。
    ship_city.sh 経由の rsync 呼び出しが同じ偽 rsync を共有するように
    なった場合の保険として、カウンタのまま残している。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    call_count = env.tmp / 'rsync_calls.txt'
    _stub(env.bin, 'rsync',
          'n=0\n'
          '[ -f "%s" ] && n=$(cat "%s")\n'
          'n=$((n + 1))\n'
          'echo "$n" > "%s"\n'
          'if [ "$n" -eq 1 ]; then exit 1; fi\n'
          'exit 0' % (call_count, call_count, call_count))

    r = _run(env)

    assert r.returncode == 4, r.stdout + r.stderr
    assert env.called.read_text().split() == ['13402', '30406', '43213']
    assert '一覧の転送が exit' in r.stdout


def test_missing_plan_csv_exits_3_with_a_distinct_message(env):
    """PLAN_CSV が無ければ、件数の門とは別のメッセージで exit 3 になる。

    PLAN_CSV が無いと tail が失敗するが、その出力を受ける grep -c . は
    入力 0 行でも exit 0 で 0 を返す。件数の門にそのまま落ちて
    「計画の件数が合わない。build_download_plan.py で足りない都市を足す」
    と出ると、運用者は CSV の中身を疑ってそちらを直しに行くが、
    実際にはパスが違うだけ (brief 実測)。
    env fixture は _write_plan を呼ばない限り PLAN_CSV の実体を作らない。
    """
    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert '計画のファイルが無い' in r.stdout, r.stdout
    assert '計画の件数が合わない' not in r.stdout, r.stdout
    assert not env.called.exists()


def test_shipped_txt_touch_failure_exits_3(env):
    """SHIPPED_TXT に書けなければ、1 都市も処理せずに exit 3 で止まる。

    touch が失敗すると、再開の飛ばし (grep -q "^$CITY ") が無言で無効に
    なり、最後の cut もリダイレクト先が無いまま TARGETS を空で作る。
    rsync 自体は空ファイルの転送に成功するので exit 4 にはならず、
    「一覧を置いた: (0 都市)」が正常終了に見えてしまう (brief 実測)。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    missing_parent = env.tmp / 'no_such_dir' / 'shipped.txt'
    ship_bad = env.tmp / 'ship_bad_shipped.env'
    ship_bad.write_text(
        (env.tmp / 'ship.env').read_text().replace(
            'SHIPPED_TXT="%s"' % env.shipped, 'SHIPPED_TXT="%s"' % missing_parent))
    env.run_env['SHIP_ENV'] = str(ship_bad)

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.called.exists()


def test_empty_target_list_after_all_cities_fail_exits_3(env):
    """全都市が失敗して SHIPPED_TXT が空のままなら、一覧を空で置かず exit 3 で止まる。

    touch 自体が成功しても、shipped.txt に一度も追記されなければ最後の
    cut は空を書き出す。rsync は空ファイルの転送に成功するので exit 4 には
    ならず、「一覧を置いた: (0 都市)」が正常終了に見えてしまう。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    _stub(env.bin, 'ship_city_stub',
          'echo "$1" >> "%s"\nexit 9' % env.called)

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert 'ABORT: 一覧が 0 都市' in r.stdout, r.stdout


def test_reports_retained_dirs_at_start(env):
    """起動時に退避の件数と合計サイズを出す。

    グロブが展開されないと "$WORK_ROOT"/*.failed.* のようなリテラルの
    文字列が for に渡るが、直後の [ -d "$d" ] ガードがそれを弾く。
    ここを実際に守っているのは nullglob ではなく、この [ -d ] ガードである。
    """
    _write_plan(env, ['11111', '22222', '33333'])
    for ts in ['20260810-000000', '20260811-000000']:
        d = env.tmp / ('11111.failed.' + ts)
        d.mkdir()
        (d / 'dummy.osm').write_text('<osm/>' * 100)

    r = _run(env)

    assert '退避 2 件' in r.stdout, r.stdout
    assert '合計' in r.stdout, r.stdout


def test_reports_zero_retained_dirs_without_failing(env):
    """退避が 0 件でも落ちない。

    グロブが展開されないと "$WORK_ROOT"/*.failed.* のようなリテラルの
    文字列が for に渡るが、直後の [ -d "$d" ] ガードがそれを弾く。
    ここを実際に守っているのは nullglob ではなく、この [ -d ] ガードである
    (このガードが無いと du が存在しないパスを見に行く)。
    ガードが外れると未展開のリテラルが件数や表示に混じるので、
    出力に '*' が現れないことも確かめる。
    """
    _write_plan(env, ['11111', '22222', '33333'])

    r = _run(env)

    assert '退避 0 件' in r.stdout, r.stdout
    assert '*' not in r.stdout, r.stdout


def test_keep_retained_dirs_not_a_number_exits_3_before_any_city(env):
    """KEEP_RETAINED_DIRS が数字でなければ、1 都市も処理せずに exit 3 で止まる。

    ship_all.sh がこれを検査しないと、自分の門を素通りして走り始め、
    1 都市目の ship_city.sh がそこで exit 1 して落ちる。ship_all.sh は
    それを「1 都市の失敗」として積んで次の都市へ進むので、148 都市すべてが
    同じ理由で落ちてから (brief 実測) ようやく設定の誤りに気づくことになる。
    """
    _write_plan(env, ['13402', '30406', '43213'])
    ship_env = env.tmp / 'ship.env'
    ship_env.write_text(ship_env.read_text() + '\nKEEP_RETAINED_DIRS=three\n')

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert 'KEEP_RETAINED_DIRS' in r.stdout, r.stdout
    assert not env.called.exists()
