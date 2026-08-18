"""ship_all.sh の件数確認と再開を固定する。"""

import os
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
                          env=e.run_env, capture_output=True, text=True)


def test_stops_when_plan_has_wrong_city_count(env):
    """計画の件数が EXPECTED_CITIES と違えば、1 都市も処理せずに止まる。

    147 件のまま流すと targets も 147 件になり、20 時間後の最終確認まで
    誰も気づかない。
    """
    _write_plan(env, ['30406', '43213'])   # 2 件。EXPECTED_CITIES は 3

    r = _run(env)

    assert r.returncode == 3, r.stdout + r.stderr
    assert not env.called.exists()


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
