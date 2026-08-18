"""reimport_one.sh の分岐を固定する。

シェルの分岐は実行時に気づきにくい。取り込み器は偽物に差し替える。
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
ONE = REPO / 'deploy' / 'reimport_one.sh'

# manifest.txt 存在検査 (reimport_one.sh:100) が出す文言。
# この文言を変えるとテストは赤くなる。
# 赤くなったら、まずここと reimport_one.sh:100 を見比べて門が消えていないか確かめる。
MISSING_MANIFEST_MESSAGE = 'manifest.txt が無い'


@pytest.fixture
def env(tmp_path):
    app = tmp_path / 'app'
    app.mkdir()
    import_dir = tmp_path / 'import'
    import_dir.mkdir()
    logs = tmp_path / 'logs'
    env_file = tmp_path / 'env'
    env_file.write_text('DATABASE_URL=postgresql://stub/stub\n')

    # 取り込み器の偽物。呼ばれた引数を記録して成功する。
    called = tmp_path / 'called.txt'
    stub = app / 'plateau_importer2postgis.py'
    stub.write_text(
        'import sys, pathlib\n'
        'pathlib.Path(%r).write_text(" ".join(sys.argv[1:]))\n'
        'sys.exit(0)\n' % str(called)
    )

    run_env = dict(os.environ)
    run_env.update({
        'PLATEAU_APP_DIR': str(app),
        'PLATEAU_ENV_FILE': str(env_file),
        'PLATEAU_IMPORT_DIR': str(import_dir),
        'PLATEAU_LOG_DIR': str(logs),
        'PYTHON_BIN': 'python3',
        'THRESHOLD_KB': '0',
    })

    class Env:
        pass

    e = Env()
    e.app = app
    e.import_dir = import_dir
    e.called = called
    e.stub = stub
    e.run_env = run_env
    e.tmp = tmp_path
    return e


def _city(e, code='30406', osm=2, manifest=None):
    d = e.import_dir / code
    d.mkdir()
    for i in range(osm):
        (d / ('5339450%d_bldg_6697_op.osm' % i)).write_text('<osm/>')
    (d / 'manifest.txt').write_text(str(osm if manifest is None else manifest) + '\n')
    return d


def _run(e, city='30406'):
    return subprocess.run(['bash', str(ONE), city],
                          env=e.run_env, capture_output=True, text=True,
                          timeout=60)


def test_missing_input_exits_with_dedicated_code(env):
    """入力ディレクトリが無ければ専用の終了コードで落ちる。

    取り込み器は mkdir(exist_ok=True) を parents 無しで呼ぶので、
    そのまま渡すと生のトレースバックが出る。
    """
    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_count_mismatch_exits_with_dedicated_code(env):
    """.osm の枚数が manifest.txt と違えば落ちる。

    「1 つ以上」では、枚数が欠けた都市がそのまま取り込まれる。
    """
    _city(env, osm=2, manifest=3)

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_unreadable_manifest_exits_with_dedicated_code(env):
    """manifest.txt が数字でなければ落ちる。

    空や壊れた manifest をそのまま比較すると integer expression expected で
    エラー終了し、if がそれを偽として扱う。壊れた manifest を弾くのが
    この門の目的なので、素通りさせると門を置いた意味が無くなる。
    """
    _city(env, osm=2, manifest='')

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_passes_citycode_and_no_zip_explicitly(env):
    """--citycode と --no-zip を明示して渡す。推定に頼らない。"""
    _city(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    args = env.called.read_text()
    assert '--citycode 30406' in args
    assert '--no-zip' in args


def test_missing_manifest_exits_with_dedicated_code(env):
    """manifest.txt が無ければ、専用の終了コードで落ちる。

    manifest.txt の存在検査そのものを削除しても、後段の
    `tr -d '[:space:]' < "$SRC/manifest.txt"` が need_int で結局
    弾かれるので終了コードは変わらず、10 件すべてが緑のままだった
    (brief 実測)。存在検査だけが出す「manifest.txt が無い」という
    メッセージを確かめて、この検査自体が生きていることを固定する。
    """
    d = _city(env, osm=2)
    (d / 'manifest.txt').unlink()

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr
    assert MISSING_MANIFEST_MESSAGE in r.stdout, r.stdout


def test_passes_data_dir_and_postgres_url(env):
    """--data-dir と --postgres-url を取り込み器に渡す。

    このテストは --citycode と --no-zip しか見ていなかったので、
    --postgres-url "$DATABASE_URL" を丸ごと削除しても気づけなかった
    (brief 実測)。DATABASE_URL は設定ファイル (PLATEAU_ENV_FILE) から
    読むので、この行は設定の読み込みが効いていることを示す唯一の観測点になる。
    """
    d = _city(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    args = env.called.read_text()
    assert '--data-dir %s' % d in args, args
    assert '--postgres-url postgresql://stub/stub' in args, args


def test_postgres_url_comes_from_the_env_file_not_a_constant(env):
    """--postgres-url の値が設定ファイルから来ている。定数ではない。

    上の 1 件だけだと、同じ文字列を決め打ちで渡す実装でも通ってしまう。
    別の DATABASE_URL を持つ設定ファイルに差し替えて区別する。
    """
    _city(env, osm=2)
    other_env_file = env.tmp / 'env2'
    other_env_file.write_text('DATABASE_URL=postgresql://stub/another\n')
    env.run_env['PLATEAU_ENV_FILE'] = str(other_env_file)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert '--postgres-url postgresql://stub/another' in env.called.read_text()


def test_input_survives_a_failed_import(env):
    """取り込みが失敗したら、入力ディレクトリを消さない。

    以前は再ダウンロードできたので消してよかったが、いまは取り出しから
    転送までをやり直すことになる。
    """
    d = _city(env, osm=2)
    env.stub.write_text('import sys\nsys.exit(7)\n')

    r = _run(env)

    assert r.returncode == 7, r.stdout + r.stderr
    assert d.exists()
    assert len(list(d.glob('*.osm'))) == 2


def test_input_removed_after_a_successful_import(env):
    """成功したときだけ入力を消す。"""
    d = _city(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert not d.exists()


def test_low_disk_exits_2(env):
    """ディスク不足は 2。バッチ全体を止める合図なので他と混ぜない。"""
    _city(env, osm=2)
    env.run_env['THRESHOLD_KB'] = '999999999999'

    r = _run(env)

    assert r.returncode == 2, r.stdout + r.stderr


def test_counts_the_osm_files_instead_of_assuming_two(env):
    """.osm の枚数を実際に数えている。定数と比べていない。

    他のテストがどれも 2 枚なので、OSM_N=2 と決め打ちした実装でも通る。
    3 枚で manifest を 2 にすると、数えている実装だけが落ちる。
    """
    _city(env, osm=3, manifest=2)

    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr


def test_importer_exit_2_becomes_14(env):
    """取り込み器の exit 2 (argparse のエラーなど) は 14 に写す。

    2 はディスク不足の予約番号で、バッチはこれを見て全体を止める。
    取り込み器は argparse を使うので、引数の綴りが違うだけで 2 を返す。
    素通しすると 1 都市目で全体が止まり、ログには「ディスク不足」と出る。
    """
    _city(env, osm=2)
    env.stub.write_text('import sys\nsys.exit(2)\n')

    r = _run(env)

    assert r.returncode == 14, r.stdout + r.stderr


def test_kills_the_importer_when_the_wrapper_is_terminated(env):
    """wrapper が SIGTERM を受けたら、取り込みの子も落とす。

    watchdog の kill は wrapper にしか届かない。子が生き残ると、
    次の都市の取り込みと id の採番が重なってノードが別都市の建物に
    ぶら下がる。例外も出ず行数も 0 にならないので他では検出できない。

    本体を `{ ... } | tee` にすると、IMPORT_PID=$! がサブシェルの中で
    起きて親に伝わらず、親の cleanup は空の IMPORT_PID を見て空振りする。

    pgrep は plateau_importer2postgis.py という一般的な名前ではなく、
    このテスト専用の --data-dir (tmp_path 配下の一意なパス) で絞る。
    無関係なプロセスを誤って拾わないようにするため。プロセスは
    finally で必ず後始末する。assert 失敗時に生き残らせない。

    途中の assert が落ちて child が None のまま finally に来ても、
    marker で pgrep をやり直して見つかったものを全部 SIGKILL する。
    proc.kill() は SIGKILL なので wrapper の EXIT トラップは走らない。
    child 変数だけを頼ると time.sleep(60) の子が 60 秒近く生き残り、
    その間に他のテスト (reimport_batch の二重取り込み検出など) が無関係な理由で赤くなる。
    実際に起きた。
    """
    import signal
    import time

    d = _city(env, osm=2)
    marker = str(d)  # --data-dir に渡る一意なパス。pgrep の的をこれに絞る。
    env.stub.write_text('import time\ntime.sleep(60)\n')

    proc = subprocess.Popen(['bash', str(ONE), '30406'], env=env.run_env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    child = None
    try:
        # 子が起動するまで待つ
        deadline = time.time() + 20
        while time.time() < deadline:
            out = subprocess.run(['pgrep', '-f', marker],
                                 capture_output=True, text=True)
            if out.stdout.strip():
                child = out.stdout.split()[0]
                break
            time.sleep(0.2)
        assert child, '取り込みの子が起動しなかった'

        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=30)

        # 子が落ちるまで少し待つ
        deadline = time.time() + 15
        alive = True
        while time.time() < deadline:
            if subprocess.run(['kill', '-0', child],
                              capture_output=True).returncode != 0:
                alive = False
                break
            time.sleep(0.2)
        assert not alive, '取り込みの子が生き残った (IMPORT_PID が親に伝わっていない)'
    finally:
        if proc.poll() is None:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass
        # child 変数を頼らず、marker で pgrep をやり直して見つかったもの
        # 全部を後始末する。assert が起動待ちの途中で落ちた場合も含む。
        out = subprocess.run(['pgrep', '-f', marker], capture_output=True, text=True)
        for pid in out.stdout.split():
            subprocess.run(['kill', '-9', pid], capture_output=True)


def test_missing_env_file_exits_with_dedicated_config_code(env):
    """PLATEAU_ENV_FILE の実体が無ければ専用の終了コードで落ちる。

    : "${PLATEAU_ENV_FILE:?...}" が保証するのは変数が設定されていることだけで、
    ファイルの実在ではない。無いまま進むと後段の source が set -e 無しで
    黙って失敗し、DATABASE_URL の unbound variable という分かりにくい形で
    落ちる。13 (入力の枚数不一致) と混ぜないよう別コードにする。
    """
    _city(env, osm=2)
    env.run_env['PLATEAU_ENV_FILE'] = str(env.tmp / 'does_not_exist.env')

    r = _run(env)

    assert r.returncode == 15, r.stdout + r.stderr


def test_existing_env_file_does_not_use_the_config_code(env):
    """PLATEAU_ENV_FILE の実体があれば、専用コードでは落ちない。

    このテストが無いと「PLATEAU_ENV_FILE を見た瞬間に必ず exit 15 する」
    実装でも上のテストが通ってしまう。
    """
    _city(env, osm=2)

    r = _run(env)

    assert r.returncode != 15, r.stdout + r.stderr
    assert r.returncode == 0, r.stdout + r.stderr
