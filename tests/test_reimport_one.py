"""reimport_one.sh の分岐を固定する。

シェルの分岐は実行時に気づきにくい。取り込み器は偽物に差し替える。
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
ONE = REPO / 'deploy' / 'reimport_one.sh'


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
                          env=e.run_env, capture_output=True, text=True)


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
