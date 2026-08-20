"""ship_city.sh から reimport_one.sh への受け渡しを繋げて確かめる。

`.incoming` という文字列は 3 箇所に独立して書かれている。
ship_city.sh の DEST、reimport_one.sh の INCOMING、
tests/test_reimport_one.py の _incoming ヘルパである。

どれか 1 箇所だけを改名しても、それぞれのテストスイートは自分の書いた
文字列を自分で検証するだけなので緑のまま通ってしまい、本番でだけ
送る側と受け取る側の受け渡しが切れる。ここでは偽 rsync に実際にコピー
させた転送先を、そのまま PLATEAU_IMPORT_DIR として reimport_one.sh に
食わせ、取り込み器が正しいディレクトリと枚数を受け取ることを見る。

実際の rsync を走らせて rename と競わせる形にはしない。遅く不安定な
うえ、そこは rename より後に始まった転送にしか保証が無い部分であり
(2026-08-20 の設計文書を参照)、ここで確かめたいこと (文字列が一致して
いること) とは別の話である。
"""

import os
import subprocess
from pathlib import Path

from tests.test_ship_city import (  # noqa: F401 (env はフィクスチャとして使う)
    SHIP_CITY,
    _good_extract,
    _good_java,
    _transfer_copies_for_real,
    env,
)

ONE = Path(__file__).resolve().parent.parent / 'deploy' / 'reimport_one.sh'


def _run_ship(e, city='30406'):
    return subprocess.run(
        ['bash', str(SHIP_CITY), city],
        env=e.run_env, capture_output=True, text=True, timeout=60)


def test_shipped_input_is_claimed_correctly_by_reimport_one(env):  # noqa: F811
    """ship_city.sh が届けた転送先を、reimport_one.sh がそのまま確定して取り込む。

    偽 rsync に実際にコピーさせ、届いた先 (<PLATEAU_IMPORT_DIR>/.incoming/<都市>/)
    を PLATEAU_IMPORT_DIR として reimport_one.sh に渡す。取り込み器の偽物に
    受け取ったディレクトリと枚数を記録させ、送った枚数とディレクトリが
    そのまま届いていることを確かめる。
    """
    server_root = env.tmp / 'server'
    _transfer_copies_for_real(env, server_root)
    _good_extract(env, n=3)
    _good_java(env)

    ship_result = _run_ship(env, city='30406')
    assert ship_result.returncode == 0, ship_result.stdout + ship_result.stderr

    # 転送直後は .incoming/<都市>/ にあり、<都市>/ はまだ無い。
    incoming = server_root / '.incoming' / '30406'
    claimed = server_root / '30406'
    assert incoming.is_dir(), sorted(p.name for p in server_root.rglob('*'))
    assert not claimed.exists()

    # 取り込み側の一式を用意する。取り込み器は偽物に差し替え、
    # 呼ばれた引数を記録させる。
    app = env.tmp / 'app'
    app.mkdir()
    called = env.tmp / 'importer_called.txt'
    stub = app / 'plateau_importer2postgis.py'
    # 取り込み器は成功後に $SRC ごと消されるので、届いた .osm の枚数は
    # 呼び出しの時点で自ら数えて記録させる。呼び出しが終わったあとに
    # 外から数えようとしても、その頃には確定した入力自体が消えている。
    stub.write_text(
        'import sys, pathlib\n'
        'data_dir = pathlib.Path(sys.argv[sys.argv.index("--data-dir") + 1])\n'
        'osm_count = len(list(data_dir.glob("*.osm")))\n'
        'pathlib.Path(%r).write_text(\n'
        '    " ".join(sys.argv[1:]) + "\\nosm_count=" + str(osm_count))\n'
        'sys.exit(0)\n' % str(called)
    )
    plateau_env_file = env.tmp / 'plateau_env'
    plateau_env_file.write_text('DATABASE_URL=postgresql://stub/stub\n')

    import_run_env = dict(os.environ)
    import_run_env.update({
        'PLATEAU_APP_DIR': str(app),
        'PLATEAU_ENV_FILE': str(plateau_env_file),
        'PLATEAU_IMPORT_DIR': str(server_root),
        'PLATEAU_LOG_DIR': str(env.tmp / 'logs'),
        'PYTHON_BIN': 'python3',
        'THRESHOLD_KB': '0',
    })

    one_result = subprocess.run(
        ['bash', str(ONE), '30406'],
        env=import_run_env, capture_output=True, text=True, timeout=60)
    assert one_result.returncode == 0, one_result.stdout + one_result.stderr

    # rename で確定して .incoming 側は空になり、取り込みの成功後は
    # 確定した入力自体も reimport_one.sh が消す。取り込み器の偽物に
    # 記録させた引数が、届いた枚数と一致する確定後のパスを見ていたことが
    # 受け渡しの証拠になる。
    assert not incoming.exists()
    assert not claimed.exists()

    args = called.read_text()
    assert '--data-dir %s' % claimed in args, args
    assert '--citycode 30406' in args, args
    assert 'osm_count=3' in args, args
