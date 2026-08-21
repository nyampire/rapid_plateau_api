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

# 退避が起きたことを運用者が知る唯一の signal (reimport_one.sh の rm -rf の前)。
# 「退避できない」(異常系) の行にも「前回の入力を」までは共通するので、
# 正常系の行だけを一意に拾える末尾側の文言を使う。
STALE_RETENTION_MESSAGE = 'へ退避する'


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


def test_threshold_kb_not_a_number_exits_the_config_code(env):
    """THRESHOLD_KB が数字でなければ、比較へ進まず専用の設定コードで落ちる。

    THRESHOLD_KB は ~/.profile から来るので、書き損じ (例 "5G") は
    148 都市すべてに等しく効く。[ "$AVAIL" -lt "$THRESHOLD_KB" ] は
    integer expression expected でエラー終了し、if がそれを偽として
    扱うので、ディスクの門が消えたまま取り込みが走ってしまう
    (brief 実測)。起動時に need_int で検査して落とす。
    """
    _city(env, osm=2)
    env.run_env['THRESHOLD_KB'] = '5G'

    r = _run(env)

    assert r.returncode == 15, r.stdout + r.stderr
    assert not env.called.exists()


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


def _incoming(e, code='30406', osm=2, manifest=None):
    """送る側が置く受け口 .incoming/<都市>/ を作る。"""
    d = e.import_dir / '.incoming' / code
    d.mkdir(parents=True)
    for i in range(osm):
        (d / ('5339450%d_bldg_6697_op.osm' % i)).write_text('<osm/>')
    (d / 'manifest.txt').write_text(str(osm if manifest is None else manifest) + '\n')
    return d


def test_claims_incoming_when_only_incoming_exists(env):
    """.incoming だけあるとき、rename して取り込む。"""
    inc = _incoming(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert not inc.exists()
    assert env.called.read_text().split()[0:2] == ['--data-dir',
                                                   str(env.import_dir / '30406')]


def test_uses_src_directly_when_only_src_exists(env):
    """<都市> だけあるとき、rename せずそのまま取り込む。

    確定したあとに落ちた回の再実行がこの経路になる。
    ここで止めると、その都市は手で片付けるまで何度でも失敗し続ける。
    """
    _city(env, osm=3)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert env.called.read_text().split()[0:2] == ['--data-dir',
                                                   str(env.import_dir / '30406')]


def test_incoming_wins_and_old_src_is_retained(env):
    """両方あるとき、新しい .incoming を採り、古い <都市> を .stale へ退避する。

    枚数を 2 種類 (2 と 5) 使い、どちらが取り込まれたかを manifest で見分ける。

    取り込みを成功させると、I-3 により ${SRC}.stale ごと消えて事後に
    確かめられなくなる。ここでは退避そのものの検証に集中したいので、
    取り込み器を失敗させて退避直後の状態を残す。成功時にも退避が消える
    ことは test_stale_removed_after_a_successful_reimport で別途確かめる。
    """
    _city(env, osm=5)
    _incoming(env, osm=2)
    # called.txt には引数を残しつつ失敗させる (呼ばれた引数の検証のため)
    env.stub.write_text(
        'import sys, pathlib\n'
        'pathlib.Path(%r).write_text(" ".join(sys.argv[1:]))\n'
        'sys.exit(7)\n' % str(env.called)
    )

    r = _run(env)

    assert r.returncode == 7, r.stdout + r.stderr
    # 古い方 (5 枚) が退避され、新しい方 (2 枚) が確定した
    stale = env.import_dir / '30406.stale'
    assert stale.is_dir()
    assert stale.joinpath('manifest.txt').read_text().strip() == '5'
    assert not (env.import_dir / '.incoming' / '30406').exists()
    # 取り込み器には確定後のパスが渡る
    assert env.called.read_text().split()[0:2] == ['--data-dir',
                                                   str(env.import_dir / '30406')]
    # 退避が起きたことがログから分かる (I-4)
    assert STALE_RETENTION_MESSAGE in r.stdout, r.stdout


def test_stale_is_replaced_not_accumulated(env):
    """<都市>.stale が既にあっても、日時を増やさず置き換える。

    サーバの空き容量の門は 5GB が既定で、1 都市の入力は最大 4.4GB ある。
    退避を積むとこの門に当たる。

    取り込みを成功させると、I-3 により ${SRC}.stale ごと消えて事後に
    確かめられなくなるので、取り込み器を失敗させて退避直後の状態を残す。
    """
    old = env.import_dir / '30406.stale'
    old.mkdir()
    (old / 'ancient.osm').write_text('<osm/>')
    _city(env, osm=5)
    _incoming(env, osm=2)
    env.stub.write_text('import sys\nsys.exit(7)\n')

    r = _run(env)

    assert r.returncode == 7, r.stdout + r.stderr
    stale_dirs = sorted(p.name for p in env.import_dir.glob('30406.stale*'))
    assert stale_dirs == ['30406.stale'], stale_dirs
    assert not (env.import_dir / '30406.stale' / 'ancient.osm').exists()


def test_aborts_when_the_old_input_cannot_be_retained(env):
    """前回の入力を退避できないなら、確定を諦めて中断する。

    ${SRC}.stale を「中身のあるディレクトリ + chmod 555」にすると、
    rm -rf も (それに続く) mv も同じ理由 (書き込み不可) で失敗する。
    ここで検査を外すと、mv "$INCOMING" "$SRC" が $SRC (まだ古いまま) の
    中へ入れ子で成功し、枚数の門は古い .osm と古い manifest.txt を
    突き合わせて一致するので、古いデータを取り込んだまま exit 0 になる。
    """
    stale = env.import_dir / '30406.stale'
    stale.mkdir()
    (stale / 'locked.osm').write_text('<osm/>')
    os.chmod(stale, 0o555)
    _city(env, osm=5)
    _incoming(env, osm=2)
    try:
        r = _run(env)
        assert r.returncode == 13, r.stdout + r.stderr
        assert not env.called.exists(), '確定できていないのに取り込みが走った'
    finally:
        os.chmod(stale, 0o755)


def test_aborts_when_the_stale_rename_itself_fails(env):
    """退避の rm -rf 自体は素通りしても、mv "$SRC" "${SRC}.stale" が
    失敗すれば中断する。

    上の test_aborts_when_the_old_input_cannot_be_retained は
    ${SRC}.stale を丸ごと書き込み不可にするので、rm -rf が失敗した時点の
    検査 (I-2) が先に引っかかり、この mv 自体の検査 (I-1) を通らずに
    中断してしまう。mv だけの失敗を切り分けるため、${SRC}.stale は
    作らず (rm -rf は無を消すだけで何もせず成功する)、PLATEAU_IMPORT_DIR
    自体を書き込み不可にして rename 操作そのものを失敗させる。
    """
    _city(env, osm=5)
    _incoming(env, osm=2)
    os.chmod(env.import_dir, 0o555)
    try:
        r = _run(env)
        assert r.returncode == 13, r.stdout + r.stderr
        assert not env.called.exists(), '確定できていないのに取り込みが走った'
    finally:
        os.chmod(env.import_dir, 0o755)


def test_aborts_when_the_old_retention_cannot_be_fully_cleared(env):
    """rm -rf "${SRC}.stale" が消しきれなければ、退避せず中断する。

    ${SRC}.stale の中に chmod 000 のサブディレクトリを仕込むと、
    rm -rf はそこだけ消せず exit 1 になるが、${SRC}.stale 自体は
    書き込み可能なままなので、続く mv "$SRC" "${SRC}.stale" は
    ${SRC}.stale/30406/ へ入れ子で成功してしまう (レビュアが mv exit=0 と
    実測した状態)。中身は壊れないが、「日時を付けず 1 件だけ持つ」という
    退避の前提が入れ子で破れ、ディスクを積み続ける。
    rm -rf の結果を確かめて落とす (I-2)。
    """
    stale = env.import_dir / '30406.stale'
    locked = stale / 'locked'
    locked.mkdir(parents=True)
    (locked / 'x.osm').write_text('<osm/>')
    os.chmod(locked, 0o000)
    _city(env, osm=5)
    _incoming(env, osm=2)
    try:
        r = _run(env)
        assert r.returncode == 13, r.stdout + r.stderr
        assert not env.called.exists(), '退避を消しきれないのに取り込みが走った'
    finally:
        os.chmod(locked, 0o755)


def test_stale_removed_after_a_successful_reimport(env):
    """やり直し経路を通って取り込みが成功したら、<都市>.stale も消す。

    退避が意味を持つのは落ちてから取り込み直すまでの間だけで、
    取り込みが通った時点で証拠としての値打ちが無くなり、
    ディスクだけを占め続ける (I-3)。
    """
    _city(env, osm=5)
    _incoming(env, osm=2)

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr
    assert not (env.import_dir / '30406.stale').exists()


def test_rejects_a_non_5_digit_citycode(env):
    """citycode が 5 桁の数字でなければ、専用の終了コードで落ちる。

    $CITY から組み立てる rm -rf や mv がこのブランチで 1 本から 2 本に
    増えたので、".." のような値が渡ると被害はサーバ側で最大になる。

    検査対象の入力は 6 桁 (304060) にする。".." だと検査を外しても
    manifest.txt が見つからず別の理由で同じ exit 13 になり、この検査
    自体が効いているかを固定できない (brief 実測)。6 桁の入力は
    ディレクトリを実在させれば検査なしでは普通に成功してしまうので、
    落ちること自体がこの検査の効果だと言える。
    """
    _city(env, code='304060', osm=2)

    r = _run(env, city='304060')

    assert r.returncode == 13, r.stdout + r.stderr


def test_accepts_a_5_digit_citycode(env):
    """5 桁の数字はそのまま通る。"""
    _city(env, code='30406', osm=2)

    r = _run(env, city='30406')

    assert r.returncode == 0, r.stdout + r.stderr


def test_stale_left_after_success_warns_but_does_not_abort(env):
    """取り込みが成功したあとに ${SRC}.stale を消せなくても、中断しない。

    取り込みは既に成功しているので、ここで exit させると成功した取り込みを
    失敗として記録してしまう。警告を出すだけで進む。
    """
    d = _city(env, osm=5)
    _incoming(env, osm=2)
    # やり直し経路 (古い <都市> を .stale へ退避) を通す。退避のもとになる
    # 旧 SRC の中に権限で消せないサブディレクトリを仕込んでおくと、
    # rename 後の ${SRC}.stale でも同じ場所が消せないまま残る。
    locked = d / 'locked'
    locked.mkdir()
    (locked / 'x.osm').write_text('<osm/>')
    os.chmod(locked, 0o000)
    try:
        r = _run(env)
        assert r.returncode == 0, r.stdout + r.stderr
        assert (env.import_dir / '30406.stale').exists()
        assert '消せずに残った' in r.stdout, r.stdout
    finally:
        # 退避 (rename) で locked は import_dir/30406.stale/locked へ移っている。
        os.chmod(env.import_dir / '30406.stale' / 'locked', 0o755)


def test_aborts_when_neither_exists(env):
    """どちらも無ければ従来どおり exit 13。"""
    r = _run(env)

    assert r.returncode == 13, r.stdout + r.stderr
    assert '入力が無い' in r.stdout


def test_resend_during_import_does_not_touch_the_claimed_input(env):
    """確定後に .incoming へ送り直しても、取り込み中の入力は変わらない。

    取り込み器の偽物を、走っている最中に .incoming へ書き込む形にする。
    確定済みの <都市> の中身が変わらないことを、取り込み器自身に数えさせる。

    以前は $SRC の前後比較 (before == after) しか見ていなかったので、
    確定を rename ではなく cp -R (.incoming を残す複製) に置き換えても
    通ってしまった (brief 実測)。$SRC が別ディレクトリな以上、.incoming
    へ書き足しても $SRC には影響しないため、この比較だけでは
    「rename で確定した」ことを示せない。確定が rename であることの
    直接の証拠は「取り込みが始まった時点で .incoming/<都市> が
    もう存在しない」ことなので、それも取り込み器自身に確かめさせる。
    """
    _incoming(env, osm=2)
    env.stub.write_text(
        'import pathlib, sys, os\n'
        'src = pathlib.Path(sys.argv[sys.argv.index("--data-dir") + 1])\n'
        'inc = src.parent / ".incoming" / "30406"\n'
        # rename で確定していれば、取り込みが始まった時点で .incoming/30406 は
        # 消えているはず。cp -R (複製) だと元のまま残る。
        'claimed_by_rename = not inc.exists()\n'
        'before = sorted(p.name for p in src.glob("*.osm"))\n'
        # 取り込み中に送り直しが起きたことにする
        'inc.mkdir(parents=True, exist_ok=True)\n'
        '(inc / "99999999_bldg_6697_op.osm").write_text("<osm/>")\n'
        'after = sorted(p.name for p in src.glob("*.osm"))\n'
        'pathlib.Path(%r).write_text(repr((claimed_by_rename, before, after)))\n'
        'sys.exit(0 if (claimed_by_rename and before == after) else 1)\n'
        % str(env.called))

    r = _run(env)

    assert r.returncode == 0, r.stdout + r.stderr + (
        env.called.read_text() if env.called.exists() else '(called.txt 無し)')
