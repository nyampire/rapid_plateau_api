#!/usr/bin/env bash
# 1 都市を取り込む。
#
#     reimport_one.sh <citycode>
#
# 入力は手元から送られてきた .osm で、PLATEAU_IMPORT_DIR/<citycode>/ に置かれる。
# ダウンロードは手元へ移った。purge は取り込みが内包しているので呼ばない。
#
# 環境変数
#   PLATEAU_APP_DIR     リポジトリの置き場所
#   PLATEAU_VENV        Python の仮想環境 (省略時は PYTHON_BIN をそのまま使う)
#   PLATEAU_ENV_FILE    DATABASE_URL を含む設定
#   PLATEAU_IMPORT_DIR  手元から送られた .osm の置き場所
#   PLATEAU_LOG_DIR     ログの置き場所
#   THRESHOLD_KB        取り込み前に要求する空き (1K ブロック、既定 5GB)
set -uo pipefail

CITY="${1:?citycode required}"

: "${PLATEAU_APP_DIR:?PLATEAU_APP_DIR が未設定}"
: "${PLATEAU_ENV_FILE:?PLATEAU_ENV_FILE が未設定}"
: "${PLATEAU_IMPORT_DIR:?PLATEAU_IMPORT_DIR が未設定}"
: "${PLATEAU_LOG_DIR:=$HOME/reimport_logs}"
: "${PYTHON_BIN:=python3}"
: "${THRESHOLD_KB:=5242880}"

EXIT_DISK=2
EXIT_INPUT=13
EXIT_CONFIG=15

# : "${PLATEAU_ENV_FILE:?...}" が保証するのは変数が設定されていることだけで、
# ファイルの実在ではない。無いまま進むと後段の `. "$PLATEAU_ENV_FILE"` が
# 失敗しても set -e が無いので気づかれず、DATABASE_URL の unbound variable と
# いう分かりにくい形で落ちる。13 (入力の枚数不一致) と混ぜないよう専用の
# コードを使う。
if [ ! -f "$PLATEAU_ENV_FILE" ]; then
  echo "設定が無い: ${PLATEAU_ENV_FILE}" >&2
  exit "$EXIT_CONFIG"
fi

mkdir -p "$PLATEAU_LOG_DIR"
LOG="$PLATEAU_LOG_DIR/${CITY}.log"
SRC="$PLATEAU_IMPORT_DIR/$CITY"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

# コマンドやファイルから読んだ値を数値として比べる前に、必ずこれを通す。
# 空文字のまま [ "$x" -ne "$y" ] を評価すると
# integer expression expected でエラー終了し、if がそれを偽として扱う。
# 分岐が黙って消えるので、門は「入力が壊れているときに限って」効かなくなる。
need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# ログはプロセス置換で複製する。`{ ... } | tee` にすると本体がサブシェルで
# 走り、そこでの IMPORT_PID=$! が親に伝わらない。watchdog が SIGTERM を
# 送るのは親なので、親の cleanup は常に空の IMPORT_PID を見て空振りする。
# 落とすべき取り込みが生き残り、この機構を置いた意味が消える。
exec > >(tee -a "$LOG") 2>&1

# 取り込み器を子として起動し、自分が終わるときに確実に落とす。
# watchdog の kill は wrapper の PID にしか届かないので、これが無いと
# 打ち切られたあとも取り込みが走り続け、次の都市と採番が重なる。
IMPORT_PID=""
cleanup() {
  if [ -n "$IMPORT_PID" ] && kill -0 "$IMPORT_PID" 2>/dev/null; then
    echo "[$(ts)] [$CITY] 取り込みの子 $IMPORT_PID を落とす"
    kill -TERM "$IMPORT_PID" 2>/dev/null || true
    sleep 5
    kill -KILL "$IMPORT_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

{
  echo "[$(ts)] [$CITY] === START ==="

  AVAIL=$(disk_kb "$PLATEAU_APP_DIR")
  if ! need_int "$AVAIL"; then
    echo "[$(ts)] [$CITY] ABORT: 空き容量を読めない (df の出力: $AVAIL)"
    exit $EXIT_DISK
  fi
  echo "[$(ts)] [$CITY] 空き $AVAIL KB (下限 $THRESHOLD_KB)"
  if [ "$AVAIL" -lt "$THRESHOLD_KB" ]; then
    echo "[$(ts)] [$CITY] ABORT: ディスクが足りない"
    exit $EXIT_DISK
  fi

  if [ ! -d "$SRC" ]; then
    echo "[$(ts)] [$CITY] ABORT: 入力が無い: $SRC"
    exit $EXIT_INPUT
  fi

  OSM_N=$(find "$SRC" -maxdepth 1 -name '*.osm' | wc -l | tr -d ' ')
  if [ ! -f "$SRC/manifest.txt" ]; then
    echo "[$(ts)] [$CITY] ABORT: manifest.txt が無い"
    exit $EXIT_INPUT
  fi
  WANT=$(tr -d '[:space:]' < "$SRC/manifest.txt")
  # manifest.txt が空だったり数字以外だったりすると、そのまま比較した時点で
  # 門が消える。壊れた manifest を弾くのがこの門の目的なので、
  # ここを素通りさせると門を置いた意味が無くなる。
  if ! need_int "$WANT"; then
    echo "[$(ts)] [$CITY] ABORT: manifest.txt が数字でない (中身: $WANT)"
    exit $EXIT_INPUT
  fi
  echo "[$(ts)] [$CITY] .osm $OSM_N 個 (manifest $WANT)"
  if [ "$OSM_N" -ne "$WANT" ]; then
    echo "[$(ts)] [$CITY] ABORT: 枚数が manifest と違う"
    exit $EXIT_INPUT
  fi

  cd "$PLATEAU_APP_DIR" || exit $EXIT_INPUT
  if [ -n "${PLATEAU_VENV:-}" ]; then
    # shellcheck disable=SC1091
    . "$PLATEAU_VENV/bin/activate"
  fi
  set -a
  # shellcheck disable=SC1090
  . "$PLATEAU_ENV_FILE"
  set +a

  echo "[$(ts)] [$CITY] 取り込み"
  # --citycode は明示する。推定が外れて "unknown" になると、既存データの
  # 削除と行政界フィルタの両方が黙って飛ぶ。
  "$PYTHON_BIN" plateau_importer2postgis.py \
    --data-dir "$SRC" --no-zip --citycode "$CITY" \
    --postgres-url "$DATABASE_URL" &
  IMPORT_PID=$!
  wait "$IMPORT_PID"
  IMP_EXIT=$?
  IMPORT_PID=""
  if [ "$IMP_EXIT" -ne 0 ]; then
    echo "[$(ts)] [$CITY] 取り込みが exit ${IMP_EXIT}。入力は残す"
    # 2 はディスク不足の予約番号で、バッチはこれを見て全体を止める。
    # 取り込み器は argparse を使うので、引数の綴りが違うと 2 を返す。
    # 素通しすると 1 都市目で全体が止まり、ログには「ディスク不足」と出る。
    if [ "$IMP_EXIT" -eq 2 ]; then
      echo "[$(ts)] [$CITY] 取り込み器の exit 2 を 14 に写す (引数の不整合の可能性)"
      exit 14
    fi
    exit $IMP_EXIT
  fi

  # 成功したときだけ消す。取り込みは --no-zip でも <data-dir>/extracted を
  # 作るので、.osm だけを消すと空のディレクトリが残る。
  rm -rf "$SRC"
  echo "[$(ts)] [$CITY] 空き $(disk_kb "$PLATEAU_APP_DIR") KB"
  echo "[$(ts)] [$CITY] === DONE ==="
}
