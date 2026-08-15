#!/usr/bin/env bash
# 1 都市を取り出し、変換し、サーバへ送る。
#
#     ship_city.sh <citycode>
#
# 設定は ship.env から読む。場所は環境変数 SHIP_ENV で変えられる。
# 段の境目ごとに門があり、通らなければ記録せずに終わる。
set -uo pipefail

CITY="${1:?citycode required}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SHIP_ENV:=$HERE/ship.env}"
if [ ! -f "$SHIP_ENV" ]; then
  echo "設定が無い: $SHIP_ENV (ship.env.example を複製する)" >&2
  exit 1
fi
# shellcheck disable=SC1090
. "$SHIP_ENV"

: "${EXTRACT_CMD:?EXTRACT_CMD が未設定}"
: "${JAVA_BIN:?JAVA_BIN が未設定}"
: "${CITYGML_OSM_JAR:?CITYGML_OSM_JAR が未設定}"
: "${CONVERSION_JSON:?CONVERSION_JSON が未設定}"
: "${WORK_ROOT:?WORK_ROOT が未設定}"
: "${SHIP_HOST:?SHIP_HOST が未設定}"
: "${SHIP_PATH:?SHIP_PATH が未設定}"
: "${SHIPPED_TXT:?SHIPPED_TXT が未設定}"
: "${DISK_MIN_KB:=5242880}"

EXIT_EXTRACT=10
EXIT_CONVERT=11
EXIT_TRANSFER=12

WORK="$WORK_ROOT/$CITY"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] [$CITY] $*"; }

# 失敗した作業ディレクトリは検査用に退避する。
# 消さずに残すだけだと、次の実行が前回の .gml と .osm を数えてしまう。
bail() {
  local code=$1 msg=$2
  say "FAIL: $msg"
  if [ -d "$WORK" ]; then
    local kept="$WORK.failed.$(date '+%Y%m%d-%H%M%S')"
    mv "$WORK" "$kept"
    say "作業ディレクトリを退避した: $kept"
  fi
  exit "$code"
}

disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

say "=== START ==="

mkdir -p "$WORK_ROOT"
AVAIL=$(disk_kb "$WORK_ROOT")
say "空き $AVAIL KB (下限 $DISK_MIN_KB)"
if [ "$AVAIL" -lt "$DISK_MIN_KB" ]; then
  say "ABORT: ディスクが足りない"
  exit 2
fi

# 再試行は必ず空のディレクトリから始める
if [ -e "$WORK" ]; then
  mv "$WORK" "$WORK.stale.$(date '+%Y%m%d-%H%M%S')"
fi
mkdir -p "$WORK"

say "1/5 取り出し"
EXTRACT_JSON=$($EXTRACT_CMD "$CITY" "$WORK")
EXTRACT_EXIT=$?
if [ "$EXTRACT_EXIT" -ne 0 ]; then
  bail "$EXIT_EXTRACT" "extract が exit $EXTRACT_EXIT"
fi
echo "$EXTRACT_JSON"

MESHES=$(echo "$EXTRACT_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin)["meshes"])')
MESHES_EXIT=$?
# 報告が読めなかったときに門をすり抜けさせない。
# MESHES が空のまま [ "$GML_N" -ne "$MESHES" ] を評価すると
# integer expression expected でエラー終了し、if はそれを偽として扱う。
if [ "$MESHES_EXIT" -ne 0 ] || ! [[ "$MESHES" =~ ^[0-9]+$ ]]; then
  bail "$EXIT_EXTRACT" "meshes を読めない (出力: $EXTRACT_JSON)"
fi
GML_N=$(find "$WORK" -maxdepth 1 -name '*.gml' | wc -l | tr -d ' ')
say "報告 $MESHES メッシュ、実ファイル $GML_N"
if [ "$GML_N" -ne "$MESHES" ]; then
  bail "$EXIT_EXTRACT" ".gml の数が報告と違う ($GML_N != $MESHES)"
fi

say "=== 取り出しまで完了 ==="
