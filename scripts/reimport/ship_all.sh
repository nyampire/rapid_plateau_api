#!/usr/bin/env bash
# 計画の都市を順に送る。
#
#     ship_all.sh
#
# shipped.txt にある都市は飛ばすので、途中で止めて再実行できる。
# 1 都市の失敗では止まらない。ディスクが閾値を割ったときだけ全体を止める。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SHIP_ENV:=$HERE/ship.env}"
if [ ! -f "$SHIP_ENV" ]; then
  echo "設定が無い: $SHIP_ENV (ship.env.example を複製する)" >&2
  exit 1
fi
# shellcheck disable=SC1090
. "$SHIP_ENV"

: "${SHIP_CITY_CMD:=bash $HERE/ship_city.sh}"
: "${PLAN_CSV:=$HERE/ckan_download_plan.csv}"
: "${SHIPPED_TXT:?SHIPPED_TXT が未設定}"
: "${SHIP_HOST:?SHIP_HOST が未設定}"
: "${SHIP_PATH:?SHIP_PATH が未設定}"
: "${WORK_ROOT:?WORK_ROOT が未設定}"
: "${DISK_MIN_KB:=5242880}"
: "${EXPECTED_CITIES:=148}"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] $*"; }

disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

# コマンドの出力を数値として比べる前に、必ずこれを通す。
# 空文字のまま [ "$x" -lt "$y" ] を評価すると
# integer expression expected でエラー終了し、if がそれを偽として扱う。
need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# ship.env はファイルなので、そこから来る数値も検査する。
# DISK_MIN_KB が壊れているとディスク不足の判定が黙って消える。
# 安全装置そのものが、設定を書き損じたときに限って効かなくなる。
for v in DISK_MIN_KB EXPECTED_CITIES; do
  eval "val=\$$v"
  if ! need_int "$val"; then
    say "ABORT: ${v} が数字でない (値: ${val})"
    exit 3
  fi
done

touch "$SHIPPED_TXT"

CODES=$(tail -n +2 "$PLAN_CSV" | cut -d, -f1 | grep -c .)
say "計画 $CODES 都市 (期待 $EXPECTED_CITIES)"
if [ "$CODES" -ne "$EXPECTED_CITIES" ]; then
  say "ABORT: 計画の件数が合わない。build_download_plan.py で足りない都市を足す"
  exit 3
fi

failed=""
ok=0
skip=0
i=0
# tr -d '\r' は CRLF の CSV に備える。\r が残ると shipped.txt との照合が
# 一致しなくなり、再開のたびに全都市を送り直す。
for CITY in $(tail -n +2 "$PLAN_CSV" | cut -d, -f1 | tr -d '\r'); do
  i=$((i + 1))

  # 行頭と末尾の空白で挟む。挟まないと 1340 が 13402 に前方一致する。
  if grep -q "^$CITY " "$SHIPPED_TXT"; then
    skip=$((skip + 1))
    continue
  fi

  AVAIL=$(disk_kb "$WORK_ROOT")
  if ! need_int "$AVAIL"; then
    say "ABORT: 空き容量を読めない (df の出力: $AVAIL)"
    exit 2
  fi
  if [ "$AVAIL" -lt "$DISK_MIN_KB" ]; then
    say "ABORT: 空きが $AVAIL KB で下限 $DISK_MIN_KB を割った"
    exit 2
  fi

  say "[$i/$CODES] $CITY: START"
  $SHIP_CITY_CMD "$CITY"
  EXIT=$?
  if [ "$EXIT" -eq 0 ]; then
    ok=$((ok + 1))
    say "[$i/$CODES] $CITY: OK"
  elif [ "$EXIT" -eq 2 ]; then
    say "ABORT: $CITY でディスク不足"
    exit 2
  else
    failed="$failed $CITY"
    say "[$i/$CODES] $CITY: FAIL exit=$EXIT"
  fi
done

# 一覧は毎回新しい名前で置く。第 2 段の走行中に送り直しても、
# 走っているバッチが読むファイルを書き換えない。
STAMP=$(date '+%Y%m%d-%H%M%S')
TARGETS="$WORK_ROOT/reimport_targets_$STAMP.txt"
# 都市コードの 1 列だけにする。shipped.txt は 2 列である。
# 第 2 段のバッチは行から空白を全部除くので、2 列のまま渡すと
# 43213 103 が 43213103 になり、その都市は永久に取り込まれない。
cut -d' ' -f1 "$SHIPPED_TXT" > "$TARGETS"
rsync -az "$TARGETS" "$SHIP_HOST:$SHIP_PATH/../reimport_targets_$STAMP.txt"
TARGETS_EXIT=$?
# ここで失敗を握りつぶすと、5〜6 時間かけて送ったあとに一覧だけ届いておらず、
# ログ上は成功して見える。第 2 段が始まらない理由が判らなくなる。
if [ "$TARGETS_EXIT" -ne 0 ]; then
  say "ABORT: 一覧の転送が exit ${TARGETS_EXIT}。手元には ${TARGETS} が残っている"
  exit 4
fi
say "一覧を置いた: reimport_targets_${STAMP}.txt ($(grep -c . "$TARGETS") 都市)"

say "=== DONE === ok=$ok skip=$skip failed=$(echo "$failed" | wc -w | tr -d ' ')"
if [ -n "$failed" ]; then
  say "失敗した都市:$failed"
  say "ship_all.sh を再実行すればこの都市だけをやり直せる"
  exit 1
fi
exit 0
