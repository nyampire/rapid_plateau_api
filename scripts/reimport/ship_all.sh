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
: "${KEEP_RETAINED_DIRS:=3}"

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

# 退避した作業ディレクトリの残量を見せる。ship_city.sh の
# KEEP_RETAINED_DIRS で自動的に減るが、残っている量は目に入れておく。
# 148 都市を流している最中に空き容量の門で止まったとき、原因が
# 退避の堆積だと気づけるようにするための 1 行である。
report_retained() {
  local d kb
  local -a dirs=()
  shopt -s nullglob
  for d in "$WORK_ROOT"/*.failed.* "$WORK_ROOT"/*.stale.*; do
    # 展開されなかった glob はこのガードが弾く。守っているのは
    # nullglob そのものではなく、このガードである
    # (nullglob が無いと未展開のリテラルがそのまま渡る)。
    [ -d "$d" ] && dirs+=("$d")
  done
  shopt -u nullglob
  if [ "${#dirs[@]}" -eq 0 ]; then
    say "退避 0 件"
    return 0
  fi
  kb=$(du -sk "${dirs[@]}" 2>/dev/null | awk '{s+=$1} END {print s+0}')
  say "退避 ${#dirs[@]} 件 (合計 ${kb} KB)"
}

# ship.env はファイルなので、そこから来る数値も検査する。
# DISK_MIN_KB が壊れているとディスク不足の判定が黙って消える。
# 安全装置そのものが、設定を書き損じたときに限って効かなくなる。
# KEEP_RETAINED_DIRS が壊れていると、ここで気づかない限り 1 都市目の
# ship_city.sh がそこで exit 1 して落ちる。ship_all.sh はそれを
# 「1 都市の失敗」として積んで次へ進むので、148 都市すべてが同じ理由で
# 落ちてから気づくことになる。ここで先に止める。
for v in DISK_MIN_KB EXPECTED_CITIES KEEP_RETAINED_DIRS; do
  eval "val=\$$v"
  if ! need_int "$val"; then
    say "ABORT: ${v} が数字でない (値: ${val})"
    exit 3
  fi
done

# 失敗すると再開の飛ばしが無言で無効になり (grep が読む相手が無い)、
# 最後の cut もリダイレクト先が無く失敗して TARGETS が空のまま作られる。
# rsync 自体は成功するので exit 4 にはならず、「一覧を置いた: (0 都市)」と
# 出て正常終了に見える。設定検査と同じ exit 3 で、ここで止める。
if ! touch "$SHIPPED_TXT"; then
  say "ABORT: SHIPPED_TXT に書けない (値: ${SHIPPED_TXT})"
  exit 3
fi
# WORK_ROOT を先に作る。無いまま disk_kb "$WORK_ROOT" を掛けると df が
# エラー終了し、1 都市目の手前で「空き容量を読めない」と exit 2 になる。
# exit 2 はディスク不足の予約番号なので、運用者はディスクを疑って調べ始めるが
# 実際にはディレクトリが無いだけ。後段の TARGETS の書き出し先も同じ
# ディレクトリなので、ここで作っておけばそちらの保険にもなる。
# ただし set -e が無いので、mkdir -p 自体の失敗は放っておくと無視される。
# 同名の通常ファイルがあると mkdir は失敗するが、df はその親の
# ファイルシステムを読めるので上の門は通り、失敗が先の段まで持ち越される。
# 設定検査 (DISK_MIN_KB / EXPECTED_CITIES) と同じ exit 3 で、ここで止める。
if ! mkdir -p "$WORK_ROOT"; then
  say "ABORT: WORK_ROOT を作れない (値: ${WORK_ROOT})"
  exit 3
fi
report_retained

# PLAN_CSV が無いと tail が失敗するが、その出力を受ける grep -c . は
# 入力 0 行でも exit 0 で 0 を返す。件数の門にそのまま落ちて
# 「計画の件数が合わない」と出るので、運用者は CSV の中身を疑って
# build_download_plan.py を探しに行くが、実際にはパスが違うだけ。
# 実体を先に確かめて、別のメッセージで exit 3 にする。
if [ ! -f "$PLAN_CSV" ]; then
  say "ABORT: 計画のファイルが無い: $PLAN_CSV"
  exit 3
fi

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
    # 起動時の report_retained (この時点では何時間も前) だけだと、原因が
    # 退避の堆積だと運用者が気づけない。実際に走行を止めるのはここなので、
    # 止まる直前にもう一度出す。
    report_retained
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
# touch "$SHIPPED_TXT" が失敗していると cut の出力先も無言で空になり、
# rsync 自体は空ファイルの転送に成功するので exit 4 では止まらない。
# 「一覧を置いた: (0 都市)」が正常終了に見えてしまうので、件数 0 も
# 同じ exit 3 で止める。
TARGET_COUNT=$(grep -c . "$TARGETS")
if [ "$TARGET_COUNT" -eq 0 ]; then
  say "ABORT: 一覧が 0 都市。SHIPPED_TXT (${SHIPPED_TXT}) の書き込みを確認する"
  exit 3
fi
say "一覧を置いた: reimport_targets_${STAMP}.txt (${TARGET_COUNT} 都市)"

say "=== DONE === ok=$ok skip=$skip failed=$(echo "$failed" | wc -w | tr -d ' ')"
if [ -n "$failed" ]; then
  say "失敗した都市:$failed"
  say "ship_all.sh を再実行すればこの都市だけをやり直せる"
  exit 1
fi
exit 0
