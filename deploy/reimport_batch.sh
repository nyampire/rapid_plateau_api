#!/usr/bin/env bash
# 一覧の都市を順に取り込む。
#
#     reimport_batch.sh [一覧のパス]
#
# 一覧のパスを省略すると $HOME/reimport_targets.txt を読む。
# done.txt にある都市は飛ばす。$HOME/reimport_pause があるか、
# wrapper が exit 2 (ディスク不足) を返したら止まる。
# apt-daily-upgrade の時間帯 (06:00-06:35 JST) は都市の手前で待つ。
set -uo pipefail

LIST="${1:-$HOME/reimport_targets.txt}"
: "${REIMPORT_LOG_DIR:=$HOME/reimport_logs}"
: "${REIMPORT_ONE:=$HOME/reimport_one.sh}"
# 二重取り込み検出の的。テストがこの機械全体ではなくテスト専用のプロセスに
# 絞れるよう変数に出す。既定はいまと同じ文字列のまま変えない。
: "${STRAY_IMPORT_PATTERN:=plateau_importer2postgis.py}"
# 連続失敗の早期打ち切り。既定 3、0 で無効。reimport_watchdog.sh は
# WRAPPER_PATH のずれや連続失敗を見張るが、監視間隔が 60 秒あるうえ
# batch_status=DONE を見ると即座に終わるので、数秒で完走する失敗の
# 連鎖には間に合わない。バッチ自身にも同じ形の門を持たせる。
: "${EARLY_ABORT_FAILS:=3}"

DONE="$REIMPORT_LOG_DIR/done.txt"
FAILED="$REIMPORT_LOG_DIR/failed.txt"
PAUSE="$HOME/reimport_pause"
SUMMARY="$REIMPORT_LOG_DIR/summary.log"
STATUS="$REIMPORT_LOG_DIR/batch_status"

need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

mkdir -p "$REIMPORT_LOG_DIR"

# 一覧の打ち間違いを握りつぶさない。無いまま進むと 148 都市が 1 件も
# 走らないまま exit 0 で終わり、5 秒後に上げる watchdog も batch_status を
# 見て即座に終了するので、監視も一緒に消える。
if [ ! -f "$LIST" ]; then
  echo "[$(date '+%F %T')] ABORT: 一覧が無い: $LIST" | tee -a "$SUMMARY"
  echo NO_LIST > "$STATUS"
  exit 6
fi

# REIMPORT_ONE は未設定かどうかしか検査していなかったので、既定
# ($HOME/reimport_one.sh) を実際に置き忘れていても bash "$REIMPORT_ONE" が
# 「そんなファイルは無い」で exit 127 を返すだけで、148 都市すべてが
# failed.txt に積まれたまま最後は batch_status=DONE、exit 0 で終わる。
# reimport_watchdog.sh は同じ WRAPPER_PATH の実体を検査して exit 3 で
# 止まるので、バッチ側にも揃える。
if [ ! -f "$REIMPORT_ONE" ]; then
  echo "[$(date '+%F %T')] ABORT: wrapper が無い: $REIMPORT_ONE" | tee -a "$SUMMARY"
  echo NO_WRAPPER > "$STATUS"
  exit 6
fi

if ! need_int "$EARLY_ABORT_FAILS"; then
  echo "[$(date '+%F %T')] ABORT: EARLY_ABORT_FAILS が数字でない (値: $EARLY_ABORT_FAILS)" | tee -a "$SUMMARY"
  echo BAD_CONFIG > "$STATUS"
  exit 6
fi

touch "$DONE" "$FAILED"

rm -f "$STATUS"
echo "[$(date '+%F %T')] === BATCH START === list=$LIST" | tee -a "$SUMMARY"
TOTAL=$(grep -c . "$LIST")
# grep -c が壊れた値を返す、または一覧が空 (0 件) だと、この先の全ての
# ループが 0 回で完走し「成功」に見える。need_int で読み、0 件も同じ形で止める。
if ! need_int "$TOTAL" || [ "$TOTAL" -eq 0 ]; then
  echo "[$(date '+%F %T')] ABORT: 一覧が空か件数を数えられない (値: $TOTAL)" | tee -a "$SUMMARY"
  echo EMPTY_LIST > "$STATUS"
  exit 6
fi
echo "[$(date '+%F %T')] Total cities: $TOTAL" | tee -a "$SUMMARY"

wait_through_upgrade_window() {
  local logged=0
  while true; do
    local h m mod
    h=$(date +%H)
    m=$(date +%M)
    mod=$((10#$h * 60 + 10#$m))
    if [ $mod -ge 360 ] && [ $mod -lt 395 ]; then
      if [ $logged -eq 0 ]; then
        echo "[$(date '+%F %T')] In apt-daily-upgrade window, waiting until 06:35" | tee -a "$SUMMARY"
        logged=1
      fi
      sleep 120
    else
      [ $logged -eq 1 ] && echo "[$(date '+%F %T')] Upgrade window passed, resuming" | tee -a "$SUMMARY"
      return
    fi
  done
}

# 取り込みが 2 つ同時に走ると osm_id の採番が重なり、ノードが別都市の
# 建物にぶら下がる。例外も出ず行数も 0 にならないので、他では検出できない。
assert_no_stray_import() {
  if pgrep -f "$STRAY_IMPORT_PATTERN" > /dev/null 2>&1; then
    echo "[$(date '+%F %T')] ABORT: 取り込みが既に走っている" | tee -a "$SUMMARY"
    echo STRAY_IMPORT > "$STATUS"
    exit 5
  fi
}

i=0
ok=0
fail=0
skip=0
consec_fail=0
while IFS= read -r CITY; do
  i=$((i+1))
  CITY=$(echo "$CITY" | tr -d '[:space:]')
  [ -z "$CITY" ] && continue

  if [ -f "$PAUSE" ]; then
    echo "[$(date '+%F %T')] PAUSE detected. Stop at $i/$TOTAL (ok=$ok fail=$fail skip=$skip)" | tee -a "$SUMMARY"
    echo PAUSED > "$STATUS"
    exit 0
  fi

  if grep -qx "$CITY" "$DONE"; then
    skip=$((skip+1))
    continue
  fi

  wait_through_upgrade_window
  assert_no_stray_import

  echo "[$(date '+%F %T')] [$i/$TOTAL] $CITY: START" | tee -a "$SUMMARY"
  # < /dev/null で while の stdin (一覧ファイル) を子に継がせない。
  # いまの取り込み器は stdin を読まないので無害だが、読む処理が入ると
  # 都市が黙って食われる。
  bash "$REIMPORT_ONE" "$CITY" < /dev/null
  EXIT=$?
  if [ "$EXIT" -eq 0 ]; then
    echo "$CITY" >> "$DONE"
    ok=$((ok+1))
    consec_fail=0
    echo "[$(date '+%F %T')] [$i/$TOTAL] $CITY: OK (cumulative ok=$ok)" | tee -a "$SUMMARY"
  else
    echo "$CITY $EXIT" >> "$FAILED"
    fail=$((fail+1))
    consec_fail=$((consec_fail+1))
    echo "[$(date '+%F %T')] [$i/$TOTAL] $CITY: FAIL exit=$EXIT" | tee -a "$SUMMARY"
    if [ "$EXIT" -eq 2 ]; then
      echo "[$(date '+%F %T')] Disk threshold breached. ABORT." | tee -a "$SUMMARY"
      echo DISK_ABORT > "$STATUS"
      exit 2
    fi
    # wrapper が無い、設定を書き損じた等で全都市が数秒で失敗する形は、
    # 成功が 1 件も無いまま完走して batch_status=DONE になる。5 秒後に
    # 上げる watchdog は既に無意味な監視をするだけで、連続失敗の検出も
    # 60 秒間隔なので瞬時の失敗連鎖には間に合わない。ここで自前に打ち切る。
    # 成功が 1 件でもあれば、途中の都市が個別に失敗する通常の運用を
    # 止めないよう打ち切らない。
    if [ "$EARLY_ABORT_FAILS" -gt 0 ] && [ "$consec_fail" -ge "$EARLY_ABORT_FAILS" ] && [ "$ok" -eq 0 ]; then
      echo "[$(date '+%F %T')] ABORT: 連続 $consec_fail 件失敗し成功が 1 件も無い (閾値 $EARLY_ABORT_FAILS)" | tee -a "$SUMMARY"
      echo EARLY_ABORT > "$STATUS"
      exit 8
    fi
  fi
done < "$LIST"

echo "[$(date '+%F %T')] === BATCH DONE === ok=$ok fail=$fail skip=$skip total=$i" | tee -a "$SUMMARY"
echo DONE > "$STATUS"
