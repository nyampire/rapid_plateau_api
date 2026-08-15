#!/usr/bin/env bash
# バッチの見張り。ディスク、連続失敗、1 都市の所要時間、メモリを見る。
#
#     WRAPPER_PATH=/path/to/reimport_one.sh reimport_watchdog.sh
#
# 打ち切りや連続失敗で pause を立て、自分も終わる。
# 再開には reimport_pause を消して、バッチと watchdog の両方を起動し直す。
set -uo pipefail
INTERVAL=${INTERVAL:-60}
DISK_WARN_KB=${DISK_WARN_KB:-5242880}
DISK_HALT_KB=${DISK_HALT_KB:-3145728}
MAX_CITY_MIN=${MAX_CITY_MIN:-90}
: "${REIMPORT_LOG_DIR:=$HOME/reimport_logs}"
# wrapper の判定に使うパス。置き場所を変えるとここが一致しなくなり、
# 打ち切りも kill も黙って効かなくなる。ログだけは正常に出続ける。
: "${WRAPPER_PATH:=$HOME/reimport_one.sh}"

WATCH_LOG="$REIMPORT_LOG_DIR/watchdog.log"
PAUSE="$HOME/reimport_pause"
DIAG="$REIMPORT_LOG_DIR/watchdog_diagnostics.log"
low_mem_streak=0

mkdir -p "$REIMPORT_LOG_DIR"
START_LINES=$(wc -l < "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null || echo 0)
echo "[$(date '+%F %T')] === WATCHDOG START === baseline_lines=$START_LINES interval=${INTERVAL}s warn=${DISK_WARN_KB}KB halt=${DISK_HALT_KB}KB max_city=${MAX_CITY_MIN}min wrapper=$WRAPPER_PATH" | tee -a "$WATCH_LOG"

is_real_wrapper() {
  local pid=$1
  local cmdline
  cmdline=$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null) || return 1
  case "$cmdline" in
    "bash $WRAPPER_PATH "*) return 0 ;;
    *) return 1 ;;
  esac
}

trigger_pause() {
  local reason="$1"
  echo "[$(date '+%F %T')] PAUSE TRIGGERED: $reason" | tee -a "$WATCH_LOG"
  touch "$PAUSE"
  {
    echo "=== Diagnostics $(date '+%F %T') ==="
    echo "Reason: $reason"
    echo
    echo '--- df -h ---'
    df -h /
    echo
    echo '--- free -m ---'
    free -m
    echo
    echo '--- top RSS ---'
    ps -e -o pid,rss,etimes,cmd --sort=-rss | head -10
    echo
    echo '--- recent summary ---'
    tail -30 "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null
    echo
    echo '--- real wrappers ---'
    for p in $(pgrep -f "$(basename "$WRAPPER_PATH")" 2>/dev/null); do
      if is_real_wrapper $p; then
        echo "PID=$p ETIMES=$(ps -o etimes= -p $p 2>/dev/null) CMD=$(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null)"
      fi
    done
  } >> "$DIAG"
  for p in $(pgrep -f "$(basename "$WRAPPER_PATH")" 2>/dev/null); do
    if is_real_wrapper $p; then
      kill -TERM $p 2>/dev/null || true
    fi
  done
}

tick=0
while true; do
  tick=$((tick+1))
  TS=$(date '+%F %T')

  if [ -f "$REIMPORT_LOG_DIR/batch_status" ]; then
    STATUS=$(cat "$REIMPORT_LOG_DIR/batch_status" 2>/dev/null)
    echo "[$TS] WATCHDOG: batch_status=$STATUS, exiting" | tee -a "$WATCH_LOG"
    exit 0
  fi

  AVAIL_KB=$(df -k / | awk 'NR==2 {print $4}')

  if [ "$AVAIL_KB" -lt "$DISK_HALT_KB" ]; then
    trigger_pause "disk HALT: ${AVAIL_KB}KB < ${DISK_HALT_KB}KB"
    exit 2
  fi
  if [ "$AVAIL_KB" -lt "$DISK_WARN_KB" ]; then
    echo "[$TS] WARN disk=${AVAIL_KB}KB" >> "$WATCH_LOG"
  fi

  TOTAL_LINES=$(wc -l < "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null || echo 0)
  if [ "$TOTAL_LINES" -gt "$START_LINES" ]; then
    NEW_TAIL=$((TOTAL_LINES - START_LINES))
    LOOK=$(( NEW_TAIL < 12 ? NEW_TAIL : 12 ))
    RECENT_FAIL=$(tail -n "$NEW_TAIL" "$REIMPORT_LOG_DIR/summary.log" 2>/dev/null | tail -n "$LOOK" | grep -c "FAIL exit=")
    if [ "$RECENT_FAIL" -ge 3 ]; then
      trigger_pause "$RECENT_FAIL FAILs in last $LOOK observed entries (since watchdog start)"
      exit 3
    fi
  fi

  for pid in $(pgrep -f "$(basename "$WRAPPER_PATH")" 2>/dev/null); do
    if is_real_wrapper "$pid"; then
      ETIMES=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')
      if [ -n "$ETIMES" ] && [ "$ETIMES" -gt "$((MAX_CITY_MIN * 60))" ]; then
        trigger_pause "wrapper stuck pid=$pid etimes=${ETIMES}s"
        exit 4
      fi
    fi
  done

  MEM_AVAIL=$(free -m | awk '/^Mem:/{print $7}')
  if [ -n "$MEM_AVAIL" ] && [ "$MEM_AVAIL" -lt 30 ]; then
    low_mem_streak=$((low_mem_streak+1))
    if [ "$low_mem_streak" -ge 3 ]; then
      echo "[$TS] WARN sustained low mem avail=${MEM_AVAIL}MB streak=$low_mem_streak" >> "$WATCH_LOG"
    fi
  else
    low_mem_streak=0
  fi

  if [ $((tick % 10)) -eq 0 ]; then
    DONE_N=$(wc -l < "$REIMPORT_LOG_DIR/done.txt" 2>/dev/null || echo 0)
    FAIL_N=$(wc -l < "$REIMPORT_LOG_DIR/failed.txt" 2>/dev/null || echo 0)
    echo "[$TS] HB disk=${AVAIL_KB}KB mem=${MEM_AVAIL}MB done=${DONE_N} failed=${FAIL_N}" >> "$WATCH_LOG"
  fi

  sleep "$INTERVAL"
done
