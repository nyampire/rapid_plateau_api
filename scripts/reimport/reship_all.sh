#!/usr/bin/env bash
# 全都市を変換し直して送り直す。
#
#     reship_all.sh
#
# ship_all.sh を呼ぶ前に shipped.txt を空にして、計画の全都市を対象に戻す。
# 空にするのは初回だけで、印のファイルが残っているあいだは続きから進む。
#
# 設定は ship.env から読む。場所は環境変数 SHIP_ENV で変えられる。
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: "${SHIP_ENV:=$HERE/ship.env}"
if [ ! -f "$SHIP_ENV" ]; then
  echo "設定が無い: $SHIP_ENV (ship.env.example を複製する)" >&2
  exit 3
fi
# shellcheck disable=SC1090
. "$SHIP_ENV"

: "${SHIP_ALL_CMD:=bash $HERE/ship_all.sh}"
# スリープを抑止するコマンド。テストでは存在しない名前に差し替えて、
# 無い環境での動きを確かめる。
: "${CAFFEINATE_BIN:=caffeinate}"

# 必須の値は個別に確かめる。${VAR:?} は終了コード 1 を返すので、
# 設定の誤りを表す 3 と区別が付かなくなる。
for v in WORK_ROOT SHIPPED_TXT SHIP_HOST; do
  eval "val=\${$v:-}"
  if [ -z "$val" ]; then
    echo "$v が未設定 ($SHIP_ENV を確認する)" >&2
    exit 3
  fi
done

if ! mkdir -p "$WORK_ROOT"; then
  echo "WORK_ROOT を作れない (値: $WORK_ROOT)" >&2
  exit 3
fi

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] $*"; }

MARKER="$WORK_ROOT/reship_in_progress"
STAMP=$(date '+%Y%m%d-%H%M%S')

say "=== reship_all 開始 ==="

if [ -e "$MARKER" ]; then
  # 前回の実行が途中で終わっている。shipped.txt には送信済みの都市が
  # 入っているので、そのまま ship_all.sh に飛ばさせる。
  say "前回の続きから進む ($(head -1 "$MARKER"))"
else
  # 初回。送信済みの記録を控えに移してから空にする。
  # これをしないと ship_all.sh が 298 都市すべてを飛ばして 1 都市も送らない。
  BACKUP_NAME="(元から無し)"
  if [ -e "$SHIPPED_TXT" ]; then
    BACKUP="$SHIPPED_TXT.$STAMP"
    if ! mv "$SHIPPED_TXT" "$BACKUP"; then
      say "中止: shipped.txt を改名できない ($SHIPPED_TXT)"
      exit 3
    fi
    BACKUP_NAME=$(basename "$BACKUP")
    say "shipped.txt を $BACKUP_NAME に改名した"
  fi
  if ! : > "$SHIPPED_TXT"; then
    say "中止: shipped.txt を作れない ($SHIPPED_TXT)"
    exit 3
  fi
  printf '開始 %s\n退避 %s\n' "$(ts)" "$BACKUP_NAME" > "$MARKER"
  say "やり直しを開始する。計画の全都市が対象になる"
fi

say "=== 1 周目 ==="
$SHIP_ALL_CMD
EXIT=$?
say "1 周目の終了コード $EXIT"

# 1 は「一部の都市が失敗した」。配布元が一時的に応答しなかった都市は、
# もう 1 度実行すると成功することがある。成功した都市は shipped.txt に
# 入っているので、2 周目は失敗した都市だけを処理する。
# 2 (ディスク不足)、3 (設定の誤り)、4 (一覧の転送失敗) はやり直しても
# 同じ結果になるので、1 周で終わる。
if [ "$EXIT" -eq 1 ]; then
  say "=== 2 周目 (失敗した都市だけ) ==="
  $SHIP_ALL_CMD
  EXIT=$?
  say "2 周目の終了コード $EXIT"
fi

exit "$EXIT"
