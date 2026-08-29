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

say "=== reship_all 開始 ==="
