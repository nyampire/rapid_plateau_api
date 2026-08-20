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
: "${KEEP_RETAINED_DIRS:=3}"

# 未設定かどうかだけでなく、実体があるかも確かめる。ship.env.example の
# 既定はどちらもプレースホルダのパスなので、書き換え漏れがあると
# cp や java の実行時まで気づけない。cp は結果を見ずに次の段へ進む形に
# なっていたので、既定のまま走らせると conversion.json が複製されずに
# 変換器が既定の設定で走り切ってしまう。
if [ ! -f "$CONVERSION_JSON" ]; then
  echo "設定の実体が無い: CONVERSION_JSON=$CONVERSION_JSON (ship.env を確認する)" >&2
  exit 1
fi
if [ ! -f "$CITYGML_OSM_JAR" ]; then
  echo "設定の実体が無い: CITYGML_OSM_JAR=$CITYGML_OSM_JAR (ship.env を確認する)" >&2
  exit 1
fi

EXIT_EXTRACT=10
EXIT_CONVERT=11
EXIT_TRANSFER=12

WORK="$WORK_ROOT/$CITY"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] [$CITY] $*"; }

# 退避した作業ディレクトリを、新しい順に KEEP_RETAINED_DIRS 件だけ残す。
#
# 並べ替えは名前の末尾の日時で行う。日時は %Y%m%d-%H%M%S なので辞書順が
# 時刻順に一致する。mtime 順にはしない。mv はディレクトリの mtime を
# 退避した時刻に更新せず、中身を最後に変えた時刻のまま残すので、
# 実際の退避の順と食い違う。
#
# 末尾が日時の形 (%Y%m%d-%H%M%S) でない名前は並べ替えの対象から外す。
# 手で作った foo.stale.backup のような名前は無検査だと sort -r で数字より
# 前に来て「最新」扱いされ、永久に残ったうえで上限の枠を占有してしまう。
# 対象から外したものは消さずにそのまま残す。
#
# 数えた結果を後で使うので while read はパイプの右側に置かない。
# パイプの左側はサブシェルになり、そこでの代入が親に伝わらない。
prune_retained() {
  if [ "$KEEP_RETAINED_DIRS" -eq 0 ]; then
    return 0
  fi
  local d ts line
  local -a entries=()
  local skipped=0
  shopt -s nullglob
  for d in "$WORK_ROOT"/*.failed.* "$WORK_ROOT"/*.stale.*; do
    # 展開されなかった glob はこのガードが弾く。守っているのは
    # nullglob そのものではなく、このガードである
    # (nullglob が無いと未展開のリテラルがそのまま渡る)。
    [ -d "$d" ] || continue
    ts="${d##*.}"
    case "$ts" in
      [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]-[0-9][0-9][0-9][0-9][0-9][0-9]) ;;
      *) skipped=$((skipped + 1)); continue ;;
    esac
    entries+=("$ts"$'\t'"$d")
  done
  shopt -u nullglob
  # 形式外の名前は消さない判断自体は正しいが、黙って残ると運用者に伝わらない。
  if [ "$skipped" -gt 0 ]; then
    say "日時の形でない退避が ${skipped} 件ある。自動では消さない"
  fi
  if [ "${#entries[@]}" -le "$KEEP_RETAINED_DIRS" ]; then
    return 0
  fi
  while IFS= read -r line; do
    d="${line#*$'\t'}"
    rm -rf "$d"
    say "古い退避を消した: $(basename "$d")"
  done < <(printf '%s\n' "${entries[@]}" | sort -r | tail -n +$((KEEP_RETAINED_DIRS + 1)))
}

# 失敗した作業ディレクトリは検査用に退避する。
# 消さずに残すだけだと、次の実行が前回の .gml と .osm を数えてしまう。
bail() {
  local code=$1 msg=$2
  say "FAIL: $msg"
  if [ -d "$WORK" ]; then
    local kept="$WORK.failed.$(date '+%Y%m%d-%H%M%S')"
    # mv の結果を見ずに進むと、退避に失敗したときも「退避した」と
    # 言って prune_retained を呼んでしまい、古い退避が消える。
    # 退避できていないのだから、掃除も呼ばない。
    if mv "$WORK" "$kept"; then
      say "作業ディレクトリを退避した: $kept"
      prune_retained
    else
      say "作業ディレクトリを退避できない: $WORK -> $kept"
    fi
  fi
  exit "$code"
}

disk_kb() { df -k "$1" | awk 'NR==2 {print $4}'; }

# コマンドの出力を数値として比べる前に、必ずこれを通す。
# 空文字のまま [ "$x" -ne "$y" ] を評価すると
# integer expression expected でエラー終了し、if がそれを偽として扱う。
# 分岐が黙って消えるので、門は「壊れているときに限って」効かなくなる。
need_int() {
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
    *) return 0 ;;
  esac
}

# ship.env はファイルなので、そこから来る数値も検査する。DISK_MIN_KB が
# 壊れていると [ "$AVAIL" -lt "$DISK_MIN_KB" ] が integer expression
# expected でエラー終了し、if がそれを偽として扱う。ディスクの門そのものが
# 「設定を書き損じたときに限って」消える。ship_all.sh と同じ検査を足す。
if ! need_int "$DISK_MIN_KB"; then
  say "ABORT: DISK_MIN_KB が数字でない (値: $DISK_MIN_KB)"
  exit 1
fi

# KEEP_RETAINED_DIRS も ship.env から来る。書き損じると
# [ "$n" -gt "$KEEP_RETAINED_DIRS" ] が integer expression expected で
# エラー終了し、if がそれを偽として扱う。掃除が黙って消えるので、ここで止める。
if ! need_int "$KEEP_RETAINED_DIRS"; then
  say "ABORT: KEEP_RETAINED_DIRS が数字でない (値: $KEEP_RETAINED_DIRS)"
  exit 1
fi

# need_int は 08 や 010 を通すが、$(( )) は先頭 0 を 8 進として読む。
# 08 は算術エラーで掃除が 1 件も走らず、010 は上限が黙ってずれる。
# 比較と算術で解釈が食い違わないよう、ここで 10 進に正規化する。
KEEP_RETAINED_DIRS=$((10#$KEEP_RETAINED_DIRS))

say "=== START ==="

# WORK_ROOT を作れないと、この先の disk_kb や extract がその場所へ
# 書けずに失敗し、「取り出しの失敗」(exit 10) に化けて原因が分かりにくくなる。
# ship_all.sh は同じ mkdir の失敗を exit 3 として分けているので揃える。
if ! mkdir -p "$WORK_ROOT"; then
  say "ABORT: WORK_ROOT を作れない (値: $WORK_ROOT)"
  exit 3
fi
AVAIL=$(disk_kb "$WORK_ROOT")
if ! need_int "$AVAIL"; then
  say "ABORT: 空き容量を読めない (df の出力: $AVAIL)"
  exit 2
fi
say "空き $AVAIL KB (下限 $DISK_MIN_KB)"
if [ "$AVAIL" -lt "$DISK_MIN_KB" ]; then
  say "ABORT: ディスクが足りない"
  exit 2
fi

# 再試行は必ず空のディレクトリから始める。退避 (mv) や再作成 (mkdir) が
# 黙って失敗すると、前回の .gml と .osm を抱えたまま先へ進んでしまい、
# 上のコメントが避けたかった状態に戻る。
if [ -e "$WORK" ]; then
  if ! mv "$WORK" "$WORK.stale.$(date '+%Y%m%d-%H%M%S')"; then
    say "ABORT: 前回の作業ディレクトリを退避できない (値: $WORK)"
    exit 3
  fi
  prune_retained
fi
if ! mkdir -p "$WORK"; then
  say "ABORT: 作業ディレクトリを作れない (値: $WORK)"
  exit 3
fi

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
if [ "$MESHES_EXIT" -ne 0 ] || ! need_int "$MESHES"; then
  bail "$EXIT_EXTRACT" "meshes を読めない (出力: $EXTRACT_JSON)"
fi
GML_N=$(find "$WORK" -maxdepth 1 -name '*.gml' | wc -l | tr -d ' ')
say "報告 $MESHES メッシュ、実ファイル $GML_N"
# extract_city.py は udx/bldg/ で始まる .gml だけを拾う。zip の内部配置が
# 想定と違う版だと members が空になり、meshes: 0 で数の一致だけを見る門を
# 全部素通りしてしまう。shipped.txt に成功として記録されると
# ship_all.sh がその都市を以後永久に飛ばす。
if [ "$MESHES" -lt 1 ]; then
  bail "$EXIT_EXTRACT" ".gml が 1 つも無い (zip の内部配置が想定と違う可能性)"
fi
if [ "$GML_N" -ne "$MESHES" ]; then
  bail "$EXIT_EXTRACT" ".gml の数が報告と違う ($GML_N != $MESHES)"
fi

say "2/5 変換"
if ! cp "$CONVERSION_JSON" "$WORK/conversion.json"; then
  bail "$EXIT_CONVERT" "conversion.json を複製できない"
fi
(
  cd "$WORK" || exit 1
  "$JAVA_BIN" -Xmx4096m -Dfile.encoding=utf-8 -jar "$CITYGML_OSM_JAR" 1st
)
JAVA_EXIT=$?
if [ "$JAVA_EXIT" -ne 0 ]; then
  bail "$EXIT_CONVERT" "java が exit $JAVA_EXIT"
fi

OSM_N=$(find "$WORK" -maxdepth 1 -name '*.osm' | wc -l | tr -d ' ')
say ".osm $OSM_N 個"
if [ "$OSM_N" -ne "$GML_N" ]; then
  bail "$EXIT_CONVERT" ".osm の数が .gml と違う ($OSM_N != $GML_N)"
fi

# 数の一致では切り詰めを検出できない。JVM が途中で落ちると、
# 書きかけの .osm も 1 個として数えられる。
# .osm が 1 つも無いとグロブが展開されず、文字列 <WORK>/*.osm が f に入る。
# 存在しないパスに対して [ ! -s ] が真になり、実態と食い違うメッセージで落ちる。
shopt -s nullglob
for f in "$WORK"/*.osm; do
  if [ ! -s "$f" ]; then
    bail "$EXIT_CONVERT" "空のファイル: $(basename "$f")"
  fi
  if ! tail -c 200 "$f" | grep -q '</osm>'; then
    bail "$EXIT_CONVERT" "閉じタグが無い: $(basename "$f")"
  fi
done
shopt -u nullglob

say "3/5 manifest"
echo "$OSM_N" > "$WORK/manifest.txt"

say "4/5 転送"
# 送る先を .incoming に分ける。取り込み側は開始時にこれを <都市> へ
# rename して自分のものにするので、確定後の入力を送る側が触らない。
# 走行中に送り直しても取り込み中の入力が入れ替わらない。
DEST="$SHIP_PATH/.incoming/$CITY"
# rsync は宛先の最後の 1 段しか作らない。.incoming と <都市> の 2 段を
# 作る必要があるので、先に作る。転送先の根が無いこともある。
if ! ssh "$SHIP_HOST" "mkdir -p '$DEST'"; then
  bail "$EXIT_TRANSFER" "転送先を作れない ($DEST)"
fi
# --delete は残す。このディレクトリを読むのは送る側だけになったので、
# 取り込み中のファイルを消す心配が無い。
rsync -az --delete \
  --include='*.osm' --include='manifest.txt' --exclude='*' \
  "$WORK/" "$SHIP_HOST:$DEST/"
RSYNC_EXIT=$?
if [ "$RSYNC_EXIT" -ne 0 ]; then
  bail "$EXIT_TRANSFER" "rsync が exit $RSYNC_EXIT"
fi

REMOTE_N=$(ssh "$SHIP_HOST" "find '$DEST' -maxdepth 1 -name '*.osm' | wc -l" | tr -d ' ')
SSH_EXIT=$?
# ssh が失敗すると REMOTE_N が空になる。そのまま比較すると門が消え、
# 転送を確かめないまま shipped.txt に記録して作業ディレクトリを消す。
# 記録された都市は ship_all.sh が永久に飛ばす。
if [ "$SSH_EXIT" -ne 0 ] || ! need_int "$REMOTE_N"; then
  bail "$EXIT_TRANSFER" "転送先の枚数を数えられない (ssh exit ${SSH_EXIT}、出力: ${REMOTE_N})"
fi
say "転送先 $REMOTE_N 個"
if [ "$REMOTE_N" -ne "$OSM_N" ]; then
  bail "$EXIT_TRANSFER" "転送先の枚数が違う ($REMOTE_N != $OSM_N)"
fi

say "5/5 記録して掃除"
echo "$CITY $OSM_N" >> "$SHIPPED_TXT"
rm -rf "$WORK"
say "=== DONE ($OSM_N メッシュ) ==="
