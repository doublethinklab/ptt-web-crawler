#!/bin/bash
# Usage: run.sh [daily|scan]
#   daily - full backfill of the previous day across all boards (default)
#   scan  - lightweight intraday pass over the high-traffic boards, fetching
#           only articles MongoDB has not seen yet
set -u

BASE=/root/ptt-web-crawler
cd "$BASE" || exit 1
source venv/bin/activate

MODE="${1:-daily}"
LOG_DIR="$BASE/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/ptt-$MODE-$(date -u +%Y%m%d).log"

if [ "$MODE" = "scan" ]; then
  BOARDS="HatePolitics Gossiping"
  ARGS="--mode scan"
else
  BOARDS="joke Military WomenTalk HatePolitics Gossiping"
  ARGS="--mode daily --offset 1"
fi

status=0
for board in $BOARDS; do
  echo "=== $(date -u +'%Y-%m-%dT%H:%M:%SZ') $board $MODE ===" >>"$LOG"
  if ! python -m PttWebCrawler -b "$board" $ARGS >>"$LOG" 2>&1; then
    echo "=== $board $MODE INCOMPLETE ===" >>"$LOG"
    status=1
  fi
done

deactivate
exit $status

# Full backfill of the previous day, at 02:00 UTC+8 (the day is closed by then)
# 0 18 * * * /root/ptt-web-crawler/run.sh daily
#
# Intraday scan of HatePolitics / Gossiping at 10:00, 14:00, 18:00, 22:00 UTC+8
# 0 2,6,10,14 * * * /root/ptt-web-crawler/run.sh scan
