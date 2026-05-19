#!/bin/bash
# JabbaFX M6E — SEC 8-K live feed (hourly during US market hours)
set -e

PIPELINE_DIR="/root/jabbafx-data-pipeline/staging/jabbafx-data/pipeline"
REPO_DIR="/root/jabbafx-data-pipeline/staging/jabbafx-data"
LOG_DIR="/root/jabbafx-data-pipeline/logs"
LOG="$LOG_DIR/cron_8k.log"
LOCK="/run/jabbafx-8k.lock"

mkdir -p "$LOG_DIR"

exec 9>"$LOCK"
if ! flock -n 9; then
    echo "[$(date -u +%FT%TZ)] another 8k run in progress, exiting" >> "$LOG"
    exit 0
fi

echo "[$(date -u +%FT%TZ)] === 8k run start ===" >> "$LOG"

cd "$REPO_DIR"
python3 "$PIPELINE_DIR/fetch_8k.py" >> "$LOG" 2>&1
EXIT=$?

cd "$REPO_DIR"
git add sec_8k/recent.json sec_8k/_cik_map.json 2>>"$LOG" || true
if ! git diff --cached --quiet; then
    git commit -m "8k: hourly feed update" --no-verify >> "$LOG" 2>&1
    git pull --rebase origin main >> "$LOG" 2>&1 || true
    git push origin main >> "$LOG" 2>&1 || echo "[$(date -u +%FT%TZ)] push failed (non-fatal)" >> "$LOG"
fi

echo "[$(date -u +%FT%TZ)] === 8k run end (exit $EXIT) ===" >> "$LOG"
exit 0
