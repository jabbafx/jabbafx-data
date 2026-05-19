#!/bin/bash
# JabbaFX M6C — NAAIM Exposure Index weekly fetch
set -e

PIPELINE_DIR="/root/jabbafx-data-pipeline/staging/jabbafx-data/pipeline"
REPO_DIR="/root/jabbafx-data-pipeline/staging/jabbafx-data"
LOG_DIR="/root/jabbafx-data-pipeline/logs"
LOG="$LOG_DIR/cron_naaim.log"
LOCK="/run/jabbafx-naaim.lock"

mkdir -p "$LOG_DIR"

exec 9>"$LOCK"
if ! flock -n 9; then
    echo "[$(date -u +%FT%TZ)] another naaim run in progress, exiting" >> "$LOG"
    exit 0
fi

echo "[$(date -u +%FT%TZ)] === naaim run start ===" >> "$LOG"

cd "$REPO_DIR"
python3 "$PIPELINE_DIR/fetch_naaim.py" >> "$LOG" 2>&1
EXIT=$?

cd "$REPO_DIR"
git add sentiment_naaim/latest.json 2>>"$LOG" || true
if ! git diff --cached --quiet; then
    git commit -m "naaim: weekly exposure update" --no-verify >> "$LOG" 2>&1
    git pull --rebase origin main >> "$LOG" 2>&1 || true
    git push origin main >> "$LOG" 2>&1 || echo "[$(date -u +%FT%TZ)] push failed (non-fatal)" >> "$LOG"
fi

echo "[$(date -u +%FT%TZ)] === naaim run end (exit $EXIT) ===" >> "$LOG"
exit 0
