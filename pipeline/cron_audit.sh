#!/bin/bash
# JabbaFX nightly Tier-3 data audit
# Runs audit_all_modules.py, writes data_audit/latest.json, commits to GitHub
set -e

PIPELINE_DIR="/root/jabbafx-data-pipeline/staging/jabbafx-data/pipeline"
REPO_DIR="/root/jabbafx-data-pipeline/staging/jabbafx-data"
LOG_DIR="/root/jabbafx-data-pipeline/logs"
LOG="$LOG_DIR/cron_audit.log"
LOCK="/run/jabbafx-audit.lock"

mkdir -p "$LOG_DIR"

# Lock to prevent concurrent runs
exec 9>"$LOCK"
if ! flock -n 9; then
    echo "[$(date -u +%FT%TZ)] another audit run in progress, exiting" >> "$LOG"
    exit 0
fi

echo "[$(date -u +%FT%TZ)] === audit run start ===" >> "$LOG"

# Run audit
cd "$REPO_DIR"
python3 "$PIPELINE_DIR/audit_all_modules.py" >> "$LOG" 2>&1
EXIT=$?

# Commit + push the result
cd "$REPO_DIR"
git add data_audit/latest.json data_audit/history.json 2>>"$LOG"
if ! git diff --cached --quiet; then
    git commit -m "audit: nightly data health check" --no-verify >> "$LOG" 2>&1
    git push origin main >> "$LOG" 2>&1 || echo "[$(date -u +%FT%TZ)] push failed (non-fatal)" >> "$LOG"
fi

echo "[$(date -u +%FT%TZ)] === audit run end (exit $EXIT) ===" >> "$LOG"
exit 0
