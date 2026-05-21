#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 5H weekly Historical State Archive — runs from VPS cron.
#
# Builds the 10-year × 10-dimensional macro state vector archive used by
# the Historical Analog Finder frontend to find dates matching today's
# macro configuration.
#
# Fires Sundays at 12:00 UTC. Off-hours, no collision with the 7 weekday
# crons (21:00-23:30 UTC) or the quarterly 13F cron (03:00 UTC).
#
# Pipeline: build_historical_state → copy to clone → commit → push
# Runtime parser at   /root/jabbafx-data-pipeline/parsers/build_historical_state.py
# Publish target      /root/jabbafx-data-pipeline/staging/jabbafx-data/historical_state/
# Logs                /root/jabbafx-data-pipeline/logs/cron_historical_state.log
#
# Flags:
#   --no-push         commit locally; do not push
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron_historical_state.log
LOCK=/run/jabbafx-historical-state.lock

mkdir -p "$BASE/logs"

# ── 1. Parse flags ────────────────────────────────────────────────────────
NO_PUSH=0
while [ $# -gt 0 ]; do
  case "$1" in
    --no-push) NO_PUSH=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ── 2. Lock ───────────────────────────────────────────────────────────────
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[$(date -u +%FT%TZ)] already running, exit" >>"$LOG"
  exit 0
fi

# ── 3. Banner ─────────────────────────────────────────────────────────────
echo "===== JABBAFX-HIST-START utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone ─────────────────────────────────────────────────────────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-HIST-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Build the historical state archive ────────────────────────────────
echo "[$(date -u +%FT%TZ)] build_historical_state" >>"$LOG"
if ! "$VENV" "$PARSERS/build_historical_state.py" \
    >>"$BASE/logs/build_historical_state.log" 2>&1; then
  echo "===== JABBAFX-HIST-FAIL step=build utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] build_historical_state ok" >>"$LOG"

# ── 6. Copy archive into clone ────────────────────────────────────────────
mkdir -p "$CLONE/historical_state"
cp "$BASE/data/output/historical_state.json" "$CLONE/historical_state/archive.json"
SIZE_KB=$(du -k "$CLONE/historical_state/archive.json" | cut -f1)
echo "[$(date -u +%FT%TZ)] archive size: ${SIZE_KB} KB" >>"$LOG"

git add historical_state/archive.json

# ── 7. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to historical_state/archive.json" >>"$LOG"
else
  python3 "$CLONE/pipeline/build_manifest.py" >>"$LOG" 2>&1 || true
  git add data_audit/manifest.json 2>>"$LOG" || true
  git commit -m "Historical state archive weekly: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-HIST-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-HIST-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
