#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 2B daily Options Anomalies — runs from VPS cron.
#
# Fires weekdays at 21:30 UTC (30min after the GEX cron at 21:00 to avoid
# simultaneous CBOE pulls + 30min before the insider cron at 22:00 to avoid
# simultaneous git push race).
#
# Pipeline: detect_anomalies (all 10) → copy to clone → commit → push
# Runtime parser at   /root/jabbafx-data-pipeline/parsers/detect_anomalies.py
# Publish target      /root/jabbafx-data-pipeline/staging/jabbafx-data/anomalies/
# Daily snapshots     /root/jabbafx-data-pipeline/data/snapshots/options/<date>/
#                      (preserved across runs — drives historical heuristics)
# Logs                /root/jabbafx-data-pipeline/logs/cron_anomalies.log
#
# Flags:
#   --symbol SYM      scan single underlying only (test mode)
#   --no-push         commit locally; do not push
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron_anomalies.log
LOCK=/run/jabbafx-anomalies.lock

mkdir -p "$BASE/logs"

# ── 1. Parse flags ────────────────────────────────────────────────────────
SYMBOL_ARG=""
NO_PUSH=0
while [ $# -gt 0 ]; do
  case "$1" in
    --symbol)  SYMBOL_ARG="--symbol $2"; shift 2 ;;
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
echo "===== JABBAFX-ANOM-START utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone (need latest 13F confluence for cross-reference) ──────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-ANOM-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Run detect_anomalies (10 underlyings unless --symbol passed) ──────
echo "[$(date -u +%FT%TZ)] detect_anomalies $SYMBOL_ARG" >>"$LOG"
if ! "$VENV" "$PARSERS/detect_anomalies.py" $SYMBOL_ARG \
    >>"$BASE/logs/detect_anomalies.log" 2>&1; then
  echo "===== JABBAFX-ANOM-FAIL step=detect_anomalies utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] detect_anomalies ok" >>"$LOG"

# ── 6. Copy output into clone ─────────────────────────────────────────────
mkdir -p "$CLONE/anomalies"
cp "$BASE/data/output/anomalies_recent.json" "$CLONE/anomalies/recent.json"

git add anomalies/recent.json

# ── 7. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to anomalies/recent.json" >>"$LOG"
else
  git commit -m "Anomalies daily: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-ANOM-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-ANOM-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
