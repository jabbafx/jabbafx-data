#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 2C daily Options Metrics — runs from VPS cron.
#
# Computes per-underlying Put/Call ratios, ATM-straddle expected move, and
# lognormal implied probabilities from a single CBOE chain fetch (shared
# data with Modules 2A/2B).
#
# Fires weekdays at 23:00 UTC. Slot rationale:
#   21:00  GEX           ← Module 2A
#   21:30  Anomalies     ← Module 2B
#   22:00  Insider       ← Module 1B
#   22:30  COT (Fri only)← Module 1C
#   23:00  Options Metrics ← Module 2C  (this script, 30-min gap from insider)
#
# Pipeline: compute_options_metrics (all 10) → copy to clone → commit → push
# Runtime parser at   /root/jabbafx-data-pipeline/parsers/compute_options_metrics.py
# Publish target      /root/jabbafx-data-pipeline/staging/jabbafx-data/options_metrics/
# Daily snapshots     /root/jabbafx-data-pipeline/data/snapshots/options_pcr/<date>/
#                      (preserved across runs — drives 30d PCR percentile)
# Logs                /root/jabbafx-data-pipeline/logs/cron_options_metrics.log
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
LOG=$BASE/logs/cron_options_metrics.log
LOCK=/run/jabbafx-options-metrics.lock

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
echo "===== JABBAFX-OPTM-START utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone ─────────────────────────────────────────────────────────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-OPTM-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Run compute_options_metrics (10 underlyings unless --symbol passed) ──
echo "[$(date -u +%FT%TZ)] compute_options_metrics $SYMBOL_ARG" >>"$LOG"
if ! "$VENV" "$PARSERS/compute_options_metrics.py" $SYMBOL_ARG \
    >>"$BASE/logs/compute_options_metrics.log" 2>&1; then
  echo "===== JABBAFX-OPTM-FAIL step=compute utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] compute_options_metrics ok" >>"$LOG"

# ── 6. Copy output into clone ─────────────────────────────────────────────
mkdir -p "$CLONE/options_metrics"
cp "$BASE/data/output/options_metrics_recent.json" "$CLONE/options_metrics/recent.json"

git add options_metrics/recent.json

# ── 7. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to options_metrics/recent.json" >>"$LOG"
else
  git commit -m "Options metrics daily: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-OPTM-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-OPTM-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
