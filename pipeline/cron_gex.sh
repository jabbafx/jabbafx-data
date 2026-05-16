#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 2A daily Dealer Gamma Exposure (GEX) — runs from VPS cron.
#
# Fires weekdays at 21:00 UTC (4pm ET winter / 5pm ET summer — just after
# US market close, when CBOE end-of-day OI snapshot settles). Scheduled 1h
# before cron_insider.sh (22:00 UTC) to avoid simultaneous push collision.
#
# Pipeline: compute_gex (all 10) → copy to clone → commit → push
# Runtime parser at   /root/jabbafx-data-pipeline/parsers/compute_gex.py
# Publish target      /root/jabbafx-data-pipeline/staging/jabbafx-data/gex/
# Logs                /root/jabbafx-data-pipeline/logs/cron_gex.log
#
# Flags:
#   --symbol SYM      compute single underlying only (test mode)
#   --no-push         commit locally; do not push
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron_gex.log
LOCK=/run/jabbafx-gex.lock

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
echo "===== JABBAFX-GEX-START utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone (lets us push without conflicting if anyone pushed since) ──
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-GEX-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Run compute_gex.py (10 underlyings unless --symbol passed) ────────
echo "[$(date -u +%FT%TZ)] compute_gex $SYMBOL_ARG" >>"$LOG"
if ! "$VENV" "$PARSERS/compute_gex.py" $SYMBOL_ARG \
    >>"$BASE/logs/compute_gex.log" 2>&1; then
  echo "===== JABBAFX-GEX-FAIL step=compute_gex utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] compute_gex ok" >>"$LOG"

# ── 6. Copy outputs into clone ────────────────────────────────────────────
mkdir -p "$CLONE/gex"
cp "$BASE/data/output/gex/"*.json "$CLONE/gex/"

git add gex/

# ── 7. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to gex/" >>"$LOG"
else
  git commit -m "GEX daily: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-GEX-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-GEX-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
