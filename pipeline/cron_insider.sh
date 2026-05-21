#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 1B daily insider feed — runs from VPS cron.
#
# Fires weekdays at 22:00 UTC (5pm ET winter / 6pm ET summer — buffer after
# 4pm ET market close + 2-business-day Form 4 filing deadline).
#
# Pipeline: build_insider_feed → copy to clone → commit → push
# Runtime orchestrator at  /root/jabbafx-data-pipeline/parsers/build_insider_feed.py
# Publish target            /root/jabbafx-data-pipeline/staging/jabbafx-data/insider/recent.json
# Logs                      /root/jabbafx-data-pipeline/logs/cron_insider.log
#
# Flags:
#   --top-n N         top-N tickers from 13F confluence (default 30)
#   --days N          rolling window for Form 4 scan (default 30)
#   --quarter YYYY-QN target 13F quarter to source tickers from (default 2026-Q1)
#   --force           pass --force through to fetcher (re-fetch even if on disk)
#   --no-push         commit locally; do not push to GitHub (test mode)
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron_insider.log
LOCK=/run/jabbafx-insider.lock

mkdir -p "$BASE/logs"

# ── 1. Parse flags ────────────────────────────────────────────────────────
TOP_N=30
DAYS=30
QUARTER=""
FORCE_FLAG=""
NO_PUSH=0
while [ $# -gt 0 ]; do
  case "$1" in
    --top-n)   TOP_N="$2"; shift 2 ;;
    --days)    DAYS="$2"; shift 2 ;;
    --quarter) QUARTER="$2"; shift 2 ;;
    --force)   FORCE_FLAG="--force"; shift ;;
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
echo "===== JABBAFX-INSIDER-START top_n=$TOP_N days=$DAYS utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone (need latest 13F confluence to source tickers) ────────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-INSIDER-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Run orchestrator ───────────────────────────────────────────────────
echo "[$(date -u +%FT%TZ)] running build_insider_feed.py" >>"$LOG"
QUARTER_ARG=""
if [ -n "$QUARTER" ]; then QUARTER_ARG="--quarter $QUARTER"; fi
if ! "$VENV" "$PARSERS/build_insider_feed.py" \
    --top-n "$TOP_N" --days "$DAYS" $QUARTER_ARG $FORCE_FLAG \
    >>"$BASE/logs/build_insider_feed.log" 2>&1; then
  echo "===== JABBAFX-INSIDER-FAIL step=build_insider_feed utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] build_insider_feed ok" >>"$LOG"

# ── 6. Copy output into clone ─────────────────────────────────────────────
mkdir -p "$CLONE/insider"
cp "$BASE/data/output/insider_recent.json" "$CLONE/insider/recent.json"

git add "insider/recent.json"

# ── 7. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to insider/recent.json" >>"$LOG"
else
  python3 "$CLONE/pipeline/build_manifest.py" >>"$LOG" 2>&1 || true
  git add data_audit/manifest.json 2>>"$LOG" || true
  git commit -m "Insider feed: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-INSIDER-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-INSIDER-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
