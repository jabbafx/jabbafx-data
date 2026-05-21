#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 1C weekly COT update — runs from VPS cron.
#
# Fires Friday 22:30 UTC (30 min after the insider cron at 22:00 UTC to
# avoid simultaneous push race against the same GitHub repo). The CFTC
# weekly release lands at 3:30pm ET, so 22:30 UTC has at least 2h buffer
# in both winter (5:30pm ET) and summer (6:30pm ET).
#
# Pipeline: fetch_cot (×8) → parse_cot → copy to clone → commit → push
# Runtime parsers at   /root/jabbafx-data-pipeline/parsers/
# Publish target       /root/jabbafx-data-pipeline/staging/jabbafx-data/cot/
# Logs                 /root/jabbafx-data-pipeline/logs/cron_cot.log
#
# Flags:
#   --weeks N         override historical window (default 260 = 5y)
#   --force           pass --force through to fetcher (re-pull all weeks)
#   --no-push         commit locally; do not push (test mode)
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron_cot.log
LOCK=/run/jabbafx-cot.lock

mkdir -p "$BASE/logs"

# 8 tracked instruments: SYMBOL:CFTC_CODE
INSTRUMENTS=(
  "ES:13874A"
  "NQ:20974+"
  "CL:067651"
  "GC:088691"
  "ZB:020601"
  "6E:099741"
  "6J:097741"
  "6B:096742"
)

# ── 1. Parse flags ────────────────────────────────────────────────────────
WEEKS=260
FORCE_FLAG=""
NO_PUSH=0
while [ $# -gt 0 ]; do
  case "$1" in
    --weeks)   WEEKS="$2"; shift 2 ;;
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
echo "===== JABBAFX-COT-START weeks=$WEEKS utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone first (lets us see latest schema/code) ──────────────────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-COT-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Fetch raw weekly data for each instrument ──────────────────────────
for entry in "${INSTRUMENTS[@]}"; do
  symbol="${entry%:*}"
  code="${entry#*:}"
  echo "[$(date -u +%FT%TZ)] fetch $symbol ($code)" >>"$LOG"
  if ! "$VENV" "$PARSERS/fetch_cot.py" \
      --code "$code" --weeks "$WEEKS" $FORCE_FLAG \
      >>"$BASE/logs/fetch_cot.log" 2>&1; then
    echo "===== JABBAFX-COT-FAIL step=fetch_cot symbol=$symbol utc=$(date -u +%FT%TZ) =====" >>"$LOG"
    exit 1
  fi
done

# ── 6. Parse all 8 → per-instrument JSONs + combined latest.json ──────────
echo "[$(date -u +%FT%TZ)] parse_cot (all 8)" >>"$LOG"
if ! "$VENV" "$PARSERS/parse_cot.py" \
    >>"$BASE/logs/parse_cot.log" 2>&1; then
  echo "===== JABBAFX-COT-FAIL step=parse_cot utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] parse_cot ok" >>"$LOG"

# ── 7. Copy outputs into clone/cot/ ───────────────────────────────────────
mkdir -p "$CLONE/cot"
cp "$BASE/data/output/cot/"*.json "$CLONE/cot/"

git add cot/

# ── 8. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to cot/" >>"$LOG"
else
  python3 "$CLONE/pipeline/build_manifest.py" >>"$LOG" 2>&1 || true
  git add data_audit/manifest.json 2>>"$LOG" || true
  git commit -m "COT weekly: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-COT-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-COT-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
