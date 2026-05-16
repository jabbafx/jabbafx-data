#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX 13F quarterly auto-update — runs from VPS cron.
#
# Fires Feb 16 / May 16 / Aug 16 / Nov 16 at 03:00 UTC (day after each
# 45-day SEC filing deadline). Auto-derives the target quarter from today's
# date unless --quarter is passed.
#
# Pipeline: fetch_edgar → parse_13f → compute_confluence → commit → push
# Runtime parsers live at  /root/jabbafx-data-pipeline/parsers/
# Publish target           /root/jabbafx-data-pipeline/staging/jabbafx-data/
# Logs                     /root/jabbafx-data-pipeline/logs/
#
# Flags:
#   --quarter YYYY-QN   override auto-derived quarter
#   --force             pass --force through to fetcher + parser
#   --no-push           commit locally; do not push to GitHub (test mode)
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron.log
LOCK=/run/jabbafx-13f.lock

mkdir -p "$BASE/logs"

# ── 1. Parse flags ────────────────────────────────────────────────────────
QUARTER=""
FORCE_FLAG=""
NO_PUSH=0
while [ $# -gt 0 ]; do
  case "$1" in
    --quarter) QUARTER="$2"; shift 2 ;;
    --force)   FORCE_FLAG="--force"; shift ;;
    --no-push) NO_PUSH=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# ── 2. Auto-derive quarter if absent ──────────────────────────────────────
if [ -z "$QUARTER" ]; then
  YEAR=$(date -u +%Y)
  MONTH=$(date -u +%-m)
  case "$MONTH" in
    2)  QUARTER="$((YEAR-1))-Q4" ;;
    5)  QUARTER="${YEAR}-Q1" ;;
    8)  QUARTER="${YEAR}-Q2" ;;
    11) QUARTER="${YEAR}-Q3" ;;
    *)  # Out-of-schedule run — derive from "quarter that ended most recently"
        # so an off-cycle manual fire still picks a sane default.
        case "$MONTH" in
          1|2|3)    QUARTER="$((YEAR-1))-Q4" ;;
          4|5|6)    QUARTER="${YEAR}-Q1" ;;
          7|8|9)    QUARTER="${YEAR}-Q2" ;;
          10|11|12) QUARTER="${YEAR}-Q3" ;;
        esac
        ;;
  esac
fi

# ── 3. Lock ───────────────────────────────────────────────────────────────
exec 9>"$LOCK"
if ! flock -n 9; then
  echo "[$(date -u +%FT%TZ)] already running, exit" >>"$LOG"
  exit 0
fi

# ── 4. Banner ─────────────────────────────────────────────────────────────
echo "===== JABBAFX-13F-START quarter=$QUARTER utc=$(date -u +%FT%TZ) =====" >>"$LOG"

run_step() {
  local script="$1"; shift
  local name="${script%.py}"
  echo "[$(date -u +%FT%TZ)] step=$name running" >>"$LOG"
  if ! "$VENV" "$PARSERS/$script" "$@" >>"$BASE/logs/${name}.log" 2>&1; then
    echo "===== JABBAFX-13F-FAIL step=$name quarter=$QUARTER =====" >>"$LOG"
    exit 1
  fi
  echo "[$(date -u +%FT%TZ)] step=$name ok" >>"$LOG"
}

# ── 5. Pipeline ───────────────────────────────────────────────────────────
run_step fetch_edgar.py        --quarter "$QUARTER" $FORCE_FLAG
run_step parse_13f.py          --quarter "$QUARTER" $FORCE_FLAG
run_step compute_confluence.py --quarter "$QUARTER" $FORCE_FLAG

# ── 6. Sync VC clone ──────────────────────────────────────────────────────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-13F-FAIL step=git-pull quarter=$QUARTER =====" >>"$LOG"
  exit 1
fi

# ── 7. Copy outputs ───────────────────────────────────────────────────────
mkdir -p "$CLONE/13f/positions" "$CLONE/13f/confluence"
cp "$BASE/data/output/${QUARTER}.json"          "$CLONE/13f/positions/${QUARTER}.json"
cp "$BASE/data/output/${QUARTER}-analysis.json" "$CLONE/13f/confluence/${QUARTER}-analysis.json"

git add "13f/positions/${QUARTER}.json" "13f/confluence/${QUARTER}-analysis.json"

# ── 8. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to commit for $QUARTER" >>"$LOG"
else
  git commit -m "${QUARTER}: automated parse $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-13F-FAIL step=git-push quarter=$QUARTER =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-13F-OK quarter=$QUARTER utc=$(date -u +%FT%TZ) =====" >>"$LOG"
