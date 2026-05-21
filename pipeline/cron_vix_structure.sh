#!/usr/bin/env bash
# ════════════════════════════════════════════════════════════════════════════
# JabbaFX Module 3A daily VIX Term Structure — runs from VPS cron.
#
# Computes VIX spot + regime + put-call-parity forward VIX curve across all
# expirations of the VIX options chain. Foundation of Phase 3 (Macro
# Intelligence) confluence with Phase 2's GEX/Anomalies/PCR stack.
#
# Fires weekdays at 23:30 UTC. Slot rationale:
#   21:00  GEX             ← Module 2A
#   21:30  Anomalies       ← Module 2B
#   22:00  Insider         ← Module 1B
#   22:30  COT (Fri only)  ← Module 1C
#   23:00  Options Metrics ← Module 2C
#   23:30  VIX Structure   ← Module 3A  (this script, 30-min gap from M2C)
#
# Pipeline: compute_vix_structure → copy to clone → commit → push
# Runtime parser at   /root/jabbafx-data-pipeline/parsers/compute_vix_structure.py
# Publish target      /root/jabbafx-data-pipeline/staging/jabbafx-data/vix_structure/
# Daily snapshots     /root/jabbafx-data-pipeline/data/snapshots/vix/<date>/
#                      (preserved across runs — drives 60d VIX percentile + 30d VRP)
# Logs                /root/jabbafx-data-pipeline/logs/cron_vix_structure.log
#
# Flags:
#   --no-push         commit locally; do not push
# ════════════════════════════════════════════════════════════════════════════

set -euo pipefail

BASE=/root/jabbafx-data-pipeline
VENV=$BASE/venv/bin/python
PARSERS=$BASE/parsers
CLONE=$BASE/staging/jabbafx-data
LOG=$BASE/logs/cron_vix_structure.log
LOCK=/run/jabbafx-vix.lock

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
echo "===== JABBAFX-VIX-START utc=$(date -u +%FT%TZ) =====" >>"$LOG"

# ── 4. Sync clone ─────────────────────────────────────────────────────────
cd "$CLONE"
echo "[$(date -u +%FT%TZ)] git pull --rebase" >>"$LOG"
if ! git pull --rebase --autostash >>"$LOG" 2>&1; then
  echo "===== JABBAFX-VIX-FAIL step=git-pull utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi

# ── 5. Run compute_vix_structure ──────────────────────────────────────────
echo "[$(date -u +%FT%TZ)] compute_vix_structure" >>"$LOG"
if ! "$VENV" "$PARSERS/compute_vix_structure.py" \
    >>"$BASE/logs/compute_vix_structure.log" 2>&1; then
  echo "===== JABBAFX-VIX-FAIL step=compute utc=$(date -u +%FT%TZ) =====" >>"$LOG"
  exit 1
fi
echo "[$(date -u +%FT%TZ)] compute_vix_structure ok" >>"$LOG"

# ── 6. Copy output into clone ─────────────────────────────────────────────
mkdir -p "$CLONE/vix_structure"
cp "$BASE/data/output/vix_structure_recent.json" "$CLONE/vix_structure/recent.json"

git add vix_structure/recent.json

# ── 7. Commit + push ──────────────────────────────────────────────────────
if git diff --cached --quiet; then
  echo "[$(date -u +%FT%TZ)] no changes to vix_structure/recent.json" >>"$LOG"
else
  python3 "$CLONE/pipeline/build_manifest.py" >>"$LOG" 2>&1 || true
  git add data_audit/manifest.json 2>>"$LOG" || true
  git commit -m "VIX structure daily: $(date -u +%FT%TZ)" >>"$LOG" 2>&1
  if [ "$NO_PUSH" -eq 0 ]; then
    if ! git push >>"$LOG" 2>&1; then
      echo "===== JABBAFX-VIX-FAIL step=git-push utc=$(date -u +%FT%TZ) =====" >>"$LOG"
      exit 1
    fi
  else
    echo "[$(date -u +%FT%TZ)] --no-push: skipping git push" >>"$LOG"
  fi
fi

echo "===== JABBAFX-VIX-OK utc=$(date -u +%FT%TZ) =====" >>"$LOG"
