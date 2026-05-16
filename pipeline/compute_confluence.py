#!/usr/bin/env python3
"""
compute_confluence.py — Aggregate the parsed 13F positions JSON into a
multi-fund confluence analysis (JabbaFX Module 1A, Session 3).

Reads:
  /root/jabbafx-data-pipeline/data/output/<quarter>.json
  /root/jabbafx-data-pipeline/data/staged_funds.json
Writes:
  /root/jabbafx-data-pipeline/data/output/<quarter>-analysis.json
  /root/jabbafx-data-pipeline/logs/compute_confluence.log

Schema per PLAN.md §6.4. Signal-strength thresholds (per Session 3
operator decision): >=6 funds = high, 4-5 = medium, 3 = low.

QoQ fields (new_entries, exits, cluster_rotations, qoq_funds_added) are
emitted empty/zero for this first run — no Q4 2025 baseline yet
(PLAN.md §9.4).
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path("/root/jabbafx-data-pipeline")
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_PATH = BASE_DIR / "logs" / "compute_confluence.log"
FUNDS_JSON_PATH = BASE_DIR / "data" / "staged_funds.json"

CLUSTERS = (
    "ai_growth",
    "quality_concentrated",
    "macro_hard_assets",
    "commodity_energy",
    "growth_innovation",
)

MIN_FUNDS_HOLDING = 3
HIGH_THRESHOLD = 6   # >=6 funds → "high"
MEDIUM_THRESHOLD = 4 # 4-5 funds → "medium"; 3 → "low"

MAG7 = {"AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA"}


def setup_logger() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(message)s"
    datefmt = "%Y-%m-%dT%H:%M:%SZ"
    logging.Formatter.converter = time.gmtime
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(logging.Formatter(fmt, datefmt))
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter(fmt, datefmt))
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers[:] = []
    root.addHandler(fh)
    root.addHandler(sh)


def load_positions(quarter: str) -> dict:
    p = OUTPUT_DIR / f"{quarter}.json"
    if not p.exists():
        raise FileNotFoundError(
            f"positions JSON not found at {p}; run parse_13f.py first"
        )
    return json.loads(p.read_text())


def load_clusters_by_cik() -> dict:
    data = json.loads(FUNDS_JSON_PATH.read_text())
    return {f["cik"]: f["cluster"] for f in data.get("funds", [])}


def signal_strength(funds_count: int) -> str:
    if funds_count >= HIGH_THRESHOLD:
        return "high"
    if funds_count >= MEDIUM_THRESHOLD:
        return "medium"
    return "low"


def group_by_cusip(positions_payload: dict, clusters: dict) -> dict:
    """Returns {cusip: {ticker, name, funds_holding: set[cik], total_value: int,
                       cluster_breakdown: {cluster: count},
                       top_position_value: int  (for tie-break of ticker/name)}}"""
    grouped: dict = {}
    for cik, fund in positions_payload["funds"].items():
        for pos in fund["positions"]:
            cusip = pos["cusip"]
            if cusip not in grouped:
                grouped[cusip] = {
                    "cusip": cusip,
                    "ticker": pos["ticker"],
                    "name": pos["name"],
                    "funds_holding": set(),
                    "total_value": 0,
                    "cluster_breakdown": {c: 0 for c in CLUSTERS},
                    "_top_value": 0,
                }
            entry = grouped[cusip]
            entry["funds_holding"].add(cik)
            entry["total_value"] += pos["value"]
            cluster = clusters.get(cik)
            if cluster in entry["cluster_breakdown"]:
                entry["cluster_breakdown"][cluster] += 1
            # Tie-break ticker/name to the position with the highest value
            if pos["value"] > entry["_top_value"]:
                entry["_top_value"] = pos["value"]
                if pos["ticker"]:
                    entry["ticker"] = pos["ticker"]
                if pos["name"]:
                    entry["name"] = pos["name"]
    return grouped


def build_high_confluence(grouped: dict) -> list:
    out = []
    for entry in grouped.values():
        funds_n = len(entry["funds_holding"])
        if funds_n < MIN_FUNDS_HOLDING:
            continue
        out.append({
            "ticker": entry["ticker"],
            "cusip": entry["cusip"],
            "name": entry["name"],
            "funds_holding": funds_n,
            "cluster_breakdown": dict(entry["cluster_breakdown"]),
            "total_value": entry["total_value"],
            "qoq_funds_added": 0,
            "signal_strength": signal_strength(funds_n),
        })
    out.sort(
        key=lambda x: (-x["funds_holding"], -x["total_value"], x["cusip"])
    )
    return out


def build_summary(positions_payload: dict, high_confluence: list) -> dict:
    funds_filed = len(positions_payload["funds"])
    total_positions = sum(
        f["position_count"] for f in positions_payload["funds"].values()
    )
    return {
        "total_funds_filed": funds_filed,
        "total_positions_aggregate": total_positions,
        "new_multi_fund_positions": 0,
        "exited_multi_fund_positions": 0,
    }


def sanity_check(high_confluence: list, halts: list) -> None:
    """PLAN.md §9.5 halt rules."""
    if not high_confluence:
        halts.append(
            "pathological zero: high_confluence is empty across 21 funds — "
            "likely a CUSIP normalization bug; do NOT ship"
        )
        return
    # Mag 7 sanity (warning, not halt per plan)
    top10 = high_confluence[:10]
    top10_tickers = {p["ticker"] for p in top10 if p["ticker"]}
    mag_in_top10 = top10_tickers & MAG7
    logging.info(
        f"Mag-7 in top-10 high_confluence: {sorted(mag_in_top10)} "
        f"({len(mag_in_top10)} of 7)"
    )
    if len(mag_in_top10) < 3:
        logging.warning(
            "Mag-7 sanity below threshold (<3 in top 10) — investigate; "
            "treating as warning not halt per plan"
        )


def write_analysis_json(payload: dict, quarter: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{quarter}-analysis.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


def print_summary(payload: dict, high_confluence: list, elapsed: float) -> None:
    print()
    print("=" * 72)
    print(f"CONFLUENCE SUMMARY  quarter={payload['quarter']}  "
          f"elapsed={elapsed:.2f}s")
    print("=" * 72)
    print(f"total_funds_filed:           {payload['summary']['total_funds_filed']}")
    print(f"total_positions_aggregate:   "
          f"{payload['summary']['total_positions_aggregate']:,}")
    print(f"high_confluence positions:   {len(high_confluence)}")
    by_strength: dict = defaultdict(int)
    for p in high_confluence:
        by_strength[p["signal_strength"]] += 1
    for k in ("high", "medium", "low"):
        print(f"  {k:7s}: {by_strength[k]}")
    print("\nTop 15 high_confluence (by funds_holding desc, total_value desc):")
    print(f"  {'rank':<5}{'ticker':<8}{'cusip':<11}{'funds':<6}{'$total':<22}{'name'}")
    for i, p in enumerate(high_confluence[:15], 1):
        ticker = (p["ticker"] or "—")[:7]
        cusip = p["cusip"][:10]
        funds = p["funds_holding"]
        val = f"${p['total_value']:,.0f}"
        name = (p["name"] or "")[:40]
        print(f"  {i:<5}{ticker:<8}{cusip:<11}{funds:<6}{val:<22}{name}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarter", default="2026-Q1")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()
    logging.info(
        f"=== confluence start  quarter={args.quarter}  "
        f"force={args.force}  dry_run={args.dry_run} ==="
    )

    try:
        positions_payload = load_positions(args.quarter)
    except FileNotFoundError as e:
        logging.error(str(e))
        return 2

    clusters = load_clusters_by_cik()
    if not clusters:
        logging.error("no clusters loaded from funds.json")
        return 2

    grouped = group_by_cusip(positions_payload, clusters)
    high_confluence = build_high_confluence(grouped)

    halts: list = []
    sanity_check(high_confluence, halts)
    if halts:
        for h in halts:
            logging.error(f"HALT: {h}")
        return 2

    summary = build_summary(positions_payload, high_confluence)

    payload = {
        "quarter": args.quarter,
        "filing_period_end": positions_payload.get("filing_period_end"),
        "data_computed": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
        "signal_strength_thresholds": {
            "high": f">={HIGH_THRESHOLD} funds",
            "medium": f"{MEDIUM_THRESHOLD}-{HIGH_THRESHOLD-1} funds",
            "low": f"{MIN_FUNDS_HOLDING} funds",
        },
        "summary": summary,
        "high_confluence": high_confluence,
        "new_entries": [],
        "exits": [],
        "cluster_rotations": [],
    }

    if args.dry_run:
        elapsed = time.monotonic() - start
        print_summary(payload, high_confluence, elapsed)
        logging.info("DRY-RUN — not writing analysis JSON")
        return 0

    out_path = write_analysis_json(payload, args.quarter)
    logging.info(f"wrote analysis JSON to {out_path}")
    elapsed = time.monotonic() - start
    print_summary(payload, high_confluence, elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
