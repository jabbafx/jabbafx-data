#!/usr/bin/env python3
"""
parse_cot.py — CFTC COT raw JSON → structured per-instrument JSON with
                5y z-score and percentile rank for commercial positioning.

Module 1C Session 2. Reads raw weekly snapshots saved by fetch_cot.py and
produces:
  data/output/cot/<symbol>.json          per-instrument history + analytics
  data/output/cot/latest.json            combined latest snapshot for frontend
  logs/parse_cot.log                     run log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.

Decision rule served:
  "I will fade a commodity move when commercial net positioning hits 5y
   extreme percentile ≥95 (overbought) or ≤5 (oversold)."
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import pstdev, mean


BASE_DIR = Path("/root/jabbafx-data-pipeline")
RAW_DIR = BASE_DIR / "data" / "raw" / "cot"
OUTPUT_DIR = BASE_DIR / "data" / "output" / "cot"
LOG_PATH = BASE_DIR / "logs" / "parse_cot.log"

# Tracked instruments: (frontend symbol, CFTC contract code, display name)
TRACKED = [
    ("ES", "13874A", "E-Mini S&P 500"),
    ("NQ", "20974+", "Nasdaq-100"),
    ("CL", "067651", "WTI Crude Oil"),
    ("GC", "088691", "Gold"),
    ("ZB", "020601", "UST Bonds"),
    ("6E", "099741", "Euro FX"),
    ("6J", "097741", "Japanese Yen"),
    ("6B", "096742", "British Pound"),
]


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


def to_int(v):
    """CFTC fields come as strings. Handle commas + missing values."""
    if v is None:
        return None
    try:
        return int(float(str(v).replace(",", "")))
    except (ValueError, TypeError):
        return None


def percentile_rank(values: list, target: float) -> float:
    """Return 0-100 percentile rank of `target` within `values` using
    'mean rank' method (counts ties as half-below)."""
    if not values:
        return 50.0
    below = sum(1 for v in values if v < target)
    equal = sum(1 for v in values if v == target)
    return (below + 0.5 * equal) / len(values) * 100


def parse_one_instrument(symbol: str, code: str, name: str) -> dict:
    """Parse raw JSON for one instrument; compute analytics."""
    raw_path = RAW_DIR / f"{code}.json"
    if not raw_path.exists():
        logging.warning(f"{symbol} ({code}): raw JSON missing at {raw_path}")
        return {}
    rows = json.loads(raw_path.read_text())
    if not isinstance(rows, list) or not rows:
        logging.warning(f"{symbol}: empty raw data")
        return {}

    history = []
    for r in rows:
        week_iso = (r.get("report_date_as_yyyy_mm_dd") or "")[:10]
        if not week_iso:
            continue
        oi = to_int(r.get("open_interest_all"))
        cL = to_int(r.get("comm_positions_long_all"))
        cS = to_int(r.get("comm_positions_short_all"))
        ncL = to_int(r.get("noncomm_positions_long_all"))
        ncS = to_int(r.get("noncomm_positions_short_all"))
        nrL = to_int(r.get("nonrept_positions_long_all"))
        nrS = to_int(r.get("nonrept_positions_short_all"))
        comm_net = (cL - cS) if (cL is not None and cS is not None) else None
        noncomm_net = (ncL - ncS) if (ncL is not None and ncS is not None) else None
        nonrept_net = (nrL - nrS) if (nrL is not None and nrS is not None) else None
        history.append({
            "week": week_iso,
            "open_interest": oi,
            "comm_long": cL, "comm_short": cS, "comm_net": comm_net,
            "noncomm_long": ncL, "noncomm_short": ncS, "noncomm_net": noncomm_net,
            "nonrept_net": nonrept_net,
        })

    history.sort(key=lambda h: h["week"])
    if len(history) < 2:
        return {}

    latest = history[-1]
    comm_nets = [h["comm_net"] for h in history if h["comm_net"] is not None]
    noncomm_nets = [h["noncomm_net"] for h in history if h["noncomm_net"] is not None]

    def stats_block(values, target):
        if not values or target is None:
            return {"percentile_5y": None, "zscore_5y": None,
                    "min_5y": None, "max_5y": None, "mean_5y": None}
        m = mean(values)
        sd = pstdev(values) if len(values) > 1 else 0
        return {
            "percentile_5y": round(percentile_rank(values, target), 1),
            "zscore_5y": round((target - m) / sd, 2) if sd > 0 else 0.0,
            "min_5y": min(values),
            "max_5y": max(values),
            "mean_5y": round(m, 0),
        }

    comm_stats = stats_block(comm_nets, latest["comm_net"])
    noncomm_stats = stats_block(noncomm_nets, latest["noncomm_net"])

    # Signal interpretation per decision rule
    pct = comm_stats["percentile_5y"]
    if pct is None:
        signal = "n/a"
    elif pct >= 95:
        signal = "extreme_long"   # commercials max-long → bullish contrarian for spec
    elif pct <= 5:
        signal = "extreme_short"  # commercials max-short → bearish contrarian for spec
    elif pct >= 80:
        signal = "elevated_long"
    elif pct <= 20:
        signal = "elevated_short"
    else:
        signal = "neutral"

    return {
        "symbol": symbol,
        "code": code,
        "name": name,
        "market_name": rows[0].get("market_and_exchange_names"),
        "latest": latest,
        "comm_net_stats": comm_stats,
        "noncomm_net_stats": noncomm_stats,
        "signal": signal,
        "history": history,
    }


def write_per_instrument(payload: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{payload['symbol']}.json"
    payload = {**payload, "data_computed": datetime.now(timezone.utc).isoformat(
        timespec="seconds").replace("+00:00", "Z")}
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


def write_combined_latest(all_parsed: list) -> Path:
    """Combined snapshot — frontend's primary feed for the COT tile."""
    out_path = OUTPUT_DIR / "latest.json"
    snapshot = {
        "data_computed": datetime.now(timezone.utc).isoformat(
            timespec="seconds").replace("+00:00", "Z"),
        "instruments_count": len(all_parsed),
        "instruments": [],
    }
    for p in all_parsed:
        if not p:
            continue
        latest = p["latest"]
        snapshot["instruments"].append({
            "symbol": p["symbol"],
            "code": p["code"],
            "name": p["name"],
            "market_name": p["market_name"],
            "week": latest["week"],
            "open_interest": latest["open_interest"],
            "comm_net": latest["comm_net"],
            "noncomm_net": latest["noncomm_net"],
            "comm_percentile_5y": p["comm_net_stats"]["percentile_5y"],
            "comm_zscore_5y": p["comm_net_stats"]["zscore_5y"],
            "noncomm_percentile_5y": p["noncomm_net_stats"]["percentile_5y"],
            "signal": p["signal"],
        })
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(snapshot, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol", help="parse a single instrument by symbol (e.g. GC); "
                          "if omitted, parses all 8 tracked"
    )
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()
    logging.info("=== parse_cot start ===")

    targets = TRACKED
    if args.symbol:
        targets = [t for t in TRACKED if t[0] == args.symbol]
        if not targets:
            logging.error(f"unknown symbol {args.symbol!r}; "
                          f"valid: {[t[0] for t in TRACKED]}")
            return 2

    parsed = []
    for symbol, code, name in targets:
        p = parse_one_instrument(symbol, code, name)
        if p:
            out = write_per_instrument(p)
            parsed.append(p)
            pct = p["comm_net_stats"]["percentile_5y"]
            sig = p["signal"]
            n = len(p["history"])
            comm_net = p["latest"]["comm_net"]
            logging.info(
                f"{symbol} ({code}): {n} weeks parsed, "
                f"latest comm_net={comm_net:+,} "
                f"percentile={pct}% signal={sig} → {out.name}"
            )

    if not args.symbol:
        write_combined_latest(parsed)

    elapsed = time.monotonic() - start

    # Summary
    print()
    print("=" * 76)
    print(f"PARSE-COT SUMMARY  elapsed={elapsed:.2f}s")
    print("=" * 76)
    print(f"  {'sym':<5}{'code':<8}{'name':<20}{'week':<12}"
          f"{'comm_net':>15}{'pct_5y':>9}  signal")
    for p in parsed:
        comm_net = p["latest"]["comm_net"]
        pct = p["comm_net_stats"]["percentile_5y"]
        print(f"  {p['symbol']:<5}{p['code']:<8}{p['name'][:18]:<20}"
              f"{p['latest']['week']:<12}{comm_net:>15,}"
              f"{pct:>9}  {p['signal']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
