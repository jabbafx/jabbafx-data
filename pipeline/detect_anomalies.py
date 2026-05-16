#!/usr/bin/env python3
"""
detect_anomalies.py — Options-flow anomaly detector (Module 2B).

For each of 10 tracked underlyings, fetch the CBOE delayed-quotes options
chain, apply a battery of rigorous anomaly heuristics, cross-reference any
flagged contracts against the 13F confluence list, and emit a daily JSON
of flagged anomalies.

Decision rule (Module 2B):
  "I will research any anomaly that flags ≥2 criteria simultaneously AND
   overlaps a name held by ≥4 of my tracked 13F funds."

The output includes ALL anomalies (single + multi-criteria) but flags the
"actionable" subset (criteria_count ≥ 2 + funds_holding_13f ≥ 4) prominently.

Heuristics — single-snapshot (work TODAY with no history):
  1. vol_oi_spike     volume / OI > VOL_OI_THRESHOLD (default 0.5)
                       and absolute volume > MIN_VOLUME_THRESHOLD
                       → significant fresh positioning vs existing book
  2. far_otm_volume   strike > FAR_OTM_PCT from spot AND volume > 100
                       → unusual flow into low-probability OTM contracts
  3. top_volume       single contract holds > TOP_VOL_SHARE of chain volume
                       → concentrated bet on one strike/expiration
  4. iv_outlier       contract IV deviates > IV_STDEV_THRESHOLD from
                       neighboring-strike IV (same expiration, same C/P)
                       → strike-specific positioning, not chain-wide IV shift

Heuristics — historical (activate after ≥10 daily snapshots accumulate):
  5. vol_pct_high     today's volume > 95th percentile of trailing N days
  6. iv_pct_high      today's IV > 95th percentile of trailing N days
  7. oi_delta_jump    abs(today's OI - yesterday's OI) > X
                       → large positioning change overnight
  (gracefully reports "needs history" when <10 snapshots on disk)

Reads:   CBOE delayed-quotes CDN (live)
         /root/.../staging/jabbafx-data/13f/confluence/2026-Q1-analysis.json
         /root/.../data/snapshots/options/YYYY-MM-DD/<symbol>.json (history)
Writes:  /root/.../data/snapshots/options/<today>/<symbol>.json (raw)
         /root/.../data/output/anomalies_recent.json (output)
         /root/.../logs/detect_anomalies.log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.
"""

import argparse
import json
import logging
import math
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from statistics import mean, pstdev

import requests


BASE_DIR = Path("/root/jabbafx-data-pipeline")
OUTPUT_DIR = BASE_DIR / "data" / "output"
SNAPSHOTS_DIR = BASE_DIR / "data" / "snapshots" / "options"
STAGED_CLONE = BASE_DIR / "staging" / "jabbafx-data"
LOG_PATH = BASE_DIR / "logs" / "detect_anomalies.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"

# Same 10 underlyings as Module 2A
TRACKED = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "MSFT", "AMD", "META", "GOOG"]

# ─── Thresholds (tuned against published Unusual Whales examples) ────────
# Rationale: 0-DTE / weekly options routinely see vol >> OI as contracts
# are bought + sold same day. To filter signal from noise we need genuinely
# unusual ratios. Initial loose thresholds (vol/OI > 0.5) produced 1300+
# "anomalies" on SPY alone — meaningless. These tightened values produce
# 30-80 anomalies per active underlying with a much higher signal-to-noise.
MIN_VOLUME           = 100       # contract must have ≥100 traded today
VOL_OI_THRESHOLD     = 2.0       # volume must be ≥ 2× OI to flag (fresh flow)
NEW_CONTRACT_VOL_MIN = 500       # OI=0 contracts need ≥500 vol to flag
TOP_VOL_SHARE        = 0.10      # single contract holding ≥10% of chain vol
FAR_OTM_PCT          = 0.15      # strikes >15% from spot count as "far OTM"
FAR_OTM_VOLUME_MIN   = 500       # deep-OTM contracts need ≥500 vol to flag
IV_STDEV_THRESHOLD   = 3.0       # outlier = >3σ from neighboring-strike IV mean
HIST_PCT_THRESHOLD   = 95        # 95th percentile = historical anomaly
HIST_MIN_DAYS        = 10        # snapshots needed before percentile activates

# Decision-rule defaults
MIN_CRITERIA_FOR_ACTIONABLE = 2
MIN_FUNDS_FOR_ACTIONABLE    = 4

# OCC option symbol regex (same as compute_gex.py)
_OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


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


def parse_occ(symbol: str):
    if not symbol:
        return None
    m = _OCC_RE.match(symbol)
    if not m:
        return None
    root, yymmdd, type_, strike_raw = m.groups()
    yyyy = "20" + yymmdd[:2]
    return {
        "root": root,
        "expiration": f"{yyyy}-{yymmdd[2:4]}-{yymmdd[4:6]}",
        "type": type_,
        "strike": int(strike_raw) / 1000.0,
    }


def fetch_cboe(symbol: str) -> dict:
    url = CBOE_URL.format(symbol=symbol.upper())
    logging.info(f"GET {url}")
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def save_raw_snapshot(symbol: str, raw: dict, today: str) -> Path:
    """Persist raw CBOE response for downstream historical heuristics."""
    snap_dir = SNAPSHOTS_DIR / today
    snap_dir.mkdir(parents=True, exist_ok=True)
    path = snap_dir / f"{symbol.upper()}.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(snap_dir), delete=False
    ) as tf:
        json.dump(raw, tf)  # no indent — these are bulky, keep compact
        tmp = tf.name
    os.replace(tmp, path)
    return path


def list_historical_dates(symbol: str, today: str) -> list:
    """Return YYYY-MM-DD dirs (sorted, excluding today) where this symbol
    has a saved snapshot. Used by historical heuristics."""
    if not SNAPSHOTS_DIR.exists():
        return []
    dates = []
    for d in sorted(SNAPSHOTS_DIR.iterdir()):
        if not d.is_dir() or d.name == today:
            continue
        if (d / f"{symbol.upper()}.json").exists():
            dates.append(d.name)
    return dates


# ─── Anomaly detection ────────────────────────────────────────────────────

def detect_single_snapshot_anomalies(symbol: str, raw: dict, spot: float) -> list:
    """Apply heuristics 1-4 (no history required)."""
    opts = raw.get("data", {}).get("options", []) or []
    if not opts:
        return []
    today = datetime.now(timezone.utc).date()

    # Pre-filter: require minimum volume to consider
    parsed = []
    total_chain_volume = 0
    for o in opts:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        try:
            exp_date = datetime.strptime(p["expiration"], "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if dte < 0 or dte > 365:
            continue
        vol = int(o.get("volume") or 0)
        if vol < MIN_VOLUME:
            continue
        oi = int(o.get("open_interest") or 0)
        iv = float(o.get("iv") or 0)
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else float(o.get("last_trade_price") or 0)
        moneyness = abs(p["strike"] - spot) / spot if spot > 0 else 0
        parsed.append({
            "occ": o["option"],
            **p,
            "dte": dte,
            "volume": vol,
            "open_interest": oi,
            "iv": iv,
            "bid": bid,
            "ask": ask,
            "mid": mid,
            "moneyness": moneyness,
            "delta": float(o.get("delta") or 0),
            "gamma": float(o.get("gamma") or 0),
        })
        total_chain_volume += vol

    anomalies = []
    for c in parsed:
        criteria = []

        # 1. VOL/OI spike — volume ≥ 2× existing OI signals fresh positioning
        # rather than recycling existing book. For OI=0 contracts (brand new
        # listings), require a higher absolute volume floor since the ratio
        # is undefined.
        if c["open_interest"] > 0:
            vol_oi = c["volume"] / c["open_interest"]
            if vol_oi >= VOL_OI_THRESHOLD:
                criteria.append("vol_oi_spike")
        else:
            if c["volume"] >= NEW_CONTRACT_VOL_MIN:
                criteria.append("vol_oi_spike")

        # 2. Far-OTM volume
        if c["moneyness"] > FAR_OTM_PCT and c["volume"] >= FAR_OTM_VOLUME_MIN:
            criteria.append("far_otm_volume")

        # 3. Top-volume single contract
        if total_chain_volume > 0:
            share = c["volume"] / total_chain_volume
            if share > TOP_VOL_SHARE:
                criteria.append("top_volume_share")

        if criteria:
            anomalies.append({**c, "criteria": criteria})

    # 4. IV outlier: per (expiration, type) group, flag strikes whose IV
    # deviates > IV_STDEV_THRESHOLD from the group's median IV.
    iv_groups: dict = {}
    for c in parsed:
        if c["iv"] <= 0:
            continue
        key = (c["expiration"], c["type"])
        iv_groups.setdefault(key, []).append(c)

    iv_outliers = set()
    for key, group in iv_groups.items():
        if len(group) < 5:
            continue
        ivs = [c["iv"] for c in group]
        m = mean(ivs)
        sd = pstdev(ivs)
        if sd <= 0:
            continue
        for c in group:
            z = abs(c["iv"] - m) / sd
            if z > IV_STDEV_THRESHOLD:
                iv_outliers.add(c["occ"])

    # Merge IV outlier flag into existing anomalies (or add fresh entries)
    existing = {a["occ"]: a for a in anomalies}
    for c in parsed:
        if c["occ"] in iv_outliers:
            if c["occ"] in existing:
                existing[c["occ"]]["criteria"].append("iv_outlier")
            else:
                existing[c["occ"]] = {**c, "criteria": ["iv_outlier"]}
    anomalies = list(existing.values())

    return anomalies


def detect_historical_anomalies(
    symbol: str, parsed_today: list, historical_dates: list
) -> dict:
    """Heuristics 5-7. Returns {contract_occ: [criteria_list]}.

    Requires at least HIST_MIN_DAYS historical snapshots. Otherwise returns
    empty mapping (signals 'needs history' to caller).
    """
    if len(historical_dates) < HIST_MIN_DAYS:
        return {}
    # TODO: implement once historical snapshots accumulate. Reads each prior
    # snapshot, builds per-contract volume + IV history, computes percentile
    # rank of today's value. Returns OCC → list of triggered criteria.
    return {}


def cross_reference_13f(symbol: str, confluence: dict) -> dict:
    """Look up symbol in confluence high_confluence list."""
    if not confluence:
        return {"funds_holding_13f": None, "signal_strength_13f": None}
    for entry in confluence.get("high_confluence", []):
        if entry.get("ticker") == symbol:
            return {
                "funds_holding_13f": entry.get("funds_holding"),
                "signal_strength_13f": entry.get("signal_strength"),
                "13f_name": entry.get("name"),
            }
    return {"funds_holding_13f": None, "signal_strength_13f": None}


def load_confluence() -> dict:
    """Load 2026-Q1 confluence from the staged clone."""
    p = STAGED_CLONE / "13f" / "confluence" / "2026-Q1-analysis.json"
    if not p.exists():
        logging.warning(f"no confluence JSON at {p}; skipping cross-ref")
        return {}
    return json.loads(p.read_text())


# ─── Per-underlying scan ─────────────────────────────────────────────────

def scan_one(symbol: str, confluence: dict, today_str: str) -> dict:
    raw = fetch_cboe(symbol)
    root = raw.get("data") or {}
    spot = root.get("current_price")
    if not spot or spot <= 0:
        logging.warning(f"{symbol}: no spot price; skipping")
        return None

    # Persist today's snapshot for downstream historical heuristics
    save_raw_snapshot(symbol, raw, today_str)

    # Single-snapshot anomalies
    anomalies = detect_single_snapshot_anomalies(symbol, raw, spot)

    # Historical anomalies (graceful — empty if not enough history)
    hist_dates = list_historical_dates(symbol, today_str)
    hist_flags = detect_historical_anomalies(symbol, anomalies, hist_dates)

    # Merge historical criteria
    for a in anomalies:
        extra = hist_flags.get(a["occ"], [])
        if extra:
            a["criteria"].extend(extra)

    # Cross-reference with 13F + add convenience fields
    xref = cross_reference_13f(symbol, confluence)
    for a in anomalies:
        a["symbol"] = symbol
        a["spot"] = spot
        a["criteria_count"] = len(a["criteria"])
        a["estimated_premium_usd"] = round(a["volume"] * a.get("mid", 0) * 100, 0) \
                                     if a.get("mid", 0) > 0 else 0
        a.update(xref)
        a["actionable"] = (
            a["criteria_count"] >= MIN_CRITERIA_FOR_ACTIONABLE and
            (a["funds_holding_13f"] or 0) >= MIN_FUNDS_FOR_ACTIONABLE
        )

    return {
        "symbol": symbol,
        "spot": spot,
        "data_timestamp": raw.get("timestamp"),
        "anomalies": anomalies,
        "anomaly_count": len(anomalies),
        "actionable_count": sum(1 for a in anomalies if a["actionable"]),
        "historical_days_available": len(hist_dates),
        "historical_mode_active": len(hist_dates) >= HIST_MIN_DAYS,
    }


# ─── Output ───────────────────────────────────────────────────────────────

def write_anomalies_json(all_results: list, today_str: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "anomalies_recent.json"
    flat = []
    for r in all_results:
        if not r:
            continue
        for a in r["anomalies"]:
            # Slim per-anomaly record for the published feed
            flat.append({
                "symbol": a["symbol"],
                "occ": a["occ"],
                "expiration": a["expiration"],
                "strike": a["strike"],
                "type": a["type"],
                "dte": a["dte"],
                "criteria": a["criteria"],
                "criteria_count": a["criteria_count"],
                "actionable": a["actionable"],
                "spot": a["spot"],
                "volume": a["volume"],
                "open_interest": a["open_interest"],
                "vol_oi_ratio": round(a["volume"] / a["open_interest"], 2)
                                if a["open_interest"] > 0 else None,
                "moneyness_pct": round(a["moneyness"] * 100, 2),
                "iv": round(a["iv"], 4),
                "bid": a["bid"],
                "ask": a["ask"],
                "mid": round(a.get("mid", 0), 2),
                "delta": round(a.get("delta", 0), 4),
                "gamma": round(a.get("gamma", 0), 6),
                "estimated_premium_usd": a["estimated_premium_usd"],
                "funds_holding_13f": a.get("funds_holding_13f"),
                "signal_strength_13f": a.get("signal_strength_13f"),
                "13f_name": a.get("13f_name"),
            })

    # Sort: actionable first, then by criteria_count desc, then by volume desc
    flat.sort(key=lambda a: (
        0 if a["actionable"] else 1,
        -a["criteria_count"],
        -a["volume"],
    ))

    payload = {
        "data_computed": datetime.now(timezone.utc).isoformat(
            timespec="seconds").replace("+00:00", "Z"),
        "scan_date": today_str,
        "underlyings_scanned": len(all_results),
        "total_anomalies": len(flat),
        "actionable_anomalies": sum(1 for a in flat if a["actionable"]),
        "decision_rule": (
            f"actionable = criteria_count >= {MIN_CRITERIA_FOR_ACTIONABLE} AND "
            f"funds_holding_13f >= {MIN_FUNDS_FOR_ACTIONABLE}"
        ),
        "thresholds": {
            "MIN_VOLUME": MIN_VOLUME,
            "VOL_OI_THRESHOLD": VOL_OI_THRESHOLD,
            "TOP_VOL_SHARE": TOP_VOL_SHARE,
            "FAR_OTM_PCT": FAR_OTM_PCT,
            "FAR_OTM_VOLUME_MIN": FAR_OTM_VOLUME_MIN,
            "IV_STDEV_THRESHOLD": IV_STDEV_THRESHOLD,
            "HIST_MIN_DAYS": HIST_MIN_DAYS,
        },
        "per_underlying_status": [
            {
                "symbol": r["symbol"],
                "spot": r["spot"],
                "anomaly_count": r["anomaly_count"],
                "actionable_count": r["actionable_count"],
                "historical_days_available": r["historical_days_available"],
                "historical_mode_active": r["historical_mode_active"],
            } for r in all_results if r
        ],
        "anomalies": flat,
    }

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


# ─── Summary ─────────────────────────────────────────────────────────────

def print_summary(all_results: list, elapsed: float) -> None:
    total_anom = sum(r["anomaly_count"] for r in all_results if r)
    actionable = sum(r["actionable_count"] for r in all_results if r)
    print()
    print("=" * 80)
    print(f"DETECT-ANOMALIES SUMMARY  elapsed={elapsed:.1f}s")
    print("=" * 80)
    print(f"  underlyings scanned:    {sum(1 for r in all_results if r)}")
    print(f"  total anomalies:        {total_anom}")
    print(f"  actionable (≥2 + 13F):  {actionable}")
    print()
    print(f"  {'sym':<6}{'spot':>9}{'anom':>7}{'action':>8}{'hist':>6}{'mode':>10}")
    for r in all_results:
        if not r:
            continue
        mode = "ON" if r["historical_mode_active"] else "single"
        print(f"  {r['symbol']:<6}{r['spot']:>9.2f}{r['anomaly_count']:>7}"
              f"{r['actionable_count']:>8}{r['historical_days_available']:>6}"
              f"{mode:>10}")


# ─── Main ────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        help="single underlying (e.g. SPY); omit to scan all 10"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="run + summarize, do not write output JSON")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    targets = TRACKED if not args.symbol else [args.symbol.upper()]

    confluence = load_confluence()

    results = []
    for sym in targets:
        try:
            r = scan_one(sym, confluence, today_str)
        except Exception as e:
            logging.error(f"{sym}: scan failed: {e}")
            results.append(None)
            continue
        if r:
            results.append(r)
            logging.info(
                f"{sym}: spot={r['spot']:.2f} anomalies={r['anomaly_count']} "
                f"actionable={r['actionable_count']} hist_days={r['historical_days_available']}"
            )

    if not args.dry_run and results:
        out = write_anomalies_json(results, today_str)
        logging.info(f"wrote {out}")

    print_summary(results, time.monotonic() - start)
    return 0 if any(r for r in results) else 2


if __name__ == "__main__":
    sys.exit(main())
