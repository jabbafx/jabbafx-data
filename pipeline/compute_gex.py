#!/usr/bin/env python3
"""
compute_gex.py — Dealer Gamma Exposure (GEX) calculator.

Module 2A Session 2. For each of 10 tracked underlyings:
  - Fetch full options chain from CBOE's public CDN (Greeks pre-computed)
  - Parse OCC option symbols (SPY260515C00360000 format)
  - Compute per-contract GEX:
        GEX_dollars = gamma × OI × 100 × spot²
        (sign: +calls, −puts per dealer-net-short-puts convention)
  - Aggregate by strike, build cumulative profile, detect zero-gamma flip
  - Output per-underlying JSON + combined snapshot

Reads:   nothing on disk (live CBOE pull)
Writes:  /root/jabbafx-data-pipeline/data/output/gex/<symbol>.json
         /root/jabbafx-data-pipeline/data/output/gex/latest.json
         /root/jabbafx-data-pipeline/logs/compute_gex.log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.

Decision rule served (Module 2A):
  "I will avoid initiating new index shorts when SPX is below zero-gamma
   flip (volatility regime suggests upside skew); switch when above."
"""

import argparse
import json
import logging
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests


BASE_DIR = Path("/root/jabbafx-data-pipeline")
OUTPUT_DIR = BASE_DIR / "data" / "output" / "gex"
LOG_PATH = BASE_DIR / "logs" / "compute_gex.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"

# 10 tracked underlyings (per roadmap)
TRACKED = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "MSFT", "AMD", "META", "GOOG"]
# (NDX dropped — CBOE only carries SPX-equivalent for index; SPY/QQQ/IWM cover
#  the main index ETFs which is what we actually trade against.)

CONTRACT_MULTIPLIER = 100   # standard equity options
MAX_DTE = 180               # days-to-expiration cap (beyond this, gamma decays to negligible)
PER_HOST_THROTTLE_SEC = 1.0


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


# OCC option symbol parser: <ROOT><YYMMDD><C|P><STRIKE×1000 padded 8 digits>
_OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


def parse_occ(symbol: str):
    if not symbol:
        return None
    m = _OCC_RE.match(symbol)
    if not m:
        return None
    root, yymmdd, type_, strike_raw = m.groups()
    yyyy = "20" + yymmdd[:2]
    expiration = f"{yyyy}-{yymmdd[2:4]}-{yymmdd[4:6]}"
    return {
        "root": root,
        "expiration": expiration,
        "type": type_,
        "strike": int(strike_raw) / 1000.0,
    }


def fetch_chain(symbol: str) -> dict:
    """Pull full CBOE options chain for one symbol."""
    url = CBOE_URL.format(symbol=symbol.upper())
    logging.info(f"GET {url}")
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def compute_for_symbol(symbol: str) -> dict:
    """Compute GEX profile for one underlying."""
    raw = fetch_chain(symbol)
    root = raw.get("data") or {}
    spot = root.get("current_price")
    if spot is None or not isinstance(spot, (int, float)) or spot <= 0:
        raise ValueError(f"{symbol}: no usable spot price")
    opts = root.get("options") or []
    if not opts:
        raise ValueError(f"{symbol}: empty options list")

    today = datetime.now(timezone.utc).date()
    spot_sq = spot * spot

    by_strike = {}      # strike → net GEX dollars (calls + put_signed)
    by_expiration = {}  # exp → GEX
    total_gex = 0.0
    total_call_gex = 0.0
    total_put_gex = 0.0
    total_call_oi = 0
    total_put_oi = 0
    contracts_used = 0
    skipped_no_gamma = 0
    skipped_dte = 0

    for o in opts:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        try:
            exp_date = datetime.strptime(p["expiration"], "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (exp_date - today).days
        if dte < 0 or dte > MAX_DTE:
            skipped_dte += 1
            continue
        gamma = float(o.get("gamma") or 0)
        oi = int(o.get("open_interest") or 0)
        if gamma == 0 or oi == 0:
            skipped_no_gamma += 1
            continue

        contract_gex = gamma * oi * CONTRACT_MULTIPLIER * spot_sq
        if p["type"] == "C":
            total_call_gex += contract_gex
            total_call_oi += oi
            net = +contract_gex
        else:
            total_put_gex += contract_gex
            total_put_oi += oi
            # Standard convention: dealers net-short puts → puts contribute NEGATIVE GEX
            net = -contract_gex

        total_gex += net
        by_strike[p["strike"]] = by_strike.get(p["strike"], 0.0) + net
        by_expiration[p["expiration"]] = by_expiration.get(p["expiration"], 0.0) + net
        contracts_used += 1

    # Build cumulative profile from lowest strike up
    sorted_strikes = sorted(by_strike.keys())
    cumulative = []
    running = 0.0
    for k in sorted_strikes:
        running += by_strike[k]
        cumulative.append((k, running))

    # Zero-gamma flip detection: strike where cumulative profile crosses zero
    # nearest to the current spot. Search in both directions from spot.
    flip = None
    flip_method = None
    prev_k, prev_cum = None, None
    for k, cum in cumulative:
        if prev_k is not None and (prev_cum * cum < 0):
            # Sign-change between prev_k and k; linear interpolation for the
            # zero-crossing strike
            denom = (cum - prev_cum)
            if denom != 0:
                interp = prev_k + (0 - prev_cum) / denom * (k - prev_k)
                # Pick the crossing closest to spot
                if flip is None or abs(interp - spot) < abs(flip - spot):
                    flip = round(interp, 2)
                    flip_method = "sign_change"
        prev_k, prev_cum = k, cum

    # Find the largest call-wall (most positive strike) and put-wall (most negative)
    call_wall = max(by_strike.items(), key=lambda kv: kv[1], default=(None, 0))
    put_wall = min(by_strike.items(), key=lambda kv: kv[1], default=(None, 0))

    # Top 10 strikes by absolute GEX (for the heatmap)
    top_strikes = sorted(by_strike.items(), key=lambda kv: abs(kv[1]), reverse=True)[:10]

    return {
        "symbol": symbol.upper(),
        "spot": spot,
        "data_timestamp": raw.get("timestamp"),
        "iv30": root.get("iv30"),
        "total_gex_dollars": round(total_gex, 0),
        "total_call_gex_dollars": round(total_call_gex, 0),
        "total_put_gex_dollars": round(total_put_gex, 0),
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "pc_oi_ratio": round(total_put_oi / total_call_oi, 3) if total_call_oi > 0 else None,
        "zero_gamma_flip": flip,
        "zero_gamma_flip_method": flip_method,
        "call_wall_strike": call_wall[0],
        "call_wall_gex": round(call_wall[1], 0),
        "put_wall_strike": put_wall[0],
        "put_wall_gex": round(put_wall[1], 0),
        "regime": "net_long_gamma" if total_gex > 0 else ("net_short_gamma" if total_gex < 0 else "balanced"),
        "contracts_used": contracts_used,
        "contracts_skipped_zero_gamma_or_oi": skipped_no_gamma,
        "contracts_skipped_dte_oor": skipped_dte,
        "strikes_count": len(by_strike),
        "expirations_count": len(by_expiration),
        # Slim arrays for the frontend
        "by_strike": [{"strike": k, "gex": round(by_strike[k], 0)} for k in sorted_strikes],
        "by_strike_cumulative": [{"strike": k, "cumulative_gex": round(v, 0)} for k, v in cumulative],
        "top_strikes": [{"strike": k, "gex": round(v, 0)} for k, v in top_strikes],
    }


def write_per_symbol(payload: dict) -> Path:
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


def write_combined_latest(all_results: list) -> Path:
    out_path = OUTPUT_DIR / "latest.json"
    snapshot = {
        "data_computed": datetime.now(timezone.utc).isoformat(
            timespec="seconds").replace("+00:00", "Z"),
        "underlyings_count": len(all_results),
        "underlyings": [],
    }
    for r in all_results:
        if not r:
            continue
        snapshot["underlyings"].append({
            "symbol": r["symbol"],
            "spot": r["spot"],
            "iv30": r["iv30"],
            "total_gex_dollars": r["total_gex_dollars"],
            "regime": r["regime"],
            "zero_gamma_flip": r["zero_gamma_flip"],
            "call_wall_strike": r["call_wall_strike"],
            "put_wall_strike": r["put_wall_strike"],
            "pc_oi_ratio": r["pc_oi_ratio"],
            "contracts_used": r["contracts_used"],
        })
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(snapshot, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


def print_summary(results: list, elapsed: float) -> None:
    print()
    print("=" * 92)
    print(f"COMPUTE-GEX SUMMARY  elapsed={elapsed:.1f}s")
    print("=" * 92)
    print(f"  {'sym':<6}{'spot':>9}{'total_gex':>20}{'regime':<18}"
          f"{'flip':>10}{'call_wall':>12}{'put_wall':>12}")
    for r in results:
        if not r:
            continue
        gex = r["total_gex_dollars"]
        gex_str = f"${gex:,.0f}" if gex < 1e9 else f"${gex/1e9:+.2f}B"
        flip = r["zero_gamma_flip"]
        flip_str = f"{flip:.2f}" if flip is not None else "—"
        cw = r["call_wall_strike"]
        pw = r["put_wall_strike"]
        print(f"  {r['symbol']:<6}{r['spot']:>9.2f}{gex_str:>20}{r['regime']:<18}"
              f"{flip_str:>10}{cw!s:>12}{pw!s:>12}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        help="compute single underlying (e.g. SPY); omit to compute all 10"
    )
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()

    targets = TRACKED if not args.symbol else [args.symbol.upper()]
    if args.symbol and args.symbol.upper() not in TRACKED:
        logging.info(f"running ad-hoc symbol {args.symbol.upper()} (not in TRACKED)")

    results = []
    for sym in targets:
        try:
            r = compute_for_symbol(sym)
        except Exception as e:
            logging.error(f"{sym}: failed: {e}")
            results.append(None)
            continue
        out = write_per_symbol(r)
        logging.info(
            f"{sym}: spot={r['spot']:.2f}  total_gex=${r['total_gex_dollars']:,.0f}  "
            f"flip={r['zero_gamma_flip']}  regime={r['regime']}  "
            f"contracts={r['contracts_used']} → {out.name}"
        )
        results.append(r)
        time.sleep(PER_HOST_THROTTLE_SEC)

    # Combined snapshot only when running all underlyings
    if not args.symbol:
        write_combined_latest(results)

    print_summary(results, time.monotonic() - start)
    return 0 if any(r for r in results) else 2


if __name__ == "__main__":
    sys.exit(main())
