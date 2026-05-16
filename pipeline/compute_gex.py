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
import math
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests


# ───── Black-Scholes gamma (for hypothetical-spot flip recomputation) ─────
RISK_FREE_RATE = 0.045   # ~current US 3M T-bill yield; refresh occasionally
FLIP_GRID_RANGE = 0.20   # scan ±20% from spot for the flip
FLIP_GRID_POINTS = 200   # resolution of the search


def _norm_pdf(x: float) -> float:
    """Standard normal probability density function."""
    return math.exp(-x * x / 2.0) / math.sqrt(2.0 * math.pi)


def bs_gamma(spot: float, strike: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes gamma for European call OR put (same formula)."""
    if sigma <= 0 or T <= 0 or spot <= 0 or strike <= 0:
        return 0.0
    sqrtT = math.sqrt(T)
    d1 = (math.log(spot / strike) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrtT)
    return _norm_pdf(d1) / (spot * sigma * sqrtT)


def compute_flip_bs(contracts_for_bs, spot, fallback_iv):
    """Find hypothetical spot where total GEX = 0.
    Holds OI + IV constant, recomputes gamma at each candidate spot.

    contracts_for_bs is a list of dicts: {strike, type, oi, iv, dte_years}
    Contracts with iv<=0 use the underlying's fallback_iv (typically IV30/100).
    Contracts with dte_years<=0 are skipped.
    Returns hypothetical spot (float) where total GEX flips sign, or None.
    """
    if not contracts_for_bs:
        return None

    def total_gex_at(hyp_spot):
        total = 0.0
        for c in contracts_for_bs:
            T = c["dte_years"]
            if T <= 0:
                continue
            sigma = c["iv"] if c["iv"] and c["iv"] > 0 else fallback_iv
            if sigma <= 0:
                continue
            g = bs_gamma(hyp_spot, c["strike"], T, RISK_FREE_RATE, sigma)
            contract_gex = g * c["oi"] * CONTRACT_MULTIPLIER * hyp_spot * hyp_spot
            if c["type"] == "C":
                total += contract_gex
            else:
                total -= contract_gex
        return total

    grid_lo = spot * (1 - FLIP_GRID_RANGE)
    grid_hi = spot * (1 + FLIP_GRID_RANGE)
    points = []
    for i in range(FLIP_GRID_POINTS + 1):
        hyp = grid_lo + (grid_hi - grid_lo) * i / FLIP_GRID_POINTS
        points.append((hyp, total_gex_at(hyp)))

    # First sign change
    best_flip = None
    for i in range(1, len(points)):
        a_s, a_g = points[i - 1]
        b_s, b_g = points[i]
        if a_g * b_g < 0:
            if (b_g - a_g) != 0:
                interp = a_s + (0 - a_g) / (b_g - a_g) * (b_s - a_s)
                if best_flip is None or abs(interp - spot) < abs(best_flip - spot):
                    best_flip = round(interp, 2)
    return best_flip


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
    iv30 = root.get("iv30") or 0.0
    # IV30 from CBOE is a percentage (e.g. 15.1). BS uses decimal (0.151).
    fallback_iv = iv30 / 100.0 if iv30 > 1 else iv30

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

    # Also collect a parallel view for BS hypothetical-spot flip recomputation.
    # This includes contracts with OI > 0 EVEN if CBOE reports gamma=0 (the BS
    # recompute will fill in their gamma at hypothetical spot levels).
    bs_contracts = []

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
        if oi == 0:
            skipped_no_gamma += 1
            continue
        iv_contract = float(o.get("iv") or 0)

        # BS-view inclusion: any contract with OI > 0 and a usable IV (per-contract
        # or fallback). Gives the BS flip search a fuller picture than just contracts
        # with non-zero CBOE gamma.
        usable_iv = iv_contract if iv_contract > 0 else fallback_iv
        if usable_iv > 0 and dte > 0:
            bs_contracts.append({
                "strike": p["strike"],
                "type": p["type"],
                "oi": oi,
                "iv": iv_contract,
                "dte_years": dte / 365.0,
            })

        # Static (Session 2 method): only count contracts where CBOE provided gamma
        if gamma == 0:
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

    # Session 3 — refined zero-gamma flip via Black-Scholes recomputation at
    # candidate hypothetical spot levels (the standard SpotGamma-style method).
    # The Session 2 "cumulative" flip is kept for comparison; the BS flip is
    # the canonical answer for the decision rule.
    bs_flip = compute_flip_bs(bs_contracts, spot, fallback_iv)

    return {
        "symbol": symbol.upper(),
        "spot": spot,
        "data_timestamp": raw.get("timestamp"),
        "iv30": root.get("iv30"),
        "fallback_iv_used": fallback_iv,
        "total_gex_dollars": round(total_gex, 0),
        "total_call_gex_dollars": round(total_call_gex, 0),
        "total_put_gex_dollars": round(total_put_gex, 0),
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "pc_oi_ratio": round(total_put_oi / total_call_oi, 3) if total_call_oi > 0 else None,
        "zero_gamma_flip": flip,
        "zero_gamma_flip_method": flip_method,
        "zero_gamma_flip_bs": bs_flip,
        "bs_contracts_in_flip_calc": len(bs_contracts),
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
            "zero_gamma_flip_bs": r["zero_gamma_flip_bs"],
            "call_wall_strike": r["call_wall_strike"],
            "put_wall_strike": r["put_wall_strike"],
            "pc_oi_ratio": r["pc_oi_ratio"],
            "contracts_used": r["contracts_used"],
            "bs_contracts_in_flip_calc": r["bs_contracts_in_flip_calc"],
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
    print(f"  {'sym':<6}{'spot':>9}{'total_gex':>14}{'regime':<18}"
          f"{'flip_cum':>10}{'flip_bs':>10}{'call_wall':>12}{'put_wall':>12}")
    for r in results:
        if not r:
            continue
        gex = r["total_gex_dollars"]
        gex_str = f"${gex/1e9:+.2f}B"
        flip = r["zero_gamma_flip"]
        fbs = r["zero_gamma_flip_bs"]
        flip_str = f"{flip:.2f}" if flip is not None else "—"
        fbs_str = f"{fbs:.2f}" if fbs is not None else "—"
        cw = r["call_wall_strike"]
        pw = r["put_wall_strike"]
        print(f"  {r['symbol']:<6}{r['spot']:>9.2f}{gex_str:>14}{r['regime']:<18}"
              f"{flip_str:>10}{fbs_str:>10}{cw!s:>12}{pw!s:>12}")


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
