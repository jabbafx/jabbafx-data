#!/usr/bin/env python3
"""
compute_options_metrics.py — Put/Call Ratios + Implied Moves (Module 2C).

For each of 10 tracked underlyings, fetches the CBOE delayed-quotes options
chain (same source as Modules 2A GEX + 2B Anomalies — single fetch, three
analytics) and computes three families of metrics:

  1. Put/Call Ratios
       volume_pcr = Σ put volume / Σ call volume   (today's flow direction)
       oi_pcr     = Σ put OI     / Σ call OI       (cumulative positioning)

  2. ATM-Straddle Expected Move
       Pick the listed expiration closest to TARGET_DTE (default 30) with
       at least MIN_STRIKES_FOR_EXP traded strikes. At that expiration:
         atm_strike       = listed strike closest to spot
         expected_move_$  = ATM-call mid + ATM-put mid
         expected_move_%  = expected_move_$ / spot * 100

  3. Implied Probabilities (lognormal, drift = 0 / r=0 simplification)
       Using straddle IV as σ_annualized and T = DTE/365:
         P(S_T > spot · (1+p))   for p ∈ {0.05, 0.10, 0.20}
         P(S_T < spot · (1-p))   for p ∈ {0.05, 0.10, 0.20}
       Closed-form via normal-CDF on log returns.

Decision rule (Module 2C, locked):
  "I will fade the prevailing directional bias when an underlying's
   volume-PCR crosses 2σ above OR below its trailing 30-day mean — extreme
   positioning that historically reverts. Required confluence: signal must
   align with at least one of [GEX flip break, COT extreme percentile, 13F
   change]. No standalone PCR-only trades."

Output: per-underlying record stitched into a single JSON. Historical PCR
percentile activates after PCR_HIST_MIN_DAYS daily snapshots accumulate (so
the trailing-30-day baseline can be derived from real history rather than a
1-day cold start).

Reads:   CBOE delayed-quotes CDN (live)
Writes:  /root/.../data/output/options_metrics_recent.json
         /root/.../data/snapshots/options_pcr/<today>/<symbol>.json
         /root/.../logs/compute_options_metrics.log

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
SNAPSHOTS_DIR = BASE_DIR / "data" / "snapshots" / "options_pcr"
LOG_PATH = BASE_DIR / "logs" / "compute_options_metrics.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"

# Same 10 underlyings as Modules 2A + 2B — shared CBOE fetch
TRACKED = ["SPY", "QQQ", "IWM", "NVDA", "TSLA", "AAPL", "MSFT", "AMD", "META", "GOOG"]

# ─── Configuration ────────────────────────────────────────────────────────
TARGET_DTE              = 30      # prefer ~monthly expiration for straddle
DTE_SEARCH_MIN          = 14      # never use <14 DTE (gamma-distorted weeklies)
DTE_SEARCH_MAX          = 60      # never use >60 DTE (illiquid)
MIN_STRIKES_FOR_EXP     = 10      # expiration needs ≥10 strikes for ATM math
PCR_HIST_MIN_DAYS       = 20      # snapshots required before percentile activates
PCR_EXTREME_Z           = 2.0     # |z| ≥ 2σ flags extreme positioning

# OCC option symbol regex (mirror detect_anomalies.py / compute_gex.py)
_OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


# ───── Setup helpers ──────────────────────────────────────────────────────

def setup_logger() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


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


# ───── Math: normal CDF + lognormal implied probabilities ─────────────────

def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def prob_above_pct(spot, pct, sigma_ann, t_years):
    """P(S_T >= spot · (1 + pct)) under risk-neutral lognormal, r = 0.

    log(S_T/S_0) ~ N(-σ²T/2, σ²T). So P(S_T > k·S_0) = P(Z > z) where
    z = (log k + σ²T/2) / (σ√T).
    """
    if sigma_ann <= 0 or t_years <= 0 or spot <= 0:
        return None
    k = 1.0 + pct
    sigma_t = sigma_ann * math.sqrt(t_years)
    z = (math.log(k) + 0.5 * sigma_ann * sigma_ann * t_years) / sigma_t
    return 1.0 - norm_cdf(z)


def prob_below_pct(spot, pct, sigma_ann, t_years):
    """P(S_T <= spot · (1 - pct)). pct in (0, 1)."""
    if sigma_ann <= 0 or t_years <= 0 or spot <= 0 or pct >= 1.0:
        return None
    k = 1.0 - pct
    sigma_t = sigma_ann * math.sqrt(t_years)
    z = (math.log(k) + 0.5 * sigma_ann * sigma_ann * t_years) / sigma_t
    return norm_cdf(z)


# ───── PCR ────────────────────────────────────────────────────────────────

def compute_pcr(opts: list) -> dict:
    """Aggregate put/call volume + OI across the entire chain."""
    call_vol = put_vol = call_oi = put_oi = 0
    for o in opts:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        vol = int(o.get("volume") or 0)
        oi = int(o.get("open_interest") or 0)
        if p["type"] == "C":
            call_vol += vol
            call_oi += oi
        else:
            put_vol += vol
            put_oi += oi
    return {
        "total_call_volume": call_vol,
        "total_put_volume": put_vol,
        "total_call_oi": call_oi,
        "total_put_oi": put_oi,
        "volume_pcr": (put_vol / call_vol) if call_vol > 0 else None,
        "oi_pcr": (put_oi / call_oi) if call_oi > 0 else None,
    }


# ───── ATM straddle expected move ─────────────────────────────────────────

def pick_target_expiration(opts, today_date):
    """Pick the expiration closest to TARGET_DTE with enough strike depth.

    Filters to [DTE_SEARCH_MIN, DTE_SEARCH_MAX]. Among those, picks the one
    closest to TARGET_DTE. Returns 'YYYY-MM-DD' or None.
    """
    by_exp: dict = {}
    for o in opts:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        try:
            exp_d = datetime.strptime(p["expiration"], "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (exp_d - today_date).days
        if dte < DTE_SEARCH_MIN or dte > DTE_SEARCH_MAX:
            continue
        by_exp.setdefault(p["expiration"], set()).add(p["strike"])

    candidates = [
        (exp, len(strikes))
        for exp, strikes in by_exp.items()
        if len(strikes) >= MIN_STRIKES_FOR_EXP
    ]
    if not candidates:
        return None

    def distance(exp: str) -> int:
        d = datetime.strptime(exp, "%Y-%m-%d").date()
        return abs((d - today_date).days - TARGET_DTE)

    candidates.sort(key=lambda x: (distance(x[0]), -x[1]))
    return candidates[0][0]


def compute_expected_move(opts, spot, target_exp, today_date):
    """ATM straddle expected move at target expiration.

    Returns dict with atm_strike, expected_move_$, expected_move_%, straddle_iv, dte_used.
    """
    if spot <= 0 or not target_exp:
        return None
    try:
        exp_date = datetime.strptime(target_exp, "%Y-%m-%d").date()
    except ValueError:
        return None
    dte = (exp_date - today_date).days

    by_strike: dict = {}
    for o in opts:
        p = parse_occ(o.get("option"))
        if not p or p["expiration"] != target_exp:
            continue
        bid = float(o.get("bid") or 0)
        ask = float(o.get("ask") or 0)
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else float(o.get("last_trade_price") or 0)
        iv = float(o.get("iv") or 0)
        by_strike.setdefault(p["strike"], {})[p["type"]] = {"mid": mid, "iv": iv}

    paired_strikes = [k for k, v in by_strike.items() if "C" in v and "P" in v]
    if not paired_strikes:
        return None

    atm_strike = min(paired_strikes, key=lambda k: abs(k - spot))
    pair = by_strike[atm_strike]
    call_mid = pair["C"]["mid"]
    put_mid = pair["P"]["mid"]
    call_iv = pair["C"]["iv"]
    put_iv = pair["P"]["iv"]

    if call_mid <= 0 or put_mid <= 0:
        return None

    expected_move_dollars = call_mid + put_mid
    expected_move_pct = expected_move_dollars / spot * 100.0

    valid_ivs = [iv for iv in (call_iv, put_iv) if iv > 0]
    straddle_iv = mean(valid_ivs) if valid_ivs else None

    return {
        "atm_strike": atm_strike,
        "atm_call_mid": round(call_mid, 4),
        "atm_put_mid": round(put_mid, 4),
        "expected_move_dollars": round(expected_move_dollars, 4),
        "expected_move_pct": round(expected_move_pct, 4),
        "straddle_iv": round(straddle_iv, 6) if straddle_iv else None,
        "dte_used": dte,
    }


def compute_implied_probabilities(spot: float, sigma_ann: float, dte: int) -> dict:
    """Lognormal implied probabilities for ±5/10/20% moves by expiration."""
    if not sigma_ann or sigma_ann <= 0 or dte <= 0 or spot <= 0:
        return {p: None for p in (
            "prob_up_5pct", "prob_up_10pct", "prob_up_20pct",
            "prob_down_5pct", "prob_down_10pct", "prob_down_20pct",
        )}
    t = dte / 365.0
    return {
        "prob_up_5pct":   round(prob_above_pct(spot, 0.05, sigma_ann, t) or 0, 4),
        "prob_up_10pct":  round(prob_above_pct(spot, 0.10, sigma_ann, t) or 0, 4),
        "prob_up_20pct":  round(prob_above_pct(spot, 0.20, sigma_ann, t) or 0, 4),
        "prob_down_5pct":  round(prob_below_pct(spot, 0.05, sigma_ann, t) or 0, 4),
        "prob_down_10pct": round(prob_below_pct(spot, 0.10, sigma_ann, t) or 0, 4),
        "prob_down_20pct": round(prob_below_pct(spot, 0.20, sigma_ann, t) or 0, 4),
    }


# ───── Historical PCR baseline ────────────────────────────────────────────

def list_historical_pcr_dates(symbol: str, today: str) -> list:
    if not SNAPSHOTS_DIR.exists():
        return []
    dates = []
    for d in sorted(SNAPSHOTS_DIR.iterdir()):
        if not d.is_dir() or d.name == today:
            continue
        if (d / f"{symbol.upper()}.json").exists():
            dates.append(d.name)
    return dates


def load_historical_pcr(symbol: str, dates: list) -> list:
    """Return list of historical volume_pcr values (most recent first)."""
    out = []
    for d in reversed(dates):  # most recent first
        path = SNAPSHOTS_DIR / d / f"{symbol.upper()}.json"
        try:
            with path.open() as f:
                snap = json.load(f)
            v = snap.get("volume_pcr")
            if v is not None and v > 0:
                out.append(float(v))
        except Exception as e:
            logging.warning(f"could not read PCR history {path}: {e}")
    return out


def compute_pcr_historical_stats(history, today_pcr):
    """Compute trailing-30-day mean + stdev + z-score for today's PCR."""
    if today_pcr is None or len(history) < PCR_HIST_MIN_DAYS:
        return {
            "history_days": len(history),
            "mean_30d": None,
            "stdev_30d": None,
            "z_score": None,
            "is_extreme": False,
            "regime_note": (
                f"history-needs-{PCR_HIST_MIN_DAYS}-days "
                f"({len(history)}/{PCR_HIST_MIN_DAYS} so far)"
            ),
        }
    window = history[:30]
    m = mean(window)
    sd = pstdev(window)
    if sd <= 0:
        return {
            "history_days": len(history),
            "mean_30d": round(m, 4),
            "stdev_30d": 0.0,
            "z_score": None,
            "is_extreme": False,
            "regime_note": "stdev-zero",
        }
    z = (today_pcr - m) / sd
    return {
        "history_days": len(history),
        "mean_30d": round(m, 4),
        "stdev_30d": round(sd, 4),
        "z_score": round(z, 3),
        "is_extreme": abs(z) >= PCR_EXTREME_Z,
        "regime_note": (
            "extreme-fear-contrarian-long" if z >= PCR_EXTREME_Z else
            "extreme-greed-contrarian-short" if z <= -PCR_EXTREME_Z else
            "neutral"
        ),
    }


# ───── Per-symbol orchestration ───────────────────────────────────────────

def save_pcr_snapshot(symbol: str, record: dict, today: str) -> Path:
    snap_dir = SNAPSHOTS_DIR / today
    snap_dir.mkdir(parents=True, exist_ok=True)
    path = snap_dir / f"{symbol.upper()}.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(snap_dir), delete=False
    ) as tf:
        json.dump(record, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, path)
    return path


def scan_one(symbol: str, today_str: str) -> dict:
    today_date = datetime.strptime(today_str, "%Y-%m-%d").date()
    try:
        raw = fetch_cboe(symbol)
    except Exception as e:
        logging.error(f"{symbol}: CBOE fetch failed: {e}")
        return {"symbol": symbol, "status": "fetch_failed", "error": str(e)}

    data = raw.get("data") or {}
    spot = data.get("current_price")
    if not isinstance(spot, (int, float)) or spot <= 0:
        return {"symbol": symbol, "status": "no_spot"}

    opts = data.get("options") or []
    if not opts:
        return {"symbol": symbol, "status": "no_options"}

    # PCR
    pcr = compute_pcr(opts)

    # Expected move
    target_exp = pick_target_expiration(opts, today_date)
    em = compute_expected_move(opts, spot, target_exp, today_date) if target_exp else None

    # Implied probabilities
    if em and em.get("straddle_iv") and em.get("dte_used"):
        probs = compute_implied_probabilities(spot, em["straddle_iv"], em["dte_used"])
    else:
        probs = compute_implied_probabilities(spot, 0, 0)  # all None

    # Historical PCR stats
    hist_dates = list_historical_pcr_dates(symbol, today_str)
    history = load_historical_pcr(symbol, hist_dates)
    pcr_stats = compute_pcr_historical_stats(history, pcr.get("volume_pcr"))

    record = {
        "symbol": symbol,
        "status": "ok",
        "spot": round(float(spot), 4),
        "scan_date": today_str,
        "data_timestamp": raw.get("timestamp"),
        "pcr": pcr,
        "expected_move": em,
        "implied_probabilities": probs,
        "pcr_historical": pcr_stats,
    }

    # Persist for tomorrow's percentile baseline
    save_pcr_snapshot(symbol, {
        "symbol": symbol,
        "scan_date": today_str,
        "volume_pcr": pcr.get("volume_pcr"),
        "oi_pcr": pcr.get("oi_pcr"),
        "expected_move_pct": em.get("expected_move_pct") if em else None,
    }, today_str)

    return record


# ───── Output ─────────────────────────────────────────────────────────────

def write_output_json(records: list, today_str: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "options_metrics_recent.json"

    ok = [r for r in records if r.get("status") == "ok"]
    aggregate_vol_pcr = None
    aggregate_oi_pcr = None
    if ok:
        tot_cv = sum(r["pcr"]["total_call_volume"] for r in ok)
        tot_pv = sum(r["pcr"]["total_put_volume"] for r in ok)
        tot_co = sum(r["pcr"]["total_call_oi"] for r in ok)
        tot_po = sum(r["pcr"]["total_put_oi"] for r in ok)
        aggregate_vol_pcr = round(tot_pv / tot_cv, 4) if tot_cv > 0 else None
        aggregate_oi_pcr = round(tot_po / tot_co, 4) if tot_co > 0 else None

    payload = {
        "data_computed_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scan_date": today_str,
        "underlyings_scanned": len(records),
        "underlyings_ok": len(ok),
        "decision_rule": (
            "Fade prevailing bias when |z_30d(volume_pcr)| ≥ 2.0 AND signal "
            "aligns with at least one of [GEX flip break, COT extreme, 13F change]. "
            "No standalone PCR-only trades."
        ),
        "thresholds": {
            "target_dte": TARGET_DTE,
            "dte_search_min": DTE_SEARCH_MIN,
            "dte_search_max": DTE_SEARCH_MAX,
            "min_strikes_for_exp": MIN_STRIKES_FOR_EXP,
            "pcr_hist_min_days": PCR_HIST_MIN_DAYS,
            "pcr_extreme_z": PCR_EXTREME_Z,
        },
        "aggregate": {
            "volume_pcr": aggregate_vol_pcr,
            "oi_pcr": aggregate_oi_pcr,
        },
        "underlyings": records,
    }

    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


def print_summary(records: list, elapsed: float) -> None:
    logging.info("=" * 70)
    logging.info(f"Scanned {len(records)} underlyings in {elapsed:.1f}s")
    for r in records:
        if r.get("status") != "ok":
            logging.info(f"  {r['symbol']:5}  {r.get('status', 'unknown')}")
            continue
        sym = r["symbol"]
        spot = r["spot"]
        vp = r["pcr"]["volume_pcr"]
        op = r["pcr"]["oi_pcr"]
        em = r.get("expected_move") or {}
        emp = em.get("expected_move_pct")
        dte = em.get("dte_used")
        hist = r.get("pcr_historical") or {}
        z = hist.get("z_score")
        regime = hist.get("regime_note", "")

        vp_s = f"{vp:.2f}" if vp is not None else "—"
        op_s = f"{op:.2f}" if op is not None else "—"
        emp_s = f"±{emp:.1f}%" if emp is not None else "—"
        dte_s = f"{dte}DTE" if dte else ""
        z_s = f"z={z:+.2f}" if z is not None else "no-hist"

        logging.info(
            f"  {sym:5}  spot={spot:8.2f}  "
            f"volPCR={vp_s:>5}  oiPCR={op_s:>5}  "
            f"EM={emp_s:>6} {dte_s:>5}  {z_s}  {regime}"
        )
    logging.info("=" * 70)


# ───── Main ───────────────────────────────────────────────────────────────

def main() -> int:
    setup_logger()

    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", help="scan single symbol only (test mode)")
    args = ap.parse_args()

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    symbols = [args.symbol.upper()] if args.symbol else TRACKED

    logging.info(f"compute_options_metrics scan_date={today_str} symbols={symbols}")
    t0 = time.time()
    records = [scan_one(s, today_str) for s in symbols]
    elapsed = time.time() - t0

    out_path = write_output_json(records, today_str)
    print_summary(records, elapsed)
    logging.info(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
