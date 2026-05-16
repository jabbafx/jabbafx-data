#!/usr/bin/env python3
"""
compute_vix_structure.py — VIX Term Structure + Volatility Regime (Module 3A).

Fetches the CBOE delayed-quotes VIX index payload (_VIX.json), extracts spot
VIX + the full VIX options chain across 13+ expirations, and derives the
market-implied forward VIX curve via put-call-parity synthetic-forward
calculation. Produces a term structure, contango/backwardation classifier,
regime label, and rolling history scaffold for variance-risk-premium (VRP)
and percentile-vs-history metrics.

Decision rule (Module 3A, locked):
  "Capitulation watch: VIX > 30 + backwardation (front > 3M) + extreme PCR
   (M2C |z| >= 2) + COT commercial net <5th percentile (M1C).
   3-of-4 = prepare contrarian long. 4-of-4 = execute size."

Synthetic forward calculation (per expiration):
  Put-call parity: F = K + (call_mid - put_mid) * exp(r*T)
  With r = 0 and using the strike K where |call - put| is minimized to
  reduce mispricing impact: forward_vix ~= K + (call_mid - put_mid)

  Practically: scan strikes, find K* where call_mid - put_mid is closest to
  zero, then forward = K* + (call_mid - put_mid at K*).

Term-structure classifier (per scan):
  front  = forward VIX at the closest expiration with DTE >= 7 days
  three_m = forward VIX at the expiration closest to 90 DTE
  shape  = "backwardation" if front > three_m
           "contango"      if front < three_m * 0.95
           "flat"          otherwise
  slope  = (three_m - front) / front

VIX regime classifier (spot only):
  < 12  : extreme_complacency
  12-15 : complacent
  15-20 : normal
  20-25 : elevated
  25-30 : high_stress
  >= 30 : panic

Historical metrics (activate as snapshots accumulate):
  - VIX percentile vs trailing N days  (activates after VIX_HIST_MIN_DAYS)
  - SPY 30d realized vol (for VRP calc) (activates after RV_HIST_MIN_DAYS)
  - VRP = VIX^2 - SPY_RV^2  (positive = options overpriced)

Reads:   CBOE delayed-quotes CDN (live)
Writes:  /root/.../data/output/vix_structure_recent.json
         /root/.../data/snapshots/vix/<today>/snapshot.json
         /root/.../logs/compute_vix_structure.log

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
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, pstdev

import requests


BASE_DIR = Path("/root/jabbafx-data-pipeline")
OUTPUT_DIR = BASE_DIR / "data" / "output"
SNAPSHOTS_DIR = BASE_DIR / "data" / "snapshots" / "vix"
LOG_PATH = BASE_DIR / "logs" / "compute_vix_structure.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"

# ─── Configuration ────────────────────────────────────────────────────────
FRONT_MIN_DTE       = 7    # exclude same-week expirations (settlement noise)
THREE_M_TARGET_DTE  = 90
BACK_TARGET_DTE     = 180  # back-month for full-curve shape stat
MAX_STRIKE_DEVIATION = 0.20  # ignore strikes >20% from forward estimate

VIX_HIST_MIN_DAYS   = 60   # history needed for percentile activation
RV_HIST_MIN_DAYS    = 30   # SPY closes needed for realized-vol calc

# Regime thresholds (spot VIX)
REGIME_BANDS = [
    (12.0, "extreme_complacency"),
    (15.0, "complacent"),
    (20.0, "normal"),
    (25.0, "elevated"),
    (30.0, "high_stress"),
]
REGIME_TOP = "panic"

# Term-structure thresholds
CONTANGO_FLAT_RATIO = 0.95   # front/3M between 0.95 and 1.00 = "flat"

# OCC option symbol (CBOE VIX uses the same OCC encoding as equity options)
_OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([CP])(\d{8})$")


def setup_logger() -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
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
    url = CBOE_URL.format(symbol=symbol)
    logging.info(f"GET {url}")
    resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ───── Forward VIX calculation (put-call parity) ──────────────────────────

def _mid(o: dict) -> float:
    bid = float(o.get("bid") or 0)
    ask = float(o.get("ask") or 0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2
    return float(o.get("last_trade_price") or 0)


def forward_vix_at_expiration(opts_at_exp: list, spot: float):
    """Compute synthetic forward VIX for a single expiration.

    Returns dict {forward, atm_strike, call_mid, put_mid, n_strikes_used}
    or None if insufficient data.

    Method: find strike K where |call_mid - put_mid| is minimized (most ATM
    by put-call parity). Then forward = K + (call_mid - put_mid).
    """
    by_strike: dict = {}
    for o in opts_at_exp:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        mid = _mid(o)
        if mid <= 0:
            continue
        by_strike.setdefault(p["strike"], {})[p["type"]] = mid

    paired = [(k, v) for k, v in by_strike.items() if "C" in v and "P" in v]
    if len(paired) < 3:
        return None

    # Filter to strikes within reasonable range of spot (avoid deep-OTM noise)
    if spot > 0:
        paired = [
            (k, v) for k, v in paired
            if abs(k - spot) / spot <= MAX_STRIKE_DEVIATION * 5  # wider for VIX
        ]
    if len(paired) < 3:
        return None

    # ATM = strike where |call - put| is minimized
    best = min(paired, key=lambda kv: abs(kv[1]["C"] - kv[1]["P"]))
    K = best[0]
    call_mid = best[1]["C"]
    put_mid = best[1]["P"]
    forward = K + (call_mid - put_mid)

    return {
        "forward": round(forward, 4),
        "atm_strike": K,
        "call_mid": round(call_mid, 4),
        "put_mid": round(put_mid, 4),
        "n_strikes_evaluated": len(paired),
    }


def build_term_structure(opts: list, spot: float, today_date) -> list:
    """Group options by expiration, compute forward VIX per expiration.

    Returns list of {expiration, dte, forward, atm_strike, ...} sorted by DTE.
    """
    by_exp: dict = {}
    for o in opts:
        p = parse_occ(o.get("option"))
        if not p:
            continue
        by_exp.setdefault(p["expiration"], []).append(o)

    curve = []
    for exp_str, exp_opts in by_exp.items():
        try:
            exp_d = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        dte = (exp_d - today_date).days
        if dte < 0:
            continue
        fwd = forward_vix_at_expiration(exp_opts, spot)
        if not fwd:
            continue
        curve.append({
            "expiration": exp_str,
            "dte": dte,
            **fwd,
        })

    curve.sort(key=lambda x: x["dte"])
    return curve


# ───── Classifiers ────────────────────────────────────────────────────────

def classify_regime(vix_spot: float) -> str:
    for threshold, label in REGIME_BANDS:
        if vix_spot < threshold:
            return label
    return REGIME_TOP


def classify_shape(curve: list):
    """Identify front-month and 3M, classify shape + slope."""
    if not curve:
        return None

    # Front = closest expiration with DTE >= FRONT_MIN_DTE
    eligible_front = [c for c in curve if c["dte"] >= FRONT_MIN_DTE]
    if not eligible_front:
        return None
    front = eligible_front[0]

    # 3M = expiration closest to THREE_M_TARGET_DTE among those after front
    later = [c for c in curve if c["dte"] > front["dte"]]
    if not later:
        return None
    three_m = min(later, key=lambda c: abs(c["dte"] - THREE_M_TARGET_DTE))

    front_f = front["forward"]
    tm_f = three_m["forward"]
    if front_f <= 0:
        return None
    ratio = front_f / tm_f if tm_f > 0 else None

    if ratio is None:
        shape = "unknown"
    elif front_f > tm_f:
        shape = "backwardation"
    elif ratio < CONTANGO_FLAT_RATIO:
        shape = "contango"
    else:
        shape = "flat"

    # Optional back-month point for full-curve display
    back = min(later, key=lambda c: abs(c["dte"] - BACK_TARGET_DTE)) \
        if len(later) > 1 else None

    return {
        "front_dte": front["dte"],
        "front_forward": front_f,
        "three_m_dte": three_m["dte"],
        "three_m_forward": tm_f,
        "back_dte": back["dte"] if back else None,
        "back_forward": back["forward"] if back else None,
        "slope_front_to_3m": round((tm_f - front_f) / front_f, 4),
        "ratio_front_to_3m": round(ratio, 4) if ratio else None,
        "shape": shape,
    }


# ───── Historical metrics (activate as snapshots accumulate) ──────────────

def list_historical_snapshots(today: str) -> list:
    if not SNAPSHOTS_DIR.exists():
        return []
    dates = []
    for d in sorted(SNAPSHOTS_DIR.iterdir()):
        if not d.is_dir() or d.name == today:
            continue
        if (d / "snapshot.json").exists():
            dates.append(d.name)
    return dates


def load_historical_vix(dates: list) -> list:
    """Return list of historical VIX spot values (oldest first)."""
    out = []
    for d in dates:
        path = SNAPSHOTS_DIR / d / "snapshot.json"
        try:
            with path.open() as f:
                snap = json.load(f)
            v = snap.get("vix_spot")
            if v is not None and v > 0:
                out.append(float(v))
        except Exception as e:
            logging.warning(f"could not read VIX history {path}: {e}")
    return out


def compute_vix_percentile(history: list, today_vix: float):
    if today_vix is None or len(history) < VIX_HIST_MIN_DAYS:
        return {
            "history_days": len(history),
            "percentile": None,
            "median": None,
            "is_low": False,
            "is_high": False,
            "note": (
                f"history-needs-{VIX_HIST_MIN_DAYS}-days "
                f"({len(history)}/{VIX_HIST_MIN_DAYS} so far)"
            ),
        }
    sorted_h = sorted(history)
    below = sum(1 for v in sorted_h if v < today_vix)
    pct = round(below / len(sorted_h) * 100, 1)
    med = round(sorted_h[len(sorted_h) // 2], 2)
    return {
        "history_days": len(history),
        "percentile": pct,
        "median": med,
        "is_low": pct <= 10,    # bottom 10% of historical VIX
        "is_high": pct >= 90,   # top 10% of historical VIX
        "note": f"percentile-rank vs trailing {len(history)} days",
    }


def load_historical_spy_closes(dates: list) -> list:
    """Return list of historical SPY spot values (chronological)."""
    out = []
    for d in dates:
        path = SNAPSHOTS_DIR / d / "snapshot.json"
        try:
            with path.open() as f:
                snap = json.load(f)
            s = snap.get("spy_spot")
            if s is not None and s > 0:
                out.append(float(s))
        except Exception:
            pass
    return out


def compute_vrp(history_spy: list, vix_spot: float):
    """Variance Risk Premium: implied (VIX^2) - realized (SPY 30d annualized var).

    Returns dict with realized_vol_pct, vrp_vol_points, regime.
    None when insufficient history.
    """
    if len(history_spy) < RV_HIST_MIN_DAYS or vix_spot is None or vix_spot <= 0:
        return {
            "history_days": len(history_spy),
            "realized_vol_pct": None,
            "vrp_vol_points": None,
            "regime": None,
            "note": (
                f"realized-vol-needs-{RV_HIST_MIN_DAYS}-spy-closes "
                f"({len(history_spy)}/{RV_HIST_MIN_DAYS} so far)"
            ),
        }
    window = history_spy[-RV_HIST_MIN_DAYS:]
    rets = []
    for i in range(1, len(window)):
        if window[i - 1] > 0:
            rets.append(math.log(window[i] / window[i - 1]))
    if len(rets) < 5:
        return {
            "history_days": len(history_spy),
            "realized_vol_pct": None,
            "vrp_vol_points": None,
            "regime": None,
            "note": "insufficient-returns",
        }
    daily_stdev = pstdev(rets)
    realized_vol_annualized = daily_stdev * math.sqrt(252) * 100
    vrp = vix_spot - realized_vol_annualized
    regime = (
        "implied-overpriced" if vrp > 4 else
        "implied-underpriced" if vrp < -2 else
        "fair"
    )
    return {
        "history_days": len(history_spy),
        "realized_vol_pct": round(realized_vol_annualized, 2),
        "vrp_vol_points": round(vrp, 2),
        "regime": regime,
        "note": f"30d realized vs implied (n={len(rets)} returns)",
    }


# ───── Persistence + output ───────────────────────────────────────────────

def save_snapshot(today: str, record: dict) -> Path:
    snap_dir = SNAPSHOTS_DIR / today
    snap_dir.mkdir(parents=True, exist_ok=True)
    path = snap_dir / "snapshot.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(snap_dir), delete=False
    ) as tf:
        json.dump(record, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, path)
    return path


def write_output_json(payload: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "vix_structure_recent.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


# ───── Orchestration ──────────────────────────────────────────────────────

def scan(today_str: str) -> dict:
    today_date = datetime.strptime(today_str, "%Y-%m-%d").date()

    # Primary: VIX
    try:
        raw = fetch_cboe("_VIX")
    except Exception as e:
        logging.error(f"VIX CBOE fetch failed: {e}")
        return {"status": "vix_fetch_failed", "error": str(e)}

    data = raw.get("data") or {}
    vix_spot = data.get("current_price")
    if not isinstance(vix_spot, (int, float)) or vix_spot <= 0:
        return {"status": "no_vix_spot"}
    opts = data.get("options") or []

    # Secondary: SPY spot (for VRP — same fetch infra)
    spy_spot = None
    try:
        spy_raw = fetch_cboe("SPY")
        spy_spot = (spy_raw.get("data") or {}).get("current_price")
        if not isinstance(spy_spot, (int, float)) or spy_spot <= 0:
            spy_spot = None
    except Exception as e:
        logging.warning(f"SPY fetch (for VRP) failed: {e}")

    # Term structure
    curve = build_term_structure(opts, vix_spot, today_date)
    shape = classify_shape(curve)
    regime = classify_regime(vix_spot)

    # Historical
    hist_dates = list_historical_snapshots(today_str)
    vix_history = load_historical_vix(hist_dates)
    vix_pct = compute_vix_percentile(vix_history, vix_spot)
    spy_history = load_historical_spy_closes(hist_dates)
    if spy_spot is not None:
        spy_history.append(spy_spot)  # include today's for the realized-vol window
    vrp = compute_vrp(spy_history, vix_spot)

    # Persist snapshot for tomorrow's history
    save_snapshot(today_str, {
        "scan_date": today_str,
        "vix_spot": round(vix_spot, 4),
        "spy_spot": round(spy_spot, 4) if spy_spot else None,
        "front_forward": shape["front_forward"] if shape else None,
        "three_m_forward": shape["three_m_forward"] if shape else None,
        "shape": shape["shape"] if shape else None,
        "regime": regime,
    })

    return {
        "status": "ok",
        "scan_date": today_str,
        "data_timestamp": raw.get("timestamp"),
        "vix": {
            "spot": round(vix_spot, 4),
            "price_change": round(float(data.get("price_change") or 0), 4),
            "price_change_percent": round(float(data.get("price_change_percent") or 0), 4),
            "prev_close": round(float(data.get("prev_day_close") or 0), 4),
            "regime": regime,
        },
        "spy_spot": round(spy_spot, 4) if spy_spot else None,
        "term_structure": {
            "curve": curve,
            "shape": shape,
        },
        "historical": {
            "vix_percentile": vix_pct,
            "variance_risk_premium": vrp,
        },
    }


def print_summary(result: dict, elapsed: float) -> None:
    logging.info("=" * 70)
    if result.get("status") != "ok":
        logging.info(f"FAIL status={result.get('status')} elapsed={elapsed:.1f}s")
        return
    v = result["vix"]
    ts = result["term_structure"]
    sh = ts.get("shape") or {}
    pct = result["historical"]["vix_percentile"]
    vrp = result["historical"]["variance_risk_premium"]

    logging.info(
        f"VIX {v['spot']:.2f} ({v['price_change_percent']:+.2f}%) "
        f"regime={v['regime']}"
    )
    if sh:
        logging.info(
            f"  Term structure: front({sh['front_dte']}d)={sh['front_forward']:.2f}  "
            f"3M({sh['three_m_dte']}d)={sh['three_m_forward']:.2f}  "
            f"shape={sh['shape']}  slope={sh['slope_front_to_3m']:+.4f}"
        )
        if sh.get("back_forward") is not None:
            logging.info(
                f"  Back({sh['back_dte']}d)={sh['back_forward']:.2f}  "
                f"({len(ts['curve'])} expirations in curve)"
            )
    if pct.get("percentile") is not None:
        logging.info(f"  VIX percentile: {pct['percentile']}% (median {pct['median']})")
    else:
        logging.info(f"  VIX history: {pct['note']}")
    if vrp.get("vrp_vol_points") is not None:
        logging.info(
            f"  VRP: implied {v['spot']:.2f} vs realized {vrp['realized_vol_pct']:.2f} "
            f"= {vrp['vrp_vol_points']:+.2f} pts ({vrp['regime']})"
        )
    else:
        logging.info(f"  VRP: {vrp['note']}")
    logging.info(f"  Scan time: {elapsed:.1f}s")
    logging.info("=" * 70)


def main() -> int:
    setup_logger()
    ap = argparse.ArgumentParser()
    ap.parse_args()  # accept --help; no flags currently

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logging.info(f"compute_vix_structure scan_date={today_str}")
    t0 = time.time()
    result = scan(today_str)
    elapsed = time.time() - t0

    payload = {
        "data_computed_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "scan_date": today_str,
        "decision_rule": (
            "Capitulation watch: VIX > 30 + backwardation (front > 3M) + extreme PCR "
            "(M2C |z| >= 2) + COT commercial net <5th percentile (M1C). "
            "3-of-4 align = prepare contrarian long. 4-of-4 = execute size."
        ),
        "thresholds": {
            "front_min_dte": FRONT_MIN_DTE,
            "three_m_target_dte": THREE_M_TARGET_DTE,
            "back_target_dte": BACK_TARGET_DTE,
            "regime_bands": REGIME_BANDS,
            "vix_hist_min_days": VIX_HIST_MIN_DAYS,
            "rv_hist_min_days": RV_HIST_MIN_DAYS,
            "contango_flat_ratio": CONTANGO_FLAT_RATIO,
        },
        "result": result,
    }
    out_path = write_output_json(payload)
    print_summary(result, elapsed)
    logging.info(f"wrote {out_path}")
    return 0 if result.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
