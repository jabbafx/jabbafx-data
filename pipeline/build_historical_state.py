#!/usr/bin/env python3
"""
build_historical_state.py — Build 10-year archive of macro state vectors.

For each trading day back to 2016-01-01, computes a 10-dimensional macro
state vector plus SPY forward returns (5d/21d/63d/252d). Frontend uses
this archive to find historical analogs of today's macro configuration.

State vector dimensions (all normalized via z-score across full history):
  1. VIX level
  2. 2s10s spread (bps)
  3. 10Y real yield (DFII10, %)
  4. DXY level
  5. Net Liquidity = WALCL - RRPONTSYD - WTREGEN (\$T)
  6. HY OAS (BAMLH0A0HYM2, %)
  7. SPY 200d MA distance (%)
  8. USSLIND (Philadelphia Fed Leading Index, %)
  9. RSP/SPY ratio (breadth proxy)
 10. Copper/Gold ratio (HG/GC) — Dr. Copper recession indicator

Outputs:
  data/output/historical_state.json
  ~2,500 records, each: {date, vector_normalized[10], vector_raw[10],
                          spy_forward_5d, spy_forward_21d, spy_forward_63d,
                          spy_forward_252d}

Reads:   FRED API directly + our Netlify yahoo-quote Function
Writes:  /root/.../data/output/historical_state.json
         /root/.../logs/build_historical_state.log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.
"""

import argparse
import json
import logging
import math
import os
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

BASE_DIR = Path("/root/jabbafx-data-pipeline")
OUTPUT_DIR = BASE_DIR / "data" / "output"
LOG_PATH = BASE_DIR / "logs" / "build_historical_state.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
FRED_API_KEY = "54de9cf7b36ca97cebc53d7fc729b125"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
YAHOO_BASE = "https://jabbafx.netlify.app/.netlify/functions/yahoo-quote"

# Archive params
START_DATE = "2016-01-01"
FRED_SERIES = {
    "VIXCLS":       "vix",
    "DGS10":        "dgs10",
    "DGS2":         "dgs2",
    "DFII10":       "real10y",
    "WALCL":        "walcl",
    "RRPONTSYD":    "rrp",
    "WTREGEN":      "tga",
    "BAA10Y":       "hy_oas",   # Moody's Baa - 10Y spread (longer history than BAMLH0A0HYM2 under our FRED access; data back to 1986)
    "CFNAI":        "lei",   # Chicago Fed National Activity Index — composite of 85 indicators, reliable back to 1967
}
YAHOO_SYMBOLS = {
    "SPY":   "spy",
    "DX-Y.NYB": "dxy",
    "RSP":   "rsp",
    "HG=F":  "copper",
    "GC=F":  "gold",
}


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


# ─── FRED fetch (direct API) ──────────────────────────────────────────────

def fetch_fred(series_id: str) -> dict:
    """Fetch full FRED observation set from START_DATE. Returns {date: value}."""
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": START_DATE,
        "limit": 100000,
    }
    logging.info(f"FRED GET {series_id}")
    resp = requests.get(FRED_BASE, params=params, timeout=30,
                        headers={"User-Agent": UA})
    resp.raise_for_status()
    obs = resp.json().get("observations", [])
    out = {}
    for o in obs:
        try:
            v = float(o["value"])
            if math.isfinite(v):
                out[o["date"]] = v
        except (ValueError, TypeError):
            continue
    logging.info(f"  → {len(out)} observations")
    return out


# ─── Yahoo fetch (via our Netlify Function) ───────────────────────────────

def fetch_yahoo(symbol: str) -> dict:
    """Fetch 10y daily closes from yahoo-quote Function. Returns {date_str: close}."""
    logging.info(f"YAHOO GET {symbol}")
    resp = requests.get(YAHOO_BASE, params={
        "type": "chart",
        "symbol": symbol,
        "interval": "1d",
        "range": "10y",
    }, timeout=30, headers={"User-Agent": UA})
    resp.raise_for_status()
    data = resp.json()
    r = (data.get("chart") or {}).get("result", [{}])[0]
    timestamps = r.get("timestamp") or []
    closes = ((r.get("indicators") or {}).get("quote") or [{}])[0].get("close") or []
    out = {}
    for ts, cl in zip(timestamps, closes):
        if cl is None:
            continue
        date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
        out[date_str] = float(cl)
    logging.info(f"  → {len(out)} daily closes")
    return out


# ─── Computation helpers ──────────────────────────────────────────────────

def forward_fill(series: dict, all_dates: list) -> dict:
    """Forward-fill values for a series across all dates (handles weekend gaps)."""
    if not series:
        return {}
    sorted_keys = sorted(series.keys())
    out = {}
    last_val = None
    series_idx = 0
    for d in all_dates:
        # Advance series_idx while series date <= d
        while series_idx < len(sorted_keys) and sorted_keys[series_idx] <= d:
            last_val = series[sorted_keys[series_idx]]
            series_idx += 1
        out[d] = last_val
    return out


def compute_zscores(values: list) -> list:
    """Z-score a list of values (None preserved as None)."""
    valid = [v for v in values if v is not None and math.isfinite(v)]
    if len(valid) < 10:
        return [None] * len(values)
    mean = statistics.mean(valid)
    sd = statistics.pstdev(valid)
    if sd == 0:
        return [0.0 if v is not None else None for v in values]
    return [((v - mean) / sd if (v is not None and math.isfinite(v)) else None)
            for v in values]


def compute_sma200_distance(spy_closes_ordered: list) -> list:
    """For each day, distance (%) from SPY close to its trailing 200d SMA."""
    out = []
    for i, close in enumerate(spy_closes_ordered):
        if i < 199 or close is None:
            out.append(None)
            continue
        window = [c for c in spy_closes_ordered[i - 199:i + 1] if c is not None]
        if len(window) < 200:
            out.append(None)
            continue
        sma = sum(window) / len(window)
        if sma == 0:
            out.append(None)
            continue
        out.append(((close - sma) / sma) * 100)
    return out


# ─── Main archive build ───────────────────────────────────────────────────

def build_archive() -> dict:
    # Fetch all sources
    fred_data = {}
    for series_id, alias in FRED_SERIES.items():
        try:
            fred_data[alias] = fetch_fred(series_id)
        except Exception as e:
            logging.error(f"FRED {series_id} fetch failed: {e}")
            fred_data[alias] = {}
        time.sleep(0.3)  # polite rate-limit

    yahoo_data = {}
    for symbol, alias in YAHOO_SYMBOLS.items():
        try:
            yahoo_data[alias] = fetch_yahoo(symbol)
        except Exception as e:
            logging.error(f"Yahoo {symbol} fetch failed: {e}")
            yahoo_data[alias] = {}
        time.sleep(0.3)

    # Anchor to SPY trading days (most reliable equity calendar)
    spy_dates = sorted(yahoo_data["spy"].keys())
    if not spy_dates:
        raise RuntimeError("SPY data empty — cannot build archive")
    logging.info(f"SPY anchor: {len(spy_dates)} trading days "
                 f"({spy_dates[0]} → {spy_dates[-1]})")

    # Forward-fill all series onto SPY dates (handles weekend / month-end gaps)
    aligned = {}
    for alias, series in fred_data.items():
        aligned[alias] = forward_fill(series, spy_dates)
    for alias, series in yahoo_data.items():
        if alias == "spy":
            aligned[alias] = {d: series.get(d) for d in spy_dates}
        else:
            aligned[alias] = forward_fill(series, spy_dates)

    # Derived series
    # Net Liquidity = WALCL - RRPONTSYD - WTREGEN (all in millions of $; convert to $T)
    net_liq = []
    for d in spy_dates:
        w = aligned["walcl"][d]
        r = aligned["rrp"][d]
        t = aligned["tga"][d]
        if w is None or r is None or t is None:
            net_liq.append(None)
        else:
            # WALCL: millions; RRP: billions; TGA: millions
            net_liq.append((w / 1e6) - (r / 1e3) - (t / 1e6))

    # 2s10s spread (bps)
    s2s10s = []
    for d in spy_dates:
        a = aligned["dgs10"][d]
        b = aligned["dgs2"][d]
        if a is None or b is None:
            s2s10s.append(None)
        else:
            s2s10s.append((a - b) * 100)

    # SPY 200d SMA distance
    spy_closes_ordered = [aligned["spy"][d] for d in spy_dates]
    sma200_dist = compute_sma200_distance(spy_closes_ordered)

    # RSP/SPY ratio
    rsp_spy = []
    for d in spy_dates:
        rsp = aligned["rsp"][d]
        spy = aligned["spy"][d]
        if rsp is None or spy is None or spy == 0:
            rsp_spy.append(None)
        else:
            rsp_spy.append(rsp / spy)

    # Copper/Gold ratio
    cu_gold = []
    for d in spy_dates:
        hg = aligned["copper"][d]
        gc = aligned["gold"][d]
        if hg is None or gc is None or gc == 0:
            cu_gold.append(None)
        else:
            cu_gold.append(hg / gc)

    # DXY raw
    dxy = [aligned["dxy"][d] for d in spy_dates]

    # Raw VIX
    vix = [aligned["vix"][d] for d in spy_dates]

    # 10Y real yield raw
    real10y = [aligned["real10y"][d] for d in spy_dates]

    # HY OAS raw
    hy_oas = [aligned["hy_oas"][d] for d in spy_dates]

    # LEI raw
    lei = [aligned["lei"][d] for d in spy_dates]

    # Z-score normalizations
    vix_z      = compute_zscores(vix)
    dxy_z      = compute_zscores(dxy)
    net_liq_z  = compute_zscores(net_liq)
    rsp_spy_z  = compute_zscores(rsp_spy)
    cu_gold_z  = compute_zscores(cu_gold)
    lei_z      = compute_zscores(lei)
    # 2s10s + real10y + hy_oas + sma200_dist kept raw (already interpretable units)

    # SPY forward returns
    def fwd_return(idx, n):
        if idx + n >= len(spy_closes_ordered):
            return None
        cur = spy_closes_ordered[idx]
        fwd = spy_closes_ordered[idx + n]
        if cur is None or fwd is None or cur == 0:
            return None
        return ((fwd - cur) / cur) * 100

    # Build per-day records — skip days where any state vector dim is null
    records = []
    skipped = 0
    for i, d in enumerate(spy_dates):
        vec_raw = [
            vix[i], s2s10s[i], real10y[i], dxy[i], net_liq[i],
            hy_oas[i], sma200_dist[i], lei[i], rsp_spy[i], cu_gold[i],
        ]
        vec_norm = [
            vix_z[i], s2s10s[i], real10y[i], dxy_z[i], net_liq_z[i],
            hy_oas[i], sma200_dist[i], lei_z[i], rsp_spy_z[i], cu_gold_z[i],
        ]
        if any(v is None for v in vec_norm):
            skipped += 1
            continue
        rec = {
            "date": d,
            "vector_raw":        [round(v, 4) for v in vec_raw],
            "vector_normalized": [round(v, 4) for v in vec_norm],
            "spy_close":         round(spy_closes_ordered[i], 2),
            "spy_fwd_5d":   round(v, 3) if (v := fwd_return(i, 5))   is not None else None,
            "spy_fwd_21d":  round(v, 3) if (v := fwd_return(i, 21))  is not None else None,
            "spy_fwd_63d":  round(v, 3) if (v := fwd_return(i, 63))  is not None else None,
            "spy_fwd_252d": round(v, 3) if (v := fwd_return(i, 252)) is not None else None,
        }
        records.append(rec)

    logging.info(f"Built {len(records)} records (skipped {skipped} for null dims)")

    return {
        "data_computed_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "start_date": START_DATE,
        "anchor_symbol": "SPY",
        "dimensions": [
            "vix",             # 0 — z-scored
            "2s10s_bps",       # 1 — raw bps
            "real_10y_pct",    # 2 — raw %
            "dxy",             # 3 — z-scored
            "net_liquidity_T", # 4 — z-scored
            "hy_oas_pct",      # 5 — raw %
            "spy_sma200_dist_pct",  # 6 — raw %
            "cfnai",           # 7 — z-scored (Chicago Fed activity composite)
            "rsp_spy_ratio",   # 8 — z-scored
            "copper_gold",     # 9 — z-scored
        ],
        "normalized_dims": [0, 3, 4, 7, 8, 9],
        "raw_dims":        [1, 2, 5, 6],
        "records": records,
    }


def write_output(archive: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "historical_state.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(archive, tf, separators=(",", ":"))  # compact — file is large
        tmp = tf.name
    os.replace(tmp, out_path)
    size_kb = out_path.stat().st_size / 1024
    logging.info(f"Wrote {out_path} ({size_kb:.0f} KB, {len(archive['records'])} records)")
    return out_path


def main() -> int:
    setup_logger()
    ap = argparse.ArgumentParser()
    ap.parse_args()

    logging.info("=" * 70)
    logging.info(f"build_historical_state starting (start_date={START_DATE})")
    t0 = time.time()
    archive = build_archive()
    write_output(archive)
    elapsed = time.time() - t0
    logging.info(f"Completed in {elapsed:.1f}s")
    logging.info("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
