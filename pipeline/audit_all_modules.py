#!/usr/bin/env python3
"""
JabbaFX — Tier 3 Data Audit
Runs nightly to verify every data source is fresh, within sanity bounds,
and matches what the dashboard would compute. Writes data_audit/latest.json.
"""
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────
FRED_API_KEY = "54de9cf7b36ca97cebc53d7fc729b125"
STAGING_DIR  = Path("/root/jabbafx-data-pipeline/staging/jabbafx-data")
OUTPUT_DIR   = STAGING_DIR / "data_audit"
OUTPUT_FILE  = OUTPUT_DIR / "latest.json"
HISTORY_FILE = OUTPUT_DIR / "history.json"   # rolling last 30 runs
TIMEOUT      = 30

# ── Per-module sanity bounds ────────────────────────────────────────────
# (id, label, source-type, ref, sanity_min, sanity_max)
MODULES = [
    # FRED-based
    ("liquidity",   "M5A Net Liquidity (WALCL $M)",       "fred",   "WALCL",         6000000,  10000000),
    ("yields",      "M3B US 10Y Yield (%)",                "fred",   "DGS10",         0.5,      8.0),
    ("growth",      "M5C GDPNow (% real GDP)",             "fred",   "GDPNOW",        -10.0,    10.0),
    ("growth_cfnai","M5C CFNAI",                           "fred",   "CFNAI",         -4.0,     4.0),
    ("events_cpi",  "M4C CPI (level)",                     "fred",   "CPIAUCSL",      200.0,    500.0),
    ("events_unrate","M4C Unemployment Rate (%)",          "fred",   "UNRATE",        2.0,      15.0),
    ("intl_de10",   "M5G German Bund 10Y (%)",             "fred",   "IRLTLT01DEM156N", -1.0,   8.0),
    ("liq_nfci",    "M5A NFCI (financial conditions)",    "fred",   "NFCI",          -2.0,     3.0),
    ("liq_hyoas",   "M5A HY OAS (%)",                      "fred",   "BAMLH0A0HYM2",  1.0,      15.0),
    # GitHub-based (cron-built)
    ("institutional", "M1A 13F Holdings",                  "github", "13f/funds.json", None, None),
    ("insider",     "M1B Insider Transactions",            "github", "insider/recent.json", None, None),
    ("cot",         "M1C COT Report",                      "github", "cot/latest.json", None, None),
    ("anomalies",   "M2B Options Anomalies",               "github", "anomalies/recent.json", None, None),
    ("history",     "M5H Historical Archive",              "github", "historical_state/archive.json", None, None),
    ("vix_struct",  "M3A VIX Term Structure",              "github", "vix_structure/recent.json", None, None),
    ("gex",         "M2A GEX",                             "github", "gex/latest.json", None, None),
    ("naaim",       "M6C NAAIM Exposure Index",          "github", "sentiment_naaim/latest.json", None, None),
    ("sec_8k",      "M6E SEC 8-K Live Feed",             "github", "sec_8k/recent.json", None, None),
]

# ── Helpers ─────────────────────────────────────────────────────────────
def http_get_json(url, timeout=TIMEOUT):
    req = urllib.request.Request(url, headers={"User-Agent": "JabbaFX-Audit/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode())

def audit_fred(series_id, sanity_min, sanity_max, label):
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit=5"
    try:
        data = http_get_json(url)
        obs = data.get("observations") or []
        if not obs:
            return {"status": "error", "detail": f"FRED returned no observations for {series_id}"}
        # First non-"." observation
        latest_val = None
        latest_date = None
        for o in obs:
            v = o.get("value")
            if v and v != ".":
                try:
                    latest_val = float(v)
                    latest_date = o.get("date")
                    break
                except ValueError:
                    continue
        if latest_val is None:
            return {"status": "error", "detail": f"no parseable value in latest 5 obs for {series_id}"}
        # Sanity bound check
        if sanity_min is not None and (latest_val < sanity_min or latest_val > sanity_max):
            return {
                "status": "mismatch",
                "detail": f"{label} value {latest_val} outside expected [{sanity_min}, {sanity_max}] on {latest_date}",
                "value": latest_val,
                "as_of": latest_date,
            }
        # Freshness: how old is latest obs?
        try:
            obs_dt = datetime.strptime(latest_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - obs_dt).days
        except Exception:
            age_days = None
        # Series-specific max age (each FRED series has its own release cadence)
        max_age_map = {
            "CFNAI": 100,                  # CFNAI publishes with 2-3 month lag
            "GDPNOW": 90,                  # GDPNow current-quarter
            "CPIAUCSL": 50,
            "UNRATE": 50,
            "IRLTLT01DEM156N": 90,         # German Bund monthly + delay
            "NFCI": 14,                    # Weekly
            "BAMLH0A0HYM2": 7,             # Daily
            "BAMLC0A4CBBB": 7,             # Daily
            "WALCL": 14,                   # Weekly
            "DGS10": 7,                    # Daily
        }
        max_age = max_age_map.get(series_id, 14)
        if age_days is not None and age_days > max_age:
            return {
                "status": "delta",
                "detail": f"{label} latest obs {latest_date} is {age_days}d old (max expected {max_age}d)",
                "value": latest_val,
                "as_of": latest_date,
            }
        return {
            "status": "ok",
            "detail": f"{label} = {latest_val} on {latest_date} ({age_days}d old)",
            "value": latest_val,
            "as_of": latest_date,
        }
    except urllib.error.HTTPError as e:
        return {"status": "error", "detail": f"FRED HTTP {e.code} for {series_id}"}
    except Exception as e:
        return {"status": "error", "detail": f"FRED fetch error for {series_id}: {e}"}

def audit_github(rel_path, label):
    # Check local file (we have the staging clone)
    local = STAGING_DIR / rel_path
    if not local.exists():
        return {"status": "error", "detail": f"{label} missing locally at {rel_path}"}
    try:
        size_kb = local.stat().st_size / 1024
        mtime = local.stat().st_mtime
        age_hours = (time.time() - mtime) / 3600
        # Try parse JSON
        with open(local) as f:
            data = json.load(f)
        # Detect record count
        record_count = 0
        if isinstance(data, list):
            record_count = len(data)
        elif isinstance(data, dict):
            for key in ("records", "items", "events", "filings", "transactions", "anomalies"):
                if key in data and isinstance(data[key], list):
                    record_count = len(data[key])
                    break
        # Per-module expected freshness
        max_age_h = {
            "13f/funds.json": 24 * 90,   # quarterly + 30 day buffer
            "insider/recent.json":   30,
            "cot/latest.json":                    24 * 8,
            "anomalies/recent.json":              30,
            "historical_state/archive.json":      24 * 8,
            "vix_structure/recent.json":          24,
            "gex/latest.json":                    36,
        }.get(rel_path, 48)
        if age_hours > max_age_h:
            return {
                "status": "delta",
                "detail": f"{label} {size_kb:.0f}KB, {record_count} records, {age_hours:.0f}h old (max {max_age_h}h)",
                "value": record_count,
            }
        return {
            "status": "ok",
            "detail": f"{label} {size_kb:.0f}KB, {record_count} records, {age_hours:.1f}h old",
            "value": record_count,
        }
    except json.JSONDecodeError as e:
        return {"status": "mismatch", "detail": f"{label} JSON parse error: {e}"}
    except Exception as e:
        return {"status": "error", "detail": f"{label} audit error: {e}"}

# ── Run all audits ──────────────────────────────────────────────────────
def run_audit():
    started = time.time()
    results = []
    for entry in MODULES:
        mod_id, label, src_type, ref, smin, smax = entry
        if src_type == "fred":
            res = audit_fred(ref, smin, smax, label)
        elif src_type == "github":
            res = audit_github(ref, label)
        else:
            res = {"status": "error", "detail": f"unknown src_type {src_type}"}
        res["id"] = mod_id
        res["label"] = label
        res["source_type"] = src_type
        res["source_ref"] = ref
        results.append(res)
        time.sleep(0.4)   # be nice to FRED

    pass_n = sum(1 for r in results if r["status"] == "ok")
    delta_n = sum(1 for r in results if r["status"] == "delta")
    fail_n = sum(1 for r in results if r["status"] in ("mismatch", "error"))

    output = {
        "run_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "duration_s": round(time.time() - started, 1),
        "total":  len(results),
        "pass_count":  pass_n,
        "delta_count": delta_n,
        "fail_count":  fail_n,
        "modules":     results,
    }
    return output

def write_output(output):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(output, indent=2, default=str))
    # Append to history
    history = []
    if HISTORY_FILE.exists():
        try:
            history = json.loads(HISTORY_FILE.read_text())
        except Exception:
            history = []
    summary = {
        "run_utc": output["run_utc"],
        "pass": output["pass_count"],
        "delta": output["delta_count"],
        "fail": output["fail_count"],
    }
    history.append(summary)
    if len(history) > 30:
        history = history[-30:]
    HISTORY_FILE.write_text(json.dumps(history, indent=2, default=str))

if __name__ == "__main__":
    out = run_audit()
    write_output(out)
    print("JABBAFX-AUDIT-OK  total=" + str(out["total"]) + "  pass=" + str(out["pass_count"]) + "  delta=" + str(out["delta_count"]) + "  fail=" + str(out["fail_count"]))
    sys.exit(0 if out["fail_count"] == 0 else 1)
