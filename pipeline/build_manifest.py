#!/usr/bin/env python3
"""
JabbaFX — Data Feed Manifest Builder

Writes data_audit/manifest.json: a frontend-friendly index of every
published data feed with its URL, last-updated timestamp, record count,
and per-tab grouping. Frontend fetches this once on load to drive
per-tab freshness badges and detect overdue feeds without having to
fetch every payload first.

Cheap, no network calls, safe to run after every cron commit.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# Same convention as audit_all_modules.py: hardcoded VPS staging path,
# fall back to repo root when running locally (Mac, CI, web sandbox).
VPS_STAGING_DIR = Path("/root/jabbafx-data-pipeline/staging/jabbafx-data")
REPO_DIR = VPS_STAGING_DIR if VPS_STAGING_DIR.exists() else Path(__file__).resolve().parent.parent
OUTPUT_FILE = REPO_DIR / "data_audit" / "manifest.json"

GITHUB_RAW_BASE = "https://raw.githubusercontent.com/jabbafx/jabbafx-data/main"

# (id, label, path, [timestamp_fields], [record_fields_or_None], max_age_hours, [tabs])
FEEDS = [
    ("sec_8k",          "SEC 8-K Live Feed",          "sec_8k/recent.json",
        ["fetched_utc"],                       ["filings"],     2,        ["watchlist"]),
    ("anomalies",       "Options Anomalies",          "anomalies/recent.json",
        ["data_computed", "data_computed_utc"], ["anomalies"],   30,       ["options"]),
    ("gex",             "GEX (Gamma Exposure)",       "gex/latest.json",
        ["data_computed", "data_computed_utc"], ["underlyings"], 36,       ["options"]),
    ("options_metrics", "Options Metrics (PCR/IV)",   "options_metrics/recent.json",
        ["data_computed_utc", "data_computed"], ["underlyings"], 30,       ["options"]),
    ("vix_struct",      "VIX Term Structure",         "vix_structure/recent.json",
        ["data_computed_utc", "data_computed"], None,            30,       ["options", "macro"]),
    ("history",         "Historical State Archive",   "historical_state/archive.json",
        ["data_computed_utc"],                  ["records"],     24 * 8,   ["macro"]),
    ("institutional",   "13F Holdings (Funds)",       "13f/funds.json",
        ["last_updated"],                       ["funds"],       24 * 100, ["institutional"]),
    ("insider",         "Form 4 Insider Transactions","insider/recent.json",
        ["data_computed"],                      ["interesting"], 30,       ["insider"]),
    ("cot",             "COT Report (Futures)",       "cot/latest.json",
        ["data_computed"],                      ["instruments"], 24 * 8,   ["commodities"]),
    ("naaim",           "NAAIM Exposure Index",       "sentiment_naaim/latest.json",
        ["fetched_utc"],                        ["history"],     24 * 8,   ["sentiment"]),
]


def parse_iso(s):
    if not isinstance(s, str):
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def build_feed_entry(feed_def, now_utc):
    fid, label, path, ts_fields, rec_fields, max_age_h, tabs = feed_def
    local = REPO_DIR / path
    entry = {
        "id": fid,
        "label": label,
        "path": path,
        "url": f"{GITHUB_RAW_BASE}/{path}",
        "tabs": tabs,
        "expected_max_age_hours": max_age_h,
    }
    if not local.exists():
        entry["status"] = "missing"
        entry["error"] = f"file not found at {path}"
        return entry

    stat = local.stat()
    entry["size_bytes"] = stat.st_size

    try:
        with open(local) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        entry["status"] = "parse_error"
        entry["error"] = str(e)
        return entry

    last_updated_str = None
    if isinstance(data, dict):
        for field in ts_fields:
            v = data.get(field)
            if v:
                last_updated_str = str(v)
                break
    if not last_updated_str:
        last_updated_str = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
    entry["last_updated"] = last_updated_str

    last_dt = parse_iso(last_updated_str)
    if last_dt is not None:
        if last_dt.tzinfo is None:
            last_dt = last_dt.replace(tzinfo=timezone.utc)
        age_hours = (now_utc - last_dt).total_seconds() / 3600
        entry["age_hours"] = round(age_hours, 2)
        entry["is_fresh"] = age_hours <= max_age_h
    else:
        entry["is_fresh"] = None

    record_count = None
    if isinstance(data, list):
        record_count = len(data)
    elif isinstance(data, dict) and rec_fields:
        for field in rec_fields:
            v = data.get(field)
            if isinstance(v, list):
                record_count = len(v)
                break
    if record_count is not None:
        entry["record_count"] = record_count

    if entry["is_fresh"] is True:
        entry["status"] = "ok"
    elif entry["is_fresh"] is False:
        entry["status"] = "stale"
    else:
        entry["status"] = "unknown"
    return entry


def build_manifest():
    now_utc = datetime.now(timezone.utc)
    feeds = [build_feed_entry(f, now_utc) for f in FEEDS]
    tabs = {}
    for feed in feeds:
        for tab in feed["tabs"]:
            tabs.setdefault(tab, []).append(feed["id"])
    return {
        "generated_at": now_utc.isoformat().replace("+00:00", "Z"),
        "base_url": GITHUB_RAW_BASE,
        "fresh_count": sum(1 for f in feeds if f.get("is_fresh") is True),
        "stale_count": sum(1 for f in feeds if f.get("is_fresh") is False),
        "missing_count": sum(1 for f in feeds if f.get("status") == "missing"),
        "feeds": feeds,
        "tabs": tabs,
    }


if __name__ == "__main__":
    manifest = build_manifest()
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(manifest, indent=2))
    rel = OUTPUT_FILE.relative_to(REPO_DIR)
    print(
        f"JABBAFX-MANIFEST-OK  feeds={len(manifest['feeds'])}  "
        f"fresh={manifest['fresh_count']}  "
        f"stale={manifest['stale_count']}  "
        f"missing={manifest['missing_count']}  -> {rel}"
    )
    sys.exit(0 if manifest["missing_count"] == 0 else 1)
