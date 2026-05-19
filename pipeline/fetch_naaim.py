#!/usr/bin/env python3
"""
M6C — NAAIM Exposure Index
Fetches NAAIM (National Association of Active Investment Managers) weekly
equity exposure data. Computes z-score vs trailing 5-year baseline + trend.

Source: NAAIM publishes weekly via their site at naaim.org. The historical
Excel/CSV is sometimes paywalled, but the recent weekly value is on the page.

Fallback strategy: try direct CSV/XLS endpoints first, then scrape page HTML.
"""
import json
import re
import sys
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path

REPO = Path("/root/jabbafx-data-pipeline/staging/jabbafx-data")
OUT  = REPO / "sentiment_naaim"
OUT_FILE = OUT / "latest.json"
TIMEOUT = 30
USER_AGENT = "JabbaFX/1.0 jordanmabbasi@gmail.com"

# Try multiple known endpoints — NAAIM has moved them around over time
ENDPOINTS = [
    "https://naaim.org/wp-content/uploads/naaim_exposure_index_history.csv",
    "https://www.naaim.org/wp-content/uploads/naaim_exposure_index_history.csv",
    "https://naaim.org/programs/naaim-exposure-index/",
]


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8", errors="replace")


def try_csv(url):
    """Attempt to parse CSV endpoint. Returns list of {date, value} or None."""
    try:
        text = http_get(url)
    except Exception as e:
        print("[NAAIM] CSV fetch " + url + " failed: " + str(type(e).__name__))
        return None
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip().strip('"') for p in line.split(",")]
        if len(parts) < 2:
            continue
        # Try common formats: "Date,Mean" or "Date,Value" or "Date,Exposure"
        d = parts[0]
        # Pick the last numeric field as the exposure value
        val = None
        for p in reversed(parts[1:]):
            try:
                val = float(p)
                break
            except ValueError:
                continue
        if val is None:
            continue
        # Normalize date: try multiple formats
        date_iso = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d", "%d-%b-%y", "%d-%b-%Y"):
            try:
                dt = datetime.strptime(d, fmt)
                date_iso = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        if not date_iso:
            continue
        rows.append({"date": date_iso, "value": val})
    if not rows:
        return None
    rows.sort(key=lambda r: r["date"])
    return rows


def try_scrape(url):
    """Scrape the NAAIM page for recent published values. Looks for table rows."""
    try:
        text = http_get(url)
    except Exception as e:
        print("[NAAIM] scrape " + url + " failed: " + str(type(e).__name__))
        return None
    # Look for date + numeric pairs in tables. NAAIM page has a recent-history table.
    # Match patterns like: 2026-05-14 ... 73.42
    # or m/d/YYYY ... numeric
    rows = []
    # Very tolerant — find date-like + number pairs
    date_num_re = re.compile(
        r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}-\d{2}-\d{2})[^0-9\-]+(-?\d+\.\d+)",
        re.IGNORECASE
    )
    for m in date_num_re.finditer(text):
        d_raw = m.group(1)
        val = float(m.group(2))
        # Skip values outside plausible NAAIM range
        if val < -100 or val > 250:
            continue
        date_iso = None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                date_iso = datetime.strptime(d_raw, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        if date_iso:
            rows.append({"date": date_iso, "value": val})
    # Dedupe + sort
    seen = set()
    uniq = []
    for r in rows:
        key = r["date"]
        if key not in seen:
            seen.add(key)
            uniq.append(r)
    uniq.sort(key=lambda r: r["date"])
    return uniq if uniq else None


def compute_stats(rows):
    """Z-score vs trailing baseline, trend over 4w."""
    if not rows or len(rows) < 8:
        return None
    values = [r["value"] for r in rows]
    # Use trailing ~260 weeks (5 years) for baseline, or all if shorter
    baseline = values[-min(260, len(values)):]
    mean = sum(baseline) / len(baseline)
    var  = sum((v - mean) ** 2 for v in baseline) / max(1, len(baseline) - 1)
    sd   = var ** 0.5
    latest = rows[-1]
    z = (latest["value"] - mean) / sd if sd else 0.0
    # 4-week trend
    trend_4w = None
    if len(rows) >= 5:
        trend_4w = latest["value"] - rows[-5]["value"]
    return {
        "latest":      latest,
        "z_score":     round(z, 2),
        "mean":        round(mean, 2),
        "sd":          round(sd, 2),
        "trend_4w":    round(trend_4w, 2) if trend_4w is not None else None,
        "baseline_n":  len(baseline),
        "extreme": (
            "greedy"      if z >= 1.5 else
            "complacent"  if z >= 0.5 else
            "fearful"     if z <= -1.5 else
            "cautious"    if z <= -0.5 else
            "normal"
        ),
    }


def run():
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print("[NAAIM] start " + now_iso)

    rows = None
    for url in ENDPOINTS:
        print("[NAAIM] trying " + url)
        if url.endswith(".csv"):
            rows = try_csv(url)
        else:
            rows = try_scrape(url)
        if rows and len(rows) > 8:
            print("[NAAIM] got " + str(len(rows)) + " observations from " + url)
            break

    if not rows:
        # Don't overwrite previous good output — emit error placeholder
        print("[NAAIM] ERROR — no data from any endpoint")
        # If existing output is good, leave it; otherwise emit error stub
        if OUT_FILE.exists():
            print("[NAAIM] keeping existing output")
            return 1
        OUT.mkdir(parents=True, exist_ok=True)
        OUT_FILE.write_text(json.dumps({
            "fetched_utc": now_iso,
            "error": "All endpoints failed",
            "latest": None,
        }, indent=2))
        return 1

    stats = compute_stats(rows)
    out = {
        "fetched_utc": now_iso,
        "rows_total":  len(rows),
        "history":     rows[-260:],   # last 5 years for sparkline
        **(stats or {}),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(out, indent=2))
    val = (stats or {}).get("latest", {}).get("value", "?")
    z = (stats or {}).get("z_score", "?")
    print("[NAAIM] wrote " + str(OUT_FILE) + " latest=" + str(val) + " z=" + str(z))
    return 0


if __name__ == "__main__":
    sys.exit(run())
