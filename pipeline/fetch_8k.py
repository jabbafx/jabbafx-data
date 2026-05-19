#!/usr/bin/env python3
"""
M6E — SEC 8-K live feed
Polls SEC EDGAR Atom feed for recent 8-K filings, maps CIK → ticker via SEC
company_tickers.json, filters to tracked tickers. Outputs rolling 7-day window.
"""
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

REPO = Path("/root/jabbafx-data-pipeline/staging/jabbafx-data")
OUT  = REPO / "sec_8k"
OUT_FILE = OUT / "recent.json"
CIK_CACHE = OUT / "_cik_map.json"
TIMEOUT = 30

# 31 watchlist tickers (operator long-term thesis list)
WATCHLIST = [
    "NVDA","AAPL","MSFT","GOOG","GOOGL","META","AMZN","TSLA","AMD","NFLX",
    "AVGO","CRWD","PLTR","COIN","HOOD","SOFI","SHOP","MELI","SQ","PYPL",
    "DIS","ABNB","UBER","BABA","TSM","ASML","ORCL","ADBE","CRM","COST","WMT",
]
MEGA = [
    "BRK.B","JPM","V","MA","JNJ","PG","UNH","XOM","CVX","KO","PEP","HD",
    "MRK","LLY","ABBV","BAC","TMO","ABT","NKE","CSCO","ACN","WFC","DHR",
    "TXN","NEE","PM","CMCSA","T","VZ","INTC","QCOM","HON","UPS","RTX",
    "CAT","DE","GS","BLK","BA","IBM","MDT","C","AXP","GE","SCHW","SBUX","MMM",
]
TICKERS = set(t.upper() for t in WATCHLIST + MEGA)

FEED         = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=8-K&output=atom&count=100"
CIK_MAP_URL  = "https://www.sec.gov/files/company_tickers.json"
ATOM_NS      = {"a": "http://www.w3.org/2005/Atom"}
CIK_RE       = re.compile(r"\((\d{10})\)")
ITEM_RE      = re.compile(r"\b(\d+\.\d{2})\b")
USER_AGENT   = "JabbaFX/1.0 jordanmabbasi@gmail.com"


def http_get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
        return resp.read().decode("utf-8")


def load_cik_map():
    # Refresh if cache > 7 days old or missing
    needs_refresh = True
    if CIK_CACHE.exists():
        age_days = (datetime.now().timestamp() - CIK_CACHE.stat().st_mtime) / 86400
        if age_days < 7:
            needs_refresh = False
    if needs_refresh:
        print("[8K] refreshing CIK->ticker map from SEC")
        try:
            text = http_get(CIK_MAP_URL)
            CIK_CACHE.parent.mkdir(parents=True, exist_ok=True)
            CIK_CACHE.write_text(text)
        except Exception as e:
            print("[8K] CIK map fetch failed (using cached): " + str(e))
            if not CIK_CACHE.exists():
                return {}
    try:
        raw = json.loads(CIK_CACHE.read_text())
        # SEC format: { "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ... }
        # Return CIK string (zero-padded to 10) -> ticker
        out = {}
        for rec in raw.values():
            cik_str = str(rec.get("cik_str", "")).zfill(10)
            ticker = (rec.get("ticker") or "").upper()
            if cik_str and ticker:
                out[cik_str] = ticker
        return out
    except Exception as e:
        print("[8K] CIK map parse failed: " + str(e))
        return {}


def parse_atom(xml_text, cik_map):
    root = ET.fromstring(xml_text)
    entries = []
    for e in root.findall("a:entry", ATOM_NS):
        title   = (e.findtext("a:title", default="", namespaces=ATOM_NS) or "").strip()
        summary = (e.findtext("a:summary", default="", namespaces=ATOM_NS) or "").strip()
        updated = (e.findtext("a:updated", default="", namespaces=ATOM_NS) or "").strip()
        link_el = e.find("a:link", ATOM_NS)
        link    = link_el.get("href") if link_el is not None else ""
        # CIK from title: e.g. "8-K - CINCINNATI FINANCIAL CORP (0000020286) (Filer)"
        cik_match = CIK_RE.search(title)
        cik = cik_match.group(1) if cik_match else None
        ticker = cik_map.get(cik) if cik else None
        items = ITEM_RE.findall(summary)
        # Filer name: between "- " and " ("
        filer = ""
        if " - " in title:
            after_dash = title.split(" - ", 1)[1]
            filer = after_dash.split(" (", 1)[0].strip()
        entries.append({
            "title":   title[:200],
            "filer":   filer,
            "ticker":  ticker,
            "cik":     cik,
            "updated": updated,
            "link":    link,
            "items":   items,
        })
    return entries


def load_existing():
    if OUT_FILE.exists():
        try:
            return json.loads(OUT_FILE.read_text())
        except Exception:
            return None
    return None


def run():
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print("[8K] start " + now_iso)

    cik_map = load_cik_map()
    print("[8K] loaded CIK map with " + str(len(cik_map)) + " entries")

    try:
        xml_text = http_get(FEED)
    except Exception as e:
        print("[8K] ERROR fetching feed: " + str(e))
        return 1

    new_entries = parse_atom(xml_text, cik_map)
    print("[8K] parsed " + str(len(new_entries)) + " entries from feed")

    # Filter to tracked tickers
    tracked = [e for e in new_entries if e.get("ticker") in TICKERS]
    print("[8K] " + str(len(tracked)) + " entries match tracked tickers")

    # Merge with existing 7-day rolling window
    existing = load_existing()
    existing_filings = (existing or {}).get("filings", [])
    seen_links = set(e.get("link") for e in tracked)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat().replace("+00:00", "Z")
    for old in existing_filings:
        if old.get("link") in seen_links:
            continue
        if (old.get("updated") or "") >= cutoff:
            tracked.append(old)

    tracked.sort(key=lambda x: x.get("updated") or "", reverse=True)

    out = {
        "fetched_utc": now_iso,
        "filings": tracked[:100],
        "ticker_universe_size": len(TICKERS),
        "feed_entries_scanned": len(new_entries),
    }
    OUT.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(out, indent=2))
    count = len(out["filings"])
    print("[8K] wrote " + str(OUT_FILE) + " with " + str(count) + " filings (universe " + str(len(TICKERS)) + ")")
    return 0


if __name__ == "__main__":
    sys.exit(run())
