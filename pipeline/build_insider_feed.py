#!/usr/bin/env python3
"""
build_insider_feed.py — Cross-issuer Form 4 orchestrator + interesting filter
                       + 13F confluence cross-reference.

Module 1B Session 3. Pipeline:
  1. Read 13f/confluence/<quarter>-analysis.json (from VPS-side staged clone)
  2. Pick top-N tickers by funds_holding
  3. Resolve each ticker → issuer CIK via cached company_tickers.json
  4. For each issuer: fetch_form4 (recent N days) → parse_form4 → filter
  5. Apply "interesting" rule and cross-reference funds_holding
  6. Emit data/output/insider_recent.json

Reads:
  /root/jabbafx-data-pipeline/staging/jabbafx-data/13f/confluence/*-analysis.json
  /root/jabbafx-data-pipeline/data/cache/company_tickers.json
  (plus raw Form 4 XMLs written by fetch_form4 in this run)

Writes:
  /root/jabbafx-data-pipeline/data/output/insider_recent.json
  /root/jabbafx-data-pipeline/data/raw/form4/<issuer-cik>/*.xml (via fetch_form4)
  /root/jabbafx-data-pipeline/data/output/form4/<issuer-cik>.json (per-issuer)
  /root/jabbafx-data-pipeline/logs/build_insider_feed.log

"Interesting" filter (Module 1B decision rule):
  - Reporting owner is director OR officer
  - Transaction code is P (open-market buy) or S (open-market sale)
  - If code is S: NOT marked aff_10b5_one (skip pre-planned sales)
  - Dollar value of transaction ≥ $250,000
  - All other codes (F tax, A grant, G gift, M/X exercise, D disposition, V) skipped

Decision rule served:
  "I will look harder at a 13F-confluence-≥6 position when ≥1 insider buys
   $250K+ in the past 30 days."
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Locate sibling parser modules
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import fetch_form4
import parse_form4


BASE_DIR = Path("/root/jabbafx-data-pipeline")
CACHE_DIR = BASE_DIR / "data" / "cache"
OUTPUT_DIR = BASE_DIR / "data" / "output"
STAGED_CLONE = BASE_DIR / "staging" / "jabbafx-data"
LOG_PATH = BASE_DIR / "logs" / "build_insider_feed.log"

DEFAULT_QUARTER = "2026-Q1"
DEFAULT_DAYS = 30
DEFAULT_TOP_N = 30
MIN_VALUE_USD = 250_000


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


# ── Loaders ───────────────────────────────────────────────────────────────

def load_confluence(quarter: str) -> dict:
    p = STAGED_CLONE / "13f" / "confluence" / f"{quarter}-analysis.json"
    if not p.exists():
        raise FileNotFoundError(
            f"no 13F confluence JSON at {p}; ensure staged clone is up-to-date"
        )
    return json.loads(p.read_text())


def load_ticker_to_cik() -> dict:
    p = CACHE_DIR / "company_tickers.json"
    if not p.exists():
        raise FileNotFoundError(
            f"no ticker cache at {p}; fetch from "
            "https://www.sec.gov/files/company_tickers.json first"
        )
    raw = json.loads(p.read_text())
    # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "..."}, ...}
    out = {}
    for v in raw.values():
        t = v.get("ticker")
        c = v.get("cik_str")
        if t and c:
            out[t.upper()] = str(c).zfill(10)
    return out


# ── Ticker selection ──────────────────────────────────────────────────────

def select_tickers(
    confluence: dict, top_n: int, min_funds: int = 3
) -> list:
    """Return top-N (ticker, name, funds_holding, signal_strength) tuples from
    confluence high_confluence list, skipping entries with null ticker.
    Sorted by funds_holding desc, then total_value desc."""
    entries = confluence.get("high_confluence", [])
    candidates = []
    for e in entries:
        t = e.get("ticker")
        if not t:
            continue
        if e.get("funds_holding", 0) < min_funds:
            continue
        candidates.append({
            "ticker": t,
            "name": e.get("name"),
            "funds_holding": e.get("funds_holding", 0),
            "total_value": e.get("total_value", 0),
            "signal_strength": e.get("signal_strength", "low"),
        })
    candidates.sort(
        key=lambda c: (-c["funds_holding"], -c["total_value"])
    )
    return candidates[:top_n]


# ── Per-issuer pipeline ───────────────────────────────────────────────────

def process_issuer(
    issuer_cik: str, days: int, force: bool
) -> tuple:
    """For one issuer: fetch recent Form 4s, parse them. Returns
    (n_fetched, n_filings_parsed)."""
    try:
        filings_meta = fetch_form4.list_form4_filings(issuer_cik, days)
    except Exception as e:
        logging.warning(f"CIK {issuer_cik}: list_form4_filings failed: {e}")
        return (0, 0)

    n_fetched = 0
    for filing in filings_meta:
        r = fetch_form4.fetch_one_filing(
            issuer_cik, filing, force=force, dry_run=False
        )
        if r.get("status") == "fetched":
            n_fetched += 1

    parsed = parse_form4.parse_issuer_dir(issuer_cik)
    # Persist per-issuer JSON for downstream re-use
    if parsed:
        parse_form4.write_issuer_json(issuer_cik, parsed)
    return (n_fetched, len(parsed))


# ── Interesting filter ────────────────────────────────────────────────────

def reporting_role_label(ro: dict) -> str:
    parts = []
    if ro.get("is_director"):
        parts.append("director")
    if ro.get("is_officer"):
        title = ro.get("officer_title")
        parts.append(f"officer ({title})" if title else "officer")
    if ro.get("is_ten_percent_owner"):
        parts.append("10%+")
    return "/".join(parts) if parts else "none"


def is_interesting(filing: dict, tx: dict) -> bool:
    """Module 1B decision rule."""
    ro = filing.get("reporting_owner", {})
    if not (ro.get("is_director") or ro.get("is_officer")):
        return False
    code = tx.get("transaction_code")
    value = tx.get("value")
    if value is None or value < MIN_VALUE_USD:
        return False
    if code == "P":
        return True  # open-market buy by officer/director — always interesting
    if code == "S":
        # Open-market sale — only interesting if NOT pre-planned
        return not filing.get("aff_10b5_one", False)
    return False


def build_interesting_records(
    parsed_filings: list, ticker: str, issuer_meta: dict,
    confluence_lookup: dict
) -> list:
    """Walk parsed filings → interesting transaction records."""
    out = []
    conf = confluence_lookup.get(ticker, {})
    for filing in parsed_filings:
        ro = filing.get("reporting_owner", {})
        for tx in filing.get("non_derivative_transactions", []):
            if not is_interesting(filing, tx):
                continue
            out.append({
                "accession": filing.get("accession"),
                "filing_date": filing.get("period_of_report"),
                "transaction_date": tx.get("transaction_date"),
                "issuer_ticker": ticker,
                "issuer_name": issuer_meta.get("name"),
                "issuer_cik": issuer_meta.get("cik"),
                "reporting_owner": ro.get("name"),
                "reporting_role": reporting_role_label(ro),
                "transaction_code": tx.get("transaction_code"),
                "shares": tx.get("shares"),
                "price_per_share": tx.get("price_per_share"),
                "value": tx.get("value"),
                "is_pre_planned": filing.get("aff_10b5_one", False),
                "ownership_type": tx.get("ownership_type"),
                "funds_holding_13f": conf.get("funds_holding"),
                "signal_strength_13f": conf.get("signal_strength"),
                "name_13f": conf.get("name"),
            })
    return out


# ── Output ────────────────────────────────────────────────────────────────

def write_output(payload: dict) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "insider_recent.json"
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


def print_summary(payload: dict, elapsed: float) -> None:
    print()
    print("=" * 72)
    print(f"INSIDER FEED SUMMARY  window={payload['window_days']}d  "
          f"elapsed={elapsed:.1f}s")
    print("=" * 72)
    print(f"source quarter:              {payload['source_quarter']}")
    print(f"tickers scanned:             {payload['tickers_scanned']}")
    print(f"issuers resolved:            {payload['issuers_resolved']}")
    print(f"issuers missing CIK:         {payload.get('tickers_unresolved', 0)}")
    print(f"total Form-4 filings parsed: {payload['total_form4_filings']}")
    print(f"interesting filings:         {payload['interesting_count']}")
    if not payload.get("interesting"):
        print("\nno interesting transactions found in the window")
        return
    print("\nTop 15 interesting by 13F-confluence × value:")
    print(f"  {'tkr':<6}{'date':<12}{'code':<5}{'owner':<28}"
          f"{'shares':>10}{'$value':>17}  funds_13f")
    sorted_i = sorted(
        payload["interesting"],
        key=lambda r: (-(r.get("funds_holding_13f") or 0), -(r.get("value") or 0))
    )
    for r in sorted_i[:15]:
        val = r.get("value") or 0
        sh = r.get("shares") or 0
        fh = r.get("funds_holding_13f")
        fh_str = f"{fh}" if fh is not None else "—"
        owner = (r.get("reporting_owner") or "")[:26]
        print(
            f"  {r['issuer_ticker']:<6}{r['transaction_date'] or '-':<12}"
            f"{r['transaction_code']:<5}{owner:<28}"
            f"{sh:>10,}{('${:,.0f}'.format(val)):>17}  {fh_str}"
        )


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarter", default=DEFAULT_QUARTER,
                        help="13F confluence quarter to source tickers from")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS,
                        help="how many days back to scan Form 4 filings")
    parser.add_argument("--top-n", type=int, default=DEFAULT_TOP_N,
                        help="top N tickers from confluence (by funds_holding)")
    parser.add_argument("--force", action="store_true",
                        help="re-fetch XMLs even if on disk")
    parser.add_argument("--dry-run", action="store_true",
                        help="resolve + report; don't fetch/parse/write")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()
    logging.info(
        f"=== build_insider_feed start  quarter={args.quarter}  "
        f"days={args.days}  top_n={args.top_n}  force={args.force} ==="
    )

    confluence = load_confluence(args.quarter)
    ticker_to_cik = load_ticker_to_cik()

    targets = select_tickers(confluence, args.top_n)
    logging.info(f"selected {len(targets)} target tickers from confluence")

    # Resolve tickers to CIKs
    resolved = []
    unresolved = []
    for t in targets:
        cik = ticker_to_cik.get(t["ticker"].upper())
        if cik:
            resolved.append({**t, "issuer_cik": cik})
        else:
            unresolved.append(t["ticker"])
    if unresolved:
        logging.warning(
            f"could not resolve CIK for {len(unresolved)} tickers: {unresolved}"
        )

    # Build confluence lookup keyed by ticker
    conf_lookup = {}
    for e in confluence.get("high_confluence", []):
        if e.get("ticker"):
            conf_lookup[e["ticker"]] = {
                "funds_holding": e.get("funds_holding"),
                "signal_strength": e.get("signal_strength"),
                "name": e.get("name"),
            }

    if args.dry_run:
        print("DRY-RUN — would scan:")
        for t in resolved:
            print(f"  {t['ticker']:<6} CIK={t['issuer_cik']}  "
                  f"funds={t['funds_holding']} ({t['signal_strength']})")
        return 0

    all_interesting = []
    total_filings = 0
    for t in resolved:
        cik = t["issuer_cik"]
        logging.info(
            f"--- {t['ticker']} CIK={cik} (funds_holding={t['funds_holding']}) ---"
        )
        n_fetched, n_parsed = process_issuer(cik, args.days, args.force)
        total_filings += n_parsed

        # Read back the per-issuer JSON we just wrote so we have a clean slice
        per_issuer_path = (
            OUTPUT_DIR / "form4" / f"{cik.zfill(10)}.json"
        )
        if not per_issuer_path.exists():
            continue
        per_issuer = json.loads(per_issuer_path.read_text())
        issuer_meta = {
            "name": per_issuer.get("issuer_name"),
            "cik": per_issuer.get("issuer_cik"),
        }
        # Only consider filings whose period falls within the window
        from datetime import datetime as dt, timedelta as td, timezone as tz
        cutoff = (dt.now(tz.utc) - td(days=args.days)).date()
        in_window = []
        for f in per_issuer.get("filings", []):
            try:
                fd = dt.strptime(f.get("period_of_report") or "", "%Y-%m-%d").date()
                if fd >= cutoff:
                    in_window.append(f)
            except ValueError:
                continue
        records = build_interesting_records(
            in_window, t["ticker"], issuer_meta, conf_lookup
        )
        all_interesting.extend(records)
        if records:
            logging.info(
                f"  {t['ticker']}: {n_parsed} filings parsed, "
                f"{len(records)} interesting"
            )

    # Sort: highest 13F confluence first, then largest value
    all_interesting.sort(
        key=lambda r: (
            -(r.get("funds_holding_13f") or 0),
            -(r.get("value") or 0),
        )
    )

    payload = {
        "data_computed": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
        "window_days": args.days,
        "source_quarter": args.quarter,
        "tickers_scanned": len(targets),
        "issuers_resolved": len(resolved),
        "tickers_unresolved": len(unresolved),
        "unresolved_tickers": unresolved,
        "total_form4_filings": total_filings,
        "interesting_count": len(all_interesting),
        "filter_rule": (
            f"reporting_owner in {{director, officer}} "
            f"AND transaction_code in {{P, S}} "
            f"AND (code==P OR NOT aff_10b5_one) "
            f"AND value >= ${MIN_VALUE_USD:,}"
        ),
        "interesting": all_interesting,
    }

    out_path = write_output(payload)
    logging.info(f"wrote {out_path}")
    print_summary(payload, time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
