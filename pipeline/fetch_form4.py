#!/usr/bin/env python3
"""
fetch_form4.py — SEC EDGAR Form 4 (insider transactions) fetcher.

Module 1B Session 1 scaffold. Fetches recent Form 4 filings for one or more
issuers and saves the raw XML to disk. Parsing into structured JSON happens
in Session 2 (parse_form4.py).

Reads:   nothing on disk
Writes:  /root/jabbafx-data-pipeline/data/raw/form4/<issuer-cik>/<accession>.xml
         /root/jabbafx-data-pipeline/logs/fetch_form4.log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.

Form 4 schema (from Session 1 recon):
  Root: <ownershipDocument> (no XML namespace)
  Fields: schemaVersion, documentType, periodOfReport, issuer, reportingOwner,
          nonDerivativeTable, derivativeTable
  Issuer block contains issuerCik, issuerName, issuerTradingSymbol — ticker
  is already present, unlike 13F (no CUSIP→ticker mapping needed for 1B).
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests


BASE_DIR = Path("/root/jabbafx-data-pipeline")
RAW_DIR = BASE_DIR / "data" / "raw" / "form4"
LOG_PATH = BASE_DIR / "logs" / "fetch_form4.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVE_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/index.json"
)
ARCHIVE_FILE_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{filename}"
)

MIN_INTERVAL_SEC = 0.105
RETRY_BACKOFFS = (1.0, 2.0, 4.0)
RATE_LIMIT_SLEEP_SEC = 60.0


# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Throttled HTTP — same pattern as fetch_edgar.py
# ---------------------------------------------------------------------------

_last_request_time = 0.0


def throttled_get(url: str, timeout: int = 30) -> requests.Response:
    global _last_request_time
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}

    for attempt in range(len(RETRY_BACKOFFS) + 1):
        elapsed = time.monotonic() - _last_request_time
        if elapsed < MIN_INTERVAL_SEC:
            time.sleep(MIN_INTERVAL_SEC - elapsed)
        _last_request_time = time.monotonic()

        try:
            logging.info(f"GET {url}")
            resp = requests.get(url, headers=headers, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < len(RETRY_BACKOFFS):
                logging.warning(
                    f"transient {e.__class__.__name__} on {url}, "
                    f"retry in {RETRY_BACKOFFS[attempt]}s"
                )
                time.sleep(RETRY_BACKOFFS[attempt])
                continue
            raise

        if resp.status_code == 429:
            logging.warning(
                f"429 on {url}; sleeping {RATE_LIMIT_SLEEP_SEC}s then 1 retry"
            )
            time.sleep(RATE_LIMIT_SLEEP_SEC)
            elapsed = time.monotonic() - _last_request_time
            if elapsed < MIN_INTERVAL_SEC:
                time.sleep(MIN_INTERVAL_SEC - elapsed)
            _last_request_time = time.monotonic()
            resp2 = requests.get(url, headers=headers, timeout=timeout)
            if resp2.status_code == 429:
                raise RuntimeError("SEC EDGAR sustained 429 after retry")
            return resp2

        if 500 <= resp.status_code < 600 and attempt < len(RETRY_BACKOFFS):
            logging.warning(
                f"{resp.status_code} on {url}, retry in {RETRY_BACKOFFS[attempt]}s"
            )
            time.sleep(RETRY_BACKOFFS[attempt])
            continue

        return resp

    raise RuntimeError(f"exhausted retries on {url}")


# ---------------------------------------------------------------------------
# Form 4 listing + fetching
# ---------------------------------------------------------------------------

def list_form4_filings(issuer_cik: str, days_back: int) -> list:
    """Return list of dicts {accession, filing_date, period_of_report, primary_doc}
    for Form 4 (not 4/A amendments) within the last `days_back` days.
    """
    resp = throttled_get(SUBMISSIONS_URL.format(cik=issuer_cik))
    if resp.status_code == 404:
        logging.warning(f"submissions 404 for issuer CIK {issuer_cik}")
        return []
    resp.raise_for_status()
    data = resp.json()
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    accs = recent.get("accessionNumber", [])
    fdates = recent.get("filingDate", [])
    periods = recent.get("reportDate", [])
    pdocs = recent.get("primaryDocument", [])

    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()
    out = []
    for i in range(len(forms)):
        if forms[i] != "4":
            continue
        try:
            fd = datetime.strptime(fdates[i], "%Y-%m-%d").date()
        except (ValueError, IndexError):
            continue
        if fd < cutoff:
            # filings array is sorted desc by filing date — can break early
            break
        out.append({
            "accession": accs[i],
            "filing_date": fdates[i],
            "period_of_report": periods[i] if i < len(periods) else None,
            "primary_doc": pdocs[i] if i < len(pdocs) else None,
        })
    return out


def find_form4_xml(cik_int: str, acc_nodash: str) -> Optional[str]:
    """Inspect filing's archive index.json and return the Form 4 XML filename.

    Form 4 archives typically have a single XML file matching `wk-form4_*.xml`
    or `<filer>_form4.xml` or similar. The primary_doc field from the
    submissions API points to the XSL-rendered version (xslF345X06/<name>.xml).
    We want the raw XML at the archive root.
    """
    url = ARCHIVE_INDEX_URL.format(cik_int=cik_int, acc_nodash=acc_nodash)
    resp = throttled_get(url)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json()
    items = data.get("directory", {}).get("item", [])
    xml_files = [
        it["name"] for it in items
        if it.get("name", "").lower().endswith(".xml")
        and "/" not in it.get("name", "")  # exclude xslF345X06/ subdir refs
    ]
    if not xml_files:
        return None
    # Prefer ones named with "form4" hint
    for n in xml_files:
        if "form4" in n.lower() or "form_4" in n.lower():
            return n
    # Otherwise just first .xml at root
    return xml_files[0]


def fetch_one_filing(
    issuer_cik: str, filing: dict, force: bool, dry_run: bool
) -> dict:
    out = {"accession": filing["accession"], "status": None, "detail": None,
           "bytes_written": 0}
    cik_int = str(int(issuer_cik))
    acc_nodash = filing["accession"].replace("-", "")
    dest_dir = RAW_DIR / issuer_cik.zfill(10)
    dest_path = dest_dir / f"{filing['accession']}.xml"

    if dest_path.exists() and not force:
        out["status"] = "skipped_exists"
        out["detail"] = f"already at {dest_path}"
        out["bytes_written"] = dest_path.stat().st_size
        return out

    xml_name = find_form4_xml(cik_int, acc_nodash)
    if not xml_name:
        out["status"] = "error"
        out["detail"] = "no XML found in archive"
        return out

    file_url = ARCHIVE_FILE_URL.format(
        cik_int=cik_int, acc_nodash=acc_nodash, filename=xml_name
    )

    if dry_run:
        out["status"] = "dry_run"
        out["detail"] = f"would fetch {file_url}"
        return out

    try:
        resp = throttled_get(file_url)
        resp.raise_for_status()
    except Exception as e:
        out["status"] = "error"
        out["detail"] = f"fetch failed: {e}"
        return out

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(resp.content)
    out["status"] = "fetched"
    out["detail"] = f"saved {xml_name} → {dest_path}"
    out["bytes_written"] = len(resp.content)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cik", required=True,
        help="issuer CIK (10-digit padded, e.g. 0001045810 for NVDA)"
    )
    parser.add_argument(
        "--days", type=int, default=7,
        help="how many days back to scan (default: 7)"
    )
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()

    logging.info(
        f"=== fetch_form4 start  issuer_cik={args.cik}  days={args.days}  "
        f"force={args.force}  dry_run={args.dry_run} ==="
    )

    try:
        filings = list_form4_filings(args.cik, args.days)
    except Exception as e:
        logging.error(f"listing failed: {e}")
        return 2

    logging.info(f"found {len(filings)} Form 4 filings in last {args.days} days")
    if not filings:
        logging.info("nothing to do")
        return 0

    results = []
    for f in filings:
        r = fetch_one_filing(args.cik, f, args.force, args.dry_run)
        results.append(r)
        logging.info(f"  {f['filing_date']} {f['accession']}: {r['status']} — {r['detail']}")

    # Summary
    by_status: dict = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r)
    elapsed = time.monotonic() - start

    print()
    print("=" * 60)
    print(f"FORM-4 SUMMARY  issuer={args.cik}  elapsed={elapsed:.1f}s")
    print("=" * 60)
    for status, items in by_status.items():
        print(f"  [{status}] {len(items)}")
    total = sum(r["bytes_written"] for r in results)
    print(f"\nbytes written this run: {total:,}")
    print(f"raw form4 dir:          {RAW_DIR / args.cik.zfill(10)}/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
