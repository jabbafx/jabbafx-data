#!/usr/bin/env python3
"""
fetch_edgar.py — Quarterly SEC EDGAR 13F-HR fetcher (JabbaFX Module 1A).

For each tracked fund, look up the original 13F-HR filing (no amendments)
whose periodOfReport matches the target quarter, find the informationTable
XML inside the filing's archive directory, and save the raw XML to disk for
downstream parsing by Session 3.

Writes only to /root/jabbafx-data-pipeline/. No GitHub commits at this stage.
"""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

import requests


BASE_DIR = Path("/root/jabbafx-data-pipeline")
LOG_PATH = BASE_DIR / "logs" / "fetch_edgar.log"
DATA_DIR = BASE_DIR / "data" / "raw"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVE_INDEX_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/index.json"
)
ARCHIVE_FILE_URL = (
    "https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{filename}"
)

MIN_INTERVAL_SEC = 0.105          # ~9.5 req/sec, under SEC's 10/sec cap
RETRY_BACKOFFS = (1.0, 2.0, 4.0)  # exponential backoff on transient errors
RATE_LIMIT_SLEEP_SEC = 60.0       # on 429, sleep this long then retry once

QUARTER_PERIOD_END = {
    "Q1": "03-31",
    "Q2": "06-30",
    "Q3": "09-30",
    "Q4": "12-31",
}

# 23 active funds, padded 10-digit CIK + display name.
# Source: https://raw.githubusercontent.com/jabbafx/jabbafx-data/main/13f/funds.json
# (24th fund Crescat Capital marked active:false in funds.json — no 13F-HR
# filer on EDGAR; excluded from this list so fetcher skips it.)
# Session 3+ will refactor to fetch dynamically from that URL and respect
# the `active` flag.
FUNDS = [
    ("0001387322", "Whale Rock Capital"),                 # was 0001631613 in briefing (wrong)
    ("0001569049", "Light Street Capital"),
    ("0001135730", "Coatue Management"),
    ("0001061165", "Lone Pine Capital"),
    ("0001167483", "Tiger Global Management"),
    ("0001103804", "Viking Global Investors"),
    ("0001747057", "D1 Capital Partners"),                # was 0001758730 in briefing (wrong)
    ("0000934639", "Maverick Capital"),                   # was 0001036855 in briefing (wrong)
    ("0002045724", "Situational Awareness LP"),
    ("0001112520", "Akre Capital Management"),
    ("0001336528", "Pershing Square Capital"),
    ("0001541617", "Altimeter Capital"),                  # was 0001517382 in briefing (wrong)
    ("0001067983", "Berkshire Hathaway"),
    ("0001040273", "Third Point"),
    ("0001418814", "ValueAct Capital"),
    ("0001061768", "Baupost Group"),
    ("0001536411", "Duquesne Family Office"),
    ("0001656456", "Appaloosa Management"),
    ("0001079114", "Greenlight Capital"),
    ("0001791786", "Elliott Management"),
    ("0001863154", "Goehring & Rozencwajg Associates"),
    ("0001056823", "Horizon Kinetics Asset Management"),
    ("0001088875", "Baillie Gifford & Co"),
]


_last_request_time = 0.0


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


def throttled_get(url: str, timeout: int = 30) -> requests.Response:
    """GET with mandatory SEC User-Agent, ~10 req/sec throttle, retries.
    On 429: sleep 60s then retry once; sustained 429 raises RuntimeError.
    On 5xx/timeout: exponential backoff up to 3 retries.
    """
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
                raise RuntimeError(
                    "SEC EDGAR sustained 429 after retry — halting run"
                )
            return resp2

        if 500 <= resp.status_code < 600:
            if attempt < len(RETRY_BACKOFFS):
                logging.warning(
                    f"{resp.status_code} on {url}, "
                    f"retry in {RETRY_BACKOFFS[attempt]}s"
                )
                time.sleep(RETRY_BACKOFFS[attempt])
                continue

        return resp

    raise RuntimeError(f"exhausted retries on {url}")


def quarter_to_period_end(quarter: str) -> str:
    """'2026-Q1' -> '2026-03-31'."""
    year, q = quarter.split("-")
    return f"{year}-{QUARTER_PERIOD_END[q]}"


def find_quarterly_filing(
    cik: str, period_end: str
) -> Tuple[Optional[dict], Optional[str]]:
    """Look up the original (non-amendment) 13F-HR with periodOfReport ==
    period_end. Returns (filing_info, most_recent_13f_periodOfReport).
    filing_info is None if not found; most_recent_period is informational.
    """
    resp = throttled_get(SUBMISSIONS_URL.format(cik=cik))
    if resp.status_code == 404:
        logging.warning(f"submissions API 404 for CIK {cik}")
        return None, None
    resp.raise_for_status()
    data = resp.json()

    recent = data.get("filings", {}).get("recent", {})
    if not recent:
        return None, None

    forms = recent.get("form", [])
    accessions = recent.get("accessionNumber", [])
    # SEC submissions API uses 'reportDate' (NOT 'periodOfReport' as the
    # briefing said). Same semantic — the filing's period-end YYYY-MM-DD.
    periods = recent.get("reportDate", [])
    filing_dates = recent.get("filingDate", [])
    primary_docs = recent.get("primaryDocument", [])

    most_recent_period = None
    for i in range(len(forms)):
        if forms[i] == "13F-HR":
            if most_recent_period is None:
                most_recent_period = periods[i]
            if periods[i] == period_end:
                return (
                    {
                        "accession": accessions[i],
                        "periodOfReport": periods[i],
                        "filingDate": filing_dates[i],
                        "primaryDocument": primary_docs[i]
                        if i < len(primary_docs)
                        else None,
                    },
                    most_recent_period,
                )

    return None, most_recent_period


def find_informationtable_xml(cik: str, accession: str) -> Optional[str]:
    """Inspect a filing's archive directory index.json and return the
    filename of the informationTable XML.

    Heuristic (in order):
      1. Filter out primary_doc.xml — that's the SEC cover form, never holdings.
      2. Among the remaining XMLs, prefer any whose name contains
         'informationtable' or 'infotable' (e.g. Pershing's infotable.xml).
      3. Otherwise return the first non-cover XML (handles wildly varied
         filer-specific names: 53405.xml, BGLLCQ12026.xml, MSFS13F033126.XML,
         edgbgcomar26.xml, form13f_20260331.xml, etc.).
      4. If the only XML is primary_doc.xml, save it and warn — holdings may
         be inline (unusual; 13F-NT notice forms can look like this).
    """
    cik_int = str(int(cik))
    acc_nodash = accession.replace("-", "")
    url = ARCHIVE_INDEX_URL.format(cik_int=cik_int, acc_nodash=acc_nodash)
    resp = throttled_get(url)
    if resp.status_code == 404:
        logging.warning(f"archive index 404: {url}")
        return None
    resp.raise_for_status()
    data = resp.json()

    items = data.get("directory", {}).get("item", [])
    xml_files = [
        it["name"] for it in items if it.get("name", "").lower().endswith(".xml")
    ]
    if not xml_files:
        logging.error(f"CIK {cik} acc {accession}: no XML files in archive")
        return None

    non_cover = [n for n in xml_files if n.lower() != "primary_doc.xml"]
    if not non_cover:
        logging.warning(
            f"CIK {cik} acc {accession}: only primary_doc.xml present; "
            "holdings may be inline"
        )
        return xml_files[0]

    for name in non_cover:
        lower = name.lower()
        if "informationtable" in lower or "infotable" in lower:
            return name

    if len(non_cover) >= 2:
        logging.warning(
            f"CIK {cik} acc {accession}: {len(non_cover)} non-cover XMLs; "
            f"using first: {non_cover[0]}"
        )
    return non_cover[0]


def fetch_one(
    cik: str,
    fund_name: str,
    quarter: str,
    period_end: str,
    force: bool,
    dry_run: bool,
) -> dict:
    """Fetch a single fund's quarterly informationtable XML.
    Returns a result dict for the run summary.
    """
    result = {
        "cik": cik,
        "fund_name": fund_name,
        "status": None,
        "detail": None,
        "filing_date": None,
        "accession": None,
        "most_recent_period": None,
        "bytes_written": 0,
    }

    out_path = DATA_DIR / quarter / f"{cik}.xml"
    if out_path.exists() and not force:
        result["status"] = "skipped_exists"
        result["detail"] = f"file already at {out_path}"
        result["bytes_written"] = out_path.stat().st_size
        logging.info(f"CIK {cik} ({fund_name}): {result['detail']}")
        return result

    logging.info(f"--- CIK {cik} ({fund_name}) start ---")
    filing, most_recent = find_quarterly_filing(cik, period_end)
    result["most_recent_period"] = most_recent

    if not filing:
        result["status"] = "not_yet_filed"
        detail = (
            f"no original 13F-HR with periodOfReport={period_end}"
        )
        if most_recent:
            detail += f"; most recent 13F-HR period = {most_recent}"
        result["detail"] = detail
        logging.info(f"CIK {cik} ({fund_name}): {detail}")
        return result

    result["accession"] = filing["accession"]
    result["filing_date"] = filing["filingDate"]

    info_xml = find_informationtable_xml(cik, filing["accession"])
    if not info_xml:
        result["status"] = "error"
        result["detail"] = "could not locate informationtable XML"
        return result

    cik_int = str(int(cik))
    acc_nodash = filing["accession"].replace("-", "")
    file_url = ARCHIVE_FILE_URL.format(
        cik_int=cik_int, acc_nodash=acc_nodash, filename=info_xml
    )

    if dry_run:
        result["status"] = "dry_run"
        result["detail"] = f"would fetch {file_url}"
        logging.info(f"CIK {cik} ({fund_name}): DRY-RUN → {file_url}")
        return result

    try:
        resp = throttled_get(file_url)
        resp.raise_for_status()
    except Exception as e:
        result["status"] = "error"
        result["detail"] = f"fetch failed: {e}"
        logging.error(f"CIK {cik} ({fund_name}): {result['detail']}")
        return result

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(resp.content)
    result["status"] = "fetched"
    result["detail"] = f"saved {info_xml} to {out_path}"
    result["bytes_written"] = len(resp.content)
    logging.info(
        f"CIK {cik} ({fund_name}): saved {len(resp.content)} bytes to {out_path}"
    )
    return result


def print_summary(quarter: str, results: list, elapsed: float) -> None:
    fetched = [r for r in results if r["status"] == "fetched"]
    skipped = [r for r in results if r["status"] == "skipped_exists"]
    not_filed = [r for r in results if r["status"] == "not_yet_filed"]
    errors = [r for r in results if r["status"] == "error"]
    dry = [r for r in results if r["status"] == "dry_run"]

    print()
    print("=" * 72)
    print(f"SUMMARY  quarter={quarter}  elapsed={elapsed:.1f}s")
    print("=" * 72)

    print(
        f"\n[fetched] {len(fetched)}/{len(results)} funds — Q1 13F-HR saved this run"
    )
    for r in fetched:
        print(
            f"  {r['cik']} {r['fund_name']:35s} "
            f"acc={r['accession']} filed={r['filing_date']} "
            f"({r['bytes_written']:,} bytes)"
        )

    if skipped:
        print(f"\n[skipped_exists] {len(skipped)} — already on disk, no refetch")
        for r in skipped:
            print(f"  {r['cik']} {r['fund_name']:35s} ({r['bytes_written']:,} bytes)")

    if not_filed:
        print(
            f"\n[not_yet_filed] {len(not_filed)} — "
            f"Q1 2026 not yet on EDGAR for these funds:"
        )
        for r in not_filed:
            mr = r["most_recent_period"] or "n/a"
            print(
                f"  {r['cik']} {r['fund_name']:35s} most-recent={mr}"
            )

    if errors:
        print(f"\n[error] {len(errors)} — investigate:")
        for r in errors:
            print(
                f"  {r['cik']} {r['fund_name']:35s} — {r['detail']}"
            )

    if dry:
        print(f"\n[dry_run] {len(dry)} — would fetch:")
        for r in dry:
            print(f"  {r['cik']} {r['fund_name']:35s} — {r['detail']}")

    total_bytes = sum(r["bytes_written"] for r in results)
    quarter_dir = DATA_DIR / quarter
    on_disk_bytes = 0
    if quarter_dir.exists():
        on_disk_bytes = sum(
            f.stat().st_size for f in quarter_dir.glob("*.xml")
        )
    print(
        f"\nThis run wrote: {total_bytes:,} bytes "
        f"({total_bytes / 1024 / 1024:.2f} MB)"
    )
    print(
        f"data/raw/{quarter}/ total on disk: {on_disk_bytes:,} bytes "
        f"({on_disk_bytes / 1024 / 1024:.2f} MB)"
    )
    print(f"Wall-clock elapsed: {elapsed:.1f}s")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cik", help="fetch single fund only (padded 10-digit CIK)"
    )
    parser.add_argument(
        "--quarter", default="2026-Q1", help="target quarter YYYY-QX"
    )
    parser.add_argument(
        "--force", action="store_true", help="re-fetch even if file exists"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="log only, write nothing"
    )
    args = parser.parse_args()

    setup_logger()

    try:
        period_end = quarter_to_period_end(args.quarter)
    except (KeyError, ValueError):
        logging.error(f"invalid --quarter {args.quarter} (expected e.g. 2026-Q1)")
        return 2

    logging.info(
        f"=== run start  quarter={args.quarter}  "
        f"periodOfReport={period_end}  force={args.force}  "
        f"dry_run={args.dry_run} ==="
    )

    if args.cik:
        name = dict(FUNDS).get(args.cik, "(not in tracked list)")
        targets = [(args.cik, name)]
    else:
        targets = FUNDS

    start = time.monotonic()
    results = []
    try:
        for cik, fund_name in targets:
            r = fetch_one(
                cik, fund_name, args.quarter, period_end, args.force, args.dry_run
            )
            results.append(r)
    except RuntimeError as e:
        logging.error(f"HALT: {e}")
        print_summary(args.quarter, results, time.monotonic() - start)
        return 2

    print_summary(args.quarter, results, time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
