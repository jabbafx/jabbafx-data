#!/usr/bin/env python3
"""
fetch_cot.py — CFTC Commitments of Traders (COT) weekly data fetcher.

Module 1C Session 1 scaffold. Pulls legacy COT (commercial vs non-commercial
positioning) from the CFTC Socrata API and saves raw JSON to disk for
downstream parsing in Session 2.

Reads:   nothing on disk
Writes:  /root/jabbafx-data-pipeline/data/raw/cot/<contract_code>.json
         /root/jabbafx-data-pipeline/logs/fetch_cot.log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.

Data source (Session 1 recon):
  Socrata API: https://publicreporting.cftc.gov/resource/6dca-aqww.json
  Free, no auth, no SoQL key required for low-volume reads.
  Filterable via $where on cftc_contract_market_code.

Schema highlights (legacy COT — matches Module 1C decision rule):
  report_date_as_yyyy_mm_dd          weekly snapshot date (Tuesday close)
  open_interest_all                  total open interest
  comm_positions_long_all            commercial (hedger) long contracts
  comm_positions_short_all           commercial short contracts
  noncomm_positions_long_all         non-commercial (speculator) long
  noncomm_positions_short_all        non-commercial short
  market_and_exchange_names          human-readable contract label

Decision rule served (Module 1C):
  "I will fade a commodity move when commercial net positioning hits 5y
   extreme percentile ≥95 (overbought) or ≤5 (oversold)."

Tracked instruments (verified codes — refine remainder in Session 2):
  088691 = GOLD - COMMODITY EXCHANGE INC.  (GC)  ✓ verified in scaffold
  067651 = CRUDE OIL - NEW YORK MERCANTILE EXCHANGE  (CL)
  020601 = U.S. TREASURY BONDS - CHICAGO BOARD OF TRADE  (ZB) — verify in S2
  099741 = EURO FX - CHICAGO MERCANTILE EXCHANGE  (6E)
  097741 = JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE  (6J)
  096742 = BRITISH POUND - CHICAGO MERCANTILE EXCHANGE  (6B)  ✓ verified
  13874A = E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE  (ES) — verify in S2
  209742 = E-MINI NASDAQ-100 - CHICAGO MERCANTILE EXCHANGE  (NQ) — verify in S2
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests


BASE_DIR = Path("/root/jabbafx-data-pipeline")
RAW_DIR = BASE_DIR / "data" / "raw" / "cot"
LOG_PATH = BASE_DIR / "logs" / "fetch_cot.log"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
SOCRATA_BASE = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"

# CFTC is less aggressive than SEC — 1 req/sec is plenty for a quarterly tool
MIN_INTERVAL_SEC = 1.0
RETRY_BACKOFFS = (1.0, 2.0, 4.0)
RATE_LIMIT_SLEEP_SEC = 30.0

# Fields requested per row (kept tight; full row is ~80 columns of which we
# need ~7). Saves on response size + simplifies downstream parser.
SELECT_FIELDS = ",".join([
    "market_and_exchange_names",
    "report_date_as_yyyy_mm_dd",
    "cftc_contract_market_code",
    "open_interest_all",
    "comm_positions_long_all",
    "comm_positions_short_all",
    "noncomm_positions_long_all",
    "noncomm_positions_short_all",
    "nonrept_positions_long_all",
    "nonrept_positions_short_all",
])


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
    """GET with rate-limit + retries. Mirrors fetch_form4.py pattern but
    with the more relaxed CFTC throttle (1 req/sec)."""
    global _last_request_time
    headers = {"User-Agent": UA, "Accept": "application/json"}

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
                    f"transient {e.__class__.__name__}, "
                    f"retry in {RETRY_BACKOFFS[attempt]}s"
                )
                time.sleep(RETRY_BACKOFFS[attempt])
                continue
            raise

        if resp.status_code == 429:
            logging.warning(
                f"429 on Socrata; sleeping {RATE_LIMIT_SLEEP_SEC}s then 1 retry"
            )
            time.sleep(RATE_LIMIT_SLEEP_SEC)
            _last_request_time = time.monotonic()
            resp2 = requests.get(url, headers=headers, timeout=timeout)
            if resp2.status_code == 429:
                raise RuntimeError("CFTC Socrata sustained 429 after retry")
            return resp2

        if 500 <= resp.status_code < 600 and attempt < len(RETRY_BACKOFFS):
            logging.warning(
                f"{resp.status_code} on {url}, retry in {RETRY_BACKOFFS[attempt]}s"
            )
            time.sleep(RETRY_BACKOFFS[attempt])
            continue

        return resp

    raise RuntimeError(f"exhausted retries on {url}")


def fetch_contract(
    contract_code: str, weeks: int, force: bool, dry_run: bool
) -> dict:
    """Fetch the last `weeks` weekly snapshots for one contract code.
    Saves raw JSON list to data/raw/cot/<code>.json."""
    out = {"code": contract_code, "status": None, "rows": 0, "bytes": 0}
    dest = RAW_DIR / f"{contract_code}.json"
    if dest.exists() and not force:
        out["status"] = "skipped_exists"
        out["bytes"] = dest.stat().st_size
        try:
            existing = json.loads(dest.read_text())
            out["rows"] = len(existing) if isinstance(existing, list) else 0
        except Exception:
            pass
        return out

    params = {
        "$where": f"cftc_contract_market_code='{contract_code}'",
        "$order": "report_date_as_yyyy_mm_dd DESC",
        "$limit": str(weeks),
        "$select": SELECT_FIELDS,
    }
    url = SOCRATA_BASE + "?" + urllib.parse.urlencode(params)
    if dry_run:
        out["status"] = "dry_run"
        logging.info(f"DRY-RUN would GET {url}")
        return out

    try:
        resp = throttled_get(url)
        resp.raise_for_status()
    except Exception as e:
        out["status"] = "error"
        logging.error(f"{contract_code}: fetch failed: {e}")
        return out

    try:
        rows = resp.json()
    except Exception as e:
        out["status"] = "error"
        logging.error(f"{contract_code}: JSON decode failed: {e}")
        return out

    if not isinstance(rows, list) or len(rows) == 0:
        out["status"] = "empty"
        logging.warning(f"{contract_code}: no rows returned")
        return out

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(RAW_DIR), delete=False
    ) as tf:
        json.dump(rows, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, dest)
    out["status"] = "fetched"
    out["rows"] = len(rows)
    out["bytes"] = dest.stat().st_size
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--code", required=True,
        help="CFTC contract market code (e.g. 088691 for Gold)"
    )
    parser.add_argument("--weeks", type=int, default=260,
                        help="how many weekly snapshots (default 260 = 5y)")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()
    logging.info(
        f"=== fetch_cot start  code={args.code}  weeks={args.weeks}  "
        f"force={args.force}  dry_run={args.dry_run} ==="
    )

    result = fetch_contract(args.code, args.weeks, args.force, args.dry_run)
    elapsed = time.monotonic() - start

    print()
    print("=" * 60)
    print(f"FETCH-COT SUMMARY  code={args.code}  elapsed={elapsed:.1f}s")
    print("=" * 60)
    print(f"  status:  {result['status']}")
    print(f"  rows:    {result['rows']}")
    print(f"  bytes:   {result['bytes']:,}")
    print(f"  saved:   {RAW_DIR / (args.code + '.json')}")
    return 0 if result["status"] in ("fetched", "skipped_exists", "dry_run") else 2


if __name__ == "__main__":
    sys.exit(main())
