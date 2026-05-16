#!/usr/bin/env python3
"""
parse_13f.py — Convert raw SEC 13F-HR informationTable XMLs to a structured
positions JSON for the JabbaFX Institutional Intelligence layer (Module 1A,
Session 3).

Reads:
  /root/jabbafx-data-pipeline/data/raw/<quarter>/<padded-CIK>.xml
  /root/jabbafx-data-pipeline/data/staged_funds.json   (scp'd from Mac)
Writes:
  /root/jabbafx-data-pipeline/data/output/<quarter>.json
  /root/jabbafx-data-pipeline/data/cache/cusip_ticker.json   (persistent)
  /root/jabbafx-data-pipeline/data/cache/filings_<quarter>.json (persistent)
  /root/jabbafx-data-pipeline/logs/parse_13f.log

Writes only to /root/jabbafx-data-pipeline/. No PMSCAN references.
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import requests
from lxml import etree


BASE_DIR = Path("/root/jabbafx-data-pipeline")
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "output"
CACHE_DIR = BASE_DIR / "data" / "cache"
LOG_PATH = BASE_DIR / "logs" / "parse_13f.log"
FUNDS_JSON_PATH = BASE_DIR / "data" / "staged_funds.json"

UA = "JabbaFX Personal Research jordanmabbasi@gmail.com"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
OPENFIGI_URL = "https://api.openfigi.com/v3/mapping"

SEC_MIN_INTERVAL = 0.105        # ~9.5 req/sec under SEC's 10/sec cap
OPENFIGI_MIN_INTERVAL = 0.25    # 4 req/sec under 25 per 6s no-key cap
RETRY_BACKOFFS = (1.0, 2.0, 4.0)
RATE_LIMIT_SLEEP_SEC = 60.0

INFORMATION_TABLE_NS = "http://www.sec.gov/edgar/document/thirteenf/informationtable"

# 13F-HR <value> semantics:
#   Pre-2023-Q1: USD thousands  (e.g. "9310043" = $9.31B)
#   2023-Q1+   : USD dollars    (SEC rule change effective Mar 2023)
# For 2026-Q1 the field is raw dollars — no multiplier. Sanity-checked
# against Pershing's $13.7B Q1 2026 AUM.
VALUE_MULTIPLIER = 1

UNKNOWN_PCT_HALT = 0.20         # halt if >20% CUSIPs unresolved
PER_FUND_RECOVERY_HALT = 0.50   # halt per-fund if <50% expected rows parsed

# Expected row count per fund for Q1 2026 (PLAN.md §5). Used for the
# per-fund recovery halt. Funds not in this table (Situational Awareness,
# Greenlight, inactive Crescat) are skipped without recovery check.
EXPECTED_ROWS_2026_Q1 = {
    "0001387322": 35,     # Whale Rock
    "0001569049": 24,     # Light Street
    "0001135730": 198,    # Coatue
    "0001061165": 36,     # Lone Pine
    "0001167483": 54,     # Tiger Global
    "0001103804": 77,     # Viking
    "0001747057": 44,     # D1 Capital
    "0000934639": 241,    # Maverick
    "0001112520": 20,     # Akre
    "0001336528": 11,     # Pershing Square
    "0001541617": 13,     # Altimeter
    "0001067983": 90,     # Berkshire
    "0001040273": 33,     # Third Point
    "0001418814": 18,     # ValueAct
    "0001061768": 22,     # Baupost
    "0001536411": 70,     # Duquesne
    "0001656456": 31,     # Appaloosa
    "0001791786": 33,     # Elliott
    "0001863154": 43,     # Goehring & Rozencwajg
    "0001056823": 353,    # Horizon Kinetics
    "0001088875": 906,    # Baillie Gifford
}


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
# Throttled HTTP — shared pattern; per-host last-request gate
# ---------------------------------------------------------------------------

_last_sec_request_time = 0.0
_last_openfigi_request_time = 0.0


def _sleep_until_min_interval(last_time: float, min_interval: float) -> float:
    elapsed = time.monotonic() - last_time
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    return time.monotonic()


def throttled_sec_get(url: str, timeout: int = 30) -> requests.Response:
    """SEC EDGAR GET with mandatory UA, 10/sec throttle, retries (same logic
    as fetch_edgar.py's throttled_get)."""
    global _last_sec_request_time
    headers = {"User-Agent": UA, "Accept-Encoding": "gzip, deflate"}

    for attempt in range(len(RETRY_BACKOFFS) + 1):
        _last_sec_request_time = _sleep_until_min_interval(
            _last_sec_request_time, SEC_MIN_INTERVAL
        )
        try:
            logging.info(f"SEC GET {url}")
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
            _last_sec_request_time = _sleep_until_min_interval(
                _last_sec_request_time, SEC_MIN_INTERVAL
            )
            resp2 = requests.get(url, headers=headers, timeout=timeout)
            if resp2.status_code == 429:
                raise RuntimeError("SEC EDGAR sustained 429 after retry — halting")
            return resp2

        if 500 <= resp.status_code < 600 and attempt < len(RETRY_BACKOFFS):
            logging.warning(
                f"{resp.status_code} on {url}, retry in {RETRY_BACKOFFS[attempt]}s"
            )
            time.sleep(RETRY_BACKOFFS[attempt])
            continue

        return resp

    raise RuntimeError(f"exhausted retries on {url}")


def throttled_openfigi_post(payload: list) -> requests.Response:
    """OpenFIGI POST with 4/sec throttle and same retry semantics."""
    global _last_openfigi_request_time
    headers = {"Content-Type": "application/json", "User-Agent": UA}

    for attempt in range(len(RETRY_BACKOFFS) + 1):
        _last_openfigi_request_time = _sleep_until_min_interval(
            _last_openfigi_request_time, OPENFIGI_MIN_INTERVAL
        )
        try:
            resp = requests.post(
                OPENFIGI_URL, json=payload, headers=headers, timeout=30
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < len(RETRY_BACKOFFS):
                logging.warning(
                    f"transient {e.__class__.__name__} on OpenFIGI, "
                    f"retry in {RETRY_BACKOFFS[attempt]}s"
                )
                time.sleep(RETRY_BACKOFFS[attempt])
                continue
            raise

        if resp.status_code == 429:
            logging.warning(
                f"OpenFIGI 429; sleeping {RATE_LIMIT_SLEEP_SEC}s then 1 retry"
            )
            time.sleep(RATE_LIMIT_SLEEP_SEC)
            _last_openfigi_request_time = _sleep_until_min_interval(
                _last_openfigi_request_time, OPENFIGI_MIN_INTERVAL
            )
            resp2 = requests.post(
                OPENFIGI_URL, json=payload, headers=headers, timeout=30
            )
            if resp2.status_code == 429:
                raise RuntimeError("OpenFIGI sustained 429 after retry — halting")
            return resp2

        if 500 <= resp.status_code < 600 and attempt < len(RETRY_BACKOFFS):
            logging.warning(
                f"OpenFIGI {resp.status_code}, retry in {RETRY_BACKOFFS[attempt]}s"
            )
            time.sleep(RETRY_BACKOFFS[attempt])
            continue

        return resp

    raise RuntimeError("exhausted retries on OpenFIGI")


# ---------------------------------------------------------------------------
# Funds + filing metadata
# ---------------------------------------------------------------------------

def load_funds_map() -> dict:
    """Return {padded_cik: {name, cluster, active}} from staged funds.json."""
    with open(FUNDS_JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    out = {}
    for entry in data.get("funds", []):
        out[entry["cik"]] = {
            "name": entry["name"],
            "cluster": entry["cluster"],
            "active": entry.get("active", True),
        }
    return out


def quarter_to_period_end(quarter: str) -> str:
    """'2026-Q1' -> '2026-03-31'."""
    year, q = quarter.split("-")
    return f"{year}-" + {"Q1": "03-31", "Q2": "06-30", "Q3": "09-30", "Q4": "12-31"}[q]


def load_filing_metadata(ciks: list, quarter: str, force: bool) -> dict:
    """For each CIK, look up filing_date + accession for the 13F-HR whose
    reportDate matches the target quarter. Cached to data/cache/."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_path = CACHE_DIR / f"filings_{quarter}.json"
    cache: dict = {}
    if cache_path.exists() and not force:
        try:
            cache = json.loads(cache_path.read_text())
        except Exception:
            cache = {}

    period_end = quarter_to_period_end(quarter)
    out: dict = {}
    for cik in ciks:
        if cik in cache:
            out[cik] = cache[cik]
            continue
        resp = throttled_sec_get(SUBMISSIONS_URL.format(cik=cik))
        if resp.status_code == 404:
            logging.warning(f"submissions 404 for CIK {cik}")
            out[cik] = {"filing_date": None, "accession": None}
            cache[cik] = out[cik]
            continue
        resp.raise_for_status()
        data = resp.json()
        recent = data.get("filings", {}).get("recent", {})
        forms = recent.get("form", [])
        accs = recent.get("accessionNumber", [])
        periods = recent.get("reportDate", [])
        fdates = recent.get("filingDate", [])
        meta = {"filing_date": None, "accession": None}
        for i in range(len(forms)):
            if forms[i] == "13F-HR" and periods[i] == period_end:
                meta = {
                    "filing_date": fdates[i] if i < len(fdates) else None,
                    "accession": accs[i] if i < len(accs) else None,
                }
                break
        out[cik] = meta
        cache[cik] = meta

    cache_path.write_text(json.dumps(cache, indent=2, sort_keys=True))
    return out


# ---------------------------------------------------------------------------
# XML traversal — namespace-agnostic
# ---------------------------------------------------------------------------

def local_child(elem: etree._Element, local_name: str) -> Optional[etree._Element]:
    for c in elem:
        if etree.QName(c).localname == local_name:
            return c
    return None


def local_child_text(elem: etree._Element, local_name: str) -> Optional[str]:
    c = local_child(elem, local_name)
    if c is None or c.text is None:
        return None
    return c.text.strip()


def local_grandchild_text(
    elem: etree._Element, parent_name: str, child_name: str
) -> Optional[str]:
    parent = local_child(elem, parent_name)
    if parent is None:
        return None
    return local_child_text(parent, child_name)


def iter_info_tables(xml_path: Path) -> Iterator[etree._Element]:
    tree = etree.parse(str(xml_path))
    root = tree.getroot()
    for elem in root.iter():
        if etree.QName(elem).localname == "infoTable":
            yield elem


# ---------------------------------------------------------------------------
# Per-position extraction
# ---------------------------------------------------------------------------

def _voting_label(elem: etree._Element) -> str:
    """Pick SOLE/SHARED/NONE by largest tally; ties favor SOLE."""
    sole = int(local_grandchild_text(elem, "votingAuthority", "Sole") or 0)
    shared = int(local_grandchild_text(elem, "votingAuthority", "Shared") or 0)
    none_ = int(local_grandchild_text(elem, "votingAuthority", "None") or 0)
    # Tie-break ordering: SOLE > SHARED > NONE
    best_label = "SOLE"
    best_val = sole
    if shared > best_val:
        best_label, best_val = "SHARED", shared
    if none_ > best_val:
        best_label, best_val = "NONE", none_
    return best_label


def extract_position(elem: etree._Element) -> Optional[dict]:
    cusip = local_child_text(elem, "cusip")
    if not cusip:
        return None
    cusip = cusip.strip().upper()

    name = (local_child_text(elem, "nameOfIssuer") or "").strip()
    share_class = (local_child_text(elem, "titleOfClass") or "COM").strip()

    value_raw_str = local_child_text(elem, "value")
    if value_raw_str is None:
        return None
    try:
        value_thousands = int(value_raw_str.replace(",", ""))
    except ValueError:
        return None

    shares = None
    shrs_amt_str = local_grandchild_text(elem, "shrsOrPrnAmt", "sshPrnamt")
    shrs_type = local_grandchild_text(elem, "shrsOrPrnAmt", "sshPrnamtType") or ""
    if shrs_type.strip().upper() == "SH" and shrs_amt_str is not None:
        try:
            shares = int(shrs_amt_str.replace(",", ""))
        except ValueError:
            shares = None

    put_call = local_child_text(elem, "putCall")  # nullable

    return {
        "cusip": cusip,
        "ticker": None,                # filled by resolve_tickers
        "name": name,
        "value": value_thousands * VALUE_MULTIPLIER,
        "shares": shares,
        "share_class": share_class,
        "put_call": put_call if put_call else None,
        "voting": _voting_label(elem),
        "pct_of_aum": 0.0,             # filled after total_value known
    }


# ---------------------------------------------------------------------------
# Per-fund parsing
# ---------------------------------------------------------------------------

def _detect_value_unit(positions: list) -> int:
    """SEC 13F rule (effective 2023) requires raw dollars, but some legacy
    filers (e.g. Baupost, Duquesne) still emit values in thousands. Detect
    via median per-share price: institutional managers hold equities mostly
    priced $5-$5000/share; a fund whose median (value/shares) is < $0.50
    is emitting values in thousands.

    Returns 1 (dollars) or 1000 (thousands -> multiplier to apply).
    """
    per_share = []
    for p in positions:
        if p["shares"] and p["shares"] > 0 and p["value"] > 0:
            per_share.append(p["value"] / p["shares"])
    if len(per_share) < 3:
        return 1  # too few share-denominated rows to detect; default dollars
    per_share.sort()
    median = per_share[len(per_share) // 2]
    return 1000 if median < 0.50 else 1


def parse_one_fund(
    cik: str, xml_path: Path, fund_meta: dict, filing_meta: dict
) -> dict:
    positions = []
    skipped = 0
    for it in iter_info_tables(xml_path):
        pos = extract_position(it)
        if pos is None:
            skipped += 1
            continue
        positions.append(pos)

    # Per-fund value-unit detection (dollars vs legacy thousands)
    unit = _detect_value_unit(positions)
    if unit != 1:
        logging.warning(
            f"CIK {cik}: detected legacy thousands convention; "
            f"scaling all position values by {unit}"
        )
        for p in positions:
            p["value"] = p["value"] * unit

    positions.sort(key=lambda p: p["value"], reverse=True)
    total_value = sum(p["value"] for p in positions)
    if total_value > 0:
        for p in positions:
            p["pct_of_aum"] = round(p["value"] / total_value * 100, 4)

    return {
        "fund_name": fund_meta["name"],
        "filing_date": filing_meta.get("filing_date"),
        "accession": filing_meta.get("accession"),
        "total_value": total_value,
        "position_count": len(positions),
        "positions_skipped": skipped,
        "value_unit_multiplier": unit,
        "positions": positions,
    }


# ---------------------------------------------------------------------------
# CUSIP → ticker resolution (OpenFIGI)
# ---------------------------------------------------------------------------

def load_cusip_cache() -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = CACHE_DIR / "cusip_ticker.json"
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            return {}
    return {}


def save_cusip_cache(cache: dict) -> None:
    p = CACHE_DIR / "cusip_ticker.json"
    p.write_text(json.dumps(cache, indent=2, sort_keys=True))


def collect_unique_cusips(funds_payload: dict) -> list:
    seen = set()
    for fund in funds_payload.values():
        for p in fund["positions"]:
            seen.add(p["cusip"])
    return sorted(seen)


def resolve_tickers(unique_cusips: list) -> dict:
    """Resolve CUSIP → {ticker, figi, name}. Uses persistent cache.
    Returns map keyed by CUSIP; missing entries have ticker=None.
    """
    cache = load_cusip_cache()
    todo = [c for c in unique_cusips if c not in cache]
    logging.info(
        f"OpenFIGI: {len(cache)} cache hits, {len(todo)} CUSIPs to resolve"
    )

    BATCH = 5
    for i in range(0, len(todo), BATCH):
        batch = todo[i : i + BATCH]
        payload = [
            {"idType": "ID_CUSIP", "idValue": c, "exchCode": "US"}
            for c in batch
        ]
        resp = throttled_openfigi_post(payload)
        if resp.status_code != 200:
            logging.warning(
                f"OpenFIGI batch {i}-{i+len(batch)} got {resp.status_code}; "
                f"caching all as null"
            )
            now = datetime.now(timezone.utc).isoformat()
            for c in batch:
                cache[c] = {
                    "ticker": None, "figi": None, "name": None,
                    "resolved_at": now,
                }
            save_cusip_cache(cache)
            continue
        try:
            results = resp.json()
        except Exception as e:
            logging.warning(f"OpenFIGI batch JSON parse error: {e}")
            results = [{} for _ in batch]

        now = datetime.now(timezone.utc).isoformat()
        for j, c in enumerate(batch):
            entry = results[j] if j < len(results) else {}
            data_list = entry.get("data") or []
            if data_list:
                pick = data_list[0]
                cache[c] = {
                    "ticker": pick.get("ticker"),
                    "figi": pick.get("figi"),
                    "name": pick.get("name"),
                    "resolved_at": now,
                }
                if len(data_list) > 1:
                    logging.info(
                        f"OpenFIGI {c}: {len(data_list)} matches; picked "
                        f"{pick.get('ticker')}"
                    )
            else:
                cache[c] = {
                    "ticker": None, "figi": None, "name": None,
                    "resolved_at": now,
                }
        save_cusip_cache(cache)
        if (i // BATCH) % 20 == 0:
            done = min(i + BATCH, len(todo))
            logging.info(f"OpenFIGI progress: {done}/{len(todo)} resolved")

    return {c: cache.get(c, {"ticker": None, "figi": None, "name": None})
            for c in unique_cusips}


def attach_tickers(funds_payload: dict, ticker_map: dict) -> tuple:
    resolved = 0
    unknown = 0
    for fund in funds_payload.values():
        for p in fund["positions"]:
            info = ticker_map.get(p["cusip"]) or {}
            p["ticker"] = info.get("ticker")
            if p["ticker"]:
                resolved += 1
            else:
                unknown += 1
    return resolved, unknown


# ---------------------------------------------------------------------------
# Output JSON
# ---------------------------------------------------------------------------

def write_positions_json(
    funds_payload: dict, quarter: str, period_end: str
) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{quarter}.json"

    funds_sorted = dict(sorted(funds_payload.items()))
    payload = {
        "quarter": quarter,
        "filing_period_end": period_end,
        "data_fetched": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
        "funds": {},
    }
    for cik, fund in funds_sorted.items():
        payload["funds"][cik] = {
            "fund_name": fund["fund_name"],
            "filing_date": fund["filing_date"],
            "accession": fund["accession"],
            "total_value": fund["total_value"],
            "position_count": fund["position_count"],
            "positions_skipped": fund["positions_skipped"],
            "value_unit_multiplier": fund.get("value_unit_multiplier", 1),
            "positions": fund["positions"],
        }

    # Atomic write via tempfile + os.replace
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def print_summary(
    quarter: str, funds_payload: dict, resolved: int, unknown: int,
    halts: list, elapsed: float
) -> None:
    total_rows = sum(f["position_count"] for f in funds_payload.values())
    total_value = sum(f["total_value"] for f in funds_payload.values())
    unique_cusips = len({
        p["cusip"] for f in funds_payload.values() for p in f["positions"]
    })
    print()
    print("=" * 72)
    print(f"PARSE SUMMARY  quarter={quarter}  elapsed={elapsed:.1f}s")
    print("=" * 72)
    print(f"funds parsed:        {len(funds_payload)}")
    print(f"total positions:     {total_rows:,}")
    print(f"unique CUSIPs:       {unique_cusips:,}")
    print(f"total AUM (USD):     ${total_value:,.0f}")
    if resolved + unknown > 0:
        pct_resolved = resolved / (resolved + unknown) * 100
        print(
            f"tickers resolved:    {resolved}/{resolved+unknown} "
            f"({pct_resolved:.1f}%); {unknown} unknown"
        )
    if halts:
        print()
        print(f"HALTS ({len(halts)}):")
        for h in halts:
            print(f"  {h}")
    else:
        print("\nno halts triggered")

    print("\nPer-fund row counts:")
    for cik in sorted(funds_payload):
        f = funds_payload[cik]
        expected = EXPECTED_ROWS_2026_Q1.get(cik, "—")
        skipped = f.get("positions_skipped", 0)
        print(
            f"  {cik}  {f['fund_name'][:30]:<30}  "
            f"rows={f['position_count']:>4}  expected={expected!s:>4}  "
            f"skipped={skipped}  total=${f['total_value']:,.0f}"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarter", default="2026-Q1")
    parser.add_argument("--cik", help="parse single CIK only")
    parser.add_argument("--force", action="store_true",
                        help="re-fetch filing metadata; overwrite output")
    parser.add_argument("--dry-run", action="store_true",
                        help="parse + log, don't write output JSON")
    parser.add_argument("--skip-tickers", action="store_true",
                        help="skip OpenFIGI; emit ticker:null everywhere")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()

    if not FUNDS_JSON_PATH.exists():
        logging.error(
            f"funds.json not staged at {FUNDS_JSON_PATH}; scp it from Mac first"
        )
        return 2

    period_end = quarter_to_period_end(args.quarter)
    funds_map = load_funds_map()
    logging.info(
        f"=== parse start  quarter={args.quarter}  period={period_end}  "
        f"force={args.force}  dry_run={args.dry_run}  "
        f"skip_tickers={args.skip_tickers} ==="
    )

    raw_quarter_dir = RAW_DIR / args.quarter
    if not raw_quarter_dir.exists():
        logging.error(f"no raw XML directory at {raw_quarter_dir}")
        return 2

    # Pick target CIKs: those active in funds_map AND present in raw dir.
    if args.cik:
        targets = [args.cik]
    else:
        targets = sorted([
            cik for cik, meta in funds_map.items() if meta["active"]
        ])

    # Filter to those with a raw XML present
    available = []
    for cik in targets:
        xml = raw_quarter_dir / f"{cik}.xml"
        if xml.exists():
            available.append(cik)
        else:
            logging.info(
                f"CIK {cik} ({funds_map.get(cik, {}).get('name','?')}): "
                f"no XML at {xml}, skipping (likely not yet filed)"
            )

    if not available:
        logging.error("no XML files match the requested CIKs")
        return 2

    # Filing metadata (cached)
    filing_meta_map = load_filing_metadata(available, args.quarter, args.force)

    # Parse each fund
    funds_payload: dict = {}
    halts: list = []
    for cik in available:
        xml = raw_quarter_dir / f"{cik}.xml"
        fund_meta = funds_map.get(cik) or {"name": "Unknown", "cluster": "?"}
        filing_meta = filing_meta_map.get(cik, {})
        try:
            fund = parse_one_fund(cik, xml, fund_meta, filing_meta)
        except Exception as e:
            msg = f"parse_one_fund({cik}) raised: {e}"
            logging.error(msg)
            halts.append(msg)
            continue

        expected = EXPECTED_ROWS_2026_Q1.get(cik)
        if expected:
            recovery = fund["position_count"] / expected
            if recovery < PER_FUND_RECOVERY_HALT:
                msg = (
                    f"per-fund recovery halt: CIK {cik} "
                    f"parsed {fund['position_count']} of expected {expected} "
                    f"({recovery:.0%} < {PER_FUND_RECOVERY_HALT:.0%})"
                )
                logging.error(msg)
                halts.append(msg)
        funds_payload[cik] = fund
        logging.info(
            f"CIK {cik}: parsed {fund['position_count']} rows  "
            f"total=${fund['total_value']:,.0f}"
        )

    # Ticker resolution
    resolved = unknown = 0
    if not args.skip_tickers and funds_payload:
        unique = collect_unique_cusips(funds_payload)
        logging.info(f"resolving {len(unique)} unique CUSIPs via OpenFIGI")
        try:
            ticker_map = resolve_tickers(unique)
            resolved, unknown = attach_tickers(funds_payload, ticker_map)
        except RuntimeError as e:
            msg = f"OpenFIGI halt: {e}"
            logging.error(msg)
            halts.append(msg)

        total_uniq = resolved + unknown
        if total_uniq > 0:
            unknown_pct = unknown / total_uniq
            if unknown_pct > UNKNOWN_PCT_HALT:
                msg = (
                    f"ticker-resolution halt: {unknown}/{total_uniq} "
                    f"unknown ({unknown_pct:.1%} > {UNKNOWN_PCT_HALT:.0%})"
                )
                logging.error(msg)
                halts.append(msg)

    # Halt handling
    if halts:
        elapsed = time.monotonic() - start
        print_summary(args.quarter, funds_payload, resolved, unknown,
                      halts, elapsed)
        logging.error("halts triggered — NOT writing positions JSON")
        return 2

    if args.dry_run:
        elapsed = time.monotonic() - start
        print_summary(args.quarter, funds_payload, resolved, unknown,
                      [], elapsed)
        logging.info("DRY-RUN — not writing output JSON")
        return 0

    out_path = write_positions_json(funds_payload, args.quarter, period_end)
    logging.info(f"wrote positions JSON to {out_path}")

    elapsed = time.monotonic() - start
    print_summary(args.quarter, funds_payload, resolved, unknown, [], elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())
