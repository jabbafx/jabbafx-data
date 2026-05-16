#!/usr/bin/env python3
"""
parse_form4.py — SEC EDGAR Form 4 XML → structured JSON.

Module 1B Session 2. Reads raw Form 4 XMLs saved by fetch_form4.py and
produces a per-issuer JSON of all parsed filings, suitable for Session 3
to filter (10b5-1 vs discretionary, transaction-code interest level,
dollar-value threshold) and cross-reference against 13F confluence.

Reads:   /root/jabbafx-data-pipeline/data/raw/form4/<issuer-cik>/*.xml
Writes:  /root/jabbafx-data-pipeline/data/output/form4/<issuer-cik>.json
         /root/jabbafx-data-pipeline/logs/parse_form4.log

Form 4 schema (Session 1 recon, confirmed):
  Root: <ownershipDocument>  (no XML namespace)
  Issuer: issuerCik, issuerName, issuerTradingSymbol
  Reporting owner: rptOwnerCik, rptOwnerName, isDirector/isOfficer/
                   isTenPercentOwner, officerTitle
  Filing flag:  <aff10b5One>1</aff10b5One>  (1 = 10b5-1 pre-planned plan)
  Transactions in <nonDerivativeTable> and <derivativeTable>.
  Every leaf value wrapped: <fieldName><value>X</value><footnoteId .../></fieldName>

Transaction codes (most relevant; full list in SEC rules):
  P — open-market purchase (HIGHEST SIGNAL when by officer/director)
  S — open-market sale       (signal IF NOT 10b5-1 pre-planned)
  A — grant/award             (auto-grant; low signal)
  D — disposition to issuer   (low signal)
  F — tax withholding         (low signal — covering vesting tax)
  G — bona fide gift          (low signal)
  M — exercise of derivative  (medium signal)
  X — exercise ITM/ATM deriv  (medium signal)
  V — voluntary report        (rare)
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

from lxml import etree


BASE_DIR = Path("/root/jabbafx-data-pipeline")
RAW_DIR = BASE_DIR / "data" / "raw" / "form4"
OUTPUT_DIR = BASE_DIR / "data" / "output" / "form4"
LOG_PATH = BASE_DIR / "logs" / "parse_form4.log"


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


# ── XML helpers ───────────────────────────────────────────────────────────

def child_text(parent: etree._Element, tag: str) -> Optional[str]:
    """Return text of the first direct child element with the given tag."""
    if parent is None:
        return None
    c = parent.find(tag)
    if c is None or c.text is None:
        return None
    return c.text.strip() or None


def value_text(parent: etree._Element, tag: str) -> Optional[str]:
    """Form 4 wraps leaf values in <value>X</value>. This returns X for
    <parent><tag><value>X</value></tag></parent>.
    """
    if parent is None:
        return None
    c = parent.find(tag)
    if c is None:
        return None
    v = c.find("value")
    if v is None or v.text is None:
        return None
    return v.text.strip() or None


def value_text_nested(
    parent: etree._Element, container: str, tag: str
) -> Optional[str]:
    """<parent><container><tag><value>X</value></tag></container></parent>."""
    cn = parent.find(container) if parent is not None else None
    return value_text(cn, tag)


def parse_int(s: Optional[str]) -> Optional[int]:
    if s is None or s == "":
        return None
    try:
        return int(float(s.replace(",", "")))
    except (ValueError, AttributeError):
        return None


def parse_float(s: Optional[str]) -> Optional[float]:
    if s is None or s == "":
        return None
    try:
        return float(s.replace(",", ""))
    except (ValueError, AttributeError):
        return None


def parse_bool_flag(s: Optional[str]) -> bool:
    """Form 4 uses '1' / '0' (or sometimes 'true'/'false') for booleans."""
    if s is None:
        return False
    return s.strip().lower() in ("1", "true", "yes")


# ── Section parsers ───────────────────────────────────────────────────────

def parse_issuer(root: etree._Element) -> dict:
    iss = root.find("issuer")
    if iss is None:
        return {"cik": None, "name": None, "ticker": None}
    return {
        "cik": child_text(iss, "issuerCik"),
        "name": child_text(iss, "issuerName"),
        "ticker": child_text(iss, "issuerTradingSymbol"),
    }


def parse_reporting_owner(root: etree._Element) -> dict:
    ro = root.find("reportingOwner")
    if ro is None:
        return {}
    ident = ro.find("reportingOwnerId")
    rel = ro.find("reportingOwnerRelationship")
    out: dict = {
        "cik": child_text(ident, "rptOwnerCik") if ident is not None else None,
        "name": child_text(ident, "rptOwnerName") if ident is not None else None,
        "is_director": False,
        "is_officer": False,
        "is_ten_percent_owner": False,
        "is_other": False,
        "officer_title": None,
        "other_text": None,
    }
    if rel is not None:
        out["is_director"] = parse_bool_flag(child_text(rel, "isDirector"))
        out["is_officer"] = parse_bool_flag(child_text(rel, "isOfficer"))
        out["is_ten_percent_owner"] = parse_bool_flag(
            child_text(rel, "isTenPercentOwner")
        )
        out["is_other"] = parse_bool_flag(child_text(rel, "isOther"))
        out["officer_title"] = child_text(rel, "officerTitle")
        out["other_text"] = child_text(rel, "otherText")
    return out


def _parse_transaction_common(tx: etree._Element) -> dict:
    """Fields shared by non-derivative and derivative transactions."""
    coding = tx.find("transactionCoding")
    code = child_text(coding, "transactionCode") if coding is not None else None
    equity_swap = (
        parse_bool_flag(child_text(coding, "equitySwapInvolved"))
        if coding is not None else False
    )
    shares = parse_int(value_text_nested(tx, "transactionAmounts", "transactionShares"))
    price = parse_float(
        value_text_nested(tx, "transactionAmounts", "transactionPricePerShare")
    )
    acquired_disposed = value_text_nested(
        tx, "transactionAmounts", "transactionAcquiredDisposedCode"
    )
    shares_after = parse_int(
        value_text_nested(tx, "postTransactionAmounts",
                          "sharesOwnedFollowingTransaction")
    )
    ownership_type = value_text_nested(
        tx, "ownershipNature", "directOrIndirectOwnership"
    )
    return {
        "security_title": value_text(tx, "securityTitle"),
        "transaction_date": value_text(tx, "transactionDate"),
        "transaction_code": code,
        "equity_swap_involved": equity_swap,
        "shares": shares,
        "price_per_share": price,
        # Computed total dollar value of the transaction (sign per direction)
        "value": (shares * price) if (shares is not None and price is not None
                                       and price > 0) else None,
        "acquired_disposed": acquired_disposed,
        "shares_after": shares_after,
        "ownership_type": ownership_type,
    }


def parse_non_derivative(root: etree._Element) -> list:
    table = root.find("nonDerivativeTable")
    if table is None:
        return []
    return [_parse_transaction_common(tx)
            for tx in table.findall("nonDerivativeTransaction")]


def parse_derivative(root: etree._Element) -> list:
    """Derivative transactions also include conversion/exercise prices and
    underlying shares. For Session 2 we capture the same canonical fields
    plus underlying-shares count; Session 3 may refine."""
    table = root.find("derivativeTable")
    if table is None:
        return []
    out = []
    for tx in table.findall("derivativeTransaction"):
        common = _parse_transaction_common(tx)
        # Derivative-specific
        underlying = tx.find("underlyingSecurity")
        common["underlying_security_title"] = (
            value_text(underlying, "underlyingSecurityTitle")
            if underlying is not None else None
        )
        common["underlying_shares"] = parse_int(
            value_text(underlying, "underlyingSecurityShares")
            if underlying is not None else None
        )
        common["conversion_price"] = parse_float(
            value_text(tx, "conversionOrExercisePrice")
        )
        common["expiration_date"] = value_text(tx, "expirationDate")
        out.append(common)
    return out


# ── Filing parser ─────────────────────────────────────────────────────────

def parse_one_filing(xml_path: Path) -> Optional[dict]:
    try:
        tree = etree.parse(str(xml_path))
    except etree.XMLSyntaxError as e:
        logging.warning(f"parse error in {xml_path.name}: {e}")
        return None
    root = tree.getroot()
    if etree.QName(root).localname != "ownershipDocument":
        logging.warning(
            f"unexpected root tag in {xml_path.name}: {root.tag}"
        )
        return None

    accession_from_filename = xml_path.stem  # e.g. "0001199039-26-000003"

    return {
        "accession": accession_from_filename,
        "source_file": xml_path.name,
        "schema_version": child_text(root, "schemaVersion"),
        "document_type": child_text(root, "documentType"),
        "period_of_report": child_text(root, "periodOfReport"),
        "not_subject_to_section_16":
            parse_bool_flag(child_text(root, "notSubjectToSection16")),
        "aff_10b5_one":
            parse_bool_flag(child_text(root, "aff10b5One")),
        "issuer": parse_issuer(root),
        "reporting_owner": parse_reporting_owner(root),
        "non_derivative_transactions": parse_non_derivative(root),
        "derivative_transactions": parse_derivative(root),
    }


def parse_issuer_dir(issuer_cik: str) -> list:
    issuer_dir = RAW_DIR / issuer_cik.zfill(10)
    if not issuer_dir.exists():
        logging.error(f"no raw dir at {issuer_dir}")
        return []
    filings = []
    for xml_path in sorted(issuer_dir.glob("*.xml")):
        rec = parse_one_filing(xml_path)
        if rec is not None:
            filings.append(rec)
    return filings


# ── Output ────────────────────────────────────────────────────────────────

def write_issuer_json(issuer_cik: str, filings: list) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"{issuer_cik.zfill(10)}.json"

    # Derive issuer name/ticker from the first parsed filing (consistent
    # across all filings for the same issuer CIK)
    issuer_meta: dict = {}
    if filings:
        issuer_meta = filings[0].get("issuer", {}) or {}

    # Sort filings by period_of_report descending (most recent first)
    filings_sorted = sorted(
        filings,
        key=lambda f: (f.get("period_of_report") or ""),
        reverse=True,
    )

    payload = {
        "issuer_cik": issuer_cik.zfill(10),
        "issuer_name": issuer_meta.get("name"),
        "issuer_ticker": issuer_meta.get("ticker"),
        "data_parsed": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z"),
        "filing_count": len(filings_sorted),
        "filings": filings_sorted,
    }

    # Atomic write
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=str(OUTPUT_DIR), delete=False
    ) as tf:
        json.dump(payload, tf, indent=2)
        tmp = tf.name
    os.replace(tmp, out_path)
    return out_path


# ── Summary ───────────────────────────────────────────────────────────────

def print_summary(issuer_cik: str, filings: list, elapsed: float) -> None:
    total_tx = sum(
        len(f.get("non_derivative_transactions", []))
        + len(f.get("derivative_transactions", []))
        for f in filings
    )
    by_code: dict = {}
    for f in filings:
        for tx in f.get("non_derivative_transactions", []):
            c = tx.get("transaction_code") or "?"
            by_code[c] = by_code.get(c, 0) + 1
    # Use same period-desc ordering as the JSON output
    filings_sorted = sorted(
        filings, key=lambda f: (f.get("period_of_report") or ""), reverse=True
    )
    print()
    print("=" * 60)
    print(f"PARSE-FORM4 SUMMARY  issuer={issuer_cik}  elapsed={elapsed:.2f}s")
    print("=" * 60)
    print(f"filings parsed:           {len(filings)}")
    print(f"total transactions:       {total_tx}")
    print(f"non-derivative tx codes:  {dict(sorted(by_code.items()))}")
    if filings_sorted:
        sample = filings_sorted[0]
        iss = sample.get("issuer", {})
        ro = sample.get("reporting_owner", {})
        role = []
        if ro.get("is_director"): role.append("director")
        if ro.get("is_officer"): role.append(f"officer ({ro.get('officer_title') or '—'})")
        if ro.get("is_ten_percent_owner"): role.append("10%+")
        print(
            f"\nmost-recent filing: {sample.get('period_of_report')} "
            f"{iss.get('ticker')} — {ro.get('name')} ({'/'.join(role) or 'none'})"
        )
        if sample.get("aff_10b5_one"):
            print("  (10b5-1 plan flagged)")


# ── Main ──────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cik", required=True,
        help="issuer CIK (10-digit padded)"
    )
    parser.add_argument("--force", action="store_true",
                        help="overwrite output JSON if present")
    parser.add_argument("--dry-run", action="store_true",
                        help="parse + summarize, write nothing")
    args = parser.parse_args()

    setup_logger()
    start = time.monotonic()
    logging.info(
        f"=== parse_form4 start  cik={args.cik}  force={args.force}  "
        f"dry_run={args.dry_run} ==="
    )

    out_path = OUTPUT_DIR / f"{args.cik.zfill(10)}.json"
    if out_path.exists() and not args.force and not args.dry_run:
        logging.warning(
            f"output already at {out_path}; use --force to overwrite"
        )
        return 0

    filings = parse_issuer_dir(args.cik)
    if not filings:
        logging.error(f"no parseable filings for CIK {args.cik}")
        return 2

    if not args.dry_run:
        out = write_issuer_json(args.cik, filings)
        logging.info(f"wrote {out}")

    print_summary(args.cik, filings, time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
