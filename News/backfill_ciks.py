"""
backfill_ciks.py  --  Phase 4: CIK Backfill for CEF Universe
=============================================================
Resolves SEC CIKs for active tickers in cef_tickers where cik IS NULL.

Primary strategy: company_tickers.json
  One SEC API call returns all ~14,000 ticker->CIK mappings.
  Covers the vast majority of standard listed CEFs.

Fallback: MANUAL_CIK_OVERRIDES dict
  Hardcoded for funds confirmed via EDGAR where ticker lookup fails
  (e.g. funds that file under a different legal name).

Sentinel: NO_SEC_CIK_TICKERS set
  Canadian-domiciled ETVs that list on US exchanges but are NOT SEC
  registrants. Stored as cik = -1 to distinguish from NULL (never tried).

Usage:
  python backfill_ciks.py --dry-run   # no DB writes
  python backfill_ciks.py             # live write to Supabase
"""

import argparse
import configparser
import json
import logging
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "gridiron.cfg"

SEC_TICKERS_JSON_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL  = "https://data.sec.gov/submissions/CIK{padded}.json"
SEC_USER_AGENT       = "Gridiron Partners mike@gridironpartners.com"

# ---------------------------------------------------------------------------
# Manual CIK overrides -- confirmed via EDGAR (ticker-based lookup fails for
# these because the SEC filing name doesn't match the trading ticker).
# ---------------------------------------------------------------------------
MANUAL_CIK_OVERRIDES = {
    "VGI":  1528811,   # Virtus Global Multi-Sector Income Fund
    "MGF":  811922,    # MFS Government Markets Income Trust
    "EVF":  1070732,   # Eaton Vance Senior Income Trust
    "EARN": 1560672,   # Ellington Credit Company
    "CXH":  847411,    # MFS Investment Grade Municipal Trust
    "RMI":  1746967,   # RiverNorth Opportunistic Municipal Income Fund
    "IAE":  1385632,   # Voya Asia Pacific High Dividend Equity Income Fund
    "CMU":  801961,    # MFS Municipal Income Trust (SEC map returns wrong CIK 809844)
}

# ---------------------------------------------------------------------------
# No-SEC-CIK sentinels -- Canadian trusts listed on NYSE Arca under exemptive
# relief. They file under Canadian securities law, not SEC. Use cik = -1.
# ---------------------------------------------------------------------------
NO_SEC_CIK_TICKERS = {
    "PSLV",  # Sprott Physical Silver Trust ETV
    "SPPP",  # Sprott Physical Platinum and Palladium Trust
    "PHYS",  # Sprott Physical Gold Trust ETV
    "CEF",   # Sprott Physical Gold and Silver Trust Units
}

# ---------------------------------------------------------------------------
# Logging -- UTF-8 forced to avoid Windows CP1252 UnicodeEncodeError
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("backfill_ciks")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # Console: force UTF-8 on Windows (avoids the charmap errors seen in v1)
    try:
        stream = open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
    except Exception:
        stream = sys.stdout
    ch = logging.StreamHandler(stream)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File: always UTF-8
    fh = logging.FileHandler(SCRIPT_DIR / "backfill_ciks.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def load_config() -> dict:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    return {
        "supabase_url": cfg["supabase"]["url"],
        "supabase_key": cfg["supabase"]["service_role_key"],
    }


# ---------------------------------------------------------------------------
# Supabase REST helpers
# ---------------------------------------------------------------------------
def sb_get(url: str, key: str, path: str, params: str = "") -> list:
    full = f"{url}/rest/v1/{path}?{params}"
    req = urllib.request.Request(full, headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def sb_patch(url: str, key: str, table: str, match: str, payload: dict) -> None:
    body = json.dumps(payload).encode()
    full = f"{url}/rest/v1/{table}?{match}"
    req = urllib.request.Request(full, data=body, method="PATCH", headers={
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    })
    with urllib.request.urlopen(req) as r:
        r.read()


# ---------------------------------------------------------------------------
# SEC company_tickers.json  -- one call to get all ~14k ticker->CIK mappings
# ---------------------------------------------------------------------------
def fetch_sec_ticker_map(logger: logging.Logger) -> dict[str, int]:
    logger.info("Fetching SEC company_tickers.json (one-time call) ...")
    req = urllib.request.Request(
        SEC_TICKERS_JSON_URL,
        headers={"User-Agent": SEC_USER_AGENT}
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = json.load(r)

    ticker_map: dict[str, int] = {}
    for entry in raw.values():
        t = entry.get("ticker", "").upper().strip()
        c = entry.get("cik_str")
        if t and c:
            ticker_map[t] = int(c)

    logger.info(f"  Loaded {len(ticker_map):,} ticker->CIK mappings")
    return ticker_map


# ---------------------------------------------------------------------------
# Optional: verify CIK via Submissions API (confirms entity name)
# ---------------------------------------------------------------------------
def verify_cik(cik: int, logger: logging.Logger) -> str | None:
    padded = str(cik).zfill(10)
    url = SEC_SUBMISSIONS_URL.format(padded=padded)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": SEC_USER_AGENT})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.load(r)
        return data.get("name", "")
    except Exception as e:
        logger.warning(f"    Submissions API call failed for CIK {cik}: {e}")
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Backfill SEC CIKs for cef_tickers")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print actions without writing to database")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=" * 60)
    logger.info(f"=== {'DRY-RUN MODE -- no database writes' if args.dry_run else 'LIVE MODE'} ===")
    logger.info(f"backfill_ciks.py | {date.today()} | dry_run={args.dry_run}")
    logger.info("=" * 60)

    cfg = load_config()
    url = cfg["supabase_url"]
    key = cfg["supabase_key"]

    # 1. Get null-CIK active tickers
    rows = sb_get(url, key, "cef_tickers",
                  "select=ticker,fund_name&status=eq.active&cik=is.null&order=ticker")
    logger.info(f"Found {len(rows)} active tickers with null CIK")

    if not rows:
        logger.info("Nothing to do.")
        return

    # 2. Fetch SEC ticker map
    sec_map = fetch_sec_ticker_map(logger)

    # 3. Process each ticker
    resolved   = {}  # ticker -> cik (int)
    no_sec_cik = []  # Canadian trusts flagged -1
    not_found  = []  # no source found

    total = len(rows)
    for i, row in enumerate(rows, 1):
        ticker = row["ticker"]
        name   = (row.get("fund_name") or "")[:48]
        logger.info(f"[{i:3d}/{total}] {ticker:<8} | {name}")

        # A) Canadian trust sentinel
        if ticker in NO_SEC_CIK_TICKERS:
            logger.info(f"  [SENTINEL] Canadian trust -- marking cik=-1 (not an SEC registrant)")
            no_sec_cik.append(ticker)
            if not args.dry_run:
                sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                         {"cik": -1})
            continue

        # B) Manual override
        if ticker in MANUAL_CIK_OVERRIDES:
            cik = MANUAL_CIK_OVERRIDES[ticker]
            entity = verify_cik(cik, logger) or "(unverified)"
            logger.info(f"  [OVERRIDE] CIK={cik} | {entity}")
            resolved[ticker] = cik
            if not args.dry_run:
                sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                         {"cik": cik})
            time.sleep(0.3)
            continue

        # C) SEC company_tickers.json
        if ticker in sec_map:
            cik = sec_map[ticker]
            entity = verify_cik(cik, logger)
            if entity:
                logger.info(f"  [SEC MAP]  CIK={cik} | {entity}")
            else:
                logger.info(f"  [SEC MAP]  CIK={cik}")
            resolved[ticker] = cik
            if not args.dry_run:
                sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                         {"cik": cik})
            time.sleep(0.3)
            continue

        # D) Not found
        logger.warning(f"  [NOT FOUND] Add to MANUAL_CIK_OVERRIDES or NO_SEC_CIK_TICKERS")
        logger.info(f"  -> https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&forms=N-2")
        not_found.append(ticker)

    # 4. Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("BACKFILL COMPLETE")
    logger.info(f"  Resolved (CIK written):    {len(resolved)}")
    logger.info(f"  No SEC CIK (flagged -1):   {len(no_sec_cik)}")
    logger.info(f"  Not found (manual needed): {len(not_found)}")
    logger.info("=" * 60)

    if resolved:
        logger.info("")
        logger.info("RESOLVED:")
        for t, c in sorted(resolved.items()):
            logger.info(f"  {t:<8}: {c}")

    if no_sec_cik:
        logger.info("")
        logger.info("NO SEC CIK (Canadian trusts, cik flagged -1):")
        for t in sorted(no_sec_cik):
            logger.info(f"  {t}")

    if not_found:
        logger.info("")
        logger.info("NOT FOUND -- add CIK to MANUAL_CIK_OVERRIDES and re-run:")
        for t in sorted(not_found):
            logger.info(f"  {t}  ->  https://efts.sec.gov/LATEST/search-index?q=%22{t}%22&forms=N-2")

    if args.dry_run:
        logger.info("")
        logger.info("Dry-run complete -- no changes written.")
    else:
        logger.info("")
        logger.info("Live run complete.")


if __name__ == "__main__":
    main()
