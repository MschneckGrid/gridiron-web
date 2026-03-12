"""
populate_ciks.py
────────────────
Maps CEF tickers in cef_tickers to their SEC EDGAR CIKs.

Uses the EDGAR company_tickers.json bulk file — one request returns
ALL public company tickers and CIKs. Much faster and more accurate
than per-ticker lookups.

Run:  python populate_ciks.py
Then: python edgar_fund_info.py
"""

import os, sys, json, time, logging
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://nzinvxticgyjobkqxxhl.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
HEADERS      = {"User-Agent": "Gridiron Partners research@gridironpartners.com"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def sb_get(path):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{path}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    )
    r.raise_for_status()
    return r.json()


def sb_patch(ticker, cik):
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/cef_tickers?ticker=eq.{ticker}",
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        },
        json={"cik": cik}
    )
    if not r.ok:
        log.error(f"  Patch error for {ticker}: {r.status_code} {r.text[:100]}")
    return r.ok


def run():
    if not SUPABASE_KEY:
        log.error("SUPABASE_KEY not set")
        sys.exit(1)

    # Load all tickers from Supabase
    log.info("Loading tickers from Supabase...")
    rows = sb_get("cef_tickers?select=ticker,cik")
    all_tickers = {r["ticker"].upper(): r.get("cik") for r in rows}
    missing = [t for t, cik in all_tickers.items() if not cik]
    log.info(f"  {len(all_tickers)} total tickers, {len(missing)} missing CIKs")

    if not missing:
        log.info("All tickers already have CIKs — nothing to do.")
        return

    # Download EDGAR company_tickers.json — all public companies in one shot
    # This is the correct bulk endpoint per SEC EDGAR APIs
    log.info("Downloading EDGAR company_tickers.json...")
    r = requests.get(
        "https://www.sec.gov/files/company_tickers.json",
        headers=HEADERS, timeout=30
    )
    r.raise_for_status()
    data = r.json()

    # Build ticker → CIK lookup
    # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    ticker_to_cik = {}
    for entry in data.values():
        t = entry.get("ticker", "").upper()
        c = entry.get("cik_str")
        if t and c:
            ticker_to_cik[t] = int(c)

    log.info(f"  EDGAR has {len(ticker_to_cik)} tickers")

    # Match and update
    found = 0
    not_found = []

    for ticker in missing:
        cik = ticker_to_cik.get(ticker)
        if cik:
            ok = sb_patch(ticker, cik)
            if ok:
                log.info(f"  ✓ {ticker} → CIK {cik}")
                found += 1
            time.sleep(0.05)  # small delay between Supabase writes
        else:
            not_found.append(ticker)

    log.info(f"\n{'='*50}")
    log.info(f"✓ Mapped {found} tickers to CIKs")

    if not_found:
        log.warning(f"✗ No CIK found for {len(not_found)} tickers: {not_found}")
        log.info("  These may be OTC funds or use a different ticker on EDGAR.")
        log.info("  They will be skipped in edgar_fund_info.py but won't cause errors.")

    log.info("\nDone. Now run edgar_fund_info.py")


if __name__ == "__main__":
    run()
