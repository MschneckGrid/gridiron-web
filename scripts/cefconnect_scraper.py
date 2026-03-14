"""
cefconnect_scraper.py
─────────────────────
Does one job per run:

JOB — Fund info scrape (run monthly):
  - Reads active CEF universe from cef_tickers WHERE status='active'
    (universe is now owned by nasdaq_cef_discovery.py — Track 2 complete)
  - Scrapes cefconnect.com/fund/{ticker} for every active ticker
  - Extracts: investment_objective, portfolio_managers, adviser_name,
              inception_date, inception_price, inception_nav,
              is_term_fund, has_tender_offer
  - Upserts into cef_fund_info with source = 'cefconnect'
  - Preserves N-CEN data (fees, AUM, FYE) — only overwrites if cefconnect has a value
  - EDGAR N-2 data (objective_source='edgar_n2') is never overwritten

Run:  python cefconnect_scraper.py
Deps: pip install requests beautifulsoup4 lxml
Schedule: Monthly via Windows Task Scheduler

NOTE: Universe management (additions, delistings, CIK resolution) is handled
by nasdaq_cef_discovery.py (daily, 09:00 EST). CEFConnect is now a data
supplement only — it no longer controls the ticker universe.
"""

import os, sys, time, logging, datetime
import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://nzinvxticgyjobkqxxhl.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

# Fallback: read key from gridiron.cfg in same directory as this script
if not SUPABASE_KEY:
    import pathlib
    cfg = pathlib.Path(__file__).parent / "gridiron.cfg"
    if cfg.exists():
        for line in cfg.read_text().splitlines():
            line = line.strip()
            if line.startswith("SUPABASE_KEY="):
                SUPABASE_KEY = line.split("=", 1)[1].strip()
                break
RATE_LIMIT   = 0.3   # seconds between CEFConnect requests
BATCH_SIZE   = 50

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SCRAPE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://www.cefconnect.com/",
}


# ── Supabase helpers ──────────────────────────────────────────────────────────

def sb_get(path):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{path}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"})
    r.raise_for_status()
    return r.json()

def sb_upsert(table, rows):
    if not rows: return

    PROTECTED_FIELDS = {
        "investment_objective", "objective_source",
        "portfolio_managers", "managers_source",
    }

    hdrs = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}

    def _post_batch(batch_rows):
        if not batch_rows: return
        all_keys = set()
        for row in batch_rows: all_keys.update(row.keys())
        normalized = [{k: row.get(k) for k in all_keys} for row in batch_rows]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=hdrs, json=normalized)
        if not r.ok:
            log.error(f"Batch failed: {r.text[:200]} — retrying row-by-row")
            for row in normalized:
                r2 = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=hdrs, json=[row])
                if not r2.ok:
                    log.error(f"  [{row.get('ticker')}] {r2.text[:100]}")

    with_protected    = []
    without_protected = []
    for row in rows:
        has_protected = any(row.get(k) is not None for k in PROTECTED_FIELDS)
        if has_protected:
            with_protected.append({k: v for k, v in row.items()
                                   if k not in PROTECTED_FIELDS or v is not None})
        else:
            without_protected.append({k: v for k, v in row.items()
                                      if k not in PROTECTED_FIELDS})

    _post_batch(with_protected)
    _post_batch(without_protected)


# ── CEFConnect scraper ────────────────────────────────────────────────────────

def scrape_fund_page(ticker):
    """
    Scrapes cefconnect.com/fund/{ticker} and returns a dict with:
      investment_objective, portfolio_managers, adviser_name,
      inception_date, inception_price, inception_nav,
      is_term_fund, has_tender_offer
    Returns empty dict on failure.
    """
    time.sleep(RATE_LIMIT)
    try:
        r = requests.get(f"https://www.cefconnect.com/fund/{ticker}",
                         headers=SCRAPE_HEADERS, timeout=20)
        if not r.ok:
            log.warning(f"  {ticker}: HTTP {r.status_code}")
            return {}
    except Exception as e:
        log.warning(f"  {ticker}: request error — {e}")
        return {}

    soup = BeautifulSoup(r.text, "lxml")
    lines = [l.strip() for l in soup.get_text(separator="\n", strip=True).split("\n") if l.strip()]

    result = {}

    for i, line in enumerate(lines):
        ll = line.lower()

        # Investment Objective — line after "Investment Objective" label
        if ll == "investment objective" and i + 1 < len(lines):
            obj = lines[i + 1]
            if len(obj) > 40 and obj.lower() != "investment objective":
                result["investment_objective"] = obj
                result["objective_source"] = "cefconnect"

        # Adviser — appears just before "Portfolio Managers"
        if i + 1 < len(lines) and lines[i + 1].lower() == "portfolio managers":
            adviser = line
            if len(adviser) > 5 and not any(x in adviser.lower() for x in
                    ("since inception", "past performance", "basic information",
                     "interactive chart", "capital structure")):
                result["adviser_name"] = adviser

        # Portfolio Managers — line after "Portfolio Managers" label
        if ll == "portfolio managers" and i + 1 < len(lines):
            mgrs = lines[i + 1]
            if len(mgrs) > 3 and mgrs.lower() != "portfolio managers":
                result["portfolio_managers"] = mgrs
                result["managers_source"] = "cefconnect"

        # Inception Date
        if ll == "inception date:" and i + 1 < len(lines):
            raw = lines[i + 1].strip()
            try:
                from dateutil import parser as dp
                result["inception_date"] = dp.parse(raw).date().isoformat()
            except Exception:
                pass

        # Inception Share Price
        if ll == "inception share price:" and i + 1 < len(lines):
            raw = lines[i + 1].replace("$", "").replace(",", "").strip()
            try:
                result["inception_price"] = float(raw)
            except Exception:
                pass

        # Inception NAV
        if ll == "inception nav:" and i + 1 < len(lines):
            raw = lines[i + 1].replace("$", "").replace(",", "").strip()
            try:
                result["inception_nav"] = float(raw)
            except Exception:
                pass

        # Term fund
        if ll == "term:" and i + 1 < len(lines):
            val = lines[i + 1].strip().lower()
            result["is_term_fund"] = val not in ("no", "n/a", "")

        # Tender offer
        if ll == "tender offer:" and i + 1 < len(lines):
            val = lines[i + 1].strip().lower()
            result["has_tender_offer"] = val not in ("no", "n/a", "")

    return result


# ── Job 2: Scrape fund info ───────────────────────────────────────────────────

def load_edgar_protected():
    """
    Return set of tickers that already have edgar_n2 as their objective_source.
    CEFConnect must not overwrite these — EDGAR data takes priority.
    """
    rows = sb_get("cef_fund_info?select=ticker,objective_source,managers_source")
    protected = set()
    for r in rows:
        if r.get("objective_source") == "edgar_n2" or r.get("managers_source") == "edgar_n2":
            protected.add(r["ticker"].upper())
    log.info(f"Source protection: {len(protected)} funds already have edgar_n2 data (will not overwrite)")
    return protected

def scrape_all_funds(tickers_to_scrape):
    tickers = sorted(tickers_to_scrape)
    total   = len(tickers)
    batch   = []

    edgar_protected = load_edgar_protected()
    EDGAR_FIELDS = {"investment_objective", "objective_source", "portfolio_managers", "managers_source"}

    for i, ticker in enumerate(tickers):
        log.info(f"[{i+1}/{total}] {ticker}")
        data = scrape_fund_page(ticker)

        if data:
            row = {"ticker": ticker, "last_updated": datetime.datetime.utcnow().isoformat()}
            if ticker.upper() in edgar_protected:
                data = {k: v for k, v in data.items() if k not in EDGAR_FIELDS}
            row.update(data)
            batch.append(row)
            log.info(f"  obj={'✓' if data.get('investment_objective') else '✗'}  "
                     f"mgrs={'✓' if data.get('portfolio_managers') else '✗'}  "
                     f"incept={'✓' if data.get('inception_date') else '✗'}")
        else:
            log.warning(f"  No data for {ticker}")

        if len(batch) >= BATCH_SIZE:
            log.info(f"  Upserting batch of {len(batch)}...")
            sb_upsert("cef_fund_info", batch)
            batch = []

    if batch:
        log.info(f"  Upserting final {len(batch)}...")
        sb_upsert("cef_fund_info", batch)


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    if not SUPABASE_KEY:
        log.error("SUPABASE_KEY not set"); sys.exit(1)

    # Universe source: cef_tickers (owned by nasdaq_cef_discovery.py)
    # CEFConnect /api/v3/funds no longer used for universe management.
    log.info("=== Loading universe from cef_tickers ===")
    rows = sb_get("cef_tickers?select=ticker&status=eq.active")
    active_tickers = {r["ticker"].upper() for r in rows}
    log.info(f"Universe source: cef_tickers ({len(active_tickers)} active tickers)")

    log.info("=== Scraping CEFConnect fund pages ===")
    scrape_all_funds(active_tickers)

    log.info("=== Summary ===")
    from time import sleep; sleep(1)
    rows = sb_get("cef_fund_info?select=ticker,investment_objective,portfolio_managers,inception_date")
    has_obj  = sum(1 for r in rows if r.get("investment_objective"))
    has_mgrs = sum(1 for r in rows if r.get("portfolio_managers"))
    has_inc  = sum(1 for r in rows if r.get("inception_date"))
    log.info(f"cef_fund_info: {len(rows)} rows | objective={has_obj} | managers={has_mgrs} | inception={has_inc}")
    log.info("✓ Done.")

if __name__ == "__main__":
    run()
