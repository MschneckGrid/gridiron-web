#!/usr/bin/env python3
"""
CEF News Scanner — Daily Material Event Monitor (v2 — CIK-based)
================================================================
Scans SEC EDGAR filings and Finnhub news for every CEF/BDC in the universe.
Classifies events, scores materiality, writes to Supabase `cef_news_alerts`.

v2 CHANGE: EDGAR now uses CIK-based submissions API instead of full-text
search. This eliminates false matches where "FT" matched TARGET CORP, etc.
Requires CIK values in cef_tickers/bdc_tickers — run populate_ciks.py first.

Sources:
  1. SEC EDGAR Submissions API — by CIK, guaranteed correct company
  2. Finnhub Company News API (free tier: 60 calls/min)

Usage:
  python cef_news_scanner.py                    # Full scan, all tickers
  python cef_news_scanner.py --ticker UTF       # Single ticker
  python cef_news_scanner.py --days 7           # Custom lookback
  python cef_news_scanner.py --dry-run -v       # Preview with verbose logging

Environment Variables (or .env file):
  SUPABASE_URL          — Your Supabase project URL
  SUPABASE_SERVICE_KEY  — Service role key for inserts
  FINNHUB_API_KEY       — Free API key from finnhub.io
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, date
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
#  Configuration
# ---------------------------------------------------------------------------
def load_dotenv(path: str = ".env"):
    if os.path.isfile(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://nzinvxticgyjobkqxxhl.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
FINNHUB_KEY  = os.environ.get("FINNHUB_API_KEY", "")

# EDGAR rate limit: 10 req/sec with proper User-Agent
EDGAR_HEADERS = {
    "User-Agent": "Gridiron Partners mike@gridironpartners.com",
    "Accept": "application/json",
}
EDGAR_DELAY = 0.15  # seconds between EDGAR requests

# Finnhub rate limit: 60 calls/min
FINNHUB_DELAY = 1.1

# 8-K Item descriptions for human-readable summaries
ITEM_8K_MAP = {
    "1.01": "Entry into a Material Agreement",
    "1.02": "Termination of a Material Agreement",
    "1.03": "Bankruptcy or Receivership",
    "2.01": "Completion of Acquisition or Disposition",
    "2.02": "Results of Operations and Financial Condition",
    "2.03": "Creation of Direct Financial Obligation",
    "2.04": "Triggering Events — Obligation Acceleration",
    "2.05": "Costs for Exit or Disposal Activities",
    "2.06": "Material Impairments",
    "3.01": "Notice of Delisting or Failure to Meet Listing Standards",
    "3.02": "Unregistered Sales of Equity Securities",
    "3.03": "Material Modification to Rights of Security Holders",
    "4.01": "Change in Registrant's Certifying Accountant",
    "4.02": "Non-Reliance on Previously Issued Financial Statements",
    "5.01": "Changes in Control of Registrant",
    "5.02": "Departure/Election of Directors or Officers",
    "5.03": "Amendments to Articles or Bylaws",
    "5.05": "Amendments to Code of Ethics",
    "5.07": "Submission of Matters to Shareholder Vote",
    "7.01": "Regulation FD Disclosure",
    "8.01": "Other Events",
    "9.01": "Financial Statements and Exhibits",
}


# ---------------------------------------------------------------------------
#  Event Classification
# ---------------------------------------------------------------------------
def classify_event(headline: str, summary: str, form_type: str, items: str = "") -> Tuple[str, int]:
    """Classify a news item and assign materiality score (0-100)."""
    text = f"{headline} {summary} {form_type} {items}".upper()

    # Check form type first
    if form_type in ("N-14",):
        return "MERGER", 90
    if form_type in ("SC 13D", "SC 13D/A"):
        return "ACTIVIST", 85
    if form_type in ("SC 13G", "SC 13G/A"):
        return "ACTIVIST", 60
    if form_type == "4":
        return "SEC_FILING", 40
    if form_type in ("DEF 14A", "DEFA14A"):
        # Proxy — check for merger language
        if any(w in text for w in ["MERGER", "REORGANIZ", "LIQUIDAT", "TERMINAT"]):
            return "MERGER", 85
        return "SEC_FILING", 50

    # Keyword-based classification
    if any(w in text for w in ["MERGER", "REORGANIZ", "ACQUI", "N-14"]):
        return "MERGER", 90
    if any(w in text for w in ["LIQUIDAT", "TERMINAT", "WIND DOWN", "DISSOLV"]):
        return "TERMINATION", 88
    if any(w in text for w in ["TENDER OFFER", "TENDER"]):
        return "TENDER_OFFER", 82
    if any(w in text for w in ["RIGHTS OFFER"]):
        return "RIGHTS_OFFERING", 75
    if any(w in text for w in ["ACTIVIST", "13D"]):
        return "ACTIVIST", 85
    if any(w in text for w in ["DISTRIBUT", "DIVIDEND", "SPECIAL DISTRIBUTION"]):
        return "DISTRIBUTION", 70
    if any(w in text for w in ["MANAGEMENT CHANGE", "OFFICER", "DIRECTOR", "PORTFOLIO MANAGER",
                                "5.02", "DEPART", "APPOINT", "RESIGN"]):
        return "MGMT_CHANGE", 78
    if any(w in text for w in ["LEVERAGE", "CREDIT FACILITY", "BORROWING", "2.03"]):
        return "LEVERAGE", 55
    if any(w in text for w in ["BUYBACK", "REPURCHASE PROGRAM"]):
        return "BUYBACK", 65
    if any(w in text for w in ["REGULATORY", "SEC ORDER", "ENFORCEMENT"]):
        return "REGULATORY", 80
    if any(w in text for w in ["IPO", "INITIAL PUBLIC", "NEW FUND"]):
        return "IPO_OFFERING", 60
    if form_type in ("N-CSR", "N-CSRS"):
        return "EARNINGS", 35
    if any(w in text for w in ["EARNING", "FINANCIAL RESULTS", "2.02", "ANNUAL REPORT",
                                "SEMI-ANNUAL"]):
        return "EARNINGS", 40

    return "OTHER", 30


# ---------------------------------------------------------------------------
#  EDGAR Scanner (CIK-based — v2)
# ---------------------------------------------------------------------------
class EDGARScanner:
    """
    Fetch filings from SEC EDGAR using the Submissions API (CIK-based).
    
    v2: Uses https://data.sec.gov/submissions/CIK{padded}.json
    This returns ONLY filings for the specific company — no false matches.
    Requires CIK values populated in cef_tickers/bdc_tickers.
    """

    # Filing types to monitor
    FILING_TYPES = {"8-K", "N-14", "DEF 14A", "DEFA14A", "SC 13D", "SC 13D/A",
                    "SC 13G", "SC 13G/A", "4", "N-CSR", "N-CSRS"}

    def __init__(self, cik_map: Dict[str, int], lookback_days: int = 2):
        self.cik_map = cik_map
        self.lookback_days = lookback_days
        self.session = requests.Session()
        self.session.headers.update(EDGAR_HEADERS)
        self._company_name_cache: Dict[str, str] = {}

    def search_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        """Fetch recent filings for a ticker using its CIK."""
        cik = self.cik_map.get(ticker)
        if not cik:
            logging.warning(f"  No CIK for {ticker} — skipping EDGAR scan")
            return []

        cutoff = date.today() - timedelta(days=self.lookback_days)
        results = []

        try:
            # Fetch company submissions JSON
            padded_cik = str(cik).zfill(10)
            url = f"https://data.sec.gov/submissions/CIK{padded_cik}.json"
            resp = self.session.get(url, timeout=15)
            time.sleep(EDGAR_DELAY)

            if resp.status_code != 200:
                logging.warning(f"  EDGAR submissions API {resp.status_code} for {ticker} (CIK {cik})")
                return []

            data = resp.json()
            company_name = data.get("name", ticker)
            self._company_name_cache[ticker] = company_name

            # Parse recent filings
            recent = data.get("filings", {}).get("recent", {})
            if not recent:
                return []

            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])
            primary_descs = recent.get("primaryDocDescription", [])
            items_list = recent.get("items", [])

            for i in range(len(forms)):
                form = forms[i] if i < len(forms) else ""
                filing_date_str = dates[i] if i < len(dates) else ""
                accession = accessions[i] if i < len(accessions) else ""
                primary_doc = primary_docs[i] if i < len(primary_docs) else ""
                primary_desc = primary_descs[i] if i < len(primary_descs) else ""
                items = items_list[i] if i < len(items_list) else ""

                # Filter by form type
                if form not in self.FILING_TYPES:
                    continue

                # Filter by date
                try:
                    filing_date = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    continue

                if filing_date < cutoff:
                    continue

                # Build the filing record
                record = self._build_record(
                    ticker=ticker,
                    company_name=company_name,
                    form_type=form,
                    filing_date=filing_date,
                    accession=accession,
                    cik=cik,
                    primary_doc=primary_doc,
                    primary_desc=primary_desc,
                    items=items,
                )
                if record:
                    results.append(record)

        except Exception as e:
            logging.warning(f"  EDGAR error for {ticker}: {e}")

        return results

    def _build_record(self, ticker: str, company_name: str, form_type: str,
                      filing_date: date, accession: str, cik: int,
                      primary_doc: str, primary_desc: str, items: str) -> Optional[Dict]:
        """Build a standardized alert record from an EDGAR filing."""

        # Clean company name
        clean_name = re.sub(r'\s*/\w+/?$', '', company_name).strip()
        clean_name = re.sub(r'\s+', ' ', clean_name).title()

        # Build headline based on form type
        headline = self._build_headline(clean_name, ticker, form_type, items, primary_desc)

        # Build summary
        summary = self._build_summary(form_type, items, primary_desc, clean_name)

        # Build filing URL — points to actual filing index page
        acc_clean = accession.replace("-", "")
        if primary_doc:
            source_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{primary_doc}"
        else:
            source_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_clean}/{accession}-index.htm"

        # Classify
        event_type, materiality = classify_event(headline, summary, form_type, items)

        return {
            "ticker": ticker,
            "event_date": filing_date.isoformat(),
            "event_type": event_type,
            "headline": headline[:500],
            "summary": summary[:1000] if summary else None,
            "source_url": source_url,
            "source_name": "SEC EDGAR",
            "filing_type": form_type,
            "materiality": materiality,
        }

    def _build_headline(self, company: str, ticker: str, form: str,
                        items: str, desc: str) -> str:
        """Generate a human-readable headline."""
        if form == "8-K":
            # Parse items for specifics
            item_labels = []
            for item_code in (items or "").split(","):
                item_code = item_code.strip()
                if item_code in ITEM_8K_MAP:
                    item_labels.append(ITEM_8K_MAP[item_code])
            if item_labels:
                return f"{company} ({ticker}): {'; '.join(item_labels[:3])}"
            return f"{company} ({ticker}) filed a material event report (8-K)"

        if form == "N-14":
            return f"{company} ({ticker}) filed merger/reorganization registration"

        if form in ("DEF 14A", "DEFA14A"):
            return f"{company} ({ticker}) filed proxy statement — shareholder vote upcoming"

        if form in ("SC 13D", "SC 13D/A"):
            return f"Activist position disclosed in {company} ({ticker})"

        if form in ("SC 13G", "SC 13G/A"):
            return f"Institutional ownership filing for {company} ({ticker})"

        if form == "4":
            return f"Insider transaction reported for {company} ({ticker})"

        if form == "N-CSR":
            return f"{company} ({ticker}) published annual shareholder report"

        if form == "N-CSRS":
            return f"{company} ({ticker}) published semi-annual shareholder report"

        return f"{company} ({ticker}) filed {form}"

    def _build_summary(self, form: str, items: str, desc: str, company: str) -> str:
        """Generate a plain-English summary explaining what this filing means."""
        if form == "8-K":
            parts = []
            for item_code in (items or "").split(","):
                item_code = item_code.strip()
                if item_code == "2.02":
                    parts.append("The fund reported financial results. Review for changes in NAV, income, or distribution coverage.")
                elif item_code == "5.02":
                    parts.append("An officer or director departed or was appointed. Check for portfolio manager changes that could affect fund strategy.")
                elif item_code == "8.01":
                    parts.append("Other material event disclosed. Review the filing for distribution announcements, NAV updates, or operational changes.")
                elif item_code == "7.01":
                    parts.append("Regulation FD disclosure — material non-public information being made public.")
                elif item_code == "2.03":
                    parts.append("New or modified credit facility/borrowing arrangement. Review for changes in leverage terms.")
                elif item_code == "3.03":
                    parts.append("Material modification to security holders' rights. Could affect preferred shares, distribution terms, or voting rights.")
                elif item_code == "1.01":
                    parts.append("New material agreement entered. Review for advisory changes, sub-advisory arrangements, or merger agreements.")
                elif item_code == "1.02":
                    parts.append("Material agreement terminated. Could signal advisory changes or strategic shift.")
                elif item_code in ITEM_8K_MAP:
                    parts.append(f"{ITEM_8K_MAP[item_code]}.")
            if parts:
                return " ".join(parts[:3])
            return "Material event report (8-K). Review the filing for details that may affect the fund's operations, distributions, or NAV."

        if form == "N-14":
            return (f"Merger/reorganization filing — {company} may be merging with another fund, "
                    "converting its structure, or reorganizing. This typically causes discounts to "
                    "narrow as the fund approaches NAV for the transaction. Review for exchange "
                    "ratios, timeline, and conditions.")

        if form in ("DEF 14A", "DEFA14A"):
            return ("Proxy statement filed ahead of a shareholder vote. May include board elections, "
                    "fee structure changes, advisory agreement renewals, or merger approvals. "
                    "Check for any proposals that could materially affect fund operations.")

        if form in ("SC 13D", "SC 13D/A"):
            return ("Activist investor has disclosed a significant position (>5%) with intent to "
                    "influence management. Watch for proposals to narrow the discount, change "
                    "the advisor, convert to open-end, or liquidate. Often bullish for discount narrowing.")

        if form in ("SC 13G", "SC 13G/A"):
            return ("Institutional investor disclosed a significant passive position (>5%). "
                    "Unlike a 13D, this is a passive filing — no activist intent. "
                    "Still noteworthy for understanding ownership concentration.")

        if form == "4":
            return ("Insider (officer, director, or 10%+ owner) reported a buy or sell transaction. "
                    "Insider buying can signal confidence; selling may be routine diversification. "
                    "Review the filing for transaction size and context.")

        if form == "N-CSR":
            return ("Annual shareholder report — contains complete financial statements, "
                    "portfolio holdings, investment commentary, and distribution details. "
                    "Key document for fundamental analysis.")

        if form == "N-CSRS":
            return ("Semi-annual shareholder report — contains interim financial statements "
                    "and portfolio holdings update. Review for changes in positioning, "
                    "distribution sustainability, and manager commentary.")

        return f"{form} filing. Review for potential impact on fund operations or valuation."


# ---------------------------------------------------------------------------
#  Finnhub News Scanner
# ---------------------------------------------------------------------------
class FinnhubScanner:
    """Fetch company news from Finnhub API."""

    def __init__(self, api_key: str, lookback_days: int = 2):
        self.api_key = api_key
        self.lookback_days = lookback_days
        self.session = requests.Session()

    def search_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        """Fetch recent news articles for a ticker from Finnhub."""
        end_date = date.today()
        start_date = end_date - timedelta(days=self.lookback_days)

        try:
            resp = self.session.get(
                "https://finnhub.io/api/v1/company-news",
                params={
                    "symbol": ticker,
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                    "token": self.api_key,
                },
                timeout=15,
            )
            time.sleep(FINNHUB_DELAY)

            if resp.status_code == 429:
                logging.warning(f"  Finnhub rate limited on {ticker}, sleeping 30s")
                time.sleep(30)
                return []

            if resp.status_code != 200:
                return []

            articles = resp.json()
            if not isinstance(articles, list):
                return []

            results = []
            for art in articles[:5]:  # Max 5 per ticker per source
                record = self._parse_article(art, ticker)
                if record:
                    results.append(record)
            return results

        except Exception as e:
            logging.warning(f"  Finnhub error for {ticker}: {e}")
            return []

    def _parse_article(self, art: Dict, ticker: str) -> Optional[Dict]:
        """Parse a Finnhub article into a standardized alert record."""
        headline = art.get("headline", "")
        summary = art.get("summary", "")
        source = art.get("source", "Finnhub")
        url = art.get("url", "")

        if not headline:
            return None

        # Parse timestamp
        ts = art.get("datetime", 0)
        try:
            event_date = datetime.fromtimestamp(ts).date() if ts else date.today()
        except (ValueError, OSError):
            event_date = date.today()

        event_type, materiality = classify_event(headline, summary, "")

        return {
            "ticker": ticker,
            "event_date": event_date.isoformat(),
            "event_type": event_type,
            "headline": headline[:500],
            "summary": summary[:1000] if summary else None,
            "source_url": url,
            "source_name": source,
            "filing_type": None,
            "materiality": materiality,
        }


# ---------------------------------------------------------------------------
#  Supabase Writer
# ---------------------------------------------------------------------------
class SupabaseWriter:
    """Write alerts to Supabase cef_news_alerts table."""

    def __init__(self):
        self.headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal,resolution=ignore-duplicates",
        }

    def write_alerts(self, alerts: List[Dict]) -> int:
        """Upsert alerts to Supabase. Returns count written."""
        if not alerts:
            return 0

        # Batch in groups of 50
        written = 0
        for i in range(0, len(alerts), 50):
            batch = alerts[i:i + 50]
            try:
                resp = requests.post(
                    f"{SUPABASE_URL}/rest/v1/cef_news_alerts",
                    headers=self.headers,
                    json=batch,
                    timeout=30,
                )
                if resp.status_code in (200, 201):
                    written += len(batch)
                elif resp.status_code == 409:
                    # Duplicate — try one by one
                    for alert in batch:
                        try:
                            r = requests.post(
                                f"{SUPABASE_URL}/rest/v1/cef_news_alerts",
                                headers=self.headers,
                                json=alert,
                                timeout=10,
                            )
                            if r.status_code in (200, 201):
                                written += 1
                        except Exception:
                            pass
                else:
                    logging.warning(f"  Supabase write error {resp.status_code}: {resp.text[:200]}")
            except Exception as e:
                logging.warning(f"  Supabase write error: {e}")

        return written

    def fetch_tickers_with_cik(self) -> Dict[str, int]:
        """Fetch ticker -> CIK mapping from both cef_tickers and bdc_tickers."""
        cik_map = {}
        for table in ("cef_tickers", "bdc_tickers"):
            try:
                resp = requests.get(
                    f"{SUPABASE_URL}/rest/v1/{table}?select=ticker,cik&cik=not.is.null",
                    headers={
                        "apikey": SUPABASE_KEY,
                        "Authorization": f"Bearer {SUPABASE_KEY}",
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    for row in resp.json():
                        cik_map[row["ticker"]] = row["cik"]
            except Exception as e:
                logging.warning(f"  Could not fetch CIKs from {table}: {e}")
        return cik_map

    def fetch_all_tickers(self) -> List[str]:
        """Fetch all tickers from cef_tickers."""
        try:
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/cef_tickers?select=ticker&order=ticker",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                return [r["ticker"] for r in resp.json()]
        except Exception as e:
            logging.warning(f"  Could not fetch tickers: {e}")
        return []


# ---------------------------------------------------------------------------
#  Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="CEF News Scanner v2 (CIK-based)")
    parser.add_argument("--ticker", help="Scan a single ticker")
    parser.add_argument("--days", type=int, default=2, help="Lookback days (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Print results, don't write")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if not SUPABASE_KEY:
        logging.error("SUPABASE_SERVICE_KEY not set. Set in .env or environment.")
        sys.exit(1)

    # Initialize
    writer = SupabaseWriter()

    # Fetch CIK map from Supabase
    logging.info("Loading CIK map from Supabase...")
    cik_map = writer.fetch_tickers_with_cik()
    logging.info(f"  {len(cik_map)} tickers have CIK values")

    if not cik_map:
        logging.error("No CIK values found! Run populate_ciks.py first.")
        sys.exit(1)

    # Determine tickers to scan
    if args.ticker:
        tickers = [args.ticker.upper()]
    else:
        tickers = writer.fetch_all_tickers()

    # Check Finnhub
    use_finnhub = bool(FINNHUB_KEY)
    if not use_finnhub:
        logging.warning("FINNHUB_API_KEY not set — only SEC EDGAR will be scanned.")

    sources = "SEC EDGAR" + (" + Finnhub" if use_finnhub else " (Finnhub disabled — no API key)")
    logging.info(f"Starting CEF News Scan — {len(tickers)} tickers, {args.days}-day lookback")
    logging.info(f"Sources: {sources}")
    logging.info("=" * 60)

    # Initialize scanners
    edgar = EDGARScanner(cik_map=cik_map, lookback_days=args.days)
    finnhub = FinnhubScanner(api_key=FINNHUB_KEY, lookback_days=args.days) if use_finnhub else None

    all_alerts = []
    tickers_with_news = set()
    type_counts: Dict[str, int] = {}
    skipped_no_cik = 0

    for idx, ticker in enumerate(tickers, 1):
        logging.info(f"[{idx}/{len(tickers)}] Scanning {ticker}...")

        alerts = []

        # EDGAR scan (CIK-based)
        if ticker in cik_map:
            logging.info(f"  EDGAR scan: {ticker} (CIK {cik_map[ticker]})")
            edgar_alerts = edgar.search_ticker(ticker)
            alerts.extend(edgar_alerts)
            if edgar_alerts:
                logging.info(f"    → {len(edgar_alerts)} EDGAR filings found")
        else:
            skipped_no_cik += 1
            logging.debug(f"  Skipping EDGAR for {ticker} — no CIK")

        # Finnhub scan
        if finnhub:
            logging.info(f"  Finnhub scan: {ticker}")
            fh_alerts = finnhub.search_ticker(ticker)
            alerts.extend(fh_alerts)
            if fh_alerts:
                logging.info(f"    → {len(fh_alerts)} Finnhub articles found")

        if alerts:
            tickers_with_news.add(ticker)
            for a in alerts:
                t = a.get("event_type", "OTHER")
                type_counts[t] = type_counts.get(t, 0) + 1
                if a.get("materiality", 0) >= 70:
                    logging.info(f"    → {ticker}: {t} — {a['headline'][:80]}")

        all_alerts.extend(alerts)

    # Write or dry-run
    written = 0
    if all_alerts:
        if args.dry_run:
            print(f"\n[DRY RUN] Would write {len(all_alerts)} alerts")
            for a in all_alerts[:20]:
                prio = "HIGH" if a.get("materiality", 0) >= 70 else "med" if a.get("materiality", 0) >= 50 else "low"
                print(f"  [{prio:4}] {a['ticker']:6} {a['event_type']:16} {a['headline'][:70]}")
            if len(all_alerts) > 20:
                print(f"  ... and {len(all_alerts) - 20} more")
        else:
            written = writer.write_alerts(all_alerts)

    # Summary
    print("\n" + "=" * 60)
    print("CEF NEWS SCAN — SUMMARY")
    print("=" * 60)
    print(f"  Date:              {date.today()}")
    print(f"  Tickers scanned:   {len(tickers)}")
    print(f"  Tickers w/ CIK:    {len(tickers) - skipped_no_cik}")
    print(f"  Skipped (no CIK):  {skipped_no_cik}")
    print(f"  Total alerts:      {len(all_alerts)}")
    print(f"  Alerts written:    {written}")
    print(f"  Tickers with news: {len(tickers_with_news)}")
    if type_counts:
        print("  By Event Type:")
        for t, c in sorted(type_counts.items(), key=lambda x: -x[1]):
            print(f"    {t:20} {c}")
    high_prio = [a for a in all_alerts if a.get("materiality", 0) >= 70]
    if high_prio:
        print(f"  ⚠ HIGH PRIORITY (materiality ≥ 70):")
        for a in high_prio[:15]:
            print(f"    • {a['ticker']}: {a['event_type']} — {a['headline'][:70]}")
        if len(high_prio) > 15:
            print(f"    ... and {len(high_prio) - 15} more")
    print("=" * 60)


if __name__ == "__main__":
    main()
