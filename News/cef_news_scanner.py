#!/usr/bin/env python3
"""
CEF News Scanner — Daily Material Event Monitor
=================================================
Scans SEC EDGAR filings and Finnhub news for every CEF in the universe.
Classifies events, scores materiality, writes to Supabase `cef_news_alerts`.

Sources:
  1. SEC EDGAR Full-Text Search (EFTS) — 8-K, N-14, DEF 14A, SC 13D/G, Form 4
  2. Finnhub Company News API (free tier: 60 calls/min)

Usage:
  # Full scan — all tickers
  python cef_news_scanner.py

  # Single ticker
  python cef_news_scanner.py --ticker UTF

  # Custom lookback (default: 2 days)
  python cef_news_scanner.py --days 7

  # Dry run — print results, don't write to Supabase
  python cef_news_scanner.py --dry-run

  # Verbose logging
  python cef_news_scanner.py -v

Environment Variables (or .env file):
  SUPABASE_URL          — Your Supabase project URL
  SUPABASE_SERVICE_KEY  — Service role key (not anon!) for inserts
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

# Load .env if present
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

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FILINGS_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_FULL_TEXT   = "https://efts.sec.gov/LATEST/search-index"

# EDGAR prefers this header — SEC requires identifying info
EDGAR_HEADERS = {
    "User-Agent": "GridironPartners/1.0 (mike@gridironpartners.com)",
    "Accept": "application/json",
}

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Rate limiting
EDGAR_DELAY  = 0.12   # SEC asks for max 10 req/sec
FINNHUB_DELAY = 1.05  # Free tier: 60/min → ~1 req/sec

# ---------------------------------------------------------------------------
#  CEF Ticker Universe
# ---------------------------------------------------------------------------

CEF_TICKERS = [
    "ASA","ADX","PEO","AVK","AWF","AFB","NFJ","EOD","EAD","ERC","ERH","FINS",
    "ARDC","BANX","DHF","DMB","DSM","LEO","BCV","MCI","BGH","MPV","BXSY","BMN",
    "BCAT","BHK","HYT","BTZ","DSU","ECAT","BGR","BDJ","BOE","BGY","CII","FRA",
    "BME","BMEZ","BKT","BKN","BLW","BTA","BIT","MUC","MUJ","MIY","MYN","MPA",
    "MYI","MUA","BTT","MHD","MVF","MVT","MYD","MQY","MQT","BCX","BSTZ","BST",
    "BBN","BFK","BTX","BUI","BGT","BGX","BSL","BGB","BPRE","BWG","RA","IGR",
    "CHY","CHI","CCD","CHW","CGO","CPZ","CSQ","CET","EMO","GLQ","GLO","FOF",
    "UTF","LDP","RQI","RNP","RLTY","PSF","PTA","RFI","STK","CLM","CRF","CIK",
    "DHY","DNP","KTF","DSL","DLY","DBL","DPG","ETX","ECC","EIC","EOI","EOS",
    "EFT","EVV","EIM","EVN","EOT","ETJ","EFR","EVG","ETG","EVT","ETO","ETB",
    "ETV","ETY","EXG","ETW","EARN","ECF","FTHY","FSCO","FSSL","FFA","FPF","FCT",
    "DFP","FFC","FLC","PFD","PFO","FTF","FT","GGN","GNT","GDV","GAB","GGM",
    "GGZ","GRX","GIM","GHI","GOF","GGT","GUT","GLU","HFRO","HIO","HQH","HQL",
    "HTD","HPF","HPI","HIX","HIE","HYB","ISD","IFN","IGD","IGA","IHTA","IHD",
    "IIM","JPC","JPI","JPT","JQC","JRS","JRI","JFR","JHI","JHY","JLS","JBBB",
    "JHB","JGH","HYI","JCE","JOF","KIO","KYN","KSM","KREF","KMF","NMZ","NZF",
    "NAD","NUV","NVG","NUW","JDD","JMM","NID","NXJ","NRK","NAC","NEV","NHA",
    "NKX","NMI","NIM","NUO","NPN","NQP","NXP","NXC","NXR","NSL","NMS","NOM",
    "NOCT","NTG","NML","OPP","OIA","OFS","OXLC","OCCI","PHK","PTY","PCM","PDI",
    "PDO","PKO","PFL","PCN","PNF","PMM","PCK","PMX","PNI","PCI","PGP","PHT",
    "PGZ","PHD","PAXS","PCQ","PML","PMF","PTN","PZC","PPT","RIV","RGT","RMT",
    "RMI","RCS","RSF","RMPL","SA","SCD","DIAX","STEW","TBLD","TDF","TSI","TWN",
    "VBF","VCV","VFL","VGM","VKI","VKQ","VPV","VTN","WDI","WEA","WIA","WIW",
    "XFLT",
]

# CIK lookup for SEC EDGAR — fund name → CIK mapping
# These are the SEC Central Index Keys for the fund entities.
# This is populated on first run or can be pre-loaded.
CIK_CACHE: Dict[str, str] = {}


# ---------------------------------------------------------------------------
#  Event Classification
# ---------------------------------------------------------------------------

EVENT_TYPES = {
    "MERGER":           {"keywords": ["merger", "merging", "combine", "consolidat", "reorganiz", "convert"],
                         "base_materiality": 90},
    "MGMT_CHANGE":      {"keywords": ["management change", "new manager", "advisor change", "portfolio manager",
                                       "officer appoint", "officer resign", "ceo change", "cio change"],
                         "base_materiality": 80},
    "DISTRIBUTION":     {"keywords": ["distribution", "dividend", "special distribution", "distribution change",
                                       "distribution cut", "distribution increase", "return of capital"],
                         "base_materiality": 75},
    "TENDER_OFFER":     {"keywords": ["tender offer", "share repurchase program", "buyback program"],
                         "base_materiality": 85},
    "RIGHTS_OFFERING":  {"keywords": ["rights offering", "transferable rights", "subscription rights"],
                         "base_materiality": 80},
    "ACTIVIST":         {"keywords": ["activist", "dissident", "proxy contest", "proxy fight", "board seat",
                                       "schedule 13d", "sc 13d", "beneficial owner"],
                         "base_materiality": 85},
    "TERMINATION":      {"keywords": ["termination", "liquidat", "wind down", "dissolution", "term fund",
                                       "maturity date"],
                         "base_materiality": 90},
    "BUYBACK":          {"keywords": ["share repurchase", "buyback", "open market purchase"],
                         "base_materiality": 70},
    "LEVERAGE":         {"keywords": ["credit facility", "leverage ratio", "borrowing", "preferred shares",
                                       "auction rate", "debt issuance"],
                         "base_materiality": 65},
    "REGULATORY":       {"keywords": ["sec action", "enforcement", "compliance", "regulatory", "exemptive order"],
                         "base_materiality": 75},
    "EARNINGS":         {"keywords": ["earnings", "financial results", "annual report", "semi-annual",
                                       "shareholder report"],
                         "base_materiality": 55},
    "IPO_OFFERING":     {"keywords": ["initial public offering", "ipo", "secondary offering", "shelf registration",
                                       "s-1", "n-2 filing"],
                         "base_materiality": 70},
}

# SEC filing type → event type hints
FILING_TYPE_MAP = {
    "8-K":      None,          # Could be anything — classify by content
    "N-14":     "MERGER",
    "DEF 14A":  None,          # Proxy — check for activist/governance
    "DEFA14A":  None,
    "SC 13D":   "ACTIVIST",
    "SC 13D/A": "ACTIVIST",
    "SC 13G":   None,          # Passive > 5% holder — lower priority
    "SC 13G/A": None,
    "4":        None,          # Insider trades — classify separately
    "3":        None,
    "N-CSR":    "EARNINGS",
    "N-CSRS":   "EARNINGS",
    "497":      None,          # Prospectus supplement
}


def classify_event(headline: str, summary: str = "", filing_type: str = "") -> Tuple[str, int]:
    """
    Classify a news item into an event type and return (event_type, materiality_score).
    Checks filing type hints first, then keyword matching on headline + summary.
    """
    text = f"{headline} {summary}".lower()

    # Filing type override
    if filing_type and filing_type in FILING_TYPE_MAP:
        forced_type = FILING_TYPE_MAP[filing_type]
        if forced_type:
            return forced_type, EVENT_TYPES[forced_type]["base_materiality"]

    # Keyword matching — first match wins (ordered by specificity)
    for event_type, config in EVENT_TYPES.items():
        for kw in config["keywords"]:
            if kw in text:
                # Adjust materiality based on confidence signals
                mat = config["base_materiality"]
                # Boost if headline (not just summary) contains keyword
                if kw in headline.lower():
                    mat = min(100, mat + 5)
                return event_type, mat

    # No match — generic
    return "OTHER", 40


# ---------------------------------------------------------------------------
#  SEC EDGAR Scanner
# ---------------------------------------------------------------------------

class EDGARScanner:
    """Fetch recent SEC filings for CEF tickers via EDGAR Full-Text Search."""

    # Filing types to monitor
    FILING_TYPES = ["8-K", "N-14", "DEF 14A", "DEFA14A", "SC 13D", "SC 13D/A",
                    "SC 13G", "4", "N-CSR", "N-CSRS"]

    def __init__(self, lookback_days: int = 2):
        self.lookback_days = lookback_days
        self.session = requests.Session()
        self.session.headers.update(EDGAR_HEADERS)

    def search_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        """
        Search EDGAR for recent filings mentioning this ticker.
        Returns list of parsed filing records.
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=self.lookback_days)

        results = []

        # Search for the ticker in filing full-text
        # EDGAR EFTS accepts queries like: q="UTF" forms=8-K
        for filing_type in self.FILING_TYPES:
            try:
                params = {
                    "q": f'"{ticker}"',
                    "forms": filing_type,
                    "dateRange": "custom",
                    "startdt": start_date.isoformat(),
                    "enddt": end_date.isoformat(),
                    "from": 0,
                    "size": 10,
                }

                resp = self.session.get(
                    "https://efts.sec.gov/LATEST/search-index",
                    params=params,
                    timeout=15,
                )

                if resp.status_code == 200:
                    data = resp.json()
                    hits = data.get("hits", {}).get("hits", [])
                    for hit in hits:
                        src = hit.get("_source", {})
                        filing = self._parse_filing(src, ticker, filing_type)
                        if filing:
                            results.append(filing)

                time.sleep(EDGAR_DELAY)

            except Exception as e:
                logging.warning(f"EDGAR search error for {ticker}/{filing_type}: {e}")
                time.sleep(EDGAR_DELAY)

        return results

    def _parse_filing(self, src: Dict, ticker: str, filing_type: str) -> Optional[Dict]:
        """Parse an EDGAR search hit into a standardized alert record."""
        file_date = src.get("file_date", "")
        form_type = src.get("form_type", filing_type)
        company_raw = src.get("display_names", [""])[0] if src.get("display_names") else ""
        file_num = src.get("file_num", [""])[0] if isinstance(src.get("file_num"), list) else ""

        # Clean company name — strip CIK, parenthetical codes, extra whitespace
        company = re.sub(r'\s*\(CIK\s*\d+\)', '', company_raw).strip()
        company = re.sub(r'\s*\(.*?\)\s*$', '', company).strip()
        if not company:
            company = ticker

        # Extract summary from filing description if available
        raw_summary = src.get("display_description", "") or ""
        if isinstance(raw_summary, list):
            raw_summary = raw_summary[0] if raw_summary else ""

        # Build URL to filing
        accession = src.get("accession_no", "")
        entity_id = src.get("entity_id", "")
        if accession:
            acc_clean = accession.replace("-", "")
            source_url = f"https://www.sec.gov/Archives/edgar/data/{entity_id}/{acc_clean}/{accession}-index.htm"
        else:
            source_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={ticker}&type={filing_type}"

        # For key filing types, fetch actual filing content for real summaries
        filing_text = ""
        fetch_types = {"8-K", "N-14", "SC 13D", "SC 13D/A", "DEF 14A", "DEFA14A", "4"}
        if form_type.upper().strip() in fetch_types and accession and entity_id:
            filing_text = self._fetch_filing_content(entity_id, accession)

        # Classify using all available text
        classify_text = f"{form_type} {company} {raw_summary} {filing_text[:500]}"
        event_type, materiality = classify_event(classify_text, raw_summary, form_type)

        # Generate headline and summary — use real filing text if we have it
        headline, summary = self._humanize_filing(
            form_type, company, ticker, event_type, raw_summary, filing_text
        )

        # Re-classify with the better headline/summary for more accurate event type
        if filing_text:
            event_type2, materiality2 = classify_event(headline, summary)
            if materiality2 > materiality:
                event_type, materiality = event_type2, materiality2

        # Parse date
        try:
            event_date = datetime.strptime(file_date, "%Y-%m-%d").date() if file_date else date.today()
        except ValueError:
            event_date = date.today()

        return {
            "ticker": ticker,
            "event_date": event_date.isoformat(),
            "event_type": event_type,
            "headline": headline[:500],
            "summary": summary[:1000] if summary else None,
            "source_url": source_url,
            "source_name": "SEC EDGAR",
            "filing_type": form_type,
            "materiality": materiality,
        }

    def _fetch_filing_content(self, entity_id: str, accession: str) -> str:
        """
        Fetch the actual filing document from EDGAR and extract readable text.
        Returns the first ~2000 chars of meaningful content.
        """
        acc_clean = accession.replace("-", "")
        index_url = f"https://www.sec.gov/Archives/edgar/data/{entity_id}/{acc_clean}/{accession}-index.htm"

        try:
            # Step 1: Fetch the filing index page
            resp = self.session.get(index_url, timeout=15)
            time.sleep(EDGAR_DELAY)
            if resp.status_code != 200:
                return ""

            index_html = resp.text

            # Step 2: Find the primary document link (usually .htm or .txt)
            # Look for the main filing document in the index table
            doc_url = None

            # Pattern: look for rows with the filing type document
            # EDGAR index pages list documents in a table with the filing as first row
            htm_links = re.findall(
                r'href="([^"]+\.htm[l]?)"', index_html, re.IGNORECASE
            )
            txt_links = re.findall(
                r'href="([^"]+\.txt)"', index_html, re.IGNORECASE
            )

            # Filter out index pages and R-files (XBRL renderings)
            for link in htm_links:
                lower = link.lower()
                if '-index' in lower or '/R' in link or 'FilingSummary' in link:
                    continue
                doc_url = link
                break

            if not doc_url and txt_links:
                for link in txt_links:
                    if '-index' not in link.lower():
                        doc_url = link
                        break

            if not doc_url:
                return ""

            # Make absolute URL
            if not doc_url.startswith('http'):
                base = f"https://www.sec.gov/Archives/edgar/data/{entity_id}/{acc_clean}/"
                doc_url = base + doc_url.lstrip('/')

            # Step 3: Fetch the actual document
            resp2 = self.session.get(doc_url, timeout=20)
            time.sleep(EDGAR_DELAY)
            if resp2.status_code != 200:
                return ""

            raw_html = resp2.text

            # Step 4: Strip HTML tags and extract readable text
            text = self._html_to_text(raw_html)

            return text[:3000]  # Cap at 3000 chars for processing

        except Exception as e:
            logging.debug(f"Filing content fetch error: {e}")
            return ""

    @staticmethod
    def _html_to_text(html: str) -> str:
        """Strip HTML tags and clean up whitespace. Returns plain text."""
        # Remove script/style blocks
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode common entities
        text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<')
        text = text.replace('&gt;', '>').replace('&quot;', '"').replace('&#8217;', "'")
        text = text.replace('&#8220;', '"').replace('&#8221;', '"')
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _humanize_filing(self, form_type: str, company: str, ticker: str,
                         event_type: str, raw_desc: str, filing_text: str = "") -> Tuple[str, str]:
        """
        Convert SEC filing metadata + actual content into human-readable headline + summary.
        Returns (headline, summary).
        """
        ft = form_type.upper().strip()

        # --- 8-K: Extract the actual Item reported ---
        if ft == "8-K" and filing_text:
            item_summary = self._extract_8k_items(filing_text, company, ticker)
            if item_summary:
                return item_summary

        # --- Form 4: Extract insider trade details ---
        if ft == "4" and filing_text:
            insider_summary = self._extract_form4(filing_text, ticker)
            if insider_summary:
                return insider_summary

        # --- SC 13D: Extract holder and position ---
        if ft in ("SC 13D", "SC 13D/A") and filing_text:
            activist_summary = self._extract_13d(filing_text, ticker)
            if activist_summary:
                return activist_summary

        # --- N-14: Extract merger details ---
        if ft == "N-14" and filing_text:
            merger_summary = self._extract_n14(filing_text, company, ticker)
            if merger_summary:
                return merger_summary

        # Fallback to template-based summaries
        FILING_HEADLINES = {
            "8-K": f"{company} filed a material event report",
            "N-14": f"{company} filed a merger or reorganization registration",
            "DEF 14A": f"{company} filed a proxy statement — shareholder vote upcoming",
            "DEFA14A": f"{company} filed additional proxy solicitation materials",
            "SC 13D": f"An activist investor disclosed a significant position in {ticker}",
            "SC 13D/A": f"An activist investor amended their position disclosure for {ticker}",
            "SC 13G": f"A large holder filed a passive ownership stake in {ticker}",
            "SC 13G/A": f"A large holder amended their passive ownership disclosure for {ticker}",
            "4": f"An insider bought or sold shares of {ticker}",
            "3": f"A new insider filed an initial ownership report for {ticker}",
            "N-CSR": f"{company} published its annual shareholder report",
            "N-CSRS": f"{company} published its semi-annual shareholder report",
            "497": f"{company} filed a prospectus supplement",
        }

        FILING_SUMMARIES = {
            "8-K": "Material event report (8-K) — could include distribution changes, "
                   "management changes, credit facility amendments, or other significant events.",
            "N-14": "Merger/reorganization filing (N-14) — this fund may be merging with another fund, "
                    "converting its structure, or reorganizing. Discounts typically narrow toward NAV.",
            "DEF 14A": "Proxy statement filed ahead of a shareholder vote. May include board elections, "
                       "fee structure changes, or advisory agreement renewals.",
            "DEFA14A": "Additional proxy materials filed — may indicate a contested proxy or "
                       "supplemental information for an upcoming shareholder vote.",
            "SC 13D": "An investor has taken a position of 5%+ and filed an activist disclosure. "
                      "This often signals pressure to narrow the discount or initiate buybacks.",
            "SC 13D/A": "Amendment to an activist investor's position disclosure. Check for changes in "
                        "ownership percentage or stated intentions.",
            "SC 13G": "A large institutional holder disclosed passive ownership of 5%+. Informational only.",
            "SC 13G/A": "Amendment to a passive large holder's ownership disclosure.",
            "4": "Insider transaction report — a fund officer, director, or affiliated person "
                 "bought or sold shares.",
            "3": "Initial statement of beneficial ownership by a new insider.",
            "N-CSR": "Annual shareholder report — contains full holdings, financials, and management discussion.",
            "N-CSRS": "Semi-annual shareholder report — interim financials and portfolio holdings.",
            "497": "Prospectus supplement — may relate to a new offering or updates to fund terms.",
        }

        headline = FILING_HEADLINES.get(ft, f"{company} filed a {ft} with the SEC")
        base_summary = FILING_SUMMARIES.get(ft, f"SEC filing type: {ft}. Review for details.")

        # Append raw EDGAR description if useful
        if raw_desc and len(raw_desc) > 20:
            summary = f"{base_summary} Filing description: {raw_desc.strip()}"
        else:
            summary = base_summary

        return headline, summary

    @staticmethod
    def _extract_8k_items(text: str, company: str, ticker: str) -> Optional[Tuple[str, str]]:
        """Extract 8-K item numbers and generate specific headline/summary."""

        # 8-K Item number descriptions
        ITEM_MAP = {
            "1.01": ("Material Agreement", "entered into a material definitive agreement"),
            "1.02": ("Agreement Termination", "terminated a material definitive agreement"),
            "2.02": ("Financial Results", "reported financial results"),
            "2.04": ("Triggering Event", "reported a triggering event related to obligations"),
            "3.02": ("Delisting", "received a delisting or compliance notice"),
            "5.01": ("Corporate Changes", "announced changes to its corporate structure"),
            "5.02": ("Officer Change", "announced a departure or appointment of an officer/director"),
            "5.03": ("Articles Amendment", "amended its articles of incorporation or bylaws"),
            "7.01": ("Regulation FD", "made a Regulation FD disclosure"),
            "8.01": ("Other Events", "announced a material event"),
            "9.01": ("Financial Exhibits", "filed financial statements and exhibits"),
        }

        # Find item numbers in the text
        items_found = []
        for item_num, (short, _) in ITEM_MAP.items():
            pattern = rf'Item\s+{re.escape(item_num)}'
            if re.search(pattern, text, re.IGNORECASE):
                items_found.append(item_num)

        if not items_found:
            return None

        primary_item = items_found[0]
        short_name, verb = ITEM_MAP.get(primary_item, ("Event", "filed a report"))

        # Try to extract specific details from text near the item
        detail = ""
        text_lower = text.lower()

        # Look for distribution/dividend amounts
        dist_match = re.search(
            r'\$\s*([\d.]+)\s*per\s*(common\s+)?share', text, re.IGNORECASE
        )
        if dist_match:
            amount = dist_match.group(1)
            detail = f" Distribution of ${amount} per share declared."

        # Look for distribution changes
        if any(w in text_lower for w in ['increase', 'raise', 'higher distribution']):
            detail += " Distribution increase announced."
        elif any(w in text_lower for w in ['decrease', 'reduce', 'cut', 'lower distribution']):
            detail += " Distribution decrease announced."

        # Look for management changes
        if primary_item == "5.02":
            name_match = re.search(
                r'(?:Mr\.|Ms\.|Mrs\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', text
            )
            if name_match:
                detail = f" Involving {name_match.group(0).strip()}."
            if 'resign' in text_lower or 'depart' in text_lower:
                detail += " Officer departure."
            elif 'appoint' in text_lower or 'elect' in text_lower:
                detail += " New appointment."

        # Look for merger/reorg language in 8-K
        if any(w in text_lower for w in ['merger', 'reorganiz', 'consolidat']):
            detail += " Merger or reorganization related."

        # Look for rights offering
        if 'rights offering' in text_lower or 'subscription right' in text_lower:
            detail += " Rights offering announced."

        # Look for tender offer
        if 'tender offer' in text_lower:
            detail += " Tender offer announced."

        # Look for record/payable dates
        record_match = re.search(
            r'record\s+date[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', text, re.IGNORECASE
        )
        pay_match = re.search(
            r'payable?\s+date[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4})', text, re.IGNORECASE
        )
        if record_match:
            detail += f" Record date: {record_match.group(1)}."
        if pay_match:
            detail += f" Payable: {pay_match.group(1)}."

        items_str = ", ".join(f"Item {i}" for i in items_found)
        headline = f"{company}: {short_name} (8-K {items_str})"
        summary = f"{company} {verb}.{detail}".strip()

        return headline, summary

    @staticmethod
    def _extract_form4(text: str, ticker: str) -> Optional[Tuple[str, str]]:
        """Extract insider trading details from Form 4 text."""
        text_lower = text.lower()

        # Try to find the reporting person's name
        name = "An insider"
        name_match = re.search(
            r'reporting\s+person[:\s]+([A-Z][A-Za-z\s,.-]+?)(?:\s{2,}|\n|$)', text
        )
        if not name_match:
            name_match = re.search(
                r'name of reporting person\s*[:\s]+([A-Z][A-Za-z\s,.-]+?)(?:\s{2,}|\n|$)', text
            )
        if name_match:
            name = name_match.group(1).strip()[:60]

        # Determine buy vs sell
        action = "transacted in"
        if 'acqui' in text_lower or 'purchase' in text_lower or re.search(r'\bA\b.*\bcodes?\b', text):
            action = "acquired shares of"
        elif 'dispos' in text_lower or 'sale' in text_lower or re.search(r'\bD\b.*\bcodes?\b', text):
            action = "disposed of shares of"

        # Try to find share count
        shares_match = re.search(r'([\d,]+)\s*(?:shares?|common)', text, re.IGNORECASE)
        shares_str = ""
        if shares_match:
            shares_str = f" ({shares_match.group(1)} shares)"

        headline = f"{name} {action} {ticker}{shares_str}"
        summary = f"Insider transaction (Form 4): {name} {action} {ticker}{shares_str}."

        return headline, summary

    @staticmethod
    def _extract_13d(text: str, ticker: str) -> Optional[Tuple[str, str]]:
        """Extract activist/large holder details from SC 13D."""
        # Try to find the filer name
        filer = "An investor"
        filer_match = re.search(
            r'name[s]?\s+of\s+reporting\s+person[s]?[:\s]+([A-Z][A-Za-z\s,&.-]+?)(?:\s{2,}|\n|$)',
            text, re.IGNORECASE
        )
        if filer_match:
            filer = filer_match.group(1).strip()[:80]

        # Try to find ownership percentage
        pct = ""
        pct_match = re.search(r'([\d.]+)\s*%\s*(?:of|percent)', text, re.IGNORECASE)
        if pct_match:
            pct = f" ({pct_match.group(1)}% ownership)"

        headline = f"{filer} disclosed activist position in {ticker}{pct}"
        summary = (f"Activist disclosure (SC 13D): {filer} has filed as a beneficial owner of 5%+ "
                   f"of {ticker}{pct}. Review for stated intentions regarding fund governance, "
                   f"discount narrowing, or strategic actions.")

        return headline, summary

    @staticmethod
    def _extract_n14(text: str, company: str, ticker: str) -> Optional[Tuple[str, str]]:
        """Extract merger details from N-14 filing."""
        text_lower = text.lower()

        # Look for acquiring/target fund names
        target = ""
        target_match = re.search(
            r'(?:merg|reorganiz|consolidat)\w*\s+(?:with|into)\s+([A-Z][A-Za-z\s&,-]+?)(?:\.|,|\s{2,})',
            text, re.IGNORECASE
        )
        if target_match:
            target = f" with {target_match.group(1).strip()[:80]}"

        headline = f"{company} filed merger/reorganization registration{target}"
        summary = (f"Merger filing (N-14): {company} is pursuing a merger or reorganization{target}. "
                   f"This typically causes the discount to narrow as the fund approaches NAV "
                   f"for the transaction. Review the filing for exchange ratios, timeline, and conditions.")

        return headline, summary


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
        if not self.api_key:
            logging.debug("Finnhub API key not set — skipping Finnhub scan")
            return []

        end_date = date.today()
        start_date = end_date - timedelta(days=self.lookback_days)

        try:
            resp = self.session.get(
                f"{FINNHUB_BASE}/company-news",
                params={
                    "symbol": ticker,
                    "from": start_date.isoformat(),
                    "to": end_date.isoformat(),
                    "token": self.api_key,
                },
                timeout=15,
            )

            if resp.status_code == 200:
                articles = resp.json()
                if isinstance(articles, list):
                    return [self._parse_article(a, ticker) for a in articles if self._is_relevant(a, ticker)]
                return []
            elif resp.status_code == 429:
                logging.warning("Finnhub rate limit hit — sleeping 60s")
                time.sleep(60)
                return []
            else:
                logging.warning(f"Finnhub error for {ticker}: HTTP {resp.status_code}")
                return []

        except Exception as e:
            logging.warning(f"Finnhub error for {ticker}: {e}")
            return []

    def _is_relevant(self, article: Dict, ticker: str) -> bool:
        """Filter out generic market news — only keep CEF-specific items."""
        headline = (article.get("headline", "") or "").lower()
        summary = (article.get("summary", "") or "").lower()
        text = f"{headline} {summary}"

        # Must mention the ticker or be clearly about a closed-end fund event
        ticker_lower = ticker.lower()
        if ticker_lower in text:
            return True

        # Check for CEF-relevant keywords
        cef_keywords = ["closed-end", "closed end", "cef", "distribution", "nav",
                        "premium", "discount", "tender", "merger", "rights offering"]
        return any(kw in text for kw in cef_keywords)

    def _parse_article(self, article: Dict, ticker: str) -> Dict[str, Any]:
        """Parse a Finnhub article into a standardized alert record."""
        headline = article.get("headline", "No headline")
        summary = article.get("summary", "")
        url = article.get("url", "")
        source = article.get("source", "Finnhub")

        # Parse timestamp
        ts = article.get("datetime", 0)
        try:
            event_date = datetime.fromtimestamp(ts).date() if ts else date.today()
        except (OSError, ValueError):
            event_date = date.today()

        # Classify
        event_type, materiality = classify_event(headline, summary)

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
    """Write news alerts to Supabase with upsert/dedup."""

    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.key = key
        self.session = requests.Session()
        self.session.headers.update({
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",  # Upsert on unique constraint
        })

    def write_alerts(self, alerts: List[Dict[str, Any]]) -> int:
        """
        Upsert alerts to cef_news_alerts table.
        Returns count of rows written.
        """
        if not alerts:
            return 0

        # Batch in groups of 100
        written = 0
        for i in range(0, len(alerts), 100):
            batch = alerts[i:i+100]
            try:
                resp = self.session.post(
                    f"{self.url}/rest/v1/cef_news_alerts",
                    json=batch,
                )
                if resp.status_code in (200, 201):
                    written += len(batch)
                elif resp.status_code == 409:
                    # Duplicate — already exists, skip
                    logging.debug(f"Duplicates in batch {i//100 + 1}, attempting individual inserts")
                    written += self._write_individual(batch)
                else:
                    logging.error(f"Supabase write error: {resp.status_code} — {resp.text[:200]}")
            except Exception as e:
                logging.error(f"Supabase write exception: {e}")

        return written

    def _write_individual(self, alerts: List[Dict]) -> int:
        """Fall back to individual inserts for dedup conflict resolution."""
        written = 0
        for alert in alerts:
            try:
                resp = self.session.post(
                    f"{self.url}/rest/v1/cef_news_alerts",
                    json=alert,
                )
                if resp.status_code in (200, 201):
                    written += 1
            except Exception:
                pass
        return written


# ---------------------------------------------------------------------------
#  Main Scanner Orchestrator
# ---------------------------------------------------------------------------

class CEFNewsScanner:
    """Orchestrates the full scan across all tickers and sources."""

    def __init__(self, lookback_days: int = 2, dry_run: bool = False):
        self.lookback_days = lookback_days
        self.dry_run = dry_run

        self.edgar = EDGARScanner(lookback_days=lookback_days)
        self.finnhub = FinnhubScanner(api_key=FINNHUB_KEY, lookback_days=lookback_days)
        self.writer = SupabaseWriter(SUPABASE_URL, SUPABASE_KEY) if not dry_run else None

    def scan_ticker(self, ticker: str) -> List[Dict[str, Any]]:
        """Scan a single ticker across all sources. Returns list of alerts."""
        alerts = []

        # 1. SEC EDGAR
        logging.info(f"  EDGAR scan: {ticker}")
        edgar_hits = self.edgar.search_ticker(ticker)
        alerts.extend(edgar_hits)

        # 2. Finnhub
        if FINNHUB_KEY:
            logging.info(f"  Finnhub scan: {ticker}")
            finnhub_hits = self.finnhub.search_ticker(ticker)
            alerts.extend(finnhub_hits)
            time.sleep(FINNHUB_DELAY)

        # Deduplicate by (ticker, event_date, source_url)
        seen = set()
        unique = []
        for a in alerts:
            key = (a["ticker"], a["event_date"], a.get("source_url", ""))
            if key not in seen:
                seen.add(key)
                unique.append(a)

        return unique

    def scan_all(self, tickers: List[str] = None) -> Dict[str, Any]:
        """
        Scan all tickers (or a subset). Returns summary stats.
        """
        tickers = tickers or CEF_TICKERS
        total_alerts = []
        ticker_counts = {}

        logging.info(f"Starting CEF News Scan — {len(tickers)} tickers, "
                     f"{self.lookback_days}-day lookback")
        logging.info(f"Sources: SEC EDGAR" + (f" + Finnhub" if FINNHUB_KEY else " (Finnhub disabled — no API key)"))
        logging.info("=" * 60)

        for i, ticker in enumerate(tickers, 1):
            logging.info(f"[{i}/{len(tickers)}] Scanning {ticker}...")
            try:
                alerts = self.scan_ticker(ticker)
                if alerts:
                    total_alerts.extend(alerts)
                    ticker_counts[ticker] = len(alerts)
                    for a in alerts:
                        logging.info(f"    → {a['event_type']:16s} | {a['headline'][:60]}")
            except Exception as e:
                logging.error(f"  Error scanning {ticker}: {e}")

        # Write to Supabase
        written = 0
        if total_alerts and not self.dry_run:
            logging.info(f"\nWriting {len(total_alerts)} alerts to Supabase...")
            written = self.writer.write_alerts(total_alerts)
            logging.info(f"Successfully wrote {written} alerts")
        elif self.dry_run:
            logging.info(f"\n[DRY RUN] Would write {len(total_alerts)} alerts")
            written = len(total_alerts)

        # Summary
        summary = {
            "scan_date": date.today().isoformat(),
            "tickers_scanned": len(tickers),
            "total_alerts": len(total_alerts),
            "alerts_written": written,
            "tickers_with_news": len(ticker_counts),
            "by_type": {},
            "high_priority": [],
        }

        for a in total_alerts:
            t = a["event_type"]
            summary["by_type"][t] = summary["by_type"].get(t, 0) + 1
            if a["materiality"] >= 70:
                summary["high_priority"].append(
                    f"{a['ticker']}: {a['event_type']} — {a['headline'][:80]}"
                )

        return summary


# ---------------------------------------------------------------------------
#  CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="CEF News Scanner — Daily Material Event Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cef_news_scanner.py                   # Full scan, all tickers
  python cef_news_scanner.py --ticker UTF      # Single ticker
  python cef_news_scanner.py --days 7          # 7-day lookback
  python cef_news_scanner.py --dry-run -v      # Verbose dry run

Environment:
  SUPABASE_URL, SUPABASE_SERVICE_KEY, FINNHUB_API_KEY
        """,
    )
    parser.add_argument("--ticker", "-t", help="Scan a single ticker")
    parser.add_argument("--days", "-d", type=int, default=2, help="Lookback days (default: 2)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to Supabase")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--output", "-o", help="Save results to JSON file")

    args = parser.parse_args()

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # Validate config
    if not args.dry_run and not SUPABASE_KEY:
        logging.error("SUPABASE_SERVICE_KEY not set. Use --dry-run or set the env var.")
        sys.exit(1)

    if not FINNHUB_KEY:
        logging.warning("FINNHUB_API_KEY not set — only SEC EDGAR will be scanned.")

    # Run scanner
    scanner = CEFNewsScanner(lookback_days=args.days, dry_run=args.dry_run)

    tickers = [args.ticker.upper()] if args.ticker else None
    summary = scanner.scan_all(tickers)

    # Print summary
    print("\n" + "=" * 60)
    print("CEF NEWS SCAN — SUMMARY")
    print("=" * 60)
    print(f"  Date:              {summary['scan_date']}")
    print(f"  Tickers scanned:   {summary['tickers_scanned']}")
    print(f"  Total alerts:      {summary['total_alerts']}")
    print(f"  Alerts written:    {summary['alerts_written']}")
    print(f"  Tickers with news: {summary['tickers_with_news']}")
    print()

    if summary["by_type"]:
        print("  By Event Type:")
        for etype, count in sorted(summary["by_type"].items(), key=lambda x: -x[1]):
            print(f"    {etype:20s}  {count}")
        print()

    if summary["high_priority"]:
        print("  ⚠ HIGH PRIORITY (materiality ≥ 70):")
        for item in summary["high_priority"][:20]:
            print(f"    • {item}")
        print()

    # Optional JSON output
    if args.output:
        with open(args.output, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"  Summary saved to: {args.output}")

    print("=" * 60)


if __name__ == "__main__":
    main()
