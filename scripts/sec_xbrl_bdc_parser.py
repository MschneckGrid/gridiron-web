#!/usr/bin/env python3
"""
SEC XBRL BDC Parser — Automated NAV & Fundamentals Refresh
===========================================================
Pulls quarterly BDC financial data from SEC EDGAR's companyfacts API
and upserts into Supabase bdc_quarterly_nav table.

Data extracted per BDC:
  - NAV per share
  - Total net assets
  - Net investment income (total + per share)
  - Distributions per share
  - Total debt (long-term)
  - Shares outstanding
  - Derived: asset coverage ratio, debt-to-equity, NII coverage

Data source: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json
CIK mapping: bdc_tickers.cik column (seeded by migration SQL)

Usage:
  # Set environment variables
  $env:SUPABASE_URL = "https://nzinvxticgyjobkqxxhl.supabase.co"
  $env:SUPABASE_SERVICE_KEY = "your-service-key"

  # Full refresh — all BDCs, last 8 quarters
  python sec_xbrl_bdc_parser.py

  # Single ticker test
  python sec_xbrl_bdc_parser.py --ticker ARCC

  # Verify CIK mappings against SEC
  python sec_xbrl_bdc_parser.py --verify-ciks

  # Dry run (parse but don't write to Supabase)
  python sec_xbrl_bdc_parser.py --dry-run

  # Verbose mode
  python sec_xbrl_bdc_parser.py -v

Gridiron Partners | IronSignal AI
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ============================================================================
#  CONFIGURATION
# ============================================================================

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://nzinvxticgyjobkqxxhl.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# SEC EDGAR API endpoints
SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

# SEC requires a User-Agent header with contact info
SEC_USER_AGENT = "GridironPartners/1.0 (mike@gridironpartners.com)"

# XBRL tags to extract from companyfacts
# Architecture doc Section 3.2: Key XBRL Tag Mapping
XBRL_TAGS = {
    # NAV & Assets
    "NetAssetValue": "nav_per_share",                    # NAV per share (instantaneous)
    "NetAssetValuePerShare": "nav_per_share",            # Alternate tag
    "NetAssetsValue": "total_net_assets",                # Total net assets
    "Assets": "total_assets",                            # Total assets (fallback)
    
    # Income
    "InvestmentIncomeNet": "net_investment_income",       # Net investment income
    "NetInvestmentIncome": "net_investment_income",       # Alternate
    "InvestmentIncomeInvestmentInInterestAndDividendIncome": "total_investment_income",
    
    # Per-share income
    "NetInvestmentIncomePerShare": "nii_per_share",
    "InvestmentIncomeNetPerShareBasic": "nii_per_share",  # Alternate
    
    # Distributions
    "DividendsAndDistributionsPaidPerShare": "distributions_per_share",
    "DividendsCommonStockCashAndInKind": "total_distributions",  # Fallback
    "CommonStockDividendsPerShareDeclared": "distributions_per_share",
    
    # Debt
    "LongTermDebt": "total_debt",
    "LongTermDebtNoncurrent": "total_debt",               # Alternate
    "DebtInstrumentCarryingAmount": "total_debt",          # Alternate
    
    # Shares
    "CommonStockSharesOutstanding": "shares_outstanding",
    "SharesOutstanding": "shares_outstanding",             # Alternate
    "EntityCommonStockSharesOutstanding": "shares_outstanding",  # dei taxonomy
}

# Forms we care about (10-Q for quarterly, 10-K for annual)
VALID_FORMS = {"10-Q", "10-K", "10-Q/A", "10-K/A", "10-KT", "10-QT"}

# How many quarters back to look
DEFAULT_QUARTERS_BACK = 8

# Rate limiting: SEC asks for max 10 requests/sec
SEC_REQUEST_DELAY = 0.15  # seconds between requests


# ============================================================================
#  SUPABASE HELPERS
# ============================================================================

def supabase_request(method: str, path: str, data: Any = None,
                     params: Dict[str, str] = None) -> Any:
    """Generic Supabase REST API request."""
    url = f"{SUPABASE_URL}/rest/v1/{path}"
    if params:
        url += "?" + "&".join(f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items())

    body = json.dumps(data).encode() if data else None
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    if method == "POST":
        # Upsert via POST with on-conflict resolution
        headers["Prefer"] = "resolution=merge-duplicates,return=minimal"

    req = urllib.request.Request(url, data=body, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as resp:
            text = resp.read().decode()
            return json.loads(text) if text.strip() else None
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        print(f"  ⚠ Supabase {method} {path}: {e.code} — {body_text[:300]}")
        return None


def load_bdc_tickers() -> List[Dict]:
    """Load BDC tickers with CIK mappings from Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/bdc_tickers?select=ticker,fund_name,cik&cik=not.is.null"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            rows = json.loads(resp.read().decode())
            return rows
    except urllib.error.HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        print(f"  ⚠ Failed to load bdc_tickers: {e.code} — {body_text[:300]}")
        return []


def upsert_quarterly_nav(rows: List[Dict]) -> int:
    """Upsert parsed quarterly data into bdc_quarterly_nav."""
    if not rows:
        return 0

    written = 0
    # Batch in groups of 20
    for i in range(0, len(rows), 20):
        batch = rows[i:i+20]
        url = f"{SUPABASE_URL}/rest/v1/bdc_quarterly_nav?on_conflict=ticker,report_date"
        body = json.dumps(batch).encode()
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                written += len(batch)
        except urllib.error.HTTPError as e:
            body_text = e.read().decode() if e.fp else ""
            print(f"  ⚠ Upsert batch {i//20 + 1} failed: {e.code} — {body_text[:300]}")
            # Try individual inserts for this batch
            for row in batch:
                try:
                    single_body = json.dumps([row]).encode()
                    single_req = urllib.request.Request(url, data=single_body,
                                                        headers=headers, method="POST")
                    with urllib.request.urlopen(single_req) as resp2:
                        written += 1
                except urllib.error.HTTPError as e2:
                    body2 = e2.read().decode() if e2.fp else ""
                    # Only print first few errors
                    if written == 0 and i == 0:
                        print(f"    Row error: {body2[:200]}")
                except Exception:
                    pass  # Skip this row
    return written


# ============================================================================
#  SEC EDGAR API HELPERS
# ============================================================================

def sec_get(url: str) -> Optional[Dict]:
    """Fetch JSON from SEC EDGAR with proper User-Agent."""
    req = urllib.request.Request(url, headers={
        "User-Agent": SEC_USER_AGENT,
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None  # CIK not found or no XBRL data
        print(f"  ⚠ SEC API error {e.code} for {url}")
        return None
    except Exception as e:
        print(f"  ⚠ SEC API exception: {e}")
        return None


def get_companyfacts(cik: str) -> Optional[Dict]:
    """Fetch all XBRL facts for a company from SEC EDGAR."""
    # CIK must be zero-padded to 10 digits
    padded_cik = str(cik).zfill(10)
    url = SEC_COMPANYFACTS_URL.format(cik=padded_cik)
    return sec_get(url)


def verify_cik_mappings(tickers: List[Dict], verbose: bool = False) -> Dict[str, str]:
    """
    Verify CIK→ticker mappings against SEC's company_tickers.json.
    Returns dict of corrections {ticker: correct_cik}.
    """
    print("\n🔍 Verifying CIK mappings against SEC company_tickers.json...")
    sec_data = sec_get(SEC_COMPANY_TICKERS_URL)
    if not sec_data:
        print("  ⚠ Could not fetch SEC company tickers file")
        return {}

    # Build ticker→CIK lookup from SEC data
    sec_lookup = {}
    for entry in sec_data.values():
        t = entry.get("ticker", "").upper()
        c = str(entry.get("cik_str", ""))
        if t and c:
            sec_lookup[t] = c

    corrections = {}
    for row in tickers:
        ticker = row["ticker"]
        our_cik = row.get("cik", "")
        sec_cik = sec_lookup.get(ticker.upper(), "")

        if not sec_cik:
            print(f"  ⚠ {ticker}: NOT FOUND in SEC tickers file (may be delisted)")
        elif our_cik != sec_cik:
            print(f"  ✗ {ticker}: CIK mismatch — ours={our_cik}, SEC={sec_cik}")
            corrections[ticker] = sec_cik
        elif verbose:
            print(f"  ✓ {ticker}: CIK {our_cik} confirmed")

    if corrections:
        print(f"\n  {len(corrections)} corrections needed. SQL to fix:")
        for ticker, correct_cik in corrections.items():
            print(f"  UPDATE bdc_tickers SET cik = '{correct_cik}' WHERE ticker = '{ticker}';")
    else:
        print(f"\n  ✅ All {len(tickers)} CIK mappings verified!")

    return corrections


def auto_populate_ciks(verbose: bool = False) -> int:
    """
    Fetch SEC company_tickers.json and auto-populate CIK column
    for any bdc_tickers rows where cik IS NULL.
    """
    print("\n🔄 Auto-populating CIK mappings from SEC...")

    # Get tickers with missing CIKs
    url = f"{SUPABASE_URL}/rest/v1/bdc_tickers?select=ticker,fund_name&cik=is.null"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            missing = json.loads(resp.read().decode())
    except urllib.error.HTTPError:
        print("  ⚠ Could not query bdc_tickers for missing CIKs")
        return 0

    if not missing:
        print("  ✅ All tickers already have CIK mappings")
        return 0

    print(f"  Found {len(missing)} tickers without CIK mappings")

    # Fetch SEC lookup
    sec_data = sec_get(SEC_COMPANY_TICKERS_URL)
    if not sec_data:
        print("  ⚠ Could not fetch SEC company tickers file")
        return 0

    sec_lookup = {}
    for entry in sec_data.values():
        t = entry.get("ticker", "").upper()
        c = str(entry.get("cik_str", ""))
        if t and c:
            sec_lookup[t] = c

    updated = 0
    for row in missing:
        ticker = row["ticker"].upper()
        sec_cik = sec_lookup.get(ticker)
        if sec_cik:
            # PATCH the row
            patch_url = (f"{SUPABASE_URL}/rest/v1/bdc_tickers"
                         f"?ticker=eq.{urllib.parse.quote(row['ticker'])}")
            patch_body = json.dumps({"cik": sec_cik}).encode()
            patch_req = urllib.request.Request(patch_url, data=patch_body, headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "return=minimal",
            }, method="PATCH")
            try:
                with urllib.request.urlopen(patch_req):
                    updated += 1
                    if verbose:
                        print(f"  ✓ {ticker} → CIK {sec_cik}")
            except urllib.error.HTTPError as e:
                print(f"  ⚠ Failed to update {ticker}: {e.code}")
        else:
            print(f"  ⚠ {ticker}: not found in SEC tickers file")

    print(f"  ✅ Updated {updated}/{len(missing)} CIK mappings")
    return updated


# ============================================================================
#  XBRL PARSING
# ============================================================================

def determine_fiscal_period(end_date: str, start_date: str = None,
                            form: str = None) -> Optional[str]:
    """
    Convert XBRL period dates to fiscal period string like '2024-Q3'.

    For instantaneous facts (balance sheet items like NAV):
      - end_date only, determine quarter from month
    For duration facts (income items like NII):
      - start_date to end_date, determine if quarterly or annual
    """
    try:
        end = datetime.strptime(end_date, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None

    year = end.year
    month = end.month

    # Determine quarter from end month
    # Most BDCs have Dec fiscal year-end, but some vary
    if month in (1, 2, 3):
        quarter = "Q1"
    elif month in (4, 5, 6):
        quarter = "Q2"
    elif month in (7, 8, 9):
        quarter = "Q3"
    elif month in (10, 11, 12):
        quarter = "Q4"
    else:
        return None

    # For annual filings (10-K), check if duration is ~365 days
    if start_date and form in ("10-K", "10-K/A", "10-KT"):
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            duration = (end - start).days
            if duration > 300:
                # Annual figure — still assign to Q4 period
                quarter = "Q4"
        except (ValueError, TypeError):
            pass

    return f"{year}-{quarter}"


def extract_facts_for_tag(facts_data: Dict, taxonomy: str, tag: str,
                          quarters_back: int = DEFAULT_QUARTERS_BACK,
                          verbose: bool = False) -> List[Dict]:
    """
    Extract quarterly facts for a specific XBRL tag.
    Handles deduplication (same period reported in multiple filings).
    Returns list of {fiscal_period, value, end_date, accession, form, filed} dicts.
    """
    try:
        tag_data = facts_data["facts"][taxonomy][tag]
    except (KeyError, TypeError):
        return []

    # Get the units — could be USD, USD-per-shares, shares, pure, etc.
    units_data = tag_data.get("units", {})
    if not units_data:
        return []

    # Collect all entries across all unit types
    all_entries = []
    for unit_type, entries in units_data.items():
        for entry in entries:
            form = entry.get("form", "")
            if form not in VALID_FORMS:
                continue  # Skip non-quarterly/annual filings

            end_date = entry.get("end", "")
            start_date = entry.get("start", "")
            val = entry.get("val")
            accession = entry.get("accn", "")
            filed = entry.get("filed", "")

            if val is None:
                continue

            fp = determine_fiscal_period(end_date, start_date, form)
            if not fp:
                continue

            # Filter to recent quarters
            try:
                fp_year = int(fp.split("-")[0])
                current_year = date.today().year
                if fp_year < current_year - (quarters_back // 4 + 1):
                    continue
            except (ValueError, IndexError):
                continue

            all_entries.append({
                "fiscal_period": fp,
                "value": float(val),
                "end_date": end_date,
                "start_date": start_date,
                "accession": accession,
                "form": form,
                "filed": filed,
                "unit": unit_type,
            })

    # Deduplicate: for each fiscal_period, keep the entry with the
    # latest filing date (handles amendments)
    by_period = defaultdict(list)
    for e in all_entries:
        by_period[e["fiscal_period"]].append(e)

    deduped = []
    for fp, entries in by_period.items():
        # Prefer 10-Q over 10-K for quarterly data, then latest filed
        entries.sort(key=lambda x: (
            0 if x["form"].startswith("10-Q") else 1,
            x["filed"]
        ), reverse=True)

        # Take the most recent filing
        best = entries[0]

        # But for duration facts, prefer the quarterly-scope version
        # (not the YTD or annual aggregate)
        if best.get("start_date"):
            quarterly_entries = [e for e in entries
                                if e.get("start_date") and
                                _is_quarterly_duration(e["start_date"], e["end_date"])]
            if quarterly_entries:
                quarterly_entries.sort(key=lambda x: x["filed"], reverse=True)
                best = quarterly_entries[0]

        deduped.append(best)

    return deduped


def _is_quarterly_duration(start: str, end: str) -> bool:
    """Check if a duration is approximately one quarter (~60-100 days)."""
    try:
        s = datetime.strptime(start, "%Y-%m-%d")
        e = datetime.strptime(end, "%Y-%m-%d")
        days = (e - s).days
        return 60 <= days <= 100
    except (ValueError, TypeError):
        return False


def parse_bdc_companyfacts(ticker: str, cik: str, facts_data: Dict,
                           quarters_back: int = DEFAULT_QUARTERS_BACK,
                           verbose: bool = False) -> List[Dict]:
    """
    Parse companyfacts JSON for a single BDC.
    Returns list of quarterly rows ready for Supabase upsert.
    """
    if not facts_data or "facts" not in facts_data:
        if verbose:
            print(f"  {ticker}: No XBRL facts data")
        return []

    # Collect all facts by fiscal_period
    period_data = defaultdict(lambda: {
        "ticker": ticker,
        "data_source": "sec_xbrl",
    })

    # Try both us-gaap and dei taxonomies
    taxonomies = ["us-gaap", "dei", "ifrs-full"]

    for tag, field_name in XBRL_TAGS.items():
        for taxonomy in taxonomies:
            facts = extract_facts_for_tag(facts_data, taxonomy, tag,
                                          quarters_back, verbose)
            for fact in facts:
                fp = fact["fiscal_period"]
                row = period_data[fp]

                # Only write if we don't already have this field
                # (first taxonomy match wins)
                if field_name not in row or row.get(field_name) is None:
                    row[field_name] = fact["value"]

                # Capture end_date for report_date
                if not row.get("end_date") or fact["end_date"] > row.get("end_date", ""):
                    row["end_date"] = fact["end_date"]

                # Always capture metadata from the latest filing
                if not row.get("accession_number") or fact["filed"] > row.get("filing_date", ""):
                    row["accession_number"] = fact["accession"]
                    row["filing_date"] = fact["filed"]
                    row["form_type"] = fact["form"]
                    row["fiscal_period"] = fp

    # Post-processing: compute derived metrics
    results = []
    for fp, row in sorted(period_data.items()):
        if not row.get("fiscal_period"):
            continue

        # Ensure we have at least NAV or total_net_assets
        has_nav = row.get("nav_per_share") is not None
        has_assets = row.get("total_net_assets") is not None
        if not has_nav and not has_assets:
            if verbose:
                print(f"  {ticker} {fp}: No NAV or net assets — skipping")
            continue

        # Derive NAV per share from total_net_assets / shares_outstanding
        if not has_nav and has_assets and row.get("shares_outstanding"):
            row["nav_per_share"] = round(
                row["total_net_assets"] / row["shares_outstanding"], 4
            )

        # Derive asset coverage ratio: (total_assets) / total_debt
        # BDC regulatory minimum is 150% (1.5x)
        if row.get("total_net_assets") and row.get("total_debt") and row["total_debt"] > 0:
            total_assets = row.get("total_assets", 0) or (
                row["total_net_assets"] + row["total_debt"]
            )
            row["asset_coverage_ratio"] = round(
                total_assets / row["total_debt"], 4
            )

        # Derive debt-to-equity
        if row.get("total_debt") and row.get("total_net_assets") and row["total_net_assets"] > 0:
            row["debt_to_equity"] = round(
                row["total_debt"] / row["total_net_assets"], 4
            )

        # NII per share from total NII / shares
        if not row.get("nii_per_share") and row.get("net_investment_income") and row.get("shares_outstanding"):
            if row["shares_outstanding"] > 0:
                row["nii_per_share"] = round(
                    row["net_investment_income"] / row["shares_outstanding"], 4
                )

        # Save end_date before cleanup
        end_date = row.get("end_date")

        # Clean up: remove temp fields not in table
        row.pop("total_assets", None)
        row.pop("total_distributions", None)
        row.pop("end_date", None)

        # Derive report_date from the fiscal period end date
        # Use the end_date from the fact data, or estimate from fiscal_period
        report_date = end_date or row.get("filing_date")
        if not report_date and fp:
            # Estimate from fiscal period: 2024-Q1 → 2024-03-31
            try:
                yr, q = fp.split("-")
                q_num = int(q[1])
                month_ends = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
                report_date = f"{yr}-{month_ends[q_num]}"
            except (ValueError, KeyError):
                report_date = row.get("filing_date")

        if not report_date:
            if verbose:
                print(f"  {ticker} {fp}: No report_date — skipping")
            continue

        # Ensure all expected columns match actual table schema
        clean_row = {
            "ticker": ticker,
            "report_date": report_date,
            "fiscal_period": fp,
            "nav_per_share": row.get("nav_per_share"),
            "total_net_assets": row.get("total_net_assets"),
            "net_investment_income": row.get("net_investment_income"),
            "total_investment_income": row.get("total_investment_income"),
            "nii_per_share": row.get("nii_per_share"),
            "distributions_per_share": row.get("distributions_per_share"),
            "total_debt": row.get("total_debt"),
            "shares_outstanding": row.get("shares_outstanding"),
            "asset_coverage_ratio": row.get("asset_coverage_ratio"),
            "debt_to_equity": row.get("debt_to_equity"),
            "accession_number": row.get("accession_number"),
            "filing_date": row.get("filing_date"),
            "form_type": row.get("form_type"),
            "data_source": "sec_xbrl",
        }

        # Remove None values to avoid overwriting manual data with nulls
        # BUT keep all keys consistent (PostgREST requires matching keys in batch)
        # Use a fixed set of columns
        COLUMNS = [
            "ticker", "report_date", "fiscal_period", "nav_per_share",
            "total_net_assets", "net_investment_income", "total_investment_income",
            "nii_per_share", "distributions_per_share", "total_debt",
            "shares_outstanding", "asset_coverage_ratio", "debt_to_equity",
            "accession_number", "filing_date", "form_type", "data_source",
        ]
        final_row = {k: clean_row.get(k) for k in COLUMNS}

        results.append(final_row)

    if verbose and results:
        print(f"  {ticker}: {len(results)} quarterly periods extracted")
        for r in results[-3:]:  # Show latest 3
            nav = r.get('nav_per_share', '?')
            nii = r.get('nii_per_share', '?')
            print(f"    {r['fiscal_period']}: NAV=${nav}  NII/sh=${nii}")

    return results


# ============================================================================
#  MAIN PIPELINE
# ============================================================================

def run_pipeline(ticker_filter: str = None, quarters_back: int = DEFAULT_QUARTERS_BACK,
                 dry_run: bool = False, verbose: bool = False) -> Dict[str, Any]:
    """
    Main pipeline: load tickers → fetch SEC data → parse → upsert.
    """
    print("=" * 60)
    print("SEC XBRL BDC PARSER")
    print("=" * 60)
    print(f"  Date:           {date.today()}")
    print(f"  Quarters back:  {quarters_back}")
    print(f"  Dry run:        {dry_run}")
    print(f"  Supabase:       {SUPABASE_URL}")

    if not SUPABASE_KEY:
        print("\n⚠ SUPABASE_SERVICE_KEY not set!")
        print("  Set it with: $env:SUPABASE_SERVICE_KEY = 'your-key'")
        sys.exit(1)

    # Step 1: Load BDC tickers with CIK mappings
    print("\n[1/4] Loading BDC tickers with CIK mappings...")
    tickers = load_bdc_tickers()

    if not tickers:
        print("  ⚠ No tickers with CIK mappings found.")
        print("  Run the migration SQL first, or use --auto-ciks to populate")
        sys.exit(1)

    if ticker_filter:
        tickers = [t for t in tickers if t["ticker"].upper() == ticker_filter.upper()]
        if not tickers:
            print(f"  ⚠ Ticker {ticker_filter} not found or has no CIK mapping")
            sys.exit(1)

    print(f"  Found {len(tickers)} BDCs with CIK mappings")

    # Step 2: Fetch companyfacts from SEC for each BDC
    print(f"\n[2/4] Fetching SEC EDGAR companyfacts...")
    all_rows = []
    errors = []
    skipped = []

    for i, t in enumerate(tickers, 1):
        ticker = t["ticker"]
        cik = t["cik"]
        name = t.get("fund_name", ticker)

        if verbose:
            print(f"\n  [{i}/{len(tickers)}] {ticker} (CIK {cik}) — {name}")
        else:
            print(f"  [{i}/{len(tickers)}] {ticker}...", end=" ", flush=True)

        # Fetch from SEC
        facts_data = get_companyfacts(cik)
        time.sleep(SEC_REQUEST_DELAY)  # Rate limiting

        if not facts_data:
            errors.append(ticker)
            if not verbose:
                print("⚠ no data")
            continue

        # Parse the facts
        rows = parse_bdc_companyfacts(ticker, cik, facts_data, quarters_back, verbose)

        if rows:
            all_rows.extend(rows)
            if not verbose:
                print(f"✓ {len(rows)} periods")
        else:
            skipped.append(ticker)
            if not verbose:
                print("— no relevant facts")

    # Step 3: Upsert to Supabase
    print(f"\n[3/4] {'DRY RUN — not writing' if dry_run else 'Upserting'} to bdc_quarterly_nav...")
    print(f"  Total rows to write: {len(all_rows)}")

    written = 0
    if not dry_run and all_rows:
        written = upsert_quarterly_nav(all_rows)
        print(f"  ✅ Written: {written}")

    # Step 4: Summary
    print(f"\n[4/4] Summary")
    print("=" * 60)

    # Count unique periods per ticker
    ticker_periods = defaultdict(set)
    for row in all_rows:
        ticker_periods[row["ticker"]].add(row["fiscal_period"])

    latest_periods = set()
    for row in all_rows:
        latest_periods.add(row["fiscal_period"])

    print(f"  Tickers scanned:    {len(tickers)}")
    print(f"  Tickers with data:  {len(ticker_periods)}")
    print(f"  Total rows parsed:  {len(all_rows)}")
    print(f"  Rows written:       {written}")
    print(f"  Periods covered:    {', '.join(sorted(latest_periods)[-4:])}")

    if errors:
        print(f"\n  ⚠ Errors ({len(errors)}): {', '.join(errors)}")
    if skipped:
        print(f"  ⚠ No XBRL data ({len(skipped)}): {', '.join(skipped)}")

    # Show a sample of latest data
    if verbose and all_rows:
        print("\n  Latest data sample:")
        # Group by period, show most recent
        latest = sorted(all_rows, key=lambda x: x.get("fiscal_period", ""), reverse=True)
        shown = set()
        for row in latest[:10]:
            t = row["ticker"]
            if t in shown:
                continue
            shown.add(t)
            nav = row.get("nav_per_share", "—")
            nii = row.get("nii_per_share", "—")
            de = row.get("debt_to_equity", "—")
            print(f"    {t:6s} {row['fiscal_period']:8s}  NAV=${nav}  NII/sh=${nii}  D/E={de}")

    print("=" * 60)

    return {
        "tickers_scanned": len(tickers),
        "tickers_with_data": len(ticker_periods),
        "rows_parsed": len(all_rows),
        "rows_written": written,
        "errors": errors,
        "skipped": skipped,
    }


# ============================================================================
#  CLI
# ============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SEC XBRL BDC Parser — Automated NAV & Fundamentals Refresh"
    )
    parser.add_argument("--ticker", "-t", help="Process single ticker (e.g., ARCC)")
    parser.add_argument("--quarters", "-q", type=int, default=DEFAULT_QUARTERS_BACK,
                        help=f"Quarters of history to fetch (default: {DEFAULT_QUARTERS_BACK})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse but don't write to Supabase")
    parser.add_argument("--verify-ciks", action="store_true",
                        help="Verify CIK mappings against SEC and exit")
    parser.add_argument("--auto-ciks", action="store_true",
                        help="Auto-populate missing CIK mappings from SEC")
    parser.add_argument("--discover-tags", action="store_true",
                        help="Show available XBRL tags for a ticker (requires --ticker)")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose output")

    args = parser.parse_args()

    if args.discover_tags:
        if not args.ticker:
            print("--discover-tags requires --ticker")
            sys.exit(1)
        tickers = load_bdc_tickers()
        t = [x for x in tickers if x["ticker"].upper() == args.ticker.upper()]
        if not t:
            print(f"Ticker {args.ticker} not found")
            sys.exit(1)
        cik = t[0]["cik"]
        print(f"Fetching XBRL tags for {args.ticker} (CIK {cik})...")
        facts = get_companyfacts(cik)
        if facts and "facts" in facts:
            for taxonomy in ("us-gaap", "dei", "ifrs-full"):
                tax_data = facts["facts"].get(taxonomy, {})
                if not tax_data:
                    continue
                # Find NAV/income/debt related tags
                relevant = []
                keywords = ["asset", "income", "invest", "debt", "dividend",
                            "distribut", "share", "nav", "leverage", "net"]
                for tag_name, tag_info in tax_data.items():
                    lower = tag_name.lower()
                    if any(kw in lower for kw in keywords):
                        units = tag_info.get("units", {})
                        total_entries = sum(len(v) for v in units.values())
                        unit_types = list(units.keys())
                        # Get most recent value
                        latest_val = None
                        latest_end = ""
                        for unit_entries in units.values():
                            for e in unit_entries:
                                if e.get("end", "") > latest_end and e.get("form", "") in VALID_FORMS:
                                    latest_end = e["end"]
                                    latest_val = e.get("val")
                        mapped = XBRL_TAGS.get(tag_name, "NOT MAPPED")
                        relevant.append((tag_name, total_entries, unit_types,
                                         latest_end, latest_val, mapped))
                if relevant:
                    print(f"\n  {taxonomy} — {len(relevant)} relevant tags:")
                    relevant.sort(key=lambda x: x[0])
                    for tag_name, count, units, end, val, mapped in relevant:
                        status = f"→ {mapped}" if mapped != "NOT MAPPED" else "  (unmapped)"
                        print(f"    {tag_name}")
                        print(f"      {count} entries, units={units}, latest={end} val={val} {status}")
        else:
            print(f"No XBRL data found for CIK {cik}")
        sys.exit(0)

    if args.verify_ciks:
        tickers = load_bdc_tickers()
        if tickers:
            verify_cik_mappings(tickers, verbose=args.verbose)
        else:
            print("No tickers with CIKs found. Run migration first.")
        sys.exit(0)

    if args.auto_ciks:
        auto_populate_ciks(verbose=args.verbose)
        sys.exit(0)

    run_pipeline(
        ticker_filter=args.ticker,
        quarters_back=args.quarters,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
