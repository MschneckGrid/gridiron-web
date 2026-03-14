#!/usr/bin/env python3
"""
SEC BDC XBRL Pipeline
=====================
Downloads SEC BDC bulk data (TSV flat files), parses SUB/NUM/TAG/SOI tables,
and upserts fundamentals + holdings into Supabase.

Data source: https://www.sec.gov/data-research/sec-markets-data/bdc-data-sets
Format:      Monthly ZIP files containing tab-delimited TSV tables.

Tables used:
  - SUB:  Filing metadata (adsh, CIK, form type, period, prevrpt)
  - NUM:  Numeric facts (NAV, NII, debt, etc.) keyed by adsh+tag+ddate
  - TAG:  Tag definitions (resolve custom tags via tlabel)
  - SOI:  Schedule of Investments (one row per holding)

Supabase targets:
  - bdc_quarterly_nav:  Fundamentals (NAV, NII, debt, distributions)
  - bdc_holdings:       Portfolio positions (issuer, type, fair value, etc.)
  - sec_filing_tracker: What we've already processed

Usage:
  python sec_bdc_pipeline.py                      # Process all available ZIP files
  python sec_bdc_pipeline.py --quarter 2025q1     # Process specific quarter
  python sec_bdc_pipeline.py --month 2025_11      # Process specific month
  python sec_bdc_pipeline.py --dry-run             # Parse only, no Supabase writes
  python sec_bdc_pipeline.py --verify ARCC,MAIN   # Verify data for specific tickers

Environment variables:
  SUPABASE_URL  - Your Supabase project URL
  SUPABASE_KEY  - Your Supabase service_role key (not anon key)

Requirements:
  pip install supabase pandas requests
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests

# ---------------------------------------------------------------------------
#  Configuration
# ---------------------------------------------------------------------------

SEC_BDC_BASE_URL = "https://www.sec.gov/files/structureddata/data/business-development-company-bdc-data-sets"
SEC_USER_AGENT = "IronSignalAI/1.0 (mike@gridironpartners.com)"  # SEC requires user-agent

# XBRL tags → our field names for NUM table extraction
# Standard taxonomy tags that BDCs commonly use
NUM_TAG_MAP = {
    # NAV per share - multiple possible tags
    "NetAssetValuePerShare": "nav_per_share",
    "NetAssetValue": "nav_per_share",  # some filers use this
    # Total net assets
    "NetAssetsValue": "total_net_assets",
    "AssetsNet": "total_net_assets",
    "NetAssets": "total_net_assets",
    # Net investment income (total, not per-share)
    "InvestmentIncomeNet": "net_investment_income",
    "NetInvestmentIncomeLoss": "net_investment_income",
    # Total investment income
    "InvestmentIncomeInvestmentInInterestAndDividendIncome": "total_investment_income",
    "InvestmentIncomeInterestAndDividend": "total_investment_income",
    "InvestmentIncomeInterest": "total_investment_income",
    # Debt
    "LongTermDebt": "total_debt",
    "LongTermDebtNoncurrent": "total_debt",
    "DebtInstrumentCarryingAmount": "total_debt",
    "SecuredDebt": "total_debt",
    # Shares outstanding
    "CommonStockSharesOutstanding": "shares_outstanding",
    "SharesOutstanding": "shares_outstanding",
    # Distributions per share
    "DividendsAndDistributionsPaidPerShare": "distributions_per_share",
    "DistributionMadeToLimitedPartnerDistributionsPerUnit": "distributions_per_share",
    "CommonStockDividendsPerShareDeclared": "distributions_per_share",
    # NII per share
    "InvestmentIncomePerShareNet": "nii_per_share",
    "NetInvestmentIncomePerShare": "nii_per_share",
    # Asset coverage ratio
    "AssetCoverageRatio": "asset_coverage_ratio",
}

# Custom tag patterns (matched against tlabel in TAG table)
CUSTOM_TAG_PATTERNS = {
    r"(?i)net\s*asset\s*value\s*per\s*share": "nav_per_share",
    r"(?i)net\s*investment\s*income\s*per\s*share": "nii_per_share",
    r"(?i)net\s*investment\s*income(?!\s*per)": "net_investment_income",
    r"(?i)total\s*investment\s*income": "total_investment_income",
    r"(?i)total\s*net\s*assets": "total_net_assets",
    r"(?i)asset\s*coverage\s*ratio": "asset_coverage_ratio",
}

# Fields we want to extract from NUM, in priority order per target field
FIELD_PRIORITY = {
    "nav_per_share": ["NetAssetValuePerShare", "NetAssetValue"],
    "total_net_assets": ["NetAssetsValue", "AssetsNet", "NetAssets"],
    "net_investment_income": ["InvestmentIncomeNet", "NetInvestmentIncomeLoss"],
    "total_investment_income": [
        "InvestmentIncomeInvestmentInInterestAndDividendIncome",
        "InvestmentIncomeInterestAndDividend",
        "InvestmentIncomeInterest",
    ],
    "total_debt": ["LongTermDebt", "LongTermDebtNoncurrent", "DebtInstrumentCarryingAmount", "SecuredDebt"],
    "shares_outstanding": ["CommonStockSharesOutstanding", "SharesOutstanding"],
    "distributions_per_share": [
        "DividendsAndDistributionsPaidPerShare",
        "DistributionMadeToLimitedPartnerDistributionsPerUnit",
        "CommonStockDividendsPerShareDeclared",
    ],
    "nii_per_share": ["InvestmentIncomePerShareNet", "NetInvestmentIncomePerShare"],
    "asset_coverage_ratio": ["AssetCoverageRatio"],
}

# ZIP files available (update as new ones are published)
AVAILABLE_ZIPS = [
    # Quarterly files (older, larger)
    "2022q4_bdc.zip",
    "2023q1_bdc.zip", "2023q2_bdc.zip", "2023q3_bdc.zip", "2023q4_bdc.zip",
    "2024q1_bdc.zip", "2024q2_bdc.zip", "2024q3_bdc.zip", "2024q4_bdc.zip",
    "2025q1_bdc.zip",
    # Monthly files (newer, smaller)
    "2025_04_bdc.zip", "2025_05_bdc.zip", "2025_06_bdc.zip",
    "2025_07_bdc.zip", "2025_08_bdc.zip", "2025_09_bdc.zip",
    "2025_10_bdc.zip", "2025_11_bdc.zip", "2025_12_bdc.zip",
    "2026_01_bdc.zip",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("sec_bdc_pipeline")


# ---------------------------------------------------------------------------
#  Supabase Client
# ---------------------------------------------------------------------------

class SupabaseClient:
    """Thin wrapper around supabase-py for our upsert operations."""

    def __init__(self, url: str, key: str):
        from supabase import create_client
        self.client = create_client(url, key)
        self._cik_to_ticker: Dict[int, str] = {}
        self._known_tickers: Set[str] = set()
        self._processed_adshs: Set[str] = set()

    def load_cik_map(self) -> Dict[int, str]:
        """Load CIK→ticker mapping from bdc_tickers."""
        resp = self.client.table("bdc_tickers").select("ticker, cik").not_.is_("cik", "null").execute()
        self._cik_to_ticker = {int(r["cik"]): r["ticker"] for r in resp.data}
        self._known_tickers = {r["ticker"] for r in resp.data}
        log.info(f"Loaded {len(self._cik_to_ticker)} CIK→ticker mappings")
        return self._cik_to_ticker

    def load_processed_filings(self) -> Set[str]:
        """Load already-processed accession numbers."""
        resp = self.client.table("sec_filing_tracker").select("adsh").execute()
        self._processed_adshs = {r["adsh"] for r in resp.data}
        log.info(f"Found {len(self._processed_adshs)} previously processed filings")
        return self._processed_adshs

    def ticker_for_cik(self, cik: int) -> Optional[str]:
        return self._cik_to_ticker.get(cik)

    def is_processed(self, adsh: str) -> bool:
        return adsh in self._processed_adshs

    def upsert_fundamentals(self, rows: List[Dict]) -> int:
        """Upsert rows into bdc_quarterly_nav."""
        if not rows:
            return 0
        # Batch in chunks of 50
        total = 0
        for i in range(0, len(rows), 50):
            batch = rows[i:i+50]
            resp = self.client.table("bdc_quarterly_nav").upsert(
                batch, on_conflict="ticker,report_date"
            ).execute()
            total += len(batch)
        return total

    def upsert_holdings(self, rows: List[Dict]) -> int:
        """Insert holdings into bdc_holdings, replacing existing for same adsh."""
        if not rows:
            return 0
        # Delete existing holdings for these accession numbers (idempotent)
        adshs = list(set(r["adsh"] for r in rows))
        for adsh in adshs:
            self.client.table("bdc_holdings").delete().eq("adsh", adsh).execute()
        # Insert in batches
        total = 0
        for i in range(0, len(rows), 200):
            batch = rows[i:i+200]
            self.client.table("bdc_holdings").insert(batch).execute()
            total += len(batch)
        return total

    def track_filing(self, adsh: str, cik: int, ticker: str, form_type: str,
                     period: str, filed: str, prevrpt: bool,
                     num_rows: int, soi_rows: int, source_file: str):
        """Record that we processed this filing."""
        self.client.table("sec_filing_tracker").upsert({
            "adsh": adsh,
            "cik": cik,
            "ticker": ticker,
            "form_type": form_type,
            "period": period,
            "filed": filed,
            "prevrpt": prevrpt,
            "num_rows_loaded": num_rows,
            "soi_rows_loaded": soi_rows,
            "source_file": source_file,
        }, on_conflict="adsh").execute()
        self._processed_adshs.add(adsh)


# ---------------------------------------------------------------------------
#  SEC Data Download
# ---------------------------------------------------------------------------

def download_zip(zip_name: str, cache_dir: Path) -> Path:
    """Download a SEC BDC ZIP file, caching locally."""
    local_path = cache_dir / zip_name
    if local_path.exists():
        log.info(f"Using cached: {zip_name}")
        return local_path

    url = f"{SEC_BDC_BASE_URL}/{zip_name}"
    log.info(f"Downloading: {url}")
    headers = {"User-Agent": SEC_USER_AGENT}
    resp = requests.get(url, headers=headers, timeout=120)
    resp.raise_for_status()

    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_bytes(resp.content)
    log.info(f"Downloaded {zip_name} ({len(resp.content) / 1024:.0f} KB)")
    return local_path


def extract_tsv(zip_path: Path, table_name: str) -> Optional[pd.DataFrame]:
    """Extract a TSV table from the BDC ZIP file."""
    with zipfile.ZipFile(zip_path) as zf:
        # Files are in a 'datasets/' subfolder or root
        candidates = [
            f"datasets/{table_name.lower()}.tsv",
            f"{table_name.lower()}.tsv",
            f"datasets/{table_name.upper()}.tsv",
            f"{table_name.upper()}.tsv",
        ]
        # Also check the SOI report which may be separate
        if table_name.upper() == "SOI":
            candidates.extend([
                f"datasets/soi_report.tsv",
                f"soi_report.tsv",
                f"datasets/soi.tsv",
                f"soi.tsv",
            ])

        for name in zf.namelist():
            if any(name.lower().endswith(c.split("/")[-1]) for c in candidates):
                log.debug(f"Found {table_name} as: {name}")
                with zf.open(name) as f:
                    try:
                        df = pd.read_csv(f, sep="\t", dtype=str, na_values=[""],
                                        keep_default_na=True, low_memory=False)
                        log.info(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
                        return df
                    except Exception as e:
                        log.warning(f"  Error reading {name}: {e}")
                        return None

        # If not found, list what's in the ZIP
        log.warning(f"  {table_name} not found in {zip_path.name}. Contents: {zf.namelist()[:20]}")
        return None


# ---------------------------------------------------------------------------
#  SUB Table Parsing
# ---------------------------------------------------------------------------

def parse_sub(df_sub: pd.DataFrame, cik_to_ticker: Dict[int, str],
              processed_adshs: Set[str]) -> pd.DataFrame:
    """
    Filter SUB table to relevant BDC filings.
    Returns DataFrame with columns: adsh, cik, ticker, name, form, period, fy, fp, filed, prevrpt
    """
    if df_sub is None or df_sub.empty:
        return pd.DataFrame()

    # Normalize column names to lowercase
    df_sub.columns = [c.lower().strip() for c in df_sub.columns]

    # Convert CIK to int
    df_sub["cik"] = pd.to_numeric(df_sub["cik"], errors="coerce").astype("Int64")

    # Filter to our known BDCs
    known_ciks = set(cik_to_ticker.keys())
    df_bdc = df_sub[df_sub["cik"].isin(known_ciks)].copy()

    if df_bdc.empty:
        log.warning("  No known BDC CIKs found in SUB table")
        return pd.DataFrame()

    # Filter to financial statement forms
    form_filter = df_bdc["form"].str.upper().isin([
        "10-K", "10-Q", "10-K/A", "10-Q/A", "10-KT", "10-QT"
    ])
    df_bdc = df_bdc[form_filter].copy()

    # Add ticker
    df_bdc["ticker"] = df_bdc["cik"].map(cik_to_ticker)

    # Parse prevrpt flag (1 = this filing was superseded by an amendment)
    df_bdc["prevrpt"] = df_bdc["prevrpt"].fillna("0").astype(str).str.strip() == "1"

    # Skip already-processed filings
    new_mask = ~df_bdc["adsh"].isin(processed_adshs)
    skipped = (~new_mask).sum()
    if skipped > 0:
        log.info(f"  Skipping {skipped} already-processed filings")
    df_bdc = df_bdc[new_mask]

    log.info(f"  SUB filtered: {len(df_bdc)} new BDC filings")
    return df_bdc


# ---------------------------------------------------------------------------
#  TAG Table Parsing (for resolving custom tags)
# ---------------------------------------------------------------------------

def build_custom_tag_map(df_tag: pd.DataFrame) -> Dict[str, str]:
    """
    Build a mapping from custom tag names to our field names
    using the tlabel (human-readable label) field.
    """
    if df_tag is None or df_tag.empty:
        return {}

    df_tag.columns = [c.lower().strip() for c in df_tag.columns]

    # Only custom tags
    custom = df_tag[df_tag["custom"].astype(str).str.strip() == "1"].copy()
    if custom.empty:
        return {}

    tag_map = {}
    for _, row in custom.iterrows():
        tlabel = str(row.get("tlabel", ""))
        tag_name = str(row.get("tag", ""))
        if not tlabel or not tag_name:
            continue
        for pattern, field_name in CUSTOM_TAG_PATTERNS.items():
            if re.search(pattern, tlabel):
                tag_map[tag_name] = field_name
                break

    log.info(f"  Resolved {len(tag_map)} custom tags to known fields")
    return tag_map


# ---------------------------------------------------------------------------
#  NUM Table Parsing → Fundamentals
# ---------------------------------------------------------------------------

def parse_num_for_filing(df_num_filing: pd.DataFrame, adsh: str, ticker: str,
                         period: str, filed: str, form_type: str, fp: str, fy: str,
                         custom_tag_map: Dict[str, str]) -> Optional[Dict]:
    """
    Extract fundamental metrics from NUM rows for a single filing.
    Returns a dict ready for bdc_quarterly_nav upsert, or None.
    """
    if df_num_filing.empty:
        return None

    # Build combined tag map (standard + custom)
    full_tag_map = {**NUM_TAG_MAP, **custom_tag_map}

    # Filter to relevant tags
    relevant_tags = set(full_tag_map.keys())
    mask = df_num_filing["tag"].isin(relevant_tags)
    df_rel = df_num_filing[mask].copy()

    if df_rel.empty:
        return None

    # Convert value to numeric
    df_rel["value"] = pd.to_numeric(df_rel["value"], errors="coerce")

    # Map tags to our field names
    df_rel["field"] = df_rel["tag"].map(full_tag_map)

    # For each target field, pick the best value:
    # 1. Prefer rows with dimn=0 (no dimensional segments - the "total" value)
    # 2. Prefer rows where ddate matches the period
    # 3. Among standard tags, prefer by FIELD_PRIORITY order
    # 4. Prefer USD unit of measure

    result = {}
    for field_name, tag_priority in FIELD_PRIORITY.items():
        candidates = df_rel[df_rel["field"] == field_name].copy()
        if candidates.empty:
            continue

        # Prefer unsegmented (dimn=0)
        dimn_zero = candidates[candidates["dimn"].astype(str).str.strip() == "0"]
        if not dimn_zero.empty:
            candidates = dimn_zero

        # Prefer USD unit
        usd = candidates[candidates["uom"].str.upper().str.contains("USD", na=False)]
        if not usd.empty:
            candidates = usd

        # Prefer the period-end date matching the filing period
        if period:
            period_match = candidates[candidates["ddate"].str.replace("-", "").str[:8] == period.replace("-", "")[:8]]
            if not period_match.empty:
                candidates = period_match

        # Take the first non-null value by tag priority
        for preferred_tag in tag_priority:
            tag_match = candidates[candidates["tag"] == preferred_tag]
            if not tag_match.empty:
                val = tag_match["value"].dropna().iloc[0] if not tag_match["value"].dropna().empty else None
                if val is not None:
                    result[field_name] = float(val)
                    break
        else:
            # Fallback: any match
            val = candidates["value"].dropna().iloc[0] if not candidates["value"].dropna().empty else None
            if val is not None:
                result[field_name] = float(val)

    if not result:
        return None

    # Parse report_date from period
    try:
        if len(period) == 8:
            report_date = f"{period[:4]}-{period[4:6]}-{period[6:8]}"
        else:
            report_date = period
    except Exception:
        return None

    # Parse fiscal_period
    fiscal_period = None
    if fy and fp:
        try:
            fiscal_period = f"{fy}-{fp}"
        except Exception:
            pass

    # Build the upsert row
    row = {
        "ticker": ticker,
        "report_date": report_date,
        "filing_date": f"{filed[:4]}-{filed[4:6]}-{filed[6:8]}" if len(str(filed)) == 8 else str(filed),
        "form_type": form_type,
        "accession_number": adsh,
        "adsh": adsh,
        "data_source": "sec_xbrl",
        "source": "sec_xbrl",
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    if fiscal_period:
        row["fiscal_period"] = fiscal_period

    # Add extracted values
    for field, value in result.items():
        row[field] = value

    # Compute debt_to_equity if we have debt and net assets
    if "total_debt" in result and "total_net_assets" in result and result["total_net_assets"]:
        row["debt_to_equity"] = round(result["total_debt"] / result["total_net_assets"], 4)

    # Compute asset_coverage_ratio if not directly available
    if "asset_coverage_ratio" not in result and "total_net_assets" in result and "total_debt" in result:
        total_assets = result["total_net_assets"] + result["total_debt"]
        if result["total_debt"] > 0:
            row["asset_coverage_ratio"] = round(total_assets / result["total_debt"], 4)

    return row


def parse_num(df_num: pd.DataFrame, df_sub_filtered: pd.DataFrame,
              custom_tag_map: Dict[str, str]) -> List[Dict]:
    """Parse NUM table for all filtered filings → fundamentals rows."""
    if df_num is None or df_num.empty or df_sub_filtered.empty:
        return []

    df_num.columns = [c.lower().strip() for c in df_num.columns]

    # Pre-filter NUM to only our filings' accession numbers
    our_adshs = set(df_sub_filtered["adsh"])
    df_num = df_num[df_num["adsh"].isin(our_adshs)].copy()

    if df_num.empty:
        return []

    results = []
    for _, sub_row in df_sub_filtered.iterrows():
        adsh = sub_row["adsh"]
        ticker = sub_row["ticker"]
        period = str(sub_row.get("period", ""))
        filed = str(sub_row.get("filed", ""))
        form_type = str(sub_row.get("form", ""))
        fp = str(sub_row.get("fp", ""))
        fy = str(sub_row.get("fy", ""))

        # Skip superseded filings
        if sub_row.get("prevrpt", False):
            log.debug(f"  Skipping superseded filing {adsh} for {ticker}")
            continue

        df_filing = df_num[df_num["adsh"] == adsh]
        row = parse_num_for_filing(df_filing, adsh, ticker, period, filed, form_type, fp, fy, custom_tag_map)
        if row:
            results.append(row)

    log.info(f"  NUM extracted {len(results)} fundamental rows")
    return results


# ---------------------------------------------------------------------------
#  SOI Table Parsing → Holdings
# ---------------------------------------------------------------------------

def normalize_soi_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    The SOI table has dynamic columns. Normalize them to consistent names.
    Column names may have spaces, mixed case, etc.
    """
    col_map = {}
    for col in df.columns:
        cl = col.lower().strip()
        if cl == "adsh":
            col_map[col] = "adsh"
        elif cl == "cik":
            col_map[col] = "cik"
        elif cl == "name":
            col_map[col] = "name"
        elif cl == "ddate":
            col_map[col] = "ddate"
        elif cl == "form":
            col_map[col] = "form"
        elif cl == "filed":
            col_map[col] = "filed"
        elif cl == "period":
            col_map[col] = "period"
        elif "industry" in cl and "sector" in cl:
            col_map[col] = "industry_sector"
        elif "identifier" in cl and "axis" in cl:
            col_map[col] = "issuer_name"
        elif "affiliation" in cl:
            col_map[col] = "affiliation"
        elif "investment type" in cl or ("type" in cl and "axis" in cl and "investment" in cl):
            col_map[col] = "investment_type"
        elif "interest rate" in cl and "basis" not in cl and "spread" not in cl:
            col_map[col] = "interest_rate"
        elif "basis" in cl and "spread" in cl:
            col_map[col] = "basis_spread"
        elif "maturity" in cl:
            col_map[col] = "maturity_date"
        elif "principal" in cl and "amount" in cl:
            col_map[col] = "principal_amount"
        elif "cost" in cl and "fair" not in cl:
            col_map[col] = "cost_basis"
        elif "fair" in cl and "value" in cl:
            col_map[col] = "fair_value"
        elif "net" in cl and "assets" in cl and "percentage" in cl:
            col_map[col] = "pct_net_assets"
        elif "inlineurl" in cl or "inline" in cl:
            col_map[col] = "inlineurl"
        elif "cstm" in cl:
            col_map[col] = "cstm"

    df = df.rename(columns=col_map)
    return df


def parse_date_flexible(val: str) -> Optional[str]:
    """Parse dates that might be mm/dd/yyyy, yyyymmdd, yyyy-mm-dd."""
    if not val or pd.isna(val):
        return None
    val = str(val).strip()
    for fmt in ["%m/%d/%Y", "%Y%m%d", "%Y-%m-%d"]:
        try:
            return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def detect_non_accrual(row: pd.DataFrame, raw_row: dict) -> bool:
    """
    Detect non-accrual status from SOI row data.
    BDCs typically indicate non-accrual via:
    - Interest rate of 0 on a debt instrument
    - Footnotes containing 'non-accrual' or 'non accrual'
    - Investment type containing 'non-accrual'
    """
    inv_type = str(raw_row.get("investment_type", "")).lower()
    if "non-accrual" in inv_type or "non accrual" in inv_type or "nonaccrual" in inv_type:
        return True

    issuer = str(raw_row.get("issuer_name", "")).lower()
    if "non-accrual" in issuer or "non accrual" in issuer:
        return True

    # Check for any column that might contain non-accrual info
    for key, val in raw_row.items():
        if pd.notna(val) and "non" in str(key).lower() and "accrual" in str(key).lower():
            return True
        if pd.notna(val) and isinstance(val, str) and ("non-accrual" in val.lower() or "nonaccrual" in val.lower()):
            return True

    return False


def detect_pik(row_dict: dict) -> bool:
    """Detect PIK (Payment In Kind) from SOI row data."""
    for key, val in row_dict.items():
        if pd.notna(val):
            val_str = str(val).lower()
            if "pik" in val_str or "payment in kind" in val_str or "payment-in-kind" in val_str:
                return True
    return False


def parse_soi(df_soi: pd.DataFrame, cik_to_ticker: Dict[int, str],
              df_sub_filtered: pd.DataFrame) -> List[Dict]:
    """Parse SOI table → holdings rows for Supabase."""
    if df_soi is None or df_soi.empty:
        log.info("  No SOI data found")
        return []

    # Normalize columns
    df_soi = normalize_soi_columns(df_soi)

    # Deduplicate columns — SOI tables often have multiple columns mapping to the same name
    # Keep the first non-null column for each duplicate
    if df_soi.columns.duplicated().any():
        # Group duplicate columns, keep first non-null value per row
        seen = {}
        cols_to_keep = []
        for i, col in enumerate(df_soi.columns):
            if col not in seen:
                seen[col] = i
                cols_to_keep.append(i)
            else:
                # Merge: fill NaN in original with values from duplicate
                orig_idx = seen[col]
                orig_col = df_soi.iloc[:, orig_idx]
                dup_col = df_soi.iloc[:, i]
                df_soi.iloc[:, orig_idx] = orig_col.fillna(dup_col)
        df_soi = df_soi.iloc[:, cols_to_keep]

    # Filter to our BDCs
    if "cik" in df_soi.columns:
        df_soi["cik"] = pd.to_numeric(df_soi["cik"], errors="coerce").astype("Int64")
        known_ciks = set(cik_to_ticker.keys())
        df_soi = df_soi[df_soi["cik"].isin(known_ciks)].copy()
    else:
        # Filter by adsh if CIK not in SOI
        our_adshs = set(df_sub_filtered["adsh"])
        df_soi = df_soi[df_soi["adsh"].isin(our_adshs)].copy()

    if df_soi.empty:
        log.info("  No SOI rows for our BDCs")
        return []

    # Build adsh→metadata lookup from SUB
    adsh_meta = {}
    for _, row in df_sub_filtered.iterrows():
        adsh_meta[row["adsh"]] = {
            "ticker": row["ticker"],
            "cik": int(row["cik"]),
            "form_type": row.get("form", ""),
            "filed": row.get("filed", ""),
            "prevrpt": row.get("prevrpt", False),
        }

    results = []
    skipped_no_meta = 0

    for _, soi_row in df_soi.iterrows():
        adsh = soi_row.get("adsh")
        if not adsh:
            continue

        # Get metadata
        if adsh in adsh_meta:
            meta = adsh_meta[adsh]
        elif "cik" in soi_row and pd.notna(soi_row["cik"]):
            cik_val = int(soi_row["cik"])
            ticker = cik_to_ticker.get(cik_val)
            if not ticker:
                skipped_no_meta += 1
                continue
            meta = {
                "ticker": ticker,
                "cik": cik_val,
                "form_type": str(soi_row.get("form", "")),
                "filed": str(soi_row.get("filed", "")),
                "prevrpt": False,
            }
        else:
            skipped_no_meta += 1
            continue

        # Skip superseded filings
        if meta.get("prevrpt"):
            continue

        # Parse dates
        report_date = parse_date_flexible(soi_row.get("ddate") or soi_row.get("period"))
        if not report_date:
            continue

        filing_date = parse_date_flexible(meta.get("filed"))

        # Parse numeric fields
        def to_float(val):
            # Handle duplicate columns returning a Series
            if isinstance(val, pd.Series):
                val = val.iloc[0]
            try:
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                s = str(val).replace(",", "").strip()
                if s == "" or s.lower() == "nan":
                    return None
                return float(s)
            except (ValueError, TypeError):
                return None

        # Helper to safely get scalar from soi_row (handles duplicate columns)
        def safe_get(row, key, default=""):
            val = row.get(key, default)
            if isinstance(val, pd.Series):
                val = val.iloc[0]
            return val

        fair_value = to_float(safe_get(soi_row, "fair_value"))
        cost_basis = to_float(safe_get(soi_row, "cost_basis"))

        # Skip rows with no fair value (empty/header rows)
        if fair_value is None and cost_basis is None:
            continue

        # Parse maturity date
        mat_date = None
        mat_raw = safe_get(soi_row, "maturity_date")
        if mat_raw is not None and pd.notna(mat_raw) and str(mat_raw).strip():
            mat_date = parse_date_flexible(str(mat_raw))

        # Build raw dict for non-accrual/PIK detection
        raw_dict = {}
        if hasattr(soi_row, 'items'):
            for k, v in soi_row.items():
                try:
                    if isinstance(v, pd.Series):
                        v = v.iloc[0]
                    if pd.notna(v):
                        raw_dict[k] = v
                except (ValueError, TypeError):
                    pass

        # Compute unrealized gain/loss
        unrealized = None
        if fair_value is not None and cost_basis is not None:
            unrealized = round(fair_value - cost_basis, 4)

        # Safe string extraction
        def safe_str(key, maxlen=512):
            v = safe_get(soi_row, key)
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            s = str(v).strip()
            return s[:maxlen] if s else None

        holding = {
            "ticker": meta["ticker"],
            "adsh": adsh,
            "cik": meta["cik"],
            "report_date": report_date,
            "filing_date": filing_date,
            "form_type": meta.get("form_type"),
            "issuer_name": safe_str("issuer_name"),
            "industry_sector": safe_str("industry_sector"),
            "investment_type": safe_str("investment_type"),
            "affiliation": safe_str("affiliation"),
            "interest_rate": to_float(safe_get(soi_row, "interest_rate")),
            "basis_spread": to_float(safe_get(soi_row, "basis_spread")),
            "maturity_date": mat_date,
            "principal_amount": to_float(safe_get(soi_row, "principal_amount")),
            "cost_basis": cost_basis,
            "fair_value": fair_value,
            "pct_net_assets": to_float(safe_get(soi_row, "pct_net_assets")),
            "unrealized_gain_loss": unrealized,
            "is_non_accrual": detect_non_accrual(soi_row, raw_dict),
            "is_pik": detect_pik(raw_dict),
        }

        results.append(holding)

    if skipped_no_meta:
        log.debug(f"  Skipped {skipped_no_meta} SOI rows (no ticker mapping)")

    log.info(f"  SOI extracted {len(results)} holdings")
    return results


# ---------------------------------------------------------------------------
#  EDGAR CompanyFacts API (Alternative/Supplemental)
# ---------------------------------------------------------------------------

def fetch_company_facts(cik: int) -> Optional[Dict]:
    """
    Fetch all XBRL facts for a company via the EDGAR CompanyFacts API.
    This is an alternative to the bulk ZIP files for fundamental data.
    
    URL: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json
    
    Returns the full JSON response or None on error.
    """
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik:010d}.json"
    headers = {"User-Agent": SEC_USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"  CompanyFacts API error for CIK {cik}: {e}")
        return None


def extract_facts_for_ticker(facts_json: Dict, ticker: str) -> List[Dict]:
    """
    Extract fundamental data from CompanyFacts JSON.
    Returns a list of dicts ready for bdc_quarterly_nav upsert.
    
    This provides an alternative path when bulk ZIP files are unavailable
    or to fill gaps for specific BDCs.
    """
    if not facts_json or "facts" not in facts_json:
        return []

    us_gaap = facts_json.get("facts", {}).get("us-gaap", {})
    if not us_gaap:
        return []

    # Collect facts by (tag, filing_end_date) → value
    # We want quarterly (10-Q) and annual (10-K) facts
    facts_by_period = {}  # {report_date: {field: value}}

    concept_map = {
        "NetAssetValuePerShare": "nav_per_share",
        "InvestmentIncomeNet": "net_investment_income",
        "NetInvestmentIncomeLoss": "net_investment_income",
        "InvestmentIncomeInvestmentInInterestAndDividendIncome": "total_investment_income",
        "LongTermDebt": "total_debt",
        "CommonStockSharesOutstanding": "shares_outstanding",
        "NetAssetsValue": "total_net_assets",
        "AssetsNet": "total_net_assets",
    }

    for concept_name, field_name in concept_map.items():
        concept = us_gaap.get(concept_name, {})
        units = concept.get("units", {})

        for unit_type, unit_facts in units.items():
            for fact in unit_facts:
                form = fact.get("form", "")
                if form not in ("10-K", "10-Q", "10-K/A", "10-Q/A"):
                    continue

                end_date = fact.get("end")
                val = fact.get("val")
                accn = fact.get("accn", "")
                filed = fact.get("filed", "")

                if not end_date or val is None:
                    continue

                if end_date not in facts_by_period:
                    facts_by_period[end_date] = {
                        "report_date": end_date,
                        "ticker": ticker,
                        "data_source": "sec_xbrl_api",
                        "source": "sec_xbrl",
                    }

                period_data = facts_by_period[end_date]

                # Only overwrite if we don't already have a value for this field
                # (earlier concepts in concept_map have priority)
                if field_name not in period_data:
                    period_data[field_name] = float(val)

                # Capture accession number and form type
                if "accession_number" not in period_data:
                    period_data["accession_number"] = accn
                    period_data["form_type"] = form
                if filed and "filing_date" not in period_data:
                    period_data["filing_date"] = filed

    return list(facts_by_period.values())


# ---------------------------------------------------------------------------
#  Main Pipeline
# ---------------------------------------------------------------------------

def process_zip(zip_path: Path, db: SupabaseClient, dry_run: bool = False) -> Dict[str, int]:
    """Process a single BDC ZIP file end-to-end."""
    zip_name = zip_path.name
    log.info(f"\n{'='*60}")
    log.info(f"Processing: {zip_name}")
    log.info(f"{'='*60}")

    stats = {"fundamentals": 0, "holdings": 0, "filings": 0}

    # 1. Extract tables
    df_sub = extract_tsv(zip_path, "sub")
    df_num = extract_tsv(zip_path, "num")
    df_tag = extract_tsv(zip_path, "tag")
    df_soi = extract_tsv(zip_path, "soi")

    if df_sub is None:
        log.warning(f"  No SUB table found in {zip_name}, skipping")
        return stats

    # 2. Parse SUB → filter to our BDCs
    df_sub_filtered = parse_sub(df_sub, db._cik_to_ticker, db._processed_adshs)
    if df_sub_filtered.empty:
        log.info(f"  No new filings in {zip_name}")
        return stats

    stats["filings"] = len(df_sub_filtered)

    # 3. Build custom tag map
    custom_tag_map = build_custom_tag_map(df_tag) if df_tag is not None else {}

    # 4. Parse NUM → fundamentals
    fundamentals = parse_num(df_num, df_sub_filtered, custom_tag_map)
    stats["fundamentals"] = len(fundamentals)

    # 5. Parse SOI → holdings
    holdings = []
    try:
        holdings = parse_soi(df_soi, db._cik_to_ticker, df_sub_filtered)
    except Exception as e:
        log.error(f"  SOI parsing failed: {e}")
        log.info("  Continuing with fundamentals only...")
    stats["holdings"] = len(holdings)

    # 6. Upsert to Supabase
    if not dry_run:
        if fundamentals:
            n = db.upsert_fundamentals(fundamentals)
            log.info(f"  Upserted {n} fundamental rows")

        if holdings:
            n = db.upsert_holdings(holdings)
            log.info(f"  Inserted {n} holding rows")

        # Track processed filings
        for _, sub_row in df_sub_filtered.iterrows():
            adsh = sub_row["adsh"]
            ticker = sub_row["ticker"]
            # Count how many fund/holding rows this filing contributed
            fund_count = sum(1 for f in fundamentals if f.get("accession_number") == adsh or f.get("adsh") == adsh)
            hold_count = sum(1 for h in holdings if h.get("adsh") == adsh)
            db.track_filing(
                adsh=adsh,
                cik=int(sub_row["cik"]),
                ticker=ticker,
                form_type=str(sub_row.get("form", "")),
                period=parse_date_flexible(str(sub_row.get("period", ""))),
                filed=parse_date_flexible(str(sub_row.get("filed", ""))),
                prevrpt=bool(sub_row.get("prevrpt", False)),
                num_rows=fund_count,
                soi_rows=hold_count,
                source_file=zip_name,
            )
    else:
        log.info(f"  DRY RUN: would upsert {len(fundamentals)} fundamentals, {len(holdings)} holdings")

    return stats


def verify_data(db: SupabaseClient, tickers: List[str]):
    """Verify loaded data for specific tickers."""
    print(f"\n{'='*60}")
    print("DATA VERIFICATION")
    print(f"{'='*60}\n")

    for ticker in tickers:
        print(f"\n--- {ticker} ---")

        # Fundamentals
        resp = db.client.table("bdc_quarterly_nav").select("*").eq("ticker", ticker).order("report_date", desc=True).limit(4).execute()
        print(f"\nQuarterly Fundamentals (last 4 periods):")
        if resp.data:
            for row in resp.data:
                print(f"  {row['report_date']} | NAV/sh: {row.get('nav_per_share', 'N/A'):>8} | "
                      f"NII: {row.get('net_investment_income', 'N/A'):>14} | "
                      f"TII: {row.get('total_investment_income', 'N/A'):>14} | "
                      f"Debt: {row.get('total_debt', 'N/A'):>14} | "
                      f"Source: {row.get('data_source', '?')}")
        else:
            print("  No data found")

        # Holdings summary
        resp = db.client.table("bdc_holdings").select("report_date, count").eq("ticker", ticker).execute()
        # Use SQL for aggregate
        resp2 = db.client.rpc("", {}).execute()  # Can't do aggregates easily via REST

        # Just count holdings per period
        resp = db.client.table("bdc_holdings").select("report_date, fair_value, investment_type, is_non_accrual").eq("ticker", ticker).order("report_date", desc=True).limit(500).execute()
        if resp.data:
            # Group by report_date
            from collections import Counter, defaultdict
            by_period = defaultdict(list)
            for row in resp.data:
                by_period[row["report_date"]].append(row)

            print(f"\nHoldings Summary:")
            for period in sorted(by_period.keys(), reverse=True)[:4]:
                rows = by_period[period]
                total_fv = sum(float(r.get("fair_value") or 0) for r in rows)
                non_accrual = sum(1 for r in rows if r.get("is_non_accrual"))
                types = Counter(r.get("investment_type", "Unknown") for r in rows)
                print(f"  {period} | {len(rows)} holdings | FV: ${total_fv/1e6:.0f}M | "
                      f"Non-accrual: {non_accrual} | Types: {dict(types.most_common(3))}")
        else:
            print("\n  No holdings data found")

        # Filing tracker
        resp = db.client.table("sec_filing_tracker").select("*").eq("ticker", ticker).order("period", desc=True).limit(4).execute()
        if resp.data:
            print(f"\nProcessed Filings:")
            for row in resp.data:
                print(f"  {row.get('period', 'N/A')} | {row.get('form_type', '?')} | "
                      f"NUM: {row.get('num_rows_loaded', 0)} | SOI: {row.get('soi_rows_loaded', 0)} | "
                      f"From: {row.get('source_file', '?')}")

    print(f"\n{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="SEC BDC XBRL Pipeline")
    parser.add_argument("--quarter", type=str, help="Process specific quarter (e.g., 2025q1)")
    parser.add_argument("--month", type=str, help="Process specific month (e.g., 2025_11)")
    parser.add_argument("--all", action="store_true", help="Process all available ZIP files")
    parser.add_argument("--latest", action="store_true", default=True, help="Process only the latest ZIP (default)")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, no Supabase writes")
    parser.add_argument("--verify", type=str, help="Verify data for tickers (comma-separated)")
    parser.add_argument("--api-mode", action="store_true",
                        help="Use CompanyFacts API instead of bulk ZIPs (fundamentals only)")
    parser.add_argument("--cache-dir", type=str, default="./sec_bdc_cache",
                        help="Directory to cache downloaded ZIP files")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize Supabase
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        log.error("Set SUPABASE_URL and SUPABASE_KEY environment variables")
        sys.exit(1)

    db = SupabaseClient(url, key)
    db.load_cik_map()
    db.load_processed_filings()

    # Verify mode
    if args.verify:
        tickers = [t.strip().upper() for t in args.verify.split(",")]
        verify_data(db, tickers)
        return

    # Determine which ZIPs to process
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if args.api_mode:
        # Use CompanyFacts API for each BDC
        log.info("Running in API mode (CompanyFacts API)")
        total_funds = 0
        for cik, ticker in db._cik_to_ticker.items():
            log.info(f"Fetching facts for {ticker} (CIK {cik})")
            facts = fetch_company_facts(cik)
            if facts:
                rows = extract_facts_for_ticker(facts, ticker)
                if rows and not args.dry_run:
                    n = db.upsert_fundamentals(rows)
                    total_funds += n
                    log.info(f"  {ticker}: upserted {n} fundamental rows")
                elif rows:
                    log.info(f"  {ticker}: DRY RUN would upsert {len(rows)} rows")
            # Be nice to SEC servers
            import time
            time.sleep(0.2)
        log.info(f"\nAPI mode complete: {total_funds} total fundamental rows upserted")
        return

    # Determine ZIP files to download
    if args.quarter:
        zips_to_process = [f"{args.quarter}_bdc.zip"]
    elif args.month:
        zips_to_process = [f"{args.month}_bdc.zip"]
    elif args.all:
        zips_to_process = AVAILABLE_ZIPS
    else:
        # Default: latest available
        zips_to_process = AVAILABLE_ZIPS[-3:]  # Last 3 months/quarters

    # Process each ZIP
    grand_totals = {"fundamentals": 0, "holdings": 0, "filings": 0}
    for zip_name in zips_to_process:
        try:
            zip_path = download_zip(zip_name, cache_dir)
            stats = process_zip(zip_path, db, dry_run=args.dry_run)
            for k, v in stats.items():
                grand_totals[k] += v
        except requests.HTTPError as e:
            log.warning(f"Download failed for {zip_name}: {e}")
        except Exception as e:
            log.error(f"Error processing {zip_name}: {e}", exc_info=True)

    # Summary
    log.info(f"\n{'='*60}")
    log.info("PIPELINE COMPLETE")
    log.info(f"{'='*60}")
    log.info(f"  Filings processed:  {grand_totals['filings']}")
    log.info(f"  Fundamentals loaded: {grand_totals['fundamentals']}")
    log.info(f"  Holdings loaded:     {grand_totals['holdings']}")


if __name__ == "__main__":
    main()
