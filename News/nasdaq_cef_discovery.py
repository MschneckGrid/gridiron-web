"""
nasdaq_cef_discovery.py — Track 2 Phase 2
Gridiron Partners | CEF Universe Independence

Downloads NYSE/NASDAQ daily listing files, identifies CEF candidates via
tiered confidence logic, diffs against cef_tickers, writes universe events
and updates ticker status.

Usage:
    python nasdaq_cef_discovery.py --dry-run     # print diff, no DB writes
    python nasdaq_cef_discovery.py               # full run with DB writes

Schedule: Mon-Fri 09:00 EST via Windows Task Scheduler
"""

import argparse
import configparser
import logging
import os
import re
import sys
import time
from datetime import date, datetime
from io import StringIO
from pathlib import Path

import requests

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas requests")
    sys.exit(1)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
CFG_PATH = SCRIPT_DIR / "gridiron.cfg"

NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

HEADERS = {"User-Agent": "Gridiron Partners mike@gridironpartners.com"}

SUPABASE_URL = "https://nzinvxticgyjobkqxxhl.supabase.co"

# ---------------------------------------------------------------------------
# CEF IDENTIFICATION — tiered confidence logic
# ---------------------------------------------------------------------------

# Inclusion patterns: name must match at least one to be a CEF candidate.
# NOTE: "preferred" intentionally removed — it draws in bank/REIT preferred
# share series. Preferred-focused CEFs (e.g. Flaherty & Crumrine) still
# score via "income" + "fund", so no real candidates are lost.
INCLUDE_PATTERNS = [
    r"\bfund\b", r"\btrust\b", r"\bincome\b", r"\bterm\b",
    r"\bmunicipal\b", r"\bmuni\b", r"\bopportunities\b", r"\bstrategies\b",
    r"\bportfolio\b", r"\bselected\b", r"\bmanaged\b", r"\benhanced\b",
    r"\bdynamic\b", r"\bconvertible\b",
]

# Exclusion patterns: name must NOT match any of these to remain a candidate.
# Sources: original design doc patterns + dry-run false positive analysis 2026-03-08
EXCLUDE_PATTERNS = [
    # Original operating company markers
    r"\bbank\b", r"\bcorp\b", r"\binc\b", r"\bltd\b", r"\bgroup\b",
    r"\bholdings\b", r"\bservices\b", r"\brealty\b", r"\bproperties\b",
    r"\bretail\b", r"\btechnologies\b", r"\bsolutions\b", r"\bsystems\b",
    r"\btherapeutics\b", r"\bpharma\b", r"\bbiosciences\b", r"\bsoftware\b",
    r"\bnetworks\b", r"\bacquisition\b", r"\bspac\b", r"\bblank check\b",
    r"\bcorporation\b",         # catches DXR (Daxor Corp)

    # --- Non-CEF trust types (dry-run 2026-03-08 false positives) ---

    # Royalty trusts — oil/gas/mineral royalty vehicles (SJT, PBT, SBR,
    # MTR, MVO, VOC, CRT, PRT, NRT, PVL, MARPS)
    r"\broyalty\b",

    # Lodging / hotel REITs (CLDT = Chatham Lodging, RLJ = RLJ Lodging,
    # PEB = Pebblebrook Hotel)
    r"\blodging\b",
    r"\bhotel\b",
    r"\bhospitality\b",         # IHT = InnSuites Hospitality

    # Self-storage REITs (NSA = National Storage Affiliates)
    r"\bstorage\b",

    # Healthcare REITs (DHC = Diversified Healthcare, CHCT = Community Healthcare)
    r"\bhealthcare\b",

    # Explicit real-estate identifiers
    r"\breal estate\b",         # NXDT (NexPoint Diversified Real Estate Trust)
    r"\breit\b",                # any ticker whose listed name uses the acronym

    # Industrial / property trusts (LXP = LXP Industrial, CPT = Camden Property)
    r"\bindustrial trust\b",
    r"\bproperty trust\b",
    r"\bproperty\b",            # broad catch for property-named REITs

    # Mortgage REITs — narrow pattern to avoid blocking "mortgage opportunity fund"
    r"\bmortgage trust\b",      # PMT (PennyMac Mortgage Trust)
    r"\bmortgage reit\b",

    # Debt instruments — not equity
    r"\bnotes\b",               # TFSA (Terra Income Fund 6 LLC Notes)
    r"\bdebentures\b",          # KTH (Kinder Morgan debenture trust)
    r"\bdebenture\b",
    r"\bterm preferred\b",      # CCID-style term preferred stock series

    # Depositary shares / ADRs (NTRSO = Northern Trust depositary shares)
    r"\bdepositary\b",
    r"\bdepositary shares\b",
    r"\b(ads|adr)\b",           # American depositary shares/receipts

    # Commodity trusts — not investment companies
    r"\bgold\b",                # BAR (GraniteShares Gold Trust)
    r"\bsilver\b",
    r"\boil trust\b",           # narrow: avoids blocking "oil & gas fund"
    r"\bgas trust\b",

    # Volatility / structured ETNs that lack ETF flag in listing file
    r"\bvolatility\b",          # generic volatility products
    r"\bvix\b",                 # VXX, VXZ (VIX Futures ETNs — "term" in name triggers include)
    r"\bfutures etn\b",         # future-linked ETNs
    r"\betracs\b",              # UBS ETRACS ETN brand (PFFL — "income" in name triggers include)

    # Energy trusts (oil/gas royalty structures not using "royalty" in name)
    r"\benergy trust\b",        # VOC (VOC Energy Trust)

    # Mortgage REITs — broadened from phrase to single word to catch PMT
    # "PennyMac Mortgage Investment Trust" — "investment" sits between words
    r"\bmortgage\b",

    # NASDAQ listing file typo: "Royality" instead of "Royalty" (NRT)
    r"\broyality\b",

    # Venture capital funds — not registered investment companies
    r"\bventures\b",            # RVI = Robinhood Ventures Fund I

    # Bank/utility capital trusts and trust-preferred debt instruments
    r"\bcapital trust\b",       # DDT = Dillard's Capital Trust I
    r"\btrust preferred\b",     # SCE Trust preferred securities
    r"\btrust preference\b",    # SCE Trust preference securities (variant wording)

    # Petroleum/mineral grantor trusts (listing file name doesn't say "royalty")
    r"\bpetroleum trust\b",     # MARPS = Marine Petroleum Trust
]

# ---------------------------------------------------------------------------
# PREFERRED SHARE TICKER SUFFIX FILTER
# ---------------------------------------------------------------------------
# Bank and REIT preferred share series are listed as 5+ character tickers
# where the last character is a series suffix letter: P, O, L, M, N, or Z.
# Examples: HBANP (HBAN Series P), FITBO (FITB Series O), MBINM (MBIN Series M)
# VLYPP, BPYPN, ONBPO, MCHPP, BRKRP, NTRSO, CCNEP, UMBFO, WTFCN, PFFL, etc.
#
# Pattern: 4+ uppercase base letters + one suffix letter = preferred series.
# False-negative risk (legitimate CEFs wrongly caught): negligible — CEF tickers
# are almost universally 2-4 characters. Review if a real candidate is dropped.
PREF_SUFFIX_RE = re.compile(r'^[A-Z]{4,}[POLMNZ]$')

# ---------------------------------------------------------------------------
# DOLLAR-SIGN TICKER FILTER
# ---------------------------------------------------------------------------
# NYSE uses '$' in ticker symbols to denote preferred share series.
# Examples: GDV$H (Gabelli preferred H), PMT$A, SCE$G, C$N, WFC$L, HPE$C.
# These are always preferred/debt instruments issued BY funds or companies,
# never CEF common shares. Filter any ticker containing '$' before scoring.
DOLLAR_TICKER_RE = re.compile(r'\$')

# ---------------------------------------------------------------------------
# KNOWN NON-CEF TICKER EXCLUSION
# ---------------------------------------------------------------------------
# Tickers that pass all pattern filters but are definitively not CEFs.
# Used sparingly — prefer pattern-based rules where possible.
KNOWN_EXCLUDED_TICKERS = {
    "MSB",   # Mesabi Trust — Minnesota iron ore grantor trust, not an investment company
}

# ---------------------------------------------------------------------------
# KNOWN BDC TICKER EXCLUSION
# ---------------------------------------------------------------------------
# BDCs are tracked in the Gridiron BDC signal engine (separate universe).
# Exclude from CEF universe to prevent cross-contamination.
# Update this set as the BDC universe expands.
KNOWN_BDC_TICKERS = {
    # Large / well-known BDCs
    "ARCC", "BXSL", "PSEC", "GLAD", "OCSL", "MFIC", "FDUS", "RAND",
    "OFS",  "MRCC", "MSDL", "BCIC", "GBDC", "TPVG", "CSWC", "MAIN",
    "HTGC", "OBDC", "CGBD", "SLRC", "TRIN", "PFLT", "PNNT", "GSBD",
    "BCSF", "FSCO", "FCRD", "FSSL", "NEWT", "GAIN", "HRZN", "SCM",
    "GECC", "KCAP", "ORCC", "BBDC", "CGCL", "TPVG", "SUNS", "LRFC",
    "TCPC", "OXSQ", "CCAP", "KCAP", "NMFC", "NXDT", "TICC", "TCRD",
}

# ---------------------------------------------------------------------------
# KNOWN OTC-TRADED TICKERS (not in NYSE/NASDAQ listing files -- expected)
# ---------------------------------------------------------------------------
OTC_TICKERS = {"BXSY", "FXBY"}

# ---------------------------------------------------------------------------
# KNOWN OTC-TRADED TICKERS (not in NYSE/NASDAQ listing files — expected)
# ---------------------------------------------------------------------------
OTC_TICKERS = {"BXSY", "FXBY"}
# Compile pattern lists
INCLUDE_RE = [re.compile(p, re.IGNORECASE) for p in INCLUDE_PATTERNS]
EXCLUDE_RE = [re.compile(p, re.IGNORECASE) for p in EXCLUDE_PATTERNS]


# ---------------------------------------------------------------------------
# CONFIG / LOGGING
# ---------------------------------------------------------------------------

def load_config() -> dict:
    """Load Supabase credentials from gridiron.cfg."""
    cfg = configparser.ConfigParser()
    if not CFG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CFG_PATH}")
    cfg.read(CFG_PATH)
    return {
        "supabase_url": cfg.get("supabase", "url", fallback=SUPABASE_URL),
        "supabase_key": cfg.get("supabase", "service_role_key"),
    }


def setup_logging(dry_run: bool) -> logging.Logger:
    log_dir = SCRIPT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"nasdaq_cef_discovery_{date.today():%Y%m%d}.log"

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    # File handler: UTF-8 so special chars are preserved
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)

    # Console handler: replace unrepresentable chars for Windows terminals
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.stream = open(
        sys.stdout.fileno(), mode="w",
        encoding=sys.stdout.encoding or "utf-8",
        errors="replace", closefd=False,
    )

    logging.basicConfig(level=logging.INFO, handlers=[fh, sh])
    logger = logging.getLogger("nasdaq_cef_discovery")
    if dry_run:
        logger.info("=== DRY-RUN MODE - no database writes ===")
    return logger


# ---------------------------------------------------------------------------
# DOWNLOAD LISTING FILES
# ---------------------------------------------------------------------------

def download_listing_file(url: str, source_name: str, logger: logging.Logger) -> pd.DataFrame:
    """Download a NASDAQ listing file and return as DataFrame."""
    logger.info(f"Downloading {source_name} from {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Failed to download {source_name}: {e}")
        return pd.DataFrame()

    lines = resp.text.strip().splitlines()
    data_lines = [l for l in lines if not l.startswith("File Creation Time")]
    content = "\n".join(data_lines)

    df = pd.read_csv(StringIO(content), sep="|", dtype=str).fillna("")
    logger.info(f"  Downloaded {len(df)} rows from {source_name}")
    return df


def parse_nasdaq_listed(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize nasdaqlisted.txt to standard schema."""
    if df.empty:
        return df
    out = pd.DataFrame()
    out["ticker"]    = df["Symbol"].str.strip()
    out["fund_name"] = df["Security Name"].str.strip()
    out["exchange"]  = "NASDAQ"
    out["is_etf"]    = df.get("ETF", "").str.strip().str.upper() == "Y"
    out["source"]    = "nasdaqlisted"
    return out[out["ticker"].str.len() > 0]


def parse_other_listed(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize otherlisted.txt to standard schema."""
    if df.empty:
        return df
    out = pd.DataFrame()
    out["ticker"]    = df["ACT Symbol"].str.strip()
    out["fund_name"] = df["Security Name"].str.strip()
    out["exchange"]  = df["Exchange"].str.strip().map({
        "A": "NYSE American", "N": "NYSE", "P": "NYSE Arca",
        "Z": "BATS", "V": "IEX",
    }).fillna(df.get("Exchange", ""))
    out["is_etf"]    = df.get("ETF", "").str.strip().str.upper() == "Y"
    out["source"]    = "otherlisted"
    return out[out["ticker"].str.len() > 0]


# ---------------------------------------------------------------------------
# CEF CANDIDATE SCORING
# ---------------------------------------------------------------------------

def score_cef_candidate(ticker: str, fund_name: str, is_etf: bool) -> dict:
    """
    Return confidence tier for a listing file entry.

    Rejection hierarchy (checked in order):
        1. ETF flag set in listing file
        2. Dollar-sign in ticker (NYSE preferred series notation: GDV$H, PMT$A)
        3. Preferred share ticker suffix (HBANP, FITBO, MBINM style)
        4. Known non-CEF ticker (KNOWN_EXCLUDED_TICKERS)
        5. Known BDC ticker
        6. Name exclusion pattern
        7. Insufficient name inclusion matches

    Tiers returned:
        HIGH     — 2+ inclusion pattern matches, passed all rejections
        MEDIUM   — 1 inclusion pattern match, passed all rejections
        REJECTED — any rejection criterion matched
    """
    # 1. ETF flag
    if is_etf:
        return {"tier": "REJECTED", "reason": "ETF flag set"}

    # 2. Dollar-sign ticker → NYSE preferred share series (GDV$H, PMT$A, SCE$G, C$N, etc.)
    if DOLLAR_TICKER_RE.search(ticker):
        return {"tier": "REJECTED", "reason": "NYSE preferred series ($ in ticker)"}

    # 3. Preferred share ticker suffix filter
    #    Tickers of 5+ chars ending in P/O/L/M/N/Z are preferred share series,
    #    not CEF common shares (e.g. HBANP, FITBO, MBINM, BPYPN, MCHPP).
    if PREF_SUFFIX_RE.match(ticker):
        return {"tier": "REJECTED", "reason": f"Preferred share ticker suffix ({ticker[-1]})"}

    # 4. Known non-CEF tickers (edge cases that pass all pattern filters)
    if ticker in KNOWN_EXCLUDED_TICKERS:
        return {"tier": "REJECTED", "reason": "Known non-CEF (excluded ticker list)"}

    # 5. Known BDC — tracked in separate Gridiron BDC engine
    if ticker in KNOWN_BDC_TICKERS:
        return {"tier": "REJECTED", "reason": "Known BDC ticker (tracked separately)"}

    name_lower = fund_name.lower()

    # 6. Name exclusion patterns
    for pat in EXCLUDE_RE:
        if pat.search(name_lower):
            return {"tier": "REJECTED", "reason": f"Exclusion pattern: {pat.pattern}"}

    # 7. Name inclusion scoring
    matches = [pat.pattern for pat in INCLUDE_RE if pat.search(name_lower)]
    if len(matches) >= 2:
        return {"tier": "HIGH", "reason": f"Name patterns: {matches[:3]}"}
    elif len(matches) == 1:
        return {"tier": "MEDIUM", "reason": f"Name pattern: {matches[0]}"}

    return {"tier": "REJECTED", "reason": "No CEF name patterns found"}


# ---------------------------------------------------------------------------
# EDGAR N-2 LOOKUP (for new candidates — resolves CIK simultaneously)
# ---------------------------------------------------------------------------

EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"
EDGAR_HEADERS    = {"User-Agent": "Gridiron Partners mike@gridironpartners.com"}


def lookup_edgar_n2(ticker: str, logger: logging.Logger) -> dict:
    """
    Search EDGAR for N-2 filings matching ticker.
    Returns {"found": bool, "cik": int|None, "entity_name": str}.
    Rate-limited to 0.5 req/sec per SEC guidelines.
    """
    try:
        resp = requests.get(
            EDGAR_SEARCH_URL,
            params={"q": f'"{ticker}"', "forms": "N-2"},
            headers=EDGAR_HEADERS,
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        if hits:
            src = hits[0].get("_source", {})
            cik_str = src.get("entity_id", "")
            cik = int(cik_str) if cik_str.isdigit() else None
            return {
                "found": True,
                "cik": cik,
                "entity_name": src.get("entity_name", ""),
            }
    except Exception as e:
        logger.warning(f"EDGAR N-2 lookup failed for {ticker}: {e}")
    return {"found": False, "cik": None, "entity_name": ""}


# ---------------------------------------------------------------------------
# SUPABASE REST HELPERS
# ---------------------------------------------------------------------------

def sf_get(endpoint: str, params: dict, key: str) -> list:
    """GET from Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def sf_upsert(endpoint: str, rows: list, key: str, on_conflict: str = "ticker") -> None:
    """Upsert rows to Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    resp = requests.post(url, headers=headers, json=rows, timeout=30)
    resp.raise_for_status()


def sf_post(endpoint: str, rows: list, key: str) -> None:
    """INSERT rows to Supabase REST API (no upsert)."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    resp = requests.post(url, headers=headers, json=rows, timeout=30)
    resp.raise_for_status()


# ---------------------------------------------------------------------------
# LOAD CURRENT UNIVERSE FROM DB
# ---------------------------------------------------------------------------

def load_current_universe(key: str, logger: logging.Logger) -> pd.DataFrame:
    """Pull all cef_tickers rows into a DataFrame."""
    logger.info("Loading current universe from cef_tickers...")
    rows = sf_get(
        "cef_tickers",
        {"select": "ticker,fund_name,status,cik,last_confirmed_date,listing_source,exchange,n2_confirmed"},
        key,
    )
    df = pd.DataFrame(rows)
    logger.info(f"  Loaded {len(df)} rows ({(df['status']=='active').sum()} active)")
    return df


# ---------------------------------------------------------------------------
# DIFF LOGIC
# ---------------------------------------------------------------------------

def classify_diff(
    listing_tickers: set,
    current_df: pd.DataFrame,
    listing_df: pd.DataFrame,
    run_date: date,
    logger: logging.Logger,
) -> dict:
    """
    Diff listing file universe against DB universe.
    Returns categorized events dict.
    """
    db_active   = set(current_df[current_df["status"] == "active"]["ticker"])
    db_delisted = set(current_df[current_df["status"] == "delisted"]["ticker"])
    db_all      = set(current_df["ticker"])

    listing_lookup = listing_df.set_index("ticker").to_dict("index")

    events = {
        "confirmed_active":     [],
        "new_candidates":       [],
        "returned_from_halt":   [],
        "missing_from_listing": [],
        "name_changes":         [],
    }

    # 1. Listing file -> DB comparison
    for ticker in listing_tickers:
        info = listing_lookup.get(ticker, {})
        fund_name = info.get("fund_name", "")
        source    = info.get("source", "")
        exchange  = info.get("exchange", "")

        if ticker in db_active:
            events["confirmed_active"].append({
                "ticker": ticker, "source": source, "exchange": exchange,
            })
            # Check name change (cosmetic — listing file often has fuller names)
            db_name = current_df.loc[current_df["ticker"] == ticker, "fund_name"].values
            if db_name.size and db_name[0] and db_name[0] != fund_name:
                events["name_changes"].append({
                    "ticker": ticker, "old_name": db_name[0], "new_name": fund_name,
                    "source": source,
                })

        elif ticker in db_delisted:
            events["returned_from_halt"].append({
                "ticker": ticker, "fund_name": fund_name,
                "source": source, "exchange": exchange,
            })

        elif ticker not in db_all:
            # Score as CEF candidate
            is_etf = info.get("is_etf", False)
            score  = score_cef_candidate(ticker, fund_name, is_etf)
            if score["tier"] in ("HIGH", "MEDIUM"):
                events["new_candidates"].append({
                    "ticker": ticker, "fund_name": fund_name,
                    "source": source, "exchange": exchange,
                    "confidence_tier": score["tier"],
                    "reason": score["reason"],
                })

    # 2. DB active -> listing comparison (detect potential delistings)
    for ticker in db_active:
        if ticker not in listing_tickers:
            if ticker in OTC_TICKERS:
                continue   # OTC-traded — never in NASDAQ/NYSE files, not a delist
            events["missing_from_listing"].append({"ticker": ticker})

    return events


# ---------------------------------------------------------------------------
# PRINT DRY-RUN REPORT
# ---------------------------------------------------------------------------

def print_dry_run_report(events: dict, logger: logging.Logger) -> None:
    logger.info("\n" + "=" * 70)
    logger.info("DRY-RUN DIFF REPORT")
    logger.info("=" * 70)

    logger.info(f"\n[OK] CONFIRMED ACTIVE: {len(events['confirmed_active'])} tickers")

    nc = events["new_candidates"]
    logger.info(f"\n[NEW] CEF CANDIDATES: {len(nc)}")
    for c in sorted(nc, key=lambda x: x["confidence_tier"]):
        logger.info(f"  [{c['confidence_tier']:6}] {c['ticker']:8} | {c['fund_name'][:60]}")
        logger.info(f"            Reason: {c['reason']} | Source: {c['source']}")

    rf = events["returned_from_halt"]
    logger.info(f"\n[<<] RETURNED FROM DELISTED: {len(rf)}")
    for r in rf:
        logger.info(f"  {r['ticker']:8} | {r['fund_name']}")

    ml = events["missing_from_listing"]
    logger.info(f"\n[!!] MISSING FROM LISTING FILES (potential delistings): {len(ml)}")
    for m in ml:
        logger.info(f"  {m['ticker']}")

    nc_names = events["name_changes"]
    logger.info(f"\n[~~] NAME CHANGES: {len(nc_names)}")
    for n in nc_names:
        logger.info(f"  {n['ticker']:8} | {n['old_name']!r} -> {n['new_name']!r}")

    logger.info("\n" + "=" * 70)
    logger.info("Review new_candidates for false positives.")
    logger.info("Review missing_from_listing - may be halted, not delisted.")
    logger.info("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# DB WRITE LAYER
# ---------------------------------------------------------------------------

def write_to_db(events: dict, current_df: pd.DataFrame, key: str,
                run_date: date, logger: logging.Logger) -> None:
    """Write all diff events to cef_tickers and cef_universe_log.

    CRITICAL — PostgREST upsert safety:
    Every batch sent to sf_upsert must have IDENTICAL keys across all rows.
    Mixed key sets cause PostgREST to null-inject missing fields into existing
    non-null DB values. We maintain three strictly separate update groups:
      - confirmed_updates : {ticker, last_confirmed_date, listing_source, exchange}
      - name_updates      : {ticker, fund_name, last_confirmed_date, listing_source}
      - status_updates    : {ticker, status, delisted_date, last_confirmed_date,
                             listing_source, exchange}
    Each group is upserted independently so batches are always uniform.
    """
    today_str = run_date.isoformat()
    log_rows = []

    # Group 1: confirmed active — {ticker, last_confirmed_date, listing_source, exchange}
    confirmed_updates = []
    for ev in events["confirmed_active"]:
        confirmed_updates.append({
            "ticker":              ev["ticker"],
            "last_confirmed_date": today_str,
            "listing_source":      "nasdaq_file",
            "exchange":            ev["exchange"],
        })

    # Group 2: name changes — {ticker, fund_name, last_confirmed_date, listing_source}
    name_updates = []
    for ev in events["name_changes"]:
        name_updates.append({
            "ticker":              ev["ticker"],
            "fund_name":           ev["new_name"],
            "last_confirmed_date": today_str,
            "listing_source":      "nasdaq_file",
        })
        log_rows.append({
            "event_date": today_str,
            "run_date":   today_str,
            "event_type": "name_change",
            "ticker":     ev["ticker"],
            "source":     ev["source"],
            "detail":     f"Name changed: {ev['old_name']!r} -> {ev['new_name']!r}",
        })

    # Group 3: returned from delisted — {ticker, status, delisted_date,
    #           last_confirmed_date, listing_source, exchange}
    status_updates = []
    for ev in events["returned_from_halt"]:
        status_updates.append({
            "ticker":              ev["ticker"],
            "status":              "active",
            "delisted_date":       None,
            "last_confirmed_date": today_str,
            "listing_source":      "nasdaq_file",
            "exchange":            ev["exchange"],
        })
        log_rows.append({
            "event_date": today_str,
            "run_date":   today_str,
            "event_type": "returned_from_halt",
            "ticker":     ev["ticker"],
            "source":     ev["source"],
            "detail":     f"Ticker re-appeared in listing file: {ev['fund_name']}",
        })

    # 4. Missing from listing — log for investigation (do NOT auto-delist)
    #    Only escalate after 5+ consecutive missing days (manual review trigger)
    for ev in events["missing_from_listing"]:
        log_rows.append({
            "event_date": today_str,
            "run_date": today_str,
            "event_type": "missing_from_listing",
            "ticker": ev["ticker"],
            "source": "nasdaqlisted+otherlisted",
            "detail": "Not found in either listing file - investigate before delisting",
        })

    # 5. New candidates — EDGAR N-2 lookup, then add as status='candidate'
    #    Requires manual promotion to 'active'. Humans stay in the loop.
    new_cef_rows = []
    for ev in events["new_candidates"]:
        ticker = ev["ticker"]
        logger.info(f"  EDGAR N-2 lookup: {ticker}...")
        n2 = lookup_edgar_n2(ticker, logger)
        time.sleep(0.5)  # SEC rate limit courtesy

        confirmed = n2["found"]
        cik       = n2["cik"]

        new_cef_rows.append({
            "ticker":              ticker,
            "fund_name":           ev["fund_name"],
            "asset_class":         "CEF",
            "status":              "candidate",  # manual promotion required
            "listing_source":      "nasdaq_file",
            "exchange":            ev["exchange"],
            "n2_confirmed":        confirmed,
            "cik":                 cik,
            "first_seen_date":     today_str,
            "last_confirmed_date": today_str,
        })
        log_rows.append({
            "event_date": today_str,
            "run_date":   today_str,
            "event_type": "new_listing",
            "ticker":     ticker,
            "source":     ev["source"],
            "detail": (
                f"New CEF candidate [{ev['confidence_tier']}] "
                f"| N-2: {'confirmed CIK=' + str(cik) if confirmed else 'not found'} "
                f"| {ev['fund_name']}"
            ),
        })

    # --- Execute DB writes ---
    # Each group is upserted independently to guarantee uniform key sets per batch.

    def upsert_in_batches(rows: list, label: str) -> None:
        if not rows:
            return
        logger.info(f"Upserting {len(rows)} {label}...")
        for i in range(0, len(rows), 100):
            sf_upsert("cef_tickers", rows[i:i+100], key, on_conflict="ticker")
        logger.info(f"  [OK] {label} done")

    upsert_in_batches(confirmed_updates, "confirmed-active updates")
    upsert_in_batches(name_updates,      "name-change updates")
    upsert_in_batches(status_updates,    "returned-from-halt updates")

    if new_cef_rows:
        logger.info(f"Inserting {len(new_cef_rows)} new CEF candidate rows...")
        sf_upsert("cef_tickers", new_cef_rows, key, on_conflict="ticker")
        logger.info("  [OK] new candidates inserted")

    if log_rows:
        logger.info(f"Writing {len(log_rows)} universe log events...")
        for i in range(0, len(log_rows), 100):
            sf_post("cef_universe_log", log_rows[i:i+100], key)
        logger.info("  [OK] log events written")

    logger.info(f"DB write complete: {len(confirmed_updates)} confirmed, "
                f"{len(name_updates)} name updates, {len(status_updates)} status updates, "
                f"{len(new_cef_rows)} new candidates, {len(log_rows)} log events")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Gridiron CEF Universe Discovery")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print diff to console only - no DB writes")
    args = parser.parse_args()

    logger = setup_logging(args.dry_run)
    run_date = date.today()
    logger.info(f"nasdaq_cef_discovery.py | run_date={run_date} | dry_run={args.dry_run}")

    key = None
    if not args.dry_run:
        try:
            cfg = load_config()
            key = cfg["supabase_key"]
        except Exception as e:
            logger.error(f"Config load failed: {e}")
            sys.exit(1)

    # 1. Download listing files
    df_nasdaq = download_listing_file(NASDAQ_LISTED_URL, "nasdaqlisted", logger)
    df_other  = download_listing_file(OTHER_LISTED_URL,  "otherlisted",  logger)

    if df_nasdaq.empty and df_other.empty:
        logger.error("Both listing files failed to download - aborting")
        sys.exit(1)

    # 2. Normalize and combine
    norm_nasdaq = parse_nasdaq_listed(df_nasdaq) if not df_nasdaq.empty else pd.DataFrame()
    norm_other  = parse_other_listed(df_other)   if not df_other.empty  else pd.DataFrame()
    listing_df  = pd.concat([norm_nasdaq, norm_other], ignore_index=True)

    # Drop tickers with dots (e.g. BRK.B) — not standard CEF format
    listing_df = listing_df[~listing_df["ticker"].str.contains(r"\.", regex=True)]
    listing_df = listing_df.drop_duplicates(subset="ticker")
    listing_tickers = set(listing_df["ticker"])
    logger.info(f"Combined listing file: {len(listing_tickers)} unique tickers")

    # 3. Load current DB universe (needed even in dry-run for diffing)
    try:
        cfg = load_config()
        key = key or cfg.get("supabase_key") or cfg.get("anon_key")
        current_df = load_current_universe(key, logger)
    except Exception as e:
        logger.warning(f"Could not load DB for diff ({e}). Using empty baseline.")
        current_df = pd.DataFrame(columns=[
            "ticker", "fund_name", "status", "cik",
            "last_confirmed_date", "listing_source", "exchange", "n2_confirmed",
        ])

    # 4. Diff
    logger.info("Running diff...")
    events = classify_diff(listing_tickers, current_df, listing_df, run_date, logger)

    # 5. Report / Write
    print_dry_run_report(events, logger)
    if not args.dry_run:
        write_to_db(events, current_df, key, run_date, logger)

    logger.info("Done.")


if __name__ == "__main__":
    main()
