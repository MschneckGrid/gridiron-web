"""
edgar_fund_info.py  (v3)
────────────────────────
Fixes vs v2:
  - ADVISER + SECURITY_EXCHANGE join via FUND_ID (not ACCESSION_NUMBER)
  - Correct column names: MANAGEMENT_FEE, NET_OPERATING_EXPENSES, FUND_TICKER_SYMBOL
  - N-2: fetches filing index to find readable HTML (not XBRL primary doc)
  - FISCAL_YEAR_END pulled from FUND_REPORTED_INFO REPORT_ENDING_PERIOD via SUBMISSION

Run:  python edgar_fund_info.py
Deps: pip install requests beautifulsoup4 lxml python-dateutil
"""

import os, io, re, csv, sys, time, zipfile, logging
from datetime import datetime
from typing import Optional

import requests
from bs4 import BeautifulSoup

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://nzinvxticgyjobkqxxhl.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
EDGAR_BASE   = "https://data.sec.gov"
HEADERS      = {"User-Agent": "Gridiron Partners research@gridironpartners.com"}
RATE_LIMIT   = 0.15
BATCH_SIZE   = 50

NCEN_ZIPS = [
    "https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2025q3_ncen.zip",
    "https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2025q2_ncen_0.zip",
    "https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2025q1_ncen_0.zip",
    "https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2024q4_ncen_0.zip",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

NUMERIC_FIELDS = {
    "total_assets","total_common_assets","total_debt","preferred_assets",
    "effective_leverage_pct","inception_price","inception_nav",
    "mgmt_fee_pct","other_expense_pct","interest_expense_pct","total_expense_pct"
}

def clean_row(row):
    out = {}
    for k, v in row.items():
        if v is None or str(v).strip() in ("","None","N/A","nan","NaN"):
            out[k] = None
        elif k in NUMERIC_FIELDS:
            try:
                f = float(v)
                out[k] = None if f != f else round(f, 6)
            except (TypeError, ValueError):
                out[k] = None
        elif isinstance(v, bool):
            out[k] = v
        elif isinstance(v, float):
            out[k] = None if v != v else v
        else:
            out[k] = v
    return out

def sb_get(path):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{path}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"})
    r.raise_for_status()
    return r.json()

def sb_upsert(table, rows):
    if not rows:
        return
    cleaned = [clean_row(r) for r in rows]
    # PostgREST requires ALL rows to have identical keys
    all_keys = set()
    for row in cleaned:
        all_keys.update(row.keys())
    normalized = [{k: row.get(k, None) for k in all_keys} for row in cleaned]
    hdrs = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
    r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=hdrs, json=normalized)
    if not r.ok:
        log.error(f"Batch upsert failed {r.status_code}: {r.text[:200]}")
        log.info("Retrying row-by-row...")
        for row in normalized:
            r2 = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=hdrs, json=[row])
            if not r2.ok:
                log.error(f"  [{row.get('ticker')}] failed: {r2.text[:120]}")

def load_cef_tickers():
    rows = sb_get("cef_tickers?select=ticker,cik&cik=not.is.null")
    result = {}
    for r in rows:
        if r.get("cik") and r.get("ticker"):
            cik = str(int(r["cik"])).zfill(10)
            result[r["ticker"].upper()] = cik
    log.info(f"Loaded {len(result)} tickers with CIKs")
    return result

def parse_tsv(content):
    if not content:
        return []
    return list(csv.DictReader(io.StringIO(content), delimiter="\t"))

def build_ncen_lookup(cik_set):
    """
    Correctly joins across N-CEN TSV tables.
    Key insight: ADVISER and SECURITY_EXCHANGE use FUND_ID, not ACCESSION_NUMBER.
    FUND_ID format: {ACCESSION_NUMBER}_{CIK}_{SERIES_ID}
    For single-series CEFs, we match FUND_ID.startswith(ACCESSION_NUMBER + "_")
    """
    cik_to_acc  = {}
    cik_to_date = {}
    cik_to_fye  = {}   # from SUBMISSION REPORT_ENDING_PERIOD
    acc_to_reg  = {}
    acc_to_fund = {}   # FUND_REPORTED_INFO row
    fund_id_to_adviser = {}
    fund_id_to_ticker  = {}

    for zip_url in NCEN_ZIPS:
        try:
            log.info(f"  Downloading {zip_url.split('/')[-1]}...")
            r = requests.get(zip_url, headers=HEADERS, stream=True, timeout=120)
            r.raise_for_status()
        except Exception as e:
            log.warning(f"  Skipping {zip_url.split('/')[-1]}: {e}")
            continue

        files = {}
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for name in z.namelist():
                base = name.split("/")[-1].upper()
                if base in ("SUBMISSION.TSV","REGISTRANT.TSV","FUND_REPORTED_INFO.TSV",
                            "ADVISER.TSV","SECURITY_EXCHANGE.TSV"):
                    files[base] = z.read(name).decode("utf-8", errors="replace")

        # SUBMISSION: CIK → most recent accession + fiscal year end
        for row in parse_tsv(files.get("SUBMISSION.TSV","")):
            cik = str(row.get("CIK","")).zfill(10)
            acc = row.get("ACCESSION_NUMBER","")
            dt  = row.get("FILING_DATE","")
            fye = row.get("REPORT_ENDING_PERIOD","")  # e.g. "31-MAY-2025"
            if cik and acc:
                if cik not in cik_to_date or dt > cik_to_date[cik]:
                    cik_to_date[cik] = dt
                    cik_to_acc[cik]  = acc
                    if fye:
                        cik_to_fye[cik] = fye

        # REGISTRANT: accession → name
        for row in parse_tsv(files.get("REGISTRANT.TSV","")):
            acc = row.get("ACCESSION_NUMBER","")
            if acc and acc not in acc_to_reg:
                acc_to_reg[acc] = row

        # FUND_REPORTED_INFO: accession → fund data (keep first per accession)
        for row in parse_tsv(files.get("FUND_REPORTED_INFO.TSV","")):
            acc = row.get("ACCESSION_NUMBER","")
            if acc and acc not in acc_to_fund:
                acc_to_fund[acc] = row

        # ADVISER: fund_id → adviser name (ADVISER_TYPE = 'Advisor', not sub-adviser)
        for row in parse_tsv(files.get("ADVISER.TSV","")):
            fid = row.get("FUND_ID","")
            atype = row.get("ADVISER_TYPE","").lower()
            name  = row.get("ADVISER_NAME","")
            if fid and name and "sub" not in atype and fid not in fund_id_to_adviser:
                fund_id_to_adviser[fid] = name

        # SECURITY_EXCHANGE: fund_id → ticker
        for row in parse_tsv(files.get("SECURITY_EXCHANGE.TSV","")):
            fid = row.get("FUND_ID","")
            sym = row.get("FUND_TICKER_SYMBOL","")
            if fid and sym and fid not in fund_id_to_ticker:
                fund_id_to_ticker[fid] = sym.upper()

    log.info(f"N-CEN: {len(cik_to_acc)} filers, {len(fund_id_to_adviser)} adviser records, {len(fund_id_to_ticker)} exchange records")

    # Build ACCESSION → FUND_ID prefix map for adviser/exchange lookup
    # FUND_ID = "{accession}_{cik}_{series_id}" — we match by accession prefix
    acc_prefix_to_fund_ids = {}
    for fid in set(fund_id_to_adviser) | set(fund_id_to_ticker):
        acc = fid.split("_")[0] if "_" in fid else ""
        if acc:
            acc_prefix_to_fund_ids.setdefault(acc, []).append(fid)

    def get_fund_id(acc):
        """Get the first FUND_ID for this accession number."""
        return (acc_prefix_to_fund_ids.get(acc) or [None])[0]

    def parse_fye(raw):
        """Convert '31-MAY-2025' → '05-31' (MM-DD)"""
        try:
            from dateutil import parser as dp
            d = dp.parse(raw)
            return f"{d.month:02d}-{d.day:02d}"
        except Exception:
            return None

    def to_pct(v):
        """MANAGEMENT_FEE is stored as a ratio 0-1, convert to percentage."""
        try:
            f = float(v)
            if f != f: return None
            # Values > 1 are already percentages; values <= 1 are ratios
            return round(f * 100, 4) if f <= 1 else round(f, 4)
        except (TypeError, ValueError):
            return None

    result = {}
    for cik in cik_set:
        acc = cik_to_acc.get(cik)
        if not acc:
            continue

        reg  = acc_to_reg.get(acc, {})
        fund = acc_to_fund.get(acc, {})
        fid  = get_fund_id(acc)

        adviser_name = fund_id_to_adviser.get(fid) if fid else None
        exchange_ticker = fund_id_to_ticker.get(fid) if fid else None

        # Fiscal year end from SUBMISSION REPORT_ENDING_PERIOD
        fye_raw = cik_to_fye.get(cik,"")
        fye = parse_fye(fye_raw) if fye_raw else None

        # Expense data from FUND_REPORTED_INFO
        mgmt_fee     = to_pct(fund.get("MANAGEMENT_FEE"))
        total_expense = to_pct(fund.get("NET_OPERATING_EXPENSES"))

        # Average net assets (monthly avg — best proxy for AUM in N-CEN)
        def to_dollars(v):
            try:
                f = float(v)
                return round(f, 2) if f and f == f else None
            except (TypeError, ValueError):
                return None

        monthly_avg_na = to_dollars(fund.get("MONTHLY_AVG_NET_ASSETS"))
        daily_avg_na   = to_dollars(fund.get("DAILY_AVG_NET_ASSETS"))
        total_common_assets = monthly_avg_na or daily_avg_na

        result[cik] = {
            "full_name":              reg.get("REGISTRANT_NAME") or None,
            "adviser_name":           adviser_name,
            "fiscal_year_end":        fye,
            "is_term_fund":           False,  # not reliably in these TSVs
            "exchange":               exchange_ticker,
            "total_common_assets":    total_common_assets,
            "mgmt_fee_pct":           mgmt_fee,
            "total_expense_pct":      total_expense,
            "as_of_date":             cik_to_date.get(cik) or None,
            "ncen_accession":         acc,
        }

    log.info(f"N-CEN matched {len(result)} of {len(cik_set)} our CEFs")
    return result


# ── N-2 fetching ──────────────────────────────────────────────────────────────
def edgar_get(url, retries=3):
    for attempt in range(retries):
        try:
            time.sleep(RATE_LIMIT)
            r = requests.get(url, headers=HEADERS, timeout=30)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                log.warning("Rate limited — sleeping 5s"); time.sleep(5)
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed: {e}")
    return None


def filing_has_readable_html(cik, acc_raw):
    acc = acc_raw.replace("-","")
    cik_int = str(int(cik))
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{acc_raw}-index.htm"
    r = edgar_get(index_url)
    if not (r and r.ok):
        return False
    soup = BeautifulSoup(r.content, "lxml")
    for row in soup.find_all("tr"):
        a = row.find("a", href=True)
        if not a or not a["href"].lower().endswith(".htm"):
            continue
        row_text = row.get_text(" ", strip=True).lower()
        if "ixbrl" in row_text or "inline xbrl" in row_text:
            continue
        if any(x in row_text for x in ("ex-99","ex-filing","consent","opinion","filing fee")):
            continue
        return True
    return False

def get_latest_n2(cik):
    r = edgar_get(f"{EDGAR_BASE}/submissions/CIK{cik}.json")
    if not r:
        return None
    data = r.json()
    filings = data.get("filings",{}).get("recent",{})
    all_n2 = []
    for form, dt, acc, doc in zip(filings.get("form",[]), filings.get("filingDate",[]),
                                   filings.get("accessionNumber",[]), filings.get("primaryDocument",[])):
        if form in ("N-2","N-2/A"):
            all_n2.append({"accession_number": acc, "primary_document": doc, "filing_date": dt})
    for f in data.get("filings",{}).get("files",[]):
        r2 = edgar_get(f"{EDGAR_BASE}/submissions/{f['name']}")
        if not r2: continue
        d2 = r2.json()
        for form, dt, acc, doc in zip(d2.get("form",[]), d2.get("filingDate",[]),
                                       d2.get("accessionNumber",[]), d2.get("primaryDocument",[])):
            if form in ("N-2","N-2/A"):
                all_n2.append({"accession_number": acc, "primary_document": doc, "filing_date": dt})
    for n2 in all_n2:
        if filing_has_readable_html(cik, n2["accession_number"]):
            return n2
    return all_n2[0] if all_n2 else None

def get_n2_html_doc(cik, acc_raw):
    """
    Fetch the EDGAR filing index to find the best readable HTML document.
    Returns the full filename (not URL) of the best non-XBRL HTM file.
    """
    acc = acc_raw.replace("-","")
    cik_int = str(int(cik))

    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{acc_raw}-index.htm"
    r = edgar_get(index_url)
    if not (r and r.ok):
        return None

    soup = BeautifulSoup(r.content, "lxml")

    candidates = []
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        # Find any link in this row
        a = row.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if not href.lower().endswith(".htm"):
            continue
        # Extract just the filename from whatever href format
        doc_name = href.split("/")[-1]
        if not doc_name:
            continue
        row_text = row.get_text(" ", strip=True).lower()
        # Skip XBRL inline viewer files
        if "ixbrl" in row_text or "inline xbrl" in row_text:
            continue
        if any(x in doc_name.lower() for x in ("xbrl",)):
            continue
        # Skip exhibits — not the prospectus
        if any(x in row_text for x in ("ex-99", "ex-filing", "consent", "opinion", "filing fee")):
            continue
        # Score by document type description
        score = 0
        if any(x in row_text for x in ("n-2", "n2", "prospectus", "registration statement")):
            score += 30
        if "complete" in row_text or "full" in row_text:
            score += 5
        # Prefer larger files (size is often in last cell)
        try:
            size = int(cells[-1].get_text(strip=True).replace(",",""))
            score += min(size // 100000, 20)  # up to +20 for large docs
        except (ValueError, AttributeError):
            pass
        candidates.append((score, doc_name))

    if candidates:
        candidates.sort(reverse=True)
        return candidates[0][1]

    return None

def fetch_n2_data(cik, n2_info):
    acc_raw = n2_info["accession_number"]
    acc     = acc_raw.replace("-","")
    cik_int = str(int(cik))

    # Try to find a better document than the XBRL primary
    doc = get_n2_html_doc(cik, acc_raw)
    if not doc:
        # Primary doc may be XBRL — log and skip; objective/managers will stay null
        log.warning(f"  No readable HTML found in filing index — skipping N-2 text extraction")
        return {"n2_accession": acc_raw}

    url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{doc}"
    r = edgar_get(url)
    if not r:
        return {"n2_accession": acc_raw}

    soup = BeautifulSoup(r.content, "lxml")
    text = soup.get_text(separator=" ", strip=True)

    # Sanity check — if text is tiny, the doc wasn't useful
    if len(text) < 500:
        log.warning(f"  N-2 doc too short ({len(text)} chars) — skipping text extraction")
        return {"n2_accession": acc_raw}

    result = {}
    obj = extract_investment_objective(soup, text)
    if obj: result["investment_objective"] = obj

    mgrs = extract_portfolio_managers(soup, text)
    if mgrs: result["portfolio_managers"] = mgrs

    result.update(extract_inception_info(text))
    result.update(extract_expenses(soup))
    result["n2_accession"] = acc_raw
    return result

def extract_investment_objective(soup, text):
    for tag in soup.find_all(["h1","h2","h3","h4","b","strong","p"]):
        t = tag.get_text(strip=True).lower()
        if "investment objective" in t and len(t) < 80:
            paras = []
            sib = tag.find_next_sibling()
            while sib and len(paras) < 3:
                if sib.name in ("p","div","td"):
                    p = sib.get_text(separator=" ", strip=True)
                    if len(p) > 30:
                        paras.append(p)
                        if len(p) > 100: break
                sib = sib.find_next_sibling()
            if paras: return " ".join(paras[:2])[:1000]
    for pat in [
        r"(?:INVESTMENT OBJECTIVE|Investment Objective)\s*[\n\r]+\s*([A-Z][^\n\r]{50,500})",
        r"investment objective[^\n]*[\n\r]+\s*(?:The\s+(?:Fund|Trust|Portfolio)[^\n\r]{50,500})",
    ]:
        m = re.search(pat, text, re.IGNORECASE|re.DOTALL)
        if m:
            raw = m.group(1) if m.lastindex else m.group(0)
            clean = re.sub(r"\s+"," ", raw).strip()[:1000]
            if len(clean) > 50: return clean
    return None

def extract_portfolio_managers(soup, text):
    managers = []
    pm_tag = next((t for t in soup.find_all(["h2","h3","h4","b","strong"])
                   if "portfolio manager" in t.get_text(strip=True).lower()), None)
    if pm_tag:
        tbl = pm_tag.find_next("table")
        if tbl:
            for row in tbl.find_all("tr")[1:]:
                cells = row.find_all(["td","th"])
                if cells:
                    name = cells[0].get_text(strip=True)
                    if name and len(name) > 2 and not name.lower().startswith("name"):
                        managers.append(name)
    if not managers:
        m = re.search(
            r"portfolio manager[s]?\s*[:\n\r]+\s*((?:[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+)+(?:,?\s+)?){1,6})",
            text, re.IGNORECASE)
        if m:
            names = [n.strip() for n in re.split(r"[,\n\r]+", m.group(1)) if n.strip()]
            managers = [n for n in names if len(n.split()) >= 2][:6]
    return ", ".join(managers) if managers else None

def extract_inception_info(text):
    result = {}
    for pat in [
        r"commenced (?:operations|trading) (?:on\s+)?([A-Z][a-z]+ \d{1,2},? \d{4})",
        r"inception date[:\s]+([A-Z][a-z]+ \d{1,2},? \d{4}|\d{1,2}/\d{1,2}/\d{4})",
        r"organized (?:on\s+)?([A-Z][a-z]+ \d{1,2},? \d{4})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                from dateutil import parser as dp
                result["inception_date"] = dp.parse(m.group(1)).date().isoformat()
                break
            except: pass
    m = re.search(r"(?:initial|offering) (?:public )?(?:offering )?price of \$([\d.]+)", text, re.IGNORECASE)
    if m:
        try: result["inception_price"] = float(m.group(1))
        except: pass
    m = re.search(r"initial NAV of \$([\d.]+)", text, re.IGNORECASE)
    if m:
        try: result["inception_nav"] = float(m.group(1))
        except: pass
    return result

def extract_expenses(soup):
    result = {}
    for tbl in soup.find_all("table"):
        tbl_text = tbl.get_text(separator="|", strip=True).lower()
        if "management fee" not in tbl_text and "management fees" not in tbl_text:
            continue
        rows_data = {}
        for row in tbl.find_all("tr"):
            cells = [c.get_text(strip=True) for c in row.find_all(["td","th"])]
            if len(cells) >= 2:
                label = cells[0].lower()
                pct = None
                for cell in cells[1:]:
                    m = re.search(r"([\d.]+)\s*%", cell)
                    if m:
                        try: pct = float(m.group(1)); break
                        except: pass
                if pct is not None:
                    if "management" in label: rows_data["mgmt_fee_pct"] = pct
                    elif "other expense" in label or "operating expense" in label: rows_data["other_expense_pct"] = pct
                    elif "interest" in label or "borrow" in label: rows_data["interest_expense_pct"] = pct
                    elif "total" in label and "annual" in label: rows_data["total_expense_pct"] = pct
        if rows_data:
            result.update(rows_data)
            break
    return result


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    if not SUPABASE_KEY:
        log.error("SUPABASE_KEY not set"); sys.exit(1)

    ticker_to_cik = load_cef_tickers()
    if not ticker_to_cik:
        log.error("No tickers with CIKs. Run populate_ciks.py first."); sys.exit(1)

    cik_set = set(ticker_to_cik.values())

    log.info("Phase 1: Building N-CEN lookup from 4 quarters...")
    ncen_data = build_ncen_lookup(cik_set)

    missing = cik_set - set(ncen_data.keys())
    if missing:
        tickers = [next(t for t,c in ticker_to_cik.items() if c==cik) for cik in list(missing)[:10]]
        log.warning(f"No N-CEN match for {len(missing)} funds: {tickers}")

    log.info("Phase 2: Fetching N-2 filings...")
    rows_to_upsert = []
    total = len(ticker_to_cik)

    for i, (ticker, cik) in enumerate(ticker_to_cik.items()):
        log.info(f"[{i+1}/{total}] {ticker}")
        row = {"ticker": ticker, "cik": cik, "last_updated": datetime.utcnow().isoformat()}

        if cik in ncen_data:
            row.update({k: v for k, v in ncen_data[cik].items() if v is not None})

        n2_info = get_latest_n2(cik)
        if n2_info:
            log.info(f"  N-2: {n2_info['accession_number']} ({n2_info['filing_date']})")
            n2_data = fetch_n2_data(cik, n2_info)
            row.update({k: v for k, v in n2_data.items() if v is not None})
        else:
            log.warning(f"  No N-2 for {ticker}")

        rows_to_upsert.append(row)

        if len(rows_to_upsert) >= BATCH_SIZE:
            log.info(f"  Upserting batch of {len(rows_to_upsert)}...")
            sb_upsert("cef_fund_info", rows_to_upsert)
            rows_to_upsert = []

    if rows_to_upsert:
        log.info(f"  Upserting final {len(rows_to_upsert)}...")
        sb_upsert("cef_fund_info", rows_to_upsert)

    log.info("✓ Done.")

if __name__ == "__main__":
    run()
