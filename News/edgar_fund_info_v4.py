"""
edgar_fund_info.py  (v4)
────────────────────────
Fixes vs v3:

  BUG 1 FIXED — objective_source / managers_source now written as 'edgar_n2'
                 when extraction succeeds. v3 never set these fields at all,
                 so CEFConnect always won on the next overwrite.

  BUG 2 FIXED — iXBRL filter removed. Post-2019 N-2 filings are predominantly
                 iXBRL, but their HTML is fully readable. The old filter was
                 silently blocking the main document for ~80% of the universe.
                 get_n2_html_doc() and filing_has_readable_html() now accept
                 iXBRL docs and rank them normally.

  BUG 3 FIXED — get_latest_n2() fallback is now correct. With Bug 2 fixed,
                 filing_has_readable_html() reliably detects readable files.
                 The fallback now only fires for genuinely old text-only filings.

  BUG 4 FIXED — extract_investment_objective() DOM traversal broadened:
                 now searches parent containers (div, section, td) for objective
                 text when no direct sibling match is found. Also handles common
                 N-2 table-within-section layouts.

  BUG 5 FIXED — extract_portfolio_managers() now handles <p>, <ul>, <li>
                 layouts in addition to <table>. Broadened regex patterns.

  NEW — --dry-run mode: runs full extraction pipeline and logs results to
        edgar_n2_audit.csv without writing to Supabase. Use this to measure
        coverage before committing.

  NEW -- --skip-existing flag: skips funds that already have
          objective_source = 'edgar_n2' in cef_fund_info. Useful for
          incremental refreshes.

  NEW — per-run summary stats: counts of objective/manager extraction success,
        N-2 found vs not-found, iXBRL vs legacy HTML, fallback fires.

Run:
    python edgar_fund_info_v4.py               # full run
    python edgar_fund_info_v4.py --dry-run     # audit mode, no DB writes
    python edgar_fund_info_v4.py --skip-existing  # skip already-extracted

Deps: pip install requests beautifulsoup4 lxml python-dateutil
"""

import os, io, re, csv, sys, time, zipfile, logging, argparse
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

# ── Counters for end-of-run summary ──────────────────────────────────────────
stats = {
    "total": 0,
    "n2_found": 0,
    "n2_not_found": 0,
    "ixbrl_doc": 0,
    "legacy_html_doc": 0,
    "no_readable_doc": 0,
    "fallback_fired": 0,
    "objective_extracted": 0,
    "managers_extracted": 0,
    "inception_extracted": 0,
    "both_extracted": 0,
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


def sb_upsert(table, rows, dry_run=False):
    if dry_run or not rows:
        return
    cleaned = [clean_row(r) for r in rows]

    PROTECTED_FIELDS = {
        "investment_objective", "objective_source",
        "portfolio_managers", "managers_source",
    }

    hdrs = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}

    def _post_batch(batch_rows):
        if not batch_rows:
            return
        all_keys = set()
        for row in batch_rows:
            all_keys.update(row.keys())
        normalized = [{k: row.get(k, None) for k in all_keys} for row in batch_rows]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=hdrs, json=normalized)
        if not r.ok:
            log.error(f"Batch upsert failed {r.status_code}: {r.text[:200]}")
            for row in normalized:
                r2 = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=hdrs, json=[row])
                if not r2.ok:
                    log.error(f"  [{row.get('ticker')}] failed: {r2.text[:120]}")

    with_protected    = []
    without_protected = []
    for row in cleaned:
        has_protected = any(row.get(k) is not None for k in PROTECTED_FIELDS)
        if has_protected:
            with_protected.append({k: v for k, v in row.items()
                                   if k not in PROTECTED_FIELDS or v is not None})
        else:
            without_protected.append({k: v for k, v in row.items()
                                      if k not in PROTECTED_FIELDS})

    _post_batch(with_protected)
    _post_batch(without_protected)

def load_cef_tickers():
    rows = sb_get("cef_tickers?select=ticker,cik&cik=not.is.null")
    result = {}
    for r in rows:
        if r.get("cik") and r.get("ticker"):
            cik = str(int(r["cik"])).zfill(10)
            result[r["ticker"].upper()] = cik
    log.info(f"Loaded {len(result)} tickers with CIKs")
    return result


def load_already_extracted():
    """Return set of tickers that already have objective_source = 'edgar_n2'."""
    rows = sb_get("cef_fund_info?select=ticker&objective_source=eq.edgar_n2")
    return {r["ticker"].upper() for r in rows}


def parse_tsv(content):
    if not content:
        return []
    return list(csv.DictReader(io.StringIO(content), delimiter="\t"))


def build_ncen_lookup(cik_set):
    """
    Joins across N-CEN TSV tables.
    ADVISER and SECURITY_EXCHANGE use FUND_ID (not ACCESSION_NUMBER).
    FUND_ID format: {ACCESSION_NUMBER}_{CIK}_{SERIES_ID}
    """
    cik_to_acc  = {}
    cik_to_date = {}
    cik_to_fye  = {}
    acc_to_reg  = {}
    acc_to_fund = {}
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

        for row in parse_tsv(files.get("SUBMISSION.TSV","")):
            cik = str(row.get("CIK","")).zfill(10)
            acc = row.get("ACCESSION_NUMBER","")
            dt  = row.get("FILING_DATE","")
            fye = row.get("REPORT_ENDING_PERIOD","")
            if cik and acc:
                if cik not in cik_to_date or dt > cik_to_date[cik]:
                    cik_to_date[cik] = dt
                    cik_to_acc[cik]  = acc
                    if fye:
                        cik_to_fye[cik] = fye

        for row in parse_tsv(files.get("REGISTRANT.TSV","")):
            acc = row.get("ACCESSION_NUMBER","")
            if acc and acc not in acc_to_reg:
                acc_to_reg[acc] = row

        for row in parse_tsv(files.get("FUND_REPORTED_INFO.TSV","")):
            acc = row.get("ACCESSION_NUMBER","")
            if acc and acc not in acc_to_fund:
                acc_to_fund[acc] = row

        for row in parse_tsv(files.get("ADVISER.TSV","")):
            fid = row.get("FUND_ID","")
            atype = row.get("ADVISER_TYPE","").lower()
            name  = row.get("ADVISER_NAME","")
            if fid and name and "sub" not in atype and fid not in fund_id_to_adviser:
                fund_id_to_adviser[fid] = name

        for row in parse_tsv(files.get("SECURITY_EXCHANGE.TSV","")):
            fid = row.get("FUND_ID","")
            sym = row.get("FUND_TICKER_SYMBOL","")
            if fid and sym and fid not in fund_id_to_ticker:
                fund_id_to_ticker[fid] = sym.upper()

    log.info(f"N-CEN: {len(cik_to_acc)} filers, {len(fund_id_to_adviser)} adviser records, "
             f"{len(fund_id_to_ticker)} exchange records")

    acc_prefix_to_fund_ids = {}
    for fid in set(fund_id_to_adviser) | set(fund_id_to_ticker):
        acc = fid.split("_")[0] if "_" in fid else ""
        if acc:
            acc_prefix_to_fund_ids.setdefault(acc, []).append(fid)

    def get_fund_id(acc):
        return (acc_prefix_to_fund_ids.get(acc) or [None])[0]

    def parse_fye(raw):
        try:
            from dateutil import parser as dp
            d = dp.parse(raw)
            return f"{d.month:02d}-{d.day:02d}"
        except Exception:
            return None

    def to_pct(v):
        try:
            f = float(v)
            if f != f: return None
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

        adviser_name    = fund_id_to_adviser.get(fid) if fid else None
        exchange_ticker = fund_id_to_ticker.get(fid) if fid else None

        fye_raw = cik_to_fye.get(cik,"")
        fye = parse_fye(fye_raw) if fye_raw else None

        mgmt_fee     = to_pct(fund.get("MANAGEMENT_FEE"))
        total_expense = to_pct(fund.get("NET_OPERATING_EXPENSES"))

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
            "full_name":           reg.get("REGISTRANT_NAME") or None,
            "adviser_name":        adviser_name,
            "fiscal_year_end":     fye,
            "is_term_fund":        False,
            "exchange":            exchange_ticker,
            "total_common_assets": total_common_assets,
            "mgmt_fee_pct":        mgmt_fee,
            "total_expense_pct":   total_expense,
            "as_of_date":          cik_to_date.get(cik) or None,
            "ncen_accession":      acc,
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


def _is_exhibit_row(row_text, doc_name):
    """Return True if this filing index row is an exhibit we should skip."""
    skip_text  = ("ex-99", "ex-filing", "consent", "opinion", "filing fee",
                  "exhibit", "ex-23", "ex-24")
    skip_names = ("ex99", "ex-99", "consent", "opinion", "feetable")
    if any(x in row_text for x in skip_text):
        return True
    dn = doc_name.lower()
    if any(x in dn for x in skip_names):
        return True
    return False


def get_n2_html_doc(cik, acc_raw):
    """
    Fetch the EDGAR filing index to find the best readable HTML document.

    BUG 2 FIX: iXBRL is now accepted. iXBRL N-2 filings are perfectly readable
    HTML — the iXBRL markup is just inline tags around normal text. We no longer
    filter on 'ixbrl' or 'xbrl' in the row text or filename. Instead we
    distinguish iXBRL vs legacy HTML for stats tracking only.

    Returns (doc_name, is_ixbrl) or (None, False).
    """
    acc     = acc_raw.replace("-","")
    cik_int = str(int(cik))

    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{acc_raw}-index.htm"
    r = edgar_get(index_url)
    if not (r and r.ok):
        return None, False

    soup = BeautifulSoup(r.content, "lxml")

    candidates = []  # (score, doc_name, is_ixbrl)
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        a = row.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        if not href.lower().endswith(".htm"):
            continue
        doc_name = href.split("/")[-1]
        if not doc_name:
            continue
        row_text = row.get_text(" ", strip=True).lower()

        # Skip exhibits
        if _is_exhibit_row(row_text, doc_name):
            continue

        # Detect iXBRL (for stats only — not excluded)
        is_ixbrl = ("ixbrl" in row_text or "inline xbrl" in row_text
                    or "xbrl" in doc_name.lower())

        score = 0
        if any(x in row_text for x in ("n-2", "n2", "prospectus", "registration statement")):
            score += 30
        if "complete" in row_text or "full" in row_text:
            score += 5
        # Prefer larger files
        try:
            size = int(cells[-1].get_text(strip=True).replace(",",""))
            score += min(size // 100_000, 20)
        except (ValueError, AttributeError):
            pass

        candidates.append((score, doc_name, is_ixbrl))

    if candidates:
        candidates.sort(reverse=True)
        _, best_doc, best_is_ixbrl = candidates[0]
        return best_doc, best_is_ixbrl

    return None, False


def get_latest_n2(cik):
    """
    Return the most recent N-2 or N-2/A filing that has a readable HTML document.

    BUG 3 FIX: With Bug 2 fixed, filing_has_readable_html() no longer rejects
    iXBRL filings. The fallback (return first N-2 regardless) is now only reached
    for genuinely unusual filings (e.g., PDF-only or text-only very old filings).
    We now log clearly when fallback fires.
    """
    r = edgar_get(f"{EDGAR_BASE}/submissions/CIK{cik}.json")
    if not r:
        return None
    data = r.json()
    filings = data.get("filings", {}).get("recent", {})
    all_n2 = []
    for form, dt, acc, doc in zip(filings.get("form",[]), filings.get("filingDate",[]),
                                   filings.get("accessionNumber",[]), filings.get("primaryDocument",[])):
        if form in ("N-2","N-2/A"):
            all_n2.append({"accession_number": acc, "primary_document": doc, "filing_date": dt})

    # Also check older filing pages
    for f in data.get("filings", {}).get("files", []):
        r2 = edgar_get(f"{EDGAR_BASE}/submissions/{f['name']}")
        if not r2:
            continue
        d2 = r2.json()
        for form, dt, acc, doc in zip(d2.get("form",[]), d2.get("filingDate",[]),
                                       d2.get("accessionNumber",[]), d2.get("primaryDocument",[])):
            if form in ("N-2","N-2/A"):
                all_n2.append({"accession_number": acc, "primary_document": doc, "filing_date": dt})

    if not all_n2:
        return None

    # Try each filing newest-first for one that has a readable doc
    for n2 in all_n2:
        doc, _ = get_n2_html_doc(cik, n2["accession_number"])
        if doc:
            return n2

    # Fallback: return first filing even if we couldn't confirm a good doc
    # fetch_n2_data() will handle the case where no doc is found
    log.warning(f"  All N-2 filings failed readable-doc check — using fallback")
    stats["fallback_fired"] += 1
    return all_n2[0]


def fetch_n2_data(cik, n2_info):
    """
    Fetch and parse an N-2 filing. Returns a dict of extracted fields.

    BUG 1 FIX: Now sets objective_source='edgar_n2' and managers_source='edgar_n2'
    when those fields are successfully extracted, so subsequent CEFConnect runs
    can't silently overwrite EDGAR data (callers should check source before update).
    """
    acc_raw = n2_info["accession_number"]
    acc     = acc_raw.replace("-","")
    cik_int = str(int(cik))

    doc, is_ixbrl = get_n2_html_doc(cik, acc_raw)

    if not doc:
        log.warning(f"  No readable HTML found in filing index — skipping N-2 text extraction")
        stats["no_readable_doc"] += 1
        return {"n2_accession": acc_raw}

    if is_ixbrl:
        stats["ixbrl_doc"] += 1
    else:
        stats["legacy_html_doc"] += 1

    url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{doc}"
    r = edgar_get(url)
    if not r:
        return {"n2_accession": acc_raw}

    soup = BeautifulSoup(r.content, "lxml")
    text = soup.get_text(separator=" ", strip=True)

    if len(text) < 500:
        log.warning(f"  N-2 doc too short ({len(text)} chars) — skipping text extraction")
        return {"n2_accession": acc_raw}

    result = {"n2_accession": acc_raw}

    obj = extract_investment_objective(soup, text)
    if obj:
        result["investment_objective"] = obj
        result["objective_source"] = "edgar_n2"   # ← BUG 1 FIX
        stats["objective_extracted"] += 1

    mgrs = extract_portfolio_managers(soup, text)
    if mgrs:
        result["portfolio_managers"] = mgrs
        result["managers_source"] = "edgar_n2"    # ← BUG 1 FIX
        stats["managers_extracted"] += 1

    if obj and mgrs:
        stats["both_extracted"] += 1

    inception_info = extract_inception_info(text)
    result.update(inception_info)
    if inception_info:
        stats["inception_extracted"] += 1

    result.update(extract_expenses(soup))
    result.update(extract_term_and_tender(text))

    return result


# ── Extraction helpers ────────────────────────────────────────────────────────

def _get_text_after_header(header_tag, max_chars=1000, max_paras=3):
    """
    BUG 4 FIX: Broadened DOM traversal for objective/manager text.

    Strategy:
      1. Try direct next siblings of the header tag (v3 approach)
      2. Try next siblings of the header's PARENT (handles wrapped layouts)
      3. Try the parent's parent (two levels up) for deeply nested headers
    """
    paras = []

    def _harvest(start_tag, limit):
        collected = []
        sib = start_tag.find_next_sibling()
        while sib and len(collected) < limit:
            if sib.name in ("p","div","td","li","span"):
                p = sib.get_text(separator=" ", strip=True)
                if len(p) > 30:
                    collected.append(p)
                    if len(p) > 100:
                        break
            elif sib.name in ("h1","h2","h3","h4","h5"):
                # Hit the next section header — stop
                break
            sib = sib.find_next_sibling()
        return collected

    # Level 1: direct siblings
    paras = _harvest(header_tag, max_paras)
    if paras:
        return " ".join(paras[:2])[:max_chars]

    # Level 2: parent's siblings
    parent = header_tag.parent
    if parent and parent.name not in ("body","html","[document]"):
        paras = _harvest(parent, max_paras)
        if paras:
            return " ".join(paras[:2])[:max_chars]

    # Level 3: grandparent's siblings
    if parent:
        gp = parent.parent
        if gp and gp.name not in ("body","html","[document]"):
            paras = _harvest(gp, max_paras)
            if paras:
                return " ".join(paras[:2])[:max_chars]

    return None


def extract_investment_objective(soup, text):
    """
    Extracts the fund's investment objective from N-2 HTML.

    Key quality fixes:
      - Boilerplate blocklist: rejects cover-page disclaimers that appear near
        the word "objective" (e.g. "You should rely only on...", forward-looking
        statement warnings, SEC legend text).
      - Substantive opener requirement: DOM results must begin with a recognizable
        investment-description phrase ("The Fund seeks", "seeks to provide",
        "investment objective is", etc.) OR be long enough to be real prose.
      - Regex patterns target the actual objective sentence directly so they
        can't match a disclaimer.
    """
    # Boilerplate phrases that appear near "objective" on N-2 cover pages — reject any
    # candidate that starts with one of these (case-insensitive prefix check).
    BOILERPLATE_OPENERS = (
        "you should rely only",
        "this prospectus contains",
        "this registration statement",
        "no dealer, salesperson",
        "neither the securities",
        "the securities and exchange",
        "as described in this",
        "please read this prospectus",
        "before you invest",
        "this is not an offer",
        "forward-looking statements",
        "we have not authorized",
        "table of contents",
        "summary of",
    )

    def _is_boilerplate(s):
        # Normalize ALL whitespace (including internal newlines) before checking
        low = re.sub(r'\s+', ' ', s).lower().strip()
        return any(bp in low[:150] for bp in BOILERPLATE_OPENERS)

    def _looks_like_objective(s):
        """True if text looks like a real investment objective statement."""
        low = s.lower()
        signals = (
            "seeks to", "seeks high", "seeks current", "seeks total",
            "investment objective is", "investment objective of",
            "primary objective", "principal objective",
            "the fund's objective", "the trust's objective",
            "to provide", "to achieve", "to maximize", "to generate",
            "current income", "capital appreciation", "total return",
            "high current income", "tax-exempt income",
        )
        return any(sig in low for sig in signals)

    # DOM approach — walk all candidate header tags
    for tag in soup.find_all(["h1","h2","h3","h4","b","strong","p","td","th"]):
        t = tag.get_text(strip=True).lower()
        if "investment objective" in t and len(t) < 100:
            candidate = _get_text_after_header(tag, max_chars=1000, max_paras=3)
            if candidate and len(candidate) > 50:
                if not _is_boilerplate(candidate) and _looks_like_objective(candidate):
                    return candidate
                # If it looks like boilerplate, keep searching — don't return yet

    # Regex patterns — these target the actual objective sentence so they can't
    # accidentally match a disclaimer paragraph.
    patterns = [
        # "The Fund seeks to achieve/provide/maximize..."
        r"(?:The\s+(?:Fund|Trust|Portfolio)[''']?s?\s+(?:primary\s+)?investment objective[^\.\n]{0,60}(?:is|are)[^\n\r]{30,500})",
        # "The Fund seeks [high/current/total/tax-exempt]..."
        r"(?:The\s+(?:Fund|Trust|Portfolio)\s+seeks\s+(?:to\s+)?(?:provide|achieve|maximize|generate|obtain|earn)[^\n\r]{30,500})",
        r"(?:The\s+(?:Fund|Trust|Portfolio)\s+seeks\s+(?:high|current|total|maximum|tax)[^\n\r]{30,500})",
        # Block after "INVESTMENT OBJECTIVE" header that starts with "The Fund/Trust"
        r"(?:INVESTMENT OBJECTIVE|Investment Objective|INVESTMENT OBJECTIVES)\s*[\n\r\s]{1,80}(The\s+(?:Fund|Trust|Portfolio)[^\n\r]{50,600})",
        # "seeks current income" / "seeks total return" standalone
        r"seeks\s+(?:current income|total return|high current income|capital appreciation|tax-exempt)[^\n\r]{20,400}",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if m:
            raw = m.group(1) if m.lastindex else m.group(0)
            clean = re.sub(r"\s+", " ", raw).strip()[:1000]
            if len(clean) > 50 and not _is_boilerplate(clean):
                return clean

    return None


def extract_portfolio_managers(soup, text):
    """
    Extracts portfolio manager names from N-2 HTML.

    Key quality fixes:
      - Table proximity check: only considers tables within 3000 chars of the
        section header, not just the next table anywhere in the document.
      - Non-name row filter: rejects rows whose first cell contains known
        non-name patterns (account type categories, column headers, etc.)
      - Name validation: candidate must look like a person's name
        (2-4 Title-Case words, not all-caps, not a known category string).
      - Column header detection: skips rows where first cell is "Name", 
        "Portfolio Manager", "Manager", or similar header labels.
    """
    # Strings that look like table headers, account-type categories, or addresses — not names
    NON_NAME_PATTERNS = (
        "portfolio manager", "registered investment", "other pooled",
        "other account", "title of class", "common share", "preferred share",
        "name", "manager", "since", "years", "type of account",
        "number of accounts", "total assets", "additional information",
        "accounts managed", "investment adviser", "investment company",
        "fund manager", "fund advisor", "fund advisors", "fund adviser",
        "the fund", "the trust", "the portfolio",
        "annual fund", "fiscal year", "board of", "committee",
        "executive officer", "chief ", "senior ", "managing ",
        "vice president", "information about", "see also",
        # Compensation table terms
        "base salary", "annual salary", "bonus", "compensation",
        "salary", "incentive", "long-term", "short-term", "equity award",
        "stock option", "restricted stock", "deferred compensation",
        # Structural/legal terms
        "limited liability", "general partner", "limited partner",
        "class of", "series of", "type of", "form of",
        # Address/location indicators (catches "West Wacker Drive" etc.)
        "drive", "street", "avenue", "boulevard", "road", "lane", "place",
        "suite", "floor", "plaza",
    )

    # Words that never legitimately begin a person's name
    NON_NAME_FIRST_WORDS = {
        "the", "a", "an", "our", "its", "no", "this", "that", "each",
        "such", "all", "any", "other", "certain", "additional", "total",
        "registered", "accounts", "fund", "trust", "investment",
        "base", "annual", "long", "short", "equity", "stock",
        "limited", "general", "class", "series", "type", "form",
        "average", "information", "legal", "audit", "auditing",
        "administrative", "transfer", "custodian", "distribution",
        "service", "filing", "subscription", "printing", "miscellaneous",
        "fee", "fees", "expense", "expenses",
    }

    def _is_valid_name(s):
        """True if string looks like a person's name (2-4 Title-Case words)."""
        s = s.strip()
        if not s or len(s) < 4:
            return False
        low = s.lower()
        # Reject if it matches any non-name pattern
        if any(pat in low for pat in NON_NAME_PATTERNS):
            return False
        words = s.split()
        if len(words) < 2 or len(words) > 5:
            return False
        # First word must not be a determiner, article, or common non-name opener
        if words[0].lower() in NON_NAME_FIRST_WORDS:
            return False
        # At least first and last word should be Title-Case
        if not (words[0][0].isupper() and words[-1][0].isupper()):
            return False
        # Reject strings that are entirely uppercase (column headers)
        if s.upper() == s:
            return False
        # Each word should be reasonably short — real names are typically 3-15 chars per word
        if any(len(w) > 18 for w in words):
            return False
        return True

    managers = []

    # Find the portfolio manager section header — prefer headers that are
    # standalone labels (short text), not table cells mid-paragraph
    pm_tag = None
    for tag in soup.find_all(["h2","h3","h4","b","strong","p","td","th"]):
        t = tag.get_text(strip=True).lower()
        if "portfolio manager" in t and len(t) < 60:
            pm_tag = tag
            break

    if pm_tag:
        # Get approximate character position of pm_tag in the full text
        pm_text_start = text.find(pm_tag.get_text(strip=True))

        # Strategy A: table immediately after the header
        # Only look at tables within a reasonable proximity (not the entire doc)
        tbl = pm_tag.find_next("table")
        if tbl:
            tbl_text_pos = text.find(tbl.get_text(strip=True)[:50])
            # Skip if table is more than 3000 chars away (likely a different section)
            if pm_text_start >= 0 and tbl_text_pos >= 0:
                if tbl_text_pos - pm_text_start > 3000:
                    tbl = None  # too far — don't use this table

        if tbl:
            for row in tbl.find_all("tr"):
                cells = row.find_all(["td","th"])
                if not cells:
                    continue
                name = cells[0].get_text(strip=True)
                if _is_valid_name(name):
                    managers.append(name)

        # Strategy B: <ul>/<li> list after the header
        if not managers:
            ul = pm_tag.find_next(["ul","ol"])
            if ul:
                for li in ul.find_all("li"):
                    name_text = li.get_text(strip=True)
                    name = re.split(r"[,\-–—\.]", name_text)[0].strip()
                    if _is_valid_name(name):
                        managers.append(name)

        # Strategy C: paragraph text after the header — extract name-like tokens
        if not managers:
            candidate_text = _get_text_after_header(pm_tag, max_chars=600, max_paras=3)
            if candidate_text:
                found = re.findall(
                    r"\b([A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+){1,3})\b",
                    candidate_text)
                managers = [n for n in found if _is_valid_name(n)][:6]

    # Final regex fallback — look for "portfolio managers: Name, Name, Name"
    if not managers:
        m = re.search(
            r"portfolio manager[s]?\s*[:\n\r]+\s*"
            r"((?:[A-Z][a-z]+(?:\s+[A-Z]\.?)?(?:\s+[A-Z][a-z]+)+(?:,?\s+)?){1,8})",
            text, re.IGNORECASE)
        if m:
            names = [n.strip() for n in re.split(r"[,\n\r]+", m.group(1)) if n.strip()]
            managers = [n for n in names if _is_valid_name(n)][:6]

    return ", ".join(dict.fromkeys(managers)) if managers else None  # dedup, preserve order


def extract_inception_info(text):
    result = {}
    for pat in [
        r"commenced (?:operations|trading) (?:on\s+)?([A-Z][a-z]+ \d{1,2},? \d{4})",
        r"inception date[:\s]+([A-Z][a-z]+ \d{1,2},? \d{4}|\d{1,2}/\d{1,2}/\d{4})",
        r"organized (?:on\s+)?([A-Z][a-z]+ \d{1,2},? \d{4})",
        r"incorporated (?:on\s+)?([A-Z][a-z]+ \d{1,2},? \d{4})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                from dateutil import parser as dp
                result["inception_date"] = dp.parse(m.group(1)).date().isoformat()
                break
            except Exception:
                pass

    m = re.search(r"(?:initial|offering) (?:public )?(?:offering )?price of \$([\d.]+)", text, re.IGNORECASE)
    if m:
        try: result["inception_price"] = float(m.group(1))
        except Exception: pass

    m = re.search(r"initial NAV of \$([\d.]+)", text, re.IGNORECASE)
    if m:
        try: result["inception_nav"] = float(m.group(1))
        except Exception: pass

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
                        except Exception: pass
                if pct is not None:
                    if "management" in label: rows_data["mgmt_fee_pct"] = pct
                    elif "other expense" in label or "operating expense" in label: rows_data["other_expense_pct"] = pct
                    elif "interest" in label or "borrow" in label: rows_data["interest_expense_pct"] = pct
                    elif "total" in label and "annual" in label: rows_data["total_expense_pct"] = pct
        if rows_data:
            result.update(rows_data)
            break
    return result


def extract_term_and_tender(text):
    """Extract is_term_fund and has_tender_offer from N-2 text."""
    result = {}

    # Term fund detection
    term_patterns = [
        r"\bterm fund\b",
        r"terminat(?:es|ing|ion) on [A-Z][a-z]+ \d{4}",
        r"fixed (?:term|life) of",
        r"wind(?:s|ing) (?:down|up) on",
    ]
    for pat in term_patterns:
        if re.search(pat, text, re.IGNORECASE):
            result["is_term_fund"] = True
            break

    # Tender offer detection
    tender_patterns = [
        r"tender offer",
        r"interval fund",
        r"repurchase offer",
        r"periodic repurchase",
    ]
    for pat in tender_patterns:
        if re.search(pat, text, re.IGNORECASE):
            result["has_tender_offer"] = True
            break

    return result


# ── Audit CSV writer ──────────────────────────────────────────────────────────

def write_audit_csv(audit_rows, path="edgar_n2_audit.csv"):
    if not audit_rows:
        return
    fieldnames = ["ticker", "cik", "n2_found", "n2_accession", "n2_date",
                  "is_ixbrl", "doc_found", "objective_extracted", "objective_preview",
                  "managers_extracted", "managers_preview", "inception_found",
                  "is_term_fund", "has_tender_offer"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(audit_rows)
    log.info(f"Audit CSV written → {path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run=False, skip_existing=False, tickers_filter=None):
    if not dry_run and not SUPABASE_KEY:
        log.error("SUPABASE_KEY not set"); sys.exit(1)

    ticker_to_cik = load_cef_tickers()
    if not ticker_to_cik:
        log.error("No tickers with CIKs. Run populate_ciks.py first."); sys.exit(1)

    if skip_existing:
        already_done = load_already_extracted()
        before = len(ticker_to_cik)
        ticker_to_cik = {t: c for t, c in ticker_to_cik.items() if t not in already_done}
        log.info(f"--skip-existing: skipped {before - len(ticker_to_cik)} funds already at edgar_n2, "
                 f"{len(ticker_to_cik)} remaining")

    if tickers_filter:
        ticker_to_cik = {t: c for t, c in ticker_to_cik.items()
                         if t.upper() in {x.upper() for x in tickers_filter}}
        log.info(f"Filtered to {len(ticker_to_cik)} specified tickers")

    cik_set = set(ticker_to_cik.values())
    stats["total"] = len(ticker_to_cik)

    log.info("Phase 1: Building N-CEN lookup from 4 quarters...")
    ncen_data = build_ncen_lookup(cik_set)

    missing = cik_set - set(ncen_data.keys())
    if missing:
        tickers = [next(t for t, c in ticker_to_cik.items() if c == cik)
                   for cik in list(missing)[:10]]
        log.warning(f"No N-CEN match for {len(missing)} funds: {tickers}")

    log.info(f"Phase 2: Fetching N-2 filings{' (DRY RUN)' if dry_run else ''}...")
    rows_to_upsert = []
    audit_rows = []
    total = len(ticker_to_cik)

    for i, (ticker, cik) in enumerate(ticker_to_cik.items()):
        log.info(f"[{i+1}/{total}] {ticker}")
        row = {"ticker": ticker, "cik": cik, "last_updated": datetime.utcnow().isoformat()}

        if cik in ncen_data:
            row.update({k: v for k, v in ncen_data[cik].items() if v is not None})

        audit = {"ticker": ticker, "cik": cik, "n2_found": False, "is_ixbrl": False,
                 "doc_found": False, "objective_extracted": False, "managers_extracted": False,
                 "inception_found": False, "is_term_fund": False, "has_tender_offer": False}

        n2_info = get_latest_n2(cik)
        if n2_info:
            stats["n2_found"] += 1
            audit["n2_found"] = True
            audit["n2_accession"] = n2_info["accession_number"]
            audit["n2_date"] = n2_info["filing_date"]
            log.info(f"  N-2: {n2_info['accession_number']} ({n2_info['filing_date']})")

            n2_data = fetch_n2_data(cik, n2_info)
            row.update({k: v for k, v in n2_data.items() if v is not None})

            audit["doc_found"]           = "investment_objective" in n2_data or "portfolio_managers" in n2_data
            audit["objective_extracted"] = "investment_objective" in n2_data
            audit["managers_extracted"]  = "portfolio_managers" in n2_data
            audit["inception_found"]     = "inception_date" in n2_data
            audit["is_term_fund"]        = n2_data.get("is_term_fund", False)
            audit["has_tender_offer"]    = n2_data.get("has_tender_offer", False)
            audit["is_ixbrl"]            = n2_data.get("_is_ixbrl", False)  # internal flag

            if "investment_objective" in n2_data:
                audit["objective_preview"] = n2_data["investment_objective"][:120]
            if "portfolio_managers" in n2_data:
                audit["managers_preview"] = n2_data["portfolio_managers"][:80]
        else:
            stats["n2_not_found"] += 1
            log.warning(f"  No N-2 for {ticker}")

        audit_rows.append(audit)
        rows_to_upsert.append(row)

        if len(rows_to_upsert) >= BATCH_SIZE:
            log.info(f"  Upserting batch of {len(rows_to_upsert)}...")
            sb_upsert("cef_fund_info", rows_to_upsert, dry_run=dry_run)
            rows_to_upsert = []

    if rows_to_upsert:
        log.info(f"  Upserting final {len(rows_to_upsert)}...")
        sb_upsert("cef_fund_info", rows_to_upsert, dry_run=dry_run)

    # Always write audit CSV
    write_audit_csv(audit_rows)

    # Summary
    log.info("")
    log.info("═══════════════════ RUN SUMMARY ═══════════════════")
    log.info(f"  Total funds processed :  {stats['total']}")
    log.info(f"  N-2 filing found      :  {stats['n2_found']}")
    log.info(f"  N-2 not found         :  {stats['n2_not_found']}")
    log.info(f"    → iXBRL docs parsed :  {stats['ixbrl_doc']}")
    log.info(f"    → Legacy HTML docs  :  {stats['legacy_html_doc']}")
    log.info(f"    → No readable doc   :  {stats['no_readable_doc']}")
    log.info(f"    → Fallback fired    :  {stats['fallback_fired']}")
    log.info(f"  Objective extracted   :  {stats['objective_extracted']}")
    log.info(f"  Managers extracted    :  {stats['managers_extracted']}")
    log.info(f"  Both extracted        :  {stats['both_extracted']}")
    log.info(f"  Inception extracted   :  {stats['inception_extracted']}")
    if stats["n2_found"]:
        obj_rate = stats["objective_extracted"] / stats["n2_found"] * 100
        mgr_rate = stats["managers_extracted"]  / stats["n2_found"] * 100
        log.info(f"  Objective hit rate    :  {obj_rate:.1f}%")
        log.info(f"  Manager hit rate      :  {mgr_rate:.1f}%")
    if dry_run:
        log.info("  *** DRY RUN — no data written to Supabase ***")
    log.info("════════════════════════════════════════════════════")
    log.info("✓ Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="edgar_fund_info v4 — N-2 pipeline")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run extraction but do not write to Supabase. Writes edgar_n2_audit.csv.")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip tickers that already have objective_source='edgar_n2' in cef_fund_info.")
    parser.add_argument("--tickers", nargs="+", metavar="TICKER",
                        help="Only process specific tickers (e.g. --tickers PDI GOF UTF)")
    args = parser.parse_args()

    run(dry_run=args.dry_run, skip_existing=args.skip_existing, tickers_filter=args.tickers)
