"""
patch_upsert.py
───────────────
Patches the sb_upsert function in both edgar_fund_info_v4.py and
cefconnect_scraper.py to use the two-group split that prevents
PostgREST batch normalization from wiping protected fields with nulls.

Run from the Reporting\\News folder:
    python patch_upsert.py
"""

import re, sys, pathlib

# ── Shared replacement function ───────────────────────────────────────────────

NEW_UPSERT_EDGAR = '''def sb_upsert(table, rows, dry_run=False):
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
'''

NEW_UPSERT_SCRAPER = '''def sb_upsert(table, rows):
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
'''

NEW_LOAD_EDGAR_PROTECTED = '''
def load_edgar_protected():
    """Return set of tickers that already have edgar_n2 as their source."""
    rows = sb_get("cef_fund_info?select=ticker,objective_source,managers_source")
    protected = set()
    for r in rows:
        if r.get("objective_source") == "edgar_n2" or r.get("managers_source") == "edgar_n2":
            protected.add(r["ticker"].upper())
    log.info(f"Source protection: {len(protected)} funds already have edgar_n2 data (will not overwrite)")
    return protected

'''

# ── Patch edgar_fund_info_v4.py ───────────────────────────────────────────────

def patch_edgar():
    path = pathlib.Path("edgar_fund_info_v4.py")
    if not path.exists():
        print(f"  NOT FOUND: {path}")
        return False

    content = path.read_text(encoding="utf-8")

    # Replace whatever sb_upsert is currently there
    # Match from "def sb_upsert" to just before the next "def " at module level
    pattern = r'(def sb_upsert\(table, rows.*?)\n(?=def )'
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        print("  edgar: could not find sb_upsert — check file manually")
        return False

    content = content[:match.start()] + NEW_UPSERT_EDGAR + "\n" + content[match.end():]
    path.write_text(content, encoding="utf-8")
    print(f"  edgar_fund_info_v4.py patched OK")
    return True


# ── Patch cefconnect_scraper.py ───────────────────────────────────────────────

def patch_scraper():
    path = pathlib.Path("cefconnect_scraper.py")
    if not path.exists():
        print(f"  NOT FOUND: {path}")
        return False

    content = path.read_text(encoding="utf-8")

    # Replace sb_upsert
    pattern = r'(def sb_upsert\(table, rows\).*?)\n(?=def )'
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        print("  scraper: could not find sb_upsert — check file manually")
        return False

    content = content[:match.start()] + NEW_UPSERT_SCRAPER + "\n" + content[match.end():]

    # Add load_edgar_protected if not already present
    if "load_edgar_protected" not in content:
        insert_before = "def scrape_all_funds("
        idx = content.find(insert_before)
        if idx == -1:
            print("  scraper: could not find scrape_all_funds insertion point")
        else:
            content = content[:idx] + NEW_LOAD_EDGAR_PROTECTED + content[idx:]

    # Add edgar_protected logic inside scrape_all_funds if not already present
    if "edgar_protected" not in content:
        old_loop = '        data = scrape_fund_page(ticker)\n\n        if data:\n            row = {"ticker": ticker, "last_updated": datetime.datetime.utcnow().isoformat()}\n            row.update(data)'
        new_loop = '''        data = scrape_fund_page(ticker)

        if data:
            row = {"ticker": ticker, "last_updated": datetime.datetime.utcnow().isoformat()}
            if ticker.upper() in edgar_protected:
                EDGAR_FIELDS = {"investment_objective", "objective_source",
                                "portfolio_managers", "managers_source"}
                data = {k: v for k, v in data.items() if k not in EDGAR_FIELDS}
            row.update(data)'''
        if old_loop in content:
            content = content.replace(old_loop, new_loop, 1)
            # Insert edgar_protected = load_edgar_protected() at top of scrape_all_funds
            old_init = '    tickers = sorted(tickers_to_scrape)\n    total   = len(tickers)\n    batch   = []'
            new_init = '    tickers = sorted(tickers_to_scrape)\n    total   = len(tickers)\n    batch   = []\n    edgar_protected = load_edgar_protected()'
            content = content.replace(old_init, new_init, 1)
        else:
            print("  scraper: could not find loop body — edgar_protected injection skipped")

    path.write_text(content, encoding="utf-8")
    print(f"  cefconnect_scraper.py patched OK")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────

print("Patching sb_upsert in both files...")
e = patch_edgar()
s = patch_scraper()

if e and s:
    print("\nBoth files patched. Run next:")
    print("  python edgar_fund_info_v4.py")
elif e:
    print("\nedgar patched. Scraper not found — patch it manually or move to this folder.")
elif s:
    print("\nScraper patched. Edgar not found — patch it manually or move to this folder.")
else:
    print("\nNeither file found in current directory. Run this script from Reporting\\News.")
