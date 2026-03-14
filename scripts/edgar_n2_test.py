"""
edgar_n2_test.py
Tests N-2 fetching for 3 funds and prints exactly what happens at each step.
Run this to diagnose why investment_objective isn't populating.
"""
import os, io, re, time, requests
from bs4 import BeautifulSoup

EDGAR_BASE = "https://data.sec.gov"
HEADERS    = {"User-Agent": "Gridiron Partners research@gridironpartners.com"}

TEST_FUNDS = [
    ("XFLT", "0001703079"),
    ("GOF",  "0001380936"),
    ("PDI",  "0001510599"),
]

def edgar_get(url, retries=3):
    for attempt in range(retries):
        try:
            time.sleep(0.2)
            r = requests.get(url, headers=HEADERS, timeout=30)
            print(f"    GET {url[-60:]} → {r.status_code}")
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                print("    Rate limited — sleeping 5s"); time.sleep(5)
        except Exception as e:
            print(f"    Request failed: {e}")
    return None

for ticker, cik in TEST_FUNDS:
    print(f"\n{'='*60}")
    print(f"FUND: {ticker}  CIK: {cik}")

    # Step 1: Get submissions
    r = edgar_get(f"{EDGAR_BASE}/submissions/CIK{cik}.json")
    if not r:
        print("  FAIL: Could not fetch submissions"); continue

    data = r.json()
    filings = data.get("filings",{}).get("recent",{})
    forms   = filings.get("form",[])
    dates   = filings.get("filingDate",[])
    accs    = filings.get("accessionNumber",[])
    docs    = filings.get("primaryDocument",[])

    n2_info = None
    for form, dt, acc, doc in zip(forms, dates, accs, docs):
        if form in ("N-2","N-2/A"):
            n2_info = {"accession_number": acc, "primary_document": doc, "filing_date": dt}
            break

    if not n2_info:
        print("  No N-2 found in recent filings"); continue

    print(f"  N-2 found: {n2_info['accession_number']} ({n2_info['filing_date']})")
    print(f"  Primary doc: {n2_info['primary_document']}")

    # Step 2: Fetch filing index
    acc_raw = n2_info["accession_number"]
    acc     = acc_raw.replace("-","")
    cik_int = str(int(cik))
    index_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{acc_raw}-index.htm"
    r2 = edgar_get(index_url)

    best_doc = None
    if r2 and r2.ok:
        soup = BeautifulSoup(r2.content, "lxml")
        print(f"  Index page fetched — scanning for HTML docs...")
        for row in soup.find_all("tr"):
            a = row.find("a", href=True)
            if not a or not a["href"].lower().endswith(".htm"):
                continue
            doc_name = a["href"].split("/")[-1]
            row_text = row.get_text(" ", strip=True)
            is_xbrl = "ixbrl" in row_text.lower() or "inline xbrl" in row_text.lower() or "xbrl" in doc_name.lower()
            print(f"    Found: {doc_name}  XBRL={is_xbrl}  ({row_text[:60]})")
            if best_doc is None and not is_xbrl:
                best_doc = doc_name
    else:
        print("  FAIL: Could not fetch index page")

    print(f"  Best doc selected: {best_doc or n2_info['primary_document']}")

    # Step 3: Fetch the actual HTML
    doc = best_doc or n2_info["primary_document"]
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc}/{doc}"
    r3 = edgar_get(doc_url)

    if not r3:
        print("  FAIL: Could not fetch N-2 HTML"); continue

    text = BeautifulSoup(r3.content, "lxml").get_text(separator=" ", strip=True)
    print(f"  N-2 HTML length: {len(text)} chars")
    print(f"  First 200 chars: {text[:200]}")

    # Check for investment objective
    obj_idx = text.lower().find("investment objective")
    if obj_idx > -1:
        print(f"  'Investment objective' found at char {obj_idx}")
        print(f"  Context: {text[obj_idx:obj_idx+300]}")
    else:
        print("  'Investment objective' NOT found in text")

