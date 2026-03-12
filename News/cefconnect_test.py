"""
cefconnect_test.py v4 - parse fund page HTML directly + check pricinghistory fields
"""
import requests, json, time, re
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Referer": "https://www.cefconnect.com/",
}
JSON_HEADERS = {**HEADERS, "Accept": "application/json"}

for ticker in ["GOF", "XFLT", "ECC"]:
    print(f"\n{'='*60}")
    print(f"FUND: {ticker}")

    r = requests.get(f"https://www.cefconnect.com/fund/{ticker}", headers=HEADERS, timeout=15)
    soup = BeautifulSoup(r.text, "lxml")

    # Dump all text content in divs/sections that might contain objective or managers
    # Look for labels like "Investment Objective", "Portfolio Manager"
    all_text = soup.get_text(separator="\n", strip=True)
    lines = [l.strip() for l in all_text.split("\n") if l.strip()]

    print(f"  Total lines: {len(lines)}")

    # Find lines near "objective" or "manager"
    for i, line in enumerate(lines):
        ll = line.lower()
        if any(x in ll for x in ("investment objective", "portfolio manager", "fund manager",
                                  "investment advisor", "adviser", "inception")):
            context = lines[max(0,i-1):i+5]
            print(f"  [Line {i}] >>> {line}")
            for c in context[1:]:
                print(f"           {c}")
            print()

    time.sleep(0.4)

# Also check pricinghistory endpoint for useful fields
print("\n=== PRICINGHISTORY FIELDS ===")
r = requests.get("https://www.cefconnect.com/api/v3/pricinghistory/GOF/5D", headers=JSON_HEADERS, timeout=10)
print(f"Status: {r.status_code}")
if r.ok:
    d = r.json()
    if isinstance(d, list) and d:
        print(f"  {len(d)} rows, keys: {list(d[0].keys())}")
        print(f"  Sample: {d[-1]}")
    elif isinstance(d, dict):
        print(f"  Keys: {list(d.keys())}")
        for k,v in d.items():
            if isinstance(v, list) and v:
                print(f"  {k}[0]: {v[0]}")

