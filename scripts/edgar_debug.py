"""
edgar_debug.py
Prints actual column names from N-CEN TSVs so we can fix the field mappings.
Run once, paste output to Claude.
"""
import io, os, sys, zipfile, csv, time, requests

SUPABASE_KEY = os.environ.get("SUPABASE_KEY","")
HEADERS = {"User-Agent": "Gridiron Partners research@gridironpartners.com"}
EDGAR_BASE = "https://data.sec.gov"

# Check one quarter's TSVs
ZIP_URL = "https://www.sec.gov/files/dera/data/form-n-cen-data-sets/2025q3_ncen.zip"

print("=== Downloading N-CEN ZIP ===")
r = requests.get(ZIP_URL, headers=HEADERS, stream=True, timeout=120)
r.raise_for_status()

TARGET_FILES = {"SUBMISSION.TSV","REGISTRANT.TSV","FUND_REPORTED_INFO.TSV","ADVISER.TSV","SECURITY_EXCHANGE.TSV"}

with zipfile.ZipFile(io.BytesIO(r.content)) as z:
    for name in z.namelist():
        base = name.split("/")[-1].upper()
        if base in TARGET_FILES:
            content = z.read(name).decode("utf-8", errors="replace")
            reader = csv.DictReader(io.StringIO(content), delimiter="\t")
            cols = reader.fieldnames
            print(f"\n--- {base} ---")
            print(f"Columns ({len(cols)}): {cols}")
            # Show first data row
            for row in reader:
                print(f"Sample row: {dict(list(row.items())[:8])}")
                break

# Also check N-2 for XFLT specifically
print("\n=== Checking N-2 for XFLT (CIK 0001703079) ===")
time.sleep(0.2)
r2 = requests.get(f"{EDGAR_BASE}/submissions/CIK0001703079.json", headers=HEADERS, timeout=30)
data = r2.json()
filings = data.get("filings",{}).get("recent",{})
forms = filings.get("form",[])
dates = filings.get("filingDate",[])
accs  = filings.get("accessionNumber",[])
docs  = filings.get("primaryDocument",[])
print(f"Recent filing types (first 20): {forms[:20]}")
for form, dt, acc, doc in zip(forms, dates, accs, docs):
    if form in ("N-2","N-2/A","N-14","485BPOS"):
        print(f"  Found: {form} | {dt} | {acc} | {doc}")
        break

