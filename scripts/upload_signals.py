#!/usr/bin/env python3
"""
upload_signals.py — Push engine output to Supabase engine_signals table
========================================================================
Called by GitHub Actions after run_cef_pipeline.py completes.

Reads cef_signals_output.json and upserts each signal into the
engine_signals table so the Command Center can auto-load via REST.

Also stores the full JSON blob in Supabase storage for backup/CC fetch.

Usage:
    python scripts/upload_signals.py
    python scripts/upload_signals.py --input custom_output.json

Requires env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import json
import os
import sys
import urllib.request
import urllib.parse
from datetime import date, datetime


# --- Config ---
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")  # service role for writes
INPUT_FILE = sys.argv[1] if len(sys.argv) > 1 else "cef_signals_output.json"
TABLE = "engine_signals"
STORAGE_BUCKET = "engine-output"


def supabase_upsert(table: str, rows: list) -> dict:
    """Upsert rows to Supabase table."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=ticker,signal_date"
    data = json.dumps(rows).encode()

    req = urllib.request.Request(url, data=data, method="POST", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",  # upsert on unique constraint
    })

    with urllib.request.urlopen(req) as resp:
        return {"status": resp.status}


def supabase_storage_upload(bucket: str, filename: str, content: bytes) -> dict:
    """Upload file to Supabase storage bucket."""
    url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{filename}"

    req = urllib.request.Request(url, data=content, method="PUT", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "x-upsert": "true",  # overwrite if exists
    })

    try:
        with urllib.request.urlopen(req) as resp:
            return {"status": resp.status}
    except urllib.error.HTTPError as e:
        # If bucket doesn't exist yet, warn but don't fail
        print(f"  Storage upload warning: {e.code} - {e.read().decode()[:200]}")
        return {"status": e.code, "error": True}


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)

    if not os.path.exists(INPUT_FILE):
        print(f"ERROR: {INPUT_FILE} not found. Did run_cef_pipeline.py succeed?")
        sys.exit(1)

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        output = json.load(f)

    signals = output.get("all_signals", [])
    today = date.today().isoformat()

    print(f"Uploading {len(signals)} signals to Supabase...")

    # --- Step 1: Upsert to engine_signals table ---
    rows = []
    for sig in signals:
        rows.append({
            "ticker": sig["ticker"],
            "signal_date": today,
            "final_signal": sig["final_signal"],
            "composite_score": sig.get("composite_score"),
            "regime_adjusted_score": sig.get("regime_adjusted_score"),
            "signal_data": sig,  # full CC-format JSON
        })

    # Batch in groups of 100
    batch_size = 100
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            result = supabase_upsert(TABLE, batch)
            print(f"  Batch {i // batch_size + 1}: {len(batch)} rows → "
                  f"HTTP {result['status']}")
        except Exception as e:
            print(f"  Batch {i // batch_size + 1} FAILED: {e}")
            # Don't exit — try remaining batches

    # --- Step 2: Upload full JSON to storage bucket ---
    print(f"Uploading full JSON to storage bucket '{STORAGE_BUCKET}'...")
    json_bytes = json.dumps(output, default=str).encode()

    # Store with date-stamped name + a "latest" copy
    supabase_storage_upload(STORAGE_BUCKET, f"signals_{today}.json", json_bytes)
    supabase_storage_upload(STORAGE_BUCKET, "latest_signals.json", json_bytes)

    print(f"Done. {len(signals)} signals uploaded for {today}.")


if __name__ == "__main__":
    main()
