#!/usr/bin/env python3
"""
health_check.py — Verify pipeline completed and data is fresh
===============================================================
Uses actual table schemas. Runs after engine pipeline.

Usage:
    python scripts/health_check.py

Requires env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import json
import os
import sys
import urllib.request
from datetime import date


SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
TODAY = date.today().isoformat()


def supabase_query(table, select="*", params=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}"
    if params:
        for k, v in params.items():
            url += f"&{k}={v}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return []


def supabase_insert(table, row):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(row).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    })
    try:
        with urllib.request.urlopen(req) as resp:
            pass
    except Exception as e:
        print(f"  Could not log alert: {e}")


def log_alert(check_name, status, message):
    print(f"  [{status.upper()}] {check_name}: {message}")
    supabase_insert("pipeline_alerts", {
        "alert_date": TODAY,
        "check_name": check_name,
        "status": status,
        "message": message,
    })


def main():
    print(f"Pipeline Health Check — {TODAY}")
    print("-" * 50)
    has_errors = False

    # Check 1: cef_daily has today's data
    rows = supabase_query("cef_daily", "ticker", {
        "trade_date": f"eq.{TODAY}", "limit": "5",
    })
    if rows:
        log_alert("fasttrack_sync", "ok", f"cef_daily has {len(rows)}+ rows for today")
    else:
        log_alert("fasttrack_sync", "warning",
                  "No cef_daily rows for today — market holiday or sync failed")

    # Check 2: engine_signals has today's data
    rows = supabase_query("engine_signals", "ticker", {
        "signal_date": f"eq.{TODAY}", "limit": "5",
    })
    if rows:
        log_alert("unified_engine", "ok",
                  f"engine_signals has {len(rows)}+ signals for today")
    else:
        has_errors = True
        log_alert("unified_engine", "error",
                  "No engine_signals for today — pipeline may have failed")

    # Check 3: signal_log has today's data (actual column: signal_date)
    rows = supabase_query("signal_log", "ticker", {
        "signal_date": f"eq.{TODAY}", "limit": "5",
    })
    if rows:
        log_alert("signal_logging", "ok",
                  f"signal_log has {len(rows)}+ entries for today")
    else:
        has_errors = True
        log_alert("signal_logging", "error",
                  "No signal_log entries for today — feedback loop not recording")

    # Check 4: Local output file
    if os.path.exists("cef_signals_output.json"):
        size = os.path.getsize("cef_signals_output.json")
        if size > 1000:
            log_alert("output_file", "ok", f"JSON output exists ({size:,} bytes)")
        else:
            has_errors = True
            log_alert("output_file", "warning",
                      f"JSON output suspiciously small ({size} bytes)")
    else:
        has_errors = True
        log_alert("output_file", "error", "cef_signals_output.json not found")

    # Check 5: Signal distribution sanity
    rows = supabase_query("engine_signals", "final_signal", {
        "signal_date": f"eq.{TODAY}",
    })
    if rows:
        from collections import Counter
        dist = Counter(r["final_signal"] for r in rows)
        hold_pct = dist.get("HOLD", 0) / len(rows) * 100 if rows else 0

        if hold_pct > 90:
            log_alert("signal_distribution", "warning",
                      f"HOLD compression: {hold_pct:.0f}% HOLD — "
                      f"reweight fix may not be applied. {dict(dist)}")
        else:
            log_alert("signal_distribution", "ok",
                      f"Distribution healthy: {dict(dist)}")

    # Check 6: Outcome tracker has rows (feedback loop accumulating)
    rows = supabase_query("outcome_tracker", "id", {"limit": "1"})
    if rows:
        unfilled = supabase_query("outcome_tracker", "id", {
            "all_horizons_filled": "eq.false", "limit": "1000",
        })
        log_alert("outcome_tracker", "ok",
                  f"Outcome tracker active, {len(unfilled)} open signals being tracked")
    else:
        log_alert("outcome_tracker", "warning",
                  "No outcome_tracker rows — feedback loop has no data yet")

    print("-" * 50)
    if has_errors:
        print("PIPELINE HAS ISSUES — check alerts above")
        sys.exit(1)
    else:
        print("All checks passed.")


if __name__ == "__main__":
    main()
