#!/usr/bin/env python3
"""
log_signals.py — Persist engine signals to signal_log for feedback loop
========================================================================
Matches actual signal_log schema:
  ticker, asset_class, sector, signal_date, signal,
  composite_score, adjusted_score, confidence,
  price_at_signal, nav_at_signal, discount_at_signal,
  layer_scores, veto_triggers, risk_penalties, key_metrics,
  bull_case, bear_case, key_assumptions, invalidation_triggers,
  parameter_version

Usage:
    python scripts/log_signals.py
    python scripts/log_signals.py custom_output.json

Requires env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY
"""

import json
import os
import sys
import urllib.request
from datetime import date


SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
INPUT_FILE = sys.argv[1] if len(sys.argv) > 1 else "cef_signals_output.json"
TABLE = "signal_log"


def supabase_upsert(table, rows):
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=ticker,signal_date,asset_class"
    data = json.dumps(rows).encode()
    req = urllib.request.Request(url, data=data, method="POST", headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    })
    with urllib.request.urlopen(req) as resp:
        return {"status": resp.status}


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        sys.exit(1)

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        output = json.load(f)

    signals = output.get("all_signals", [])
    today = date.today().isoformat()

    print(f"Logging {len(signals)} signals to signal_log...")

    rows = []
    for sig in signals:
        conviction = sig.get("conviction") or {}
        bull = conviction.get("bull_case", "") if isinstance(conviction, dict) else ""
        bear = conviction.get("bear_case", "") if isinstance(conviction, dict) else ""
        assumptions = conviction.get("key_assumptions", []) if isinstance(conviction, dict) else []
        invalidation = conviction.get("invalidation_triggers", []) if isinstance(conviction, dict) else []

        rows.append({
            "ticker": sig["ticker"],
            "asset_class": "CEF",
            "sector": sig.get("peer_group", ""),
            "signal_date": today,
            "signal": sig["final_signal"],
            "composite_score": sig.get("composite_score"),
            "adjusted_score": sig.get("regime_adjusted_score"),
            "confidence": sig.get("confidence", "LOW"),
            "price_at_signal": sig.get("price"),
            "nav_at_signal": sig.get("nav"),
            "discount_at_signal": sig.get("discount_pct"),
            "layer_scores": json.dumps(sig.get("layer_scores", [])),
            "veto_triggers": json.dumps(sig.get("veto_triggers", [])),
            "risk_penalties": sig.get("risk_penalties", 0),
            "key_metrics": json.dumps({
                "zscore": sig.get("zscore"),
                "peer_percentile": sig.get("peer_percentile"),
                "momentum_score": sig.get("momentum_score"),
                "momentum_confirmed": sig.get("momentum_confirmed"),
                "volume_ratio": sig.get("volume_ratio"),
                "yield_pct": sig.get("yield_pct"),
                "seasonal_score": sig.get("seasonal_score"),
            }),
            "bull_case": bull,
            "bear_case": bear,
            "key_assumptions": json.dumps(assumptions),
            "invalidation_triggers": json.dumps(invalidation),
            "parameter_version": output.get("engine_version", "gridiron_engine v1.0"),
        })

    batch_size = 100
    logged = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            result = supabase_upsert(TABLE, batch)
            logged += len(batch)
            print(f"  Batch {i // batch_size + 1}: {len(batch)} signals -> HTTP {result['status']}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()[:200] if e.fp else ""
            print(f"  Batch {i // batch_size + 1} FAILED: {e.code} - {body}")

    print(f"Done. {logged}/{len(signals)} signals logged.")


if __name__ == "__main__":
    main()
