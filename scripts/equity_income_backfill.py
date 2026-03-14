"""
equity_income_backfill.py
==========================
Historical backfill of MLP and REIT daily price/volume data from FastTrack
into the Supabase equity_income_daily table.

Usage:
    python equity_income_backfill.py                  # full 2016-present
    python equity_income_backfill.py --start 2020-01-01
    python equity_income_backfill.py --asset-class MLP
    python equity_income_backfill.py --asset-class REIT
    python equity_income_backfill.py --dry-run        # print what would be done

Place in same folder as other Gridiron scripts alongside .env file.
"""

import os, sys, time, json, argparse, requests
from datetime import date, datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv

# ── Load env ─────────────────────────────────────────────────────────────────
load_dotenv()
SUPABASE_URL   = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY   = os.getenv("SUPABASE_SERVICE_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
FT_API_KEY     = os.getenv("FASTTRACK_API_KEY", "")
FT_BASE        = "https://api.fasttrack.net/v2"

REQUIRED = {"SUPABASE_URL": SUPABASE_URL, "SUPABASE_KEY": SUPABASE_KEY, "FASTTRACK_API_KEY": FT_API_KEY}

# ── Constants ─────────────────────────────────────────────────────────────────
BATCH_SIZE      = 25     # tickers per FastTrack request (credit-efficient)
INTER_BATCH_DELAY = 1.5  # seconds between batches (respect rate limit)
UPSERT_CHUNK    = 500    # rows per Supabase upsert call
DEFAULT_START   = "2016-01-01"

# ── Supabase helpers ──────────────────────────────────────────────────────────
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }

def sb_get(path, params=None):
    r = requests.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=sb_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def sb_upsert(table, rows):
    """Upsert rows in chunks. Returns (inserted_count, error_count)."""
    inserted = 0
    errors = 0
    for i in range(0, len(rows), UPSERT_CHUNK):
        chunk = rows[i:i+UPSERT_CHUNK]
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/{table}",
                headers={**sb_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
                params={"on_conflict": "ticker,trade_date"},
                json=chunk,
                timeout=60
            )
            if r.status_code in (200, 201, 204):
                inserted += len(chunk)
            else:
                print(f"  [WARN] Upsert chunk error {r.status_code}: {r.text[:200]}")
                errors += len(chunk)
        except Exception as e:
            print(f"  [WARN] Upsert exception: {e}")
            errors += len(chunk)
    return inserted, errors

# ── FastTrack helpers ─────────────────────────────────────────────────────────
def ft_authenticate():
    print("Authenticating with FastTrack...")
    r = requests.post(f"{FT_BASE}/auth", headers={"x-api-key": FT_API_KEY, "Content-Type": "application/json"}, timeout=30)
    if not r.ok:
        raise RuntimeError(f"FT auth failed {r.status_code}: {r.text[:200]}")
    token = r.json().get("id_token")
    print("  FastTrack authenticated OK")
    return token

def trading_days(start_date, end_date):
    """Generate list of weekday date strings between start and end (inclusive)."""
    days = []
    cur = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    while cur <= end:
        if cur.weekday() < 5:  # Mon–Fri
            days.append(cur.isoformat())
        cur += timedelta(days=1)
    return days


def ft_fetch_range(token, tickers, start_date, end_date):
    """
    Fetch daily prices + volume for tickers over a date range.
    Returns: {ticker: [(date_str, price, volume), ...], ...}
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "assets": tickers,
        "include": ["prices", "volume"],
        "start_date": start_date,
        "end_date": end_date,
        "frequency": "daily"
    }
    r = requests.post(f"{FT_BASE}/data", headers=headers, json=payload, timeout=120)

    if r.status_code == 429:
        raise RuntimeError("FT_RATE_LIMIT")
    if not r.ok:
        raise RuntimeError(f"FT data error {r.status_code}: {r.text[:300]}")

    data = r.json()

    if getattr(ft_fetch_range, '_debug', False):
        print(f"\n  [DEBUG] Top-level keys: {list(data.keys())}")
        print(f"  [DEBUG] Raw (first 1500 chars):\n{json.dumps(data)[:1500]}\n")

    results = data.get("results") or data.get("datarange") or data.get("data") or []

    # FastTrack /data does NOT return a dates array — prices are ordered weekday-only
    # from start_date to end_date. Build the calendar ourselves to match by index.
    dates = data.get("dates") or []
    if not dates:
        # Check all top-level keys for a date-string list
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 1 and isinstance(v[0], str) and len(v[0]) == 10 and "-" in v[0]:
                dates = v
                if getattr(ft_fetch_range, '_debug', False):
                    print(f"  [DEBUG] Found dates under key '{k}': {dates[:3]}")
                break
    if not dates:
        # Fall back to generating weekday calendar for the range
        dates = trading_days(start_date, end_date)
        if getattr(ft_fetch_range, '_debug', False):
            print(f"  [DEBUG] Generated {len(dates)} trading days: {dates[:3]} … {dates[-3:]}")

    output = {}
    for item in results:
        ticker = item.get("ticker")
        if not ticker or item.get("error"):
            continue

        if getattr(ft_fetch_range, '_debug', False) and not output:
            print(f"  [DEBUG] First ticker '{ticker}' keys: {list(item.keys())}")
            for k, v in item.items():
                snippet = json.dumps(v)[:120] if not isinstance(v, str) else v[:120]
                print(f"    {k}: {snippet}")

        prices = item.get("prices") or item.get("price") or []
        volumes = item.get("volume") or item.get("volumes") or []

        # Handle dict-keyed responses: {"2026-01-02": 45.23, ...}
        if isinstance(prices, dict):
            for dt, price in prices.items():
                if price and price > 0:
                    vol = volumes.get(dt) if isinstance(volumes, dict) else None
                    vol_int = int(vol) if vol and vol > 0 else None
                    output.setdefault(ticker, []).append((dt, round(float(price), 4), vol_int))
            continue

        if not prices:
            continue

        # Prices array may be shorter than the full calendar (data gaps, new listings)
        # Align from the END — the last price corresponds to end_date
        rows = []
        offset = len(dates) - len(prices)  # how many leading dates to skip
        for i, price in enumerate(prices):
            if price is None or price <= 0:
                continue
            date_idx = offset + i
            if date_idx < 0 or date_idx >= len(dates):
                continue
            trade_date = dates[date_idx]
            vol = volumes[i] if isinstance(volumes, list) and i < len(volumes) else None
            vol_int = int(vol) if vol and vol > 0 else None
            rows.append((trade_date, round(float(price), 4), vol_int))

        if rows:
            output[ticker] = rows

    return output, dates

# ── Existing data check ───────────────────────────────────────────────────────
def get_existing_dates(tickers):
    """
    Returns a set of (ticker, date_str) tuples already in equity_income_daily.
    Efficient: queries all tickers at once.
    """
    print("Checking existing data in equity_income_daily...")
    existing = set()
    # Query in chunks to avoid URL length limits
    for i in range(0, len(tickers), 200):
        chunk = tickers[i:i+200]
        ticker_filter = ",".join(f'"{t}"' for t in chunk)
        try:
            rows = sb_get(
                "equity_income_daily",
                params={
                    "select": "ticker,trade_date",
                    "ticker": f"in.({','.join(chunk)})"
                }
            )
            for row in rows:
                existing.add((row["ticker"], row["trade_date"]))
        except Exception as e:
            print(f"  [WARN] Could not check existing for chunk: {e}")
    print(f"  Found {len(existing):,} existing (ticker, date) pairs")
    return existing

# ── Main backfill logic ───────────────────────────────────────────────────────
def run_backfill(args):
    # Validate env
    for name, val in REQUIRED.items():
        if not val:
            print(f"[ERROR] {name} not set in .env")
            sys.exit(1)

    start_date = args.start
    end_date = date.today().isoformat()
    asset_class_filter = args.asset_class  # MLP, REIT, or None (all)

    print(f"\n{'='*60}")
    print(f"Gridiron - Equity Income Historical Backfill")
    print(f"  Date range:   {start_date} → {end_date}")
    print(f"  Asset class:  {asset_class_filter or 'ALL (MLP + REIT)'}")
    print(f"  Dry run:      {args.dry_run}")
    print(f"{'='*60}\n")

    # Fetch active tickers from Supabase
    params = {"status": "eq.active", "select": "ticker,asset_class,sector"}
    if asset_class_filter:
        params["asset_class"] = f"eq.{asset_class_filter}"
    all_tickers_data = sb_get("equity_income_tickers", params=params)
    tickers = [t["ticker"] for t in all_tickers_data]
    print(f"Universe: {len(tickers)} active tickers")
    for ac, count in [(k, sum(1 for t in all_tickers_data if t["asset_class"] == k))
                       for k in ["MLP", "REIT"]]:
        if count:
            print(f"  {ac}: {count}")

    if not tickers:
        print("[ERROR] No tickers found.")
        sys.exit(1)

    if args.dry_run:
        print(f"\n[DRY RUN] Would backfill {len(tickers)} tickers from {start_date} to {end_date}")
        print(f"  Batches of {BATCH_SIZE}: {len(tickers) // BATCH_SIZE + 1} batches")
        estimated_rows = len(tickers) * 2500  # ~10 years of trading days
        print(f"  Estimated rows: ~{estimated_rows:,}")
        print(f"  FastTrack credits: ~{len(tickers) // BATCH_SIZE + 1} calls")
        return

    # Check what already exists to enable resume
    existing = get_existing_dates(tickers)

    # Authenticate with FastTrack
    ft_token = ft_authenticate()
    token_refresh_time = time.time()
    TOKEN_LIFETIME = 3500  # ~58 minutes (typical JWT lifetime)

    # Process in batches
    total_rows_written = 0
    total_rows_skipped = 0
    total_errors = 0

    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    print(f"\nProcessing {len(batches)} batches of up to {BATCH_SIZE} tickers...\n")

    for batch_num, batch in enumerate(batches, 1):
        print(f"Batch {batch_num}/{len(batches)}: {batch} ...")

        # Refresh token if needed
        if time.time() - token_refresh_time > TOKEN_LIFETIME:
            print("  Refreshing FastTrack token...")
            ft_token = ft_authenticate()
            token_refresh_time = time.time()

        # Fetch data from FastTrack
        retry_count = 0
        ticker_data = {}
        while retry_count < 3:
            try:
                ticker_data, all_dates = ft_fetch_range(ft_token, batch, start_date, end_date)
                print(f"  Got data for {len(ticker_data)}/{len(batch)} tickers, {len(all_dates)} dates")
                break
            except RuntimeError as e:
                if "FT_RATE_LIMIT" in str(e):
                    print(f"  Rate limit hit. Waiting 20 minutes...")
                    time.sleep(1200)
                    ft_token = ft_authenticate()
                    token_refresh_time = time.time()
                    retry_count += 1
                else:
                    print(f"  [ERROR] {e}")
                    total_errors += len(batch)
                    break
            except Exception as e:
                print(f"  [ERROR] Unexpected: {e}")
                total_errors += len(batch)
                break

        if not ticker_data:
            print(f"  No data returned for batch {batch_num}, skipping.")
            if batch_num < len(batches):
                time.sleep(INTER_BATCH_DELAY)
            continue

        # Build rows to insert, skipping already-existing (ticker, date) pairs
        rows_to_insert = []
        for ticker, day_rows in ticker_data.items():
            for (trade_date, price, volume) in day_rows:
                if (ticker, trade_date) in existing:
                    total_rows_skipped += 1
                    continue
                rows_to_insert.append({
                    "ticker": ticker,
                    "trade_date": trade_date,
                    "price": price,
                    "volume": volume
                })

        print(f"  New rows: {len(rows_to_insert):,}  |  Already existed: {total_rows_skipped:,}")

        if rows_to_insert and not args.dry_run:
            inserted, errs = sb_upsert("equity_income_daily", rows_to_insert)
            total_rows_written += inserted
            total_errors += errs
            # Add newly inserted to existing set to prevent re-inserting in later batches
            for row in rows_to_insert:
                existing.add((row["ticker"], row["trade_date"]))
            print(f"  Wrote: {inserted:,}  Errors: {errs}")
        elif rows_to_insert:
            print(f"  [DRY RUN] Would write {len(rows_to_insert):,} rows")

        if batch_num < len(batches):
            time.sleep(INTER_BATCH_DELAY)

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"BACKFILL COMPLETE")
    print(f"  Rows written:  {total_rows_written:,}")
    print(f"  Rows skipped:  {total_rows_skipped:,} (already existed)")
    print(f"  Errors:        {total_errors}")
    print(f"{'='*60}\n")

    if total_rows_written > 0:
        print("Triggering z-score refresh in Supabase...")
        try:
            r = requests.post(
                f"{SUPABASE_URL}/rest/v1/rpc/refresh_equity_income_zscore_cache",
                headers=sb_headers(),
                json={},
                timeout=60
            )
            if r.ok:
                print(f"  Z-score refresh: {r.json()}")
            else:
                print(f"  Z-score refresh error: {r.status_code} {r.text[:200]}")
        except Exception as e:
            print(f"  Z-score refresh exception: {e}")
            print("  (Run manually in Supabase SQL Editor: SELECT refresh_equity_income_zscore_cache();)")

    print("\nDone.")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Equity Income (MLP/REIT) historical backfill from FastTrack")
    parser.add_argument("--start", default=DEFAULT_START, help=f"Start date YYYY-MM-DD (default: {DEFAULT_START})")
    parser.add_argument("--end",   default=None,          help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--asset-class", choices=["MLP", "REIT"], default=None, help="Limit to one asset class")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be done without writing")
    parser.add_argument("--debug",   action="store_true", help="Print raw FastTrack response for first batch")
    args = parser.parse_args()

    if args.debug:
        ft_fetch_range._debug = True

    if args.end:
        # Override end date if provided
        end_override = args.end
        original_run = run_backfill
        def run_backfill_with_end(a):
            import functools
            a.end = end_override
            original_run(a)
        run_backfill = run_backfill_with_end

    run_backfill(args)
