# Supabase patterns & gotchas

Project ID: `nzinvxticgyjobkqxxhl` · Org ID: `dbutqecscvbguljulzxy` · Pro plan

## Critical rules

**`execute_sql` silently fails on UPDATEs when RLS is active.** No error is returned but `updated_at` timestamps remain unchanged. Use `apply_migration` with a unique migration name for all direct data correction UPDATEs — it runs under the migration role and bypasses RLS.

**Always verify writes.** Follow any UPDATE with a SELECT on the same WHERE clause checking `updated_at` to confirm persistence.

**Trigger functions must use `SECURITY DEFINER`.** Functions set to `SECURITY INVOKER` fail on cascading writes to downstream tables with RLS. This caused the RLS 42501 error — `calc_returns_trigger`, `calc_composite_return_trigger`, and `check_large_flow` all needed `SECURITY DEFINER`.

**`pg_policies` queries must include the `roles` column.** `{public}` and `{anon, authenticated}` behave differently in Supabase even though PostgreSQL treats `public` as inherited.

**RLS diagnosis pattern:** Check `pg_tables` for `rowsecurity = true`, then `pg_policies` for that table — if only a SELECT `cmd` policy exists, writes are blocked. Batch remediation across all under-protected tables at once rather than one at a time.

**PostgREST returns numeric view columns as strings.** All numeric fields from views require explicit `parseFloat()` in JavaScript before arithmetic or `.toFixed()` calls.

**PostgREST batch upserts require identical key sets.** Rows with different column shapes in a single batch cause null injection into existing non-null values. Fix: split into separate upsert groups per key shape.

**Materialized views must be explicitly refreshed.** Writes to underlying tables do not trigger auto-refresh. Pattern: `SECURITY DEFINER` RPC function called after data writes.

## Column and table discovery

Always run `information_schema.columns` before constructing detail queries on unfamiliar tables:
```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'your_table' ORDER BY ordinal_position;
```

For trigger inspection:
```sql
SELECT prosrc FROM pg_proc WHERE proname = 'trigger_name';
SELECT * FROM information_schema.triggers WHERE event_object_table = 'monthly_data';
```

## Key tables (shared across all projects)

### Client & performance
| Table | Purpose |
|-------|---------|
| `clients` | Master client list with categories, fee types |
| `monthly_data` | Monthly performance, balances, flows |
| `holdings_matrix` | Daily position snapshots from recon |
| `composite_returns` | Composite-level return calculations |
| `composite_portfolio_details` | Client-to-composite mappings |
| `index_returns` | Benchmark index returns |
| `invoices` | Fee invoice records |
| `high_water_marks` | HWM tracking for performance fees |
| `fee_schedule` | Client fee structures |
| `account_performance_daily` | Modified Dietz DTD/MTD/YTD returns |
| `schwab_custody` | Schwab custody account data |

### CEF / signal engine
| Table | Purpose |
|-------|---------|
| `cef_daily` | CEF daily price/NAV data |
| `cef_tickers` | CEF universe metadata (incl. canonical sector assignment) |
| `cef_zscore_cache` | Pre-calculated z-scores |
| `cef_splits` | Stock split records with split_factor |
| `cef_distributions` | Distribution/dividend records |
| `cef_fund_info` | Sponsor, leverage, expense ratio, distribution rate |
| `cef_nport_data` | SEC N-PORT filing data |
| `signal_log` | Every signal with full context (asset_class, sector fields) |
| `outcome_tracker` | Forward returns at 30/60/90/180d horizons |
| `parameter_history` | Versioned parameter snapshots per asset class |

### BDC / equity income
| Table | Purpose |
|-------|---------|
| `bdc_daily` | BDC daily price/NAV data |
| `equity_income_daily` | Equity income (MLPs, REITs) daily data |
| `cef_bdc_8k_events` | SEC 8-K filings parsed for material events |
| `cef_bdc_sentiment_summary` | Sentiment analysis on BDC news |

### Monthly letter / macro
| Table | Purpose |
|-------|---------|
| `macro_indicators` | Latest FRED data (HY spreads, 10Y yield, VIX, CPI, etc.) with direction |
| `macro_news_log` | Daily scraped news/macro events, relevance-tagged per CEF sector |
| `sector_writeups` | Monthly AI-generated verdict + narrative per sector, approval workflow |
| `macro_regime_snapshots` | Periodic cycle label snapshot (EARLY/MID/LATE/STRESSED) |
| `cef_news_alerts` | News alerts from cef_news_scanner.py |

## Key views and functions

| Object | Type | Purpose |
|--------|------|---------|
| `data_validation_flags` | View | 7 flag types across all active clients from 2023+ |
| `cef_daily_clean` | View | Split-adjusted, distribution-adjusted CEF data |
| `cef_latest_with_zscores` | Mat View | Latest price/NAV/discount with z-scores per ticker |
| `cef_monthly_discounts` | View | Monthly aggregated discount data |
| `calc_returns_trigger` | Function | Triggers on monthly_data changes |
| `calc_composite_return_trigger` | Function | Composite return recalculation |
| `refresh_zscore_cache` | Function | Refreshes cef_zscore_cache from cef_daily_clean |
| `detect_splits` | Function | Auto-detects stock splits from price jumps |
| `cef_daily_post_load` | Function | Chains: detect splits → refresh mat view → refresh cache |

## Edge Functions

| Function | Purpose | Auth |
|----------|---------|------|
| `generate-sector-writeup` | Claude API proxy for monthly letter. Voice spec baked into system prompt. | `ANTHROPIC_API_KEY` secret |
| `run-signal-engine` | Daily signal generation | Cron secret |
| `detect-manager-trades` | David trade detection → Telegram | Telegram token |
| `telegram-poller` | Polls Telegram for David's responses | Telegram token |
| `compute-account-performance` | Modified Dietz returns | Cron secret |

## pg_cron

pg_cron v1.6.4 is available. Jobs are scheduled in UTC. Check existing jobs:
```sql
SELECT jobid, jobname, schedule, LEFT(command, 80) FROM cron.job ORDER BY jobid;
```
