# Supabase patterns & gotchas

Project ID: `nzinvxticgyjobkqxxhl` · Org ID: `dbutqecscvbguljulzxy` · Pro plan

## Critical rules

**`execute_sql` silently fails on UPDATEs when RLS is active.** No error is returned but `updated_at` timestamps remain unchanged. Use `apply_migration` with a unique migration name for all direct data correction UPDATEs — it runs under the migration role and bypasses RLS.

**Always verify writes.** Follow any UPDATE with a SELECT on the same WHERE clause checking `updated_at` to confirm persistence.

**Trigger functions must use `SECURITY DEFINER`.** Functions set to `SECURITY INVOKER` fail on cascading writes to downstream tables with RLS. This caused the RLS 42501 error — `calc_returns_trigger`, `calc_composite_return_trigger`, and `check_large_flow` all needed `SECURITY DEFINER`.

**`pg_policies` queries must include the `roles` column.** `{public}` and `{anon, authenticated}` behave differently in Supabase even though PostgreSQL treats `public` as inherited.

**RLS diagnosis pattern:** Check `pg_tables` for `rowsecurity = true`, then `pg_policies` for that table — if only a SELECT `cmd` policy exists, writes are blocked. Batch remediation across all under-protected tables at once rather than one at a time.

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
| `cef_daily` | CEF daily price/NAV data |
| `cef_tickers` | CEF universe metadata |
| `cef_zscore_cache` | Pre-calculated z-scores |
| `cef_splits` | Stock split records with split_factor |
| `cef_distributions` | Distribution/dividend records |
| `bdc_daily` | BDC daily price/NAV data |
| `equity_income_daily` | Equity income (MLPs, REITs) daily data |
| `schwab_custody` | Schwab custody account data |

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

## pg_cron

pg_cron v1.6.4 is available. Jobs are scheduled in UTC. Check existing jobs:
```sql
SELECT jobid, jobname, schedule, LEFT(command, 80) FROM cron.job ORDER BY jobid;
```
