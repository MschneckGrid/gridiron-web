# Dashboard tabs & architecture

File: `gridiron-dashboard-with-fasttrack.html` (~10,000 lines)
Hosted: Netlify via `MschneckGrid/gridiron-web`

## Tab overview

| Tab | Purpose | Key behavior |
|-----|---------|-------------|
| Firm Data | AUM summary, client counts by category, Form ADV data | Aggregates from monthly_data, respects overlap rules for client counts |
| Statement | Monthly client statement with balances, flows, returns | **Recalculates on load** — overwrites stored computed fields in Supabase |
| Fact Sheet | Client-facing performance summary | Pulls from monthly_data and composite_returns |
| Disclosures | Regulatory disclosure text | Static text blocks with date parameters |
| Invoice | Fee invoice generation per client/quarter | Pulls from fee_schedule, monthly_data; generates invoice records |
| Data Entry | Manual entry of monthly data points | Upserts to monthly_data table |
| Performance Fees | Performance fee calculations with HWM tracking | Complex: HWM balance, crystallization, spread over hurdle |
| Composite Reporting | GIPS-compliant composite reports | Reads composite_returns, composite_portfolio_details |
| Composite Data | Raw composite membership and return data | Admin view of composite assignments |
| Holdings Matrix | Cross-client position grid by ticker | Reads from holdings_matrix table (populated by recon_sync.py) |
| Comp Planner | Partner compensation planning | Quarterly fee projections, salary, auto allowance splits |

## Statement tab recalculation (critical)

The Statement tab is **not read-only**. When a client/month is loaded:
1. It reads raw inputs from `monthly_data` (beginning_balance, ending_balance, contributions, withdrawals, management_fee, etc.)
2. It recalculates derived fields: `gross_return_pct`, `net_return_pct`, `investment_gain`, `account_gain_since_hwm`, `hwm_balance`, `spread_over_hurdle`
3. It **auto-saves** these recalculated values back to Supabase

This means: if you manually correct a computed field in Supabase without fixing the calculation logic in the code, the Statement tab will overwrite your correction the next time someone views that client/month.

**Rule: Fix code first → deploy → let Statement recalculate.**

## Data validation banner

A Supabase view `data_validation_flags` checks all active clients from 2023 onward for 7 flag types:
- Net return > gross return
- Large return (>15% or < -15%)
- Missing flow (balance changed >$5K with no contribution/withdrawal)
- Contribution day set but amount is zero
- Withdrawal day set but amount is zero
- Gross return stored but net is NULL
- Zero gross return with non-zero balance change

Dashboard displays flagged client/month chips on login, clickable to navigate to Statement tab.

Query: `supabaseFetch('data_validation_flags', '?order=month_end_date.desc&limit=50')`

## Authentication

Supabase Auth with email/password. Token stored in state, passed to all `supabaseFetch` calls. `checkExistingSession` runs on mount to restore sessions.
