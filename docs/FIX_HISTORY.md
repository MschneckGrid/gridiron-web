# Fix history & lessons learned

## Watson Institute performance fee bug (Q4 2025)

**Client:** Watson Institute, client_id 27
**Problem:** Double-counting bug involving `fee_payment` withdrawal types caused inflated performance fee calculations in Q4 2025 invoicing.
**Root cause:** The `fee_payment` withdrawal type was being included in gain calculations and flow weighting, effectively double-counting the fee deduction.
**Fix:** 5 code changes across HWM calculation logic, crystallization check passes, `accountGainSinceHWM` loop, invoice display, and fee payment withdrawal exclusion.
**Lesson:** Fix code first → deploy → let Statement tab recalculate and auto-save. Don't manually correct Supabase values before the code fix is live.

## Comp Planner inflation bug

**Problem:** Mike + Mandy Total row was inflated.
**Root cause:** `msPlusMandy` and `dkTotal` incorrectly included auto allowance values.
**Fix:** Simplified both to pure salary sums.

## RLS 42501 error

**Problem:** Cascading write failures on downstream tables when triggers fired.
**Root cause:** Trigger functions (`calc_returns_trigger`, `calc_composite_return_trigger`, `check_large_flow`) were set to `SECURITY INVOKER`. When an authenticated user wrote to `monthly_data`, the trigger tried to write to downstream tables — but the user's role didn't have write policies on those tables.
**Fix:** `ALTER FUNCTION ... SECURITY DEFINER` on all three trigger functions.
**Lesson:** Always use `SECURITY DEFINER` for trigger functions that write to downstream tables.

## Parser overwrite protection

**Problem:** `statement_watcher.py` was overwriting manually corrected `contributions` and `withdrawals` values with zero on re-runs.
**Root cause:** The upsert logic didn't check for existing non-zero values before writing zeros from the parser.
**Fix:** Added `or (existing.get(...) if existing else 0)` fallback guard in `upsert_monthly_data`.
**Detection signature:** Contributions zeroed out while `contribution_day` remains set = parser re-run, not user deletion.

## Goldman statement parsing (Steamfitters/Plumbers)

**Problem:** JATC and SMW 33 Goldman statements parsed correctly, but Plumbers and Steamfitters did not.
**Root cause:** Client name matching in the parser didn't match the Goldman statement format for these specific accounts.
**Fix:** Updated client name matching patterns in the parser.

## Cash flow direction errors

**Lesson:** When correcting cash flow direction errors, BOTH the amount field AND the day field for the incorrect direction must be explicitly zeroed out. Setting only the correct direction's fields leaves stale values that cause calculation errors.

## Dashboard login freeze

**Problem:** Dashboard froze on login attempt.
**Root cause:** Authentication flow timing issue with `checkExistingSession` and React state initialization.
**Fix:** Addressed token readiness sequencing in the main App component.
