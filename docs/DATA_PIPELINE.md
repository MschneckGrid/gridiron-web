# Data pipeline (local scripts)

Both scripts run on Mike's Windows machine via Task Scheduler and write to the shared Supabase instance.

## statement_watcher.py

**Location:** `C:\` (exact path in gridiron-statement-watcher repo)
**Schedule:** Task Scheduler, monitors OneDrive client folders
**Purpose:** Parses custodian PDF monthly statements into `monthly_data` table

### Supported custodians

| Custodian | Format | Notes |
|-----------|--------|-------|
| PNC | PDF | Standard text extraction |
| Goldman Sachs | PDF | Multiple account formats (JATC, SMW 33 parse correctly; Plumbers/Steamfitters needed client name matching fix) |
| BNY Mellon | PDF | Standard |
| Pershing | PDF | Standard |
| Charles Schwab | PDF | Standard |
| US Bank | PDF | Standard |
| Somerset Trust (STC) | PDF | Standard |
| FNB | PDF | Standard |
| Velocity GS | PDF | Goldman sub-brand |

Some statements require Tesseract OCR for image-based PDFs.

### Parser overwrite protection

**Problem (resolved):** `statement_watcher.py` was overwriting manually corrected `contributions` and `withdrawals` values with zero on re-runs.

**Fix:** Added `or (existing.get(...) if existing else 0)` fallback guard in `upsert_monthly_data` for both fields on the non-fee client path. This preserves manually entered values when the parser re-runs and finds zero.

### Parser re-run detection

**Signature:** Contributions zeroed out while `contribution_day` remains set = parser re-run overwrote the value, not a user deletion. Check `updated_at` to confirm.

## gridiron_recon_sync.py

**Location:** `C:\ReconFiles\`
**Log:** `C:\ReconFiles\sync_log.txt`
**Schedule:** Task Scheduler, hourly during business hours
**Purpose:** Pulls daily reconciliation Excel files from Outlook emails → parses holdings → upserts to `holdings_matrix` table

### How it works

1. Scans Outlook Inbox via COM automation for emails with recon Excel attachments
2. Extracts holdings data (ticker, shares, market value) per client account
3. Upserts to `holdings_matrix` with the `recon_date` from the file

### Debugging pattern

Check `recon_date` vs `updated_at` separately — the sync may succeed while writing data with an older `recon_date` that doesn't match the dashboard's date filter. If the email is deleted before the script runs, the data won't sync.
