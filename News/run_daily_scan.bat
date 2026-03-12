@echo off
REM ============================================================
REM CEF News Scanner — Daily Automated Run
REM ============================================================
REM Scheduled via Windows Task Scheduler to run every weekday.
REM ============================================================

cd /d "C:\Users\Mike\OneDrive - Gridiron Partners\Gridiron Partners - Shared Documents\Reporting\News"

REM Set API keys
set SUPABASE_URL=https://nzinvxticgyjobkqxxhl.supabase.co
set set SUPABASE_SERVICE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im56aW52eHRpY2d5am9ia3F4eGhsIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODk1NTA5MiwiZXhwIjoyMDg0NTMxMDkyfQ.IEgJeC2WcBq1kY7h25cuQLl1J3qs8HsZetErt3ADKC8
set FINNHUB_API_KEY=d6j3ne9r01ql467i4v6gd6j3ne9r01ql467i4v70

REM Create logs folder if it doesn't exist
if not exist "logs" mkdir logs

REM Run the scanner with 2-day lookback, log output
echo [%date% %time%] Starting CEF News Scan... >> logs\scanner_%date:~-4%%date:~4,2%%date:~7,2%.log
python cef_news_scanner.py --days 2 >> logs\scanner_%date:~-4%%date:~4,2%%date:~7,2%.log 2>&1
echo [%date% %time%] Scan complete. >> logs\scanner_%date:~-4%%date:~4,2%%date:~7,2%.log
