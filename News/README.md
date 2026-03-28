# CEF News Scanner

**Daily material event monitor for closed-end funds.**  
Feeds the Command Center's **News** tab with merger, management change, distribution, activist, and other material events.

---

## Architecture

```
                    ┌──────────────────┐
                    │   cef_news_      │
  SEC EDGAR ──────▶ │   scanner.py     │ ──────▶  Supabase
  (8-K, N-14,      │                  │          cef_news_alerts
   DEF 14A,        │  classify +      │
   SC 13D,         │  score           │
   Form 4)         │  materiality     │
                    │                  │
  Finnhub   ──────▶ │                  │ ──────▶  Command Center
  (company news)    └──────────────────┘          News Tab (live)
```

**Two sources → one pipeline → one table → one UI.**

---

## Files

| File | Purpose |
|------|---------|
| `cef_news_scanner.py` | Main Python scanner (SEC EDGAR + Finnhub) |
| `001_create_cef_news_alerts.sql` | Supabase migration — run once |
| `command_center_news_patch.js` | 4-part patch for Command Center HTML |
| `setup_cron.sh` | Installs daily cron job |
| `.env.example` | Environment variable template |

---

## Setup

### 1. Create the Supabase table

Run `001_create_cef_news_alerts.sql` in your Supabase SQL editor.  
This creates the `cef_news_alerts` table with indexes, RLS policies, and an auto-updating `updated_at` trigger.

### 2. Get API keys

- **Finnhub**: Free account at [finnhub.io](https://finnhub.io). The free tier gives 60 calls/min — plenty for ~170 tickers daily.
- **Supabase Service Key**: Found in your Supabase project → Settings → API → `service_role` key. This key bypasses RLS for inserts.

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your actual keys
```

### 4. Install Python dependencies

```bash
pip install requests
```

That's it — `requests` is the only external dependency.

### 5. Test with a dry run

```bash
# Single ticker, verbose, no database writes
python cef_news_scanner.py --ticker UTF --dry-run -v

# Full universe, dry run
python cef_news_scanner.py --dry-run
```

### 6. Run for real

```bash
# Full scan, writes to Supabase
python cef_news_scanner.py

# Save summary to JSON
python cef_news_scanner.py --output scan_results.json
```

### 7. Patch the Command Center

Open `cef_command_center.html` and apply the 4 patches described in `command_center_news_patch.js`:

1. **CSS** — paste after existing alert styles (~line 246)
2. **Tab button** — paste before the "Model Portfolio" tab (~line 398)
3. **HTML panel** — paste between `panel-engine-alerts` and `panel-engine-model` (~line 646)
4. **JavaScript** — paste before the closing `</script>` tag (~line 3540)

### 8. Set up daily cron

```bash
chmod +x setup_cron.sh
./setup_cron.sh
```

This installs:
- **Weekdays 7:00 AM ET**: 2-day lookback scan
- **Sundays 8:00 AM ET** (optional): 7-day deep scan

---

## Event Types

| Type | Examples | Base Materiality |
|------|----------|:---:|
| `MERGER` | Fund merging, reorganization, conversion | 90 |
| `TERMINATION` | Fund liquidation, wind-down | 90 |
| `TENDER_OFFER` | Share repurchase at discount to NAV | 85 |
| `ACTIVIST` | SC 13D filing, proxy contest, board seat | 85 |
| `RIGHTS_OFFERING` | Transferable rights, subscription rights | 80 |
| `MGMT_CHANGE` | New portfolio manager, advisor change | 80 |
| `DISTRIBUTION` | Distribution change, special distribution | 75 |
| `REGULATORY` | SEC action, exemptive order | 75 |
| `BUYBACK` | Open market share repurchase | 70 |
| `IPO_OFFERING` | Secondary offering, shelf registration | 70 |
| `LEVERAGE` | Credit facility changes, preferred shares | 65 |
| `EARNINGS` | Shareholder report, financial results | 55 |
| `OTHER` | Unclassified news | 40 |

Materiality scores are boosted +5 when the keyword appears in the headline (vs. only in body text).

---

## Command Center UI

The News tab provides:

- **Stats bar**: Total alerts, high-priority count, ticker coverage, source breakdown
- **Filters**: Event type, priority level, source, date range, free-text search
- **Date-grouped list**: Items grouped by date with color-coded event badges
- **Direct links**: Every item links to the source article or SEC filing
- **Review workflow**: Mark items as reviewed (☑) or dismissed (✕)
- **Badge count**: Tab shows high-priority alert count

---

## CLI Reference

```
usage: cef_news_scanner.py [-h] [--ticker TICKER] [--days DAYS]
                           [--dry-run] [--verbose] [--output OUTPUT]

  --ticker, -t   Scan a single ticker (e.g., UTF)
  --days, -d     Lookback days (default: 2)
  --dry-run      Print results without writing to Supabase
  --verbose, -v  Verbose/debug logging
  --output, -o   Save summary to JSON file
```

---

## Future Enhancements

- **Google News RSS**: Free fallback source for broader coverage
- **AI summarization**: Use Claude API to generate plain-English summaries of 8-K filings
- **Borrower cross-reference**: For BDCs, monitor news on portfolio companies (leverages SOI data)
- **Email digest**: Daily email with high-priority news items
- **Slack webhook**: Real-time alerts for critical events (merger, termination)
