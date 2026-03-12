-- =============================================================================
-- CEF News Alerts Table — Supabase Migration
-- =============================================================================
-- Stores material news events discovered by the daily news scanner.
-- Sources: SEC EDGAR (8-K, N-14, DEF 14A, SC 13D, Form 4) + Finnhub news API.
-- Consumed by: Command Center "News" tab.
-- =============================================================================

CREATE TABLE IF NOT EXISTS cef_news_alerts (
    id              BIGSERIAL PRIMARY KEY,
    ticker          TEXT NOT NULL,
    event_date      DATE NOT NULL,
    event_type      TEXT NOT NULL,          -- MERGER, MGMT_CHANGE, DISTRIBUTION, TENDER_OFFER,
                                            -- RIGHTS_OFFERING, ACTIVIST, TERMINATION, SEC_FILING,
                                            -- EARNINGS, REGULATORY, BUYBACK, OTHER
    headline        TEXT NOT NULL,
    summary         TEXT,                   -- 1-2 sentence plain-English summary
    source_url      TEXT,                   -- Link to article or filing
    source_name     TEXT,                   -- 'SEC EDGAR', 'Finnhub', 'PR Newswire', etc.
    filing_type     TEXT,                   -- SEC filing type if applicable: '8-K', 'N-14', etc.
    materiality     INTEGER DEFAULT 50      -- 0-100 score; >=70 = high priority
                        CHECK (materiality >= 0 AND materiality <= 100),
    reviewed        BOOLEAN DEFAULT FALSE,  -- Manual flag: analyst has reviewed this alert
    dismissed       BOOLEAN DEFAULT FALSE,  -- Manual flag: not relevant, hide from default view
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Prevent duplicate entries for the same event
CREATE UNIQUE INDEX IF NOT EXISTS idx_news_alerts_dedup
    ON cef_news_alerts (ticker, event_date, source_url);

-- Fast lookups by ticker
CREATE INDEX IF NOT EXISTS idx_news_alerts_ticker
    ON cef_news_alerts (ticker, event_date DESC);

-- Fast lookups by event type
CREATE INDEX IF NOT EXISTS idx_news_alerts_type
    ON cef_news_alerts (event_type, event_date DESC);

-- Fast lookups for high-priority unreviewed items
CREATE INDEX IF NOT EXISTS idx_news_alerts_priority
    ON cef_news_alerts (materiality DESC, reviewed, dismissed)
    WHERE dismissed = FALSE;

-- Date range queries
CREATE INDEX IF NOT EXISTS idx_news_alerts_date
    ON cef_news_alerts (event_date DESC);

-- Auto-update updated_at on row change
CREATE OR REPLACE FUNCTION update_news_alerts_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_news_alerts_updated ON cef_news_alerts;
CREATE TRIGGER trg_news_alerts_updated
    BEFORE UPDATE ON cef_news_alerts
    FOR EACH ROW
    EXECUTE FUNCTION update_news_alerts_timestamp();

-- Enable RLS (Row Level Security) — match your existing pattern
ALTER TABLE cef_news_alerts ENABLE ROW LEVEL SECURITY;

-- Allow authenticated users to read
CREATE POLICY "Allow authenticated read" ON cef_news_alerts
    FOR SELECT TO authenticated USING (TRUE);

-- Allow authenticated users to update (review/dismiss flags)
CREATE POLICY "Allow authenticated update" ON cef_news_alerts
    FOR UPDATE TO authenticated USING (TRUE);

-- Allow service_role (Python scanner) to insert
CREATE POLICY "Allow service insert" ON cef_news_alerts
    FOR INSERT TO authenticated WITH CHECK (TRUE);

-- Also allow anon key to read (Command Center uses anon + auth token)
CREATE POLICY "Allow anon read" ON cef_news_alerts
    FOR SELECT TO anon USING (TRUE);

COMMENT ON TABLE cef_news_alerts IS 'Material news events for CEFs from SEC EDGAR + Finnhub. Populated daily by cef_news_scanner.py.';
