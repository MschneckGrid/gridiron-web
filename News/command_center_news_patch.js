/* ===========================================================================
   CEF COMMAND CENTER — NEWS TAB PATCH
   ===========================================================================
   This file contains all additions to cef_command_center.html to add the
   "News" tab for material event monitoring.

   INTEGRATION INSTRUCTIONS:
   There are 4 insertions to make in your existing file. Each is labeled
   with EXACTLY where to paste it. Search for the marker text.

   =========================================================================== */


/* ===========================================================================
   PATCH 1 of 4 — CSS
   ===========================================================================
   INSERT this block after the existing alert CSS (around line 246, after:
     .alert-item .a-meta{font-size:10px;color:var(--text3)...}
   )
   =========================================================================== */

/* --- NEWS ALERTS --- */
.news-controls{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:16px}
.news-controls select,.news-controls input{padding:6px 10px;border-radius:6px;border:1px solid rgba(0,0,0,0.12);font-size:12px;font-family:'JetBrains Mono',monospace}
.news-controls .news-search{flex:1;min-width:180px;padding:8px 12px;font-family:inherit}
.news-item{display:flex;align-items:flex-start;gap:12px;background:var(--bg2);border-radius:10px;padding:14px 16px;margin-bottom:6px;border-left:4px solid var(--text3);transition:background 0.15s,box-shadow 0.15s;cursor:default}
.news-item:hover{background:#fff;box-shadow:0 2px 12px rgba(0,0,0,0.06)}
.news-item.high-priority{border-left-color:#d32f2f;background:rgba(211,47,47,0.03)}
.news-item.high-priority:hover{background:rgba(211,47,47,0.06)}
.news-item .n-ticker{min-width:48px;font-weight:700;font-family:'JetBrains Mono',monospace;font-size:13px;color:var(--accent);cursor:pointer}
.news-item .n-ticker:hover{text-decoration:underline}
.news-item .n-type{padding:2px 8px;border-radius:4px;font-size:9px;font-weight:700;letter-spacing:0.06em;min-width:90px;text-align:center;white-space:nowrap;flex-shrink:0}
.news-item .n-body{flex:1;min-width:0}
.news-item .n-headline{font-size:12px;font-weight:600;color:var(--text);line-height:1.4;margin-bottom:3px}
.news-item .n-headline a{color:inherit;text-decoration:none;border-bottom:1px solid transparent;transition:border-color 0.15s}
.news-item .n-headline a:hover{border-bottom-color:var(--accent);color:var(--accent)}
.news-item .n-summary{font-size:11px;color:var(--text2);line-height:1.45;margin-bottom:4px;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.news-item .n-meta{font-size:10px;color:var(--text3);font-family:'JetBrains Mono',monospace;display:flex;gap:12px;align-items:center}
.news-item .n-meta .n-source{color:var(--accent2,#1565c0)}
.news-item .n-meta .n-mat{padding:1px 5px;border-radius:3px;font-size:8px;font-weight:700}
.news-item .n-meta .n-mat.mat-high{background:rgba(211,47,47,0.1);color:#d32f2f}
.news-item .n-meta .n-mat.mat-med{background:rgba(230,81,0,0.1);color:#e65100}
.news-item .n-meta .n-mat.mat-low{background:rgba(138,155,181,0.1);color:#8a9bb5}
.news-item .n-actions{display:flex;gap:6px;flex-shrink:0;align-items:flex-start}
.news-item .n-actions button{padding:4px 8px;border:1px solid rgba(0,0,0,0.1);border-radius:4px;font-size:9px;background:transparent;cursor:pointer;color:var(--text3);transition:all 0.15s}
.news-item .n-actions button:hover{background:var(--bg2);color:var(--text)}
.news-item .n-actions .n-link{background:rgba(21,101,192,0.08);border-color:rgba(21,101,192,0.2);color:#1565c0}
.news-item .n-actions .n-link:hover{background:rgba(21,101,192,0.15)}
.news-item.dismissed{opacity:0.4}
.news-date-group{font-size:11px;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:0.08em;padding:12px 0 6px;border-bottom:1px solid var(--border);margin-bottom:8px}
.news-empty{text-align:center;padding:60px;color:var(--text3);font-family:'JetBrains Mono',monospace;font-size:12px}
.news-stats{display:flex;gap:16px;margin-bottom:16px;flex-wrap:wrap}
.news-stat{background:var(--bg2);border-radius:8px;padding:10px 16px;text-align:center;min-width:100px}
.news-stat .ns-val{font-size:22px;font-weight:700;font-family:'JetBrains Mono',monospace}
.news-stat .ns-label{font-size:9px;color:var(--text3);text-transform:uppercase;letter-spacing:0.06em;margin-top:2px}
.news-loading{text-align:center;padding:40px;color:var(--text3)}
.news-loading .spinner{display:inline-block;width:24px;height:24px;border:3px solid var(--border);border-top-color:var(--accent);border-radius:50%;animation:spin 0.8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}


/* ===========================================================================
   PATCH 2 of 4 — TAB BUTTON
   ===========================================================================
   INSERT this button AFTER the existing "Alerts" tab button (around line 397,
   after the closing </button> of the Alerts tab and before Model Portfolio).

   Find this line:
     <button class="master-tab engine-tab" onclick="switchMasterTab('engine-model',this)">Model Portfolio</button>
   
   Insert BEFORE it:
   =========================================================================== */

  <button class="master-tab engine-tab" onclick="switchMasterTab('engine-news',this)">
    News <span class="tab-badge" id="masterNewsBadge" style="background:rgba(106,27,154,0.12);color:#6a1b9a">0</span>
  </button>


/* ===========================================================================
   PATCH 3 of 4 — HTML PANEL
   ===========================================================================
   INSERT this panel AFTER the existing engine-alerts panel (around line 646,
   after the closing </div> of panel-engine-alerts and before panel-engine-model).

   Find this line:
     </div>  <!-- end panel-engine-alerts -->
   
   Or find:
     <div class="master-panel" id="panel-engine-model">
   
   Insert BEFORE it:
   =========================================================================== */

<div class="master-panel" id="panel-engine-news">
  <div class="body" style="padding:18px 28px;">
    <!-- Stats row -->
    <div class="news-stats" id="newsStats"></div>

    <!-- Controls -->
    <div class="news-controls">
      <select id="newsTypeFilter" onchange="renderNews()">
        <option value="ALL">All Event Types</option>
        <option value="MERGER">Merger / Reorganization</option>
        <option value="MGMT_CHANGE">Management Change</option>
        <option value="DISTRIBUTION">Distribution</option>
        <option value="TENDER_OFFER">Tender Offer</option>
        <option value="RIGHTS_OFFERING">Rights Offering</option>
        <option value="ACTIVIST">Activist Investor</option>
        <option value="TERMINATION">Fund Termination</option>
        <option value="BUYBACK">Share Buyback</option>
        <option value="LEVERAGE">Leverage / Credit</option>
        <option value="REGULATORY">Regulatory</option>
        <option value="EARNINGS">Earnings / Report</option>
        <option value="SEC_FILING">SEC Filing</option>
        <option value="OTHER">Other</option>
      </select>
      <select id="newsPriorityFilter" onchange="renderNews()">
        <option value="ALL">All Priority</option>
        <option value="HIGH">High Priority (70+)</option>
        <option value="MED">Medium (50-69)</option>
        <option value="LOW">Low (&lt;50)</option>
      </select>
      <select id="newsSourceFilter" onchange="renderNews()">
        <option value="ALL">All Sources</option>
        <option value="SEC EDGAR">SEC EDGAR</option>
        <option value="Finnhub">Finnhub</option>
      </select>
      <select id="newsDaysFilter" onchange="loadNewsAlerts()">
        <option value="7">Last 7 Days</option>
        <option value="14">Last 14 Days</option>
        <option value="30" selected>Last 30 Days</option>
        <option value="90">Last 90 Days</option>
      </select>
      <input type="text" class="news-search" id="newsSearch" placeholder="Search ticker or headline..." oninput="renderNews()">
      <button onclick="loadNewsAlerts()" style="padding:7px 14px;border-radius:6px;border:1px solid rgba(21,101,192,0.3);background:rgba(21,101,192,0.08);color:#1565c0;font-size:11px;font-weight:600;cursor:pointer;font-family:'JetBrains Mono',monospace" title="Refresh news from Supabase">⟳ Refresh</button>
      <label style="display:flex;align-items:center;gap:4px;font-size:11px;color:var(--text3);cursor:pointer">
        <input type="checkbox" id="newsShowDismissed" onchange="renderNews()"> Show dismissed
      </label>
    </div>

    <!-- News list -->
    <div id="newsList">
      <div class="news-empty">Click ⟳ Refresh or switch to this tab to load news alerts from Supabase.</div>
    </div>
  </div>
</div>


/* ===========================================================================
   PATCH 4 of 4 — JAVASCRIPT
   ===========================================================================
   INSERT this entire script block at the end of your <script> section,
   BEFORE the closing </script> tag (around line 3540+).

   This adds:
   - News data storage
   - Supabase fetch for cef_news_alerts
   - Rendering with date grouping
   - Filtering (type, priority, source, search)
   - Dismiss/review toggles
   - Auto-load on tab switch
   =========================================================================== */

// ═══════════════════════════════════════════════════════════════════════════
//  NEWS ALERTS MODULE
// ═══════════════════════════════════════════════════════════════════════════

let NEWS_DATA = [];       // Full array from Supabase
let NEWS_LOADED = false;  // Prevent redundant loads

const NEWS_TYPE_COLORS = {
  MERGER:          { color: '#6a1b9a', label: 'MERGER' },
  MGMT_CHANGE:     { color: '#c62828', label: 'MGMT CHANGE' },
  DISTRIBUTION:    { color: '#2e7d32', label: 'DISTRIBUTION' },
  TENDER_OFFER:    { color: '#1565c0', label: 'TENDER OFFER' },
  RIGHTS_OFFERING: { color: '#e65100', label: 'RIGHTS OFFER' },
  ACTIVIST:        { color: '#ad1457', label: 'ACTIVIST' },
  TERMINATION:     { color: '#b71c1c', label: 'TERMINATION' },
  BUYBACK:         { color: '#00838f', label: 'BUYBACK' },
  LEVERAGE:        { color: '#5d4037', label: 'LEVERAGE' },
  REGULATORY:      { color: '#ff6f00', label: 'REGULATORY' },
  EARNINGS:        { color: '#37474f', label: 'EARNINGS' },
  SEC_FILING:      { color: '#455a64', label: 'SEC FILING' },
  IPO_OFFERING:    { color: '#0d47a1', label: 'IPO/OFFERING' },
  OTHER:           { color: '#8a9bb5', label: 'OTHER' },
};

async function loadNewsAlerts() {
  const el = document.getElementById('newsList');
  el.innerHTML = '<div class="news-loading"><div class="spinner"></div><div style="margin-top:10px">Loading news alerts from Supabase...</div></div>';

  const days = parseInt(document.getElementById('newsDaysFilter').value) || 30;
  const since = new Date();
  since.setDate(since.getDate() - days);
  const sinceStr = since.toISOString().split('T')[0];

  try {
    const resp = await fetch(
      `${SUPABASE_URL}/rest/v1/cef_news_alerts?event_date=gte.${sinceStr}&order=event_date.desc,materiality.desc&limit=500`,
      { headers: hdrs() }
    );

    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
    }

    NEWS_DATA = await resp.json();
    NEWS_LOADED = true;

    // Update badge
    const highPriority = NEWS_DATA.filter(n => n.materiality >= 70 && !n.dismissed).length;
    document.getElementById('masterNewsBadge').textContent = highPriority || NEWS_DATA.filter(n => !n.dismissed).length;

    renderNewsStats();
    renderNews();
  } catch (e) {
    console.error('News fetch error:', e);
    el.innerHTML = '<div class="news-empty">⚠ Failed to load news alerts.<br><span style="font-size:10px;margin-top:6px;display:block">'
      + e.message + '</span><br><span style="font-size:10px">Make sure the cef_news_alerts table exists in Supabase and RLS policies allow reads.</span></div>';
  }
}

function renderNewsStats() {
  const el = document.getElementById('newsStats');
  const active = NEWS_DATA.filter(n => !n.dismissed);
  const high = active.filter(n => n.materiality >= 70).length;
  const uniqueTickers = new Set(active.map(n => n.ticker)).size;
  const sources = {};
  active.forEach(n => { sources[n.source_name] = (sources[n.source_name] || 0) + 1; });

  const types = {};
  active.forEach(n => { types[n.event_type] = (types[n.event_type] || 0) + 1; });
  const topType = Object.entries(types).sort((a,b) => b[1] - a[1])[0];

  el.innerHTML = `
    <div class="news-stat"><div class="ns-val">${active.length}</div><div class="ns-label">Total Alerts</div></div>
    <div class="news-stat"><div class="ns-val" style="color:#d32f2f">${high}</div><div class="ns-label">High Priority</div></div>
    <div class="news-stat"><div class="ns-val">${uniqueTickers}</div><div class="ns-label">Tickers</div></div>
    <div class="news-stat"><div class="ns-val">${topType ? topType[0].replace(/_/g,' ') : '—'}</div><div class="ns-label">Most Common Type</div></div>
    ${Object.entries(sources).map(([s,c]) => `<div class="news-stat"><div class="ns-val">${c}</div><div class="ns-label">${s}</div></div>`).join('')}
  `;
}

function renderNews() {
  const el = document.getElementById('newsList');
  if (!NEWS_DATA.length) {
    el.innerHTML = '<div class="news-empty">No news alerts found for this period.<br><span style="font-size:10px;margin-top:6px;display:block">Run cef_news_scanner.py to populate data.</span></div>';
    return;
  }

  // Filters
  const typeFilter = document.getElementById('newsTypeFilter').value;
  const priorityFilter = document.getElementById('newsPriorityFilter').value;
  const sourceFilter = document.getElementById('newsSourceFilter').value;
  const searchText = (document.getElementById('newsSearch').value || '').toLowerCase().trim();
  const showDismissed = document.getElementById('newsShowDismissed').checked;

  let filtered = NEWS_DATA.filter(n => {
    if (!showDismissed && n.dismissed) return false;
    if (typeFilter !== 'ALL' && n.event_type !== typeFilter) return false;
    if (sourceFilter !== 'ALL' && n.source_name !== sourceFilter) return false;
    if (priorityFilter === 'HIGH' && n.materiality < 70) return false;
    if (priorityFilter === 'MED' && (n.materiality < 50 || n.materiality >= 70)) return false;
    if (priorityFilter === 'LOW' && n.materiality >= 50) return false;
    if (searchText) {
      const haystack = `${n.ticker} ${n.headline} ${n.summary || ''} ${n.event_type}`.toLowerCase();
      if (!haystack.includes(searchText)) return false;
    }
    return true;
  });

  if (!filtered.length) {
    el.innerHTML = '<div class="news-empty">No alerts match the current filters.</div>';
    return;
  }

  // Group by date
  const groups = {};
  filtered.forEach(n => {
    const d = n.event_date || 'Unknown';
    if (!groups[d]) groups[d] = [];
    groups[d].push(n);
  });

  let html = '';
  for (const [dateStr, items] of Object.entries(groups)) {
    // Format date label
    let label = dateStr;
    try {
      const d = new Date(dateStr + 'T12:00:00');
      const today = new Date(); today.setHours(12,0,0,0);
      const yesterday = new Date(today); yesterday.setDate(yesterday.getDate() - 1);
      if (d.toDateString() === today.toDateString()) label = 'Today — ' + dateStr;
      else if (d.toDateString() === yesterday.toDateString()) label = 'Yesterday — ' + dateStr;
      else label = d.toLocaleDateString('en-US', { weekday:'long', month:'short', day:'numeric', year:'numeric' });
    } catch(e) {}

    html += `<div class="news-date-group">${label} <span style="float:right;font-weight:400;font-size:10px">${items.length} alert${items.length !== 1 ? 's' : ''}</span></div>`;

    for (const n of items) {
      const tc = NEWS_TYPE_COLORS[n.event_type] || NEWS_TYPE_COLORS.OTHER;
      const isHigh = n.materiality >= 70;
      const matClass = n.materiality >= 70 ? 'mat-high' : n.materiality >= 50 ? 'mat-med' : 'mat-low';
      const matLabel = n.materiality >= 70 ? 'HIGH' : n.materiality >= 50 ? 'MED' : 'LOW';

      html += `<div class="news-item${isHigh ? ' high-priority' : ''}${n.dismissed ? ' dismissed' : ''}" data-id="${n.id}">
        <span class="n-ticker" onclick="openDetail('${n.ticker}')" title="Open ${n.ticker} detail">${n.ticker}</span>
        <span class="n-type" style="color:${tc.color};background:${tc.color}14">${tc.label}</span>
        <div class="n-body">
          <div class="n-headline">${n.source_url
            ? `<a href="${n.source_url}" target="_blank" rel="noopener" title="Open source article">${escHtml(n.headline)}</a>`
            : escHtml(n.headline)}</div>
          ${n.summary ? `<div class="n-summary">${escHtml(n.summary)}</div>` : ''}
          <div class="n-meta">
            <span class="n-source">${escHtml(n.source_name || '')}</span>
            ${n.filing_type ? `<span>Filing: ${n.filing_type}</span>` : ''}
            <span class="n-mat ${matClass}">${matLabel} (${n.materiality})</span>
            ${n.reviewed ? '<span style="color:#2e7d32">✓ Reviewed</span>' : ''}
          </div>
        </div>
        <div class="n-actions">
          ${n.source_url ? `<a href="${n.source_url}" target="_blank" rel="noopener" class="n-link" style="padding:4px 8px;border:1px solid rgba(21,101,192,0.2);border-radius:4px;font-size:9px;text-decoration:none;background:rgba(21,101,192,0.08);color:#1565c0">Open ↗</a>` : ''}
          <button onclick="toggleNewsReview(${n.id})" title="${n.reviewed ? 'Mark unreviewed' : 'Mark reviewed'}">${n.reviewed ? '☑' : '☐'}</button>
          <button onclick="toggleNewsDismiss(${n.id})" title="${n.dismissed ? 'Restore' : 'Dismiss'}">${n.dismissed ? '↩' : '✕'}</button>
        </div>
      </div>`;
    }
  }

  el.innerHTML = html;
}

function escHtml(str) {
  const div = document.createElement('div');
  div.textContent = str || '';
  return div.innerHTML;
}

async function toggleNewsReview(id) {
  const item = NEWS_DATA.find(n => n.id === id);
  if (!item) return;
  const newVal = !item.reviewed;

  try {
    await fetch(`${SUPABASE_URL}/rest/v1/cef_news_alerts?id=eq.${id}`, {
      method: 'PATCH',
      headers: hdrs(),
      body: JSON.stringify({ reviewed: newVal }),
    });
    item.reviewed = newVal;
    renderNews();
  } catch(e) {
    console.error('Review toggle error:', e);
  }
}

async function toggleNewsDismiss(id) {
  const item = NEWS_DATA.find(n => n.id === id);
  if (!item) return;
  const newVal = !item.dismissed;

  try {
    await fetch(`${SUPABASE_URL}/rest/v1/cef_news_alerts?id=eq.${id}`, {
      method: 'PATCH',
      headers: hdrs(),
      body: JSON.stringify({ dismissed: newVal }),
    });
    item.dismissed = newVal;
    renderNewsStats();
    renderNews();
    // Update badge
    const active = NEWS_DATA.filter(n => !n.dismissed);
    const high = active.filter(n => n.materiality >= 70).length;
    document.getElementById('masterNewsBadge').textContent = high || active.length;
  } catch(e) {
    console.error('Dismiss toggle error:', e);
  }
}

// ═══ Auto-load news on tab switch ═══
// Patch into switchMasterTab to auto-load news when the tab is first opened.
const _origSwitchMasterTab = switchMasterTab;
switchMasterTab = function(tab, el) {
  _origSwitchMasterTab(tab, el);
  if (tab === 'engine-news' && !NEWS_LOADED) {
    loadNewsAlerts();
  }
};


/* ===========================================================================
   END OF PATCH
   ===========================================================================
   After applying all 4 patches, the News tab will:
   - Appear between Alerts and Model Portfolio in the tab bar
   - Auto-load from Supabase when first clicked
   - Show stats: total alerts, high priority count, ticker coverage
   - Filter by event type, priority level, source, date range, free-text
   - Group news items by date with color-coded event types
   - Link directly to source articles/filings (opens in new tab)
   - Allow marking items as reviewed or dismissed
   - Update the badge count in the tab bar
   =========================================================================== */
