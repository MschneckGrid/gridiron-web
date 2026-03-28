# Knowledge file index

This file maps every knowledge file across all of Mike's Claude projects. When adding new features, components, or fixing bugs, consult this index to determine which file to update — or whether a new file is needed.

All projects share one Supabase instance (nzinvxticgyjobkqxxhl). Knowledge files are stored in each project's Claude.ai knowledge and backed up in `docs/` folders in the corresponding GitHub repos.

---

## Shared files (duplicated in all 3 projects)

| File | Purpose | Update when... |
|------|---------|---------------|
| `SUPABASE_PATTERNS.md` | RLS rules, trigger patterns, silent fail detection, key table inventory, pg_cron reference | You discover a new Supabase gotcha, add a major table, or learn a new pattern |
| `DEPLOYMENT.md` | CC prompt format, GitHub → Netlify flow, repo list, single-file HTML architecture, local script overview | You add a new repo, change the deploy process, or add a new local script |
| `KNOWLEDGE_INDEX.md` | This file — master map of all knowledge files across all projects | You create, rename, or retire any knowledge file in any project |

---

## Project 1: Gridiron Dashboard

**Claude project:** Gridiron Dashboard
**Repos:** gridiron-web, gridiron-reporting, gridiron-statement-watcher
**Attached file:** gridiron-dashboard-with-fasttrack.html

| File | Purpose | Update when... |
|------|---------|---------------|
| `DASHBOARD_TABS.md` | All 11 tabs, what each does, Statement recalc behavior, data validation banner, auth flow | You add a new tab, change how a tab works, or modify the validation logic |
| `CLIENT_UNIVERSE.md` | 66 clients, 5 categories, multi-account structures, overlap rules, fee types, client name matching | You add a client, change fee structures, or modify overlap/counting rules |
| `RETURN_FORMULAS.md` | Modified Dietz method, gross/net by fee type, performance fee / HWM logic, Comp Planner formulas | You change any return calculation, fix a formula bug, or modify fee logic |
| `DATA_PIPELINE.md` | statement_watcher.py architecture, recon_sync.py architecture, custodian list, parser overwrite guard | You add a custodian parser, change the sync logic, or fix a parsing bug |
| `FIX_HISTORY.md` | Past bugs and how they were fixed — Watson perf fee, Comp Planner, RLS 42501, parser overwrite, Goldman parsing, cash flow direction, login freeze | You fix any significant bug (add it here so we never repeat it) |

**When to create a new file in this project:**
- Adding a major new feature area (e.g., a new reporting module, a new data import pipeline)
- A single file exceeds ~200 lines and covers two distinct topics that could be split

---

## Project 2: Command Center + IronSignal

**Claude project:** Command Center + IronSignal
**Repos:** gridiron-core (canonical), ironsignal (options module), weekly-options (archived)
**Attached file:** cef_command_center.html

| File | Purpose | Update when... |
|------|---------|---------------|
| `SIGNAL_ENGINE.md` | Unified signal engine core — z-score calculation, calibration, buy/sell thresholds, split detection, asset class routing | You change how signals are generated, add a new signal type, or modify the z-score methodology |
| `DATA_PIPELINE_MKT.md` | FastTrack API integration, prices_unadjusted strategy, cef_daily table, cef_daily_clean view, cef_splits table, split detection function, daily pipeline chain | You change the data ingestion, add a new data source, modify the clean view, or fix a split detection issue |
| `ASSET_CLASSES.md` | CEF ticker universe (170 tickers by sector), BDC tables and 8K parsing, MLP/REIT structure, options universe (51 stocks), fund characteristics | You add/remove tickers, add a new asset class, change sector mappings, or update the options universe |
| `IRONSIGNAL_PRODUCT.md` | IronSignal product definition, four-pillar platform architecture, modules, tiering, newsletter workflow, signal tables | You build newsletter features, add subscriber management, change the product scope, or add a new module |
| `OPTIONS_STRATEGY.md` | Options Income module — four strategies, scoring models, ironsignal repo, Black-Scholes engine, FastTrack integration | You design or build any part of the options strategy |
| `CRON_SCHEDULE.md` | All 25+ pg_cron jobs with UTC/ET times, edge function endpoints, daily pipeline execution order | You add, modify, or remove any cron job or edge function |

**When to create a new file in this project:**
- Adding a completely new asset class with its own pipeline (e.g., commodities, crypto)
- Building out the newsletter into a complex system that outgrows `IRONSIGNAL_PRODUCT.md`
- The options strategy grows complex enough to need multiple files (e.g., `OPTIONS_TRADES.md` for trade history, `OPTIONS_STRATEGY.md` for logic)

### ironsignal (Options Income Module)

**Repo:** `MschneckGrid/ironsignal` (React 18 + Vite + Tailwind)
**Canonical docs:** gridiron-core/docs/ (this repo)

| File | Purpose | Update when... |
|------|---------|---------------|
| `CLAUDE.md` (in ironsignal repo) | Project context, build instructions, architecture | You change the ironsignal build, add features, or modify the frontend |
| `OPTIONS_STRATEGY.md` (this repo) | Canonical strategy spec — scoring models, data flow, Black-Scholes engine | You change any strategy logic, scoring weights, or the options universe |
| `IRONSIGNAL_PRODUCT.md` (this repo) | Product overview — four pillars, modules, tiering | You add modules, change product scope, or update the tiering model |

**Note:** Options strategy knowledge lives in gridiron-core (canonical). The ironsignal repo's `CLAUDE.md` references it. weekly-options is archived.

---

## Project 3: SideBet

**Claude project:** SideBet
**Repos:** Gridiron-Partners/sidebet
**Attached file:** main SideBet app file

| File | Purpose | Update when... |
|------|---------|---------------|
| `SIDEBET_ARCH.md` | App structure, auth flow, commissioner vs user roles, frontend framework, deployment | You change the app architecture, auth system, or add new user roles |
| `GAME_LOGIC.md` | Fantasy game types, scoring rules, PGA Tour data source, player selection mechanics, league management | You add game types, change scoring, or modify draft/pick rules |
| `SIDEBET_TABLES.md` | All ~28 Supabase tables grouped by domain (core, competition, pipeline, intelligence, comms, multi-tenant) | You add tables, modify schemas, or change relationships |

**When to create a new file in this project:**
- Adding a major new game type with complex rules
- Building a separate admin/commissioner tool
- Integrating a new sports data provider

---

## Rules for maintaining this system

1. **Every significant code fix gets added to `FIX_HISTORY.md`** (Dashboard) or a similar section in the relevant project file. Past bugs are the most valuable knowledge — they prevent repeat mistakes.

2. **Every new cron job or edge function gets added to `CRON_SCHEDULE.md`** immediately. The timing dependencies between jobs are critical and easy to forget.

3. **When you create a new knowledge file**, update this index in all 3 projects so Claude always knows the full map.

4. **When a file exceeds ~200 lines**, consider splitting it. A file that's too long defeats the purpose — Claude loads the whole thing when it matches a search, so shorter, focused files mean less noise.

5. **Keep files factual, not conversational.** Tables, code snippets, and concise descriptions. No "we discussed..." or "as mentioned..." — write as if explaining to a new team member.

6. **Re-upload to Claude.ai after every update.** Editing the file in GitHub is good for version control, but Claude only reads from the project knowledge uploads. Both need to stay in sync.
