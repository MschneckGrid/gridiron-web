# Deployment & workflow patterns

## Claude Code (CC) workflow

**Always deliver fixes as CC prompts with exact find-and-replace strings.** Never apply changes directly in the conversation environment. The dashboard and command center live on Netlify/GitHub — only CC prompts that push to git result in deployed changes.

**CC prompt format:**
```
Find this exact text in [filename]:
[verbatim text to find]

Replace with:
[verbatim replacement text]
```

Provide exact, copy-pasteable strings. Multiple replacements in one prompt are fine — list them sequentially with clear labels.

## GitHub repositories

| Repo | Purpose | Deploy target |
|------|---------|---------------|
| `MschneckGrid/gridiron-web` | Main dashboard HTML + CEF Command Center (`command-center/cef_command_center.html`) | Netlify (gridiron-partners.netlify.app) |
| `MschneckGrid/gridiron-reporting` | Reporting tools | Netlify |
| `MschneckGrid/gridiron-engine` | IronSignal frontend, newsletter templates, active scripts | Netlify |
| `MschneckGrid/ironsignal-ai` | IronSignal frontend (alternate/future repo) | Netlify (not deployed yet) |
| `MschneckGrid/ironsignal` | IronSignal Options Income — React 18 + Vite + Tailwind frontend, Python engines | Netlify (ironsignal-options.netlify.app) |
| `MschneckGrid/weekly-options` | Options strategy (archived — migrated to ironsignal) | — |
| `MschneckGrid/gridiron-core` | Core/shared code | N/A |
| `Gridiron-Partners/sidebet` | SideBet golf fantasy | Netlify |
| `MschneckGrid/gridiron-statement-watcher` | PDF parser script | Local (Windows Task Scheduler) |
| `MschneckGrid/gridiron-13f-py` | 13F filing tools | Local |
| `MschneckGrid/barbell-tracker` | Barbell strategy tracker | TBD |

**Note:** The CEF Command Center lives in `gridiron-web`, not `gridiron-engine`. The `gridiron-engine` repo is archived — gridiron-core is canonical. The `ironsignal` repo is a React + Vite app (build: `npm run build`, publish: `dist/`). Data refresh via GitHub Action daily 8:30 AM ET.

## Frontend architecture

All frontends use the same single-file pattern:
- Single HTML file with React/JSX/Tailwind
- Babel in-browser transpilation via CDN
- Supabase JS client via CDN
- No build step — edit HTML, push to GitHub, Netlify auto-deploys

## Netlify deployment

Push to `main` branch → Netlify auto-deploys within ~30 seconds. No build command needed — static HTML files.

## Fix code before data

**Critical principle:** Manually correcting Supabase values before deploying the code fix is unreliable. The Statement tab recalculates on load and overwrites stored computed fields (`account_gain_since_hwm`, `hwm_balance`, `spread_over_hurdle`). Always deploy the code fix first, then let the view recalculate and auto-save.

## Local scripts (stay on laptop)

Two Python scripts run on Mike's Windows machine via Task Scheduler:
- `statement_watcher.py` — parses custodian PDF statements from OneDrive client folders into Supabase
- `gridiron_recon_sync.py` — pulls daily reconciliation Excel data from Outlook emails into Supabase

These operate independently of the dashboard's hosting. They require Windows-specific access (OneDrive folders, Outlook COM automation) and cannot be moved to the cloud.
