# gridiron-web

Web dashboards and tools for Gridiron Partners. Formerly `gridiron-dashboards`.

## Hosting

Deployed on Netlify at [gridiron-partners.netlify.app](https://gridiron-partners.netlify.app).
Pushes to `main` trigger automatic deploys.

## Data

All dashboards connect to Supabase project `nzinvxticgyjobkqxxhl` for CEF analytics, signals, and news data.

## Folder Structure

```
dashboard/          Gridiron dashboards, sector views, engine review, email generator
command-center/     CEF Command Center, signal engine, signals output
presentations/      Presentation generator and PPTX templates
scripts/            Python pipeline scripts (news scanner, EDGAR, scrapers)
News/               News scanner pipeline config, logs, and batch files
```

## Key Files

- `netlify.toml` — Netlify config: redirects, security headers
- `dashboard/gridiron-dashboard-with-fasttrack.html` — Main dashboard (default route)
- `command-center/cef_command_center.html` — CEF Command Center
- `presentations/gridiron_presentation_generator.html` — Client presentation generator
