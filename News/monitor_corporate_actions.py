"""
monitor_corporate_actions.py  --  CEF Corporate Action Monitor
==============================================================
Runs weekly (Sunday). For every active CEF with a CIK, checks SEC EDGAR
for recent filings that signal pending or completed corporate actions.

KEY LOGIC — N-14 vs. Target Detection
--------------------------------------
N-14 is filed by the ACQUIRING fund (the one issuing new shares to absorb
others). Flagging the N-14 filer as pending_merger is WRONG — that fund
survives the merger. N-14 filings are logged as 'acquirer_note' only.

Target funds are identified by:
  - DEF 14A / DEFC14A  : Proxy asking TARGET shareholders to approve merger
                         (description scanned for merger/liquidation keywords)
  - SC TO-C / SC TO-T  : Tender offer communication filed re: TARGET fund
  - 15-12G / 15-12B    : Deregistration (fund winding down / merged away)

Status transitions written to cef_tickers:
  active          -> pending_merger    (merger proxy or tender offer detected)
  active          -> pending_delisting (15-12G/B detected)
  pending_merger  -> merged            (15-12G detected after merger proxy)
  missing_from_listing -> inactive     (missing 60+ days with no merger signal)

N-14 detections: logged to cef_universe_log as 'acquirer_note' only.
All other events logged to cef_universe_log.

Usage:
  python monitor_corporate_actions.py --dry-run
  python monitor_corporate_actions.py
"""

import argparse
import configparser
import json
import logging
import sys
import time
import urllib.request
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR   = Path(__file__).parent
CONFIG_FILE  = SCRIPT_DIR / "gridiron.cfg"
SEC_UA       = "Gridiron Partners mike@gridironpartners.com"
LOOKBACK_DAYS = 120   # how far back to scan recent filings
MISSING_INACTIVE_DAYS = 60   # days missing before escalating to inactive

# Form types that signal a corporate action
# NOTE: N-14 is filed by ACQUIRER — do NOT mark pending_merger on N-14 filers
N14_FORMS       = {"N-14", "N-14 8C", "N-14MEF", "N-14 8C/A"}
PROXY_FORMS     = {"DEF 14A", "DEFC14A", "DEFR14A", "PRE 14A"}
TENDER_FORMS    = {"SC TO-C", "SC TO-T", "SC TO-T/A"}
DEREG_FORMS     = {"15-12G", "15-12B", "15-12G/A"}

MERGER_KEYWORDS = {
    "reorganiz", "reorgainis", "merger", "merging", "acquire",
    "acquis", "liquidat", "dissolv", "terminat", "wind up",
    "combine", "absorb", "consolidat"
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("monitor_corporate_actions")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    try:
        stream = open(sys.stdout.fileno(), mode="w", encoding="utf-8", closefd=False)
    except Exception:
        stream = sys.stdout
    ch = logging.StreamHandler(stream)
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    fh = logging.FileHandler(SCRIPT_DIR / "monitor_corporate_actions.log", encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------
def load_config() -> dict:
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    return {
        "supabase_url": cfg["supabase"]["url"],
        "supabase_key": cfg["supabase"]["service_role_key"],
    }


# ---------------------------------------------------------------------------
# Supabase helpers
# ---------------------------------------------------------------------------
def sb_get(url, key, path, params="") -> list:
    req = urllib.request.Request(
        f"{url}/rest/v1/{path}?{params}",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Accept": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        return json.load(r)


def sb_patch(url, key, table, match, payload, dry_run=False) -> None:
    if dry_run:
        return
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{url}/rest/v1/{table}?{match}",
        data=body, method="PATCH",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/json", "Prefer": "return=minimal"}
    )
    with urllib.request.urlopen(req) as r:
        r.read()


def sb_insert(url, key, table, payload, dry_run=False) -> None:
    if dry_run:
        return
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{url}/rest/v1/{table}",
        data=body, method="POST",
        headers={"apikey": key, "Authorization": f"Bearer {key}",
                 "Content-Type": "application/json", "Prefer": "return=minimal"}
    )
    with urllib.request.urlopen(req) as r:
        r.read()


# ---------------------------------------------------------------------------
# SEC Submissions API
# ---------------------------------------------------------------------------
def fetch_recent_filings(cik: int, logger: logging.Logger) -> list:
    """
    Returns list of recent filings from SEC Submissions API.
    Each dict has: form, date, description
    """
    padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{padded}.json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": SEC_UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.load(r)
    except Exception as e:
        logger.warning(f"    SEC API error for CIK {cik}: {e}")
        return []

    recent = data.get("filings", {}).get("recent", {})
    forms        = recent.get("form", [])
    dates        = recent.get("filingDate", [])
    descriptions = recent.get("primaryDocDescription", [])

    cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).isoformat()

    filings = []
    for form, fdate, desc in zip(forms, dates, descriptions):
        if fdate >= cutoff:
            filings.append({
                "form": form.strip(),
                "date": fdate,
                "description": (desc or "").strip(),
            })

    return filings


# ---------------------------------------------------------------------------
# Classify filings
# ---------------------------------------------------------------------------
def classify_filings(filings: list) -> dict:
    """
    Returns dict with keys:
      n14          : N-14 filing (acquirer filed this — log only, no status change)
      proxy_merger : DEF 14A with merger keywords (target filed this)
      tender       : SC TO-C/T (tender offer for this fund — target)
      dereg        : 15-12G/B (deregistration)
    Each value is the first matching filing dict, or None.
    """
    result = {"n14": None, "proxy_merger": None, "tender": None, "dereg": None}

    for f in filings:
        form = f["form"].upper()
        desc = f["description"].lower()

        # N-14: acquirer filed — just note it, don't change status
        if any(form == m or form.startswith(m.replace(" ", "-")) or form == m.replace(" ", "-")
               for m in N14_FORMS) or form.startswith("N-14"):
            if not result["n14"]:
                result["n14"] = f

        elif form in TENDER_FORMS:
            if not result["tender"]:
                result["tender"] = f

        elif form in DEREG_FORMS:
            if not result["dereg"]:
                result["dereg"] = f

        elif form in PROXY_FORMS:
            if any(kw in desc for kw in MERGER_KEYWORDS):
                if not result["proxy_merger"]:
                    result["proxy_merger"] = f

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logger = setup_logging()
    today = date.today()
    logger.info("=" * 60)
    logger.info(f"=== {'DRY-RUN' if args.dry_run else 'LIVE'} | monitor_corporate_actions.py | {today} ===")
    logger.info("=" * 60)

    cfg = load_config()
    url = cfg["supabase_url"]
    key = cfg["supabase_key"]

    # Pull active + pending + missing tickers with real CIKs
    rows = sb_get(url, key, "cef_tickers",
        "select=ticker,fund_name,cik,status,last_confirmed_date,merged_into"
        "&status=in.(active,pending_merger,missing_from_listing)"
        "&cik=gt.0"          # exclude NULL and -1 (Canadian trusts)
        "&order=ticker"
    )
    logger.info(f"Checking {len(rows)} tickers for corporate actions")

    stats = {"acquirer_noted": 0, "pending_merger": 0, "pending_delisting": 0,
             "merged": 0, "inactive": 0, "no_action": 0}

    for row in rows:
        ticker  = row["ticker"]
        cik     = row["cik"]
        status  = row["status"]
        name    = (row.get("fund_name") or "")[:45]
        last_confirmed = row.get("last_confirmed_date")

        logger.info(f"  {ticker:<8} [{status:<22}] CIK={cik} | {name}")

        # --- Already missing_from_listing: check for escalation ---
        if status == "missing_from_listing":
            if last_confirmed:
                days_missing = (today - date.fromisoformat(last_confirmed)).days
            else:
                days_missing = 999

            if days_missing >= MISSING_INACTIVE_DAYS:
                logger.info(f"    -> Missing {days_missing}d >= {MISSING_INACTIVE_DAYS}d threshold -- marking inactive")
                sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                         {"status": "inactive", "delisted_date": today.isoformat()},
                         dry_run=args.dry_run)
                sb_insert(url, key, "cef_universe_log", {
                    "event_date": today.isoformat(), "run_date": today.isoformat(),
                    "event_type": "status_change",
                    "ticker": ticker,
                    "detail": f"Marked inactive after {days_missing} days missing from listing files",
                    "source": "monitor_corporate_actions"
                }, dry_run=args.dry_run)
                stats["inactive"] += 1
            else:
                logger.info(f"    Missing {days_missing}d -- watching, not yet inactive")
                stats["no_action"] += 1
            time.sleep(0.4)
            continue

        # --- Already pending_merger: check for completed deregistration ---
        if status == "pending_merger":
            filings = fetch_recent_filings(cik, logger)
            classified = classify_filings(filings)
            time.sleep(0.4)

            if classified["dereg"]:
                f = classified["dereg"]
                logger.info(f"    -> Deregistration {f['form']} on {f['date']} -- marking merged")
                sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                         {"status": "merged", "delisted_date": f["date"]},
                         dry_run=args.dry_run)
                sb_insert(url, key, "cef_universe_log", {
                    "event_date": f["date"], "run_date": today.isoformat(),
                    "event_type": "merged",
                    "ticker": ticker,
                    "detail": f"Deregistration {f['form']} filed {f['date']} -- marked merged",
                    "source": "monitor_corporate_actions"
                }, dry_run=args.dry_run)
                stats["merged"] += 1
            else:
                logger.info(f"    Still pending_merger, no deregistration yet")
                stats["no_action"] += 1
            continue

        # --- Active: scan for new corporate action filings ---
        filings = fetch_recent_filings(cik, logger)
        classified = classify_filings(filings)
        time.sleep(0.4)

        action_taken = False

        # N-14: This fund is the ACQUIRER -- log note only, do NOT mark pending_merger
        if classified["n14"]:
            f = classified["n14"]
            logger.info(f"    [ACQUIRER-NOTE] {f['form']} filed {f['date']} -- this fund is acquiring; no status change")
            sb_insert(url, key, "cef_universe_log", {
                "event_date": f["date"], "run_date": today.isoformat(),
                "event_type": "acquirer_note",
                "ticker": ticker,
                "detail": f"N-14 filed {f['date']}: this fund is the acquirer in a merger. Investigate target fund(s) manually.",
                "source": "monitor_corporate_actions"
            }, dry_run=args.dry_run)
            stats["acquirer_noted"] += 1
            action_taken = True  # don't also log "no action"

        # DEF 14A with merger keywords: TARGET fund asking shareholders to approve
        if classified["proxy_merger"] and not action_taken:
            f = classified["proxy_merger"]
            logger.info(f"    [PROXY-MERGER] {f['form']} filed {f['date']}: {f['description'][:60]}")
            sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                     {"status": "pending_merger"},
                     dry_run=args.dry_run)
            sb_insert(url, key, "cef_universe_log", {
                "event_date": f["date"], "run_date": today.isoformat(),
                "event_type": "pending_merger",
                "ticker": ticker,
                "detail": f"Merger proxy {f['form']} filed {f['date']}: {f['description'][:120]}",
                "source": "monitor_corporate_actions"
            }, dry_run=args.dry_run)
            stats["pending_merger"] += 1
            action_taken = True

        # SC TO-C/T: Tender offer -- this fund is the target
        if classified["tender"] and not action_taken:
            f = classified["tender"]
            logger.info(f"    [TENDER] {f['form']} filed {f['date']}: {f['description'][:60]}")
            sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                     {"status": "pending_merger"},
                     dry_run=args.dry_run)
            sb_insert(url, key, "cef_universe_log", {
                "event_date": f["date"], "run_date": today.isoformat(),
                "event_type": "pending_merger",
                "ticker": ticker,
                "detail": f"Tender offer {f['form']} filed {f['date']}: {f['description'][:120]}",
                "source": "monitor_corporate_actions"
            }, dry_run=args.dry_run)
            stats["pending_merger"] += 1
            action_taken = True

        # 15-12G/B: Deregistration (winding down without a merger proxy signal)
        if classified["dereg"] and not action_taken:
            f = classified["dereg"]
            logger.info(f"    [DEREG] {f['form']} filed {f['date']} -- pending delisting")
            sb_patch(url, key, "cef_tickers", f"ticker=eq.{ticker}",
                     {"status": "pending_delisting"},
                     dry_run=args.dry_run)
            sb_insert(url, key, "cef_universe_log", {
                "event_date": f["date"], "run_date": today.isoformat(),
                "event_type": "pending_delisting",
                "ticker": ticker,
                "detail": f"Deregistration {f['form']} filed {f['date']}",
                "source": "monitor_corporate_actions"
            }, dry_run=args.dry_run)
            stats["pending_delisting"] += 1
            action_taken = True

        if not action_taken:
            logger.info(f"    No action signals in last {LOOKBACK_DAYS} days")
            stats["no_action"] += 1

    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("MONITOR COMPLETE")
    logger.info(f"  Acquirer N-14 noted:  {stats['acquirer_noted']}")
    logger.info(f"  New pending_merger:   {stats['pending_merger']}")
    logger.info(f"  New pending_delisting:{stats['pending_delisting']}")
    logger.info(f"  Escalated to merged:  {stats['merged']}")
    logger.info(f"  Escalated to inactive:{stats['inactive']}")
    logger.info(f"  No action:            {stats['no_action']}")
    logger.info("=" * 60)
    if args.dry_run:
        logger.info("Dry-run complete -- no changes written.")
    else:
        logger.info("Live run complete.")


if __name__ == "__main__":
    main()
