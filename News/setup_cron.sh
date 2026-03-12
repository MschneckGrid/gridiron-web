#!/bin/bash
# =============================================================================
# CEF News Scanner — Cron Setup
# =============================================================================
# Run this script to install a daily cron job that runs the news scanner
# at 7:00 AM ET every weekday.
#
# Usage:
#   chmod +x setup_cron.sh
#   ./setup_cron.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON_PATH="$(which python3)"
SCANNER="${SCRIPT_DIR}/cef_news_scanner.py"
LOG_DIR="${SCRIPT_DIR}/logs"

# Create log directory
mkdir -p "$LOG_DIR"

# Cron expression: 7:00 AM ET (12:00 UTC) Mon-Fri
CRON_EXPR="0 12 * * 1-5"

# Build the cron command
CRON_CMD="${CRON_EXPR} cd ${SCRIPT_DIR} && ${PYTHON_PATH} ${SCANNER} --days 2 >> ${LOG_DIR}/scanner_\$(date +\%Y\%m\%d).log 2>&1"

echo "CEF News Scanner — Cron Setup"
echo "=============================="
echo ""
echo "Script:    ${SCANNER}"
echo "Python:    ${PYTHON_PATH}"
echo "Logs:      ${LOG_DIR}/"
echo "Schedule:  Weekdays at 7:00 AM ET (12:00 UTC)"
echo ""
echo "Cron entry:"
echo "  ${CRON_CMD}"
echo ""

# Check if already installed
if crontab -l 2>/dev/null | grep -q "cef_news_scanner.py"; then
    echo "⚠ A cron job for cef_news_scanner.py already exists."
    echo "  Current crontab entries:"
    crontab -l | grep "cef_news_scanner"
    echo ""
    read -p "Replace existing entry? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
    # Remove old entry
    crontab -l | grep -v "cef_news_scanner.py" | crontab -
fi

# Install new cron job
(crontab -l 2>/dev/null; echo "${CRON_CMD}") | crontab -

echo "✓ Cron job installed successfully."
echo ""
echo "To verify:  crontab -l"
echo "To remove:  crontab -l | grep -v 'cef_news_scanner' | crontab -"
echo ""

# Also add a weekly deep scan (7-day lookback) on Sundays at 8 AM ET
WEEKLY_CMD="0 13 * * 0 cd ${SCRIPT_DIR} && ${PYTHON_PATH} ${SCANNER} --days 7 >> ${LOG_DIR}/scanner_weekly_\$(date +\%Y\%m\%d).log 2>&1"

read -p "Also install weekly deep scan (7-day lookback on Sundays)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    (crontab -l 2>/dev/null; echo "${WEEKLY_CMD}") | crontab -
    echo "✓ Weekly deep scan installed (Sundays at 8:00 AM ET)."
fi

echo ""
echo "Done. Make sure your .env file has SUPABASE_SERVICE_KEY and FINNHUB_API_KEY set."
