#!/bin/bash
# ============================================================
# IB AutoTrader — Daily Cron Wrapper
# Runs ib_autotrader.py at 8:00 CT Mon-Fri via crontab
#
# CRONTAB LINE (install with: crontab -e):
#   0 8 * * 1-5 /Users/kevinheaney/Desktop/Claude_Programs/Trading_Programs/ib_execution/run_ib_autotrader.sh
#
# TO GO LIVE (after 20 paper trades):
#   Edit this file and remove --dry-run from the python3 line at the bottom
# ============================================================

# Set PATH so macOS cron can find python3 (covers Homebrew + system locations)
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

# Script directory
SCRIPT_DIR="/Users/kevinheaney/Desktop/Claude_Programs/Trading_Programs/ib_execution"

# Log file — rolling, capped at 500 lines
LOG_FILE="$SCRIPT_DIR/cron_run.log"

# Rotate log if over 500 lines
if [ -f "$LOG_FILE" ] && [ $(wc -l < "$LOG_FILE") -gt 500 ]; then
    tail -400 "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
fi

# Log header
echo "" >> "$LOG_FILE"
echo "============================================================" >> "$LOG_FILE"
echo "CRON RUN: $(date '+%Y-%m-%d %H:%M:%S %Z')" >> "$LOG_FILE"
echo "============================================================" >> "$LOG_FILE"

# Run autotrader — REMOVE --dry-run when ready to go live
cd "$SCRIPT_DIR" && python3 ib_autotrader.py --dry-run -v >> "$LOG_FILE" 2>&1

echo "Exit code: $?" >> "$LOG_FILE"
