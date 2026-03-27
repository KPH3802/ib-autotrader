# IB Auto-Trader — Equity Signal Execution Layer

Reads nightly signals from three sources — 8-K scanner emails, Form4 insider activity, and Dividend Cut scanner emails — applies playbook rules, and submits market orders via the IBKR Client Portal Web API. Includes a 60-day position tracker for long dividend cut positions.

## Signal Sources

| Source | Direction | Trigger |
|--------|-----------|---------|
| 8-K Item 1.01 | SHORT | Score >= 2, parsed from Gmail IMAP |
| Dividend Cut Score 3+ | BUY | Net score >= 3, parsed from Gmail IMAP, 60-day hold |
| Form4 Insider Buy Cluster | BUY | 2+ insiders buying same ticker within 14 days |
| Form4 Insider Sell S1 | SHORT | Officer+Director selling $250K-$5M |
| Form4 Insider Sell S2 | SHORT | Officer OR Director selling $250K-$5M |

## Playbook Rules

- **VIX Kill Switch:** Skip ALL trades if VIX >= 30
- **Position Sizing:** Score 2 = $2,500 (half), Score 3+ = $5,000 (full)
- **M&A Filter:** Skip 8-K shorts if acquisition/merger news detected via yfinance
- **Div Cut Exits:** Day 60 time exit OR -40% catastrophic circuit breaker. No stop loss. No profit target.
- **Max orders per run:** 10 (configurable)

## Prerequisites

1. **IBKR Client Portal Gateway** running locally on port 7462.
   - Download from: https://www.interactivebrokers.com/en/trading/ib-api.php
   - Unzip to `clientportal_new/`, edit `root/conf.yaml` to set port 7462
   - Run via the included `Start_IB_Gateway.command` desktop script
   - Log in via browser at https://localhost:7462

2. **Paper or live trading account** — log into the gateway with your IBKR credentials.

3. **PA scanners running nightly** — 8-K scanner (21:45 UTC), Form4 scanner (22:00 UTC), Dividend Cut scanner (23:30 UTC) must be active on PythonAnywhere.

## Setup

```bash
cd ib_execution/
pip3 install -r requirements.txt
cp config_example.py config.py
# Edit config.py with your email, IBKR, and path settings
```

## Usage

```bash
# Dry run — log signals and size positions, no orders placed
python3 ib_autotrader.py --dry-run

# Verbose dry run — full detail
python3 ib_autotrader.py --dry-run -v

# Live execution — places real orders
python3 ib_autotrader.py
```

## Automated Daily Execution (macOS cron)

`run_ib_autotrader.sh` is a cron wrapper that fires at 8:00 CT Monday–Friday. Install with:

```bash
chmod +x run_ib_autotrader.sh
(crontab -l 2>/dev/null; echo "0 8 * * 1-5 /path/to/ib_execution/run_ib_autotrader.sh") | crontab -
```

Output is logged to `cron_run.log` (auto-rotates at 500 lines). To go live, remove `--dry-run` from the last line of the script.

**Note:** The IBKR Gateway and browser login must be completed manually before the cron job fires — authentication cannot be automated.

## Output

- **trade_log.csv** — every signal processed (executed or skipped), appended per run
- **positions.db** — open dividend cut positions tracked for exit management
- **cron_run.log** — automated run log
- **Email summary** — sent after each run with orders placed, signals skipped, and positions closed

## File Structure

```
ib_execution/
  ib_autotrader.py        # Main script
  run_ib_autotrader.sh    # Cron wrapper for automated daily execution
  config_example.py       # Template — copy to config.py
  config.py               # Your settings (git-ignored)
  trade_log.csv           # Trade log (git-ignored)
  positions.db            # Open position tracker (git-ignored)
  cron_run.log            # Cron run log (git-ignored)
  requirements.txt
  README.md
  .gitignore
```

## How It Works

1. Fetch VIX via yfinance — abort all trades if >= 30
2. Check open div cut positions for Day 60 exits or -40% circuit breaker closes
3. Parse today's 8-K SHORT email from Gmail IMAP (7-day fallback)
4. Query Form4 DB for today's insider buy clusters and S1/S2 sells
5. Parse today's Dividend Cut ALERT email from Gmail IMAP (Score 3+ only)
6. Deduplicate signals (same ticker+direction keeps highest score)
7. Filter div cut signals for already-open positions
8. For each signal: M&A check, fetch live price, calculate shares, look up IB contract ID
9. Place market orders via IB Client Portal REST API
10. Log entries to trade_log.csv and positions.db
11. Send summary email with all executed orders and any closed positions

## Backtest Performance

| Signal | Alpha | Basis |
|--------|-------|-------|
| 8-K Item 1.01 shorts | -2.89% at 5d (t=-9.98) | 2020–2025, n=500+ |
| Dividend Cut Score 3+ longs | +15.77% at 60d | 2020–2025, era-validated |
| Insider buy clusters | +0.60% per trade | 2020–2025, clean |

## Disclaimer

This software is for educational and research purposes. Trading involves substantial risk of loss. Past backtest performance does not guarantee future results.

MIT License
