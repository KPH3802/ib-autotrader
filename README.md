# IB Auto-Trader — Equity Signal Execution Layer

Reads tonight's signals from the 8-K and Form4 scanner databases, applies playbook rules, and submits market orders via the IBKR Client Portal Web API.

## Signal Sources

| Source | Direction | Trigger |
|--------|-----------|---------|
| 8-K Item 1.01 | SHORT | `signal_score >= 2`, today's scan |
| Form4 Insider Buy Cluster | BUY | 2+ insiders buying same ticker within 14 days |
| Form4 Insider Sell S1 | SHORT | Officer+Director selling $250K-$5M |
| Form4 Insider Sell S2 | SHORT | Officer OR Director selling $250K-$5M |

## Playbook Rules

- **VIX Kill Switch:** Skip ALL trades if VIX >= 30
- **Position Sizing:** Score 2 = $2,500 (half), Score 3+ = $5,000 (full)
- **S1 sells** map to Score 3 (full size), **S2 sells** map to Score 2 (half size)
- **Buy clusters** map to Score 3 (full size)
- **Max orders per run:** 10 (configurable)

## Prerequisites

1. **IBKR Client Portal Gateway** must be running locally on port 5000.
   - Download from: https://www.interactivebrokers.com/en/trading/ib-api.php
   - Unzip, then run: `bin/run.sh root/conf.yaml`
   - Log in via browser at https://localhost:5000
   - The gateway uses a self-signed SSL certificate (the script handles this)

2. **Paper trading account** — log into the gateway with your paper account credentials.

3. **Scanner databases** — the 8-K scanner (21:45 UTC) and Form4 scanner (22:00 UTC) must have already run tonight.

## Setup

```bash
cd ib_execution/
pip3 install -r requirements.txt
cp config_example.py config.py
# Edit config.py with your email and IB settings
```

## Usage

```bash
# Dry run — see what would trade, no orders placed
python3 ib_autotrader.py --dry-run

# Verbose dry run — full detail
python3 ib_autotrader.py --dry-run -v

# Live execution — places real orders
python3 ib_autotrader.py
```

## Output

- **trade_log.csv** — every signal processed (executed or skipped), appended per run
- **Email summary** — sent after each run with orders placed and signals skipped

## File Structure

```
ib_execution/
  ib_autotrader.py      # Main script
  config_example.py     # Template — copy to config.py
  config.py             # Your settings (git-ignored)
  trade_log.csv         # Trade log (git-ignored)
  requirements.txt
  README.md
  .gitignore
```

## How It Works

1. Fetch VIX via yfinance — abort if >= 30
2. Query `eight_k_filings.db` for today's SHORT signals (score >= 2)
3. Query `form4_insider_trades.db` for today's BUY clusters and S1/S2 sells
4. Deduplicate (same ticker+direction keeps highest score)
5. For each signal: fetch live price, calculate shares, look up IB contract ID
6. Place market orders via IB Client Portal REST API
7. Log everything to trade_log.csv
8. Send summary email
