# IB Auto-Trader -- Equity Signal Execution Layer

Reads tonight's signals from 8 systematic sources, applies playbook rules, and submits orders to Interactive Brokers via the IBKR Client Portal Web API. Full position lifecycle management with per-signal exit rules, two-tier daily loss limits, and SPY-relative alpha tracking.

## Signal Sources

| Source | Direction | Hold | Trigger |
|--------|-----------|------|---------|
| 8-K Item 1.01 | SHORT | 5d | Score >= 2, parsed from Gmail IMAP |
| PEAD BULL/BEAR | BUY/SHORT | 28d | Score 3, parsed from Gmail IMAP |
| Form4 Insider Buy Cluster | BUY | 5d | 2+ insiders buying same ticker within 14d |
| Form4 Insider Sell S1/S2 | SHORT | 5d | Officer+Director selling \$250K-\$5M |
| SI Squeeze | BUY | 28d | Score 3, parsed from Gmail IMAP |
| COT BULL/BEAR | BUY/SHORT | 56d | Wheat BEAR validated; parsed from Gmail IMAP |
| CEL BEAR | SHORT | 5d | Commodity-equity lag, parsed from Gmail IMAP |
| 13F Institutional BULL | BUY | 91d | Score 3, parsed from Gmail IMAP, quarterly cadence |
| Dividend Cut Score 3+ | BUY | 60d | SUSPENDED Apr 11 2026 -- broad-universe alpha collapse, scanner runs for data only |

## Playbook Rules

- **VIX Kill Switch:** Skip ALL trades if VIX >= 30 (configurable as VIX_KILL_SWITCH).
- **VIX Half-Sizing:** When VIX >= 25 (VIX_WARN), all positions enter at half their score-based size.
- **Position Sizing (percentage of NLV):** Score 2 = 3 percent, Score 3 = 5 percent, Score 4-5 = 8 percent. Sizes are computed against live IB Gateway account value, not a hardcoded constant.
- **Maximum total open positions:** 20 across all signals (MAX_TOTAL_OPEN_POSITIONS). Cap-bound days log skipped signals to missed_trades.csv for later analysis.
- **M&A Filter:** Skip 8-K SHORT signals if acquisition/merger news is detected via yfinance.
- **Per-Signal Exits:** DAY_5 (8-K, Form4, CEL), DAY_28 (PEAD, SI Squeeze), DAY_56 (COT), DAY_60 (DIV_CUT, currently suspended), DAY_91 (13F). Time exits run at the top of every daily execution before processing new entries.
- **Catastrophic Circuit Breaker:** -40 percent absolute return on any open position triggers immediate close + Pushover + email alert.
- **Daily Loss Limit (two tier):** Tier 1 (LOSS_WARN_PCT, 15 percent of deployed capital): warning email. Tier 2 (LOSS_HALT_PCT, 8 percent of total account): create loss_limit_halt.flag and SMS alert; new entries halted until flag is manually deleted.
- **Gateway-Fail Halt:** If IB Gateway cannot return an account value, the trader sends a [GMC CRITICAL] alert and raises RuntimeError rather than falling back to a hardcoded value. Prevents oversizing during gateway outages.

## Prerequisites

1. **IBKR Client Portal Gateway** running locally on port 7462.
   - Download from https://www.interactivebrokers.com/en/trading/ib-api.php
   - Unzip to clientportal_new/, edit root/conf.yaml to set port 7462.
   - Daily auto-restart at 6:00 CT Mon-Fri via launchd agent (see gmc-infra repo).
   - Manual 2FA required after each restart -- authentication cannot be automated.
2. **Paper or live trading account** -- log into the gateway with your IBKR credentials.
3. **PA scanners running nightly** -- 8-K (21:45 UTC), PEAD (01:30 UTC), SI Squeeze (01:45 UTC), CEL (02:00 UTC), 13F (03:00 UTC, currently disabled), COT (Fri 21:00 UTC), DIV scanners on PythonAnywhere. Form4 scanner runs locally on Mac Studio (07:50 CT).

## Setup

```bash
cd ib_execution/
pip3 install -r requirements.txt
cp config_example.py config.py
# Edit config.py with your email, IBKR, Pushover, Healthchecks.io, and path settings
```

## Usage

```bash
# Dry run -- log signals and size positions, no orders placed
python3 ib_autotrader.py --dry-run

# Verbose dry run -- full detail
python3 ib_autotrader.py --dry-run -v

# Live execution -- places real orders
python3 ib_autotrader.py
```

## Automated Daily Execution (macOS cron)

The cron wrapper run_ib_autotrader.sh lives in the companion infrastructure repo [gmc-infra](https://github.com/KPH3802/gmc-infra) and runs at 8:00 CT Monday-Friday. It sets PATH, rotates the log file, and invokes python3 ib_autotrader.py --dry-run -v from this directory.

Runtime path (installed by gmc-infra):

```bash
crontab -l  # shows:  0 8 * * 1-5 /bin/bash /Users/<user>/run_ib_autotrader.sh
```

Output is logged to ~/gmc_cron_run.log (auto-rotates at 500 lines). To go live, remove --dry-run from the invocation line in gmc-infra/scripts/run_ib_autotrader.sh, then copy the updated file to ~/run_ib_autotrader.sh.

**Why the wrapper and its log live outside this directory:** The wrapper lived alongside the script until Apr 24 2026, but iCloud sync contention with concurrent backup processes caused bash to hit kernel EDEADLK on the wrapper file read, silently killing the cron run. Relocation to ~/ (non-iCloud) fixed it. The cron_run.log was relocated to ~/gmc_cron_run.log Apr 29 2026 for the same reason -- writes to iCloud-synced log files hit EDEADLK during the morning Time Machine + Backblaze window. Canonical wrapper source is version-controlled in gmc-infra.

## Position Tracking + Exit Management

positions.db (SQLite, located at ~/gmc_data/positions.db outside iCloud) tracks every open position across all 8 signal sources with full lifecycle metadata:

- entry_date, entry_price, shares, position_size
- source (8K_1.01, PEAD_BULL, etc.), direction (BUY / SHORT)
- expected_return_pct, expected_hold_days (from signal_benchmarks lookup)
- spy_entry_price (for SPY-relative alpha tracking on close)
- close_date, close_price, close_reason, return_pct, alpha_vs_spy

Schema initialization is idempotent -- init_positions_db() runs ALTER TABLE migrations on every fire to handle older DB schemas. The signal_benchmarks table provides expected return and hold-period defaults per signal source.

Backed up to PythonAnywhere via API after every successful cron run. Exit logic computes return percent and SPY-relative alpha for every closed position.

## Output

- ~/gmc_data/trade_log.csv -- every signal processed (executed or skipped), appended per run
- ~/gmc_data/positions.db -- open positions across all 8 sources tracked for exit management
- ~/gmc_data/missed_trades.csv -- cap-bound signals skipped during 20/20 days
- ~/gmc_cron_run.log -- automated run log
- Email summary sent after each run with orders placed, signals skipped, positions closed (and per-position alpha vs SPY for closes)

## File Structure

```
ib_execution/
  ib_autotrader.py        # Main script (~2500 lines)
  config_example.py       # Template -- copy to config.py
  config.py               # Your settings (git-ignored)
  clientportal_new/       # IBKR gateway (git-ignored)
  requirements.txt
  README.md
  .gitignore

External runtime files (outside iCloud, not in this repo):
  ~/gmc_data/positions.db
  ~/gmc_data/trade_log.csv
  ~/gmc_data/missed_trades.csv
  ~/gmc_cron_run.log
  ~/run_ib_autotrader.sh  # Wrapper, canonical in gmc-infra
```

## How It Works

1. Init positions DB (idempotent schema migrations).
2. Scanner watchdog -- check that each upstream PA scanner has emitted recently; warn on stale ones.
3. VIX check (yfinance with multi-source fallback) -- abort all trades if VIX >= VIX_KILL.
4. Daily loss limit check (two tier).
5. IB Gateway check + account value pull.
6. Holiday check (NYSE calendar).
7. Process exits FIRST: walk every OPEN position, fetch live price, compute days held + return percent, fire DAY_N or CATASTROPHIC_BREAKER closes; capture spy_return and alpha_vs_spy on close.
8. Gather signals from all 8 sources (Gmail IMAP for 7 of them, local F4 DB for Form4).
9. Deduplicate (same ticker+direction keeps highest score) and filter (already-open, capital cap).
10. For each surviving signal: M&A check (8-K SHORT only), fetch live price, calculate shares, look up IB contract ID.
11. Place market orders via IB Client Portal REST API.
12. Log entries to trade_log.csv and positions.db; emit Pushover + email alerts on entries above priority threshold.
13. Backup positions.db to PythonAnywhere.
14. Send summary email with all executed orders, closed positions, and SPY-relative alpha for closes.

## Backtest Performance

| Signal | Alpha | Basis |
|--------|-------|-------|
| 8-K Item 1.01 SHORT | +3.17 percent at 5d (t=-9.98) | 2020-2025, n=500+ |
| PEAD BULL/BEAR | +4.24 percent at 28d | 2020-2025 |
| SI Squeeze | +1.70 percent at 28d | 2020-2025 |
| 13F Institutional BULL | +9.97 percent at 91d | 2020-2025 |
| COT Wheat BEAR | +8.41 percent monotonic (t=0.003) | 2020-2025 |
| CEL BEAR | marginally positive | 2020-2025 |
| Form4 Insider Buy Cluster | clean positive | 2020-2025 |
| Dividend Cut Score 3+ | -2.15 percent at 60d (broad universe) | SUSPENDED -- large-cap filter required |

## Alerting

Three-layer alerting matrix:

- **Layer A (Pushover):** instant phone alert on entries above priority threshold, exits, halt conditions.
- **Layer B (parallel email):** backup channel firing alongside Layer A on priority=high.
- **Layer C (Healthchecks.io heartbeat):** out-of-band watchdog hourly; fires if Mac Studio is unreachable, Tailscale is down, or cron is broken.

## Disclaimer

This software is for educational and research purposes. Trading involves substantial risk of loss. Past backtest performance does not guarantee future results.

MIT License
