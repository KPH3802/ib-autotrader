#!/usr/bin/env python3
"""
GMC Performance Tracker — Phase 2

Read-only performance summary across all strategy books:
  - Bedrock (manual IB positions, hardcoded)
  - Event Alpha (positions.db)
  - Digital Alpha (Coinbase)

Usage:
  python3 performance_tracker.py              # Print formatted JSON
  from performance_tracker import get_performance_summary  # Importable
"""

import json
import logging
import os
import sqlite3
import sys
import threading
from datetime import date, datetime, timedelta

import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration — same sys.path.insert pattern as codebase
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
import config

# News digest config for Coinbase CDP key
NEWS_DIGEST_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "news_digest")
sys.path.insert(0, NEWS_DIGEST_DIR)
try:
    import config as nd_config
except ImportError:
    nd_config = None

POSITIONS_DB = config.POSITIONS_DB
FMP_API_KEY = config.FMP_API_KEY
COINBASE_CDP_KEY = getattr(nd_config, "COINBASE_CDP_KEY", None) if nd_config else None
MAX_TOTAL_OPEN = getattr(config, "MAX_TOTAL_OPEN_POSITIONS", 20)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Bedrock hardcoded positions (manual IB, NOT in positions.db)
# ---------------------------------------------------------------------------
BEDROCK_ENTRY_DATE = "2026-04-07"
BEDROCK_HOLDINGS = [
    {"ticker": "ASML", "shares": 1, "cost_per_share": 1291.44},
    {"ticker": "TSM",  "shares": 3, "cost_per_share": 340.33},
    {"ticker": "ANET", "shares": 9, "cost_per_share": 127.96},
    {"ticker": "VRT",  "shares": 5, "cost_per_share": 257.17},
]
BEDROCK_TOTAL_COST = 4749.94

# Digital Alpha constants
DIGITAL_ALPHA_COST_BASIS = 5000
DIGITAL_ALPHA_FUNDING_DATE = "2026-03-09"
USDC_STAKING_APY = 0.035


# ---------------------------------------------------------------------------
# Price fetching — batch yfinance primary, 3-source fallback per ticker
# ---------------------------------------------------------------------------

def _batch_fetch_yfinance(tickers, timeout=30):
    """Batch fetch current prices via yf.download(). Returns {ticker: price}."""
    import pandas
    prices = {}
    if not tickers:
        return prices

    result = [None]
    def _fetch():
        try:
            df = yf.download(tickers, period="5d", progress=False, threads=True)
            if df is not None and not df.empty:
                result[0] = df
        except Exception as e:
            logger.warning(f"yf.download batch failed: {e}")

    t = threading.Thread(target=_fetch, daemon=True)
    t.start()
    t.join(timeout=timeout)

    df = result[0]
    if df is None:
        return prices

    try:
        if isinstance(df.columns, pandas.MultiIndex):
            # Multi-ticker: columns are (Price, Ticker)
            close_cols = df["Close"] if "Close" in df.columns.get_level_values(0) else None
            if close_cols is not None:
                for tk in tickers:
                    try:
                        col = close_cols[tk] if tk in close_cols.columns else None
                        if col is not None and not col.dropna().empty:
                            prices[tk] = float(col.dropna().iloc[-1])
                    except Exception:
                        pass
        else:
            # Single ticker
            cols = df.columns
            if isinstance(cols, pandas.MultiIndex):
                cols = [c[0] for c in cols]
                df.columns = cols
            if "Close" in df.columns and not df["Close"].dropna().empty:
                prices[tickers[0]] = float(df["Close"].dropna().iloc[-1])
    except Exception as e:
        logger.warning(f"yf.download parse failed: {e}")

    return prices


def _fetch_yahoo_direct(ticker):
    """Fallback: Yahoo Finance direct HTTP."""
    try:
        encoded = requests.utils.quote(ticker)
        r = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        return float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception:
        return None


def _fetch_fmp(ticker):
    """Fallback: FMP quote."""
    try:
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={FMP_API_KEY}",
            timeout=10,
        )
        data = r.json()
        if data and isinstance(data, list) and "price" in data[0]:
            return float(data[0]["price"])
    except Exception:
        pass
    return None


def fetch_prices(tickers):
    """Fetch current prices for a list of tickers. Returns {ticker: price_or_None}."""
    if not tickers:
        return {}

    unique = list(set(tickers))
    prices = _batch_fetch_yfinance(unique)

    # Fill missing with fallbacks
    for tk in unique:
        if tk not in prices or prices[tk] is None:
            p = _fetch_yahoo_direct(tk)
            if p is not None:
                prices[tk] = p
                continue
            p = _fetch_fmp(tk)
            if p is not None:
                prices[tk] = p
            else:
                prices[tk] = None
                logger.warning(f"Price fetch failed all 3 sources for {tk}")

    return prices


def fetch_historical_price(ticker, target_date_str):
    """Fetch close price on a specific date. Returns float or None."""
    try:
        dt = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        start = dt - timedelta(days=5)
        end = dt + timedelta(days=5)

        result = [None]
        def _fetch():
            try:
                import pandas
                df = yf.download(ticker, start=start.isoformat(), end=end.isoformat(),
                                 progress=False)
                if df is not None and not df.empty:
                    if isinstance(df.columns, pandas.MultiIndex):
                        df.columns = [c[0] for c in df.columns]
                    # Get closest date <= target
                    valid = df[df.index.date <= dt]
                    if not valid.empty:
                        result[0] = float(valid["Close"].iloc[-1])
                    elif not df.empty:
                        result[0] = float(df["Close"].iloc[0])
            except Exception:
                pass

        t = threading.Thread(target=_fetch, daemon=True)
        t.start()
        t.join(timeout=15)
        return result[0]
    except Exception:
        return None


def fetch_historical_return_pct(ticker, from_date_str):
    """Calculate return from from_date to now. Returns pct or None."""
    hist_price = fetch_historical_price(ticker, from_date_str)
    if hist_price is None:
        return None
    prices = fetch_prices([ticker])
    current = prices.get(ticker)
    if current is None:
        return None
    return round(((current - hist_price) / hist_price) * 100, 2)


# ---------------------------------------------------------------------------
# Crypto — Coinbase via coinbase-advanced-py (same pattern as news_digest.py)
# ---------------------------------------------------------------------------

def get_crypto_portfolio_value():
    """Get total Coinbase portfolio value in USD. Returns (value, positions_detail) or (None, [])."""
    if COINBASE_CDP_KEY is None:
        logger.warning("Coinbase CDP key not configured")
        return None, []

    try:
        from coinbase.rest import RESTClient
        key_path = os.path.join(NEWS_DIGEST_DIR, COINBASE_CDP_KEY)
        with open(key_path) as f:
            creds = json.load(f)
        client = RESTClient(api_key=creds["name"], api_secret=creds["privateKey"])
        accounts = client.get_accounts()

        total_usd = 0.0
        positions = []
        for acct in accounts.accounts:
            ab = acct.available_balance
            if isinstance(ab, dict):
                balance = float(ab.get("value", 0))
                currency = ab.get("currency", "")
            else:
                balance = float(getattr(ab, "value", 0))
                currency = getattr(ab, "currency", "")

            if balance <= 0.0001:
                continue

            if currency in ("USD", "USDC"):
                total_usd += balance
                positions.append({"currency": currency, "balance": balance, "usd_value": balance})
            else:
                # Get USD value
                try:
                    product_id = f"{currency}-USD"
                    product = client.get_product(product_id)
                    price = float(product.price) if hasattr(product, 'price') else None
                    if price:
                        usd_val = balance * price
                        total_usd += usd_val
                        positions.append({"currency": currency, "balance": balance,
                                          "usd_value": round(usd_val, 2), "price": price})
                except Exception:
                    positions.append({"currency": currency, "balance": balance,
                                      "usd_value": None})

        return round(total_usd, 2), positions
    except Exception as e:
        logger.warning(f"Coinbase fetch failed: {e}")
        return None, []


# ---------------------------------------------------------------------------
# Database reads (read-only)
# ---------------------------------------------------------------------------

def _read_positions():
    """Read all positions from positions.db. Returns (open_list, closed_list)."""
    if not os.path.exists(POSITIONS_DB):
        logger.warning(f"positions.db not found: {POSITIONS_DB}")
        return [], []

    try:
        conn = sqlite3.connect(POSITIONS_DB)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT ticker, direction, entry_date, entry_price, shares, position_size,
                   source, status, close_date, close_price, close_reason,
                   return_pct, expected_return_pct, expected_hold_days, score,
                   spy_entry_price, vs_expected_pct, spy_return_pct, alpha_vs_spy
            FROM open_positions
        """)
        rows = c.fetchall()
        conn.close()

        open_pos = [dict(r) for r in rows if r["status"] == "OPEN"]
        closed_pos = [dict(r) for r in rows if r["status"] == "CLOSED"]
        return open_pos, closed_pos
    except Exception as e:
        logger.error(f"Failed to read positions.db: {e}")
        return [], []


def _read_benchmarks():
    """Read signal_benchmarks table. Returns {(source, direction): {expected_return_pct, expected_hold_days}}."""
    benchmarks = {}
    if not os.path.exists(POSITIONS_DB):
        return benchmarks
    try:
        conn = sqlite3.connect(POSITIONS_DB)
        c = conn.cursor()
        c.execute("SELECT source, direction, expected_return_pct, expected_hold_days FROM signal_benchmarks")
        for row in c.fetchall():
            benchmarks[(row[0], row[1])] = {
                "expected_return_pct": row[2],
                "expected_hold_days": row[3],
            }
        conn.close()
    except Exception as e:
        logger.warning(f"Failed to read signal_benchmarks: {e}")
    return benchmarks


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _safe_round(val, decimals=2):
    """Round if not None, else return None."""
    if val is None:
        return None
    return round(val, decimals)


def _safe_pct(numerator, denominator):
    """Safe division returning percentage or None."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round((numerator / denominator) * 100, 2)


def build_portfolio_header(open_pos):
    """Section 1: Portfolio header."""
    count = len(open_pos)
    return {
        "as_of_date": date.today().isoformat(),
        "open_positions_count": count,
        "max_positions": MAX_TOTAL_OPEN,
        "slots_free": MAX_TOTAL_OPEN - count,
    }


def build_bedrock(prices):
    """Section 2: Bedrock portfolio."""
    entry_dt = datetime.strptime(BEDROCK_ENTRY_DATE, "%Y-%m-%d").date()
    days_held = (date.today() - entry_dt).days

    holdings = []
    total_current = 0.0
    for h in BEDROCK_HOLDINGS:
        tk = h["ticker"]
        cost = h["shares"] * h["cost_per_share"]
        current_price = prices.get(tk)
        if current_price is not None:
            current_val = h["shares"] * current_price
            ret_dollar = current_val - cost
            ret_pct = ((current_price - h["cost_per_share"]) / h["cost_per_share"]) * 100
            total_current += current_val
        else:
            current_val = None
            ret_dollar = None
            ret_pct = None

        holdings.append({
            "ticker": tk,
            "shares": h["shares"],
            "cost_per_share": h["cost_per_share"],
            "cost_basis": round(cost, 2),
            "current_price": _safe_round(current_price),
            "current_value": _safe_round(current_val),
            "return_pct": _safe_round(ret_pct),
            "return_dollar": _safe_round(ret_dollar),
        })

    total_return_dollar = round(total_current - BEDROCK_TOTAL_COST, 2) if total_current else None
    total_return_pct = _safe_round(
        ((total_current - BEDROCK_TOTAL_COST) / BEDROCK_TOTAL_COST) * 100
    ) if total_current else None

    # SPY/QQQ benchmark from Bedrock entry date
    spy_ret = fetch_historical_return_pct("SPY", BEDROCK_ENTRY_DATE)
    qqq_ret = fetch_historical_return_pct("QQQ", BEDROCK_ENTRY_DATE)

    return {
        "cost_basis": BEDROCK_TOTAL_COST,
        "current_value": _safe_round(total_current) if total_current else None,
        "return_pct": total_return_pct,
        "return_dollar": total_return_dollar,
        "days_held": days_held,
        "spy_return_pct": spy_ret,
        "qqq_return_pct": qqq_ret,
        "holdings": holdings,
    }


def build_event_alpha(open_pos, closed_pos, prices, benchmarks):
    """Section 3: Event Alpha aggregate stats."""
    today = date.today()

    # Compute return for each closed trade (use stored return_pct, or recalc if NULL)
    closed_returns = []
    closed_spy_returns = []
    for pos in closed_pos:
        ret = pos["return_pct"]
        if ret is not None:
            closed_returns.append(ret)
        spy_ret = pos["spy_return_pct"]
        if spy_ret is not None:
            closed_spy_returns.append(spy_ret)

    # Compute return for each open trade (live)
    open_returns = []
    for pos in open_pos:
        tk = pos["ticker"]
        entry_price = pos["entry_price"]
        direction = pos["direction"] or "BUY"
        current = prices.get(tk)
        if current is None or entry_price is None:
            continue
        if direction == "SHORT":
            ret = ((entry_price - current) / entry_price) * 100
        else:
            ret = ((current - entry_price) / entry_price) * 100
        open_returns.append(ret)

    all_returns = closed_returns  # win rate based on closed only
    total_closed = len(closed_returns)
    wins = sum(1 for r in closed_returns if r > 0)
    win_rate = _safe_pct(wins, total_closed) if total_closed > 0 else None

    avg_return = _safe_round(sum(closed_returns) / len(closed_returns)) if closed_returns else None
    total_return_dollar = None  # Would need position sizes; approximate from closed_pos
    total_dollar = 0.0
    has_dollar = False
    for pos in closed_pos:
        ret = pos["return_pct"]
        psize = pos["position_size"]
        if ret is not None and psize is not None:
            total_dollar += (ret / 100) * psize
            has_dollar = True
    total_return_dollar = _safe_round(total_dollar) if has_dollar else None

    avg_spy_return = _safe_round(
        sum(closed_spy_returns) / len(closed_spy_returns)
    ) if closed_spy_returns else None

    alpha = _safe_round(avg_return - avg_spy_return) if (avg_return is not None and avg_spy_return is not None) else None

    return {
        "closed_trades": total_closed,
        "open_trades": len(open_pos),
        "win_rate_pct": win_rate,
        "avg_return_pct": avg_return,
        "total_return_dollar": total_return_dollar,
        "spy_avg_return_pct": avg_spy_return,
        "alpha_vs_spy": alpha,
    }


def build_signal_scorecard(open_pos, closed_pos, prices, benchmarks):
    """Section 4: Per-signal stats."""
    today = date.today()
    import statistics

    # Group all trades by source
    signal_data = {}
    for pos in closed_pos + open_pos:
        src = pos["source"] or "UNKNOWN"
        if src not in signal_data:
            signal_data[src] = {"closed": [], "open": []}
        if pos["status"] == "CLOSED":
            signal_data[src]["closed"].append(pos)
        else:
            signal_data[src]["open"].append(pos)

    scorecard = {}
    for src, data in signal_data.items():
        closed = data["closed"]
        open_trades = data["open"]

        # Returns from closed trades
        closed_returns = [p["return_pct"] for p in closed if p["return_pct"] is not None]
        closed_spy_returns = [p["spy_return_pct"] for p in closed if p["spy_return_pct"] is not None]

        total_trades = len(closed) + len(open_trades)
        n_closed = len(closed_returns)
        wins = sum(1 for r in closed_returns if r > 0)
        win_rate = _safe_pct(wins, n_closed) if n_closed > 0 else None
        avg_ret = _safe_round(sum(closed_returns) / n_closed) if n_closed > 0 else None

        # Look up benchmark
        direction = None
        for p in closed + open_trades:
            if p.get("direction"):
                direction = p["direction"]
                break
        bm = benchmarks.get((src, direction), {})
        expected_ret = bm.get("expected_return_pct")
        expected_hold = bm.get("expected_hold_days")

        avg_vs_expected = _safe_round(avg_ret - expected_ret) if (avg_ret is not None and expected_ret is not None) else None

        avg_spy = _safe_round(
            sum(closed_spy_returns) / len(closed_spy_returns)
        ) if closed_spy_returns else None
        avg_alpha = _safe_round(avg_ret - avg_spy) if (avg_ret is not None and avg_spy is not None) else None

        # Drift alert: only flag if 5+ closed trades AND avg < (benchmark - 1.5*stdev)
        drift_alert = False
        if n_closed >= 5 and expected_ret is not None and avg_ret is not None:
            try:
                stdev = statistics.stdev(closed_returns)
                if avg_ret < (expected_ret - 1.5 * stdev):
                    drift_alert = True
            except Exception:
                pass

        scorecard[src] = {
            "trades": total_trades,
            "closed": n_closed,
            "open": len(open_trades),
            "win_rate_pct": win_rate,
            "avg_return_pct": avg_ret,
            "avg_expected_pct": expected_ret,
            "avg_vs_expected": avg_vs_expected,
            "avg_alpha_vs_spy": avg_alpha,
            "drift_alert": drift_alert,
        }

    return scorecard


def build_open_positions_detail(open_pos, prices, benchmarks):
    """Section 5: Per-position detail for open trades."""
    today = date.today()
    details = []

    for pos in open_pos:
        tk = pos["ticker"]
        direction = pos["direction"] or "BUY"
        entry_price = pos["entry_price"]
        source = pos["source"] or "UNKNOWN"
        entry_date_str = pos["entry_date"]

        try:
            entry_dt = datetime.strptime(entry_date_str, "%Y-%m-%d").date()
            days_held = (today - entry_dt).days
        except Exception:
            days_held = None

        current_price = prices.get(tk)

        # Return calculation
        if current_price is not None and entry_price is not None and entry_price != 0:
            if direction == "SHORT":
                return_pct = round(((entry_price - current_price) / entry_price) * 100, 2)
            else:
                return_pct = round(((current_price - entry_price) / entry_price) * 100, 2)
        else:
            return_pct = None

        # Use stored expected values, fall back to benchmarks table
        expected_return_pct = pos.get("expected_return_pct")
        expected_hold_days = pos.get("expected_hold_days")
        if expected_return_pct is None or expected_hold_days is None:
            bm = benchmarks.get((source, direction), {})
            if expected_return_pct is None:
                expected_return_pct = bm.get("expected_return_pct")
            if expected_hold_days is None:
                expected_hold_days = bm.get("expected_hold_days")

        # Pro-rata expected return
        prorata_expected_pct = None
        vs_prorata = None
        if days_held is not None and expected_hold_days and expected_return_pct is not None:
            if expected_hold_days > 0:
                prorata_expected_pct = round((days_held / expected_hold_days) * expected_return_pct, 2)
                if return_pct is not None:
                    vs_prorata = round(return_pct - prorata_expected_pct, 2)

        days_remaining = None
        if days_held is not None and expected_hold_days is not None:
            days_remaining = max(0, expected_hold_days - days_held)

        details.append({
            "ticker": tk,
            "source": source,
            "direction": direction,
            "entry_date": entry_date_str,
            "days_held": days_held,
            "expected_hold_days": expected_hold_days,
            "days_remaining": days_remaining,
            "entry_price": entry_price,
            "current_price": _safe_round(current_price),
            "return_pct": return_pct,
            "expected_return_pct": expected_return_pct,
            "prorata_expected_pct": prorata_expected_pct,
            "vs_prorata": vs_prorata,
        })

    return details


def build_digital_alpha(prices):
    """Section 6: Digital Alpha (Coinbase crypto)."""
    funding_dt = datetime.strptime(DIGITAL_ALPHA_FUNDING_DATE, "%Y-%m-%d").date()
    days_since_funding = (date.today() - funding_dt).days

    # USDC staking return (pro-rated from APY)
    usdc_staking_return_pct = round((USDC_STAKING_APY * days_since_funding / 365) * 100, 2)

    # BTC hold benchmark
    btc_hold = fetch_historical_return_pct("BTC-USD", DIGITAL_ALPHA_FUNDING_DATE)

    # ETH hold benchmark
    eth_hold = fetch_historical_return_pct("ETH-USD", DIGITAL_ALPHA_FUNDING_DATE)

    # BTC+ETH 50/50 hold benchmark
    btc_eth_hold = None
    if btc_hold is not None and eth_hold is not None:
        btc_eth_hold = round((btc_hold + eth_hold) / 2, 2)

    # Get Coinbase portfolio value
    current_value, positions = get_crypto_portfolio_value()

    return_dollar = round(current_value - DIGITAL_ALPHA_COST_BASIS, 2) if current_value is not None else None
    return_pct = round(((current_value - DIGITAL_ALPHA_COST_BASIS) / DIGITAL_ALPHA_COST_BASIS) * 100, 2) if current_value is not None else None

    return {
        "current_value": current_value,
        "cost_basis": DIGITAL_ALPHA_COST_BASIS,
        "return_pct": return_pct,
        "return_dollar": return_dollar,
        "days_since_funding": days_since_funding,
        "btc_hold_return_pct": btc_hold,
        "btc_eth_hold_return_pct": btc_eth_hold,
        "usdc_staking_return_pct": usdc_staking_return_pct,
        "positions": positions,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def get_performance_summary():
    """
    Build and return complete performance summary dict.
    Read-only — does NOT write to positions.db or send emails.
    """
    logger.info("Building performance summary...")

    # Read positions and benchmarks
    open_pos, closed_pos = _read_positions()
    benchmarks = _read_benchmarks()

    # Collect all tickers we need prices for
    all_tickers = []
    for pos in open_pos:
        all_tickers.append(pos["ticker"])
    for h in BEDROCK_HOLDINGS:
        all_tickers.append(h["ticker"])
    all_tickers.extend(["SPY", "QQQ"])

    # Batch fetch all equity prices
    logger.info(f"Fetching prices for {len(set(all_tickers))} tickers...")
    prices = fetch_prices(list(set(all_tickers)))

    # Build sections
    summary = {
        "portfolio_header": build_portfolio_header(open_pos),
        "bedrock": build_bedrock(prices),
        "event_alpha": build_event_alpha(open_pos, closed_pos, prices, benchmarks),
        "signal_scorecard": build_signal_scorecard(open_pos, closed_pos, prices, benchmarks),
        "open_positions": build_open_positions_detail(open_pos, prices, benchmarks),
        "digital_alpha": build_digital_alpha(prices),
    }

    logger.info("Performance summary complete.")
    return summary


# ---------------------------------------------------------------------------
# Standalone execution
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    summary = get_performance_summary()
    print(json.dumps(summary, indent=2, default=str))
