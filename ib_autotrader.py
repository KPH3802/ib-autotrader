#!/usr/bin/env python3
"""
IB Auto-Trader — Equity Signal Execution Layer

Reads tonight's signals from:
  - 8-K scanner: Gmail IMAP (scanner DB lives on PythonAnywhere, not local)
  - Form4 scanner: local SQLite DB
  - Dividend Cut scanner: Gmail IMAP (Score 3+ signals from PA scanner)

Applies playbook rules (VIX kill switch, position sizing), and
submits market orders via the IBKR Client Portal Web API.

Signal sources:
  - 8-K Item 1.01: SHORT signals with score >= 2 (from email)
  - Form4 Insider Buy Clusters: BUY signals (from local DB)
  - Form4 Insider Sells (S1/S2): SHORT signals (from local DB)
  - Dividend Cut Score 3+: BUY signals (from email, 60-day hold)

Position tracking (div cut only):
  - positions.db tracks OPEN positions with entry date + price
  - Day 60: auto-close at market open
  - -40% absolute return: catastrophic circuit breaker close
  - No stop loss. No profit target. Data-driven exit rules only.

Requires: IBKR Client Portal Gateway running on localhost:5001

Usage:
  python3 ib_autotrader.py                # Live execution
  python3 ib_autotrader.py --dry-run      # Log signals, no orders
  python3 ib_autotrader.py --dry-run -v   # Verbose dry run
"""

import os
import re
import sys
import csv
import json
import email
import imaplib
import sqlite3
import logging
import argparse
import smtplib
import urllib3
from datetime import datetime, date, timedelta
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
import yfinance as yf

# Suppress SSL warnings for IB Gateway self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent

try:
    import config
except ImportError:
    print("ERROR: config.py not found. Copy config_example.py to config.py and fill in your values.")
    sys.exit(1)

# Pull settings from config
IB_GATEWAY_URL = config.IB_GATEWAY_URL
IB_ACCOUNT_ID = getattr(config, "IB_ACCOUNT_ID", "")
FULL_SIZE = config.FULL_POSITION_SIZE
HALF_SIZE = config.HALF_POSITION_SIZE
VIX_KILL = config.VIX_KILL_SWITCH
MAX_ORDERS = config.MAX_POSITIONS_PER_RUN

FORM4_DB = SCRIPT_DIR / config.FORM4_DB
TRADE_LOG = SCRIPT_DIR / config.TRADE_LOG_PATH
POSITIONS_DB = SCRIPT_DIR / getattr(config, "POSITIONS_DB", "positions.db")

# Dividend cut signal settings
DIV_CUT_MIN_SCORE = 3       # Minimum net_score to enter
DIV_CUT_HOLD_DAYS = 60      # Primary exit: Day 60
DIV_CUT_BREAKER = -39.9     # Catastrophic circuit breaker (% return)
DIV_CUT_LOOKBACK_DAYS = 1   # How many days back

# PEAD signal settings
PEAD_HOLD_DAYS = 28         # 4-week hold (matches backtest)
PEAD_BREAKER   = -39.9      # Catastrophic circuit breaker
PEAD_LOOKBACK_DAYS = 2      # Check emails from last 2 days to scan for cut emails (1 = today's email only — matches backtest entry timing)
# 13F Institutional Initiations signal settings
THIRTEENF_HOLD_DAYS     = 91     # 13-week hold (matches backtest)
THIRTEENF_BREAKER       = -39.9  # Catastrophic circuit breaker
THIRTEENF_LOOKBACK_DAYS = 7      # Check last 7 days (scanner fires quarterly)

# 8-K Item 1.01 signal settings
EIGHT_K_HOLD_DAYS  = 5       # 5-day hold (validated window in backtest, t=-9.98)
EIGHT_K_BREAKER   = -39.9   # Catastrophic circuit breaker (% return)

# Form4 signal settings
F4_HOLD_DAYS  = 5       # 5-day hold (validated window in backtest)
F4_BREAKER    = -39.9   # Catastrophic circuit breaker (% return)

# Gmail IMAP settings (same credentials as outbound email)
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Warnings collected during run -- appended to summary email
system_warnings = []


# ---------------------------------------------------------------------------
# VIX check
# ---------------------------------------------------------------------------

def get_current_vix(timeout=30):
    """Fetch VIX with fallback. Primary: yfinance. Fallback: direct Yahoo Finance HTTP.
    Returns None only if both sources fail — caller must fail safe on None.
    """
    import threading

    # --- Primary: yfinance with hard timeout ---
    result = [None]
    yf_error = [None]

    def _fetch_yfinance():
        try:
            import pandas
            ticker = yf.Ticker("^VIX")
            hist = ticker.history(period="1d")
            if not hist.empty:
                if isinstance(hist.columns, pandas.MultiIndex):
                    hist.columns = [c[0] for c in hist.columns]
                result[0] = float(hist["Close"].iloc[-1])
        except Exception as e:
            yf_error[0] = e

    t = threading.Thread(target=_fetch_yfinance, daemon=True)
    t.start()
    t.join(timeout=timeout)

    if result[0] is not None:
        logger.info(f"VIX: {result[0]:.2f} (yfinance)")
        return result[0]

    if t.is_alive():
        logger.warning(f"VIX yfinance timed out after {timeout}s — trying fallback")
    else:
        logger.warning(f"VIX yfinance failed ({yf_error[0]}) — trying fallback")

    # --- Fallback 1: direct Yahoo Finance HTTP (bypasses yfinance library) ---
    try:
        import requests as req
        r = req.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EVIX",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        price = float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
        logger.info(f"VIX: {price:.2f} (Yahoo direct fallback)")
        return price
    except Exception as e:
        logger.warning(f"VIX Yahoo direct failed: {e} — trying FMP")

    # --- Fallback 2: FMP quote endpoint (independent provider) ---
    try:
        import requests as req
        r = req.get(
            f"https://financialmodelingprep.com/api/v3/quote/%5EVIX?apikey={config.FMP_API_KEY}",
            timeout=10
        )
        data = r.json()
        if data and isinstance(data, list) and "price" in data[0]:
            price = float(data[0]["price"])
            logger.info(f"VIX: {price:.2f} (FMP fallback)")
            return price
        logger.warning("FMP VIX response empty or malformed")
    except Exception as e:
        logger.error(f"VIX FMP fallback failed: {e}")

    logger.error("VIX fetch failed on all 3 sources (yfinance, Yahoo direct, FMP) — fail-safe will block new entries")
    return None


# ---------------------------------------------------------------------------
# 8-K signals — parsed from Gmail IMAP
# ---------------------------------------------------------------------------

def _connect_gmail():
    """Open authenticated Gmail IMAP connection. Returns mail object or None."""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
        return mail
    except Exception as e:
        logger.error(f"Gmail IMAP login failed: {e}")
        return None


def _parse_8k_html(html):
    """
    Extract (ticker, score) pairs from 8-K scanner HTML email.

    The emailer builds cards with:
      <span style="font-size:22px;font-weight:bold">TICKER</span>
      ...
      <span style="...">Score N</span>

    Only TRADE signals (score >= 2) appear in these cards.
    """
    signals = []
    pattern = r'font-size:22px;font-weight:bold">([\w.\-]+)</span>.*?Score\s+(\d+)'
    matches = re.findall(pattern, html, re.DOTALL)

    for ticker, score_str in matches:
        score = int(score_str)
        if score >= 2:
            signals.append({
                "source": "8K_1.01",
                "ticker": ticker.upper(),
                "direction": "SHORT",
                "score": score,
                "price": None,
                "company": "",
                "sector": "",
                "detail": f"8-K Item 1.01, Score {score}",
            })
    return signals


def query_8k_signals_from_email(today_str):
    """
    Search Gmail for today's 8-K scanner email and parse SHORT signals.

    Looks for emails with subject starting with '8-K SHORT:' sent today.
    Falls back to most recent 8-K SHORT email if none found for today.
    """
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning("8-K signals unavailable — Gmail IMAP failed")
        system_warnings.append("WARNING: Gmail IMAP unavailable -- signals may be missing")
        return signals

    try:
        mail.select("INBOX")

        dt = datetime.strptime(today_str, "%Y-%m-%d")
        imap_date = dt.strftime("%d-%b-%Y")

        typ, data = mail.search(None,
            f'(SUBJECT "8-K SHORT:" SINCE "{imap_date}")')
        msg_ids = data[0].split() if data[0] else []

        if not msg_ids:
            logger.info("No 8-K SHORT email today — checking last 4 days")
            four_days_ago = (dt - timedelta(days=4)).strftime("%d-%b-%Y")
            typ, data = mail.search(None,
                f'(SUBJECT "8-K SHORT:" SINCE "{four_days_ago}")')
            msg_ids = data[0].split() if data[0] else []

        if not msg_ids:
            logger.info("No 8-K SHORT emails found")
            mail.logout()
            return signals

        latest_id = msg_ids[-1]
        typ, msg_data = mail.fetch(latest_id, "(RFC822)")

        if not msg_data or not msg_data[0]:
            logger.warning("Could not fetch 8-K email body")
            mail.logout()
            return signals

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        subj = msg.get("Subject", "")
        sent = msg.get("Date", "")
        logger.info(f"8-K email found: '{subj}' ({sent})")

        html_body = None
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/html":
                    html_body = part.get_payload(decode=True).decode(
                        part.get_content_charset() or "utf-8", errors="replace")
                    break
        elif msg.get_content_type() == "text/html":
            html_body = msg.get_payload(decode=True).decode(
                msg.get_content_charset() or "utf-8", errors="replace")

        if not html_body:
            logger.warning("No HTML body in 8-K email")
            mail.logout()
            return signals

        signals = _parse_8k_html(html_body)
        logger.info(f"8-K signals parsed from email: {len(signals)} SHORT")

    except Exception as e:
        logger.error(f"8-K email parse failed: {e}")
    finally:
        try:
            mail.logout()
        except Exception:
            pass

    return signals


# ---------------------------------------------------------------------------
# Dividend Cut signals — parsed from Gmail IMAP
# ---------------------------------------------------------------------------

def _parse_div_cut_html(html):
    """
    Extract (ticker, net_score) pairs from dividend cut scanner HTML email.

    Scans each cut card. Only returns signals with net_score >= DIV_CUT_MIN_SCORE.
    Card structure: class="cut-card ..." contains a ticker span and net score metric.
    """
    signals = []

    # --- Debug: log what we're working with ---
    html_len = len(html) if html else 0
    logger.info(f"  Div cut HTML: {html_len} chars")
    if html_len < 100:
        logger.warning(f"  Div cut HTML suspiciously short: '{html[:200]}'")
        return signals

    # --- Primary parser: split on cut-card class attribute ---
    # Gmail may modify HTML in transit; if this fails we fall back to a looser approach
    card_blocks = html.split('class="cut-card')
    if len(card_blocks) <= 1:
        # Fallback: try single-quote variant (some mail clients use single quotes)
        card_blocks = html.split("class='cut-card")

    if len(card_blocks) <= 1:
        logger.warning(f"  Div cut HTML: no cut-card blocks found. First 400 chars: {html[:400]}")
        return signals

    logger.info(f"  Div cut HTML: {len(card_blocks) - 1} cut-card block(s) found")

    for card in card_blocks[1:]:
        # Extract ticker — try class="ticker" first, then class='ticker'
        ticker_match = (
            re.search(r'class="ticker">([A-Z][A-Z0-9.\-]*)</span>', card)
            or re.search(r"class='ticker'>([A-Z][A-Z0-9.\-]*)</span>", card)
            or re.search(r'class=["\']ticker["\']>([A-Z][A-Z0-9.\-]*)</span>', card)
        )
        if not ticker_match:
            logger.debug(f"  Div cut: no ticker found in card block (first 200 chars): {card[:200]}")
            continue
        ticker = ticker_match.group(1).strip()

        # Extract net score — appears as +N or -N just before NET SCORE label.
        # Try both class="metric-label" and class='metric-label' variants.
        score_match = (
            re.search(
                r'>([\+\-]?\d+)</div>\s*<div[^>]+class=["\']metric-label["\']>NET SCORE',
                card
            )
            or re.search(
                r'>([\+\-]?\d+)<\/div>\s*<div[^>]*>NET SCORE',
                card
            )
        )
        if not score_match:
            logger.warning(f"  Div cut: no NET SCORE found for {ticker}. Card snippet: {card[:300]}")
            continue

        try:
            net_score = int(score_match.group(1))
        except ValueError:
            continue

        logger.info(f"  Div cut: {ticker} net_score={net_score:+d}")
        if net_score < DIV_CUT_MIN_SCORE:
            logger.info(f"  Div cut: {ticker} score {net_score} below threshold {DIV_CUT_MIN_SCORE} — skip")
            continue

        signals.append({
            "source": "DIV_CUT",
            "ticker": ticker,
            "direction": "BUY",
            "score": net_score,
            "price": None,
            "company": "",
            "sector": "",
            "detail": f"Dividend Cut Score {net_score}, 60-day hold",
        })

    return signals


def query_div_cut_signals(today_str):
    """
    Search Gmail for recent dividend cut scanner emails and parse BUY signals.

    Searches last DIV_CUT_LOOKBACK_DAYS days for 'Dividend Cut ALERT' emails.
    Only returns signals with net_score >= DIV_CUT_MIN_SCORE (default: 3).
    """
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning("Div cut signals unavailable — Gmail IMAP failed")
        return signals

    try:
        mail.select("INBOX")

        dt = datetime.strptime(today_str, "%Y-%m-%d")
        since_date = (dt - timedelta(days=DIV_CUT_LOOKBACK_DAYS)).strftime("%d-%b-%Y")

        typ, data = mail.search(None,
            f'(SUBJECT "Dividend Cut ALERT" SINCE "{since_date}")')
        msg_ids = data[0].split() if data[0] else []

        if not msg_ids:
            logger.info("No Dividend Cut ALERT emails in last 3 days")
            mail.logout()
            return signals

        # Process all matching emails (could be multiple days' worth)
        seen_tickers = set()
        for msg_id in msg_ids:
            typ, msg_data = mail.fetch(msg_id, "(RFC822)")
            if not msg_data or not msg_data[0]:
                continue

            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)
            subj = msg.get("Subject", "")
            sent = msg.get("Date", "")
            logger.info(f"Div cut email found: '{subj}' ({sent})")

            html_body = None
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/html":
                        html_body = part.get_payload(decode=True).decode(
                            part.get_content_charset() or "utf-8", errors="replace")
                        break
            elif msg.get_content_type() == "text/html":
                html_body = msg.get_payload(decode=True).decode(
                    msg.get_content_charset() or "utf-8", errors="replace")

            if not html_body:
                continue

            parsed = _parse_div_cut_html(html_body)
            for s in parsed:
                if s["ticker"] not in seen_tickers:
                    seen_tickers.add(s["ticker"])
                    signals.append(s)

        logger.info(f"Div cut signals (Score {DIV_CUT_MIN_SCORE}+): {len(signals)} BUY")

    except Exception as e:
        logger.error(f"Div cut email parse failed: {e}")
    finally:
        try:
            mail.logout()
        except Exception:
            pass

    return signals



# ---------------------------------------------------------------------------
# PEAD signals -- parsed from Gmail IMAP
# ---------------------------------------------------------------------------

def _parse_pead_subject(subject):
    import re
    results = []
    bull = re.search(r'PEAD BULL:\s*([A-Z ,]+?)(?:\s*\||$)', subject)
    if bull:
        for t in bull.group(1).split(','):
            t = t.strip()
            if t: results.append((t, 'BUY'))
    bear = re.search(r'PEAD BEAR:\s*([A-Z ,]+?)(?:\s*\||$)', subject)
    if bear:
        for t in bear.group(1).split(','):
            t = t.strip()
            if t: results.append((t, 'SHORT'))
    return results


def query_pead_signals_from_email(today_str):
    """Parse PEAD BULL/BEAR signals from Gmail. BULL->BUY, BEAR->SHORT, score=3 (full size)."""
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning('PEAD signals unavailable -- Gmail IMAP failed')
        return signals
    try:
        mail.select('INBOX')
        dt = datetime.strptime(today_str, '%Y-%m-%d')
        since_date = (dt - timedelta(days=PEAD_LOOKBACK_DAYS)).strftime('%d-%b-%Y')
        typ, data = mail.search(None, f'(SUBJECT "PEAD" SINCE "{since_date}")')
        msg_ids = data[0].split() if data[0] else []
        if not msg_ids:
            logger.info('No PEAD emails in lookback window')
            mail.logout()
            return signals
        seen = set()
        for msg_id in msg_ids[-3:]:
            typ, msg_data = mail.fetch(msg_id, '(RFC822)')
            if not msg_data or not msg_data[0]: continue
            msg = email.message_from_bytes(msg_data[0][1])
            subj = msg.get('Subject', '')
            logger.info(f"PEAD email: '{subj}'")
            if 'PEAD BULL:' not in subj and 'PEAD BEAR:' not in subj:
                continue
            for ticker, direction in _parse_pead_subject(subj):
                key = (ticker, direction)
                if key not in seen:
                    seen.add(key)
                    src_name = 'PEAD_BULL' if direction == 'BUY' else 'PEAD_BEAR'
                    signals.append({
                        'source': src_name, 'ticker': ticker,
                        'direction': direction, 'score': 3,
                        'price': None, 'company': '', 'sector': '',
                        'detail': f"PEAD {'beat' if direction=='BUY' else 'miss'} >=5% EPS surprise",
                    })
        bulls = sum(1 for s in signals if s['direction']=='BUY')
        bears = sum(1 for s in signals if s['direction']=='SHORT')
        logger.info(f'PEAD signals: {bulls} BULL, {bears} BEAR')
    except Exception as e:
        logger.error(f'PEAD email parse failed: {e}')
    finally:
        try: mail.logout()
        except Exception: pass
    return signals


# ---------------------------------------------------------------------------
# Short Interest Squeeze signals -- parsed from Gmail IMAP
# ---------------------------------------------------------------------------

def query_si_squeeze_signals_from_email(today_str):
    """Parse SI Squeeze signals from Gmail. Subject: 'SI SQUEEZE: TICK1, TICK2'.
    All tickers = BUY (long), score=3 (full size), 28-day hold."""
    import re
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning('SI Squeeze signals unavailable -- Gmail IMAP failed')
        return signals
    try:
        mail.select('INBOX')
        dt = datetime.strptime(today_str, '%Y-%m-%d')
        since_date = (dt - timedelta(days=3)).strftime('%d-%b-%Y')
        typ, data = mail.search(None, f'(SUBJECT "SI SQUEEZE:" SINCE "{since_date}")')
        msg_ids = data[0].split() if data[0] else []
        if not msg_ids:
            logger.info('No SI SQUEEZE emails in lookback window')
            mail.logout()
            return signals
        seen = set()
        for msg_id in msg_ids[-3:]:
            typ, msg_data = mail.fetch(msg_id, '(RFC822)')
            if not msg_data or not msg_data[0]: continue
            msg = email.message_from_bytes(msg_data[0][1])
            subj = msg.get('Subject', '')
            logger.info(f"SI Squeeze email: '{subj}'")
            if 'SI SQUEEZE:' not in subj:
                continue
            # Parse: 'SI SQUEEZE: TICK1, TICK2, TICK3 +N more'
            after = subj.split('SI SQUEEZE:', 1)[1]
            # Strip '+N more' suffix if present
            after = re.sub(r'\+\d+ more.*', '', after)
            for t in after.split(','):
                ticker = t.strip().upper()
                if ticker and re.match(r'^[A-Z]{1,5}$', ticker) and ticker not in seen:
                    seen.add(ticker)
                    signals.append({
                        'source': 'SI_SQUEEZE',
                        'ticker': ticker,
                        'direction': 'BUY',
                        'score': 3,
                        'price': None,
                        'company': '',
                        'sector': '',
                        'detail': 'Short interest increase >=30% squeeze signal',
                    })
        logger.info(f'SI Squeeze signals: {len(signals)} BUY')
    except Exception as e:
        logger.error(f'SI Squeeze email parse failed: {e}')
    finally:
        try: mail.logout()
        except Exception: pass
    return signals


# ---------------------------------------------------------------------------
# COT signals -- parsed from Gmail IMAP
# ---------------------------------------------------------------------------

def query_cot_signals_from_email(today_str):
    """Parse COT BULL/BEAR signals. Subject: 'COT BULL: XOP, GLD | COT BEAR: USO'."""
    import re
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning('COT signals unavailable -- Gmail IMAP failed')
        return signals
    try:
        mail.select('INBOX')
        dt = datetime.strptime(today_str, '%Y-%m-%d')
        since_date = (dt - timedelta(days=7)).strftime('%d-%b-%Y')
        typ, data = mail.search(None, f'(SUBJECT "COT" SINCE "{since_date}")')
        msg_ids = data[0].split() if data[0] else []
        if not msg_ids:
            logger.info('No COT emails in lookback window')
            mail.logout()
            return signals
        seen = set()
        for msg_id in msg_ids[-2:]:
            typ, msg_data = mail.fetch(msg_id, '(RFC822)')
            if not msg_data or not msg_data[0]: continue
            msg = email.message_from_bytes(msg_data[0][1])
            subj = msg.get('Subject', '')
            logger.info(f"COT email: '{subj}'")
            if 'COT BULL:' not in subj and 'COT BEAR:' not in subj:
                continue
            bull = re.search(r'COT BULL:\s*([A-Z ,]+?)(?:\s*\||$)', subj)
            if bull:
                for t in bull.group(1).split(','):
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        signals.append({'source':'COT_BULL','ticker':t,'direction':'BUY',
                                        'score':3,'price':None,'company':'','sector':'',
                                        'detail':'COT commercial extreme long signal'})
            bear = re.search(r'COT BEAR:\s*([A-Z ,]+?)(?:\s*\||$)', subj)
            if bear:
                for t in bear.group(1).split(','):
                    t = t.strip()
                    if t and t not in seen:
                        seen.add(t)
                        signals.append({'source':'COT_BEAR','ticker':t,'direction':'SHORT',
                                        'score':3,'price':None,'company':'','sector':'',
                                        'detail':'COT commercial extreme short signal'})
        logger.info(f'COT signals: {len([s for s in signals if s["direction"]=="BUY"])} BULL, {len([s for s in signals if s["direction"]=="SHORT"])} BEAR')
    except Exception as e:
        logger.error(f'COT email parse failed: {e}')
    finally:
        try: mail.logout()
        except Exception: pass
    return signals


# ---------------------------------------------------------------------------
# CEL (Commodity-Equity Lag) signals -- parsed from Gmail IMAP
# ---------------------------------------------------------------------------

def query_cel_signals_from_email(today_str):
    """Parse CEL BEAR signals. Subject: 'CEL BEAR: XOP, XLE, CVX, XOM, COP'.
    All tickers = SHORT, score=3, 5-day hold (tracked via positions.db)."""
    import re
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning('CEL signals unavailable -- Gmail IMAP failed')
        return signals
    try:
        mail.select('INBOX')
        dt = datetime.strptime(today_str, '%Y-%m-%d')
        since_date = (dt - timedelta(days=2)).strftime('%d-%b-%Y')
        typ, data = mail.search(None, f'(SUBJECT "CEL BEAR:" SINCE "{since_date}")')
        msg_ids = data[0].split() if data[0] else []
        if not msg_ids:
            logger.info('No CEL BEAR emails in lookback window')
            mail.logout()
            return signals
        seen = set()
        for msg_id in msg_ids[-2:]:
            typ, msg_data = mail.fetch(msg_id, '(RFC822)')
            if not msg_data or not msg_data[0]: continue
            msg = email.message_from_bytes(msg_data[0][1])
            subj = msg.get('Subject', '')
            logger.info(f"CEL email: '{subj}'")
            if 'CEL BEAR:' not in subj: continue
            after = subj.split('CEL BEAR:', 1)[1]
            for t in after.split(','):
                t = t.strip()
                if t and t not in seen:
                    seen.add(t)
                    signals.append({
                        'source': 'CEL_BEAR', 'ticker': t, 'direction': 'SHORT',
                        'score': 3, 'price': None, 'company': '', 'sector': '',
                        'detail': 'USO drop >=2% commodity-equity lag signal',
                    })
        logger.info(f'CEL signals: {len(signals)} SHORT')
    except Exception as e:
        logger.error(f'CEL email parse failed: {e}')
    finally:
        try: mail.logout()
        except Exception: pass
    return signals

# ---------------------------------------------------------------------------
# 13F Institutional Initiations signals -- parsed from Gmail IMAP
# ---------------------------------------------------------------------------
def query_13f_signals_from_email(today_str):
    """Parse 13F BULL signals. Subject: '13F BULL: TICK1, TICK2'.
    All tickers -> BUY, score=3 (full size), 91-day hold."""
    signals = []
    mail = _connect_gmail()
    if not mail:
        logger.warning('13F signals unavailable -- Gmail IMAP failed')
        return signals
    try:
        mail.select('INBOX')
        dt = datetime.strptime(today_str, '%Y-%m-%d')
        since_date = (dt - timedelta(days=THIRTEENF_LOOKBACK_DAYS)).strftime('%d-%b-%Y')
        typ, data = mail.search(None, '(SUBJECT "13F BULL:" SINCE "' + since_date + '")') 
        msg_ids = data[0].split() if data[0] else []
        if not msg_ids:
            logger.info('No 13F BULL emails in lookback window')
            mail.logout()
            return signals
        seen = set()
        for msg_id in msg_ids[-3:]:
            typ, msg_data = mail.fetch(msg_id, '(RFC822)')
            if not msg_data or not msg_data[0]: continue
            msg = email.message_from_bytes(msg_data[0][1])
            subj = msg.get('Subject', '')
            logger.info('13F email: ' + repr(subj))
            if '13F BULL:' not in subj: continue
            after = subj.split('13F BULL:', 1)[1]
            after = re.sub(r'\+\d+ more.*', '', after)
            for t in after.split(','):
                ticker = t.strip().upper()
                if ticker and re.match(r'^[A-Z]{1,5}$', ticker) and ticker not in seen:
                    seen.add(ticker)
                    signals.append({
                        'source':    'THIRTEENF_BULL',
                        'ticker':    ticker,
                        'direction': 'BUY',
                        'score':     3,
                        'price':     None,
                        'company':   '',
                        'sector':    '',
                        'detail':    '13F initiation: 3+ hedge fund new positions same quarter',
                    })
        logger.info('13F signals: ' + str(len(signals)) + ' BUY')
    except Exception as e:
        logger.error('13F email parse failed: ' + str(e))
    finally:
        try: mail.logout()
        except Exception: pass
    return signals


# ---------------------------------------------------------------------------
# Form4 signals — read from local DB
# ---------------------------------------------------------------------------

def query_form4_signals(today_str):
    """Query Form4 database for tonight's BUY and SHORT signals."""
    signals = []

    if not FORM4_DB.exists():
        logger.warning(f"Form4 DB not found: {FORM4_DB}")
        return signals

    try:
        conn = sqlite3.connect(str(FORM4_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            SELECT issuer_ticker, alert_type, details
            FROM sent_alerts
            WHERE alert_type = 'cluster'
              AND alert_date = ?
        """, (today_str,))

        for row in cur.fetchall():
            if row["issuer_ticker"]:
                signals.append({
                    "source": "F4_BUY_CLUSTER",
                    "ticker": row["issuer_ticker"],
                    "direction": "BUY",
                    "score": 3,
                    "price": None,
                    "company": "",
                    "sector": "",
                    "detail": row["details"] or "Insider buy cluster",
                })

        cur.execute("""
            SELECT issuer_ticker, alert_type, details
            FROM sent_alerts
            WHERE alert_type IN ('sell_s1', 'sell_s2')
              AND alert_date = ?
        """, (today_str,))

        for row in cur.fetchall():
            if row["issuer_ticker"]:
                tier = "S1" if row["alert_type"] == "sell_s1" else "S2"
                signals.append({
                    "source": f"F4_SELL_{tier}",
                    "ticker": row["issuer_ticker"],
                    "direction": "SHORT",
                    "score": 3 if tier == "S1" else 2,
                    "price": None,
                    "company": "",
                    "sector": "",
                    "detail": row["details"] or f"Insider sell {tier}",
                })

        conn.close()

    except Exception as e:
        logger.error(f"Form4 DB query failed: {e}")

    buys = sum(1 for s in signals if s["direction"] == "BUY")
    shorts = sum(1 for s in signals if s["direction"] == "SHORT")
    logger.info(f"Form4 signals: {buys} BUY, {shorts} SHORT")
    return signals


# ---------------------------------------------------------------------------
# M&A risk check
# ---------------------------------------------------------------------------

MA_KEYWORDS = [
    "acqui", "merger", "buyout", "takeover", "tender offer",
    "going private", "going-private", "purchased by", "acquired by",
    "to be acquired", "definitive agreement", "acquisition agreement",
]

def check_ma_risk(ticker):
    """
    Check if ticker has recent M&A news that would invalidate a short signal.
    Returns (is_risky: bool, reason: str or None).
    """
    try:
        t = yf.Ticker(ticker)
        news = t.news
        if not news:
            return False, None
        for item in news[:10]:
            title = item.get("title", "").lower()
            summary = item.get("summary", "").lower()
            text = title + " " + summary
            for kw in MA_KEYWORDS:
                if kw in text:
                    headline = item.get("title", "")[:80]
                    reason = f"M&A keyword '{kw}' found — {headline}"
                    return True, reason
        return False, None
    except Exception as e:
        logger.warning(f"M&A check failed for {ticker}: {e}")
        system_warnings.append(f"WARNING: M&A filter down for {ticker} -- proceeding without M&A check")
        return False, None


# ---------------------------------------------------------------------------
# Price lookup
# ---------------------------------------------------------------------------

def fetch_live_price(ticker):
    """Fetch price with fallback. yfinance -> Yahoo direct HTTP -> FMP.
    Returns float or None only if all 3 sources fail.
    """
    import threading
    result = [None]

    def _fetch_yf():
        try:
            import pandas
            t = yf.Ticker(ticker)
            hist = t.history(period="1d")
            if not hist.empty:
                if isinstance(hist.columns, pandas.MultiIndex):
                    hist.columns = [c[0] for c in hist.columns]
                result[0] = float(hist["Close"].iloc[-1])
        except Exception:
            pass

    t = threading.Thread(target=_fetch_yf, daemon=True)
    t.start()
    t.join(timeout=15)
    if result[0] is not None:
        return result[0]

    # Fallback 1: Yahoo direct HTTP
    try:
        import requests as req
        encoded = req.utils.quote(ticker)
        r = req.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10
        )
        price = float(r.json()["chart"]["result"][0]["meta"]["regularMarketPrice"])
        logger.info(f"Price {ticker}: ${price:.2f} (Yahoo direct)")
        return price
    except Exception as e:
        logger.warning(f"Price Yahoo direct failed for {ticker}: {e}")

    # Fallback 2: FMP
    try:
        import requests as req
        r = req.get(
            f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={config.FMP_API_KEY}",
            timeout=10
        )
        data = r.json()
        if data and isinstance(data, list) and "price" in data[0]:
            price = float(data[0]["price"])
            logger.info(f"Price {ticker}: ${price:.2f} (FMP)")
            return price
    except Exception as e:
        logger.warning(f"Price FMP failed for {ticker}: {e}")

    logger.error(f"Price fetch failed on all 3 sources for {ticker}")
    return None

# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------

def calculate_shares(signal, live_price):
    """
    Calculate number of shares based on playbook rules.
    Score 2 = half size, Score 3+ = full size.
    """
    price = live_price or signal.get("price")
    if not price or price <= 0:
        return 0, 0.0

    size = FULL_SIZE if signal["score"] >= 3 else HALF_SIZE
    shares = int(size / price)
    return max(shares, 1), size


# ---------------------------------------------------------------------------
# IB Client Portal API
# ---------------------------------------------------------------------------

def ib_request(method, endpoint, data=None):
    """Make a request to the IB Client Portal Gateway."""
    url = f"{IB_GATEWAY_URL}{endpoint}"
    try:
        if method == "GET":
            resp = requests.get(url, verify=False, timeout=10)
        elif method == "POST":
            resp = requests.post(url, json=data, verify=False, timeout=10)
        else:
            return None

        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"IB API {method} {endpoint}: {resp.status_code} — {resp.text[:200]}")
            return None
    except requests.exceptions.ConnectionError:
        logger.error(f"IB Gateway not reachable at {IB_GATEWAY_URL}")
        return None
    except Exception as e:
        logger.error(f"IB API error: {e}")
        return None


def check_ib_connection():
    """Verify IB Gateway is running and authenticated."""
    result = ib_request("GET", "/iserver/auth/status")
    if result and result.get("authenticated"):
        logger.info("IB Gateway: authenticated")
        return True
    logger.error("IB Gateway: NOT authenticated or unreachable")
    return False


def get_account_id():
    """Get the trading account ID from the gateway."""
    if IB_ACCOUNT_ID:
        return IB_ACCOUNT_ID

    result = ib_request("GET", "/iserver/accounts")
    if result and result.get("accounts"):
        acct = result["accounts"][0]
        logger.info(f"IB Account: {acct}")
        return acct
    logger.error("Could not retrieve IB account ID")
    return None


def search_contract(ticker):
    """Search for a stock contract ID (conid) by ticker symbol."""
    result = ib_request("GET", f"/iserver/secdef/search?symbol={ticker}")
    if result and isinstance(result, list) and len(result) > 0:
        for item in result:
            if item.get("conid"):
                return int(item["conid"])
            sections = item.get("sections", [])
            for sec in sections:
                if sec.get("secType") == "STK":
                    return int(item.get("conid", 0))
        return int(result[0].get("conid", 0))
    logger.warning(f"No contract found for {ticker}")
    return None


def place_order(account_id, conid, side, quantity):
    """
    Place a market order via IB Client Portal API.
    Returns order ID or None.
    """
    payload = {
        "orders": [{
            "conid": conid,
            "orderType": "MKT",
            "side": side,
            "quantity": quantity,
            "tif": "DAY",
        }]
    }

    result = ib_request("POST", f"/iserver/account/{account_id}/orders", payload)
    if not result:
        return None

    if isinstance(result, list) and len(result) > 0:
        item = result[0]
        if item.get("id") and item.get("message"):
            reply_id = item["id"]
            logger.info(f"  Confirming order: {item.get('message', [''])[0][:80]}")
            confirm = ib_request("POST", f"/iserver/reply/{reply_id}", {"confirmed": True})
            if confirm and isinstance(confirm, list) and len(confirm) > 0:
                return confirm[0].get("order_id")
        if item.get("order_id"):
            return item["order_id"]

    return None


# ---------------------------------------------------------------------------
# Positions database — Dividend Cut tracker
# ---------------------------------------------------------------------------

def init_positions_db():
    """Create positions.db with open_positions table if it doesn't exist."""
    conn = sqlite3.connect(str(POSITIONS_DB))
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS open_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            direction TEXT NOT NULL DEFAULT 'BUY',
            entry_date TEXT NOT NULL,
            entry_price REAL NOT NULL,
            shares INTEGER NOT NULL,
            position_size REAL NOT NULL,
            order_id TEXT,
            source TEXT DEFAULT 'DIV_CUT',
            status TEXT NOT NULL DEFAULT 'OPEN',
            close_date TEXT,
            close_price REAL,
            close_reason TEXT,
            return_pct REAL,
            UNIQUE(ticker, entry_date)
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"Positions DB ready: {POSITIONS_DB}")


def log_position_entry(ticker, entry_date, entry_price, shares, position_size,
                       order_id=None, source="DIV_CUT", direction="BUY"):
    """Write a new OPEN position to positions.db."""
    try:
        conn = sqlite3.connect(str(POSITIONS_DB))
        c = conn.cursor()
        c.execute("""
            INSERT OR IGNORE INTO open_positions
            (ticker, direction, entry_date, entry_price, shares, position_size, order_id, source, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
        """, (ticker, direction, entry_date, entry_price, shares, position_size, order_id, source))
        conn.commit()
        conn.close()
        logger.info(f"  Position logged: {ticker} | {shares} shares @ ${entry_price:.2f} | Entry: {entry_date}")
    except Exception as e:
        logger.error(f"Failed to log position entry for {ticker}: {e}")


def check_and_close_positions(account_id, dry_run, vix):
    """
    Run at the top of every daily execution before processing new signals.

    For each OPEN position:
      - Fetch current price
      - Calculate days held and return %
      - Day 60: submit SELL (or dry-run log), mark CLOSED, reason=DAY_60
      - Return <= -40%: submit SELL, mark CLOSED, reason=CATASTROPHIC_BREAKER

    Returns list of dicts describing closed positions for the summary email.
    """
    closed_today = []

    if not POSITIONS_DB.exists():
        return closed_today

    try:
        conn = sqlite3.connect(str(POSITIONS_DB))
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("""
            SELECT id, ticker, entry_date, entry_price, shares, position_size, order_id, source, direction
            FROM open_positions
            WHERE status = 'OPEN'
        """)
        open_positions = c.fetchall()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to read open positions: {e}")
        return closed_today

    if not open_positions:
        logger.info("No open div cut positions to check.")
        return closed_today

    logger.info(f"Checking {len(open_positions)} open div cut position(s)...")
    today = date.today()

    for pos in open_positions:
        ticker = pos["ticker"]
        entry_date = datetime.strptime(pos["entry_date"], "%Y-%m-%d").date()
        entry_price = pos["entry_price"]
        shares = pos["shares"]
        pos_id = pos["id"]

        days_held = (today - entry_date).days

        # Source-aware hold period and close side
        pos_source    = pos["source"] or "DIV_CUT"
        pos_direction = pos["direction"] or "BUY"
        if pos_source.startswith("PEAD"):
            hold_limit    = PEAD_HOLD_DAYS
            day_exit_lbl  = "DAY_28"
            breaker_val   = PEAD_BREAKER
        elif pos_source == "THIRTEENF_BULL":
            hold_limit    = THIRTEENF_HOLD_DAYS
            day_exit_lbl  = "DAY_91"
            breaker_val   = THIRTEENF_BREAKER
        elif pos_source == "8K_1.01":
            hold_limit    = EIGHT_K_HOLD_DAYS
            day_exit_lbl  = "DAY_5"
            breaker_val   = EIGHT_K_BREAKER
        elif pos_source.startswith("F4_"):
            hold_limit    = F4_HOLD_DAYS
            day_exit_lbl  = "DAY_5"
            breaker_val   = F4_BREAKER
        else:
            hold_limit    = DIV_CUT_HOLD_DAYS
            day_exit_lbl  = "DAY_60"
            breaker_val   = DIV_CUT_BREAKER
        close_side = "BUY" if pos_direction == "SHORT" else "SELL"

        # Fetch current price
        current_price = fetch_live_price(ticker)
        if not current_price:
            logger.warning(f"  {ticker}: price fetch failed — cannot evaluate exit")
            continue

        if pos_direction == "SHORT":
            return_pct = ((entry_price - current_price) / entry_price) * 100
        else:
            return_pct = ((current_price - entry_price) / entry_price) * 100

        logger.info(
            f"  {ticker}: entry={entry_price:.2f} | now={current_price:.2f} | "
            f"return={return_pct:+.1f}% | days={days_held}"
        )

        close_reason = None
        if days_held >= hold_limit:
            close_reason = day_exit_lbl
            logger.info(f"  {ticker}: {day_exit_lbl} exit triggered ({days_held} days held)")
        elif return_pct <= breaker_val:
            close_reason = "CATASTROPHIC_BREAKER"
            logger.warning(f"  {ticker}: CATASTROPHIC BREAKER triggered ({return_pct:.1f}%)")

        if close_reason is None:
            continue

        # Execute close
        close_order_id = None
        close_status = "DRY_RUN_CLOSE" if dry_run else "CLOSE_FAILED"

        if not dry_run:
            conid = search_contract(ticker)
            if conid and account_id:
                close_order_id = place_order(account_id, conid, close_side, shares)
                if close_order_id:
                    close_status = "CLOSED"
                    logger.info(f"  {ticker}: SELL {shares} shares — Order ID {close_order_id}")
                else:
                    logger.error(f"  {ticker}: SELL order failed")
            else:
                logger.error(f"  {ticker}: cannot close — no conid or account_id")

        # Mark closed in DB
        try:
            conn = sqlite3.connect(str(POSITIONS_DB))
            c = conn.cursor()
            c.execute("""
                UPDATE open_positions
                SET status = 'CLOSED',
                    close_date = ?,
                    close_price = ?,
                    close_reason = ?,
                    return_pct = ?
                WHERE id = ?
            """, (today.isoformat(), current_price, close_reason, round(return_pct, 2), pos_id))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to update position close for {ticker}: {e}")

        closed_today.append({
            "ticker": ticker,
            "entry_date": pos["entry_date"],
            "entry_price": entry_price,
            "close_price": current_price,
            "close_reason": close_reason,
            "return_pct": round(return_pct, 2),
            "days_held": days_held,
            "shares": shares,
            "status": close_status,
        })

    return closed_today


# ---------------------------------------------------------------------------
# Trade log
# ---------------------------------------------------------------------------

def log_trade(signal, shares, size, status, order_id=None, vix=None):
    """Append a trade record to the CSV log."""
    file_exists = TRADE_LOG.exists()

    with open(TRADE_LOG, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "date", "source", "ticker", "direction",
                "shares", "position_size", "score", "price",
                "vix", "status", "order_id", "detail",
            ])

        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            date.today().isoformat(),
            signal["source"],
            signal["ticker"],
            signal["direction"],
            shares,
            f"{size:.0f}",
            signal["score"],
            signal.get("live_price", signal.get("price", "")),
            f"{vix:.2f}" if vix else "",
            status,
            order_id or "",
            signal["detail"],
        ])


# ---------------------------------------------------------------------------
# Holiday notification email
# ---------------------------------------------------------------------------
def send_holiday_email(today_str, dry_run=False):
    """Send brief notification that market is closed today."""
    mode = "DRY RUN" if dry_run else "LIVE"
    subject = f"IB AutoTrader [{mode}] -- NYSE Closed {today_str}"
    body = (
        f"NYSE CLOSED: {today_str} is a market holiday.\n\n"
        "No orders placed. No action required.\n"
        "System is healthy -- signals will be re-evaluated next trading day.\n"
    )
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = config.EMAIL_SENDER
        msg["To"] = ", ".join(config.EMAIL_RECIPIENTS)
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as s:
            s.starttls()
            s.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
            s.sendmail(config.EMAIL_SENDER, config.EMAIL_RECIPIENTS, msg.as_string())
        logger.info(f"Holiday notification email sent: {subject}")
    except Exception as e:
        logger.error(f"Holiday email failed: {e}")


# ---------------------------------------------------------------------------
# Email summary
# ---------------------------------------------------------------------------

def send_summary_email(signals_executed, signals_skipped, vix, dry_run=False,
                       positions_closed=None):
    """Send execution summary email."""
    today_str = date.today().strftime("%Y-%m-%d")
    mode = "DRY RUN" if dry_run else "LIVE"
    total = len(signals_executed) + len(signals_skipped)
    closed_count = len(positions_closed) if positions_closed else 0

    subject = f"IB AutoTrader [{mode}] — {len(signals_executed)} orders, {today_str}"
    if total == 0 and closed_count == 0:
        subject = f"IB AutoTrader [{mode}] — No signals, {today_str}"
    elif closed_count > 0 and total == 0:
        subject = f"IB AutoTrader [{mode}] — {closed_count} position(s) closed, {today_str}"

    lines = []
    lines.append(f"IB AUTO-TRADER EXECUTION SUMMARY")
    lines.append(f"Mode: {mode}")
    lines.append(f"Date: {today_str}")
    lines.append(f"VIX: {vix:.2f}" if vix else "VIX: N/A")
    lines.append("")

    # Closed positions section
    if positions_closed:
        lines.append(f"POSITIONS CLOSED TODAY ({closed_count}):")
        lines.append("-" * 50)
        for p in positions_closed:
            reason_label = ("Day 5 Exit" if p["close_reason"] == "DAY_5"
                else "Day 28 Exit" if p["close_reason"] == "DAY_28"
                else "Day 60 Exit" if p["close_reason"] == "DAY_60"
                else "Day 91 Exit" if p["close_reason"] == "DAY_91"
                else "CATASTROPHIC BREAKER")
            lines.append(
                f"  SELL {p['ticker']:<8s} "
                f"{p['shares']:>4d} shares | "
                f"Entry: ${p['entry_price']:.2f} → Close: ${p['close_price']:.2f} | "
                f"Return: {p['return_pct']:+.1f}% | "
                f"{p['days_held']}d held | {reason_label} | {p['status']}"
            )
        lines.append("")

    if signals_executed:
        lines.append(f"NEW ORDERS PLACED ({len(signals_executed)}):")
        lines.append("-" * 50)
        for s in signals_executed:
            lines.append(
                f"  {s['direction']:5s} {s['ticker']:<8s} "
                f"{s.get('shares', 0):>4d} shares @ ${s.get('live_price', 0):>8.2f}  "
                f"[{s['source']}] Score {s['score']}"
            )
        lines.append("")

    if system_warnings:
        lines.append("SYSTEM WARNINGS:")
        lines.append("-" * 50)
        for w in system_warnings:
            lines.append(f"  !! {w}")
        lines.append("")
        subject = subject + " [WARNINGS]"
    if signals_skipped:
        lines.append(f"SKIPPED ({len(signals_skipped)}):")
        lines.append("-" * 50)
        for s in signals_skipped:
            lines.append(
                f"  {s['direction']:5s} {s['ticker']:<8s}  "
                f"Reason: {s.get('skip_reason', 'unknown')}  [{s['source']}]"
            )
        lines.append("")

    if not signals_executed and not signals_skipped and not positions_closed:
        lines.append("No signals from any scanner tonight.")

    body = "\n".join(lines)

    try:
        msg = MIMEMultipart()
        msg["From"] = config.EMAIL_SENDER
        msg["To"] = ", ".join(config.EMAIL_RECIPIENTS)
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls()
            server.login(config.EMAIL_SENDER, config.EMAIL_PASSWORD)
            server.send_message(msg)

        logger.info(f"Summary email sent: {subject}")
    except Exception as e:
        logger.error(f"Email failed: {e}")


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def is_nyse_open_today():
    """Return True if NYSE open today. Fails open if calendar unavailable."""
    try:
        import exchange_calendars as xcals
        nyse = xcals.get_calendar("XNYS")
        return nyse.is_session(date.today().isoformat())
    except Exception as e:
        logger.warning(f"NYSE calendar check failed ({e}) -- assuming market open")
        return True


def run(dry_run=False, verbose=False):
    """Main execution flow."""
    today_str = date.today().isoformat()
    logger.info("=" * 60)
    logger.info(f"IB AUTO-TRADER — {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"Date: {today_str}")
    logger.info("=" * 60)

    global system_warnings
    system_warnings = []

    # Step 0: NYSE market holiday check
    if not is_nyse_open_today():
        logger.info(f"NYSE CLOSED today ({today_str}) -- market holiday. Skipping execution.")
        logger.info("Signals re-evaluated next trading day via normal lookback window.")
        send_holiday_email(today_str, dry_run)
        return

    # Step 1: Init positions DB
    init_positions_db()

    # Step 2: VIX check
    vix = get_current_vix()
    if vix is None:
        logger.error("VIX FAIL-SAFE: VIX data unavailable. Blocking ALL new entries. Exits will still process.")
        positions_closed = check_and_close_positions(account_id, dry_run, vix)
        send_summary_email([], [], None, dry_run, positions_closed)
        return
    if vix >= VIX_KILL:
        logger.warning(f"VIX KILL SWITCH: {vix:.2f} >= {VIX_KILL}. Skipping ALL trades.")
        send_summary_email([], [], vix, dry_run)
        return

    # Step 3: IB Gateway check (skip in dry-run)
    account_id = None
    if not dry_run:
        if not check_ib_connection():
            logger.error("Cannot connect to IB Gateway. Aborting.")
            send_summary_email([], [], vix, dry_run)
            return

        account_id = get_account_id()
        if not account_id:
            logger.error("No account ID. Aborting.")
            send_summary_email([], [], vix, dry_run)
            return

    # Step 4: Check and close open div cut positions BEFORE processing new signals
    logger.info("-" * 60)
    logger.info("CHECKING OPEN POSITIONS FOR EXITS...")
    logger.info("-" * 60)
    positions_closed = check_and_close_positions(account_id, dry_run, vix)

    # Step 5: Gather new entry signals
    logger.info("-" * 60)
    logger.info("GATHERING NEW SIGNALS...")
    logger.info("-" * 60)
    signals = []
    signals.extend(query_8k_signals_from_email(today_str))
    signals.extend(query_form4_signals(today_str))
    signals.extend(query_div_cut_signals(today_str))
    signals.extend(query_pead_signals_from_email(today_str))
    signals.extend(query_si_squeeze_signals_from_email(today_str))
    signals.extend(query_cot_signals_from_email(today_str))
    signals.extend(query_cel_signals_from_email(today_str))
    signals.extend(query_13f_signals_from_email(today_str))

    if not signals:
        logger.info("No new signals tonight.")
        send_summary_email([], [], vix, dry_run, positions_closed)
        return

    logger.info(f"Total signals: {len(signals)}")

    # Deduplicate — same ticker+direction keeps highest score
    seen = {}
    for s in signals:
        key = (s["ticker"], s["direction"])
        if key not in seen or s["score"] > seen[key]["score"]:
            seen[key] = s
    signals = list(seen.values())
    logger.info(f"After dedup: {len(signals)}")

    # Filter out div cut tickers that already have an OPEN position
    if POSITIONS_DB.exists():
        try:
            conn = sqlite3.connect(str(POSITIONS_DB))
            c = conn.cursor()
            c.execute("SELECT ticker FROM open_positions WHERE status = 'OPEN'")
            already_open = {row[0] for row in c.fetchall()}
            conn.close()
        except Exception:
            already_open = set()

        pre_filter_count = len(signals)
        tracked_sources = {"DIV_CUT", "PEAD_BULL", "PEAD_BEAR", "8K_1.01", "SI_SQUEEZE", "COT_BULL", "COT_BEAR", "CEL_BEAR", "THIRTEENF_BULL", "F4_BUY_CLUSTER", "F4_SELL_S1", "F4_SELL_S2"}
        signals = [
            s for s in signals
            if not (s["source"] in tracked_sources and s["ticker"] in already_open)
        ]
        filtered = pre_filter_count - len(signals)
        if filtered > 0:
            logger.info(f"Filtered {filtered} div cut signal(s) — positions already open")

    # Step 6: Process each signal
    executed = []
    skipped = []
    order_count = 0

    for signal in signals:
        ticker = signal["ticker"]

        if order_count >= MAX_ORDERS:
            signal["skip_reason"] = f"Max orders ({MAX_ORDERS}) reached"
            skipped.append(signal)
            continue

        # M&A check — skip if acquisition/merger news detected
        ma_risk, ma_reason = check_ma_risk(ticker)
        if ma_risk:
            signal["skip_reason"] = f"M&A risk: {ma_reason}"
            skipped.append(signal)
            log_trade(signal, 0, 0, "SKIP_MA_RISK", vix=vix)
            logger.warning(f"  SKIPPED {ticker}: {signal['skip_reason']}")
            continue

        # Fetch live price
        live_price = fetch_live_price(ticker)
        if not live_price:
            signal["skip_reason"] = "Price fetch failed"
            skipped.append(signal)
            log_trade(signal, 0, 0, "SKIP_NO_PRICE", vix=vix)
            continue

        signal["live_price"] = live_price

        # Position sizing
        shares, size = calculate_shares(signal, live_price)
        if shares <= 0:
            signal["skip_reason"] = "Zero shares"
            skipped.append(signal)
            continue

        signal["shares"] = shares
        signal["size"] = size

        side = "BUY" if signal["direction"] == "BUY" else "SELL"

        if verbose or dry_run:
            logger.info(
                f"  {signal['direction']:5s} {ticker:<8s} "
                f"{shares:>4d} shares @ ${live_price:.2f} = ${size:.0f}  "
                f"[{signal['source']}] Score {signal['score']}"
            )

        if dry_run:
            log_trade(signal, shares, size, "DRY_RUN", vix=vix)
            # Log tracked-source entries to positions DB even in dry-run
            if signal["source"] in ("DIV_CUT", "PEAD_BULL", "PEAD_BEAR", "8K_1.01", "SI_SQUEEZE", "COT_BULL", "COT_BEAR", "CEL_BEAR", "THIRTEENF_BULL", "F4_BUY_CLUSTER", "F4_SELL_S1", "F4_SELL_S2"):
                log_position_entry(
                    ticker=ticker,
                    entry_date=today_str,
                    entry_price=live_price,
                    shares=shares,
                    position_size=size,
                    order_id="DRY_RUN",
                    source=signal["source"],
                    direction=signal["direction"],
                )
            executed.append(signal)
            order_count += 1
            continue

        # Look up contract ID
        conid = search_contract(ticker)
        if not conid:
            signal["skip_reason"] = f"No contract ID for {ticker}"
            skipped.append(signal)
            log_trade(signal, shares, size, "SKIP_NO_CONID", vix=vix)
            continue

        # Place order
        order_id = place_order(account_id, conid, side, shares)
        if order_id:
            logger.info(f"  ORDER PLACED: {side} {shares} {ticker} — ID {order_id}")
            log_trade(signal, shares, size, "FILLED", order_id=order_id, vix=vix)
            # Log tracked-source entries to positions tracker
            if signal["source"] in ("DIV_CUT", "PEAD_BULL", "PEAD_BEAR", "8K_1.01", "SI_SQUEEZE", "COT_BULL", "COT_BEAR", "CEL_BEAR", "THIRTEENF_BULL", "F4_BUY_CLUSTER", "F4_SELL_S1", "F4_SELL_S2"):
                log_position_entry(
                    ticker=ticker,
                    entry_date=today_str,
                    entry_price=live_price,
                    shares=shares,
                    position_size=size,
                    order_id=str(order_id),
                    source=signal["source"],
                    direction=signal["direction"],
                )
            executed.append(signal)
            order_count += 1
        else:
            signal["skip_reason"] = "Order submission failed"
            skipped.append(signal)
            log_trade(signal, shares, size, "ORDER_FAILED", vix=vix)

    # Step 7: Summary
    logger.info("=" * 60)
    logger.info(
        f"RESULTS: {len(executed)} executed, {len(skipped)} skipped, "
        f"{len(positions_closed)} position(s) closed"
    )
    logger.info("=" * 60)

    send_summary_email(executed, skipped, vix, dry_run, positions_closed)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="IB Auto-Trader — Equity Signal Execution",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 ib_autotrader.py                # Live execution
  python3 ib_autotrader.py --dry-run      # Log signals, skip orders
  python3 ib_autotrader.py --dry-run -v   # Verbose dry run
        """
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Log signals and sizing but do not place orders")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose output")

    args = parser.parse_args()
    run(dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
