#!/usr/bin/env python3
"""
IB Auto-Trader — Equity Signal Execution Layer

Reads tonight's signals from:
  - 8-K scanner: Gmail IMAP (scanner DB lives on PythonAnywhere, not local)
  - Form4 scanner: local SQLite DB

Applies playbook rules (VIX kill switch, position sizing), and
submits market orders via the IBKR Client Portal Web API.

Signal sources:
  - 8-K Item 1.01: SHORT signals with score >= 2 (from email)
  - Form4 Insider Buy Clusters: BUY signals (from local DB)
  - Form4 Insider Sells (S1/S2): SHORT signals (from local DB)

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
from datetime import datetime, date
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

# Gmail IMAP settings (same credentials as outbound email)
IMAP_SERVER = "imap.gmail.com"
IMAP_PORT = 993

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VIX check
# ---------------------------------------------------------------------------

def get_current_vix():
    """Fetch current VIX level via yfinance."""
    try:
        import pandas
        vix = yf.Ticker("^VIX")
        hist = vix.history(period="1d")
        if not hist.empty:
            if isinstance(hist.columns, pandas.MultiIndex):
                hist.columns = [c[0] for c in hist.columns]
            close = float(hist["Close"].iloc[-1])
            logger.info(f"VIX: {close:.2f}")
            return close
    except Exception as e:
        logger.error(f"VIX fetch failed: {e}")
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
    # Each card block contains one ticker then one score badge
    # We match them in sequence across the HTML
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
        return signals

    try:
        mail.select("INBOX")

        # Search for 8-K SHORT emails from today
        # IMAP date format: DD-Mon-YYYY
        dt = datetime.strptime(today_str, "%Y-%m-%d")
        imap_date = dt.strftime("%d-%b-%Y")

        # Try today's date first
        typ, data = mail.search(None,
            f'(SUBJECT "8-K SHORT:" SINCE "{imap_date}")')

        msg_ids = data[0].split() if data[0] else []

        # If nothing today, try most recent 8-K SHORT email (last 7 days)
        if not msg_ids:
            logger.info("No 8-K SHORT email today — checking last 7 days")
            from datetime import timedelta
            week_ago = (dt - timedelta(days=7)).strftime("%d-%b-%Y")
            typ, data = mail.search(None,
                f'(SUBJECT "8-K SHORT:" SINCE "{week_ago}")')
            msg_ids = data[0].split() if data[0] else []

        if not msg_ids:
            logger.info("No 8-K SHORT emails found")
            mail.logout()
            return signals

        # Take the most recent (last in list)
        latest_id = msg_ids[-1]
        typ, msg_data = mail.fetch(latest_id, "(RFC822)")

        if not msg_data or not msg_data[0]:
            logger.warning("Could not fetch 8-K email body")
            mail.logout()
            return signals

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        # Log the email subject and date for confirmation
        subj = msg.get("Subject", "")
        sent = msg.get("Date", "")
        logger.info(f"8-K email found: '{subj}' ({sent})")

        # Extract HTML body
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
# Form4 signals — read from local DB (DB lives in form4_scanner folder)
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

        # Buy clusters
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
                    "score": 3,  # Clusters are high-conviction
                    "price": None,
                    "company": "",
                    "sector": "",
                    "detail": row["details"] or "Insider buy cluster",
                })

        # Sell signals (S1 and S2)
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
    Scans the last 10 yfinance news headlines for M&A keywords.
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
        return False, None  # Don't block trade on check failure


# ---------------------------------------------------------------------------
# Price lookup
# ---------------------------------------------------------------------------

def fetch_live_price(ticker):
    """Fetch current price via yfinance. Returns float or None."""
    try:
        import pandas
        t = yf.Ticker(ticker)
        hist = t.history(period="1d")
        if not hist.empty:
            if isinstance(hist.columns, pandas.MultiIndex):
                hist.columns = [c[0] for c in hist.columns]
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.warning(f"Price fetch failed for {ticker}: {e}")
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
            "side": side,  # "BUY" or "SELL"
            "quantity": quantity,
            "tif": "DAY",
        }]
    }

    result = ib_request("POST", f"/iserver/account/{account_id}/orders", payload)
    if not result:
        return None

    # Handle order confirmation prompts
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
# Email summary
# ---------------------------------------------------------------------------

def send_summary_email(signals_executed, signals_skipped, vix, dry_run=False):
    """Send execution summary email."""
    today_str = date.today().strftime("%Y-%m-%d")
    mode = "DRY RUN" if dry_run else "LIVE"
    total = len(signals_executed) + len(signals_skipped)

    subject = f"IB AutoTrader [{mode}] — {len(signals_executed)} orders, {today_str}"
    if total == 0:
        subject = f"IB AutoTrader [{mode}] — No signals, {today_str}"

    lines = []
    lines.append(f"IB AUTO-TRADER EXECUTION SUMMARY")
    lines.append(f"Mode: {mode}")
    lines.append(f"Date: {today_str}")
    lines.append(f"VIX: {vix:.2f}" if vix else "VIX: N/A")
    lines.append("")

    if signals_executed:
        lines.append(f"ORDERS PLACED ({len(signals_executed)}):")
        lines.append("-" * 50)
        for s in signals_executed:
            lines.append(
                f"  {s['direction']:5s} {s['ticker']:<8s} "
                f"{s.get('shares', 0):>4d} shares @ ${s.get('live_price', 0):>8.2f}  "
                f"[{s['source']}] Score {s['score']}"
            )
        lines.append("")

    if signals_skipped:
        lines.append(f"SKIPPED ({len(signals_skipped)}):")
        lines.append("-" * 50)
        for s in signals_skipped:
            lines.append(
                f"  {s['direction']:5s} {s['ticker']:<8s}  "
                f"Reason: {s.get('skip_reason', 'unknown')}  [{s['source']}]"
            )
        lines.append("")

    if not signals_executed and not signals_skipped:
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

def run(dry_run=False, verbose=False):
    """Main execution flow."""
    today_str = date.today().isoformat()
    logger.info("=" * 60)
    logger.info(f"IB AUTO-TRADER — {'DRY RUN' if dry_run else 'LIVE'}")
    logger.info(f"Date: {today_str}")
    logger.info("=" * 60)

    # Step 1: VIX check
    vix = get_current_vix()
    if vix is not None and vix >= VIX_KILL:
        logger.warning(f"VIX KILL SWITCH: {vix:.2f} >= {VIX_KILL}. Skipping ALL trades.")
        send_summary_email([], [], vix, dry_run)
        return

    # Step 2: Gather signals
    signals = []
    signals.extend(query_8k_signals_from_email(today_str))
    signals.extend(query_form4_signals(today_str))

    if not signals:
        logger.info("No signals tonight.")
        send_summary_email([], [], vix, dry_run)
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

    # Step 3: IB Gateway check (skip in dry-run)
    account_id = None
    if not dry_run:
        if not check_ib_connection():
            logger.error("Cannot connect to IB Gateway. Aborting.")
            for s in signals:
                s["skip_reason"] = "IB Gateway offline"
            send_summary_email([], signals, vix, dry_run)
            return

        account_id = get_account_id()
        if not account_id:
            logger.error("No account ID. Aborting.")
            for s in signals:
                s["skip_reason"] = "No account ID"
            send_summary_email([], signals, vix, dry_run)
            return

    # Step 4: Process each signal
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
            executed.append(signal)
            order_count += 1
        else:
            signal["skip_reason"] = "Order submission failed"
            skipped.append(signal)
            log_trade(signal, shares, size, "ORDER_FAILED", vix=vix)

    # Step 5: Summary
    logger.info("=" * 60)
    logger.info(f"RESULTS: {len(executed)} executed, {len(skipped)} skipped")
    logger.info("=" * 60)

    send_summary_email(executed, skipped, vix, dry_run)


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
