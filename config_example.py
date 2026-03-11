"""
IB Auto-Trader Configuration — EXAMPLE
Copy this file to config.py and fill in your values.
Never commit config.py to git.
"""

# ---------------------------------------------------------------------------
# Email settings (Gmail app password)
# ---------------------------------------------------------------------------
EMAIL_SENDER = "your_email@gmail.com"
EMAIL_PASSWORD = "your_app_password"       # Gmail App Password (not your login password)
EMAIL_RECIPIENTS = ["your_email@gmail.com"]
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# ---------------------------------------------------------------------------
# IB Client Portal Gateway
# ---------------------------------------------------------------------------
IB_GATEWAY_URL = "https://localhost:5000/v1/api"
IB_ACCOUNT_ID = ""  # Leave blank to auto-detect from gateway

# ---------------------------------------------------------------------------
# Position sizing
# ---------------------------------------------------------------------------
FULL_POSITION_SIZE = 5000    # USD — full size per trade
HALF_POSITION_SIZE = 2500    # USD — half size (Score 2 or Tier S2)

# ---------------------------------------------------------------------------
# Risk limits
# ---------------------------------------------------------------------------
VIX_KILL_SWITCH = 30         # Skip ALL trades if VIX >= this level
MAX_POSITIONS_PER_RUN = 10   # Safety cap on orders per execution

# ---------------------------------------------------------------------------
# Scanner DB paths (relative to this script)
# ---------------------------------------------------------------------------
EIGHT_K_DB = "../eight_k_scanner/eight_k_filings.db"
FORM4_DB = "../form4_scanner/form4_insider_trades.db"

# ---------------------------------------------------------------------------
# Trade log
# ---------------------------------------------------------------------------
TRADE_LOG_PATH = "trade_log.csv"
