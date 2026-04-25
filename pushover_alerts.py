#!/usr/bin/env python3
"""
GMC critical alerts via Pushover (HTTPS API) with SMTP email fallback.

Public:
    send_pushover(message, priority='high', title='GMC Alert') -> bool
    send_critical_alert(message) -> bool   # emergency-priority wrapper

On Pushover API failure (non-200, status != 1, timeout, exception),
falls back to a [CRITICAL][GMC] SMTP email to CRITICAL_ALERT_RECIPIENTS
and logs the [FALLBACK] line per Apr 17 canon.
"""

import sys
import logging
import smtplib
from pathlib import Path
from email.mime.text import MIMEText

import requests

# Make config.py importable regardless of cwd (e.g., cron context).
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import config  # noqa: E402

logger = logging.getLogger(__name__)

PUSHOVER_API_URL = 'https://api.pushover.net/1/messages.json'

_PRIORITY_MAP = {
    'normal': 0,
    'high': 1,
    'emergency': 2,
}


def send_pushover(message: str, priority: str = 'high', title: str = 'GMC Alert') -> bool:
    """Send Pushover notification. Returns True if delivered, False if fell back to email.

    priority: 'normal' (0), 'high' (1), 'emergency' (2)
    Emergency uses retry=30s, expire=3hr -- repeats until acknowledged in app.
    """
    if priority not in _PRIORITY_MAP:
        logger.error("Invalid priority '%s'; defaulting to 'high'", priority)
        priority = 'high'
    p_int = _PRIORITY_MAP[priority]

    user_key = getattr(config, 'PUSHOVER_USER_KEY', '')
    app_token = getattr(config, 'PUSHOVER_APP_TOKEN', '')
    if not user_key or not app_token:
        reason = 'missing_credentials'
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False

    payload = {
        'token': app_token,
        'user': user_key,
        'message': message,
        'title': title,
        'priority': p_int,
    }
    if p_int == 2:
        payload['retry'] = 30
        payload['expire'] = 10800  # 3 hours

    try:
        resp = requests.post(PUSHOVER_API_URL, data=payload, timeout=10)
    except requests.exceptions.Timeout:
        reason = 'timeout'
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False
    except requests.exceptions.ConnectionError as e:
        reason = 'connection_error:' + type(e).__name__
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False
    except Exception as e:
        reason = 'exception:' + type(e).__name__
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False

    if resp.status_code != 200:
        reason = 'http_' + str(resp.status_code)
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False

    try:
        body = resp.json()
    except ValueError:
        reason = 'invalid_json'
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False

    if body.get('status') != 1:
        errors = body.get('errors', [])
        reason = 'status_not_1:' + ','.join(str(e) for e in errors) if errors else 'status_not_1'
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        _send_critical_email(message, reason)
        return False

    logger.info('Pushover sent [' + priority + ']: ' + message[:80])
    return True


def send_critical_alert(message: str) -> bool:
    """Convenience wrapper for emergency-priority alerts (catastrophic events only)."""
    return send_pushover(message, priority='emergency', title='[GMC EMERGENCY]')


def _send_critical_email(message: str, fallback_reason: str) -> bool:
    """SMTP fallback when Pushover API call fails."""
    try:
        sender = config.EMAIL_SENDER
        password = config.EMAIL_PASSWORD
        smtp_server = config.SMTP_SERVER
        smtp_port = config.SMTP_PORT
        recipients = config.CRITICAL_ALERT_RECIPIENTS
    except AttributeError as e:
        logger.error('Critical fallback email misconfigured: %s', e)
        return False

    subject = '[CRITICAL][GMC] ' + message[:80]
    body = (
        'A critical GMC alert could not be delivered via Pushover.\n\n'
        'Original message:\n' + message + '\n\n'
        'Pushover failure reason: ' + fallback_reason + '\n\n'
        'This email is the fallback channel (Apr 17 Fallback Observability canon).\n'
    )

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        logger.info('Critical fallback email sent: ' + subject)
        return True
    except Exception as e:
        logger.error('Critical fallback email FAILED (%s): %s', type(e).__name__, e)
        return False


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )

    args = sys.argv[1:]
    if args and args[0] in _PRIORITY_MAP:
        cli_priority = args[0]
        cli_message = args[1] if len(args) > 1 else '[TEST] pushover_alerts.py smoke'
    else:
        cli_priority = 'high'
        cli_message = args[0] if args else '[TEST] pushover_alerts.py smoke'

    result = send_pushover(cli_message, priority=cli_priority)
    print('send_pushover returned: ' + str(result))
    sys.exit(0 if result else 1)
