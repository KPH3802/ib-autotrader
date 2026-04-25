#!/usr/bin/env python3
"""
GMC critical alerts via Pushover (HTTPS API) with parallel + fallback SMTP email.

Public:
    send_pushover(message, priority='high', title='GMC Alert') -> bool
    send_critical_alert(message) -> bool   # emergency-priority wrapper
    send_alert_email(message, severity_label, fallback_reason=None) -> bool

On Pushover success for high/emergency priority, a parallel email is also
sent (subject [HIGH][GMC] or [CRITICAL][GMC]) so a missed phone buzz does
not mean a missed alert.

On Pushover API failure (non-200, status != 1, timeout, exception), the same
SMTP function fires with a fallback header noting the primary failure reason,
and the [FALLBACK] line is logged per Apr 17 canon.
"""

import sys
import socket
import logging
import smtplib
from datetime import datetime, timezone
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
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
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
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
        return False
    except requests.exceptions.ConnectionError as e:
        reason = 'connection_error:' + type(e).__name__
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
        return False
    except Exception as e:
        reason = 'exception:' + type(e).__name__
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
        return False

    if resp.status_code != 200:
        reason = 'http_' + str(resp.status_code)
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
        return False

    try:
        body = resp.json()
    except ValueError:
        reason = 'invalid_json'
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
        return False

    if body.get('status') != 1:
        errors = body.get('errors', [])
        reason = 'status_not_1:' + ','.join(str(e) for e in errors) if errors else 'status_not_1'
        logger.error('[FALLBACK] source=pushover primary=https_api primary_error=%s fallback=critical_email', reason)
        send_alert_email(message, '[CRITICAL][GMC]', fallback_reason=reason)
        return False

    logger.info('Pushover sent [' + priority + ']: ' + message[:80])

    # Layer B: parallel email for high+ priority. Pushover already delivered,
    # so any email failure here is non-fatal and must not flip the return value.
    if priority in ('high', 'emergency'):
        severity_label = '[CRITICAL][GMC]' if priority == 'emergency' else '[HIGH][GMC]'
        try:
            send_alert_email(message, severity_label)
        except Exception as e:
            logger.warning('Layer B parallel email failed (Pushover succeeded): ' + repr(e))

    return True


def send_critical_alert(message: str) -> bool:
    """Convenience wrapper for emergency-priority alerts (catastrophic events only)."""
    return send_pushover(message, priority='emergency', title='[GMC EMERGENCY]')


def send_alert_email(message: str, severity_label: str, fallback_reason: str = None) -> bool:
    """Send one alert email to CRITICAL_ALERT_RECIPIENTS.

    severity_label: subject prefix, e.g. '[HIGH][GMC]' or '[CRITICAL][GMC]'.
    fallback_reason: when set, body marks this as the Pushover fallback channel
        and includes the primary failure reason for diagnosis.
    Returns True on SMTP success, False on failure (logged, not raised).
    """
    try:
        sender = config.EMAIL_SENDER
        password = config.EMAIL_PASSWORD
        smtp_server = config.SMTP_SERVER
        smtp_port = config.SMTP_PORT
        recipients = config.CRITICAL_ALERT_RECIPIENTS
    except AttributeError as e:
        logger.error('Alert email misconfigured: %s', e)
        return False

    subject = severity_label + ' ' + message[:80]
    timestamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
    hostname = socket.gethostname()
    footer = '\n\n---\n' + timestamp + ' on ' + hostname + '\n'

    if fallback_reason:
        body = (
            'Pushover delivery failed (' + fallback_reason + ') -- this is the email fallback channel.\n\n'
            + message
            + footer
        )
    else:
        body = message + footer

    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)

    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=15) as server:
            server.starttls()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_string())
        logger.info('Alert email sent: ' + subject)
        return True
    except Exception as e:
        logger.error('Alert email send failed: ' + repr(e))
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
