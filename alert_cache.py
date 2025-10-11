import os
import time

from dotenv import load_dotenv

load_dotenv()

_recent_alerts = {}

ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", 30))
ALERT_COOLDOWN = ALERT_COOLDOWN_MINUTES * 60

def _make_key(data):
    return f"{data['token']}:{data['sortBy']}:{data['feeds'][0]['feed']}:{data['feeds'][-1]['feed']}"


def should_send_alert(data):
    key = _make_key(data)
    now = time.time()
    last_sent = _recent_alerts.get(key)

    if last_sent is None or now - last_sent > ALERT_COOLDOWN:
        _recent_alerts[key] = now
        return True
    return False
