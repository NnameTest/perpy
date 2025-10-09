# alert_cache.py
import os
import time

from dotenv import load_dotenv

load_dotenv()

# Dictionary: key = alert identifier, value = timestamp when sent
_recent_alerts = {}

ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", 30))
ALERT_COOLDOWN = ALERT_COOLDOWN_MINUTES * 60  # 30 minutes in seconds

def _make_key(category, data):
    """Combine category + data to make a unique cache key."""
    if category == "funding_next_diff":
        return f"{category}:{data['token']}:{"-".join(data['feeds'])}:{data['nearest_funding_time']}"
    return f"{category}:{data['token']}:{data['max_feed']}:{data['min_feed']}"


def should_send_alert(category, data) -> bool:
    """Return True if alert can be sent (not throttled)."""
    key = _make_key(category, data)
    now = time.time()
    last_sent = _recent_alerts.get(key)

    if last_sent is None or now - last_sent > ALERT_COOLDOWN:
        _recent_alerts[key] = now
        return True
    return False
