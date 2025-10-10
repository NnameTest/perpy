import time
import numpy as np


def find_price_diff_task(data: dict, threshold_percent: float = 0.1):
    """
    Compare token prices across multiple feeds and return sorted differences.

    Args:
        data: Nested dict {feed: {token: {"price": float}}}
        threshold_percent: Minimum relative % difference to include in results.
    """
    results = []
    all_tokens = {token for feed in data.values() for token in feed.keys()}

    for token in sorted(all_tokens):
        # Collect token prices from each feed
        prices = {
            feed: feed_data[token]["price"]
            for feed, feed_data in data.items()
            if token in feed_data and "price" in feed_data[token]
        }

        if len(prices) < 2:
            continue

        min_feed, min_price = min(prices.items(), key=lambda x: x[1])
        max_feed, max_price = max(prices.items(), key=lambda x: x[1])

        diff = max_price - min_price
        diff_pct = (abs(diff) / min_price * 100) if min_price else 0

        if diff_pct < threshold_percent:
            continue

        results.append({
            "token": token,
            "min_feed": min_feed,
            "max_feed": max_feed,
            "min_price": min_price,
            "max_price": max_price,
            "diff": diff,
            "diff_pct": diff_pct,
            "all_prices": prices,
        })

    return sorted(results, key=lambda x: x["diff_pct"], reverse=True)


def find_funding_24h_rate_diff(data: dict, min_diff_pct: float = 0.1):
    """
    Compare 24h-normalized funding rates across multiple feeds.

    Args:
        data: {feed: {token: {"funding_rate": float, "funding_interval_hours": int}}}
        min_diff_pct: Minimum relative % difference.
    """
    results = []
    all_tokens = {token for feed in data.values() for token in feed.keys()}

    for token in sorted(all_tokens):
        # Normalize all funding rates to 24h basis
        rates_24h = {}
        for feed, feed_data in data.items():
            token_info = feed_data.get(token)
            if not token_info:
                continue

            rate = token_info.get("funding_rate")
            interval = token_info.get("funding_interval_hours")

            if not rate or not interval:
                continue

            rates_24h[feed] = rate * (24 / interval)

        if len(rates_24h) < 2:
            continue

        min_feed, min_rate = min(rates_24h.items(), key=lambda x: x[1])
        max_feed, max_rate = max(rates_24h.items(), key=lambda x: x[1])

        diff = max_rate - min_rate
        diff_pct = abs(diff) * 100 if min_rate else 0

        if diff_pct < min_diff_pct:
            continue

        results.append({
            "token": token,
            "min_feed": min_feed,
            "max_feed": max_feed,
            "min_rate_24h": min_rate,
            "max_rate_24h": max_rate,
            "diff": diff,
            "diff_pct": diff_pct,
            "all_rates_24h": rates_24h,
        })

    return sorted(results, key=lambda x: abs(x["diff_pct"]), reverse=True)


import numpy as np
import time

def find_next_funding_rate_diff(feeds_data, threshold_percent=0, time_tolerance_minutes=1):
    """
    Find tokens with the highest funding rate gaps for the nearest upcoming funding window.

    - If some feeds have later funding times, their rate is treated as 0 for the nearest window.
    - Keeps same output shape + adds `min_feed` and `max_feed` for clarity.
    """
    diffs = []
    feeds = list(feeds_data.keys())
    all_tokens = set().union(*(feeds_data[f].keys() for f in feeds))
    now_ms = int(time.time() * 1000)
    tolerance_ms = time_tolerance_minutes * 60 * 1000

    for token in all_tokens:
        token_entries = []
        for f in feeds:
            t = feeds_data[f].get(token)
            if not t:
                continue
            if all(k in t for k in ("funding_interval_hours", "next_funding_time", "funding_rate")):
                token_entries.append({
                    "feed": f,
                    "time": t["next_funding_time"],
                    "rate": t["funding_rate"],
                    "interval": t["funding_interval_hours"]
                })

        if len(token_entries) < 2:
            continue

        feeds_arr = np.array([e["feed"] for e in token_entries])
        times_arr = np.array([e["time"] for e in token_entries], dtype=np.int64)
        rates_arr = np.array([e["rate"] for e in token_entries], dtype=float)

        nearest_time = np.min(times_arr)
        time_mask = np.abs(times_arr - nearest_time) <= tolerance_ms

        # Feeds funding soon
        nearest_group = [token_entries[i] for i in np.where(time_mask)[0]]

        # Feeds funding later → treat as 0 rate for this upcoming event
        later_rates = np.zeros(np.sum(~time_mask))

        effective_rates = np.concatenate([rates_arr[time_mask], later_rates])
        effective_feeds = np.concatenate([feeds_arr[time_mask], feeds_arr[~time_mask]])

        max_idx = np.argmax(effective_rates)
        min_idx = np.argmin(effective_rates)

        max_rate = float(effective_rates[max_idx])
        min_rate = float(effective_rates[min_idx])
        max_feed = str(effective_feeds[max_idx])
        min_feed = str(effective_feeds[min_idx])

        diff = max_rate - min_rate
        diff_pct = abs(diff) * 100
        time_until_funding_hours = (nearest_time - now_ms) / (1000 * 60 * 60)

        if diff_pct < threshold_percent:
            continue

        diffs.append({
            "token": token,
            "feeds": effective_feeds.tolist(),
            "nearest_funding_time": int(nearest_time),
            "time_until_funding_hours": round(float(time_until_funding_hours), 2),
            "funding_rate_diff": float(diff),
            "funding_rate_diff_percent": float(diff_pct),
            "max_rate": float(max_rate),
            "min_rate": float(min_rate),
            "max_feed": max_feed,
            "min_feed": min_feed,
            "count_feeds": len(effective_feeds)
        })

    return sorted(diffs, key=lambda x: x["funding_rate_diff_percent"], reverse=True)
