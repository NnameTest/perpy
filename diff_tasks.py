import time


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


def find_next_funding_rate_diff(feeds_data: dict, threshold_percent: float = 0, time_tolerance_minutes: int = 1):
    """
    Find tokens with the highest funding rate gaps for the nearest upcoming funding window.

    Logic:
    - Group feeds that have upcoming funding events within a small time window.
    - Include even one-sided feeds if they match the nearest time.
    - Funding gap = abs(max_rate - min_rate)
    - Filters small differences below threshold_percent (%)
    - Adds time_until_funding_hours for live timing insight.
    """
    now_ms = int(time.time() * 1000)
    diffs = []

    all_tokens = {token for f in feeds_data.values() for token in f.keys()}

    for token in all_tokens:
        # Collect all available funding data across feeds
        entries = [
            {
                "feed": f,
                "time": t["next_funding_time"],
                "rate": t["funding_rate"],
                "interval": t["funding_interval_hours"],
            }
            for f, fd in feeds_data.items()
            if (t := fd.get(token))
            and {"next_funding_time", "funding_rate", "funding_interval_hours"} <= t.keys()
        ]

        if len(entries) < 2:
            continue

        nearest_time = min(e["time"] for e in entries)

        # Group entries close to that nearest time
        nearest_group = [
            e for e in entries
            if abs(e["time"] - nearest_time) <= time_tolerance_minutes * 60 * 1000
        ]

        if not nearest_group:
            continue

        rates = [e["rate"] for e in nearest_group]
        max_rate, min_rate = max(rates), min(rates)
        diff = max_rate - min_rate if len(nearest_group) > 1 else max_rate
        diff_pct = abs(diff) * 100
        time_until_funding_hours = (nearest_time - now_ms) / (1000 * 60 * 60)

        if diff_pct < threshold_percent:
            continue

        diffs.append({
            "token": token,
            "feeds": [e["feed"] for e in nearest_group],
            "nearest_funding_time": nearest_time,
            "time_until_funding_hours": round(time_until_funding_hours, 2),
            "funding_rate_diff": diff,
            "funding_rate_diff_percent": diff_pct,
            "max_rate": max_rate,
            "min_rate": min_rate,
            "count_feeds": len(nearest_group),
        })

    return sorted(diffs, key=lambda x: x["funding_rate_diff_percent"], reverse=True)
