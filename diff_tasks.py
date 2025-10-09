import time

def find_price_diff_task(data, threshold_percent=0.1):
    """
    Compare token prices across multiple feeds and return sorted differences.
    
    Args:
        data (dict): Nested feed→token→price data.
        min_diff_pct (float): Minimum relative % difference to include in results.
    """
    results = []

    # collect all tokens across all feeds
    all_tokens = set()
    for feed_data in data.values():
        all_tokens.update(feed_data.keys())

    for token in sorted(all_tokens):
        prices = {}
        for feed_name, feed_data in data.items():
            token_info = feed_data.get(token)
            if token_info and "price" in token_info:
                prices[feed_name] = token_info["price"]

        # only compare if at least 2 sources have this token
        if len(prices) < 2:
            continue

        min_feed = min(prices, key=prices.get)
        max_feed = max(prices, key=prices.get)
        min_price = prices[min_feed]
        max_price = prices[max_feed]

        diff = max_price - min_price
        rel_diff = diff / min_price * 100 if min_price != 0 else 0

        # skip if below threshold
        if rel_diff < threshold_percent:
            continue

        results.append({
            "token": token,
            "min_feed": min_feed,
            "max_feed": max_feed,
            "min_price": min_price,
            "max_price": max_price,
            "abs_diff": diff,
            "rel_diff_pct": rel_diff,
            "all_prices": prices,
        })

    # sort by relative difference (descending)
    results.sort(key=lambda x: x["rel_diff_pct"], reverse=True)
    return results

def find_funding_24h_rate_diff(data, min_diff_pct=0.1):
    """
    Compare 24h-normalized funding rates across multiple feeds.

    Args:
        data (dict): feed -> token -> {funding_rate, funding_interval_hours}
        min_diff_pct (float): Minimum relative % difference (optional)
    """
    results = []

    # collect all tokens
    all_tokens = set()
    for feed_data in data.values():
        all_tokens.update(feed_data.keys())

    for token in sorted(all_tokens):
        rates_24h = {}

        # normalize all funding rates to 24h
        for feed_name, feed_data in data.items():
            token_info = feed_data.get(token)
            if not token_info:
                continue

            rate = token_info.get("funding_rate")
            interval = token_info.get("funding_interval_hours")

            if rate is None or not interval:
                continue

            normalized_rate = rate * (24 / interval)
            rates_24h[feed_name] = normalized_rate

        if len(rates_24h.keys()) < 2:
            continue

        min_feed = min(rates_24h, key=rates_24h.get)
        max_feed = max(rates_24h, key=rates_24h.get)
        min_rate = rates_24h[min_feed]
        max_rate = rates_24h[max_feed]
        diff = max_rate - min_rate
        diff_pct = abs(diff) * 100 if min_rate != 0 else 0

        # skip if below threshold
        if abs(diff_pct) < min_diff_pct:
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

    # sort by absolute difference
    results.sort(key=lambda x: abs(x["diff_pct"]), reverse=True)
    return results

def find_next_funding_rate_diff(feeds_data, threshold_percent=0, time_tolerance_minutes=1):
    """
    Find tokens with the highest funding rate gaps for the nearest upcoming funding window.

    Logic:
    - Include all feeds that have a funding event near the nearest upcoming one.
    - Even one-sided feeds (only one source having that time) are included.
    - Funding gap = abs(max_rate - min_rate)
    - Filters out small differences using threshold_percent (%)
    - Adds time_until_funding_hours for live timing insight.
    """

    diffs = []
    feeds = list(feeds_data.keys())
    all_tokens = set().union(*(feeds_data[f].keys() for f in feeds))
    now_ms = int(time.time() * 1000)

    for token in all_tokens:
        # Collect all available funding data across feeds
        token_entries = []
        for f in feeds:
            t = feeds_data[f].get(token)
            if not t:
                continue
            if "next_funding_time" in t and "funding_rate" in t:
                token_entries.append({
                    "feed": f,
                    "time": t["next_funding_time"],
                    "rate": t["funding_rate"],
                    "interval": t["funding_interval_hours"]
                })

        if len(token_entries) < 2:
            continue

        # Find the nearest upcoming funding time
        nearest_time = min(e["time"] for e in token_entries)

        # Select all feeds within the tolerance window (to group same funding times)
        nearest_group = [
            e for e in token_entries
            if abs(e["time"] - nearest_time) <= time_tolerance_minutes * 60 * 1000
        ]

        # Compute diff
        rates = [e["rate"] for e in nearest_group]
        max_rate, min_rate = max(rates), min(rates)
        diff = max_rate - min_rate if len(nearest_group) > 1 else max_rate
        diff_pct = abs(diff) * 100  # convert to percent

        # Time until nearest funding
        time_until_funding_hours = (nearest_time - now_ms) / (1000 * 60 * 60)

        if diff_pct >= threshold_percent:
            diffs.append({
                "token": token,
                "feeds": [e["feed"] for e in nearest_group],
                "nearest_funding_time": nearest_time,
                "time_until_funding_hours": round(time_until_funding_hours, 2),
                "funding_rate_diff": diff,
                "funding_rate_diff_percent": diff_pct,
                "max_rate": max_rate,
                "min_rate": min_rate,
                "count_feeds": len(nearest_group)
            })

    return sorted(diffs, key=lambda x: x["funding_rate_diff_percent"], reverse=True)
