import numpy as np
from tabulate import tabulate

# --- Middleware: prepare feed data for diff checker ---
def prepare_diff_data(raw_data):
    """
    Convert raw feed data to the structure expected by diff checker.

    raw_data: {feed: {token: {"price": float, "funding_rate": float, "funding_interval_hours": float, ...}}}

    Returns:
        processed_data: {feed: {token: {"price": float, "funding24hRate": float}}}
    """
    processed_data = {}
    for feed_name, tokens in raw_data.items():
        processed_data[feed_name] = {}
        for symbol, info in tokens.items():
            
            price = info.get("price")
            funding_rate = info.get("funding_rate")
            interval = info.get("funding_interval_hours")

            if price is None or funding_rate is None or interval is None or interval == 0:
                continue

            funding24hRate = funding_rate * (24 / interval)
            processed_data[feed_name][symbol] = {
                "price": price,
                "funding24hRate": funding24hRate
            }
    return processed_data

# --- Core diff calculation using NumPy ---
def calculate_diffs_numpy(token, feeds, sort_by="price"):
    """
    Calculate price and funding diffs across feeds for a given token using NumPy.
    Returns a dict with detailed diff info.
    """
    if not feeds:
        return {"token": token, "feeds": []}

    feed_names = [f["feed"] for f in feeds]
    prices = np.array([f.get("price", 0) for f in feeds], dtype=float)
    fundings = np.array([f.get("funding24hRate", 0) for f in feeds], dtype=float)

    # Reference values for diff calculation
    if sort_by == "price":
        ref_idx = np.argmax(prices)
    else:
        ref_idx = np.argmax(fundings)
    ref_price = prices[ref_idx]
    ref_funding = fundings[ref_idx]

    price_diff = ref_price - prices
    price_diff_pct = (price_diff / ref_price * 100) if ref_price else np.zeros_like(prices)
    funding_diff = ref_funding - fundings
    funding_diff_pct = abs(funding_diff) * 100  # actual % charged from position

    detailed = []
    for i, feed_name in enumerate(feed_names):
        detailed.append({
            "feed": feed_name,
            "price": round(prices[i], 8),
            "priceDiff": round(price_diff[i], 8),
            "priceDiffPct": round(price_diff_pct[i], 2),
            "funding24hRate": round(fundings[i], 8),
            "funding24RateDiff": round(funding_diff[i], 8),
            "funding24RateDiffPct": round(funding_diff_pct[i], 2),
        })

    return {"token": token, "sortBy": sort_by, "feeds": detailed}

# --- Build full tables using NumPy ---
def find_price_diff_table(raw_data, threshold_percent: float = 0.1):
    data = prepare_diff_data(raw_data)
    results = []
    all_tokens = {token for feed in data.values() for token in feed.keys()}

    for token in all_tokens:
        feeds = []
        for feed_name, feed_data in data.items():
            token_data = feed_data.get(token)
            if token_data and "price" in token_data:
                feeds.append({
                    "feed": feed_name,
                    "price": token_data.get("price", 0),
                    "funding24hRate": token_data.get("funding24hRate", 0),
                })

        if len(feeds) < 2:
            continue

        diffs = calculate_diffs_numpy(token, feeds, sort_by="price")
        prices = np.array([f["price"] for f in feeds])
        min_price, max_price = np.min(prices), np.max(prices)
        price_diff_pct_total = ((max_price - min_price) / min_price * 100) if min_price else 0

        if price_diff_pct_total >= threshold_percent:
            diffs["feeds"] = sorted(diffs["feeds"], key=lambda f: f["price"], reverse=True)
            results.append(diffs)

    return sorted(results, key=lambda x: max(f["priceDiffPct"] for f in x["feeds"]), reverse=True)

def find_funding_diff_table(raw_data, threshold_percent: float = 0.1):
    data = prepare_diff_data(raw_data)
    results = []
    all_tokens = {token for feed in data.values() for token in feed.keys()}

    for token in all_tokens:
        feeds = []
        for feed_name, feed_data in data.items():
            token_data = feed_data.get(token)
            if token_data and "funding24hRate" in token_data:
                feeds.append({
                    "feed": feed_name,
                    "price": token_data.get("price", 0),
                    "funding24hRate": token_data.get("funding24hRate", 0),
                })

        if len(feeds) < 2:
            continue

        diffs = calculate_diffs_numpy(token, feeds, sort_by="funding")
        fundings = np.array([f["funding24RateDiffPct"] for f in diffs['feeds']])
        funding_diff_pct_total = max(fundings)

        if funding_diff_pct_total >= threshold_percent:
            diffs["feeds"] = sorted(diffs["feeds"], key=lambda f: f["funding24hRate"], reverse=True)
            results.append(diffs)

    return sorted(results, key=lambda x: max(f["funding24RateDiffPct"] for f in x["feeds"]), reverse=True)

# --- Pretty-print helper ---
def print_diff_table(diff_table):
    for token_data in diff_table:
        token = token_data["token"]
        sort_by = token_data["sortBy"]
        feeds = token_data["feeds"]

        print(f"\n{token} (sorted by {sort_by.upper()})")
        headers = ["Feed", "Price", "ΔPrice%", "Funding24h", "ΔFunding%"]

        def format_number(x, precision=4):
            if isinstance(x, (int, float)):
                return f"{x: .{precision}f}"  # space for positive numbers
            return str(x)

        table = [
            [
                f["feed"],
                format_number(f['price']),
                format_number(f['priceDiffPct']),
                format_number(f['funding24hRate']),
                format_number(f['funding24RateDiffPct']),
            ]
            for f in feeds
        ]

        print(tabulate(table, headers=headers, tablefmt="pretty", colalign=["left", "right", "right", "right", "right"]))
