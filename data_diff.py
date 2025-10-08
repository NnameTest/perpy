import json

# Load JSON files
with open("data/asterdex_data.json") as f1, open("data/hyperliquid_data.json") as f2:
    data1 = json.load(f1)
    data2 = json.load(f2)

common_tokens = set(data1.keys()) & set(data2.keys())

price_diff_list = []
funding_gap_list = []

for token in common_tokens:
    info1 = data1[token]
    info2 = data2[token]
    
    # Relative price difference in percent
    price1 = info1.get("price", 0)
    price2 = info2.get("price", 0)
    if price1 and price2:
        price_diff_pct = abs(price1 - price2) / ((price1 + price2)/2) * 100
    else:
        price_diff_pct = 0
    
    # Absolute funding rate gap
    funding1 = info1.get("funding_rate", 0) * 24 / info1.get("funding_interval_hours", 1)
    funding2 = info2.get("funding_rate", 0) * 24 / info2.get("funding_interval_hours", 1)
    funding_gap_24h = abs(funding1 - funding2)
    
    # Save info with diffs
    price_diff_list.append({
        "token": token,
        "file1_price": price1,
        "file2_price": price2,
        "price_diff_pct": price_diff_pct
    })
    
    funding_gap_list.append({
        "token": token,
        "file1_funding": funding1,
        "file2_funding": funding2,
        "funding_gap": funding_gap_24h
    })

# Sort lists
price_diff_list.sort(key=lambda x: x["price_diff_pct"], reverse=True)
funding_gap_list.sort(key=lambda x: x["funding_gap"], reverse=True)

# Write to JSON files
with open("data/by_price_diff.json", "w") as f:
    json.dump(price_diff_list, f, indent=4)

with open("data/by_funding_gap.json", "w") as f:
    json.dump(funding_gap_list, f, indent=4)

print("Files generated: data/by_price_diff.json, data/by_funding_gap.json")
