import asyncio
import time
from collections import defaultdict
import json
from asterdex_feed import asterdex_feed
from hyperliquid_feed import hyperliquid_feed
from diff_tasks import find_price_diff_task, find_funding_24h_rate_diff, find_next_funding_rate_diff

DATA_FILE = "data/combined_data.json"

START_DELAY = 15
PRINT_INTERVAL = 10

async def monitor_prices(state):
    """Print a few symbols periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(json.dumps(state, indent=2))
        # if state:
        #     sample = list(state.items())
        #     print("\n📊 Live sample:")
        #     for sym, d in sample:
        #         next_funding = time.strftime(
        #             "%H:%M:%S", time.localtime(d["next_funding_time"] / 1000)
        #         )
        #         print(
        #             f"{sym:<15} {d['price']:<15.4f} "
        #             f"funding_rate={d['funding_rate']:<15.6f} "
        #             f"next={next_funding:<15} interval={d.get('funding_interval_hours','?')}h"
        #         )

async def save_snapshot(state):
    """Periodic redundant save in case of missed flush."""
    await asyncio.sleep(START_DELAY)
    
    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        if state:
            try:
                with open(DATA_FILE, "w") as f:
                    json.dump(state, f, indent=2)
                print(f"🕒 Periodic save: {len(state)} symbols → {DATA_FILE}")
            except Exception as e:
                print(f"❌ Failed to write file: {e}")

async def monitor_prices_diff(state, min_price_diff_pct):
    """Print tokens with significant price differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{min_price_diff_pct}% price difference...")
        tokens_with_diff = find_price_diff_task(state, min_price_diff_pct)
        if tokens_with_diff:
            print(f"📊 Tokens with >{min_price_diff_pct}% price difference:")
            print(json.dumps(tokens_with_diff, indent=2))
        else:
            print(f"📊 No tokens with >{min_price_diff_pct}% price difference found.")

async def monitor_next_funding_rate_diff(state, time_tolerance_minutes=5, threshold_percent=0.1):
    """Print tokens with significant next funding rate differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{threshold_percent}% next funding rate difference within ±{time_tolerance_minutes} minutes...")
        tokens_with_diff = find_next_funding_rate_diff(state, time_tolerance_minutes, threshold_percent)
        if tokens_with_diff:
            print(f"📊 Tokens with >{threshold_percent}% next funding rate difference within ±{time_tolerance_minutes} minutes:")
            print(json.dumps(tokens_with_diff, indent=2))
        else:
            print(f"📊 No tokens with >{threshold_percent}% next funding rate difference within ±{time_tolerance_minutes} minutes found.")

async def monitor_24h_funding_rate_diff(state, threshold_percent=0.1):
    """Print tokens with significant 24h funding rate differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{threshold_percent}% 24h funding rate difference...")
        tokens_with_diff = find_funding_24h_rate_diff(state, threshold_percent)
        if tokens_with_diff:
            print(f"📊 Tokens with >{threshold_percent}% 24h funding rate difference:")
            print(json.dumps(tokens_with_diff, indent=2))
        else:
            print(f"📊 No tokens with >{threshold_percent}% 24h funding rate difference found.")


async def main():
    state = defaultdict(dict)

    asterdex_state_key = "asterdex"
    hyperliquid_state_key = "hyperliquid"

    await asyncio.gather(
        asterdex_feed(state[asterdex_state_key]),
        hyperliquid_feed(state[hyperliquid_state_key]),
        monitor_prices_diff(state, min_price_diff_pct=0.1),
        monitor_next_funding_rate_diff(state, time_tolerance_minutes=5, threshold_percent=0.01),
        monitor_24h_funding_rate_diff(state, threshold_percent=0.1),
        # monitor_prices(state),
        # save_snapshot(state),
    )

if __name__ == "__main__":
    asyncio.run(main())