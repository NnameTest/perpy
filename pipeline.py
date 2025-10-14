import os
import time
from collections import defaultdict

import asyncio
from dotenv import load_dotenv 

from asterdex_feed import asterdex_feed
from hyperliquid_feed import hyperliquid_feed
from lighter_feed import lighter_feed
from edgex_feed import edgex_feed
from diffs import find_funding_diff_table, find_price_diff_table, print_diff_table
from telegram import send_detailed_diff_telegram_message

load_dotenv()

START_DELAY=int(os.getenv("START_DELAY"))
PRINT_INTERVAL=int(os.getenv("PRINT_INTERVAL"))
CLEAR_STATE_INTERVAL=int(os.getenv("CLEAR_STATE_INTERVAL"))
PRICE_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("PRICE_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_NEXT_TIME_TOLERANCE_MINUTES=float(os.getenv("FUNDING_NEXT_TIME_TOLERANCE_MINUTES"))

async def monitor_prices_diff(state, threshold_percent):
    """Print tokens with significant price differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{threshold_percent}% price difference...")
        tokens_with_diff = find_price_diff_table(state, threshold_percent)
        if tokens_with_diff:
            print(f"📊 Tokens with >{threshold_percent}% price difference:")
            print_diff_table(tokens_with_diff)
            await send_detailed_diff_telegram_message(tokens_with_diff)
        else:
            print(f"📊 No tokens with >{threshold_percent}% price difference found.")

async def monitor_24h_funding_rate_diff(state, threshold_percent=0.1):
    """Print tokens with significant 24h funding rate differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{threshold_percent}% 24h funding rate difference...")
        tokens_with_diff = find_funding_diff_table(state, threshold_percent)
        if tokens_with_diff:
            print(f"📊 Tokens with >{threshold_percent}% 24h funding rate difference:")
            print_diff_table(tokens_with_diff)
            await send_detailed_diff_telegram_message(tokens_with_diff)
        else:
            print(f"📊 No tokens with >{threshold_percent}% 24h funding rate difference found.")

async def periodic_clear_state(state):
    while True:
        await asyncio.sleep(CLEAR_STATE_INTERVAL)
        state.clear()
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("State cleared to prevent memory bloat.")

async def main():
    state = defaultdict(dict)

    await asyncio.gather(
        asterdex_feed(state["aster"]),
        hyperliquid_feed(state["hl"]),
        lighter_feed(state["lighter"]),
        edgex_feed(state["edgex"]),
        monitor_prices_diff(state, threshold_percent=PRICE_DIFF_PERCENTAGE_THRESHOLD),
        monitor_24h_funding_rate_diff(state, threshold_percent=FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD),
        periodic_clear_state(state),
    )

if __name__ == "__main__":
    asyncio.run(main())