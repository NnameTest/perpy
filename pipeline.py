import os
import time
import json
from collections import defaultdict

import asyncio
from dotenv import load_dotenv 

from asterdex_feed import asterdex_feed
from hyperliquid_feed import hyperliquid_feed
from diff_tasks import find_price_diff_task, find_funding_24h_rate_diff, find_next_funding_rate_diff
from telegram import send_telegram_message

load_dotenv()

START_DELAY=int(os.getenv("START_DELAY"))
PRINT_INTERVAL=int(os.getenv("PRINT_INTERVAL"))
PRICE_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("PRICE_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_NEXT_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("FUNDING_NEXT_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_NEXT_TIME_TOLERANCE_MINUTES=float(os.getenv("FUNDING_NEXT_TIME_TOLERANCE_MINUTES"))

async def monitor_prices_diff(state, threshold_percent):
    """Print tokens with significant price differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{threshold_percent}% price difference...")
        tokens_with_diff = find_price_diff_task(state, threshold_percent)
        if tokens_with_diff:
            print(f"📊 Tokens with >{threshold_percent}% price difference:")
            print(json.dumps(tokens_with_diff, indent=2))
            message = f"📊 Tokens with >{threshold_percent}% price difference:\n" + json.dumps(tokens_with_diff, indent=2)
            await send_telegram_message(message)
        else:
            print(f"📊 No tokens with >{threshold_percent}% price difference found.")

async def monitor_next_funding_rate_diff(state, threshold_percent=0.1):
    """Print tokens with significant next funding rate differences periodically."""
    await asyncio.sleep(START_DELAY)

    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Checking for tokens with >{threshold_percent}% next funding rate difference...")
        tokens_with_diff = find_next_funding_rate_diff(state, threshold_percent)
        if tokens_with_diff:
            print(f"📊 Tokens with >{threshold_percent}% next funding rate difference:")
            print(json.dumps(tokens_with_diff, indent=2))
            message = f"📊 Tokens with >{threshold_percent}% next funding rate difference:\n" + json.dumps(tokens_with_diff, indent=2)
            await send_telegram_message(message)
        else:
            print(f"📊 No tokens with >{threshold_percent}% next funding rate difference found.")

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
            message = f"📊 Tokens with >{threshold_percent}% 24h funding rate difference:\n" + json.dumps(tokens_with_diff, indent=2)
            await send_telegram_message(message)
        else:
            print(f"📊 No tokens with >{threshold_percent}% 24h funding rate difference found.")


async def main():
    state = defaultdict(dict)

    asterdex_state_key = "asterdex"
    hyperliquid_state_key = "hyperliquid"

    await asyncio.gather(
        asterdex_feed(state[asterdex_state_key]),
        hyperliquid_feed(state[hyperliquid_state_key]),
        monitor_prices_diff(state, threshold_percent=PRICE_DIFF_PERCENTAGE_THRESHOLD),
        monitor_next_funding_rate_diff(state, threshold_percent=FUNDING_NEXT_DIFF_PERCENTAGE_THRESHOLD),
        monitor_24h_funding_rate_diff(state, threshold_percent=FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD),
    )

if __name__ == "__main__":
    asyncio.run(main())