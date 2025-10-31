import os
import time
from collections import defaultdict

import asyncio
from dotenv import load_dotenv 

from diffs import find_funding_diff_table, find_price_diff_table, print_diff_table
from telegram import send_detailed_diff_telegram_message
from asterdex_feed import asterdex_feed
from hyperliquid_feed import hyperliquid_feed
from lighter_feed import lighter_feed
from edgex_feed import edgex_feed
from extended_feed import extended_feed
from mexc_feed import mexc_feed
from gate_feed import gate_feed

load_dotenv()

IS_PRICE_DIFF_ENABLED=int(os.getenv("IS_PRICE_DIFF_ENABLED"))
IS_FUNDING_DIFF_ENABLED=int(os.getenv("IS_ASTER_ENABLED"))

START_DELAY=int(os.getenv("START_DELAY"))
PRINT_INTERVAL=int(os.getenv("PRINT_INTERVAL"))
CLEAR_STATE_INTERVAL=int(os.getenv("CLEAR_STATE_INTERVAL"))
PRICE_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("PRICE_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD=float(os.getenv("FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD"))
FUNDING_NEXT_TIME_TOLERANCE_MINUTES=float(os.getenv("FUNDING_NEXT_TIME_TOLERANCE_MINUTES"))

IS_ASTER_ENABLED=int(os.getenv("IS_ASTER_ENABLED"))
IS_EDGEX_ENABLED=int(os.getenv("IS_EDGEX_ENABLED"))
IS_LIGHTER_ENABLED=int(os.getenv("IS_LIGHTER_ENABLED"))
IS_HL_ENABLED=int(os.getenv("IS_HL_ENABLED"))
IS_EXTENDED_ENABLED=int(os.getenv("IS_EXTENDED_ENABLED"))
IS_GATE_ENABLED=int(os.getenv("IS_GATE_ENABLED"))
IS_MEXC_ENABLED=int(os.getenv("IS_MEXC_ENABLED"))

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
        for key in state:
            state[key].clear()
        print(f"\n🕒 {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("State sub-dictionaries cleared to prevent memory bloat.")

async def main():
    state = defaultdict(dict)

    tasks = []

    if IS_ASTER_ENABLED:
        tasks.append(asterdex_feed(state["aster"]))
    if IS_HL_ENABLED:
        tasks.append(hyperliquid_feed(state["hl"]))
    if IS_LIGHTER_ENABLED:
        tasks.append(lighter_feed(state["lighter"]))
    if IS_EDGEX_ENABLED:
        tasks.append(edgex_feed(state["edgex"]))
    if IS_EXTENDED_ENABLED:
        tasks.append(extended_feed(state["extended"]))
    if IS_MEXC_ENABLED:
        tasks.append(mexc_feed(state["mexc"]))
    if IS_GATE_ENABLED:
        tasks.append(gate_feed(state["gate"]))

    if IS_PRICE_DIFF_ENABLED:
        tasks.append(monitor_prices_diff(state, threshold_percent=PRICE_DIFF_PERCENTAGE_THRESHOLD))
    if IS_FUNDING_DIFF_ENABLED:
        tasks.append(monitor_24h_funding_rate_diff(state, threshold_percent=FUNDING_24H_DIFF_PERCENTAGE_THRESHOLD))
    
    tasks += [
        periodic_clear_state(state),
    ]

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())