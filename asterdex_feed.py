import asyncio
import aiohttp
import websockets
import json
from datetime import datetime

# WS_URL = "wss://fstream.asterdex.com/ws"
WS_URL = "wss://fstream.asterdex.com/ws/!markPrice@arr"
FUNDING_URL = "https://fapi.asterdex.com/fapi/v1/fundingInfo"
INFO_URL = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"

INITIAL_STREAM_START_DELAY = 5             # seconds before starting main loop
INITIAL_PRINT_DELAY = 15                    # seconds before first print
UPDATE_FUNDING_INTERVAL = 60               # seconds between funding updates
PRINT_INTERVAL = 10                        # seconds between prints
PONG_INTERVAL = 30                         # seconds between pings
RECONNECT_DELAY = 5                        # seconds before reconnect

ignore_tokens = []

async def fill_ignore_tokens_list():
    """Fetch all symbols and filter out non-trading ones."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(INFO_URL) as resp:
                data = await resp.json()
                for item in data["symbols"]:
                    if item["status"] != "TRADING":
                        ignore_tokens.append(item["symbol"])  # remove "USD" suffix
        except Exception as e:
            print("Error fetching exchange info:", e)
            return

async def fetch_funding_info(state):
    """Fetch funding info from REST and update intervals."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(FUNDING_URL) as resp:
                data = await resp.json()
                for item in data:
                    if item["symbol"].endswith("USD") or item["symbol"] in ignore_tokens:
                        continue

                    symbol = item["symbol"][:-4]  # remove "USDT" suffix

                    state.setdefault(symbol, {})
                    state[symbol].update({
                        "funding_interval_hours": item.get("fundingIntervalHours"),
                    })
        except Exception as e:
            print("Error fetching funding info:", e)

async def periodic_funding_refresh(state):
    """Refetch funding info every 60 seconds."""
    while True:
        await asyncio.sleep(UPDATE_FUNDING_INTERVAL)
        await fetch_funding_info(state)

async def process_message(message: str, state):
    """Parse mark price updates and save to disk immediately."""
    try:
        data = json.loads(message)
        if isinstance(data, list):
          for item in data:
              if item["s"].endswith("USD") or item["s"] in ignore_tokens:
                  continue

              symbol = item["s"][:-4]  # remove "USDT" suffix
              state.setdefault(symbol, {}).update({
                  "price": float(item["p"]),
                  "funding_rate": float(item["r"]),
                  "next_funding_time": item["T"],  # ms since epoch
              })

    except Exception as e:
        print(f"⚠️ Failed to parse message: {e}")


async def pong_loop(ws):
    """Send manual pongs every minute to keep connection alive."""
    while True:
        try:
            await asyncio.sleep(PONG_INTERVAL)
            await ws.pong()
        except Exception as e:
            print(f"⚠️ Pong failed: {e}")
            break


async def handle_stream(state):
    """Main WebSocket connection handler with reconnect logic."""
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # we manage manually
                max_size=2**20,
            ) as ws:
                pong_task = asyncio.create_task(pong_loop(ws))
                async for message in ws:
                    await process_message(message, state)

        except Exception as e:
            print(f"❌ Connection error: {e}")
            print(f"Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        finally:
            if 'pong_task' in locals():
                pong_task.cancel()


async def asterdex_feed(state):
    await fill_ignore_tokens_list()
    await fetch_funding_info(state)  # initial load before stream
    await asyncio.gather(
        periodic_funding_refresh(state),
        handle_stream(state)
    )
