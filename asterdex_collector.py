import asyncio
import aiohttp
import websockets
import json
import os
import time

DATA_FILE = "data/asterdex_data.json"

WS_URL = "wss://fstream.asterdex.com/ws/!markPrice@arr"
FUNDING_URL = "https://fapi.asterdex.com/fapi/v1/fundingInfo"
INFO_URL = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"

INITIAL_STREAM_START_DELAY = 5             # seconds before starting main loop
UPDATE_FUNDING_INTERVAL = 60               # seconds between funding updates
SAVE_INTERVAL = 30                         # redundant safety save
PRINT_INTERVAL = 10                        # seconds between prints
PING_INTERVAL = 60                         # seconds between pings
RECONNECT_DELAY = 5                        # seconds before reconnect

ignore_tokens = []
combined_data = {}


async def fill_ignore_tokens_list():
    """Fetch all symbols and filter out non-trading ones."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(INFO_URL) as resp:
                data = await resp.json()
                for item in data["symbols"]:
                    if item["status"] != "TRADING":
                        ignore_tokens.append(item["symbol"])  # remove "USD" suffix
                print(f"Ignoring {len(ignore_tokens)} symbols")
                print(ignore_tokens)
        except Exception as e:
            print("Error fetching exchange info:", e)
            return

async def fetch_funding_info():
    """Fetch funding info from REST and update intervals."""
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(FUNDING_URL) as resp:
                data = await resp.json()
                for item in data:
                    if item["symbol"].endswith("USD") or item["symbol"] in ignore_tokens:
                        continue

                    symbol = item["symbol"][:-4]  # remove "USDT" suffix
                    combined_data.setdefault(symbol, {})
                    combined_data[symbol].update({
                        "funding_interval_hours": item.get("fundingIntervalHours"),
                    })
                print(f"Funding info updated")
        except Exception as e:
            print("Error fetching funding info:", e)

async def periodic_funding_refresh():
    """Refetch funding info every 60 seconds."""
    while True:
        await asyncio.sleep(UPDATE_FUNDING_INTERVAL)
        await fetch_funding_info()

async def process_message(message: str):
    """Parse mark price updates and save to disk immediately."""
    try:
        data = json.loads(message)
        for item in data:
            if item["s"].endswith("USD") or item["s"] in ignore_tokens:
                continue

            symbol = item["s"][:-4]  # remove "USDT" suffix
            combined_data.setdefault(symbol, {}).update({
                "price": float(item["p"]),
                "funding_rate": float(item["r"]),
                "next_funding_time": item["T"],  # ms since epoch
            })

    except Exception as e:
        print(f"⚠️ Failed to parse message: {e}")


async def ping_loop(ws):
    """Send manual pings every minute to keep connection alive."""
    while True:
        try:
            await asyncio.sleep(PING_INTERVAL)
            await ws.ping()
        except Exception as e:
            print(f"⚠️ Ping failed: {e}")
            break


async def save_snapshot():
    """Periodic redundant save in case of missed flush."""
    while True:
        await asyncio.sleep(SAVE_INTERVAL)
        if combined_data:
            try:
                with open(DATA_FILE, "w") as f:
                    json.dump(combined_data, f, indent=2)
                print(f"🕒 Periodic save: {len(combined_data)} symbols → {DATA_FILE}")
            except Exception as e:
                print(f"❌ Failed to write file: {e}")


async def handle_stream():
    """Main WebSocket connection handler with reconnect logic."""
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            print("🔌 Connecting to stream...")
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # we manage manually
                max_size=2**20,
            ) as ws:
                print("✅ Connected to stream")

                ping_task = asyncio.create_task(ping_loop(ws))

                async for message in ws:
                    await process_message(message)

        except Exception as e:
            print(f"❌ Connection error: {e}")
            print(f"Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)
        finally:
            if 'ping_task' in locals():
                ping_task.cancel()


async def monitor_prices():
    """Print a few symbols periodically."""
    while True:
        await asyncio.sleep(PRINT_INTERVAL)
        if combined_data:
            sample = list(combined_data.items())
            print("\n📊 Live sample:")
            for sym, d in sample:
                next_funding = time.strftime(
                    "%H:%M:%S", time.localtime(d["next_funding_time"] / 1000)
                )
                print(
                    f"{sym:<15} {d['price']:<15.4f} "
                    f"funding_rate={d['funding_rate']:<15.6f} "
                    f"next={next_funding:<15} interval={d.get('funding_interval_hours','?')}h"
                )


async def main():
    os.makedirs(os.path.dirname(DATA_FILE) or ".", exist_ok=True)
    await fill_ignore_tokens_list()
    await fetch_funding_info()  # initial load before stream
    await asyncio.gather(
        periodic_funding_refresh(),
        handle_stream(),
        monitor_prices(),
        save_snapshot(),
    )


if __name__ == "__main__":
    asyncio.run(main())
