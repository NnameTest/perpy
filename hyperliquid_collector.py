import asyncio
import aiohttp
import websockets
import json
import os
import time

DATA_FILE = "data/hyperliquid_data.json"

FUNDING_URL = "https://api.hyperliquid.xyz/info"
API_POST_MSG = json.dumps({
	"type": "predictedFundings"
})

WS_URL = "wss://api.hyperliquid.xyz/ws"
WS_POST_MSG = {
  "method": "subscribe",
  "subscription": {
    "type": "allMids"
  }
}

INITIAL_STREAM_START_DELAY = 5             # seconds before starting main loop
UPDATE_FUNDING_INTERVAL = 60               # seconds between funding updates
SAVE_INTERVAL = 30                         # redundant safety save
PRINT_INTERVAL = 10                        # seconds between prints
PING_INTERVAL = 60                         # seconds between pings
RECONNECT_DELAY = 5                        # seconds before reconnect

combined_data = {}

async def fetch_funding_info():
    """Fetch funding info from REST and update intervals."""
    async with aiohttp.ClientSession() as session:
        try:
            session.headers.update({'Content-Type': 'application/json'})
            async with session.post(url=FUNDING_URL, data=API_POST_MSG) as resp:
                data = await resp.json()

                for item in data:
                    symbol = item[0]

                    symbol_funding_data = [x for x in item[1] if x[0] == "HlPerp"][0][1]
                    funding_rate = float(symbol_funding_data["fundingRate"])
                    next_funding_time = symbol_funding_data["nextFundingTime"]
                    funding_interval_hours = symbol_funding_data["fundingIntervalHours"]

                    combined_data.setdefault(symbol, {})
                    combined_data[symbol].update({
                        "funding_rate": funding_rate,
                        "next_funding_time": next_funding_time,
                        "funding_interval_hours": funding_interval_hours,
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
        message = json.loads(message)

        if message["channel"] != "allMids":
            return

        data = message["data"]["mids"]

        for symbol, price in data.items():
            if "@" in symbol or "/" in symbol:
                continue

            combined_data.setdefault(symbol, {}).update({
                "price": float(price),
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
            print("🔌 Connecting to price stream...")
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # we manage manually
                max_size=2**20,
            ) as ws:
                print("✅ Connected to stream")
                await ws.send(json.dumps(WS_POST_MSG))

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
    await fetch_funding_info()  # initial load before stream
    await asyncio.gather(
        periodic_funding_refresh(),
        handle_stream(),
        monitor_prices(),
        save_snapshot(),
    )


if __name__ == "__main__":
    asyncio.run(main())
