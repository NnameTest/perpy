import asyncio
import aiohttp
import websockets
import json
from datetime import datetime, timedelta, timezone

INFO_URL = "https://api.starknet.extended.exchange/api/v1/info/markets"

WS_URL = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1/prices/mark"

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
RECONNECT_DELAY = 5                        # seconds before reconnect

PRINT_PREFIX = "[extended]: "

is_feed_available = False

async def check_exchange_health_and_fill_data(state):
    global is_feed_available
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=INFO_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Health check failed, status: {resp.status}")
                    is_feed_available = False
                    state.clear()
                    return

                data = await resp.json()
                
                if "status" in data and data["status"] == "OK":
                    if not is_feed_available:
                        print(f"{PRINT_PREFIX}✅ Exchange feed is alive.")
                    is_feed_available = True
                    fill_periodic_data(data["data"], state)
                else:
                    print(f"{PRINT_PREFIX}❌ Health check returned unexpected format.")
                    is_feed_available = False
                    state.clear()
    except Exception as e:
        if is_feed_available:
            print(f"{PRINT_PREFIX}❌ Exchange health check failed:", e)
        is_feed_available = False
        state.clear()

def fill_periodic_data(data, state):
    funding_interval_hours = 1
    next_funding_time = int((datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).timestamp()*1000)

    for item in data:
        if item["active"] == True and item["status"] == "ACTIVE":
            symbol = item["assetName"]

            price = float(item["marketStats"]["markPrice"])
            funding_rate = float(item["marketStats"]["fundingRate"])

            state.setdefault(symbol, {})
            state[symbol].update({
                "price": price,
                "funding_rate": funding_rate,
                "next_funding_time": next_funding_time,
                "funding_interval_hours": funding_interval_hours,
            })

async def periodic_data_refresh(state):
    while True:
        await check_exchange_health_and_fill_data(state)
        await asyncio.sleep(UPDATE_DATA_INTERVAL)

async def process_message(message: str, state):
    try:
        message = json.loads(message)
        if message["type"] == "MP":
          data = message["data"]
          symbol = data["m"][: -4]

          if symbol in state:
              state[symbol].update({
                  "price": float(data["p"]),
              })
        else:
          print(f"{PRINT_PREFIX}❌ Unknown message type: {message['type']}")
          print(f"{PRINT_PREFIX}Message content: {message}")
    except Exception as e:
        print(f"{PRINT_PREFIX}❌ Failed to parse message: {e}")

async def handle_stream(state):
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
            ) as ws:
                async for message in ws:
                    # Skip processing if feed marked unavailable
                    if not is_feed_available:
                        await asyncio.sleep(RECONNECT_DELAY)
                        continue
                    
                    await process_message(message, state)
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Connection error: {e}")
            print(f"{PRINT_PREFIX}Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

async def extended_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )

