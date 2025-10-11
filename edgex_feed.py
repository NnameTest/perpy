import asyncio
import aiohttp
import websockets
import json
import time

API_URL = "https://pro.edgex.exchange"
INFO_URL = f"{API_URL}/api/v1/public/meta/getServerTime"
META_URL = f"{API_URL}/api/v1/public/meta/getMetaData"

WS_URL = "wss://quote.edgex.exchange/api/v1/public/ws"
WS_POST_MSG = json.dumps({
  "type": "subscribe",
  "channel": "ticker.all"
})

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
PONG_INTERVAL = 30                         # seconds between pongs
RECONNECT_DELAY = 5                        # seconds before reconnect

PRINT_PREFIX = "[edgex]: "

is_feed_available = False
ignore_tokens = []

async def check_exchange_health(state):
    global is_feed_available
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=INFO_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}⚠️ Health check failed, status: {resp.status}")
                    is_feed_available = False
                    state.clear()
                    return

                data = await resp.json()
                
                if "code" in data and data["code"] == "SUCCESS":
                    if not is_feed_available:
                        print(f"{PRINT_PREFIX}✅ Exchange feed is alive.")
                    is_feed_available = True
                else:
                    print(f"{PRINT_PREFIX}❌ Health check returned unexpected format.")
                    is_feed_available = False
                    state.clear()
    except Exception as e:
        if is_feed_available:
            print(f"{PRINT_PREFIX}❌ Exchange health check failed:", e)
        is_feed_available = False
        state.clear()

async def fill_ignore_tokens_list():
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=META_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch meta data:", await resp.text())
                    return
                data = (await resp.json())["data"]["contractList"]
                for item in data:
                    if item["enableTrade"] is False or item["enableDisplay"] is False or item["enableOpenPosition"] is False:
                        ignore_tokens.append(item["contractName"][:-3])  # remove "USD" suffix
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Error fetching exchange info:", e)

async def periodic_data_refresh(state):
    while True:
        await check_exchange_health(state)
        if is_feed_available:
            await fill_ignore_tokens_list()
        await asyncio.sleep(UPDATE_DATA_INTERVAL)

async def process_message(ws, message, state):
    try:
        data = json.loads(message)
        if "type" in message and data["type"] == "ping":
            await ws.send(json.dumps({"type": "pong", "time": data.get("time", time.time())}))
        elif "channel" in data and data["channel"] == "ticker.all" and "content" in data:
            for item in data["content"]["data"]:
                symbol = item["contractName"][:-3]  # remove "USD" suffix
                if symbol in ignore_tokens:
                    continue
                state.setdefault(symbol, {}).update({
                    "price": float(item["lastPrice"]),
                    "funding_rate": float(item["fundingRate"]),
                    "next_funding_time": item["nextFundingTime"],  # ms since epoch
                    "funding_interval_hours": (float(item["nextFundingTime"]) - float(item["fundingTime"])) / 3600000,
                })
    except Exception as e:
        print(f"{PRINT_PREFIX}❌ Failed to parse message: {e}")

async def handle_stream(state):
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
            ) as ws:
                await ws.send(WS_POST_MSG)
                async for message in ws:
                    # Skip processing if feed marked unavailable
                    if not is_feed_available:
                        await asyncio.sleep(RECONNECT_DELAY)
                        continue
                    await process_message(ws, message, state)
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Connection error: {e}")
            print(f"{PRINT_PREFIX}Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


async def edgex_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )
