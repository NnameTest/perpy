import asyncio
import aiohttp
import websockets
import json
from datetime import datetime, timedelta, timezone

INFO_URL = "https://api.hyperliquid.xyz/info"
HEALTH_API_POST_MSG = json.dumps({ 
  "type": "exchangeStatus" 
})
META_API_POST_MSG = json.dumps({
	"type": "metaAndAssetCtxs"
})

WS_URL = "wss://api.hyperliquid.xyz/ws"
WS_POST_MSG = json.dumps({
  "method": "subscribe",
  "subscription": {
    "type": "allMids"
  }
})

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
RECONNECT_DELAY = 5                        # seconds before reconnect

PRINT_PREFIX = "[hyperliquid]: "

is_feed_available = False
ignore_tokens = []

async def check_exchange_health(state):
    global is_feed_available
    try:
        async with aiohttp.ClientSession() as session:
            session.headers.update({'Content-Type': 'application/json'})
            async with session.post(url=INFO_URL, data=HEALTH_API_POST_MSG) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Health check failed, status: {resp.status}")
                    is_feed_available = False
                    state.clear()
                    return

                data = await resp.json()
                
                if "time" in data and data["time"] > 0:
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
            session.headers.update({'Content-Type': 'application/json'})
            async with session.post(url=INFO_URL, data=META_API_POST_MSG) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch meta data:", await resp.text())
                    return
                data = (await resp.json())[0]
                for item in data["universe"]:
                    if "isDelisted" in item or "onlyIsolated" in item:
                        ignore_tokens.append(item["name"])
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Error fetching exchange info:", e)

async def fetch_funding_info(state):
    async with aiohttp.ClientSession() as session:
        try:
            session.headers.update({'Content-Type': 'application/json'})
            async with session.post(url=INFO_URL, data=META_API_POST_MSG) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch meta data:", await resp.text())
                    return
            
                data = await resp.json()

                funding_interval_hours = 1
                next_funding_time = int((datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).timestamp()*1000)

                for i in range(len(data[1])):
                    symbol = data[0]["universe"][i]["name"]

                    if symbol in ignore_tokens:
                        continue

                    funding_rate = float(data[1][i]["funding"])

                    state.setdefault(symbol, {})
                    state[symbol].update({
                        "funding_rate": funding_rate,
                        "next_funding_time": next_funding_time,
                        "funding_interval_hours": funding_interval_hours,
                    })
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Error fetching funding info:", e)

async def periodic_data_refresh(state):
    while True:
        await check_exchange_health(state)
        if is_feed_available:
          await fill_ignore_tokens_list()
          await fetch_funding_info(state)
        await asyncio.sleep(UPDATE_DATA_INTERVAL)

async def process_message(message: str, state):
    try:
        message = json.loads(message)
        if message["channel"] == "allMids":
          data = message["data"]["mids"]
          for symbol, price in data.items():
              if "@" in symbol or "/" in symbol or symbol in ignore_tokens:
                  continue
              state.setdefault(symbol, {}).update({
                  "price": float(price),
              })
    except Exception as e:
        print(f"{PRINT_PREFIX}❌ Failed to parse message: {e}")

async def handle_stream(state):
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
            ) as ws:
                await ws.send(WS_POST_MSG)
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

async def hyperliquid_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )

