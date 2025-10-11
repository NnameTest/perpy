import asyncio
import aiohttp
import websockets
import json
from datetime import datetime, timedelta, timezone

INFO_URL = "https://api.hyperliquid.xyz/info"
META_API_POST_MSG = json.dumps({
	"type": "metaAndAssetCtxs"
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
PRINT_INTERVAL = 10                        # seconds between prints
RECONNECT_DELAY = 5                        # seconds before reconnect

ignore_tokens = []

async def fill_ignore_tokens_list():
    """Fetch all symbols and filter not suitible."""
    async with aiohttp.ClientSession() as session:
        try:
            session.headers.update({'Content-Type': 'application/json'})
            async with session.post(url=INFO_URL, data=META_API_POST_MSG) as resp:
                data = (await resp.json())[0]
                for item in data["universe"]:
                    if item.get("isDelisted") == True:
                        ignore_tokens.append(item["name"])
        except Exception as e:
            print("Error fetching exchange info:", e)
            return

async def fetch_funding_info(state):
    """Fetch funding info from REST and update intervals."""
    async with aiohttp.ClientSession() as session:
        try:
            session.headers.update({'Content-Type': 'application/json'})
            async with session.post(url=INFO_URL, data=META_API_POST_MSG) as resp:
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
            print("Error fetching funding info:", e)

async def periodic_funding_refresh(state):
    """Refetch funding info every 60 seconds."""
    while True:
        await asyncio.sleep(UPDATE_FUNDING_INTERVAL)
        await fetch_funding_info(state)

async def process_message(message: str, state):
    """Parse mark price updates and save to disk immediately."""
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
        print(f"⚠️ Failed to parse message: {e}")

async def handle_stream(state):
    """Main WebSocket connection handler with reconnect logic."""
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,  # we manage manually
            ) as ws:
                await ws.send(json.dumps(WS_POST_MSG))
                async for message in ws:
                    await process_message(message, state)
        except Exception as e:
            print(f"❌ Connection error: {e}")
            print(f"Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)


async def hyperliquid_feed(state):
    await fill_ignore_tokens_list()
    await fetch_funding_info(state)  # initial load before stream
    await asyncio.gather(
        periodic_funding_refresh(state),
        handle_stream(state),
    )

