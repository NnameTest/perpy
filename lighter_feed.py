import asyncio
import aiohttp
import websockets
import json
from datetime import datetime, timedelta, timezone

API_URL = "https://mainnet.zklighter.elliot.ai"
ORDER_BOOK_URL = f"{API_URL}/api/v1/orderBooks"

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
WS_POST_MSG ={
  "type": "subscribe",
  "channel": "market_stats/all"
}

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
RECONNECT_DELAY = 5                        # seconds before reconnect

PRINT_PREFIX = "[lighter]: "

is_feed_available = False
market_to_symbol_data = {}

async def check_exchange_health(state):
    global is_feed_available
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=API_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Health check failed, status: {resp.status}")
                    is_feed_available = False
                    state.clear()
                    return

                data = await resp.json()
                
                if "timestamp" in data and data["timestamp"] > 0:
                    if not is_feed_available:
                        print(f"{PRINT_PREFIX}✅ Exchange feed recovered.")
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

async def fill_market_to_symbol_data():
    global market_to_symbol_data
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(ORDER_BOOK_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch market data:", await resp.text())
                    return
                data = await resp.json()
                symbols_data = data.get("order_books")

                for item in symbols_data:
                    if item.get("status") != "active":
                        continue
                    market_to_symbol_data[item.get("market_id")] = item.get("symbol")
    except Exception as e:
        print(f"{PRINT_PREFIX}❌ Error fetching market info:", e)

async def periodic_data_refresh(state):
    while True:
        await check_exchange_health(state)
        if is_feed_available:
          await fill_market_to_symbol_data()
        await asyncio.sleep(UPDATE_DATA_INTERVAL)

async def process_message(ws, message, state):
    try:
        data = json.loads(message)
        if "type" in data and data["type"] == "ping":
            await ws.send(json.dumps({"type": "pong"}))
        elif "channel" in data and "market_stats" in data["channel"]:
            for item in data.get("market_stats").values():
                market_id = item.get("market_id")
                if market_id not in market_to_symbol_data:
                    continue
                
                symbol = market_to_symbol_data[market_id]

                funding_interval_hours = 1
                next_funding_time = int((datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).timestamp()*1000)

                state.setdefault(symbol, {}).update({
                    "price": float(item.get("last_trade_price")),
                    "funding_rate": float(item.get("current_funding_rate"))/100,
                    "next_funding_time": next_funding_time,
                    "funding_interval_hours": funding_interval_hours,
                })
    except Exception as e:
        print(f"❌ Failed to parse message: {e}")


async def handle_stream(state):
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL
            ) as ws:
                await ws.send(json.dumps(WS_POST_MSG))
                async for message in ws:
                    # Skip processing if feed marked unavailable
                    if not is_feed_available:
                        await asyncio.sleep(RECONNECT_DELAY)
                        continue
                    await process_message(ws, message, state)
        except Exception as e:
            print(f"❌ Connection error: {e}")
            print(f"Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

async def lighter_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )

