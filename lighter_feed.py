import asyncio
import aiohttp
import websockets
import json
from datetime import datetime, timedelta, timezone, time

API_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"

WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
WS_POST_MSG ={
  "type": "subscribe",
  "channel": "market_stats/all"
}

UDATE_MARKETS_INTERVAL = 60                # seconds between market list updates
INITIAL_STREAM_START_DELAY = 5             # seconds before starting main loop
PRINT_INTERVAL = 10                        # seconds between prints
RECONNECT_DELAY = 5                        # seconds before reconnect

market_to_symbol_data = {}

async def fill_market_to_symbol_data():
    async with aiohttp.ClientSession() as session:
        async with session.get(API_URL) as resp:
            data = await resp.json()
            symbols_data = data.get("order_books")

            for item in symbols_data:
                if item.get("status") != "active":
                    continue
                market_to_symbol_data[item.get("market_id")] = item.get("symbol")

async def periodic_market_to_symbol_refresh():
    """Refetch funding info every 60 seconds."""
    while True:
        await asyncio.sleep(UDATE_MARKETS_INTERVAL)
        await fill_market_to_symbol_data()


async def process_message(ws, message, state):
    """Parse mark price updates and save to disk immediately."""
    try:
        data = json.loads(message)

        if "type" in data and data["type"] == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        if "channel" in data and "market_stats" in data["channel"]:
            for item in data.get("market_stats").values():
                market_id = item.get("market_id")
                if market_id not in market_to_symbol_data:
                    continue
                symbol = market_to_symbol_data[market_id]

                funding_interval_hours = 1
                next_funding_time = int((datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).timestamp()*1000)

                state.setdefault(symbol, {}).update({
                    "price": float(item.get("mark_price")),
                    "funding_rate": float(item.get("current_funding_rate"))/100,
                    "next_funding_time": next_funding_time,
                    "funding_interval_hours": funding_interval_hours,
                })
            return

    except Exception as e:
        print(f"⚠️ Failed to parse message: {e}")


async def handle_stream(state):
    """Main WebSocket connection handler with reconnect logic."""
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL
            ) as ws:
                await ws.send(json.dumps(WS_POST_MSG))

                async for message in ws:
                    await process_message(ws, message, state)

        except Exception as e:
            print(f"❌ Connection error: {e}")
            print(f"Reconnecting in {RECONNECT_DELAY}s...")
            await asyncio.sleep(RECONNECT_DELAY)

async def lighter_feed(state):
    await fill_market_to_symbol_data()
    await asyncio.gather(
        periodic_market_to_symbol_refresh(),
        handle_stream(state),
    )

