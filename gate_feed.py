import asyncio
import aiohttp
import websockets
import json

BASE_URL = "https://api.gateio.ws/api/v4"
CONTRACTS_URL = f"{BASE_URL}/futures/usdt/contracts"


WS_URL = "wss://fx-ws.gateio.ws/v4/ws/usdt"
DATA_MSG = json.dumps({"channel": "futures.tickers", "event": "subscribe", "payload": ["!all"]})

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
RECONNECT_DELAY = 5                        # seconds before reconnect

PRINT_PREFIX = "[gate]: "

is_feed_available = False

async def check_exchange_health_and_fill_tokens_data(state):
    global is_feed_available
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=CONTRACTS_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Health check failed, status: {resp.status}")
                    is_feed_available = False
                    state.clear()
                    return

                data = await resp.json()
                
                if isinstance(data, list) and len(data) > 0:
                    if not is_feed_available:
                        print(f"{PRINT_PREFIX}✅ Exchange feed is alive.")
                    is_feed_available = True
                    fill_tokens_data(data, state)
                else:
                    print(f"{PRINT_PREFIX}❌ Health check returned unexpected format.")
                    is_feed_available = False
                    state.clear()
    except Exception as e:
        if is_feed_available:
            print(f"{PRINT_PREFIX}❌ Exchange health check failed:", e)
        is_feed_available = False
        state.clear()

def fill_tokens_data(data, state):
    for item in data:
        if item["in_delisting"] == True or item["status"] != "trading" or item["is_pre_market"] == True:
            continue
        
        symbol = item["name"][:-5]
        state.setdefault(symbol, {})

        state[symbol].update({
            "price": float(item["last_price"]),
            "funding_rate": float(item["funding_rate"]),
            "next_funding_time": item["funding_next_apply"] * 1000,
            "funding_interval_hours": item["funding_interval"] / 3600,
        })


async def periodic_data_refresh(state):
    while True:
        await check_exchange_health_and_fill_tokens_data(state)
        await asyncio.sleep(UPDATE_DATA_INTERVAL)

async def process_message(message, state):
    try:
        message = json.loads(message)

        if "channel" in message and message["channel"] == "futures.tickers" and "event" in message and message["event"] == "update":
          data = message["result"]
          for item in data:
              symbol = item["contract"][:-5]
              if symbol in state:
                  state[symbol].update({
                      "price": float(item["last"])
                  })
        else:
            print(f"{PRINT_PREFIX} Not Ticker Message")
            print(json.dumps(message, indent=4))
    except Exception as e:
        print(f"{PRINT_PREFIX}❌ Failed to parse message: {e}")

async def handle_stream(state):
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
            ) as ws:
                await ws.send(DATA_MSG)
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

async def gate_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )

