import asyncio
import aiohttp
import websockets
import json

BASE_URL = "https://contract.mexc.com/api/v1/contract"
PING_URL = f"{BASE_URL}/ping"
DETAIL_URL = f"{BASE_URL}/detail"
FUNDING_URL = f"{BASE_URL}/funding_rate"


WS_URL = "wss://contract.mexc.com/edge"
PRICE_MSG = json.dumps({
  "method": "sub.tickers",
  "param": {}
})
PING_MSG = json.dumps({
  "method": "ping"
})

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
RECONNECT_DELAY = 5                        # seconds before reconnect
PING_INTERVAL = 30

PRINT_PREFIX = "[mexc]: "

is_feed_available = False

async def check_exchange_health(state):
    global is_feed_available
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=PING_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Health check failed, status: {resp.status}")
                    is_feed_available = False
                    state.clear()
                    return

                data = await resp.json()
                
                if "success" in data and data["success"] == True:
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

async def fetch_tokens(state):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=DETAIL_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch meta data:", await resp.text())
                    return
                data = (await resp.json())
                for item in data["data"]:
                    if item["state"] == 0 and item["isHidden"] == False and item["type"] == 1 and item["quoteCoin"] == "USDT":
                        symbol = item["baseCoin"]
                        state.setdefault(symbol, {})
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Error fetching exchange info:", e)

async def fetch_funding_info(state):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=FUNDING_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch meta data:", await resp.text())
                    return
            
                data = await resp.json()

                for item in data["data"]:
                    if item["symbol"].endswith("_USDT"):
                        symbol = item["symbol"][:-5]
                        if symbol in state:
                            state[symbol].update({
                                "funding_rate": item["fundingRate"],
                                "next_funding_time": item["nextSettleTime"],
                                "funding_interval_hours": item["collectCycle"],
                            })
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Error fetching funding info:", e)

async def ping_loop(ws):
    while True:
        try:
            await asyncio.sleep(PING_INTERVAL)
            await ws.send(PING_MSG)
        except Exception as e:
            print(f"❌ Ping failed: {e}")
            break


async def periodic_data_refresh(state):
    while True:
        await check_exchange_health(state)
        if is_feed_available:
          await fetch_tokens(state)
          await fetch_funding_info(state)
        await asyncio.sleep(UPDATE_DATA_INTERVAL)

async def process_message(message, state):
    try:
        message = json.loads(message)
        if "channel" in message and message["channel"] == "pong" or message["channel"] == "rs.sub.tickers":
          return
        if "channel" in message and message["channel"] == "push.tickers":
          data = message["data"]
          for item in data:
              if item["symbol"].endswith("_USDT") and "lastPrice" in item and item["lastPrice"] > 0:
                  symbol = item["symbol"][:-5]
                  if symbol in state:
                      state[symbol].update({
                          "price": item["lastPrice"]
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
                await ws.send(PRICE_MSG)
                ping_task = asyncio.create_task(ping_loop(ws))
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
        finally:
            if 'ping_task' in locals():
                ping_task.cancel()

async def mexc_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )

