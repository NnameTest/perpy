import asyncio
import aiohttp
import websockets
import json

WS_URL = "wss://fstream.asterdex.com/ws/!markPrice@arr"
FUNDING_URL = "https://fapi.asterdex.com/fapi/v1/fundingInfo"
INFO_URL = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"

INITIAL_STREAM_START_DELAY = 30            # seconds before starting main loop
UPDATE_DATA_INTERVAL = 60                  # seconds between data updates
PONG_INTERVAL = 30                         # seconds between pongs
RECONNECT_DELAY = 5                        # seconds before reconnect

PRINT_PREFIX = "[asterdex]: "

is_feed_available = False
ignore_tokens = []

async def check_exchange_health(state):
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
                
                if "serverTime" in data and data["serverTime"] > 0:
                    if not is_feed_available:
                        print(f"{PRINT_PREFIX}✅ Exchange is alive.")
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
            async with session.get(INFO_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch info data:", await resp.text())
                    return
                data = await resp.json()
                for item in data["symbols"]:
                    if item["status"] != "TRADING":
                        ignore_tokens.append(item["symbol"])
        except Exception as e:
            print(f"{PRINT_PREFIX}❌ Error fetching exchange info:", e)

async def fetch_funding_info(state):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(FUNDING_URL) as resp:
                if resp.status != 200:
                    print(f"{PRINT_PREFIX}❌ Failed to fetch funding data:", await resp.text())
                    return
                data = await resp.json()
                for item in data:
                    if item["symbol"].endswith("USD") or item["symbol"] in ignore_tokens:
                        continue

                    symbol = item["symbol"][:-4]  # remove "USDT" suffix

                    state.setdefault(symbol, {})
                    state[symbol].update({
                        "funding_interval_hours": item.get("fundingIntervalHours"),
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
        data = json.loads(message)
        if isinstance(data, list):
          for item in data:
              if item["s"].endswith("USD") or item["s"] in ignore_tokens:
                  continue

              symbol = item["s"][:-4]  # remove "USDT" suffix
              state.setdefault(symbol, {}).update({
                  "price": float(item["p"]),
                  "funding_rate": float(item["r"]),
                  "next_funding_time": item["T"],  # ms since epoch
              })
    except Exception as e:
        print(f"{PRINT_PREFIX}❌ Failed to parse message: {e}")


async def pong_loop(ws):
    while True:
        try:
            await asyncio.sleep(PONG_INTERVAL)
            await ws.pong()
        except Exception as e:
            print(f"❌ Pong failed: {e}")
            break


async def handle_stream(state):
    await asyncio.sleep(INITIAL_STREAM_START_DELAY)
    while True:
        try:
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
            ) as ws:
                pong_task = asyncio.create_task(pong_loop(ws))
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
            if 'pong_task' in locals():
                pong_task.cancel()


async def asterdex_feed(state):
    await asyncio.gather(
        periodic_data_refresh(state),
        handle_stream(state),
    )
