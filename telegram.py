# telegram_utils.py
import os
from datetime import datetime, timezone

import aiohttp
import asyncio

from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

async def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Missing TELEGRAM_BOT_TOKEN or CHAT_ID in .env")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            if resp.status != 200:
                print("❌ Failed to send Telegram message:", await resp.text())

def format_price_diff_message(data):
    token = data["token"]
    min_feed = data["min_feed"]
    max_feed = data["max_feed"]
    min_price = data["min_price"]
    max_price = data["max_price"]
    abs_diff = data["abs_diff"]
    diff_pct = data["diff_pct"]
    all_prices = data["all_prices"]

    # build "all prices" list
    prices_str = "\n".join(
        [f"• `{feed}` — *{price:.4f}*" for feed, price in all_prices.items()]
    )

    msg = (
        f"📊 *Price Difference Alert*\n"
        f"💎 *Token:* `{token}`\n"
        f"🔺 *Max Feed:* {max_feed} — *{max_price:.4f}*\n"
        f"🔻 *Min Feed:* {min_feed} — *{min_price:.4f}*\n"
        f"📈 *Diff:* `${abs_diff:.4f}` (*{diff_pct:.2f}%*)\n\n"
        f"💬 *All Prices:*\n{prices_str}"
    )

    return msg

async def send_price_diff_telegram_message(data):
    """Send formatted Telegram alerts for all results."""
    for item in data:
        msg = format_price_diff_message(item)
        await send_telegram_message(msg)
        await asyncio.sleep(0.2) 

def format_24h_funding_rate_diff_message(data):
    token = data["token"]
    min_feed = data["min_feed"]
    max_feed = data["max_feed"]
    min_rate = data["min_rate_24h"]
    max_rate = data["max_rate_24h"]
    diff = data["diff"]
    diff_pct = data["diff_pct"]
    all_rates = data["all_rates_24h"]

    rates_str = "\n".join(
        [f"• `{feed}` — *{rate * 100:.4f}%*" for feed, rate in all_rates.items()]
    )

    msg = (
        f"💰 *24h Funding Rate Difference Alert*\n"
        f"💎 *Token:* `{token}`\n"
        f"🔺 *Max Feed:* {max_feed} — *{max_rate * 100:.4f}%*\n"
        f"🔻 *Min Feed:* {min_feed} — *{min_rate * 100:.4f}%*\n"
        f"📊 *Diff:* *{diff * 100:.4f}%* (*{diff_pct:.2f}%*)\n\n"
        f"💬 *24h Funding Rates:*\n{rates_str}"
    )

    return msg


async def send_24h_funding_rate_diff_notifications(data):
    """Send formatted Telegram alerts for funding rate diffs."""
    for item in data:
        msg = format_24h_funding_rate_diff_message(item)
        await send_telegram_message(msg)
        await asyncio.sleep(0.2)

def format_next_funding_diff_message(data):
    token = data["token"]
    feeds = ", ".join(data["feeds"])
    nearest_time = datetime.fromtimestamp(data["nearest_funding_time"] / 1000, tz=timezone.utc)
    time_until = data["time_until_funding_hours"]
    funding_diff = data["funding_rate_diff"]
    funding_diff_pct = data["funding_rate_diff_percent"]
    max_rate = data["max_rate"]
    min_rate = data["min_rate"]
    count_feeds = data["count_feeds"]

    msg = (
        f"⏳ *Next Funding Rate Diff Alert*\n"
        f"💎 *Token:* `{token}`\n"
        f"📝 *Feeds:* {feeds} ({count_feeds})\n"
        f"⏰ *Next Time:* {nearest_time.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"⏳ *Time Until Funding:* {time_until:.2f}h\n"
        f"🔺 Max Rate: {max_rate*100:.4f}%"
        f"🔻 Min Rate: {min_rate*100:.4f}%"
        f"📊 *Funding Rate Diff:* {funding_diff*100:.4f}% ({funding_diff_pct:.2f}%)\n"
    )

    return msg


async def send_next_funding_diff_notifications(data):
    """Send formatted Telegram alerts for next funding rate differences."""
    for item in data:
        msg = format_next_funding_diff_message(item)
        await send_telegram_message(msg)
        await asyncio.sleep(0.2)  # avoid flooding Telegram