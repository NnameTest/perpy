# telegram_utils.py
import os
from datetime import datetime, timezone

import aiohttp
import asyncio

from dotenv import load_dotenv

from alert_cache import should_send_alert

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

async def send_telegram_message(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Missing TELEGRAM_BOT_TOKEN or CHAT_ID in .env")
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


def format_number(x, precision=8):
    if isinstance(x, (int, float)):
        if abs(x) < 1e-4 and x != 0:   # threshold for scientific notation
            return f"{x:.2e}"          # e.g. 1.23e-05
        return f"{x: .{precision}f}"  # regular fixed-point
    return str(x) 

def format_diff_for_telegram(diff_table, top_n_feeds=None):
    """
    Telegram-friendly table
    
    Args:
        diff_table: output from find_price_diff_table() or find_funding_diff_table()
        top_n_feeds: int or None, limit number of feeds shown per token
    """
    posts = []
    for token_data in diff_table:
        if not should_send_alert(token_data):
            continue
        
        lines = []
      
        token = token_data["token"]
        sort_by = token_data["sortBy"]
        feeds = token_data["feeds"]

        if top_n_feeds:
            feeds = feeds[:top_n_feeds]

        # Header
        lines.append(f"{token} (sorted by {sort_by.upper()})\n")
        header_fmt = "{:<8}  {:<10} {:>6} {:>10}"
        lines.append(header_fmt.format("Source", "Price", "ΔPrice%", "ΔFund24h%"))

        # Rows
        row_fmt = "{:<8} {:<10}   {:>6} {:>10}"
        for f in feeds:
            lines.append(row_fmt.format(
                f["feed"],
                f"{format_number(f['price'], precision=6)}",
                f"{format_number(f['priceDiffPct'], precision=2)}%",
                f"{format_number(f['funding24RateDiffPct'], precision=2)}%"
            ))

        lines.append("")  # empty line between tokens
        posts.append("```\n" + "\n".join(lines) + "\n```")

    return posts


async def send_detailed_diff_telegram_message(diff_table):
    """Send detailed diff table as a Telegram message."""
    if not diff_table:
        return

    posts = format_diff_for_telegram(diff_table)
    for post in posts:    
      await send_telegram_message(post)
