import asyncio
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from statistics import mean

import httpx
from telegram import Bot
from telegram.error import TelegramError
from zoneinfo import ZoneInfo

# =========================================================
# 🔴 DEBUG TOKEN (CLAVE)
# =========================================================

BOT_TOKEN = os.getenv("8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ")
CHAT_ID = os.getenv("1983622390")

print("=== DEBUG VARIABLES ===")
print("BOT_TOKEN raw:", repr(BOT_TOKEN))
print("CHAT_ID raw:", repr(CHAT_ID))
print("=======================")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN NO EXISTE en Railway")

BOT_TOKEN = BOT_TOKEN.strip()

if ":" not in BOT_TOKEN:
    raise ValueError(f"❌ BOT_TOKEN inválido: {repr(BOT_TOKEN)}")

# =========================================================
# CONFIG
# =========================================================

ODDS_API_KEY = os.getenv("92f3a8c48fe9834c7b1e6bbf38346064")
API_FOOTBALL_KEY = os.getenv("c455630d0023ef208f93dd0567164905")

TIMEZONE = "America/Mexico_City"
ZONE = ZoneInfo(TIMEZONE)

DB_PATH = os.getenv("DB_PATH", "/data/bot_state.db")

# =========================================================
# BOT
# =========================================================

bot = Bot(token=BOT_TOKEN)

logging.basicConfig(level=logging.INFO)

# =========================================================
# LIGAS
# =========================================================

LEAGUES = [262, 39, 135, 2]

LEAGUE_NAMES = {
    262: "Liga MX",
    39: "Premier League",
    135: "Serie A",
    2: "Champions League",
}

SEASONS = {
    262: 2025,
    39: 2025,
    135: 2025,
    2: 2025,
}

ODDS_KEYS = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_italy_serie_a",
    "soccer_uefa_champs_league",
]

# =========================================================
# HELPERS
# =========================================================

def now():
    return datetime.now(ZONE)

def today():
    return now().strftime("%Y-%m-%d")

async def send(msg):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        print("Error Telegram:", e)

# =========================================================
# REQUESTS
# =========================================================

async def api(url, params=None):
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers={"x-apisports-key": API_FOOTBALL_KEY}, params=params)
        return r.json()

async def odds(url, params=None):
    async with httpx.AsyncClient() as client:
        r = await client.get(url, headers={"apiKey": ODDS_API_KEY}, params=params)
        return r.json()

# =========================================================
# DAILY CHECK
# =========================================================

async def check_today():
    total = 0
    per = {}

    for l in LEAGUES:
        data = await api(
            "https://v3.football.api-sports.io/fixtures",
            {
                "league": l,
                "season": SEASONS[l],
                "date": today(),
                "timezone": TIMEZONE,
            },
        )

        c = len(data.get("response", []))
        per[LEAGUE_NAMES[l]] = c
        total += c

    return total, per

async def sleep_tomorrow():
    now_dt = now()
    target = now_dt.replace(hour=7, minute=0, second=0)

    if now_dt >= target:
        target += timedelta(days=1)

    await asyncio.sleep(int((target - now_dt).total_seconds()))

# =========================================================
# PREMATCH
# =========================================================

async def prematch():
    for sport in ODDS_KEYS:
        try:
            data = await odds(
                f"https://api.the-odds-api.com/v4/sports/{sport}/odds",
                {"markets": "h2h"},
            )

            for e in data:
                prices = []
                for b in e.get("bookmakers", []):
                    for m in b.get("markets", []):
                        for o in m.get("outcomes", []):
                            if o.get("price"):
                                prices.append(o["price"])

                if prices:
                    avg = mean(prices)
                    if 1.7 <= avg <= 2.4:
                        await send(f"🔥 PREMATCH\n{e['home_team']} vs {e['away_team']}\nCuota {avg:.2f}")

        except:
            pass

# =========================================================
# LOOP
# =========================================================

last_day = None

async def loop():
    global last_day

    while True:
        t = today()

        if last_day != t and now().hour >= 7:
            last_day = t

            total, per = await check_today()

            if total == 0:
                msg = f"😴 No hay partidos hoy ({t})\n\n"
                for k, v in per.items():
                    msg += f"{k}: {v}\n"
                await send(msg)
                await sleep_tomorrow()
                continue
            else:
                msg = f"✅ Hay partidos hoy ({t})\nTotal: {total}\n\n"
                for k, v in per.items():
                    msg += f"{k}: {v}\n"
                await send(msg)

        await prematch()
        await asyncio.sleep(180)

# =========================================================
# MAIN
# =========================================================

async def main():
    await send("🤖 BOT V9 DEBUG INICIADO")
    await loop()

if __name__ == "__main__":
    asyncio.run(main())
