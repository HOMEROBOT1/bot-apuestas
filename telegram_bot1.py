# ==============================
# V13.4 PRO - MULTI MERCADO
# ==============================

import asyncio
import logging
import os
import re
from datetime import datetime, timedelta
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

LOCAL_TZ = ZoneInfo("America/Mexico_City")

ODDS_MARKETS = "h2h,totals,btts"
MIN_VALUE_EDGE = 0.06

ACTIVE_START = 7
ACTIVE_END = 22

bot = Bot(token=BOT_TOKEN)

# ==============================
# LIGAS + SEASON
# ==============================

LEAGUES = {
    262: {"name": "Liga MX", "season": 2025},
    39: {"name": "Premier League", "season": 2025},
    2: {"name": "Champions League", "season": 2025},
}

SPORT_KEYS = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_uefa_champs_league"
]

sent_signals = set()
last_daily_alert = None

# ==============================
# UTIL
# ==============================

def now():
    return datetime.now(LOCAL_TZ)

def to_local(x):
    return datetime.fromisoformat(x.replace("Z","+00:00")).astimezone(LOCAL_TZ)

def safe_float(x):
    try: return float(x)
    except: return None

def prob(o): return 1/o if o else None
def odds(p): return 1/p if p else None

async def send(msg):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
    except Exception as e:
        logging.error(e)

# ==============================
# ODDS ANALYSIS MULTI
# ==============================

def analyze(match):
    home = match["home_team"]
    away = match["away_team"]

    candidates = []

    for bm in match.get("bookmakers", []):
        for m in bm.get("markets", []):

            # H2H
            if m["key"] == "h2h":
                for o in m["outcomes"]:
                    name = o["name"]
                    price = safe_float(o["price"])
                    if not price: continue

                    label = None
                    if name.lower() == home.lower():
                        label = f"Gana {home}"
                    elif name.lower() == away.lower():
                        label = f"Gana {away}"
                    elif name.lower() in ["draw","empate"]:
                        label = "Empate"

                    if label:
                        candidates.append(("h2h", label, price))

            # TOTALS
            if m["key"] == "totals":
                for o in m["outcomes"]:
                    side = o["name"].lower()
                    pt = safe_float(o["point"])
                    price = safe_float(o["price"])
                    if not price or pt is None: continue

                    if pt == 1.5 and side == "over":
                        candidates.append(("totals", "Over 1.5 goles", price))
                    if pt == 2.5 and side == "over":
                        candidates.append(("totals", "Over 2.5 goles", price))
                    if pt == 3.5 and side == "under":
                        candidates.append(("totals", "Under 3.5 goles", price))

            # BTTS
            if m["key"] == "btts":
                for o in m["outcomes"]:
                    if o["name"].lower() in ["yes","si","sí"]:
                        price = safe_float(o["price"])
                        if price:
                            candidates.append(("btts","Ambos anotan: Sí", price))

    if not candidates:
        return None

    best = max(candidates, key=lambda x: x[2])
    return {
        "market": best[0],
        "pick": best[1],
        "odds": round(best[2],2)
    }

# ==============================
# API CALLS
# ==============================

async def get_odds():
    all = []
    async with httpx.AsyncClient() as client:
        for s in SPORT_KEYS:
            url = f"https://api.the-odds-api.com/v4/sports/{s}/odds"
            r = await client.get(url, params={
                "apiKey": ODDS_API_KEY,
                "regions":"uk",
                "markets":ODDS_MARKETS
            })
            data = r.json()
            all.extend(data)
    return all

async def get_fixtures():
    today = now().strftime("%Y-%m-%d")
    all = []

    async with httpx.AsyncClient() as client:
        for lid,cfg in LEAGUES.items():
            r = await client.get(
                "https://v3.football.api-sports.io/fixtures",
                headers={"x-apisports-key": API_FOOTBALL_KEY},
                params={
                    "league": lid,
                    "season": cfg["season"],
                    "date": today,
                    "timezone":"America/Mexico_City"
                }
            )
            data = r.json()["response"]

            logging.info(f"{cfg['name']} -> {len(data)} partidos")

            for f in data:
                all.append(f)

    logging.info(f"TOTAL HOY: {len(all)}")
    return all

# ==============================
# CICLO
# ==============================

async def run():

    global last_daily_alert

    hour = now().hour

    if hour < ACTIVE_START or hour >= ACTIVE_END:
        return True

    fixtures = await get_fixtures()

    today = now().strftime("%Y-%m-%d")

    if not fixtures:
        if last_daily_alert != today:
            await send("📭 Hoy no hay partidos en tus ligas.")
            last_daily_alert = today
        return True
    else:
        if last_daily_alert != today:
            await send("📅 Hay partidos hoy. Bot activo.")
            last_daily_alert = today

    odds = await get_odds()

    for m in odds:
        analysis = analyze(m)
        if not analysis:
            continue

        key = f"{m['home_team']}|{analysis['pick']}"
        if key in sent_signals:
            continue

        msg = (
            f"📊 BETTING SIGNAL\n\n"
            f"{m['home_team']} vs {m['away_team']}\n"
            f"🎯 {analysis['pick']}\n"
            f"💰 Cuota: {analysis['odds']}"
        )

        await send(msg)
        sent_signals.add(key)

    return False

# ==============================
# LOOP
# ==============================

async def main():
    while True:
        try:
            sleep = await run()
            await asyncio.sleep(900 if not sleep else 1800)
        except Exception as e:
            logging.exception(e)
            await asyncio.sleep(60)

asyncio.run(main())
