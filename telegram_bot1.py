"""
V11 PREMATCH PRO - Betting Bot
--------------------------------
Prioridad: PRE-MATCH COMBINADAS
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot

# =========================
# CONFIG
# =========================

BOT_TOKEN = os.getenv("8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ")
CHAT_ID = os.getenv("1983622390")
ODDS_API_KEY = os.getenv("92f3a8c48fe9834c7b1e6bbf38346064")

TZ = ZoneInfo("America/Mexico_City")

LEAGUES = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_uefa_champs_league"
]

SCAN_INTERVAL = 900  # 15 min
PREMATCH_WINDOW_HOURS = 18

# =========================
# FILTROS
# =========================

MIN_ODD = 1.35
MAX_ODD = 2.20

MIN_COMBO = 2.0
MAX_COMBO = 4.2

MAX_LEGS = 3

# =========================
# CONTROL
# =========================

sent_signals = set()

# =========================
# BOT
# =========================

bot = Bot(token=BOT_TOKEN)

# =========================
# HELPERS
# =========================

def format_time(utc_str):
    dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
    return dt.astimezone(TZ).strftime("%I:%M %p")

def valid_odd(o):
    return MIN_ODD <= o <= MAX_ODD

def combo_price(picks):
    price = 1
    for p in picks:
        price *= p["odds"]
    return price

def valid_combo(price):
    return MIN_COMBO <= price <= MAX_COMBO

def dedupe_key(game, picks):
    labels = "|".join(sorted([p["label"] for p in picks]))
    return f"{game}:{labels}"

# =========================
# FETCH ODDS
# =========================

async def get_odds():
    url = "https://api.the-odds-api.com/v4/sports/upcoming/odds/"
    
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "uk",
        "markets": "h2h,totals,btts",
        "oddsFormat": "decimal"
    }

    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params)
        return r.json()

# =========================
# ANALISIS PICKS
# =========================

def analyze_game(game):
    picks = []

    try:
        book = game["bookmakers"][0]
        markets = book["markets"]

        for m in markets:

            # =========================
            # GOALS
            # =========================
            if m["key"] == "totals":
                for o in m["outcomes"]:
                    if "Over" in o["name"] and o["point"] == 2.5:
                        if valid_odd(o["price"]):
                            picks.append({
                                "label": "Over 2.5 goles",
                                "odds": o["price"],
                                "reason": "Ambos equipos con tendencia ofensiva"
                            })

                    if "Under" in o["name"] and o["point"] == 3.5:
                        if valid_odd(o["price"]):
                            picks.append({
                                "label": "Under 3.5 goles",
                                "odds": o["price"],
                                "reason": "Partido con tendencia cerrada"
                            })

            # =========================
            # BTTS
            # =========================
            if m["key"] == "btts":
                for o in m["outcomes"]:
                    if o["name"] == "Yes" and valid_odd(o["price"]):
                        picks.append({
                            "label": "Ambos anotan",
                            "odds": o["price"],
                            "reason": "Ambos equipos suelen marcar"
                        })

                    if o["name"] == "No" and valid_odd(o["price"]):
                        picks.append({
                            "label": "Ambos NO anotan",
                            "odds": o["price"],
                            "reason": "Uno de los equipos tiene baja ofensiva"
                        })

            # =========================
            # WINNER SAFE
            # =========================
            if m["key"] == "h2h":
                for o in m["outcomes"]:
                    if valid_odd(o["price"]) and o["price"] < 2.0:
                        picks.append({
                            "label": f"{o['name']} gana",
                            "odds": o["price"],
                            "reason": "Equipo con ligera ventaja"
                        })

    except:
        pass

    return picks

# =========================
# ARMAR COMBINADAS
# =========================

def build_combo(picks):
    combos = []

    for i in range(len(picks)):
        for j in range(i+1, len(picks)):
            combo = [picks[i], picks[j]]
            price = combo_price(combo)

            if valid_combo(price):
                combos.append((combo, price))

    for i in range(len(picks)):
        for j in range(i+1, len(picks)):
            for k in range(j+1, len(picks)):
                combo = [picks[i], picks[j], picks[k]]
                price = combo_price(combo)

                if valid_combo(price):
                    combos.append((combo, price))

    return combos

# =========================
# MENSAJE
# =========================

def format_message(game, combo, price):
    home = game["home_team"]
    away = game["away_team"]
    time = format_time(game["commence_time"])

    msg = "📊 SEÑAL PREPARTIDO\n\n"
    msg += f"🏟 {home} vs {away}\n"
    msg += f"🕒 {time}\n\n"

    msg += "✅ Picks:\n"
    for p in combo:
        msg += f"- {p['label']} @ {p['odds']}\n"

    msg += f"\n🎯 Cuota: {round(price,2)}\n\n"

    msg += "🧠 Motivo:\n"
    for p in combo:
        msg += f"• {p['reason']}\n"

    msg += "\n💰 Stake: 0.5u - 1u"

    return msg

# =========================
# MAIN LOOP
# =========================

async def run_bot():
    while True:
        try:
            games = await get_odds()

            for game in games:

                start = datetime.fromisoformat(game["commence_time"].replace("Z","+00:00"))
                now = datetime.utcnow()

                if (start - now).total_seconds() > PREMATCH_WINDOW_HOURS * 3600:
                    continue

                picks = analyze_game(game)

                if len(picks) < 2:
                    continue

                combos = build_combo(picks)

                for combo, price in combos:
                    key = dedupe_key(game["id"], combo)

                    if key in sent_signals:
                        continue

                    sent_signals.add(key)

                    msg = format_message(game, combo, price)
                    await bot.send_message(chat_id=CHAT_ID, text=msg)

                    break  # solo una señal por partido

        except Exception as e:
            print("ERROR:", e)

        await asyncio.sleep(SCAN_INTERVAL)

# =========================
# START
# =========================

if __name__ == "__main__":
    asyncio.run(run_bot())
