import asyncio
import logging
import os
from datetime import datetime
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot

# ==============================
# CONFIG
# ==============================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

LOCAL_TZ = ZoneInfo("America/Mexico_City")

MARKETS = "h2h,totals,btts"

ACTIVE_START = 7
ACTIVE_END = 22

MIN_EDGE = 0.05

bot = Bot(token=BOT_TOKEN)

sent = set()

# ==============================
# UTIL
# ==============================

def now():
    return datetime.now(LOCAL_TZ)

def safe_float(x):
    try: return float(x)
    except: return None

def prob(o):
    return 1/o if o else None

def odds(p):
    return 1/p if p else None

# ==============================
# ANALISIS PRO
# ==============================

def analyze_match(match):

    home = match["home_team"]
    away = match["away_team"]

    probs = []
    best = []

    for bm in match.get("bookmakers", []):
        for m in bm.get("markets", []):

            # H2H
            if m["key"] == "h2h":
                for o in m["outcomes"]:
                    price = safe_float(o["price"])
                    if not price: continue

                    p = prob(price)
                    if not p: continue

                    probs.append((o["name"], p, price, "h2h"))

            # TOTALS
            if m["key"] == "totals":
                for o in m["outcomes"]:
                    pt = safe_float(o.get("point"))
                    price = safe_float(o["price"])
                    name = o["name"].lower()

                    if not price or pt is None:
                        continue

                    if (pt, name) in [(1.5,"over"), (2.5,"over"), (3.5,"under")]:
                        p = prob(price)
                        probs.append((f"{name} {pt}", p, price, "totals"))

            # BTTS
            if m["key"] == "btts":
                for o in m["outcomes"]:
                    if o["name"].lower() in ["yes","si","sí"]:
                        price = safe_float(o["price"])
                        if price:
                            p = prob(price)
                            probs.append(("btts yes", p, price, "btts"))

    if not probs:
        return None

    # Promedio prob
    avg_prob = mean([p[1] for p in probs])
    fair_odds = odds(avg_prob)

    candidates = []

    for name, p, price, market in probs:

        edge = (price / fair_odds) - 1

        if edge < MIN_EDGE:
            continue

        candidates.append({
            "pick": name,
            "odds": round(price,2),
            "edge": edge,
            "market": market
        })

    if not candidates:
        return None

    best = sorted(candidates, key=lambda x: x["edge"], reverse=True)[0]

    # Stake automático
    stake = 1
    if best["edge"] > 0.10: stake = 2
    if best["edge"] > 0.18: stake = 3

    best["stake"] = stake

    # Texto bonito
    if best["market"] == "h2h":
        if best["pick"].lower() == home.lower():
            best["pick"] = f"Gana {home}"
        elif best["pick"].lower() == away.lower():
            best["pick"] = f"Gana {away}"
        else:
            best["pick"] = "Empate"

    if best["market"] == "totals":
        if "over" in best["pick"]:
            best["pick"] = f"Over {best['pick'].split()[1]} goles"
        else:
            best["pick"] = f"Under {best['pick'].split()[1]} goles"

    if best["market"] == "btts":
        best["pick"] = "Ambos anotan: Sí"

    return best

# ==============================
# API
# ==============================

async def get_odds():
    all = []
    async with httpx.AsyncClient() as client:
        for sport in ["soccer_mexico_ligamx","soccer_epl","soccer_uefa_champs_league"]:
            r = await client.get(
                f"https://api.the-odds-api.com/v4/sports/{sport}/odds",
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions":"uk",
                    "markets":MARKETS
                }
            )
            all.extend(r.json())
    return all

# ==============================
# PARLAY PRO
# ==============================

def build_parlay(picks):

    picks = sorted(picks, key=lambda x: x["edge"], reverse=True)

    selected = []
    total_odds = 1

    for p in picks:
        if len(selected) >= 3:
            break

        if total_odds * p["odds"] <= 4.5:
            selected.append(p)
            total_odds *= p["odds"]

    if len(selected) < 2:
        return None

    return selected, round(total_odds,2)

# ==============================
# LOOP
# ==============================

async def run():

    if now().hour < ACTIVE_START or now().hour >= ACTIVE_END:
        return True

    odds = await get_odds()

    picks = []

    for m in odds:

        a = analyze_match(m)
        if not a:
            continue

        key = f"{m['home_team']}|{a['pick']}"
        if key in sent:
            continue

        msg = (
            f"📊 BETTING SIGNAL\n\n"
            f"{m['home_team']} vs {m['away_team']}\n"
            f"🎯 {a['pick']}\n"
            f"💰 Cuota: {a['odds']}\n"
            f"🔥 Edge: {round(a['edge']*100,1)}%\n"
            f"📈 Stake: {a['stake']}/10"
        )

        await bot.send_message(chat_id=CHAT_ID, text=msg)

        sent.add(key)
        picks.append(a)

    # PARLAY
    parlay = build_parlay(picks)
    if parlay:
        legs, total = parlay

        txt = "🔥 PARLAY PRO\n\n"

        for i,l in enumerate(legs,1):
            txt += f"{i}. {l['pick']} @ {l['odds']}\n"

        txt += f"\n💰 Total: {total}"

        await bot.send_message(chat_id=CHAT_ID, text=txt)

    return False

# ==============================
# MAIN
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
