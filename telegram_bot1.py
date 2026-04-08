import asyncio
import logging
from datetime import datetime, timezone, timedelta
from statistics import mean

import httpx
from telegram import Bot
from telegram.error import TelegramError
from zoneinfo import ZoneInfo

# ================= CONFIG =================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
FOOTBALL_API_KEY = "c455630d0023ef208f93dd0567164905"
CHAT_ID = "1983622390"

LOCAL_TZ = ZoneInfo("America/Mexico_City")

PREMATCH_MIN_MINUTES = 15
PREMATCH_MAX_MINUTES = 120
MIN_VALUE_EDGE = 0.06

LIVE_MIN_MINUTE = 8
LIVE_GOAL_MIN_SCORE = 7
LIVE_CORNERS_MIN_SCORE = 6
LIVE_CARDS_MIN_SCORE = 6

ENABLE_V4_LIVE_ODDS = True
LIVE_ODDS_MIN_EDGE = 0.08

MAX_STATS_REQUESTS_PER_CYCLE = 5
MAX_LIVE_ALERTS_PER_CYCLE = 5

WHITELISTED_SPORTS = {
    "soccer_epl": "Premier League",
    "soccer_uefa_champs_league": "Champions League",
    "soccer_mexico_ligamx": "Liga MX",
}

ALLOWED_LEAGUE_IDS = {39, 2, 262}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

sent_live_signals = set()
sent_pre_match_signals = set()

# ================= HELPERS =================

def stat_num(x):
    try:
        return int(float(str(x).replace("%","")))
    except:
        return 0

def build_live_key(fixture_id, minute, t):
    return f"{fixture_id}_{minute//5}_{t}"

def implied_prob(o): return 1/o if o>1 else 0

def should_send_prematch(dt):
    mins = int((dt - datetime.now(timezone.utc)).total_seconds()/60)
    return PREMATCH_MIN_MINUTES <= mins <= PREMATCH_MAX_MINUTES

# ================= LIVE STATS =================

async def fetch_match_stats(client, fixture_id):
    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures/statistics",
            params={"fixture": fixture_id},
            headers={"x-apisports-key": FOOTBALL_API_KEY}
        )
        data = r.json()["response"]
        if len(data)<2: return None

        def parse(d):
            return {x["type"]: x["value"] for x in d["statistics"]}

        return parse(data[0]), parse(data[1])
    except:
        return None

def normalize_stats(s):
    return {
        "shots": stat_num(s.get("Shots on Goal")),
        "attacks": stat_num(s.get("Dangerous Attacks")),
        "corners": stat_num(s.get("Corner Kicks")),
        "fouls": stat_num(s.get("Fouls")),
        "yellow": stat_num(s.get("Yellow Cards")),
    }

# ================= SCORING =================

def score_goal(minute, s, g, oppg):
    score=0
    if minute>=8: score+=1
    if s["shots"]>=2: score+=2
    if s["attacks"]>=20: score+=2
    if s["corners"]>=3: score+=1
    if g<=oppg: score+=1
    return score

def score_corners(minute, h, a):
    score=0
    if minute>=15: score+=1
    if h["corners"]+a["corners"]>=5: score+=2
    if h["attacks"]+a["attacks"]>=25: score+=2
    return score

def score_cards(minute, h, a):
    score=0
    if minute>=20: score+=1
    if h["fouls"]+a["fouls"]>=12: score+=2
    if h["yellow"]+a["yellow"]>=2: score+=2
    return score

def tier(score):
    if score>=9: return "VIP"
    if score>=7: return "PRO"
    if score>=6: return "BUENA"
    return "NORMAL"

# ================= LIVE ODDS =================

async def fetch_live_odds(client, fixture_id):
    try:
        r = await client.get(
            "https://v3.football.api-sports.io/odds/live",
            params={"fixture": fixture_id},
            headers={"x-apisports-key": FOOTBALL_API_KEY}
        )
        data = r.json()["response"]
        if not data: return None

        vals=[]
        for bm in data[0]["bookmakers"]:
            for bet in bm["bets"]:
                for v in bet["values"]:
                    vals.append((bet["name"],v["value"],float(v["odd"])))
        return vals
    except:
        return None

def best_odd(vals, labels):
    best=None
    for name,label,odd in vals:
        if label.lower() in [l.lower() for l in labels]:
            if not best or odd>best[2]:
                best=(name,label,odd)
    return best

def fair_from_score(score):
    return {6:2.2,7:2.0,8:1.8,9:1.7,10:1.6}.get(score,2.5)

def edge(live,fair):
    return (live/fair)-1

# ================= FORMAT =================

def format_live(a):
    return f"""🔥 ALERTA {a['tier']}

⚽ {a['home']} vs {a['away']}
📊 {a['score']} | {a['minute']}'
🎯 {a['signal']}
⭐ Score: {a['score_val']}/10
💰 Cuota: {a.get('odd','-')}
🔥 Edge: {a.get('edge','-')}
📌 {a['reason']}
"""

# ================= MAIN LIVE =================

async def fetch_live_alerts(client):
    alerts=[]
    r=await client.get(
        "https://v3.football.api-sports.io/fixtures",
        params={"live":"all"},
        headers={"x-apisports-key":FOOTBALL_API_KEY}
    )

    data=r.json()["response"]
    count=0

    for m in data:
        if count>=MAX_STATS_REQUESTS_PER_CYCLE: break

        fid=m["fixture"]["id"]
        minute=m["fixture"]["status"]["elapsed"]
        if minute<LIVE_MIN_MINUTE: continue

        home=m["teams"]["home"]["name"]
        away=m["teams"]["away"]["name"]
        hg=m["goals"]["home"]
        ag=m["goals"]["away"]

        stats=await fetch_match_stats(client,fid)
        if not stats: continue
        count+=1

        hs,as_ = map(normalize_stats,stats)

        # GOAL
        sc=score_goal(minute,hs,hg,ag)
        if sc>=LIVE_GOAL_MIN_SCORE:
            fair=fair_from_score(sc)
            odd=None;edgev=None

            if ENABLE_V4_LIVE_ODDS:
                odds=await fetch_live_odds(client,fid)
                if odds:
                    o=best_odd(odds,[home])
                    if o:
                        odd=o[2]
                        edgev=round(edge(odd,fair)*100,2)
                        if edgev<LIVE_ODDS_MIN_EDGE*100: continue

            alerts.append({
                "signal_key":build_live_key(fid,minute,"g"),
                "home":home,"away":away,
                "score":f"{hg}-{ag}",
                "minute":minute,
                "signal":f"Gol probable {home}",
                "score_val":sc,
                "tier":tier(sc),
                "odd":odd,
                "edge":edgev,
                "reason":"Presión ofensiva alta"
            })

    return alerts[:MAX_LIVE_ALERTS_PER_CYCLE]

# ================= LOOP =================

async def main():
    bot=Bot(token=BOT_TOKEN)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                alerts=await fetch_live_alerts(client)

                for a in alerts:
                    if a["signal_key"] in sent_live_signals:
                        continue

                    await bot.send_message(CHAT_ID,format_live(a))
                    sent_live_signals.add(a["signal_key"])

                await asyncio.sleep(60)

            except Exception as e:
                logger.error(e)
                await asyncio.sleep(60)

if __name__=="__main__":
    asyncio.run(main())
