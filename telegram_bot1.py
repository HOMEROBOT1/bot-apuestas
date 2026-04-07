"""
Football Betting Alerts Bot
----------------------------
Data sources:
  - The Odds API  (https://the-odds-api.com)  → pre-match odds
  - API-Football  (https://api-sports.io)      → live match scores / events
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from statistics import mean
import re

import httpx
from telegram import Bot
from telegram.error import TelegramError
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Runtime dedupe
# ---------------------------------------------------------------------------

sent_live_signals = set()
sent_parley_signals = set()
sent_upcoming_match_alerts = set()
odds_credits_alert_sent = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
FOOTBALL_API_KEY = "c455630d0023ef208f93dd0567164905"
CHAT_ID = "1983622390"

PRE_MATCH_WINDOW_HOURS = 24

print("DEBUG BOT_TOKEN TYPE:", type(BOT_TOKEN), BOT_TOKEN)
print("DEBUG ODDS_API_KEY TYPE:", type(ODDS_API_KEY), ODDS_API_KEY)
print("DEBUG FOOTBALL_API_KEY TYPE:", type(FOOTBALL_API_KEY), FOOTBALL_API_KEY)
print("DEBUG CHAT_ID TYPE:", type(CHAT_ID), CHAT_ID)
# How often to run a full cycle (seconds)
CYCLE_INTERVAL = 900   # 15 minutes

# How far ahead to look for upcoming matches (hours)
PRE_MATCH_WINDOW_HOURS = 6

# Maximum alerts to send per cycle
MAX_ALERTS_PER_CYCLE = 5

# Minimum value edge to flag a bet (e.g. 0.06 = 6 % edge over fair odds)
MIN_VALUE_EDGE = 0.06

# ---------------------------------------------------------------------------
# Whitelisted top leagues (Odds API sport keys)
# ---------------------------------------------------------------------------

WHITELISTED_SPORTS = {
    "soccer_epl": "🇬🇧 Premier League",
    "soccer_uefa_champs_league": "🏆 Champions League",
    "soccer_mexico_ligamx": "🇲🇽 Liga MX",
} 
ODDS_MARKETS = "h2h"
ODDS_REGIONS = "uk"

# API-Football competition IDs for live checks (must mirror WHITELISTED_SPORTS)
LIVE_LEAGUE_IDS = {
    39,   # Premier League
    2,    # Champions League
    262,  # Liga MX
}

# Delay between consecutive Odds API requests (seconds) to avoid rate limits
ODDS_API_REQUEST_DELAY = 60

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

MIN_SCORE = 7


def score_badge(score: int) -> str:
    if score >= 9:
        return "🟢"
    if score >= 7:
        return "🟡"
    return "🔴"


def score_pre_match(edge: float, book_count: int) -> int:
    pct = edge * 100
    if pct >= 18:
        base = 9
    elif pct >= 15:
        base = 8
    elif pct >= 12:
        base = 7
    elif pct >= 10:
        base = 6
    elif pct >= 8:
        base = 5
    else:
        base = 4

    if book_count >= 8:
        mod = 2
    elif book_count >= 6:
        mod = 1
    elif book_count < 4:
        mod = -1
    else:
        mod = 0

    return max(1, min(10, base + mod))


def score_live(priority: int, minute: int) -> int:
    if priority == 3:
        if minute >= 85:
            return 9
        if minute >= 75:
            return 8
        return 7

    if priority == 2:
        if minute >= 85:
            return 8
        if minute >= 75:
            return 7
        return 6

    if minute >= 80:
        return 7
    if minute >= 75:
        return 6
    return 4


def passes_score(alert: dict) -> bool:
    return alert["score"] >= MIN_SCORE


def suggested_stake(score: int) -> str:
    if score >= 9:
        return "3% of bankroll"
    if score >= 8:
        return "2% of bankroll"
    return "1% of bankroll"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def odds_to_prob(odds: float) -> float:
    return 1.0 / odds if odds > 0 else 0.0


def overround(probs: list[float]) -> float:
    return sum(probs) - 1.0


def best_value_outcome(bookmakers: list[dict]) -> dict | None:
    prices: dict[str, list[float]] = {"Home": [], "Draw": [], "Away": []}

    for bm in bookmakers:
        for market in bm.get("markets", []):
            if market["key"] != "h2h":
                continue
            for outcome in market["outcomes"]:
                name = outcome["name"]
                price = outcome["price"]
                if name in prices:
                    prices[name].append(price)

    if not all(prices.values()):
        return None

    fair_probs: dict[str, float] = {}
    for outcome, odds_list in prices.items():
        raw_probs = [odds_to_prob(o) for o in odds_list]
        total_overround = overround(
            [odds_to_prob(o) for o in [mean(prices[k]) for k in prices]]
        )
        share = mean(raw_probs) / (1.0 + total_overround)
        fair_probs[outcome] = min(share, 0.99)

    best_outcome = None
    best_edge = MIN_VALUE_EDGE

    for outcome, odds_list in prices.items():
        best_odds = max(odds_list)
        implied = odds_to_prob(best_odds)
        fair = fair_probs[outcome]
        edge = fair - implied
        if edge > best_edge:
            best_edge = edge
            best_outcome = {
                "label": outcome,
                "best_odds": best_odds,
                "fair_prob": fair,
                "edge": edge,
                "book_count": len(odds_list),
            }

    return best_outcome


def format_kickoff(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    local = dt.astimezone()
    return local.strftime("%H:%M")


def traducir_signal(reason: str) -> str:
    if "comeback / Draw" in reason:
        return "Apostar a que el equipo local empata o remonta"
    elif "Over" in reason:
        return "Se esperan más goles en el partido"
    elif "Next goal" in reason:
        return "Próximo gol del equipo indicado"
    elif "lead by 1" in reason:
        return "El equipo visitante va ganando por 1 gol"
    else:
        return reason


def normalize_reason(reason: str) -> str:
    reason = reason.strip().lower()
    reason = re.sub(r"at\s+\d{1,3}'?", "", reason)
    reason = re.sub(r"\s+", " ", reason).strip()
    return reason


def make_signal_key(alert: dict) -> str:
    home = alert["home"].strip().lower()
    away = alert["away"].strip().lower()
    league = alert["league"].strip().lower()
    reason = normalize_reason(alert["reason"])
    return f"{league}|{home}|{away}|{reason}"


def make_parley_key(alert: dict) -> str:
    league = alert["league"].strip().lower()
    home = alert["home"].strip().lower()
    away = alert["away"].strip().lower()
    legs = "|".join(leg["label"].strip().lower() for leg in alert["legs"])
    return f"{league}|{home}|{away}|{legs}"


# ---------------------------------------------------------------------------
# Parley helpers
# ---------------------------------------------------------------------------

def extract_parley_legs(bookmaker: dict) -> list[dict]:
    legs: list[dict] = []

    for market in bookmaker.get("markets", []):
        key = market.get("key")
        outcomes = market.get("outcomes", [])

        if key == "h2h":
            valid = []
            for o in outcomes:
                price = o.get("price")
                name = o.get("name")
                if (
                    name
                    and isinstance(price, (int, float))
                    and 1.45 <= price <= 1.95
                ):
                    valid.append({
                        "type": "winner",
                        "label": f"Gana {name}",
                        "odds": price,
                    })

            if valid:
                valid.sort(key=lambda x: x["odds"])
                legs.append(valid[0])

        elif key == "totals":
            valid = []
            for o in outcomes:
                name = str(o.get("name", "")).lower()
                point = o.get("point")
                price = o.get("price")

                if (
                    name == "over"
                    and point in (1.5, 2.5)
                    and isinstance(price, (int, float))
                    and 1.55 <= price <= 2.10
                ):
                    valid.append({
                        "type": "goals",
                        "label": f"Más de {point} goles",
                        "odds": price,
                        "point": point,
                    })

            if valid:
                valid.sort(key=lambda x: x["point"])
                legs.append(valid[0])

        elif key == "btts":
            for o in outcomes:
                name = str(o.get("name", "")).lower()
                price = o.get("price")

                if (
                    name in ("yes", "si", "sí")
                    and isinstance(price, (int, float))
                    and 1.60 <= price <= 2.05
                ):
                    legs.append({
                        "type": "btts",
                        "label": "Ambos anotan",
                        "odds": price,
                    })
                    break

    return legs


def build_parley_alert(event: dict, league_name: str) -> dict | None:
    home = event.get("home_team", "Local")
    away = event.get("away_team", "Visitante")
    kickoff_iso = event.get("commence_time", "")
    bookmakers = event.get("bookmakers", [])

    if not bookmakers:
        return None

    bookmaker = bookmakers[0]
    extracted = extract_parley_legs(bookmaker)

    winner_leg = next((x for x in extracted if x["type"] == "winner"), None)
    goals_leg = next((x for x in extracted if x["type"] == "goals"), None)
    btts_leg = next((x for x in extracted if x["type"] == "btts"), None)

    selected_legs = None

    if winner_leg and goals_leg:
        selected_legs = [winner_leg, goals_leg]
    elif btts_leg and goals_leg:
        selected_legs = [btts_leg, goals_leg]

    if not selected_legs:
        return None

    combined_odds = 1.0
    for leg in selected_legs:
        combined_odds *= leg["odds"]

    combined_odds = round(combined_odds, 2)

    if not (2.00 <= combined_odds <= 4.50):
        return None

    return {
        "type": "parley",
        "score": 8,
        "league": league_name,
        "home": home,
        "away": away,
        "match": f"{home} vs {away}",
        "kickoff": format_kickoff(kickoff_iso) if kickoff_iso else "",
        "legs": selected_legs,
        "combined_odds": combined_odds,
        "book_count": len(bookmakers),
        "event_id": event.get("id", f"{home}_{away}_{kickoff_iso}"),
    }


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------
async def fetch_pre_match_alerts(bot: Bot, client: httpx.AsyncClient) -> list[dict]:
    alerts: list[dict] = []
    global odds_credits_alert_sent

    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=PRE_MATCH_WINDOW_HOURS)

    for sport_key, league_name in WHITELISTED_SPORTS.items():
        try:
            r = await client.get(
                f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/",
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": ODDS_REGIONS,
                    "markets": ODDS_MARKETS,
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                },
                timeout=10,
            )

            if r.status_code != 200:
                logger.warning("Odds API %s -> %s | response: %s", sport_key, r.status_code, r.text)

                if "OUT_OF_USAGE_CREDITS" in r.text and not odds_credits_alert_sent:
                    try:
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text="⚠️ Aviso: te quedaste sin créditos en The Odds API."
                        )
                        odds_credits_alert_sent = True
                    except Exception as exc:
                        logger.warning("Error enviando alerta de créditos: %s", exc)

                await asyncio.sleep(ODDS_API_REQUEST_DELAY)
                continue

            odds_credits_alert_sent = False
            games = r.json()

            if not games:
                logger.info("Sin partidos en %s", sport_key)
                await asyncio.sleep(ODDS_API_REQUEST_DELAY)
                continue

            for game in games:
                commence = datetime.fromisoformat(
                    game["commence_time"].replace("Z", "+00:00")
                )

                if not (-7200 <= (commence - now).total_seconds() <= PRE_MATCH_WINDOW_HOURS * 3600):
                    continue

                home_team = game.get("home_team")
                away_team = next(
                    (t for t in game.get("teams", []) if t != home_team),
                    "Away"
                )

                bookmakers = game.get("bookmakers", [])
                if not bookmakers:
                    continue

                outcomes_by_team: dict[str, list[float]] = {}

                for bookmaker in bookmakers:
                    for market in bookmaker.get("markets", []):
                        if market.get("key") != "h2h":
                            continue
                        for outcome in market.get("outcomes", []):
                            name = outcome.get("name")
                            price = outcome.get("price")
                            if name and isinstance(price, (int, float)):
                                outcomes_by_team.setdefault(name, []).append(float(price))

                if not outcomes_by_team:
                    continue

                for team_name, prices in outcomes_by_team.items():
                    if len(prices) < 2:
                        continue

                    best_odds = max(prices)
                    avg_odds = mean(prices)

                    if avg_odds <= 1:
                        continue

                    fair_prob = 1 / avg_odds
                    implied_prob_best = 1 / best_odds
                    edge = fair_prob - implied_prob_best

                    if edge >= MIN_VALUE_EDGE:
                        match_key = f"{sport_key}|{home_team}|{away_team}|{team_name}"
                        alerts.append({
                            "match_key": match_key,
                            "league": league_name,
                            "home_team": home_team,
                            "away_team": away_team,
                            "pick": team_name,
                            "best_odds": round(best_odds, 2),
                            "avg_odds": round(avg_odds, 2),
                            "edge": round(edge * 100, 2),
                            "commence_time": commence,
                        })

            await asyncio.sleep(ODDS_API_REQUEST_DELAY)

        except Exception as exc:
            logger.warning("Error consultando %s: %s", sport_key, exc)
            await asyncio.sleep(ODDS_API_REQUEST_DELAY)
            continue

    return alerts



# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

DIV = "━━━━━━━━━━━━━━━━"


def format_pre_match(alert: dict) -> str:
    edge_pct = round(alert["edge"] * 100, 1)
    fair_pct = round(alert["fair_prob"] * 100, 1)
    mins = alert["minutes_to_ko"]
    time_str = f"{mins}m" if mins < 60 else f"{mins // 60}h {mins % 60}m"
    s = alert["score"]

    header = "💎 *VIP SIGNAL*" if s >= 8 else "📊 *BETTING SIGNAL*"

    return "\n".join([
        header,
        f"{DIV}",
        f"{alert['league']}",
        f"🕐  Pre-Match  ·  In {time_str}  ·  {alert['kickoff']}",
        "",
        f"*{alert['home']}*  vs  *{alert['away']}*",
        f"{DIV}",
        f"✅  *PICK:  {alert['outcome'].upper()}*",
        f"💰  Odds:   `{alert['odds']}`",
        f"📈  Edge:   *+{edge_pct}%*   ·   Fair:  {fair_pct}%",
        f"📚  Books:  {alert['book_count']} bookmakers",
        f"{DIV}",
        f"🎯  Confidence:  *{s} / 10*",
        f"💵  Stake:       *{suggested_stake(s)}*",
        f"{DIV}",
    ])


def format_parley(alert: dict) -> str:
    s = alert["score"]
    header = "💎 *VIP PARLEY*" if s >= 8 else "🔥 *PARLEY SUGERIDO*"

    legs_text = "\n".join(
        [f"• {leg['label']} @ `{leg['odds']}`" for leg in alert["legs"]]
    )

    return "\n".join([
        header,
        f"{DIV}",
        f"{alert['league']}",
        f"🕐  Pre-Match  ·  {alert['kickoff']}" if alert.get("kickoff") else "🕐  Pre-Match",
        "",
        f"*{alert['home']}*  vs  *{alert['away']}*",
        f"{DIV}",
        "🎯 *SELECCIONES:*",
        legs_text,
        f"{DIV}",
        f"💰 *Cuota total:* `{alert['combined_odds']}`",
        f"📚 Books: {alert.get('book_count', 0)} bookmakers",
        f"🎯 Confianza: *{s} / 10*",
        f"💵 Stake: *{suggested_stake(s)}*",
        f"{DIV}",
    ])


def format_live(alert: dict) -> str:
    s = alert["score"]
    home, away = alert["home"], alert["away"]
    sh, sa = alert["score_str"].split("-")

    header = "💎 *SEÑAL VIP EN VIVO*" if s >= 8 else "🔴 *SEÑAL EN VIVO*"

    return "\n".join([
        header,
        f"{DIV}",
        f"{alert['league']}",
        f"⏱  *{alert['minute']}'*  ·  In Play",
        "",
        f"*{home}*   {sh} — {sa}   *{away}*",
        f"{DIV}",
        f"✅ *SEÑAL:* {traducir_signal(alert['reason'])}",
        f"{DIV}",
        f"🎯 Confianza: *{s} / 10*",
        f"💵 Apuesta: *{suggested_stake(s)}*",
        f"{DIV}",
    ])


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

async def send_message(bot: Bot, text: str) -> None:
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode="Markdown",
        )
        logger.info("Sent alert (%d chars)", len(text))
    except TelegramError as exc:
        logger.error("Telegram error: %s", exc)

def format_upcoming_match_alert(match: dict) -> str:
    local_time = match["kickoff"].astimezone()
    hour_text = local_time.strftime("%I:%M %p").lstrip("0")

    return (
        f"📅 PRÓXIMO PARTIDO\n\n"
        f"{match['home_team']} vs {match['away_team']}\n"
        f"Liga: {match['league']}\n"
        f"Hora: {hour_text}"
    )
  
async def fetch_upcoming_matches(client: httpx.AsyncClient) -> list[dict]:
    matches: list[dict] = []

    try:
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%Y-%m-%d")

        r = await client.get(
            "https://v3.football.api-sports.io/fixtures",
            params={"date": today_str},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=10,
        )

        if r.status_code != 200:
            logger.warning("API-Football fixtures -> %s | %s", r.status_code, r.text)
            return matches

        data = r.json().get("response", [])

        allowed_league_ids = set(LIVE_LEAGUE_IDS)

        for item in data:
            league = item.get("league", {})
            fixture = item.get("fixture", {})
            teams = item.get("teams", {})

            league_id = league.get("id")
            if league_id not in allowed_league_ids:
                continue

            kickoff_str = fixture.get("date")
            if not kickoff_str:
                continue

            kickoff = datetime.fromisoformat(kickoff_str.replace("Z", "+00:00"))
            local_hour = kickoff.astimezone().hour

            # Solo partidos entre 7 AM y 10 PM hora local
            if not (7 <= local_hour < 22):
                continue

            home_team = teams.get("home", {}).get("name", "Home")
            away_team = teams.get("away", {}).get("name", "Away")
            league_name = league.get("name", "Liga")

            match_key = f"{league_id}|{home_team}|{away_team}|{kickoff.isoformat()}"

            matches.append({
                "match_key": match_key,
                "league": league_name,
                "home_team": home_team,
                "away_team": away_team,
                "kickoff": kickoff,
            })

    except Exception as exc:
        logger.warning("Error obteniendo próximos partidos: %s", exc)

    return matches

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_cycle(bot: Bot, client: httpx.AsyncClient) -> None:
    logger.info("── Starting new cycle ──────────────────────────────────")

    pre_task = asyncio.create_task(fetch_pre_match_alerts(client))
    live_task = asyncio.create_task(fetch_live_alerts(client))

    pre_alerts, live_alerts = await asyncio.gather(pre_task, live_task)

    all_alerts = live_alerts + pre_alerts

    qualified = [a for a in all_alerts if passes_score(a)]
    dropped = len(all_alerts) - len(qualified)
    if dropped:
        logger.info("Dropped %d alert(s) below score threshold (%d).", dropped, MIN_SCORE)

    qualified.sort(key=lambda a: -a["score"])
    selected = qualified[:MAX_ALERTS_PER_CYCLE]

    if not selected:
        logger.info("No qualifying alerts this cycle.")
        return

    logger.info(
        "Sending %d alert(s) — scores: %s",
        len(selected),
        [f"{a['type']}={a['score']}/10" for a in selected],
    )

    for alert in selected:
        if alert["type"] == "pre_match":
            text = format_pre_match(alert)
            await send_message(bot, text)

        elif alert["type"] == "parley":
            parley_key = make_parley_key(alert)
            if parley_key in sent_parley_signals:
                logger.info("🚫 Parley duplicado omitido: %s", parley_key)
                continue

            text = format_parley(alert)
            await send_message(bot, text)
            sent_parley_signals.add(parley_key)

        else:
            signal_key = make_signal_key(alert)
            if signal_key in sent_live_signals:
                logger.info("🚫 Señal duplicada omitida: %s", signal_key)
                continue

            text = format_live(alert)
            await send_message(bot, text)
            sent_live_signals.add(signal_key)

        await asyncio.sleep(1)
      
async def fetch_live_alerts(client: httpx.AsyncClient) -> list[dict]:
    alerts = []

    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures",
            params={"live": "all"},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=10,
        )

        if r.status_code != 200:
            logger.warning("API-Football live -> %s | %s", r.status_code, r.text)
            return alerts

        data = r.json().get("response", [])

        for match in data:
            fixture = match.get("fixture", {})
            teams = match.get("teams", {})
            goals = match.get("goals", {})

            home = teams.get("home", {}).get("name")
            away = teams.get("away", {}).get("name")

            home_goals = goals.get("home", 0)
            away_goals = goals.get("away", 0)

            minute = fixture.get("status", {}).get("elapsed", 0)

            # Ejemplo simple de señal: empate en minuto 70+
            if minute and minute >= 70 and home_goals == away_goals:
                signal_key = f"{home}-{away}-{minute}"

                alerts.append({
                    "signal_key": signal_key,
                    "home": home,
                    "away": away,
                    "minute": minute,
                    "score": f"{home_goals}-{away_goals}",
                })

    except Exception as exc:
        logger.warning("Error en live fetch: %s", exc)

    return alerts
def format_live_alert(alert: dict) -> str:
    return (
        f"🔥 ALERTA EN VIVO\n\n"
        f"{alert['home']} vs {alert['away']}\n"
        f"Marcador: {alert['score']}\n"
        f"Minuto: {alert['minute']}\n\n"
        f"📊 Posible oportunidad en vivo"
    )

async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN secret not set.")

    if not CHAT_ID:
        raise RuntimeError("CHAT_ID secret not set.")

    bot = Bot(token=BOT_TOKEN)
    info = await bot.get_me()
    logger.info("Bot online: @%s", info.username)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                now_local = datetime.now(ZoneInfo("America/Mexico_City"))
                hour = now_local.hour

                if hour < 7 or hour >= 22:
                    next_start = now_local.replace(hour=7, minute=0, second=0, microsecond=0)

                    if hour >= 22:
                        next_start = next_start + timedelta(days=1)

                    sleep_seconds = int((next_start - now_local).total_seconds())
                    logger.info("Fuera de horario. Durmiendo hasta 7:00 AM (%ds).", sleep_seconds)
                    await asyncio.sleep(sleep_seconds)
                    continue

                upcoming_matches = await fetch_upcoming_matches(client)

                for match in upcoming_matches:
                    if match["match_key"] not in sent_upcoming_match_alerts:
                        text = format_upcoming_match_alert(match)
                        await bot.send_message(chat_id=CHAT_ID, text=text)
                        sent_upcoming_match_alerts.add(match["match_key"])

                alerts = await fetch_pre_match_alerts(bot, client)

                if alerts:
                    for alert in alerts:
                        if alert["match_key"] not in sent_parley_signals:
                            text = format_pre_match_alert(alert)
                            await bot.send_message(chat_id=CHAT_ID, text=text)
                            sent_parley_signals.add(alert["match_key"])

                    sleep_seconds = 60
                    logger.info("Hay partidos cercanos. Revisando en %ds...", sleep_seconds)
                else:
                    sleep_seconds = 1800
                    logger.info("No hay partidos cercanos. Revisando en %ds...", sleep_seconds)

                try:
                    live_alerts = await fetch_live_alerts(client)
                    for alert in live_alerts:
                        if alert["signal_key"] not in sent_live_signals:
                            text = format_live_alert(alert)
                            await bot.send_message(chat_id=CHAT_ID, text=text)
                            sent_live_signals.add(alert["signal_key"])
                except Exception as exc:
                    logger.warning("Live alerts error: %s", exc)

            except Exception as exc:
                logger.error("Cycle error: %s", exc)
                sleep_seconds = CYCLE_INTERVAL

            logger.info("Sleeping %ds until next cycle...", sleep_seconds)
            await asyncio.sleep(sleep_seconds)




if __name__ == "__main__":
    asyncio.run(main())
