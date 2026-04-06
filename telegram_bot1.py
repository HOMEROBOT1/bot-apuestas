"""
Football Betting Alerts Bot
----------------------------
Data sources:
  - The Odds API  (https://the-odds-api.com)  → pre-match odds
  - API-Football  (https://api-sports.io)      → live match scores / events

Get free keys at those sites and fill in the two placeholders below.
The Odds API free tier: 500 requests/month.
API-Football free tier: 100 requests/day.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from statistics import mean
import re

sent_live_signals = set()

import httpx
from telegram import Bot
from telegram.error import TelegramError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

import os

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
ODDS_API_KEY = "6a49cb00636453d9e9f8dc0603021a11"
FOOTBALL_API_KEY = "c455630d0023ef208f93dd0567164905"
CHAT_ID   = "1983622390"

ODDS_API_KEY      = os.environ.get("ODDS_API_KEY", "")      # https://the-odds-api.com
FOOTBALL_API_KEY  = os.environ.get("FOOTBALL_API_KEY", "")  # https://api-sports.io

# How often to run a full cycle (seconds)
CYCLE_INTERVAL = 300   # 5 minutes

# How far ahead to look for upcoming matches (hours)
PRE_MATCH_WINDOW_HOURS = 3

# Maximum alerts to send per cycle
MAX_ALERTS_PER_CYCLE = 5

# Minimum value edge to flag a bet (e.g. 0.06 = 6 % edge over fair odds)
MIN_VALUE_EDGE = 0.06

# ---------------------------------------------------------------------------
# Whitelisted top leagues (Odds API sport keys)
# ---------------------------------------------------------------------------

WHITELISTED_SPORTS = {
    "soccer_epl":                "🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League",
    "soccer_spain_la_liga":      "🇪🇸 La Liga",
    "soccer_italy_serie_a":      "🇮🇹 Serie A",
    "soccer_germany_bundesliga": "🇩🇪 Bundesliga",
    "soccer_uefa_champs_league": "🏆 Champions League",
    "soccer_mexico_ligamx":      "🇲🇽 Liga MX",
}

# API-Football competition IDs for live checks (must mirror WHITELISTED_SPORTS)
LIVE_LEAGUE_IDS = {
    39,   # Premier League
    140,  # La Liga
    135,  # Serie A
    78,   # Bundesliga
    2,    # Champions League
    262,  # Liga MX
}

# Delay between consecutive Odds API requests (seconds) to avoid rate limits
ODDS_API_REQUEST_DELAY = 2

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

# Minimum numeric score (1–10) required to send an alert
MIN_SCORE = 7


def score_badge(score: int) -> str:
    if score >= 9:   return "🟢"
    if score >= 7:   return "🟡"
    return "🔴"


def score_pre_match(edge: float, book_count: int) -> int:
    """
    Base score from edge %, then adjusted by how many bookmakers agree.

    Edge base:
      6–8  %  → 4    8–10 %  → 5    10–12 % → 6
      12–15%  → 7    15–18%  → 8    18 %+   → 9

    Book count modifier:
      < 4 books → −1    4–5 → 0    6–7 → +1    8+ → +2
    """
    pct = edge * 100
    if pct >= 18:   base = 9
    elif pct >= 15: base = 8
    elif pct >= 12: base = 7
    elif pct >= 10: base = 6
    elif pct >= 8:  base = 5
    else:           base = 4

    if book_count >= 8:   mod = 2
    elif book_count >= 6: mod = 1
    elif book_count < 4:  mod = -1
    else:                 mod = 0

    return max(1, min(10, base + mod))


def score_live(priority: int, minute: int) -> int:
    """
    Red card (priority 3):
      60–74' → 7    75–84' → 8    85'+ → 9

    0-0 stalemate (priority 2):
      60–74' → 6    75–84' → 7    85'+ → 8

    One-goal lead (priority 1):
      60–74' → 4    75–79' → 6    80'+ → 7
    """
    if priority == 3:
        if minute >= 85:   return 9
        if minute >= 75:   return 8
        return 7

    if priority == 2:
        if minute >= 85:   return 8
        if minute >= 75:   return 7
        return 6

    # priority 1 — one-goal lead
    if minute >= 80:   return 7
    if minute >= 75:   return 6
    return 4


def passes_score(alert: dict) -> bool:
    return alert["score"] >= MIN_SCORE


def suggested_stake(score: int) -> str:
    """
    7  → 1% of bankroll  (meets threshold, moderate conviction)
    8  → 2% of bankroll  (strong signal)
    9+ → 3% of bankroll  (highest conviction)
    """
    if score >= 9:  return "3% of bankroll"
    if score >= 8:  return "2% of bankroll"
    return "1% of bankroll"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def odds_to_prob(odds: float) -> float:
    """American / decimal odds → implied probability (decimal odds assumed)."""
    return 1.0 / odds if odds > 0 else 0.0


def overround(probs: list[float]) -> float:
    return sum(probs) - 1.0


def best_value_outcome(bookmakers: list[dict]) -> dict | None:
    """
    Collect all 1X2 prices, compute fair probability per outcome,
    then find the single outcome with the biggest positive edge.
    Returns dict with outcome label, best odds, fair prob, edge.
    """
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

    # Fair probability = average implied prob across all books (no-vig line)
    fair_probs: dict[str, float] = {}
    for outcome, odds_list in prices.items():
        raw_probs = [odds_to_prob(o) for o in odds_list]
        total_overround = overround([odds_to_prob(o) for o in
                                     [mean(prices[k]) for k in prices]])
        share = mean(raw_probs) / (1.0 + total_overround)
        fair_probs[outcome] = min(share, 0.99)

    best_outcome = None
    best_edge = MIN_VALUE_EDGE  # must beat the threshold

    for outcome, odds_list in prices.items():
        best_odds = max(odds_list)
        implied = odds_to_prob(best_odds)
        fair = fair_probs[outcome]
        edge = fair - implied  # positive → value on this outcome
        if edge > best_edge:
            best_edge = edge
            best_outcome = {
                "label":      outcome,
                "best_odds":  best_odds,
                "fair_prob":  fair,
                "edge":       edge,
                "book_count": len(odds_list),
            }

    return best_outcome


def format_kickoff(iso_str: str) -> str:
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    local = dt.astimezone()
    return local.strftime("%H:%M")


# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

async def fetch_pre_match_alerts(client: httpx.AsyncClient) -> list[dict]:
    """
    Pull upcoming matches (within PRE_MATCH_WINDOW_HOURS) from whitelisted
    leagues, evaluate value, return sorted alert dicts.
    """
    alerts: list[dict] = []
    now = datetime.now(timezone.utc)
    cutoff = now + timedelta(hours=PRE_MATCH_WINDOW_HOURS)

    for sport_key, league_name in WHITELISTED_SPORTS.items():
        try:
            r = await client.get(
                f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/",
                params={
                    "apiKey": ODDS_API_KEY,
                    "regions": "eu",
                    "markets": "h2h",
                    "oddsFormat": "decimal",
                    "dateFormat": "iso",
                },
                timeout=10,
            )
            if r.status_code != 200:
                logger.warning("Odds API %s → %s", sport_key, r.status_code)
                continue

            for game in r.json():
                commence = datetime.fromisoformat(
                    game["commence_time"].replace("Z", "+00:00")
                )
                if not (now <= commence <= cutoff):
                    continue

                value = best_value_outcome(game.get("bookmakers", []))
                if value is None:
                    continue

                minutes_to_ko = int((commence - now).total_seconds() / 60)

                alerts.append({
                    "type":          "pre_match",
                    "score":         score_pre_match(value["edge"], value["book_count"]),
                    "league":        league_name,
                    "home":          game["home_team"],
                    "away":          game["away_team"],
                    "kickoff":       format_kickoff(game["commence_time"]),
                    "minutes_to_ko": minutes_to_ko,
                    "outcome":       value["label"],
                    "odds":          value["best_odds"],
                    "fair_prob":     value["fair_prob"],
                    "edge":          value["edge"],
                    "book_count":    value["book_count"],
                })

        except Exception as exc:
            logger.error("Pre-match fetch error (%s): %s", sport_key, exc)

        await asyncio.sleep(ODDS_API_REQUEST_DELAY)

    return sorted(alerts, key=lambda a: -a["edge"])


async def fetch_live_alerts(client: httpx.AsyncClient) -> list[dict]:
    """
    Pull in-progress matches from API-Football for whitelisted leagues.
    Flag matches with a red card or a goal in the last 10 minutes (late swing).
    """
    alerts: list[dict] = []
    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures",
            params={"live": "all"},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=10,
        )
        if r.status_code != 200:
            logger.warning("API-Football live → %s", r.status_code)
            return []

        for fixture in r.json().get("response", []):
            league_id = fixture["league"]["id"]
            if league_id not in LIVE_LEAGUE_IDS:
                continue

            status_short = fixture["fixture"]["status"]["short"]
            if status_short not in ("2H", "ET"):
                continue  # only second half / extra time

            minute = fixture["fixture"]["status"].get("elapsed") or 0
            if minute < 60:
                continue  # hard gate — nothing before 60'

            league_name = fixture["league"]["name"]
            home = fixture["teams"]["home"]["name"]
            away = fixture["teams"]["away"]["name"]
            home_goals = fixture["goals"]["home"] or 0
            away_goals = fixture["goals"]["away"] or 0
            goal_diff = abs(home_goals - away_goals)

            # Red cards from events list (more reliable than statistics block)
            red_home = sum(
                1 for e in fixture.get("events", [])
                if e.get("team", {}).get("id") == fixture["teams"]["home"]["id"]
                and e.get("type") == "Card"
                and e.get("detail") in ("Red Card", "Second Yellow Card")
            )
            red_away = sum(
                1 for e in fixture.get("events", [])
                if e.get("team", {}).get("id") == fixture["teams"]["away"]["id"]
                and e.get("type") == "Card"
                and e.get("detail") in ("Red Card", "Second Yellow Card")
            )

            # --- Signal evaluation (priority order: red card > 0-0 > one-goal) ---
            signal = None

            if red_home > 0 or red_away > 0:
                # Highest priority — man advantage changes the market significantly
                parts = []
                if red_home > 0:
                    parts.append(f"🟥 {home} down to {11 - red_home} men")
                if red_away > 0:
                    parts.append(f"🟥 {away} down to {11 - red_away} men")
                signal = {"priority": 3, "reason": " · ".join(parts)}

            elif home_goals == 0 and away_goals == 0:
                # 0-0 past 60' — strong Under / BTTS No signal
                signal = {
                    "priority": 2,
                    "reason": f"0-0 at {minute}' — Under 2.5 / BTTS No value",
                }

            elif goal_diff == 1:
                # One-goal margin — draw or comeback value for trailing team
                leader  = home if home_goals > away_goals else away
                trailer = away if home_goals > away_goals else home
                signal = {
                    "priority": 1,
                    "reason": (
                        f"{leader} lead by 1 at {minute}' — "
                        f"{trailer} comeback / Draw value"
                    ),
                }

            if signal is None:
                continue  # multi-goal gap or no notable situation

            alerts.append({
                "type":      "live",
                "score":     score_live(signal["priority"], minute),
                "priority":  signal["priority"],
                "league":    league_name,
                "home":      home,
                "away":      away,
                "score_str": f"{home_goals}-{away_goals}",
                "minute":    minute,
                "reason":    signal["reason"],
            })

    except Exception as exc:
        logger.error("Live fetch error: %s", exc)

    return sorted(alerts, key=lambda a: -a["priority"])


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

DIV = "━━━━━━━━━━━━━━━━"


def format_pre_match(alert: dict) -> str:
    edge_pct = round(alert["edge"] * 100, 1)
    fair_pct = round(alert["fair_prob"] * 100, 1)
    mins     = alert["minutes_to_ko"]
    time_str = f"{mins}m" if mins < 60 else f"{mins // 60}h {mins % 60}m"
    s        = alert["score"]

    header = "💎 *VIP SIGNAL*" if s >= 8 else "📊 *BETTING SIGNAL*"

    return "\n".join([
        header,
        f"{DIV}",
        f"{alert['league']}",
        f"🕐  Pre-Match  ·  In {time_str}  ·  {alert['kickoff']}",
        f"",
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
def traducir_signal(reason):
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

def format_live(alert: dict) -> str:
    s          = alert["score"]
    home, away = alert["home"], alert["away"]
    sh, sa     = alert["score_str"].split("-")

    header = "💎 *SEÑAL VIP EN VIVO*" if s >= 8 else "🔴 *SEÑAL EN VIVO*"

    return "\n".join([
        header,
        f"{DIV}",
        f"{alert['league']}",
        f"⏱  *{alert['minute']}'*  ·  In Play",
        f"",
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


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def run_cycle(bot: Bot, client: httpx.AsyncClient) -> None:
    logger.info("── Starting new cycle ──────────────────────────────────")

    # Run both fetches concurrently to save time
    pre_task  = asyncio.create_task(fetch_pre_match_alerts(client))
    live_task = asyncio.create_task(fetch_live_alerts(client))

    pre_alerts, live_alerts = await asyncio.gather(pre_task, live_task)

    # Live alerts are more time-sensitive; put them first
    all_alerts = live_alerts + pre_alerts

    # Drop anything below the minimum score threshold
    qualified = [a for a in all_alerts if passes_score(a)]
    dropped   = len(all_alerts) - len(qualified)
    if dropped:
        logger.info("Dropped %d alert(s) below score threshold (%d).", dropped, MIN_SCORE)

    # Sort qualified by score descending so best alerts go first
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
    else:
        signal_key = make_signal_key(alert)

        if signal_key in sent_live_signals:
            print(f"🚫 Señal duplicada omitida: {signal_key}")
            continue

        text = format_live(alert)
        await send_message(bot, text)
        sent_live_signals.add(signal_key)

    await asyncio.sleep(1)  # brief pause between messages 


async def main() -> None:
    bot    = Bot(token=BOT_TOKEN)
    info   = await bot.get_me()
    logger.info("Bot online: @%s", info.username)
  

    # Warn clearly if API keys are missing
    if not ODDS_API_KEY:
        logger.warning(
            "ODDS_API_KEY secret not set — pre-match alerts disabled. "
            "Get a free key at https://the-odds-api.com"
        )
    if not FOOTBALL_API_KEY:
        logger.warning(
            "FOOTBALL_API_KEY secret not set — live alerts disabled. "
            "Get a free key at https://api-sports.io"
        )

    async with httpx.AsyncClient() as client:
        while True:
            try:
                await run_cycle(bot, client)
            except Exception as exc:
                logger.error("Cycle error: %s", exc)

            logger.info("Sleeping %ds until next cycle…", CYCLE_INTERVAL)
            await asyncio.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
