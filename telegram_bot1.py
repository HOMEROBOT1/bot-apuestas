"""
Football Betting Alerts Bot
----------------------------
Data sources:
  - The Odds API  -> pre-match odds
  - API-Football  -> live fixtures / scores / statistics
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from statistics import mean

import httpx
from telegram import Bot
from telegram.error import TelegramError
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Runtime dedupe
# ---------------------------------------------------------------------------

sent_live_signals = set()
sent_pre_match_signals = set()
sent_upcoming_match_alerts = set()
odds_credits_alert_sent = False

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
FOOTBALL_API_KEY = "c455630d0023ef208f93dd0567164905"
CHAT_ID = "1983622390"

LOCAL_TZ = ZoneInfo("America/Mexico_City")

# Ciclos
PREMATCH_SCAN_INTERVAL = 180   # 3 min
LIVE_SCAN_INTERVAL = 60        # 1 min
UPCOMING_SCAN_INTERVAL = 300   # 5 min
MAIN_LOOP_SLEEP = 60           # 1 min

# Ventana pre-match
PREMATCH_MIN_MINUTES = 15
PREMATCH_MAX_MINUTES = 120

# Próximos partidos informativos
UPCOMING_LOOKAHEAD_HOURS = 12

# Filtro de valor pre-match
MIN_VALUE_EDGE = 0.06  # 6%

# Límites
MAX_PREMATCH_ALERTS_PER_CYCLE = 5
MAX_LIVE_ALERTS_PER_CYCLE = 6

# Odds API
ODDS_MARKETS = "h2h"
ODDS_REGIONS = "uk"
ODDS_API_REQUEST_DELAY = 1

# ---------------------------------------------------------------------------
# V3 LIVE SETTINGS
# ---------------------------------------------------------------------------

LIVE_MIN_MINUTE = 8
LIVE_GOAL_MIN_SCORE = 7
LIVE_CORNERS_MIN_SCORE = 6
LIVE_CARDS_MIN_SCORE = 6
MAX_STATS_REQUESTS_PER_CYCLE = 5

# ---------------------------------------------------------------------------
# Whitelisted leagues
# ---------------------------------------------------------------------------

WHITELISTED_SPORTS = {
    "soccer_epl": "🇬🇧 Premier League",
    "soccer_uefa_champs_league": "🏆 Champions League",
    "soccer_mexico_ligamx": "🇲🇽 Liga MX",
}

ALLOWED_LEAGUE_IDS = {39, 2, 262}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def utc_to_local(dt: datetime) -> datetime:
    return dt.astimezone(LOCAL_TZ)


def parse_iso_datetime(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))


def format_local_hour(dt: datetime) -> str:
    return utc_to_local(dt).strftime("%I:%M %p").lstrip("0")


def get_minutes_to_start(commence_time: datetime) -> int:
    diff = commence_time - datetime.now(timezone.utc)
    return int(diff.total_seconds() // 60)


def should_send_prematch(commence_time: datetime) -> bool:
    minutes_to_start = get_minutes_to_start(commence_time)
    return PREMATCH_MIN_MINUTES <= minutes_to_start <= PREMATCH_MAX_MINUTES


def normalize_text(value: str) -> str:
    return (value or "").strip().lower()


def get_match_teams_from_odds_event(game: dict) -> tuple[str, str]:
    home_team = game.get("home_team") or "Local"
    away_team = game.get("away_team")

    if away_team:
        return home_team, away_team

    teams = game.get("teams", [])
    for team in teams:
        if team != home_team:
            return home_team, team

    return home_team, "Visitante"


def build_prematch_key(sport_key: str, home_team: str, away_team: str, pick: str) -> str:
    return f"{normalize_text(sport_key)}|{normalize_text(home_team)}|{normalize_text(away_team)}|{normalize_text(pick)}"


def build_upcoming_key(match_id: str) -> str:
    return str(match_id)


def build_live_key(fixture_id: int, minute: int, signal_type: str) -> str:
    minute_bucket = minute // 5
    return f"{fixture_id}|{minute_bucket}|{normalize_text(signal_type)}"


def implied_probability(decimal_odds: float) -> float:
    if not decimal_odds or decimal_odds <= 1:
        return 0.0
    return 1 / decimal_odds


def translate_pick(team_name: str, home_team: str, away_team: str) -> str:
    if team_name == home_team:
        return f"Gana {home_team}"
    if team_name == away_team:
        return f"Gana {away_team}"
    if normalize_text(team_name) == "draw":
        return "Empate"
    return team_name


def stat_num(value) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if value.endswith("%"):
            value = value[:-1]
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def extract_live_stat_block(stats: dict) -> dict:
    return {
        "shots_on_goal": stat_num(stats.get("Shots on Goal")),
        "shots_off_goal": stat_num(stats.get("Shots off Goal")),
        "total_shots": stat_num(stats.get("Total Shots")),
        "blocked_shots": stat_num(stats.get("Blocked Shots")),
        "corner_kicks": stat_num(stats.get("Corner Kicks")),
        "dangerous_attacks": stat_num(stats.get("Dangerous Attacks")),
        "ball_possession": stat_num(stats.get("Ball Possession")),
        "yellow_cards": stat_num(stats.get("Yellow Cards")),
        "red_cards": stat_num(stats.get("Red Cards")),
        "fouls": stat_num(stats.get("Fouls")),
    }


def classify_live_tier(score: int) -> str:
    if score >= 9:
        return "VIP"
    if score >= 7:
        return "PRO"
    if score >= 6:
        return "BUENA"
    return "NORMAL"


def score_goal_signal(
    minute: int,
    team_stats: dict,
    opp_stats: dict,
    team_goals: int,
    opp_goals: int,
) -> tuple[int, list[str]]:
    score = 0
    reasons = []

    shots_on = team_stats["shots_on_goal"]
    dangerous = team_stats["dangerous_attacks"]
    total_shots = team_stats["total_shots"]
    corners = team_stats["corner_kicks"]

    if minute >= 8:
        score += 1
        reasons.append("minuto útil")

    if shots_on >= 2:
        score += 2
        reasons.append(f"{shots_on} tiros a puerta")

    if shots_on >= 4:
        score += 1

    if dangerous >= 18:
        score += 2
        reasons.append(f"{dangerous} ataques peligrosos")

    if dangerous >= 28:
        score += 1

    if total_shots >= 8:
        score += 1
        reasons.append(f"{total_shots} tiros totales")

    if corners >= 4:
        score += 1
        reasons.append(f"{corners} corners")

    if team_goals <= opp_goals:
        score += 1
        reasons.append("equipo presionando sin ir cómodo")

    return score, reasons


def score_corners_signal(minute: int, home_stats: dict, away_stats: dict) -> tuple[int, list[str]]:
    score = 0
    reasons = []

    total_corners = home_stats["corner_kicks"] + away_stats["corner_kicks"]
    total_dangerous = home_stats["dangerous_attacks"] + away_stats["dangerous_attacks"]
    total_shots = home_stats["total_shots"] + away_stats["total_shots"]

    if minute >= 15:
        score += 1
        reasons.append("partido avanzado")

    if total_corners >= 4:
        score += 2
        reasons.append(f"{total_corners} corners")

    if total_corners >= 6:
        score += 1

    if total_dangerous >= 24:
        score += 2
        reasons.append(f"{total_dangerous} ataques peligrosos")

    if total_shots >= 10:
        score += 1
        reasons.append(f"{total_shots} tiros totales")

    return score, reasons


def score_cards_signal(minute: int, home_stats: dict, away_stats: dict) -> tuple[int, list[str]]:
    score = 0
    reasons = []

    total_fouls = home_stats["fouls"] + away_stats["fouls"]
    total_yellow = home_stats["yellow_cards"] + away_stats["yellow_cards"]
    total_red = home_stats["red_cards"] + away_stats["red_cards"]

    if minute >= 20:
        score += 1
        reasons.append("minuto suficiente")

    if total_fouls >= 12:
        score += 2
        reasons.append(f"{total_fouls} faltas")

    if total_fouls >= 18:
        score += 1

    if total_yellow >= 2:
        score += 2
        reasons.append(f"{total_yellow} amarillas")

    if total_yellow >= 4:
        score += 1

    if total_red >= 1:
        score += 1
        reasons.append("partido caliente")

    return score, reasons


# ---------------------------------------------------------------------------
# Formatting
# ---------------------------------------------------------------------------

def format_upcoming_match_alert(match: dict) -> str:
    return (
        f"📅 PRÓXIMO PARTIDO\n\n"
        f"⚽ {match['home_team']} vs {match['away_team']}\n"
        f"🏆 Liga: {match['league']}\n"
        f"🕒 Hora: {format_local_hour(match['kickoff'])}"
    )


def format_pre_match_alert(alert: dict) -> str:
    return (
        f"📊 ALERTA PRE-PARTIDO\n\n"
        f"⚽ {alert['home_team']} vs {alert['away_team']}\n"
        f"🏆 Liga: {alert['league']}\n"
        f"🎯 Pick: {alert['pick_text']}\n"
        f"💰 Mejor cuota: {alert['best_odds']:.2f}\n"
        f"📉 Cuota promedio: {alert['avg_odds']:.2f}\n"
        f"🔥 Edge: {alert['edge_pct']:.2f}%\n"
        f"⏳ Faltan: {alert['minutes_to_start']} min\n"
        f"🕒 Hora: {format_local_hour(alert['commence_time'])}"
    )


def format_live_alert(alert: dict) -> str:
    badge = {
        "VIP": "💎",
        "PRO": "🔥",
        "BUENA": "📈",
        "NORMAL": "📊",
    }.get(alert["tier"], "📊")

    return (
        f"{badge} ALERTA EN VIVO {alert['tier']}\n\n"
        f"⚽ {alert['home']} vs {alert['away']}\n"
        f"🏆 Liga: {alert['league']}\n"
        f"📊 Marcador: {alert['score']}\n"
        f"⏱ Minuto: {alert['minute']}\n"
        f"🎯 Señal: {alert['signal_type']}\n"
        f"⭐ Score: {alert['signal_score']}/10\n"
        f"📌 Motivo: {alert['reason']}"
    )


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

async def send_message(bot: Bot, text: str) -> None:
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
        )
        logger.info("Mensaje enviado (%d chars)", len(text))
    except TelegramError as exc:
        logger.error("Telegram error: %s", exc)


# ---------------------------------------------------------------------------
# Upcoming matches
# ---------------------------------------------------------------------------

async def fetch_upcoming_matches(client: httpx.AsyncClient) -> list[dict]:
    matches: list[dict] = []

    try:
        now_mx = now_local()
        end_time = now_mx + timedelta(hours=UPCOMING_LOOKAHEAD_HOURS)

        seen_match_ids = set()
        dates_to_check = {
            now_mx.strftime("%Y-%m-%d"),
            end_time.strftime("%Y-%m-%d"),
        }

        for league_id in ALLOWED_LEAGUE_IDS:
            for date_str in sorted(dates_to_check):
                r = await client.get(
                    "https://v3.football.api-sports.io/fixtures",
                    params={
                        "league": league_id,
                        "season": 2025,
                        "date": date_str,
                        "timezone": "America/Mexico_City",
                    },
                    headers={"x-apisports-key": FOOTBALL_API_KEY},
                    timeout=20,
                )

                if r.status_code != 200:
                    logger.warning(
                        "API-Football fixtures league=%s date=%s -> %s | %s",
                        league_id, date_str, r.status_code, r.text
                    )
                    continue

                response_data = r.json().get("response", [])

                for item in response_data:
                    league = item.get("league", {})
                    fixture = item.get("fixture", {})
                    teams = item.get("teams", {})

                    fixture_id = fixture.get("id")
                    if not fixture_id or fixture_id in seen_match_ids:
                        continue
                    seen_match_ids.add(fixture_id)

                    status = fixture.get("status", {}).get("short")
                    if status not in {"NS", "TBD"}:
                        continue

                    kickoff_str = fixture.get("date")
                    if not kickoff_str:
                        continue

                    kickoff = parse_iso_datetime(kickoff_str)
                    kickoff_local = utc_to_local(kickoff)

                    if not (now_mx <= kickoff_local <= end_time):
                        continue

                    home = teams.get("home", {}).get("name")
                    away = teams.get("away", {}).get("name")
                    if not home or not away:
                        continue

                    matches.append({
                        "match_key": build_upcoming_key(fixture_id),
                        "league": league.get("name", "Liga"),
                        "home_team": home,
                        "away_team": away,
                        "kickoff": kickoff,
                    })

    except Exception as exc:
        logger.warning("Error obteniendo próximos partidos: %s", exc)

    return matches


# ---------------------------------------------------------------------------
# Pre-match alerts
# ---------------------------------------------------------------------------

async def fetch_pre_match_alerts(bot: Bot, client: httpx.AsyncClient) -> list[dict]:
    alerts: list[dict] = []
    global odds_credits_alert_sent

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
                timeout=15,
            )

            if r.status_code != 200:
                logger.warning("Odds API %s -> %s | %s", sport_key, r.status_code, r.text)

                if "OUT_OF_USAGE_CREDITS" in r.text and not odds_credits_alert_sent:
                    try:
                        await send_message(
                            bot,
                            "⚠️ Aviso: te quedaste sin créditos en The Odds API."
                        )
                        odds_credits_alert_sent = True
                    except Exception as exc:
                        logger.warning("No se pudo enviar alerta de créditos: %s", exc)

                await asyncio.sleep(ODDS_API_REQUEST_DELAY)
                continue

            odds_credits_alert_sent = False
            games = r.json()

            for game in games:
                commence_time_raw = game.get("commence_time")
                if not commence_time_raw:
                    continue

                commence_time = parse_iso_datetime(commence_time_raw)

                if not should_send_prematch(commence_time):
                    continue

                home_team, away_team = get_match_teams_from_odds_event(game)
                bookmakers = game.get("bookmakers", [])

                if not bookmakers:
                    continue

                outcomes_by_name: dict[str, list[float]] = {}

                for bookmaker in bookmakers:
                    for market in bookmaker.get("markets", []):
                        if market.get("key") != "h2h":
                            continue

                        for outcome in market.get("outcomes", []):
                            name = outcome.get("name")
                            price = outcome.get("price")

                            if not name or not isinstance(price, (int, float)):
                                continue

                            outcomes_by_name.setdefault(name, []).append(float(price))

                if not outcomes_by_name:
                    continue

                for outcome_name, prices in outcomes_by_name.items():
                    if len(prices) < 2:
                        continue

                    best_odds = max(prices)
                    avg_odds = mean(prices)

                    if avg_odds <= 1 or best_odds <= 1:
                        continue

                    fair_prob = implied_probability(avg_odds)
                    best_implied_prob = implied_probability(best_odds)
                    edge_decimal = fair_prob - best_implied_prob

                    if edge_decimal < MIN_VALUE_EDGE:
                        continue

                    match_key = build_prematch_key(
                        sport_key=sport_key,
                        home_team=home_team,
                        away_team=away_team,
                        pick=outcome_name,
                    )

                    alerts.append({
                        "match_key": match_key,
                        "league": league_name,
                        "home_team": home_team,
                        "away_team": away_team,
                        "pick": outcome_name,
                        "pick_text": translate_pick(outcome_name, home_team, away_team),
                        "best_odds": float(best_odds),
                        "avg_odds": float(avg_odds),
                        "edge_pct": edge_decimal * 100,
                        "minutes_to_start": get_minutes_to_start(commence_time),
                        "commence_time": commence_time,
                    })

            await asyncio.sleep(ODDS_API_REQUEST_DELAY)

        except Exception as exc:
            logger.warning("Error consultando %s: %s", sport_key, exc)
            await asyncio.sleep(ODDS_API_REQUEST_DELAY)

    alerts.sort(key=lambda x: (-x["edge_pct"], x["minutes_to_start"]))
    return alerts[:MAX_PREMATCH_ALERTS_PER_CYCLE]


# ---------------------------------------------------------------------------
# Live statistics fetcher
# ---------------------------------------------------------------------------

async def fetch_match_stats(client: httpx.AsyncClient, fixture_id: int):
    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures/statistics",
            params={"fixture": fixture_id},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=15,
        )

        if r.status_code != 200:
            logger.warning("Stats fixture=%s -> %s | %s", fixture_id, r.status_code, r.text)
            return None

        response = r.json().get("response", [])
        if len(response) < 2:
            return None

        def parse_statistics(team_block: dict) -> dict:
            parsed = {}
            for item in team_block.get("statistics", []):
                stat_type = item.get("type")
                stat_value = item.get("value")
                parsed[stat_type] = stat_value
            return parsed

        home_block = response[0]
        away_block = response[1]

        return {
            "home": parse_statistics(home_block),
            "away": parse_statistics(away_block),
        }

    except Exception as exc:
        logger.warning("Error obteniendo stats fixture=%s: %s", fixture_id, exc)
        return None


# ---------------------------------------------------------------------------
# Live alerts
# ---------------------------------------------------------------------------

async def fetch_live_alerts(client: httpx.AsyncClient) -> list[dict]:
    alerts: list[dict] = []

    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures",
            params={
                "live": "all",
                "timezone": "America/Mexico_City",
            },
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=20,
        )

        if r.status_code != 200:
            logger.warning("API-Football live -> %s | %s", r.status_code, r.text)
            return alerts

        data = r.json().get("response", [])
        stats_requests_used = 0

        for match in data:
            fixture = match.get("fixture", {})
            teams = match.get("teams", {})
            goals = match.get("goals", {})
            league = match.get("league", {})

            league_id = league.get("id")
            if league_id not in ALLOWED_LEAGUE_IDS:
                continue

            fixture_id = fixture.get("id")
            home = teams.get("home", {}).get("name")
            away = teams.get("away", {}).get("name")
            minute = fixture.get("status", {}).get("elapsed", 0)

            home_goals = goals.get("home", 0)
            away_goals = goals.get("away", 0)

            if not fixture_id or not home or not away or not minute:
                continue

            if minute < LIVE_MIN_MINUTE:
                continue

            if stats_requests_used >= MAX_STATS_REQUESTS_PER_CYCLE:
                break

            stats_payload = await fetch_match_stats(client, fixture_id)
            stats_requests_used += 1

            if not stats_payload:
                continue

            home_stats = extract_live_stat_block(stats_payload["home"])
            away_stats = extract_live_stat_block(stats_payload["away"])

            # GOAL probable local
            home_goal_score, home_goal_reasons = score_goal_signal(
                minute=minute,
                team_stats=home_stats,
                opp_stats=away_stats,
                team_goals=home_goals,
                opp_goals=away_goals,
            )

            if home_goal_score >= LIVE_GOAL_MIN_SCORE:
                alerts.append({
                    "signal_key": build_live_key(fixture_id, minute, "goal_local"),
                    "league": league.get("name", "Liga"),
                    "home": home,
                    "away": away,
                    "minute": minute,
                    "score": f"{home_goals}-{away_goals}",
                    "signal_type": f"Gol probable - {home}",
                    "signal_score": home_goal_score,
                    "tier": classify_live_tier(home_goal_score),
                    "reason": ", ".join(home_goal_reasons),
                })

            # GOAL probable visitante
            away_goal_score, away_goal_reasons = score_goal_signal(
                minute=minute,
                team_stats=away_stats,
                opp_stats=home_stats,
                team_goals=away_goals,
                opp_goals=home_goals,
            )

            if away_goal_score >= LIVE_GOAL_MIN_SCORE:
                alerts.append({
                    "signal_key": build_live_key(fixture_id, minute, "goal_visitante"),
                    "league": league.get("name", "Liga"),
                    "home": home,
                    "away": away,
                    "minute": minute,
                    "score": f"{home_goals}-{away_goals}",
                    "signal_type": f"Gol probable - {away}",
                    "signal_score": away_goal_score,
                    "tier": classify_live_tier(away_goal_score),
                    "reason": ", ".join(away_goal_reasons),
                })

            # Corners
            corners_score, corners_reasons = score_corners_signal(
                minute=minute,
                home_stats=home_stats,
                away_stats=away_stats,
            )

            if corners_score >= LIVE_CORNERS_MIN_SCORE:
                alerts.append({
                    "signal_key": build_live_key(fixture_id, minute, "corners"),
                    "league": league.get("name", "Liga"),
                    "home": home,
                    "away": away,
                    "minute": minute,
                    "score": f"{home_goals}-{away_goals}",
                    "signal_type": "Over corners live",
                    "signal_score": corners_score,
                    "tier": classify_live_tier(corners_score),
                    "reason": ", ".join(corners_reasons),
                })

            # Cards
            cards_score, cards_reasons = score_cards_signal(
                minute=minute,
                home_stats=home_stats,
                away_stats=away_stats,
            )

            if cards_score >= LIVE_CARDS_MIN_SCORE:
                alerts.append({
                    "signal_key": build_live_key(fixture_id, minute, "cards"),
                    "league": league.get("name", "Liga"),
                    "home": home,
                    "away": away,
                    "minute": minute,
                    "score": f"{home_goals}-{away_goals}",
                    "signal_type": "Posibles más tarjetas",
                    "signal_score": cards_score,
                    "tier": classify_live_tier(cards_score),
                    "reason": ", ".join(cards_reasons),
                })

        alerts.sort(key=lambda x: (-x["signal_score"], x["minute"]))
        return alerts[:MAX_LIVE_ALERTS_PER_CYCLE]

    except Exception as exc:
        logger.warning("Error en live fetch: %s", exc)
        return alerts


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

async def main():
    if not BOT_TOKEN or BOT_TOKEN == "PON_AQUI_TU_BOT_TOKEN":
        raise RuntimeError("BOT_TOKEN no configurado.")
    if not CHAT_ID or CHAT_ID == "PON_AQUI_TU_CHAT_ID":
        raise RuntimeError("CHAT_ID no configurado.")
    if not ODDS_API_KEY or ODDS_API_KEY == "PON_AQUI_TU_ODDS_API_KEY":
        raise RuntimeError("ODDS_API_KEY no configurada.")
    if not FOOTBALL_API_KEY or FOOTBALL_API_KEY == "PON_AQUI_TU_FOOTBALL_API_KEY":
        raise RuntimeError("FOOTBALL_API_KEY no configurada.")

    bot = Bot(token=BOT_TOKEN)
    info = await bot.get_me()
    logger.info("Bot online: @%s", info.username)

    last_upcoming_scan = datetime.min.replace(tzinfo=timezone.utc)
    last_prematch_scan = datetime.min.replace(tzinfo=timezone.utc)
    last_live_scan = datetime.min.replace(tzinfo=timezone.utc)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                current_local = now_local()
                current_utc = datetime.now(timezone.utc)

                if current_local.hour < 7 or current_local.hour >= 22:
                    next_start = current_local.replace(hour=7, minute=0, second=0, microsecond=0)
                    if current_local.hour >= 22:
                        next_start += timedelta(days=1)

                    sleep_seconds = int((next_start - current_local).total_seconds())
                    logger.info("Fuera de horario. Durmiendo hasta las 7:00 AM (%ds).", sleep_seconds)
                    await asyncio.sleep(sleep_seconds)
                    continue

                # Próximos partidos
                if (current_utc - last_upcoming_scan).total_seconds() >= UPCOMING_SCAN_INTERVAL:
                    upcoming_matches = await fetch_upcoming_matches(client)
                    logger.info("Próximos partidos detectados: %s", len(upcoming_matches))

                    for match in upcoming_matches:
                        if match["match_key"] in sent_upcoming_match_alerts:
                            continue

                        text = format_upcoming_match_alert(match)
                        await send_message(bot, text)
                        sent_upcoming_match_alerts.add(match["match_key"])

                    last_upcoming_scan = current_utc

                # Pre-match
                if (current_utc - last_prematch_scan).total_seconds() >= PREMATCH_SCAN_INTERVAL:
                    prematch_alerts = await fetch_pre_match_alerts(bot, client)
                    logger.info("Alertas pre-partido detectadas: %s", len(prematch_alerts))

                    for alert in prematch_alerts:
                        if alert["match_key"] in sent_pre_match_signals:
                            continue

                        text = format_pre_match_alert(alert)
                        await send_message(bot, text)
                        sent_pre_match_signals.add(alert["match_key"])

                    last_prematch_scan = current_utc

                # Live
                if (current_utc - last_live_scan).total_seconds() >= LIVE_SCAN_INTERVAL:
                    live_alerts = await fetch_live_alerts(client)
                    logger.info("Alertas live detectadas: %s", len(live_alerts))

                    for alert in live_alerts:
                        if alert["signal_key"] in sent_live_signals:
                            continue

                        text = format_live_alert(alert)
                        await send_message(bot, text)
                        sent_live_signals.add(alert["signal_key"])

                    last_live_scan = current_utc

                await asyncio.sleep(MAIN_LOOP_SLEEP)

            except Exception as exc:
                logger.error("Cycle error: %s", exc)
                await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
