import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =========================================================
# FOOTBALL BETTING ALERTS BOT - V14.1 DIOS
# =========================================================
# FUENTES:
# - The Odds API   -> prepartido
# - API-Football   -> live + estadísticas + eventos
#
# FUNCIONES:
# - aviso diario de partidos
# - picks prepartido
# - parley 10 min antes
# - señales live avanzadas
# - anti duplicados
# - ahorro de créditos
# =========================================================

# -------------------------
# CONFIG GENERAL
# -------------------------
BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
TELEGRAM_CHAT_ID= "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

TIMEZONE_NAME = os.getenv("TIMEZONE_NAME", "America/Mexico_City")
LOCAL_TZ = ZoneInfo(TIMEZONE_NAME)

DB_PATH = os.getenv("DB_PATH", "bot_v14_1_dios.db")

# Más preciso para prematch
CYCLE_INTERVAL = int(os.getenv("CYCLE_INTERVAL", "300"))           # 5 min
LIVE_CYCLE_INTERVAL = int(os.getenv("LIVE_CYCLE_INTERVAL", "180")) # 3 min

WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "7"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "22"))

PRE_MATCH_WINDOW_HOURS = int(os.getenv("PRE_MATCH_WINDOW_HOURS", "18"))
PREMATCH_ALERT_MINUTES = int(os.getenv("PREMATCH_ALERT_MINUTES", "10"))
PREMATCH_ALERT_TOLERANCE_MINUTES = int(os.getenv("PREMATCH_ALERT_TOLERANCE_MINUTES", "6"))

MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.06"))
VIP_EDGE = float(os.getenv("VIP_EDGE", "0.10"))

ODDS_REGIONS = os.getenv("ODDS_REGIONS", "uk")
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h")
ODDS_BOOKMAKERS = [b.strip() for b in os.getenv("ODDS_BOOKMAKERS", "").split(",") if b.strip()]

ENABLE_DAILY_FIXTURE_MESSAGE = os.getenv("ENABLE_DAILY_FIXTURE_MESSAGE", "true").lower() == "true"
ENABLE_PREMATCH = os.getenv("ENABLE_PREMATCH", "true").lower() == "true"
ENABLE_LIVE = os.getenv("ENABLE_LIVE", "true").lower() == "true"
ENABLE_PARLAY = os.getenv("ENABLE_PARLAY", "true").lower() == "true"
ENABLE_CREDIT_ALERT = os.getenv("ENABLE_CREDIT_ALERT", "true").lower() == "true"

# Live avanzadas
ENABLE_LIVE_GOALS = os.getenv("ENABLE_LIVE_GOALS", "true").lower() == "true"
ENABLE_LIVE_CORNERS = os.getenv("ENABLE_LIVE_CORNERS", "true").lower() == "true"
ENABLE_LIVE_CARDS = os.getenv("ENABLE_LIVE_CARDS", "true").lower() == "true"
ENABLE_LIVE_BTTS = os.getenv("ENABLE_LIVE_BTTS", "true").lower() == "true"

# Ligas permitidas
ALLOWED_ODDS_SPORT_KEYS = {
    "soccer_epl": "Premier League",
    "soccer_mexico_ligamx": "Liga MX",
}

API_FOOTBALL_LEAGUES = {
    39: "Premier League",
    262: "Liga MX",
}

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("v14_1_dios")

# -------------------------
# TELEGRAM
# -------------------------
bot = Bot(token=BOT_TOKEN)

# -------------------------
# HTTP
# -------------------------
HTTP_TIMEOUT = 35.0

# -------------------------
# DB
# -------------------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_signals (
            signal_key TEXT PRIMARY KEY,
            sent_at TEXT NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_messages (
            message_key TEXT PRIMARY KEY,
            sent_at TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


def db_execute(query, params=(), fetchone=False, fetchall=False):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(query, params)

    result = None
    if fetchone:
        result = cur.fetchone()
    elif fetchall:
        result = cur.fetchall()

    conn.commit()
    conn.close()
    return result


def has_signal_been_sent(signal_key: str) -> bool:
    row = db_execute(
        "SELECT signal_key FROM sent_signals WHERE signal_key = ?",
        (signal_key,),
        fetchone=True
    )
    return row is not None


def mark_signal_sent(signal_key: str):
    db_execute(
        "INSERT OR REPLACE INTO sent_signals(signal_key, sent_at) VALUES(?, ?)",
        (signal_key, now_local().isoformat())
    )


def has_message_been_sent(message_key: str) -> bool:
    row = db_execute(
        "SELECT message_key FROM sent_messages WHERE message_key = ?",
        (message_key,),
        fetchone=True
    )
    return row is not None


def mark_message_sent(message_key: str):
    db_execute(
        "INSERT OR REPLACE INTO sent_messages(message_key, sent_at) VALUES(?, ?)",
        (message_key, now_local().isoformat())
    )

# -------------------------
# UTILS
# -------------------------
def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(dt_str: str) -> datetime:
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str).astimezone(LOCAL_TZ)


def is_within_working_hours() -> bool:
    n = now_local()
    return WORK_START_HOUR <= n.hour < WORK_END_HOUR


def normalize_text(text: str) -> str:
    return " ".join((text or "").strip().lower().split())


def safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace("%", "").strip()
        return int(float(x))
    except Exception:
        return default


def implied_probability(odds: float) -> float:
    if odds <= 0:
        return 0.0
    return 1.0 / odds


def format_local_time(dt: datetime) -> str:
    return dt.astimezone(LOCAL_TZ).strftime("%H:%M")


def format_local_date(dt: datetime) -> str:
    return dt.astimezone(LOCAL_TZ).strftime("%d/%m/%Y")


def today_str() -> str:
    return now_local().strftime("%Y-%m-%d")


async def send_telegram_message(text: str):
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID vacío.")
        return
    try:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text)
        logger.info("Mensaje enviado a Telegram.")
    except TelegramError as e:
        logger.error(f"Error enviando Telegram: {e}")

# -------------------------
# API CALLS
# -------------------------
async def fetch_odds_events_for_sport(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    if ODDS_BOOKMAKERS:
        params["bookmakers"] = ",".join(ODDS_BOOKMAKERS)

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        return r.json(), r.headers


async def fetch_api_football_live():
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    params = {"live": "all"}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()


async def fetch_fixture_statistics(fixture_id: int):
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    params = {"fixture": fixture_id}

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        r = await client.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json()


# -------------------------
# CREDIT ALERT
# -------------------------
async def maybe_alert_odds_credits(headers):
    if not ENABLE_CREDIT_ALERT:
        return

    remaining = headers.get("x-requests-remaining")
    used = headers.get("x-requests-used")

    if remaining is None:
        return

    try:
        remaining_int = int(remaining)
    except Exception:
        return

    alert_key = f"odds_credit_alert_{today_str()}_{remaining_int}"

    if remaining_int <= 20 and not has_message_been_sent(alert_key):
        text = (
            "⚠️ ALERTA DE CRÉDITOS ODDS API\n\n"
            f"Créditos restantes: {remaining_int}\n"
            f"Créditos usados: {used or 'N/D'}\n\n"
            "Ojo: ya quedan pocos créditos."
        )
        await send_telegram_message(text)
        mark_message_sent(alert_key)

# -------------------------
# PREMATCH
# -------------------------
def extract_h2h_outcomes(event: dict):
    all_prices = {}

    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []):
                name = outcome.get("name")
                price = safe_float(outcome.get("price"), 0.0)
                if name and price > 1.0:
                    all_prices.setdefault(name, []).append(price)

    return all_prices


def compute_value_signal(event: dict):
    outcome_prices = extract_h2h_outcomes(event)
    if not outcome_prices:
        return None

    best_pick = None

    for outcome_name, prices in outcome_prices.items():
        if len(prices) < 2:
            continue

        avg_odds = mean(prices)
        best_odds = max(prices)

        fair_prob = implied_probability(avg_odds)
        market_prob_from_best = implied_probability(best_odds)
        edge = fair_prob - market_prob_from_best

        if edge >= MIN_VALUE_EDGE:
            candidate = {
                "pick": outcome_name,
                "best_odds": round(best_odds, 2),
                "avg_odds": round(avg_odds, 2),
                "edge": round(edge, 4),
                "vip": edge >= VIP_EDGE,
            }
            if best_pick is None or candidate["edge"] > best_pick["edge"]:
                best_pick = candidate

    return best_pick


def build_prematch_message(event: dict, league_name: str, signal: dict):
    commence = parse_iso_datetime(event["commence_time"])
    home = event.get("home_team", "Local")
    away = event.get("away_team", "Visitante")
    edge_pct = round(signal["edge"] * 100, 1)

    header = "💎 VIP SIGNAL PREPARTIDO" if signal["vip"] else "📊 BETTING SIGNAL PREPARTIDO"

    return (
        f"{header}\n\n"
        f"🏆 {league_name}\n"
        f"⚽ {home} vs {away}\n"
        f"🕒 Hora: {format_local_time(commence)}\n"
        f"📅 Fecha: {format_local_date(commence)}\n"
        f"🎯 Pick: {signal['pick']}\n"
        f"💰 Cuota: {signal['best_odds']}\n"
        f"📈 Edge estimado: {edge_pct}%\n"
        f"🧠 Lectura: valor prepartido detectado por diferencia entre la mejor cuota y la línea justa promedio."
    )


def build_parlay_candidates(all_candidates: list):
    filtered = [
        x for x in all_candidates
        if 1.45 <= x["signal"]["best_odds"] <= 2.10
    ]

    filtered.sort(key=lambda x: x["signal"]["edge"], reverse=True)

    parlay = []
    used_matches = set()

    for item in filtered:
        match_key = item["match_key"]
        if match_key in used_matches:
            continue
        parlay.append(item)
        used_matches.add(match_key)
        if len(parlay) == 2:
            break

    if len(parlay) < 2:
        return None

    combined_odds = 1.0
    for item in parlay:
        combined_odds *= item["signal"]["best_odds"]

    combined_odds = round(combined_odds, 2)

    if not (2.00 <= combined_odds <= 4.50):
        return None

    return parlay, combined_odds


def build_parlay_message(parlay_data):
    parlay, combined_odds = parlay_data
    lines = ["🔥 PARLEY ELITE PREPARTIDO\n"]

    for i, item in enumerate(parlay, start=1):
        e = item["event"]
        sig = item["signal"]
        commence = parse_iso_datetime(e["commence_time"])
        home = e.get("home_team", "Local")
        away = e.get("away_team", "Visitante")
        league = item["league_name"]

        lines.append(
            f"Leg {i}\n"
            f"🏆 {league}\n"
            f"⚽ {home} vs {away}\n"
            f"🕒 {format_local_time(commence)}\n"
            f"🎯 {sig['pick']} @ {sig['best_odds']}\n"
        )

    lines.append(f"💰 Cuota combinada aprox: {combined_odds}")
    lines.append("🧠 Parley armado con picks de valor detectados por el modelo.")
    return "\n".join(lines)


async def get_today_odds_events():
    all_events = []

    for sport_key, league_name in ALLOWED_ODDS_SPORT_KEYS.items():
        try:
            events, headers = await fetch_odds_events_for_sport(sport_key)
            await maybe_alert_odds_credits(headers)

            for event in events:
                try:
                    commence = parse_iso_datetime(event["commence_time"])
                except Exception:
                    continue

                now_ = now_local()
                delta_hours = (commence - now_).total_seconds() / 3600

                if commence.date() != now_.date():
                    continue

                if delta_hours < -3:
                    continue

                if delta_hours > PRE_MATCH_WINDOW_HOURS:
                    continue

                event["_league_name"] = league_name
                all_events.append(event)

        except Exception as e:
            logger.error(f"Error leyendo Odds API {sport_key}: {e}")

    return all_events


async def send_daily_fixture_message_if_needed(events_today: list):
    if not ENABLE_DAILY_FIXTURE_MESSAGE:
        return

    daily_key = f"daily_fixture_notice_{today_str()}"
    if has_message_been_sent(daily_key):
        return

    leagues_found = sorted(set(event["_league_name"] for event in events_today))

    if leagues_found:
        leagues_txt = ", ".join(leagues_found)
        text = (
            f"📅 Hoy sí hay partidos en: {leagues_txt}.\n"
            f"El bot queda atento y mandará picks/parley {PREMATCH_ALERT_MINUTES} min antes."
        )
    else:
        text = (
            "📅 Hoy no encontré partidos en tus ligas configuradas.\n"
            "El bot seguirá atento dentro del horario de trabajo."
        )

    await send_telegram_message(text)
    mark_message_sent(daily_key)


async def process_prematch_alerts(events_today: list):
    if not ENABLE_PREMATCH:
        return

    now_ = now_local()
    candidates = []

    for event in events_today:
        try:
            commence = parse_iso_datetime(event["commence_time"])
            mins_to_start = (commence - now_).total_seconds() / 60

            # ventana más flexible para no perder el disparo
            if mins_to_start < 0:
                continue

            lower_bound = PREMATCH_ALERT_MINUTES - PREMATCH_ALERT_TOLERANCE_MINUTES
            upper_bound = PREMATCH_ALERT_MINUTES + PREMATCH_ALERT_TOLERANCE_MINUTES

            if not (lower_bound <= mins_to_start <= upper_bound):
                continue

            signal = compute_value_signal(event)
            if not signal:
                continue

            event_id = event.get("id")
            if not event_id:
                continue

            match_key = f"{event_id}_prematch_{normalize_text(signal['pick'])}"
            if has_signal_been_sent(match_key):
                continue

            candidates.append({
                "event": event,
                "signal": signal,
                "league_name": event["_league_name"],
                "match_key": str(event_id),
                "signal_key": match_key,
            })

        except Exception as e:
            logger.error(f"Error procesando prematch: {e}")

    for item in candidates:
        msg = build_prematch_message(item["event"], item["league_name"], item["signal"])
        await send_telegram_message(msg)
        mark_signal_sent(item["signal_key"])

    if ENABLE_PARLAY and len(candidates) >= 2:
        parlay_data = build_parlay_candidates(candidates)
        if parlay_data:
            parlay_key = f"parlay_{today_str()}_{PREMATCH_ALERT_MINUTES}"
            if not has_signal_been_sent(parlay_key):
                msg = build_parlay_message(parlay_data)
                await send_telegram_message(msg)
                mark_signal_sent(parlay_key)

# -------------------------
# LIVE
# -------------------------
def format_live_message(header: str, league_name: str, home: str, away: str, minute: str, score: str, reason_text: str):
    return (
        f"{header}\n\n"
        f"🏆 {league_name}\n"
        f"⚽ {home} vs {away}\n"
        f"⏱️ Minuto: {minute}\n"
        f"📊 Marcador: {score}\n"
        f"🧠 Motivo: {reason_text}"
    )


def build_live_signal_key(match_id: str, signal_type: str):
    return f"live_{match_id}_{signal_type}"


def parse_statistics_response(stats_json: dict, home_name: str, away_name: str):
    result = {
        "home_shots_on": 0,
        "away_shots_on": 0,
        "home_shots_total": 0,
        "away_shots_total": 0,
        "home_corners": 0,
        "away_corners": 0,
        "home_yellow": 0,
        "away_yellow": 0,
        "home_red": 0,
        "away_red": 0,
        "home_possession": 0,
        "away_possession": 0,
    }

    response = stats_json.get("response", []) or []

    for team_block in response:
        team = team_block.get("team", {})
        team_name = team.get("name", "")
        stats = team_block.get("statistics", []) or []

        prefix = None
        if team_name == home_name:
            prefix = "home"
        elif team_name == away_name:
            prefix = "away"

        if not prefix:
            continue

        for stat in stats:
            s_type = stat.get("type")
            s_value = stat.get("value")

            if s_type == "Shots on Goal":
                result[f"{prefix}_shots_on"] = safe_int(s_value, 0)
            elif s_type == "Total Shots":
                result[f"{prefix}_shots_total"] = safe_int(s_value, 0)
            elif s_type == "Corner Kicks":
                result[f"{prefix}_corners"] = safe_int(s_value, 0)
            elif s_type == "Yellow Cards":
                result[f"{prefix}_yellow"] = safe_int(s_value, 0)
            elif s_type == "Red Cards":
                result[f"{prefix}_red"] = safe_int(s_value, 0)
            elif s_type == "Ball Possession":
                result[f"{prefix}_possession"] = safe_int(s_value, 0)

    result["shots_total"] = result["home_shots_total"] + result["away_shots_total"]
    result["shots_on"] = result["home_shots_on"] + result["away_shots_on"]
    result["corners_total"] = result["home_corners"] + result["away_corners"]
    result["yellow_total"] = result["home_yellow"] + result["away_yellow"]

    return result


def reason_pressure_goals(minute_int, stats):
    if not ENABLE_LIVE_GOALS:
        return None
    if minute_int >= 60 and (stats["shots_total"] >= 18 or stats["shots_on"] >= 8):
        return "🔴 SEÑAL EN VIVO", f"Mucho volumen ofensivo ({stats['shots_total']} tiros / {stats['shots_on']} al arco). Posible gol tardío / over live."
    return None


def reason_draw_late(minute_int, home_goals, away_goals, stats):
    if not ENABLE_LIVE_GOALS:
        return None
    if minute_int >= 70 and home_goals == away_goals and stats["shots_total"] >= 14:
        return "🔴 SEÑAL EN VIVO", f"Empate en tramo final con ritmo ofensivo ({stats['shots_total']} tiros). Ojo con gol tardío."
    return None


def reason_one_goal_margin(minute_int, home_goals, away_goals):
    if not ENABLE_LIVE_GOALS:
        return None
    if minute_int >= 65 and abs(home_goals - away_goals) == 1:
        return "🔴 SEÑAL EN VIVO", "Diferencia de un gol en tramo avanzado. Ojo con gol tardío / empate / over live."
    return None


def reason_red_card(minute_int, stats):
    if not ENABLE_LIVE_CARDS:
        return None
    if minute_int >= 55 and (stats["home_red"] > 0 or stats["away_red"] > 0):
        return "💎 SEÑAL VIP EN VIVO", f"Partido condicionado por roja (Local {stats['home_red']} - Visitante {stats['away_red']}). Puede abrir mucho valor live."
    return None


def reason_corners(minute_int, stats):
    if not ENABLE_LIVE_CORNERS:
        return None
    if minute_int >= 60 and stats["corners_total"] >= 9:
        return "🔴 SEÑAL EN VIVO", f"Ritmo alto de corners ({stats['corners_total']}). Ojo con over de corners live."
    return None


def reason_cards(minute_int, stats):
    if not ENABLE_LIVE_CARDS:
        return None
    if minute_int >= 55 and stats["yellow_total"] >= 5:
        return "🔴 SEÑAL EN VIVO", f"Partido caliente con muchas tarjetas ({stats['yellow_total']} amarillas). Ojo con over tarjetas live."
    return None


def reason_btts(minute_int, home_goals, away_goals, stats):
    if not ENABLE_LIVE_BTTS:
        return None
    if minute_int >= 55:
        if (home_goals == 0 and away_goals >= 1) or (away_goals == 0 and home_goals >= 1):
            if stats["shots_on"] >= 7 and stats["shots_total"] >= 15:
                return "🔴 SEÑAL EN VIVO", "Un equipo ya marcó y el partido sigue abierto. Ojo con ambos anotan live."
    return None


async def process_live_alerts():
    if not ENABLE_LIVE:
        return

    try:
        data = await fetch_api_football_live()
    except Exception as e:
        logger.error(f"Error leyendo API-Football live: {e}")
        return

    fixtures = data.get("response", []) or []

    for item in fixtures:
        try:
            league = item.get("league", {})
            fixture = item.get("fixture", {})
            teams = item.get("teams", {})
            goals = item.get("goals", {})

            league_id = league.get("id")
            if league_id not in API_FOOTBALL_LEAGUES:
                continue

            match_id = fixture.get("id")
            if not match_id:
                continue

            league_name = API_FOOTBALL_LEAGUES[league_id]
            home = teams.get("home", {}).get("name", "Local")
            away = teams.get("away", {}).get("name", "Visitante")

            minute = fixture.get("status", {}).get("elapsed")
            if minute is None:
                continue

            minute_int = safe_int(minute, 0)
            if minute_int < 55:
                continue

            home_goals = goals.get("home", 0) if goals.get("home") is not None else 0
            away_goals = goals.get("away", 0) if goals.get("away") is not None else 0

            score_text = f"{home} {home_goals}-{away_goals} {away}"

            # Pedimos estadísticas solo para partidos en vivo y ligas permitidas
            try:
                stats_json = await fetch_fixture_statistics(match_id)
                stats = parse_statistics_response(stats_json, home, away)
            except Exception as e:
                logger.error(f"Error obteniendo estadísticas fixture {match_id}: {e}")
                continue

            checks = {
                "pressure_goals": reason_pressure_goals(minute_int, stats),
                "draw_late": reason_draw_late(minute_int, home_goals, away_goals, stats),
                "one_goal_margin": reason_one_goal_margin(minute_int, home_goals, away_goals),
                "red_card": reason_red_card(minute_int, stats),
                "corners": reason_corners(minute_int, stats),
                "cards": reason_cards(minute_int, stats),
                "btts": reason_btts(minute_int, home_goals, away_goals, stats),
            }

            for signal_type, outcome in checks.items():
                if not outcome:
                    continue

                signal_key = build_live_signal_key(str(match_id), signal_type)
                if has_signal_been_sent(signal_key):
                    continue

                header, reason_text = outcome
                msg = format_live_message(
                    header=header,
                    league_name=league_name,
                    home=home,
                    away=away,
                    minute=f"{minute_int}'",
                    score=score_text,
                    reason_text=reason_text
                )
                await send_telegram_message(msg)
                mark_signal_sent(signal_key)

        except Exception as e:
            logger.error(f"Error procesando live fixture: {e}")

# -------------------------
# CLEANUP
# -------------------------
def cleanup_old_records(days: int = 7):
    threshold = (now_local() - timedelta(days=days)).isoformat()
    db_execute("DELETE FROM sent_signals WHERE sent_at < ?", (threshold,))
    db_execute("DELETE FROM sent_messages WHERE sent_at < ?", (threshold,))

# -------------------------
# CICLOS
# -------------------------
async def prematch_cycle():
    if not is_within_working_hours():
        logger.info("Fuera de horario. Prematch en espera.")
        return

    events_today = await get_today_odds_events()
    await send_daily_fixture_message_if_needed(events_today)

    if not events_today:
        logger.info("No hay partidos hoy en ligas permitidas.")
        return

    await process_prematch_alerts(events_today)


async def live_cycle():
    if not is_within_working_hours():
        logger.info("Fuera de horario. Live en espera.")
        return

    await process_live_alerts()

# -------------------------
# MAIN
# -------------------------
async def main():
    init_db()
    cleanup_old_records()

    if not BOT_TOKEN:
        raise ValueError("Falta BOT_TOKEN")
    if not ODDS_API_KEY:
        raise ValueError("Falta ODDS_API_KEY")
    if not API_FOOTBALL_KEY:
        raise ValueError("Falta API_FOOTBALL_KEY")
    if not TELEGRAM_CHAT_ID:
        raise ValueError("Falta TELEGRAM_CHAT_ID")

    logger.info("Bot V14.1 DIOS iniciado.")

    last_cleanup_day = today_str()
    last_live_run = utc_now() - timedelta(seconds=LIVE_CYCLE_INTERVAL)

    while True:
        try:
            current_day = today_str()
            if current_day != last_cleanup_day:
                cleanup_old_records()
                last_cleanup_day = current_day

            await prematch_cycle()

            now_utc = utc_now()
            if (now_utc - last_live_run).total_seconds() >= LIVE_CYCLE_INTERVAL:
                await live_cycle()
                last_live_run = now_utc

        except Exception as e:
            logger.error(f"Error general del loop: {e}")

        await asyncio.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
