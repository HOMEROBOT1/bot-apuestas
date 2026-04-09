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
# CONFIGURACIÓN GENERAL
# =========================================================

BOT_TOKEN = os.getenv("BOT_TOKEN", "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ")
CHAT_ID = str(os.getenv("CHAT_ID", "1983622390"))
ODDS_API_KEY = os.getenv("ODDS_API_KEY", "92f3a8c48fe9834c7b1e6bbf38346064")
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "c455630d0023ef208f93dd0567164905")

TIMEZONE = "America/Mexico_City"
ZONE = ZoneInfo(TIMEZONE)

# En Railway usa mejor:
# DB_PATH=/data/bot_state.db
DB_PATH = os.getenv("DB_PATH", "bot_state.db")

# =========================================================
# LIGAS
# =========================================================

# 262 = Liga MX
# 39  = Premier League
# 135 = Serie A
# 2   = UEFA Champions League
LEAGUES = [262, 39, 135, 2]

DEFAULT_LEAGUE_SEASONS = {
    262: 2025,
    39: 2025,
    135: 2025,
    2: 2025,
}

LEAGUE_NAMES = {
    262: "Liga MX",
    39: "Premier League",
    135: "Serie A",
    2: "Champions League",
}

ODDS_SPORT_KEYS = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_italy_serie_a",
    "soccer_uefa_champs_league",
]

# =========================================================
# MODO FIJO
# Opciones: conservador | balanceado | agresivo
# =========================================================

STRATEGY_MODE = os.getenv("STRATEGY_MODE", "balanceado").strip().lower()

STRATEGY_PROFILES = {
    "conservador": {
        "main_loop_seconds": 240,
        "prematch_min_avg_odd": 1.80,
        "prematch_max_avg_odd": 2.20,
        "live_min_minute": 25,
        "live_max_minute": 72,
        "live_goals_max": 3,
        "min_total_shots_on_target": 3,
        "min_total_corners": 4,
        "min_attack_diff": 2,
        "signal_window_minutes": 15,
        "live_ttl_minutes": 50,
    },
    "balanceado": {
        "main_loop_seconds": 180,
        "prematch_min_avg_odd": 1.70,
        "prematch_max_avg_odd": 2.40,
        "live_min_minute": 20,
        "live_max_minute": 75,
        "live_goals_max": 3,
        "min_total_shots_on_target": 2,
        "min_total_corners": 3,
        "min_attack_diff": 2,
        "signal_window_minutes": 10,
        "live_ttl_minutes": 45,
    },
    "agresivo": {
        "main_loop_seconds": 120,
        "prematch_min_avg_odd": 1.60,
        "prematch_max_avg_odd": 2.60,
        "live_min_minute": 15,
        "live_max_minute": 80,
        "live_goals_max": 4,
        "min_total_shots_on_target": 1,
        "min_total_corners": 2,
        "min_attack_diff": 1,
        "signal_window_minutes": 8,
        "live_ttl_minutes": 35,
    },
}

if STRATEGY_MODE not in STRATEGY_PROFILES:
    STRATEGY_MODE = "balanceado"

PROFILE = STRATEGY_PROFILES[STRATEGY_MODE]

MAIN_LOOP_SECONDS = PROFILE["main_loop_seconds"]
PREMATCH_MIN_AVG_ODD = PROFILE["prematch_min_avg_odd"]
PREMATCH_MAX_AVG_ODD = PROFILE["prematch_max_avg_odd"]
LIVE_MIN_MINUTE = PROFILE["live_min_minute"]
LIVE_MAX_MINUTE = PROFILE["live_max_minute"]
LIVE_GOALS_MAX = PROFILE["live_goals_max"]
MIN_TOTAL_SHOTS_ON_TARGET = PROFILE["min_total_shots_on_target"]
MIN_TOTAL_CORNERS = PROFILE["min_total_corners"]
MIN_ATTACK_DIFF = PROFILE["min_attack_diff"]
SIGNAL_WINDOW_MINUTES = PROFILE["signal_window_minutes"]
LIVE_TTL_MINUTES = PROFILE["live_ttl_minutes"]

# =========================================================
# REGLAS
# =========================================================

NO_MATCHES_SLEEP_HOUR = 7
SEASON_RECHECK_HOUR = 6

UPCOMING_TTL_HOURS = 24
PREMATCH_TTL_HOURS = 24

ONLY_ONE_SIGNAL_PER_TYPE_PER_MATCH = True

ENABLE_PRESSURE_SIGNAL = True
ENABLE_OVER15_SIGNAL = True
ENABLE_OVER25_SIGNAL = True
ENABLE_UNDER35_SIGNAL = True
ENABLE_NEXT_GOAL_SIGNAL = True

OVER15_MIN_MINUTE = 20
OVER15_MAX_MINUTE = 70

OVER25_MIN_MINUTE = 25
OVER25_MAX_MINUTE = 75

UNDER35_MIN_MINUTE = 20
UNDER35_MAX_MINUTE = 78

NEXT_GOAL_MIN_MINUTE = 18
NEXT_GOAL_MAX_MINUTE = 80

AUTO_UPDATE_SEASONS = True
ALERT_ON_SEASON_CHANGE = True

# =========================================================
# ESTADO
# =========================================================

LEAGUE_SEASONS = DEFAULT_LEAGUE_SEASONS.copy()

sent_live_signals = {}
sent_prematch_signals = {}
sent_upcoming_match_alerts = {}

last_daily_check_date = None
last_season_sync_date = None

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# =========================================================
# CLIENTES
# =========================================================

bot = Bot(token=BOT_TOKEN)

ODDS_HEADERS = {"apiKey": ODDS_API_KEY}
API_FOOTBALL_HEADERS = {"x-apisports-key": API_FOOTBALL_KEY}

# =========================================================
# DB
# =========================================================

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    db_dir = os.path.dirname(DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    with get_db_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS league_seasons (
                league_id INTEGER PRIMARY KEY,
                season INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.commit()

def load_league_seasons_from_db():
    global LEAGUE_SEASONS

    merged = DEFAULT_LEAGUE_SEASONS.copy()

    with get_db_connection() as conn:
        rows = conn.execute("SELECT league_id, season FROM league_seasons").fetchall()

    for row in rows:
        try:
            merged[int(row["league_id"])] = int(row["season"])
        except Exception:
            continue

    LEAGUE_SEASONS = merged
    logging.info(f"Temporadas cargadas desde SQLite: {LEAGUE_SEASONS}")

def save_league_season_to_db(league_id: int, season: int):
    with get_db_connection() as conn:
        conn.execute("""
            INSERT INTO league_seasons (league_id, season, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(league_id) DO UPDATE SET
                season=excluded.season,
                updated_at=excluded.updated_at
        """, (league_id, season, now_local().isoformat()))
        conn.commit()

def save_all_default_seasons_if_missing():
    with get_db_connection() as conn:
        for league_id, season in DEFAULT_LEAGUE_SEASONS.items():
            conn.execute("""
                INSERT OR IGNORE INTO league_seasons (league_id, season, updated_at)
                VALUES (?, ?, ?)
            """, (league_id, season, now_local().isoformat()))
        conn.commit()

# =========================================================
# HELPERS
# =========================================================

def now_local() -> datetime:
    return datetime.now(ZONE)

def utc_now() -> datetime:
    return datetime.utcnow()

def today_str() -> str:
    return now_local().strftime("%Y-%m-%d")

def leagues_text() -> str:
    return ", ".join(LEAGUE_NAMES.get(league_id, str(league_id)) for league_id in LEAGUES)

def normalize_text(text: str) -> str:
    if not text:
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())

def make_match_key(home: str, away: str, league: str, date_str: str) -> str:
    return f"{normalize_text(home)}|{normalize_text(away)}|{normalize_text(league)}|{date_str}"

def safe_float(value, default=None):
    try:
        return float(value)
    except Exception:
        return default

def minute_from_elapsed(elapsed):
    try:
        return int(elapsed)
    except Exception:
        return 0

def parse_iso_to_local(iso_string: str) -> datetime | None:
    try:
        return datetime.fromisoformat(iso_string.replace("Z", "+00:00")).astimezone(ZONE)
    except Exception:
        return None

def is_recent_sent(cache: dict, key: str, ttl_minutes: int) -> bool:
    sent_at = cache.get(key)
    if not sent_at:
        return False
    return (utc_now() - sent_at) < timedelta(minutes=ttl_minutes)

def remember_sent(cache: dict, key: str):
    cache[key] = utc_now()

def cleanup_cache(cache: dict, ttl_minutes: int):
    now = utc_now()
    expired = [key for key, sent_at in cache.items() if (now - sent_at) >= timedelta(minutes=ttl_minutes)]
    for key in expired:
        del cache[key]

def cleanup_all_caches():
    cleanup_cache(sent_upcoming_match_alerts, UPCOMING_TTL_HOURS * 60)
    cleanup_cache(sent_prematch_signals, PREMATCH_TTL_HOURS * 60)
    cleanup_cache(sent_live_signals, LIVE_TTL_MINUTES)

def signal_key_for_match(fixture_id: int, signal_type: str, minute: int) -> str:
    if ONLY_ONE_SIGNAL_PER_TYPE_PER_MATCH:
        return f"{fixture_id}|{signal_type}"
    return f"{fixture_id}|{signal_type}|{minute // SIGNAL_WINDOW_MINUTES}"

def scoreline_text(score_home: int, score_away: int) -> str:
    return f"{score_home}-{score_away}"

async def send_telegram_message(text: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
        logging.info("Mensaje enviado a Telegram.")
    except TelegramError as e:
        logging.exception(f"Error enviando mensaje a Telegram: {e}")
    except Exception as e:
        logging.exception(f"Error general enviando mensaje a Telegram: {e}")

# =========================================================
# REQUESTS
# =========================================================

async def odds_api_get(url: str, params: dict | None = None) -> dict | list:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=ODDS_HEADERS, params=params)
        resp.raise_for_status()
        return resp.json()

async def api_football_get(url: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=API_FOOTBALL_HEADERS, params=params)
        resp.raise_for_status()
        return resp.json()

# =========================================================
# MENSAJES
# =========================================================

def format_upcoming_match_alert(home: str, away: str, league_name: str, kickoff_local: str) -> str:
    return (
        f"📅 Próximo partido detectado\n\n"
        f"🏆 Liga: {league_name}\n"
        f"⚽ {home} vs {away}\n"
        f"🕒 Hora: {kickoff_local}\n\n"
        f"Pendiente por si aparece oportunidad pre-match o en vivo."
    )

def format_prematch_signal(home: str, away: str, league_name: str, avg_price: float, market: str) -> str:
    return (
        f"🔥 Señal pre-match\n\n"
        f"🏆 Liga: {league_name}\n"
        f"⚽ {home} vs {away}\n"
        f"📊 Mercado: {market}\n"
        f"💰 Cuota promedio: {avg_price:.2f}\n\n"
        f"Revisar valor antes de entrar."
    )

def format_live_signal(
    signal_type: str,
    home: str,
    away: str,
    league_name: str,
    minute: int,
    score_home: int,
    score_away: int,
    shots_on_target_home: int,
    shots_on_target_away: int,
    corners_home: int,
    corners_away: int,
    pressure_side: str | None,
) -> str:
    type_map = {
        "pressure": "📈 Presión ofensiva",
        "over15": "⚽ Oportunidad Over 1.5",
        "over25": "🔥 Oportunidad Over 2.5",
        "under35": "🧊 Oportunidad Under 3.5",
        "next_goal": "🚨 Oportunidad Siguiente Gol",
    }

    reason_map = {
        "pressure": "Se detecta presión ofensiva útil para monitorear entrada live.",
        "over15": "El ritmo del partido sugiere valor para línea Over 1.5.",
        "over25": "El contexto live puede favorecer una línea Over 2.5.",
        "under35": "El ritmo actual puede favorecer una lectura Under 3.5.",
        "next_goal": "La presión del partido sugiere opción de siguiente gol.",
    }

    extra_lines = [
        f"🎯 Tiros a puerta: {shots_on_target_home}-{shots_on_target_away}",
        f"🚩 Córners: {corners_home}-{corners_away}",
    ]

    if pressure_side:
        extra_lines.append(f"📌 Equipo con más empuje: {pressure_side}")

    extra = "\n".join(extra_lines)

    return (
        f"{type_map.get(signal_type, '🚨 Señal EN VIVO')}\n\n"
        f"🏆 Liga: {league_name}\n"
        f"⚽ {home} vs {away}\n"
        f"⏱ Minuto: {minute}\n"
        f"📍 Marcador: {scoreline_text(score_home, score_away)}\n"
        f"{extra}\n\n"
        f"{reason_map.get(signal_type, 'Revisar momentum y cuotas.')}"
    )

async def send_no_matches_today_alert():
    message = (
        f"😴 No encontré partidos en tus ligas para el día de hoy ({today_str()}).\n\n"
        f"🔎 Ligas revisadas: {leagues_text()}\n\n"
        f"El bot entrará en descanso hasta mañana a las {NO_MATCHES_SLEEP_HOUR:02d}:00."
    )
    await send_telegram_message(message)

async def send_matches_found_today_alert():
    message = (
        f"✅ Sí encontré partidos en tus ligas para el día de hoy ({today_str()}).\n\n"
        f"🔎 Ligas monitoreadas: {leagues_text()}\n\n"
        f"El bot seguirá activo buscando alertas pre-match y en vivo."
    )
    await send_telegram_message(message)

async def send_daily_summary(total_matches: int, per_league_counts: dict):
    lines = [
        f"📋 Resumen diario de partidos ({today_str()})",
        "",
        f"⚽ Total de partidos detectados: {total_matches}",
        "",
        "📊 Por liga:",
    ]
    for league_name, count in per_league_counts.items():
        lines.append(f"• {league_name}: {count}")
    lines.append("")
    lines.append(f"🧠 Modo activo: {STRATEGY_MODE}")
    await send_telegram_message("\n".join(lines))

async def send_season_change_alert(league_id: int, old_season: int | None, new_season: int, auto_applied: bool):
    league_name = LEAGUE_NAMES.get(league_id, str(league_id))
    if auto_applied:
        msg = (
            f"🔄 Cambio de temporada detectado\n\n"
            f"🏆 Liga: {league_name}\n"
            f"📆 Temporada anterior: {old_season}\n"
            f"✅ Nueva temporada detectada: {new_season}\n\n"
            f"El bot la actualizó en SQLite y seguirá operando normal."
        )
    else:
        msg = (
            f"⚠️ Posible cambio de temporada detectado\n\n"
            f"🏆 Liga: {league_name}\n"
            f"📆 Temporada anterior: {old_season}\n"
            f"🆕 Temporada detectada: {new_season}\n\n"
            f"Revisa tu configuración."
        )
    await send_telegram_message(msg)

async def send_db_loaded_alert():
    lines = [
        "💾 Temporadas cargadas desde SQLite",
        "",
        "Estas son las temporadas activas:",
    ]
    for league_id in LEAGUES:
        lines.append(f"• {LEAGUE_NAMES.get(league_id, league_id)}: {LEAGUE_SEASONS.get(league_id)}")
    lines.append("")
    lines.append(f"📁 DB_PATH: {DB_PATH}")
    await send_telegram_message("\n".join(lines))

# =========================================================
# TEMPORADAS AUTOMÁTICAS
# =========================================================

def extract_current_season_from_league_response(data: dict, league_id: int) -> int | None:
    response = data.get("response", [])
    if not response:
        return None

    for item in response:
        league = item.get("league", {})
        if league.get("id") != league_id:
            continue

        seasons = item.get("seasons", [])
        if not seasons:
            continue

        current_candidates = [s for s in seasons if s.get("current") is True]
        if current_candidates:
            years = [s.get("year") for s in current_candidates if s.get("year") is not None]
            if years:
                return max(years)

        years = [s.get("year") for s in seasons if s.get("year") is not None]
        if years:
            return max(years)

    return None

async def detect_current_season_for_league(league_id: int) -> int | None:
    url = "https://v3.football.api-sports.io/leagues"
    params = {"id": league_id}

    try:
        data = await api_football_get(url, params=params)
        detected = extract_current_season_from_league_response(data, league_id)
        logging.info(f"Temporada detectada para liga {league_id}: {detected}")
        return detected
    except Exception as e:
        logging.exception(f"Error detectando temporada de liga {league_id}: {e}")
        return None

async def sync_league_seasons():
    for league_id in LEAGUES:
        current_config = LEAGUE_SEASONS.get(league_id)
        detected = await detect_current_season_for_league(league_id)

        if detected is None:
            logging.warning(f"No se pudo detectar temporada actual para liga {league_id}. Se mantiene {current_config}.")
            continue

        if current_config != detected:
            old_value = current_config

            if AUTO_UPDATE_SEASONS:
                LEAGUE_SEASONS[league_id] = detected
                save_league_season_to_db(league_id, detected)

                logging.info(f"Temporada actualizada automáticamente para liga {league_id}: {old_value} -> {detected}")

                if ALERT_ON_SEASON_CHANGE:
                    await send_season_change_alert(league_id, old_value, detected, True)
            else:
                logging.warning(f"Cambio de temporada detectado en liga {league_id}: {old_value} -> {detected}")
                if ALERT_ON_SEASON_CHANGE:
                    await send_season_change_alert(league_id, old_value, detected, False)

# =========================================================
# REVISIÓN DEL DÍA
# =========================================================

async def get_today_matches_summary():
    today = today_str()
    total_matches = 0
    per_league_counts = {}

    for league_id in LEAGUES:
        season = LEAGUE_SEASONS.get(league_id, DEFAULT_LEAGUE_SEASONS.get(league_id, now_local().year))
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        url = "https://v3.football.api-sports.io/fixtures"
        params = {
            "league": league_id,
            "season": season,
            "date": today,
            "timezone": TIMEZONE,
        }

        try:
            data = await api_football_get(url, params=params)
            fixtures = data.get("response", [])
            count = len(fixtures)
            per_league_counts[league_name] = count
            total_matches += count
            logging.info(f"Resumen diario {league_name} | temporada {season}: {count} partidos")
        except Exception as e:
            logging.exception(f"Error obteniendo resumen del día para {league_name}: {e}")
            per_league_counts[league_name] = 0

    return total_matches, per_league_counts

async def wait_until_next_daily_check():
    now = now_local()
    next_run = now.replace(hour=NO_MATCHES_SLEEP_HOUR, minute=0, second=0, microsecond=0)
    if now >= next_run:
        next_run += timedelta(days=1)

    sleep_seconds = max(1, int((next_run - now).total_seconds()))
    logging.info(f"No hay partidos hoy. Durmiendo hasta {next_run.isoformat()}")
    await asyncio.sleep(sleep_seconds)

async def send_upcoming_matches_alerts():
    today = today_str()

    for league_id in LEAGUES:
        season = LEAGUE_SEASONS.get(league_id, DEFAULT_LEAGUE_SEASONS.get(league_id, now_local().year))
        league_name = LEAGUE_NAMES.get(league_id, str(league_id))
        url = "https://v3.football.api-sports.io/fixtures"
        params = {
            "league": league_id,
            "season": season,
            "date": today,
            "timezone": TIMEZONE,
        }

        try:
            data = await api_football_get(url, params=params)
            fixtures = data.get("response", [])

            for fixture in fixtures:
                fixture_info = fixture.get("fixture", {})
                teams = fixture.get("teams", {})
                home = teams.get("home", {}).get("name", "Local")
                away = teams.get("away", {}).get("name", "Visitante")
                kickoff = fixture_info.get("date", "")

                kickoff_dt = parse_iso_to_local(kickoff)
                if kickoff_dt:
                    kickoff_local = kickoff_dt.strftime("%H:%M")
                    date_key = kickoff_dt.strftime("%Y-%m-%d")
                else:
                    kickoff_local = kickoff
                    date_key = today

                match_key = make_match_key(home, away, league_name, date_key)

                if is_recent_sent(sent_upcoming_match_alerts, match_key, UPCOMING_TTL_HOURS * 60):
                    continue

                remember_sent(sent_upcoming_match_alerts, match_key)
                await send_telegram_message(format_upcoming_match_alert(home, away, league_name, kickoff_local))

            logging.info(f"Próximos partidos detectados en {league_name}: {len(fixtures)}")

        except Exception as e:
            logging.exception(f"Error consultando próximos partidos {league_name}: {e}")

# =========================================================
# PREMATCH
# =========================================================

def extract_h2h_prices(bookmakers: list) -> list[float]:
    prices = []
    for bookmaker in bookmakers:
        for market in bookmaker.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []):
                price = safe_float(outcome.get("price"))
                if price:
                    prices.append(price)
    return prices

async def check_prematch_odds():
    for sport_key in ODDS_SPORT_KEYS:
        url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
        params = {
            "regions": "eu",
            "markets": "h2h",
            "oddsFormat": "decimal",
        }

        try:
            events = await odds_api_get(url, params=params)

            for event in events:
                home = event.get("home_team", "Local")
                away = event.get("away_team", "Visitante")
                sport_title = event.get("sport_title", sport_key)
                commence_time = event.get("commence_time", "")

                dt = parse_iso_to_local(commence_time)
                date_key = dt.strftime("%Y-%m-%d") if dt else today_str()

                match_key = make_match_key(home, away, sport_title, date_key)
                if is_recent_sent(sent_prematch_signals, match_key, PREMATCH_TTL_HOURS * 60):
                    continue

                prices = extract_h2h_prices(event.get("bookmakers", []))
                if not prices:
                    continue

                avg_price = mean(prices)

                if PREMATCH_MIN_AVG_ODD <= avg_price <= PREMATCH_MAX_AVG_ODD:
                    remember_sent(sent_prematch_signals, match_key)
                    await send_telegram_message(
                        format_prematch_signal(
                            home=home,
                            away=away,
                            league_name=sport_title,
                            avg_price=avg_price,
                            market="Ganador del partido (1X2/H2H)",
                        )
                    )

            logging.info(f"Revisión pre-match terminada para {sport_key}")

        except Exception as e:
            logging.exception(f"Error en pre-match {sport_key}: {e}")

# =========================================================
# LIVE
# =========================================================

def parse_stat_value(stats: list, stat_name: str) -> int | None:
    for item in stats:
        if item.get("type") == stat_name:
            value = item.get("value")
            if value is None:
                return 0
            if isinstance(value, str):
                value = value.replace("%", "").strip()
            try:
                return int(float(value))
            except Exception:
                return 0
    return None

def infer_pressure_side(home: str, away: str, shots_h: int, shots_a: int, corners_h: int, corners_a: int) -> str | None:
    home_score = shots_h + corners_h
    away_score = shots_a + corners_a
    if home_score - away_score >= MIN_ATTACK_DIFF:
        return home
    if away_score - home_score >= MIN_ATTACK_DIFF:
        return away
    return "Parejo"

def live_stats_pass_filter(shots_h: int, shots_a: int, corners_h: int, corners_a: int) -> bool:
    total_shots = shots_h + shots_a
    total_corners = corners_h + corners_a
    shot_diff = abs(shots_h - shots_a)
    corner_diff = abs(corners_h - corners_a)

    return (
        total_shots >= MIN_TOTAL_SHOTS_ON_TARGET
        or total_corners >= MIN_TOTAL_CORNERS
        or shot_diff >= MIN_ATTACK_DIFF
        or corner_diff >= MIN_ATTACK_DIFF
    )

def is_good_under35_state(minute: int, total_goals: int, shots_total: int, corners_total: int) -> bool:
    return (
        UNDER35_MIN_MINUTE <= minute <= UNDER35_MAX_MINUTE
        and total_goals <= 2
        and shots_total <= 6
        and corners_total <= 9
    )

def should_signal_over15(minute: int, total_goals: int, shots_total: int, corners_total: int) -> bool:
    return (
        OVER15_MIN_MINUTE <= minute <= OVER15_MAX_MINUTE
        and total_goals <= 1
        and (shots_total >= 2 or corners_total >= 4)
    )

def should_signal_over25(minute: int, total_goals: int, shots_total: int, corners_total: int) -> bool:
    return (
        OVER25_MIN_MINUTE <= minute <= OVER25_MAX_MINUTE
        and total_goals <= 2
        and (shots_total >= 3 or corners_total >= 5)
    )

def should_signal_next_goal(minute: int, shots_total: int, corners_total: int, attack_diff: int) -> bool:
    return (
        NEXT_GOAL_MIN_MINUTE <= minute <= NEXT_GOAL_MAX_MINUTE
        and (shots_total >= 3 or corners_total >= 4 or attack_diff >= 2)
    )

def pressure_score(shots_h: int, shots_a: int, corners_h: int, corners_a: int) -> int:
    return (shots_h + shots_a) + (corners_h + corners_a)

async def get_fixture_statistics(fixture_id: int):
    url = "https://v3.football.api-sports.io/fixtures/statistics"
    params = {"fixture": fixture_id}

    try:
        data = await api_football_get(url, params=params)
        response = data.get("response", [])

        if len(response) < 2:
            return None

        home_stats = response[0].get("statistics", [])
        away_stats = response[1].get("statistics", [])

        return {
            "shots_on_target_home": parse_stat_value(home_stats, "Shots on Goal") or 0,
            "shots_on_target_away": parse_stat_value(away_stats, "Shots on Goal") or 0,
            "corners_home": parse_stat_value(home_stats, "Corner Kicks") or 0,
            "corners_away": parse_stat_value(away_stats, "Corner Kicks") or 0,
        }
    except Exception as e:
        logging.exception(f"Error obteniendo estadísticas del fixture {fixture_id}: {e}")
        return None

async def send_live_signal_if_needed(
    fixture_id: int,
    signal_type: str,
    home: str,
    away: str,
    league_name: str,
    minute: int,
    score_home: int,
    score_away: int,
    shots_on_target_home: int,
    shots_on_target_away: int,
    corners_home: int,
    corners_away: int,
    pressure_side: str | None,
):
    key = signal_key_for_match(fixture_id, signal_type, minute)

    if is_recent_sent(sent_live_signals, key, LIVE_TTL_MINUTES):
        return False

    remember_sent(sent_live_signals, key)

    await send_telegram_message(
        format_live_signal(
            signal_type=signal_type,
            home=home,
            away=away,
            league_name=league_name,
            minute=minute,
            score_home=score_home,
            score_away=score_away,
            shots_on_target_home=shots_on_target_home,
            shots_on_target_away=shots_on_target_away,
            corners_home=corners_home,
            corners_away=corners_away,
            pressure_side=pressure_side,
        )
    )
    return True

async def check_live_matches():
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"live": "all", "timezone": TIMEZONE}

    live_alerts = 0

    try:
        data = await api_football_get(url, params=params)
        fixtures = data.get("response", [])
        logging.info(f"Partidos live detectados: {len(fixtures)}")

        for fixture in fixtures:
            fixture_info = fixture.get("fixture", {})
            league = fixture.get("league", {})
            teams = fixture.get("teams", {})
            goals = fixture.get("goals", {})

            fixture_id = fixture_info.get("id")
            league_id = league.get("id")
            league_name = league.get("name", "Liga")

            if league_id not in LEAGUES:
                continue

            home = teams.get("home", {}).get("name", "Local")
            away = teams.get("away", {}).get("name", "Visitante")
            score_home = goals.get("home", 0) or 0
            score_away = goals.get("away", 0) or 0
            elapsed = fixture_info.get("status", {}).get("elapsed", 0)
            minute = minute_from_elapsed(elapsed)
            total_goals = score_home + score_away

            if minute < LIVE_MIN_MINUTE or minute > LIVE_MAX_MINUTE:
                continue

            if total_goals > LIVE_GOALS_MAX:
                continue

            stats = await get_fixture_statistics(fixture_id)
            if not stats:
                continue

            shots_h = stats["shots_on_target_home"]
            shots_a = stats["shots_on_target_away"]
            corners_h = stats["corners_home"]
            corners_a = stats["corners_away"]

            if not live_stats_pass_filter(shots_h, shots_a, corners_h, corners_a):
                continue

            shots_total = shots_h + shots_a
            corners_total = corners_h + corners_a
            attack_diff = max(abs(shots_h - shots_a), abs(corners_h - corners_a))
            pressure_side = infer_pressure_side(home, away, shots_h, shots_a, corners_h, corners_a)

            if ENABLE_PRESSURE_SIGNAL:
                if await send_live_signal_if_needed(
                    fixture_id, "pressure", home, away, league_name, minute,
                    score_home, score_away, shots_h, shots_a, corners_h, corners_a, pressure_side
                ):
                    live_alerts += 1

            if ENABLE_OVER15_SIGNAL and should_signal_over15(minute, total_goals, shots_total, corners_total):
                if await send_live_signal_if_needed(
                    fixture_id, "over15", home, away, league_name, minute,
                    score_home, score_away, shots_h, shots_a, corners_h, corners_a, pressure_side
                ):
                    live_alerts += 1

            if ENABLE_OVER25_SIGNAL and should_signal_over25(minute, total_goals, shots_total, corners_total):
                if await send_live_signal_if_needed(
                    fixture_id, "over25", home, away, league_name, minute,
                    score_home, score_away, shots_h, shots_a, corners_h, corners_a, pressure_side
                ):
                    live_alerts += 1

            if ENABLE_UNDER35_SIGNAL and is_good_under35_state(minute, total_goals, shots_total, corners_total):
                if await send_live_signal_if_needed(
                    fixture_id, "under35", home, away, league_name, minute,
                    score_home, score_away, shots_h, shots_a, corners_h, corners_a, pressure_side
                ):
                    live_alerts += 1

            if ENABLE_NEXT_GOAL_SIGNAL and should_signal_next_goal(minute, shots_total, corners_total, attack_diff):
                if await send_live_signal_if_needed(
                    fixture_id, "next_goal", home, away, league_name, minute,
                    score_home, score_away, shots_h, shots_a, corners_h, corners_a, pressure_side
                ):
                    live_alerts += 1

            logging.info(
                f"Fixture {fixture_id} | {home} vs {away} | {minute}' | marcador {score_home}-{score_away} | presión {pressure_score(shots_h, shots_a, corners_h, corners_a)}"
            )

        logging.info(f"Alertas live enviadas: {live_alerts}")

    except Exception as e:
        logging.exception(f"Error revisando partidos live: {e}")

# =========================================================
# LOOP PRINCIPAL
# =========================================================

async def main_loop():
    global last_daily_check_date, last_season_sync_date

    while True:
        try:
            now = now_local()
            today = now.strftime("%Y-%m-%d")

            cleanup_all_caches()

            if last_season_sync_date != today and now.hour >= SEASON_RECHECK_HOUR:
                last_season_sync_date = today
                logging.info("Iniciando sincronización diaria de temporadas.")
                await sync_league_seasons()

            if last_daily_check_date != today and now.hour >= NO_MATCHES_SLEEP_HOUR:
                last_daily_check_date = today

                logging.info("Iniciando revisión diaria de partidos.")
                total_matches, per_league_counts = await get_today_matches_summary()

                if total_matches <= 0:
                    logging.info("No hay partidos hoy en las ligas configuradas.")
                    await send_no_matches_today_alert()
                    await wait_until_next_daily_check()
                    continue

                logging.info("Sí hay partidos hoy. Bot activo.")
                await send_matches_found_today_alert()
                await send_daily_summary(total_matches, per_league_counts)
                await send_upcoming_matches_alerts()

            await check_prematch_odds()
            await check_live_matches()

            await asyncio.sleep(MAIN_LOOP_SECONDS)

        except Exception as e:
            logging.exception(f"Error en main_loop: {e}")
            await asyncio.sleep(60)

# =========================================================
# ENTRYPOINT
# =========================================================

async def startup_message():
    msg = (
        f"🤖 Bot de apuestas V8 limpia + Champions iniciado\n\n"
        f"Funciones activas:\n"
        f"• Revisión diaria a las 7:00 am\n"
        f"• Sincronización automática de temporadas\n"
        f"• Persistencia en SQLite\n"
        f"• Sin comandos de Telegram\n"
        f"• Sin getUpdates para evitar conflicto 409\n"
        f"• Incluye: Liga MX, Premier League, Serie A y Champions League\n"
        f"• Si cambia la temporada, el bot la actualiza en DB y te avisa\n"
        f"• Si no hay partidos hoy, avisa por Telegram y duerme hasta mañana\n"
        f"• Resumen diario de partidos por liga\n"
        f"• Señales pre-match\n"
        f"• Señales live por tipo de oportunidad\n"
        f"• Modo activo: {STRATEGY_MODE}\n"
        f"• DB_PATH: {DB_PATH}"
    )
    await send_telegram_message(msg)

async def main():
    init_db()
    save_all_default_seasons_if_missing()
    load_league_seasons_from_db()
    await send_db_loaded_alert()
    await sync_league_seasons()
    await startup_message()
    await main_loop()

if __name__ == "__main__":
    asyncio.run(main())
