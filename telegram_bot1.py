import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timezone, timedelta

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =========================================================
# CONFIG
# =========================================================

TIMEZONE = "America/Mexico_City"
RUN_EVERY_SECONDS = 240
LIVE_LOOKBACK_MINUTES = 12
SIGNAL_COOLDOWN_MINUTES = 35
HTTP_TIMEOUT = 30.0
MAX_LIVE_ALERTS_PER_CYCLE = 2
STRONG_BET_THRESHOLD = 82

TRACKED_LEAGUE_IDS = {
    262,  # Liga MX
    39,   # Premier League
    140,  # La Liga
    135,  # Serie A
    78,   # Bundesliga
    61,   # Ligue 1
    2,    # Champions League
    3,    # Europa League
}

BOT_TOKEN_FALLBACK = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID_FALLBACK = "1983622390"
ODDS_API_KEY_FALLBACK = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY_FALLBACK = "c455630d0023ef208f93dd0567164905"

# =========================================================
# HELPERS
# =========================================================

def get_env(name: str, default=None):
    value = os.getenv(name)
    if value and value.strip():
        return value.strip()
    return default

BOT_TOKEN = get_env("BOT_TOKEN", BOT_TOKEN_FALLBACK)
CHAT_ID = get_env("CHAT_ID", CHAT_ID_FALLBACK)
ODDS_API_KEY = get_env("ODDS_API_KEY", ODDS_API_KEY_FALLBACK)
API_FOOTBALL_KEY = get_env("API_FOOTBALL_KEY", API_FOOTBALL_KEY_FALLBACK)
DB_PATH = get_env("DB_PATH", "bot_state.db")

if not BOT_TOKEN or ":" not in BOT_TOKEN:
    raise ValueError("BOT_TOKEN inválido")
if not CHAT_ID:
    raise ValueError("CHAT_ID no configurado")
if not ODDS_API_KEY:
    raise ValueError("ODDS_API_KEY no configurado")
if not API_FOOTBALL_KEY:
    raise ValueError("API_FOOTBALL_KEY no configurado")

bot = Bot(token=BOT_TOKEN)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

sent_cache = {}
last_cycle_sent = set()

# =========================================================
# DB
# =========================================================

def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_signals (
            key TEXT PRIMARY KEY,
            sent_at TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn

def was_sent_recently(key: str, cooldown_minutes: int) -> bool:
    now = datetime.now(timezone.utc)

    if key in sent_cache and now - sent_cache[key] < timedelta(minutes=cooldown_minutes):
        return True

    conn = db_conn()
    row = conn.execute(
        "SELECT sent_at FROM sent_signals WHERE key = ?",
        (key,)
    ).fetchone()
    conn.close()

    if not row:
        return False

    sent_at = datetime.fromisoformat(row[0])
    sent_cache[key] = sent_at
    return now - sent_at < timedelta(minutes=cooldown_minutes)

def mark_sent(key: str):
    now = datetime.now(timezone.utc)
    sent_cache[key] = now

    conn = db_conn()
    conn.execute(
        "INSERT OR REPLACE INTO sent_signals(key, sent_at) VALUES(?, ?)",
        (key, now.isoformat())
    )
    conn.commit()
    conn.close()

def cleanup_cache():
    now = datetime.now(timezone.utc)
    old_keys = [k for k, ts in sent_cache.items() if now - ts > timedelta(hours=12)]
    for k in old_keys:
        del sent_cache[k]

# =========================================================
# UTILS
# =========================================================

def safe_int(x, default=0):
    try:
        if x is None:
            return default
        if isinstance(x, str):
            x = x.replace("%", "").strip()
        return int(float(x))
    except Exception:
        return default

def get_stat(stats_map: dict, *names: str) -> int:
    for name in names:
        if name in stats_map:
            return safe_int(stats_map.get(name), 0)
    return 0

def fmt_conf(conf: int) -> str:
    if conf >= 88:
        return "Muy Alta"
    if conf >= 78:
        return "Alta"
    if conf >= 68:
        return "Media-Alta"
    if conf >= 58:
        return "Media"
    return "Baja"

def league_name_from_fixture(fx: dict) -> str:
    league = fx.get("league", {})
    country = league.get("country", "")
    name = league.get("name", "")
    if country and country not in {"World", "Europe"}:
        return f"{country} {name}"
    return name or "Liga desconocida"

def market_family(market: str) -> str:
    m = market.lower()
    if "over" in m and "córners" not in m and "tarjetas" not in m:
        return "goals_over"
    if "under" in m:
        return "goals_under"
    if "córners" in m:
        return "corners"
    if "tarjetas" in m:
        return "cards"
    return "other"

def signal_priority(sig: dict) -> int:
    bonus = 0
    if sig["market"] in {"Over 1.5", "Over 2.5"}:
        bonus += 6
    if sig["confidence"] >= STRONG_BET_THRESHOLD:
        bonus += 8
    return sig["confidence"] + bonus

# =========================================================
# HTTP
# =========================================================

async def football_api_get(path: str, params: dict | None = None):
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    async with httpx.AsyncClient(
        base_url="https://v3.football.api-sports.io",
        headers=headers,
        timeout=HTTP_TIMEOUT
    ) as client:
        response = await client.get(path, params=params or {})
        response.raise_for_status()
        return response.json()

# =========================================================
# TELEGRAM
# =========================================================

async def send_telegram(text: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
    except TelegramError as e:
        logging.exception("Error enviando Telegram: %s", e)

# =========================================================
# LIVE FETCH
# =========================================================

async def get_live_fixtures():
    data = await football_api_get(
        "/fixtures",
        params={"live": "all", "timezone": TIMEZONE},
    )
    return data.get("response", [])

async def get_fixture_stats(fixture_id: int):
    data = await football_api_get(
        "/fixtures/statistics",
        params={"fixture": fixture_id},
    )
    return data.get("response", [])

def build_team_stats(stats_resp: list[dict]):
    if len(stats_resp) < 2:
        return None

    def parse_team(item: dict):
        stats_map = {}
        for s in item.get("statistics", []):
            stats_map[s.get("type")] = s.get("value")

        return {
            "team": item.get("team", {}).get("name", ""),
            "shots_on": get_stat(stats_map, "Shots on Goal"),
            "shots_off": get_stat(stats_map, "Shots off Goal"),
            "corners": get_stat(stats_map, "Corner Kicks"),
            "yellow": get_stat(stats_map, "Yellow Cards"),
            "red": get_stat(stats_map, "Red Cards"),
            "possession": get_stat(stats_map, "Ball Possession"),
            "dangerous": get_stat(stats_map, "Dangerous Attacks", "Attacks"),
            "attacks": get_stat(stats_map, "Attacks", "Total Shots"),
            "total_shots": get_stat(stats_map, "Total Shots"),
        }

    return parse_team(stats_resp[0]), parse_team(stats_resp[1])

def summarize_pressure(home: dict, away: dict) -> str:
    home_score = (
        home["shots_on"] * 3 +
        home["shots_off"] * 2 +
        home["corners"] * 2 +
        home["dangerous"] * 2 +
        home["attacks"]
    )
    away_score = (
        away["shots_on"] * 3 +
        away["shots_off"] * 2 +
        away["corners"] * 2 +
        away["dangerous"] * 2 +
        away["attacks"]
    )
    diff = home_score - away_score
    if abs(diff) <= 6:
        return "Parejo"
    return "Local" if diff > 0 else "Visitante"

def calculate_pressure_score(home_stats: dict, away_stats: dict) -> int:
    return (
        (home_stats["shots_on"] + away_stats["shots_on"]) * 4 +
        (home_stats["shots_off"] + away_stats["shots_off"]) * 2 +
        (home_stats["corners"] + away_stats["corners"]) * 2 +
        home_stats["dangerous"] + away_stats["dangerous"] +
        home_stats["attacks"] + away_stats["attacks"]
    )

# =========================================================
# LIVE EVALUATION
# =========================================================

def evaluate_live_signal(fx: dict, home_stats: dict, away_stats: dict):
    fixture = fx.get("fixture", {})
    teams = fx.get("teams", {})
    goals = fx.get("goals", {})
    score_home = safe_int(goals.get("home"))
    score_away = safe_int(goals.get("away"))
    total_goals = score_home + score_away
    elapsed = safe_int((fixture.get("status") or {}).get("elapsed"))

    if elapsed < 14 or elapsed > 80:
        return None

    total_shots_on = home_stats["shots_on"] + away_stats["shots_on"]
    total_shots_off = home_stats["shots_off"] + away_stats["shots_off"]
    total_corners = home_stats["corners"] + away_stats["corners"]
    total_yellow = home_stats["yellow"] + away_stats["yellow"]
    total_red = home_stats["red"] + away_stats["red"]
    pressure_score = calculate_pressure_score(home_stats, away_stats)
    pressure_side = summarize_pressure(home_stats, away_stats)

    candidates = []

    # OVER 1.5
    conf_over15 = 0
    if 18 <= elapsed <= 68:
        conf_over15 += 10
    if total_goals == 0 and elapsed <= 34:
        conf_over15 += 10
    if total_goals == 1 and elapsed <= 58:
        conf_over15 += 20
    if total_shots_on >= 5:
        conf_over15 += 24
    if total_shots_off >= 6:
        conf_over15 += 10
    if total_corners >= 5:
        conf_over15 += 8
    if pressure_score >= 30:
        conf_over15 += 20
    if home_stats["shots_on"] >= 3 or away_stats["shots_on"] >= 3:
        conf_over15 += 10

    if conf_over15 >= 72 and total_goals <= 1:
        candidates.append({
            "market": "Over 1.5",
            "confidence": min(conf_over15, 95),
            "reason": "Ritmo ofensivo alto y buena lectura para un gol más",
        })

    # OVER 2.5
    conf_over25 = 0
    if 22 <= elapsed <= 64:
        conf_over25 += 10
    if total_goals == 1 and elapsed <= 38:
        conf_over25 += 18
    if total_goals == 2 and elapsed <= 55:
        conf_over25 += 14
    if total_shots_on >= 7:
        conf_over25 += 24
    if total_shots_off >= 8:
        conf_over25 += 10
    if total_corners >= 6:
        conf_over25 += 10
    if pressure_score >= 36:
        conf_over25 += 20

    if conf_over25 >= 78 and total_goals <= 2:
        candidates.append({
            "market": "Over 2.5",
            "confidence": min(conf_over25, 95),
            "reason": "El partido trae ritmo fuerte para otra anotación",
        })

    # UNDER 3.5
    conf_under35 = 0
    if elapsed >= 24:
        conf_under35 += 8
    if total_goals <= 1 and elapsed >= 24:
        conf_under35 += 28
    if total_shots_on <= 3:
        conf_under35 += 22
    if total_corners <= 4:
        conf_under35 += 10
    if pressure_score <= 16:
        conf_under35 += 22
    if total_red > 0:
        conf_under35 -= 10

    if conf_under35 >= 74 and total_goals <= 2 and pressure_score < 20:
        candidates.append({
            "market": "Under 3.5",
            "confidence": min(conf_under35, 95),
            "reason": "Partido muy cerrado y con ritmo bajo",
        })

    # OVER 8.5 CÓRNERS
    conf_corners = 0
    if 28 <= elapsed <= 76:
        conf_corners += 8
    if total_corners >= 6 and elapsed <= 58:
        conf_corners += 24
    if total_corners >= 7 and elapsed <= 68:
        conf_corners += 28
    if total_shots_on >= 5:
        conf_corners += 10
    if pressure_score >= 32:
        conf_corners += 16

    if conf_corners >= 72 and total_corners <= 8:
        candidates.append({
            "market": "Over 8.5 córners",
            "confidence": min(conf_corners, 95),
            "reason": "La dinámica del juego sigue empujando más córners",
        })

    # OVER 3.5 TARJETAS
    conf_cards = 0
    if 28 <= elapsed <= 84:
        conf_cards += 8
    if total_yellow >= 2 and elapsed <= 45:
        conf_cards += 20
    if total_yellow >= 3 and elapsed <= 70:
        conf_cards += 24
    if total_red >= 1:
        conf_cards += 16
    if pressure_score >= 26:
        conf_cards += 8

    if conf_cards >= 68 and (total_yellow + total_red) <= 4:
        candidates.append({
            "market": "Over 3.5 tarjetas",
            "confidence": min(conf_cards, 95),
            "reason": "La intensidad del partido favorece más amonestaciones",
        })

    if not candidates:
        return None

    goals_over = [c for c in candidates if market_family(c["market"]) == "goals_over"]
    goals_under = [c for c in candidates if market_family(c["market"]) == "goals_under"]

    if goals_over and goals_under:
        best_over = max(goals_over, key=lambda x: x["confidence"])
        best_under = max(goals_under, key=lambda x: x["confidence"])
        if best_over["confidence"] >= best_under["confidence"]:
            candidates = [c for c in candidates if market_family(c["market"]) != "goals_under"]
        else:
            candidates = [c for c in candidates if market_family(c["market"]) != "goals_over"]

    best = max(candidates, key=lambda x: x["confidence"])

    if best["confidence"] < 68:
        return None

    return {
        "fixture_id": safe_int(fixture.get("id")),
        "league": league_name_from_fixture(fx),
        "home": teams.get("home", {}).get("name", "Local"),
        "away": teams.get("away", {}).get("name", "Visitante"),
        "minute": elapsed,
        "score": f"{score_home}-{score_away}",
        "shots_on": f"{home_stats['shots_on']}-{away_stats['shots_on']}",
        "corners": f"{home_stats['corners']}-{away_stats['corners']}",
        "cards": f"{home_stats['yellow'] + home_stats['red']}-{away_stats['yellow'] + away_stats['red']}",
        "pressure_side": pressure_side,
        "market": best["market"],
        "confidence": best["confidence"],
        "reason": best["reason"],
        "family": market_family(best["market"]),
    }

def format_live_signal(sig: dict) -> str:
    strong_tag = "💰 APUESTA FUERTE\n\n" if sig["confidence"] >= STRONG_BET_THRESHOLD else ""
    return (
        f"{strong_tag}"
        f"🚨 Oportunidad {sig['market']}\n\n"
        f"🏆 Liga: {sig['league']}\n"
        f"⚽ {sig['home']} vs {sig['away']}\n"
        f"⏱ Minuto: {sig['minute']}\n"
        f"📍 Marcador: {sig['score']}\n"
        f"🎯 Tiros a puerta: {sig['shots_on']}\n"
        f"🚩 Córners: {sig['corners']}\n"
        f"🟨 Tarjetas: {sig['cards']}\n"
        f"📌 Equipo con más empuje: {sig['pressure_side']}\n"
        f"🔥 Confianza: {fmt_conf(sig['confidence'])} ({sig['confidence']}/100)\n\n"
        f"{sig['reason']}."
    )

def build_live_keys(sig: dict):
    fixture_id = sig["fixture_id"]
    family = sig["family"]
    market = sig["market"]
    score = sig["score"]
    minute_bucket = sig["minute"] // LIVE_LOOKBACK_MINUTES

    exact_key = f"live_exact:{fixture_id}:{market}:{score}"
    family_key = f"live_family:{fixture_id}:{family}"
    match_key = f"live_match:{fixture_id}"
    window_key = f"live_window:{fixture_id}:{minute_bucket}"
    strong_key = f"live_strong:{fixture_id}:{market}"

    return exact_key, family_key, match_key, window_key, strong_key

# =========================================================
# LIVE CHECK
# =========================================================

async def check_live_matches():
    total_live = 0
    total_sent = 0
    candidates_to_send = []

    try:
        fixtures = await get_live_fixtures()
    except Exception as e:
        logging.exception("Error obteniendo fixtures live: %s", e)
        return

    logging.info("Partidos live detectados: %s", len(fixtures))

    for fx in fixtures:
        total_live += 1

        league_id = safe_int((fx.get("league") or {}).get("id"))
        if league_id not in TRACKED_LEAGUE_IDS:
            continue

        fixture = fx.get("fixture", {})
        fixture_id = safe_int(fixture.get("id"))
        elapsed = safe_int((fixture.get("status") or {}).get("elapsed"))

        if elapsed < 14 or elapsed > 80:
            continue

        try:
            stats_resp = await get_fixture_stats(fixture_id)
        except Exception as e:
            logging.exception("Error fixture stats %s: %s", fixture_id, e)
            continue

        parsed = build_team_stats(stats_resp)
        if not parsed:
            continue

        home_stats, away_stats = parsed
        sig = evaluate_live_signal(fx, home_stats, away_stats)
        if not sig:
            continue

        exact_key, family_key, match_key, window_key, strong_key = build_live_keys(sig)

        if exact_key in last_cycle_sent or was_sent_recently(exact_key, SIGNAL_COOLDOWN_MINUTES):
            continue
        if family_key in last_cycle_sent or was_sent_recently(family_key, SIGNAL_COOLDOWN_MINUTES):
            continue
        if match_key in last_cycle_sent or was_sent_recently(match_key, SIGNAL_COOLDOWN_MINUTES):
            continue
        if window_key in last_cycle_sent or was_sent_recently(window_key, LIVE_LOOKBACK_MINUTES):
            continue
        if sig["confidence"] >= STRONG_BET_THRESHOLD:
            if strong_key in last_cycle_sent or was_sent_recently(strong_key, SIGNAL_COOLDOWN_MINUTES + 10):
                continue

        candidates_to_send.append(sig)

    candidates_to_send.sort(key=signal_priority, reverse=True)
    selected = candidates_to_send[:MAX_LIVE_ALERTS_PER_CYCLE]

    for sig in selected:
        exact_key, family_key, match_key, window_key, strong_key = build_live_keys(sig)

        await send_telegram(format_live_signal(sig))

        mark_sent(exact_key)
        mark_sent(family_key)
        mark_sent(match_key)
        mark_sent(window_key)
        if sig["confidence"] >= STRONG_BET_THRESHOLD:
            mark_sent(strong_key)

        last_cycle_sent.add(exact_key)
        last_cycle_sent.add(family_key)
        last_cycle_sent.add(match_key)
        last_cycle_sent.add(window_key)
        if sig["confidence"] >= STRONG_BET_THRESHOLD:
            last_cycle_sent.add(strong_key)

        total_sent += 1

    logging.info("Partidos live revisados: %s", total_live)
    logging.info("Candidatas live filtradas: %s", len(candidates_to_send))
    logging.info("Alertas live enviadas: %s", total_sent)

# =========================================================
# LOOP
# =========================================================

async def run_cycle():
    global last_cycle_sent
    last_cycle_sent = set()
    cleanup_cache()

    logging.info("========== NUEVO CICLO ==========")
    await check_live_matches()

async def main():
    logging.info("Bot iniciado")
    logging.info("BOT_TOKEN OK: %s", bool(BOT_TOKEN))
    logging.info("CHAT_ID: %s", CHAT_ID)
    logging.info("API_FOOTBALL_KEY activa: %s...", API_FOOTBALL_KEY[:6] if API_FOOTBALL_KEY else "None")
    logging.info("DB_PATH: %s", DB_PATH)

    while True:
        try:
            await run_cycle()
        except Exception as e:
            logging.exception("Error general del ciclo: %s", e)

        await asyncio.sleep(RUN_EVERY_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
