import asyncio
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =============================================================================
# CONFIG DESDE VARIABLES DE ENTORNO
# =============================================================================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

DB_PATH = os.getenv("DB_PATH", "bot_state.txt").strip()

CYCLE_INTERVAL = int(os.getenv("CYCLE_INTERVAL", "900"))  # 15 min
LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Mexico_City"))

ODDS_REGIONS = os.getenv("ODDS_REGIONS", "uk").strip()
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h").strip()
ODDS_BOOKMAKERS = os.getenv("ODDS_BOOKMAKERS", "").strip()

MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.06"))
PRE_MATCH_WINDOW_HOURS = int(os.getenv("PRE_MATCH_WINDOW_HOURS", "24"))

ACTIVE_START_HOUR = int(os.getenv("ACTIVE_START_HOUR", "7"))
ACTIVE_END_HOUR = int(os.getenv("ACTIVE_END_HOUR", "22"))

ENABLE_PREMATCH = os.getenv("ENABLE_PREMATCH", "true").strip().lower() == "true"
ENABLE_LIVE = os.getenv("ENABLE_LIVE", "true").strip().lower() == "true"
ENABLE_PARLAY = os.getenv("ENABLE_PARLAY", "true").strip().lower() == "true"

LIVE_MINUTE_THRESHOLD = int(os.getenv("LIVE_MINUTE_THRESHOLD", "60"))
LIVE_DRAW_ONLY_MINUTE = int(os.getenv("LIVE_DRAW_ONLY_MINUTE", "70"))

# =============================================================================
# VALIDACIONES
# =============================================================================

required_vars = {
    "BOT_TOKEN": BOT_TOKEN,
    "CHAT_ID": CHAT_ID,
    "ODDS_API_KEY": ODDS_API_KEY,
    "API_FOOTBALL_KEY": API_FOOTBALL_KEY,
}

missing = [k for k, v in required_vars.items() if not v]
if missing:
    raise RuntimeError(f"Faltan variables de entorno: {', '.join(missing)}")

bot = Bot(token=BOT_TOKEN)

# =============================================================================
# ESTADO EN MEMORIA
# =============================================================================

sent_live_signals = set()
sent_prematch_signals = set()
sent_parley_signals = set()
sent_upcoming_match_alerts = set()

odds_credits_alert_sent = False
last_fixtures_found_alert_date = None
last_no_fixtures_alert_date = None

# =============================================================================
# LIGAS PERMITIDAS
# =============================================================================

ODDS_SPORT_KEYS = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_uefa_champs_league",
]

API_FOOTBALL_LEAGUES = {
    262: "Liga MX",
    39: "Premier League",
    2: "Champions League",
}

# =============================================================================
# UTILIDADES
# =============================================================================

def now_local():
    return datetime.now(LOCAL_TZ)

def to_local(dt_str: str):
    """
    Convierte ISO UTC a hora local.
    """
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(LOCAL_TZ)
    except Exception:
        return None

def parse_minute(text):
    if text is None:
        return None
    txt = str(text).strip()
    m = re.search(r"(\d+)", txt)
    if m:
        return int(m.group(1))
    return None

def normalize_team(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().lower())

def fixture_key(home: str, away: str, kickoff_local: datetime | None):
    base = f"{normalize_team(home)}|{normalize_team(away)}"
    if kickoff_local:
        base += "|" + kickoff_local.strftime("%Y-%m-%d %H:%M")
    return base

def save_state():
    try:
        with open(DB_PATH, "w", encoding="utf-8") as f:
            for item in sent_live_signals:
                f.write(f"LIVE::{item}\n")
            for item in sent_prematch_signals:
                f.write(f"PRE::{item}\n")
            for item in sent_parley_signals:
                f.write(f"PAR::{item}\n")
    except Exception as e:
        logging.warning(f"No se pudo guardar estado: {e}")

def load_state():
    if not os.path.exists(DB_PATH):
        return
    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("LIVE::"):
                    sent_live_signals.add(line.replace("LIVE::", "", 1))
                elif line.startswith("PRE::"):
                    sent_prematch_signals.add(line.replace("PRE::", "", 1))
                elif line.startswith("PAR::"):
                    sent_parley_signals.add(line.replace("PAR::", "", 1))
    except Exception as e:
        logging.warning(f"No se pudo cargar estado: {e}")

def implied_prob_from_decimal(odds: float):
    if not odds or odds <= 1:
        return None
    return 1 / odds

def decimal_from_prob(prob: float):
    if not prob or prob <= 0:
        return None
    return 1 / prob

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

async def safe_send_message(bot_obj, chat_id, text):
    try:
        await bot_obj.send_message(chat_id=chat_id, text=text)
    except TelegramError as e:
        logging.error(f"Error enviando Telegram: {e}")

def translate_reason(reason_code: str, home: str, away: str, score_home: int, score_away: int):
    reasons = {
        "late_draw_0_0": f"Empate 0-0 muy avanzado entre {home} y {away}. Puede haber valor en mercado de gol tardío.",
        "one_goal_margin_late": f"Partido cerrado con diferencia de un gol entre {home} y {away}. Puede haber valor en siguientes mercados.",
        "red_card_pressure": f"Se detectó tarjeta roja en el juego {home} vs {away}. Esto puede cambiar mucho el valor del partido.",
        "late_draw_any": f"Empate avanzado en {home} vs {away}. Posible oportunidad en gol tardío o siguiente gol.",
    }
    return reasons.get(reason_code, "Se detectó una condición interesante en vivo.")

def format_match_time(local_dt: datetime | None):
    if not local_dt:
        return "Hora no disponible"
    return local_dt.strftime("%d/%m %I:%M %p")

# =============================================================================
# HTTP CLIENT
# =============================================================================

async def get_json(url: str, headers=None, params=None):
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json(), resp.headers

# =============================================================================
# ODDS API
# =============================================================================

async def get_odds_for_sport(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
    }

    if ODDS_BOOKMAKERS:
        params["bookmakers"] = ODDS_BOOKMAKERS

    data, headers = await get_json(url, params=params)
    return data, headers

async def check_odds_credits(headers):
    global odds_credits_alert_sent

    remaining = headers.get("x-requests-remaining") or headers.get("X-Requests-Remaining")
    used = headers.get("x-requests-used") or headers.get("X-Requests-Used")

    logging.info(f"Odds API créditos | usados={used} | restantes={remaining}")

    try:
        remaining_int = int(remaining)
    except Exception:
        return

    if remaining_int <= 0 and not odds_credits_alert_sent:
        await safe_send_message(
            bot,
            CHAT_ID,
            "⚠️ Tus créditos de The Odds API parecen haberse terminado. Revisa tu panel y cambia la API key si ya compraste más."
        )
        odds_credits_alert_sent = True

def extract_h2h_market(bookmakers):
    prices = {"home": [], "draw": [], "away": []}
    best_odds = {"home": None, "draw": None, "away": None}

    for bm in bookmakers or []:
        for market in bm.get("markets", []):
            if market.get("key") != "h2h":
                continue

            outcomes = market.get("outcomes", [])
            for oc in outcomes:
                name = (oc.get("name") or "").strip().lower()
                price = safe_float(oc.get("price"))
                if not price:
                    continue

                if name in ["draw", "empate"]:
                    prices["draw"].append(price)
                    if best_odds["draw"] is None or price > best_odds["draw"]:
                        best_odds["draw"] = price
                else:
                    # home/away se definen luego por nombre exacto
                    pass

    return prices, best_odds

def analyze_match_value(match):
    home = match.get("home_team")
    away = match.get("away_team")
    bookmakers = match.get("bookmakers", [])

    fair_probs = {"home": [], "draw": [], "away": []}
    best_odds = {"home": None, "draw": None, "away": None}

    for bm in bookmakers:
        for market in bm.get("markets", []):
            if market.get("key") != "h2h":
                continue

            for oc in market.get("outcomes", []):
                name = (oc.get("name") or "").strip()
                price = safe_float(oc.get("price"))
                if not price:
                    continue

                prob = implied_prob_from_decimal(price)
                if prob is None:
                    continue

                low = name.lower()
                if low == home.lower():
                    fair_probs["home"].append(prob)
                    if best_odds["home"] is None or price > best_odds["home"]:
                        best_odds["home"] = price
                elif low == away.lower():
                    fair_probs["away"].append(prob)
                    if best_odds["away"] is None or price > best_odds["away"]:
                        best_odds["away"] = price
                elif low in ["draw", "empate"]:
                    fair_probs["draw"].append(prob)
                    if best_odds["draw"] is None or price > best_odds["draw"]:
                        best_odds["draw"] = price

    candidates = []
    for side in ["home", "draw", "away"]:
        if not fair_probs[side] or not best_odds[side]:
            continue

        avg_prob = mean(fair_probs[side])
        fair_odds = decimal_from_prob(avg_prob)
        offered = best_odds[side]

        if not fair_odds or not offered:
            continue

        edge = (offered / fair_odds) - 1

        candidates.append({
            "side": side,
            "edge": edge,
            "fair_odds": round(fair_odds, 2),
            "offered_odds": round(offered, 2),
        })

    if not candidates:
        return None

    best = max(candidates, key=lambda x: x["edge"])
    if best["edge"] < MIN_VALUE_EDGE:
        return None

    label = {
        "home": f"Gana {home}",
        "draw": "Empate",
        "away": f"Gana {away}",
    }[best["side"]]

    return {
        "pick": label,
        "edge": best["edge"],
        "fair_odds": best["fair_odds"],
        "offered_odds": best["offered_odds"],
    }

def format_prematch_signal(match, analysis):
    local_dt = to_local(match.get("commence_time"))
    home = match.get("home_team")
    away = match.get("away_team")
    sport_title = match.get("sport_title", "Fútbol")

    vip = "💎 VIP SIGNAL" if analysis["edge"] >= 0.10 else "📊 BETTING SIGNAL"

    text = (
        f"{vip}\n\n"
        f"🏆 Liga: {sport_title}\n"
        f"⚽ Partido: {home} vs {away}\n"
        f"🕒 Hora: {format_match_time(local_dt)}\n"
        f"🎯 Pick: {analysis['pick']}\n"
        f"📈 Cuota encontrada: {analysis['offered_odds']}\n"
        f"📉 Cuota justa estimada: {analysis['fair_odds']}\n"
        f"🔥 Edge estimado: {round(analysis['edge'] * 100, 1)}%\n"
    )
    return text

# =============================================================================
# PARLAY
# =============================================================================

def build_parlay_candidates(matches):
    """
    Parlay simple usando solo H2H por ahora.
    """
    legs = []

    for match in matches:
        analysis = analyze_match_value(match)
        if not analysis:
            continue

        odd = analysis["offered_odds"]
        if odd < 1.45 or odd > 1.95:
            continue

        home = match.get("home_team")
        away = match.get("away_team")
        local_dt = to_local(match.get("commence_time"))

        legs.append({
            "fixture_id": fixture_key(home, away, local_dt),
            "match": f"{home} vs {away}",
            "pick": analysis["pick"],
            "odds": odd,
            "time": format_match_time(local_dt),
        })

    legs = sorted(legs, key=lambda x: x["odds"])
    if len(legs) < 2:
        return None

    selected = []
    combined = 1.0

    for leg in legs:
        if len(selected) >= 3:
            break
        projected = combined * leg["odds"]
        if projected <= 4.50:
            selected.append(leg)
            combined = projected

    if len(selected) < 2:
        return None

    if combined < 2.00 or combined > 4.50:
        return None

    return {
        "legs": selected,
        "combined_odds": round(combined, 2),
    }

def format_parlay_signal(parlay):
    legs_text = []
    for i, leg in enumerate(parlay["legs"], start=1):
        legs_text.append(
            f"{i}. {leg['match']}\n"
            f"   🎯 {leg['pick']} @ {leg['odds']}\n"
            f"   🕒 {leg['time']}"
        )

    return (
        f"🔥 PARLAY DEL DÍA\n\n"
        f"{chr(10).join(legs_text)}\n\n"
        f"💰 Cuota combinada: {parlay['combined_odds']}"
    )

# =============================================================================
# API FOOTBALL
# =============================================================================

API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

async def api_football_get(path: str, params=None):
    headers = {
        "x-apisports-key": API_FOOTBALL_KEY
    }
    url = f"{API_FOOTBALL_BASE}{path}"
    data, _ = await get_json(url, headers=headers, params=params)
    return data

async def get_today_fixtures():
    today = now_local().strftime("%Y-%m-%d")
    fixtures = []

    for league_id, league_name in API_FOOTBALL_LEAGUES.items():
        params = {
            "league": league_id,
            "date": today,
            "timezone": "America/Mexico_City",
        }
        try:
            data = await api_football_get("/fixtures", params=params)
            for item in data.get("response", []):
                fixture = item.get("fixture", {})
                teams = item.get("teams", {})
                status = fixture.get("status", {})

                local_dt = to_local(fixture.get("date"))
                fixtures.append({
                    "fixture_id": fixture.get("id"),
                    "league_id": league_id,
                    "league_name": league_name,
                    "home": teams.get("home", {}).get("name"),
                    "away": teams.get("away", {}).get("name"),
                    "date_local": local_dt,
                    "status_short": status.get("short"),
                })
        except Exception as e:
            logging.warning(f"No se pudieron obtener fixtures de {league_name}: {e}")

    return fixtures

async def get_live_matches():
    live_matches = []

    for league_id, league_name in API_FOOTBALL_LEAGUES.items():
        params = {
            "live": "all",
            "league": league_id,
            "timezone": "America/Mexico_City",
        }
        try:
            data = await api_football_get("/fixtures", params=params)
            for item in data.get("response", []):
                fixture = item.get("fixture", {})
                teams = item.get("teams", {})
                goals = item.get("goals", {})
                status = fixture.get("status", {})
                score = item.get("score", {})

                live_matches.append({
                    "fixture_id": fixture.get("id"),
                    "league_name": league_name,
                    "home": teams.get("home", {}).get("name"),
                    "away": teams.get("away", {}).get("name"),
                    "home_goals": goals.get("home", 0) or 0,
                    "away_goals": goals.get("away", 0) or 0,
                    "minute": status.get("elapsed"),
                    "status_short": status.get("short"),
                    "score_raw": score,
                })
        except Exception as e:
            logging.warning(f"No se pudieron obtener partidos en vivo de {league_name}: {e}")

    return live_matches

async def get_fixture_events(fixture_id: int):
    try:
        data = await api_football_get("/fixtures/events", params={"fixture": fixture_id})
        return data.get("response", [])
    except Exception as e:
        logging.warning(f"No se pudieron obtener eventos del fixture {fixture_id}: {e}")
        return []

# =============================================================================
# LÓGICA DE SEÑALES EN VIVO
# =============================================================================

async def analyze_live_match(match):
    fixture_id = match["fixture_id"]
    home = match["home"]
    away = match["away"]
    hg = int(match["home_goals"])
    ag = int(match["away_goals"])
    minute = match["minute"] or 0

    if minute < LIVE_MINUTE_THRESHOLD:
        return []

    signals = []

    # 0-0 tardío
    if hg == 0 and ag == 0 and minute >= LIVE_DRAW_ONLY_MINUTE:
        signals.append("late_draw_0_0")

    # empate tardío en general
    if hg == ag and minute >= LIVE_DRAW_ONLY_MINUTE:
        signals.append("late_draw_any")

    # diferencia de un gol avanzada
    if abs(hg - ag) == 1 and minute >= LIVE_MINUTE_THRESHOLD:
        signals.append("one_goal_margin_late")

    # revisar roja
    events = await get_fixture_events(fixture_id)
    red_found = False
    for ev in events:
        detail = (ev.get("detail") or "").lower()
        typ = (ev.get("type") or "").lower()
        if "red card" in detail or "red card" in typ or "tarjeta roja" in detail:
            red_found = True
            break
    if red_found and minute >= LIVE_MINUTE_THRESHOLD:
        signals.append("red_card_pressure")

    return signals

def format_live_signal(match, reason_code):
    home = match["home"]
    away = match["away"]
    hg = match["home_goals"]
    ag = match["away_goals"]
    minute = match["minute"]
    league = match["league_name"]

    reason_text = translate_reason(reason_code, home, away, hg, ag)

    vip = "💎 SEÑAL VIP EN VIVO" if reason_code in ["red_card_pressure", "late_draw_0_0"] else "🔴 SEÑAL EN VIVO"

    return (
        f"{vip}\n\n"
        f"🏆 Liga: {league}\n"
        f"⚽ Partido: {home} vs {away}\n"
        f"⏱ Minuto: {minute}'\n"
        f"📊 Marcador: {home} {hg}-{ag} {away}\n"
        f"🧠 Motivo: {reason_text}"
    )

# =============================================================================
# CICLOS
# =============================================================================

async def process_prematch():
    if not ENABLE_PREMATCH:
        return []

    all_matches = []

    for sport_key in ODDS_SPORT_KEYS:
        try:
            matches, headers = await get_odds_for_sport(sport_key)
            await check_odds_credits(headers)
            all_matches.extend(matches)
        except Exception as e:
            logging.warning(f"Error obteniendo odds de {sport_key}: {e}")

    sent_messages = []

    now_dt = now_local()
    limit_dt = now_dt + timedelta(hours=PRE_MATCH_WINDOW_HOURS)

    for match in all_matches:
        local_dt = to_local(match.get("commence_time"))
        if not local_dt:
            continue

        if not (now_dt <= local_dt <= limit_dt):
            continue

        home = match.get("home_team")
        away = match.get("away_team")
        key = fixture_key(home, away, local_dt)

        analysis = analyze_match_value(match)
        if not analysis:
            continue

        dedupe_key = f"{key}|{analysis['pick']}"
        if dedupe_key in sent_prematch_signals:
            continue

        text = format_prematch_signal(match, analysis)
        await safe_send_message(bot, CHAT_ID, text)
        sent_prematch_signals.add(dedupe_key)
        sent_messages.append(text)

    return all_matches

async def process_parlay(all_matches):
    if not ENABLE_PARLAY:
        return

    parlay = build_parlay_candidates(all_matches)
    if not parlay:
        return

    parlay_key = "|".join([f"{x['fixture_id']}::{x['pick']}" for x in parlay["legs"]])
    if parlay_key in sent_parley_signals:
        return

    text = format_parlay_signal(parlay)
    await safe_send_message(bot, CHAT_ID, text)
    sent_parley_signals.add(parlay_key)

async def process_live():
    if not ENABLE_LIVE:
        return

    live_matches = await get_live_matches()

    for match in live_matches:
        try:
            signals = await analyze_live_match(match)
            for reason_code in signals:
                dedupe_key = f"{match['fixture_id']}|{reason_code}"
                if dedupe_key in sent_live_signals:
                    continue

                text = format_live_signal(match, reason_code)
                await safe_send_message(bot, CHAT_ID, text)
                sent_live_signals.add(dedupe_key)
        except Exception as e:
            logging.warning(f"Error analizando partido en vivo {match.get('fixture_id')}: {e}")

async def send_daily_fixture_status(fixtures):
    global last_fixtures_found_alert_date, last_no_fixtures_alert_date

    today_str = now_local().strftime("%Y-%m-%d")

    if not fixtures:
        if last_no_fixtures_alert_date != today_str:
            await safe_send_message(
                bot,
                CHAT_ID,
                "📭 Hoy no encontré partidos de tus ligas configuradas. El bot dormirá hasta mañana para ahorrar créditos."
            )
            last_no_fixtures_alert_date = today_str
        return

    if last_fixtures_found_alert_date != today_str:
        leagues_found = sorted(list({f.get("league_name", "Liga") for f in fixtures}))
        leagues_text = ", ".join(leagues_found) if leagues_found else "tus ligas configuradas"

        await safe_send_message(
            bot,
            CHAT_ID,
            f"📅 Encontré partidos para hoy en {leagues_text}. El bot queda atento para mandarte señales y parleys."
        )
        last_fixtures_found_alert_date = today_str

async def run_cycle():
    now_dt = now_local()
    current_hour = now_dt.hour

    # Antes de las 7 AM -> dormir
    if current_hour < ACTIVE_START_HOUR:
        logging.info("Fuera de horario. Esperando a las 7 AM.")
        return True

    # 10 PM o más -> dormir hasta mañana
    if current_hour >= ACTIVE_END_HOUR:
        logging.info("Después de las 10 PM. Durmiendo hasta mañana.")
        return True

    fixtures = await get_today_fixtures()
    await send_daily_fixture_status(fixtures)

    if not fixtures:
        return True

    all_matches = await process_prematch()
    await process_parlay(all_matches)
    await process_live()

    save_state()
    return False

def seconds_until_next_active_window():
    now_dt = now_local()

    if now_dt.hour >= ACTIVE_END_HOUR:
        target = (now_dt + timedelta(days=1)).replace(
            hour=ACTIVE_START_HOUR, minute=0, second=0, microsecond=0
        )
    elif now_dt.hour < ACTIVE_START_HOUR:
        target = now_dt.replace(
            hour=ACTIVE_START_HOUR, minute=0, second=0, microsecond=0
        )
    else:
        return CYCLE_INTERVAL

    return max(60, int((target - now_dt).total_seconds()))

# =============================================================================
# MAIN
# =============================================================================

async def main():
    load_state()
    logging.info("Iniciando ciclo V13.3 PRO...")

    while True:
        try:
            should_sleep_until_tomorrow = await run_cycle()

            if should_sleep_until_tomorrow:
                sleep_seconds = seconds_until_next_active_window()
                logging.info(f"Durmiendo {sleep_seconds} segundos hasta siguiente ventana activa.")
                await asyncio.sleep(sleep_seconds)
            else:
                logging.info(f"Esperando {CYCLE_INTERVAL} segundos para el siguiente ciclo.")
                await asyncio.sleep(CYCLE_INTERVAL)

        except Exception as e:
            logging.exception(f"Error en loop principal: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
