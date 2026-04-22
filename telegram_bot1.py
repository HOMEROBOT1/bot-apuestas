import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

TIMEZONE = "America/Mexico_City"
LOCAL_TZ = ZoneInfo(TIMEZONE)

CYCLE_INTERVAL = int(os.getenv("CYCLE_INTERVAL", "900"))  # 15 min
WORKDAY_START_HOUR = int(os.getenv("WORKDAY_START_HOUR", "7"))
WORKDAY_END_HOUR = int(os.getenv("WORKDAY_END_HOUR", "22"))

PREMATCH_ALERT_MINUTES = int(os.getenv("PREMATCH_ALERT_MINUTES", "10"))
PREMATCH_WINDOW_HOURS = int(os.getenv("PREMATCH_WINDOW_HOURS", "24"))

MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.06"))

ODDS_REGIONS = os.getenv("ODDS_REGIONS", "uk")
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h,totals,btts")

SPORT_KEYS = [
    "soccer_epl",
    "soccer_uefa_champs_league",
    "soccer_mexico_ligamx",
]

API_FOOTBALL_LEAGUES = {
    39: "Premier League",
    2: "Champions League",
    262: "Liga MX",
}

NO_FIXTURES_ALERT_HOUR = int(os.getenv("NO_FIXTURES_ALERT_HOUR", "7"))

# =========================================================
# RUNTIME STATE
# =========================================================

sent_prematch_signals = set()
sent_live_signals = set()
sent_upcoming_match_alerts = set()

no_fixtures_alert_sent_for_day = None
odds_credits_alert_sent_for_day = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# =========================================================
# HELPERS
# =========================================================

def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def is_working_hours() -> bool:
    current = now_local()
    return WORKDAY_START_HOUR <= current.hour < WORKDAY_END_HOUR

def today_local_str() -> str:
    return now_local().strftime("%Y-%m-%d")

def format_local_dt(dt_utc: datetime) -> str:
    return dt_utc.astimezone(LOCAL_TZ).strftime("%H:%M")

def parse_iso_datetime(dt_str: str) -> datetime:
    # Maneja formatos típicos con Z
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)

def confidence_and_stake(score: float):
    if score >= 0.80:
        return "Alta", "2/10"
    elif score >= 0.65:
        return "Media", "1/10"
    return "Baja", "0.5/10"

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def normalize_team(name: str) -> str:
    return (name or "").strip().lower()

def market_pick_key(match_id: str, bet: str, reason: str) -> str:
    return f"{match_id}|{bet}|{reason}"

def prematch_key(match_id: str) -> str:
    return f"prematch|{match_id}"

def parley_key(match_id: str) -> str:
    return f"parley|{match_id}"

def should_send_prematch(match_start_utc: datetime, current_utc: datetime) -> bool:
    diff = (match_start_utc - current_utc).total_seconds() / 60
    return 0 <= diff <= PREMATCH_ALERT_MINUTES

def is_today_local(dt_utc: datetime) -> bool:
    return dt_utc.astimezone(LOCAL_TZ).date() == now_local().date()

# =========================================================
# TELEGRAM
# =========================================================

async def send_telegram(bot: Bot, text: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
    except TelegramError as e:
        logging.exception("Error enviando Telegram: %s", e)

# =========================================================
# FORMATTERS
# =========================================================

def format_prematch_pick(signal: dict) -> str:
    return (
        "📊 PICK PREPARTIDO\n\n"
        f"🏆 {signal['league']}\n"
        f"⚽ {signal['home']} vs {signal['away']}\n"
        f"🕒 Inicio: {signal['start_time']}\n\n"
        f"🎯 Apuesta: {signal['bet']}\n"
        f"💰 Cuota mínima recomendada: {signal['min_odds']:.2f}\n"
        f"🔥 Confianza: {signal['confidence']}\n"
        f"📦 Stake: {signal['stake']}\n\n"
        f"🧠 Motivo: {signal['reason']}"
    )

def format_live_pick(signal: dict) -> str:
    return (
        "🔴 PICK EN VIVO\n\n"
        f"🏆 {signal['league']}\n"
        f"⚽ {signal['home']} vs {signal['away']}\n"
        f"⏱ Minuto: {signal['minute']}'\n"
        f"📊 Marcador: {signal['home']} {signal['home_goals']}-{signal['away_goals']} {signal['away']}\n\n"
        f"🎯 Apuesta: {signal['bet']}\n"
        f"💰 Cuota mínima recomendada: {signal['min_odds']:.2f}\n"
        f"🔥 Confianza: {signal['confidence']}\n"
        f"📦 Stake: {signal['stake']}\n\n"
        f"🧠 Motivo: {signal['reason']}"
    )

def format_parley_pick(signal: dict) -> str:
    legs_text = "\n".join([f"• {leg}" for leg in signal["legs"]])
    return (
        "💎 PARLEY PREPARTIDO\n\n"
        f"🏆 {signal['league']}\n"
        f"⚽ {signal['home']} vs {signal['away']}\n"
        f"🕒 Inicio: {signal['start_time']}\n\n"
        f"🧩 Selecciones:\n{legs_text}\n\n"
        f"💰 Cuota combinada estimada: {signal['combined_odds']:.2f}\n"
        f"🔥 Confianza: {signal['confidence']}\n"
        f"📦 Stake: {signal['stake']}\n\n"
        f"🧠 Motivo: {signal['reason']}"
    )

def format_no_fixtures_message() -> str:
    return (
        "📅 PARTIDOS DEL DÍA\n\n"
        "No encontré partidos hoy en tus ligas configuradas:\n"
        "• Premier League\n"
        "• Champions League\n"
        "• Liga MX\n\n"
        "😴 El bot seguirá en espera y volverá a revisar más tarde."
    )

def format_upcoming_match_message(match: dict) -> str:
    return (
        "📅 PRÓXIMO PARTIDO\n\n"
        f"🏆 {match['league']}\n"
        f"⚽ {match['home']} vs {match['away']}\n"
        f"🕒 Hora: {match['start_time_local']}\n\n"
        "👀 Partido detectado. Queda atento a picks prepartido y señales en vivo."
    )

def format_odds_credits_empty() -> str:
    return (
        "⚠️ ALERTA DE CRÉDITOS\n\n"
        "Parece que los créditos de The Odds API están agotados o muy bajos.\n"
        "Revisa tu panel para evitar que el bot se quede sin picks prepartido."
    )

# =========================================================
# ODDS API
# =========================================================

async def fetch_odds_for_sport(client: httpx.AsyncClient, sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
    }

    resp = await client.get(url, params=params, timeout=30.0)

    remaining = resp.headers.get("x-requests-remaining")
    used = resp.headers.get("x-requests-used")

    logging.info("Odds %s | status=%s | remaining=%s | used=%s", sport_key, resp.status_code, remaining, used)

    if resp.status_code == 401:
        logging.error("Odds API unauthorized. Revisa ODDS_API_KEY.")
        return [], {"remaining": 0, "used": used}

    if resp.status_code != 200:
        logging.error("Error Odds API %s: %s", resp.status_code, resp.text[:500])
        return [], {"remaining": safe_float(remaining) if remaining else None, "used": used}

    data = resp.json()
    return data, {"remaining": safe_float(remaining) if remaining else None, "used": used}

def extract_match_markets_from_odds_event(event: dict):
    home_team = event.get("home_team")
    away_team = event.get("away_team")
    commence_time = event.get("commence_time")
    event_id = event.get("id")

    market_data = {
        "id": event_id,
        "home": home_team,
        "away": away_team,
        "commence_time": commence_time,
        "h2h_home": None,
        "h2h_draw": None,
        "h2h_away": None,
        "over_2_5": None,
        "under_2_5": None,
        "btts_yes": None,
        "btts_no": None,
    }

    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return market_data

    h2h_home_prices = []
    h2h_draw_prices = []
    h2h_away_prices = []
    over25_prices = []
    under25_prices = []
    btts_yes_prices = []
    btts_no_prices = []

    for book in bookmakers:
        for market in book.get("markets", []):
            key = market.get("key")
            outcomes = market.get("outcomes", [])

            if key == "h2h":
                for outcome in outcomes:
                    name = outcome.get("name")
                    price = safe_float(outcome.get("price"))
                    if price is None:
                        continue
                    if name == home_team:
                        h2h_home_prices.append(price)
                    elif name == away_team:
                        h2h_away_prices.append(price)
                    elif name and name.lower() == "draw":
                        h2h_draw_prices.append(price)

            elif key == "totals":
                for outcome in outcomes:
                    name = outcome.get("name")
                    point = safe_float(outcome.get("point"))
                    price = safe_float(outcome.get("price"))
                    if price is None or point is None:
                        continue
                    if point == 2.5:
                        if name == "Over":
                            over25_prices.append(price)
                        elif name == "Under":
                            under25_prices.append(price)

            elif key == "btts":
                for outcome in outcomes:
                    name = (outcome.get("name") or "").strip().lower()
                    price = safe_float(outcome.get("price"))
                    if price is None:
                        continue
                    if name in ("yes", "sí", "si"):
                        btts_yes_prices.append(price)
                    elif name == "no":
                        btts_no_prices.append(price)

    if h2h_home_prices:
        market_data["h2h_home"] = max(h2h_home_prices)
    if h2h_draw_prices:
        market_data["h2h_draw"] = max(h2h_draw_prices)
    if h2h_away_prices:
        market_data["h2h_away"] = max(h2h_away_prices)
    if over25_prices:
        market_data["over_2_5"] = max(over25_prices)
    if under25_prices:
        market_data["under_2_5"] = max(under25_prices)
    if btts_yes_prices:
        market_data["btts_yes"] = max(btts_yes_prices)
    if btts_no_prices:
        market_data["btts_no"] = max(btts_no_prices)

    return market_data

async def fetch_all_odds(client: httpx.AsyncClient):
    all_markets = []
    remaining_values = []

    for sport_key in SPORT_KEYS:
        events, credits = await fetch_odds_for_sport(client, sport_key)
        if credits.get("remaining") is not None:
            remaining_values.append(credits["remaining"])
        for event in events:
            all_markets.append(extract_match_markets_from_odds_event(event))

    return all_markets, remaining_values

# =========================================================
# API-FOOTBALL
# =========================================================

async def api_football_get(client: httpx.AsyncClient, path: str, params: dict):
    url = f"https://v3.football.api-sports.io/{path}"
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    resp = await client.get(url, headers=headers, params=params, timeout=30.0)

    if resp.status_code != 200:
        logging.error("API-Football error %s on %s: %s", resp.status_code, path, resp.text[:500])
        return None

    data = resp.json()
    if not isinstance(data, dict):
        return None
    return data

async def fetch_today_fixtures(client: httpx.AsyncClient):
    results = []
    today = now_local().strftime("%Y-%m-%d")

    for league_id, league_name in API_FOOTBALL_LEAGUES.items():
        data = await api_football_get(
            client,
            "fixtures",
            {
                "league": league_id,
                "date": today,
                "timezone": TIMEZONE,
            },
        )
        if not data:
            continue

        for item in data.get("response", []):
            fixture = item.get("fixture", {})
            teams = item.get("teams", {})
            league = item.get("league", {})

            fixture_id = str(fixture.get("id"))
            date_str = fixture.get("date")
            if not fixture_id or not date_str:
                continue

            dt_utc = parse_iso_datetime(date_str)
            home = teams.get("home", {}).get("name")
            away = teams.get("away", {}).get("name")

            results.append({
                "id": fixture_id,
                "league": league.get("name") or league_name,
                "league_id": league_id,
                "home": home,
                "away": away,
                "start_time_utc": dt_utc,
                "start_time_local": format_local_dt(dt_utc),
            })

    return results

async def fetch_live_fixtures(client: httpx.AsyncClient):
    results = []

    for league_id, league_name in API_FOOTBALL_LEAGUES.items():
        data = await api_football_get(
            client,
            "fixtures",
            {
                "live": "all",
                "league": league_id,
                "timezone": TIMEZONE,
            },
        )
        if not data:
            continue

        for item in data.get("response", []):
            fixture = item.get("fixture", {})
            teams = item.get("teams", {})
            goals = item.get("goals", {})
            league = item.get("league", {})
            score = item.get("score", {})
            events = item.get("events", [])

            minute = fixture.get("status", {}).get("elapsed")
            if minute is None:
                continue

            yellow_cards = 0
            red_cards = 0

            for event in events:
                detail = (event.get("detail") or "").lower()
                if "yellow" in detail:
                    yellow_cards += 1
                if "red" in detail:
                    red_cards += 1

            results.append({
                "id": str(fixture.get("id")),
                "league": league.get("name") or league_name,
                "home": teams.get("home", {}).get("name"),
                "away": teams.get("away", {}).get("name"),
                "minute": int(minute),
                "home_goals": int(goals.get("home") or 0),
                "away_goals": int(goals.get("away") or 0),
                "yellow_cards": yellow_cards,
                "red_cards": red_cards,
                "score_data": score,
            })

    return results

# =========================================================
# MATCHING ODDS WITH FIXTURES
# =========================================================

def odds_match_score(fixture: dict, odds_market: dict) -> int:
    score = 0

    if normalize_team(fixture["home"]) == normalize_team(odds_market["home"]):
        score += 1
    if normalize_team(fixture["away"]) == normalize_team(odds_market["away"]):
        score += 1

    try:
        odds_dt = parse_iso_datetime(odds_market["commence_time"])
        diff_mins = abs((fixture["start_time_utc"] - odds_dt).total_seconds()) / 60
        if diff_mins <= 30:
            score += 1
    except Exception:
        pass

    return score

def map_odds_to_fixtures(fixtures: list, odds_markets: list):
    by_fixture_id = {}

    for fixture in fixtures:
        best = None
        best_score = -1

        for market in odds_markets:
            s = odds_match_score(fixture, market)
            if s > best_score:
                best_score = s
                best = market

        if best and best_score >= 2:
            by_fixture_id[fixture["id"]] = best

    return by_fixture_id

# =========================================================
# PREMATCH LOGIC
# =========================================================

def implied_probability(decimal_odds: float) -> float:
    if not decimal_odds or decimal_odds <= 1:
        return 0.0
    return 1 / decimal_odds

def build_prematch_pick(match: dict, market_data: dict):
    league = match["league"]
    home = match["home"]
    away = match["away"]
    start_time = match["start_time_local"]

    h2h_home = market_data.get("h2h_home")
    h2h_draw = market_data.get("h2h_draw")
    h2h_away = market_data.get("h2h_away")
    over25 = market_data.get("over_2_5")
    under25 = market_data.get("under_2_5")
    btts_yes = market_data.get("btts_yes")
    btts_no = market_data.get("btts_no")

    candidates = []

    if over25 and 1.55 <= over25 <= 2.10:
        fair_prob = implied_probability(over25)
        edge_score = 0.68 + max(0.0, fair_prob - 0.50)
        candidates.append({
            "bet": "Over 2.5 goles",
            "min_odds": over25,
            "score": edge_score,
            "reason": "Línea de goles con cuota dentro del rango objetivo y perfil abierto.",
        })

    if btts_yes and 1.60 <= btts_yes <= 2.05:
        fair_prob = implied_probability(btts_yes)
        edge_score = 0.66 + max(0.0, fair_prob - 0.48)
        candidates.append({
            "bet": "Ambos anotan: Sí",
            "min_odds": btts_yes,
            "score": edge_score,
            "reason": "Mercado de ambos anotan con cuota aprovechable.",
        })

    if h2h_home and 1.45 <= h2h_home <= 1.95:
        fair_prob = implied_probability(h2h_home)
        edge_score = 0.64 + max(0.0, fair_prob - 0.52)
        candidates.append({
            "bet": f"Gana {home}",
            "min_odds": h2h_home,
            "score": edge_score,
            "reason": f"{home} aparece con cuota competitiva en rango objetivo.",
        })

    if h2h_away and 1.45 <= h2h_away <= 1.95:
        fair_prob = implied_probability(h2h_away)
        edge_score = 0.64 + max(0.0, fair_prob - 0.52)
        candidates.append({
            "bet": f"Gana {away}",
            "min_odds": h2h_away,
            "score": edge_score,
            "reason": f"{away} aparece con cuota competitiva en rango objetivo.",
        })

    if under25 and 1.60 <= under25 <= 2.10:
        fair_prob = implied_probability(under25)
        edge_score = 0.61 + max(0.0, fair_prob - 0.50)
        candidates.append({
            "bet": "Under 2.5 goles",
            "min_odds": under25,
            "score": edge_score,
            "reason": "Línea de pocos goles con cuota aceptable para perfil más cerrado.",
        })

    if btts_no and 1.60 <= btts_no <= 2.05:
        fair_prob = implied_probability(btts_no)
        edge_score = 0.60 + max(0.0, fair_prob - 0.49)
        candidates.append({
            "bet": "Ambos anotan: No",
            "min_odds": btts_no,
            "score": edge_score,
            "reason": "Mercado negativo de ambos anotan con valor aceptable.",
        })

    if not candidates:
        return None

    best = max(candidates, key=lambda x: x["score"])
    confidence, stake = confidence_and_stake(best["score"])

    return {
        "league": league,
        "home": home,
        "away": away,
        "start_time": start_time,
        "bet": best["bet"],
        "min_odds": best["min_odds"],
        "confidence": confidence,
        "stake": stake,
        "reason": best["reason"],
    }

def build_prematch_parley(match: dict, market_data: dict):
    home = match["home"]
    away = match["away"]

    legs = []
    combined = 1.0

    over25 = market_data.get("over_2_5")
    btts_yes = market_data.get("btts_yes")
    h2h_home = market_data.get("h2h_home")
    h2h_away = market_data.get("h2h_away")

    if h2h_home and 1.45 <= h2h_home <= 1.95:
        legs.append((f"Gana {home}", h2h_home))
    elif h2h_away and 1.45 <= h2h_away <= 1.95:
        legs.append((f"Gana {away}", h2h_away))

    if over25 and 1.55 <= over25 <= 2.10:
        legs.append(("Over 1.5/2.5 goles", over25))

    if btts_yes and 1.60 <= btts_yes <= 2.05 and len(legs) < 2:
        legs.append(("Ambos anotan: Sí", btts_yes))

    if len(legs) < 2:
        return None

    picked_legs = legs[:2]
    leg_texts = []

    for bet_name, odd in picked_legs:
        combined *= odd
        leg_texts.append(f"{bet_name} ({odd:.2f})")

    if not (2.00 <= combined <= 4.50):
        return None

    score = 0.69 if combined <= 3.20 else 0.64
    confidence, stake = confidence_and_stake(score)

    return {
        "league": match["league"],
        "home": match["home"],
        "away": match["away"],
        "start_time": match["start_time_local"],
        "legs": leg_texts,
        "combined_odds": combined,
        "confidence": confidence,
        "stake": stake,
        "reason": "Parley simple armado con dos selecciones dentro del rango objetivo.",
    }

# =========================================================
# LIVE LOGIC
# =========================================================

def build_live_pick(match: dict):
    league = match["league"]
    home = match["home"]
    away = match["away"]
    minute = match["minute"]
    home_goals = match["home_goals"]
    away_goals = match["away_goals"]
    total_goals = home_goals + away_goals
    goal_diff = abs(home_goals - away_goals)
    yellow_cards = match.get("yellow_cards", 0)
    red_cards = match.get("red_cards", 0)

    if minute >= 65 and goal_diff == 1 and total_goals >= 2:
        score = 0.67
        confidence, stake = confidence_and_stake(score)
        return {
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "bet": f"Over {total_goals + 0.5} goles live",
            "min_odds": 1.80,
            "confidence": confidence,
            "stake": stake,
            "reason": "Diferencia de un gol en tramo avanzado y ritmo favorable para otro gol.",
        }

    if minute >= 70 and total_goals == 0:
        score = 0.61
        confidence, stake = confidence_and_stake(score)
        return {
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "bet": "Over 0.5 goles live",
            "min_odds": 1.70,
            "confidence": confidence,
            "stake": stake,
            "reason": "Empate sin goles en tramo avanzado, escenario de gol tardío.",
        }

    if minute >= 55 and yellow_cards >= 5:
        score = 0.60
        confidence, stake = confidence_and_stake(score)
        return {
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "bet": "Over tarjetas live",
            "min_odds": 1.75,
            "confidence": confidence,
            "stake": stake,
            "reason": f"Partido caliente con {yellow_cards} amarillas.",
        }

    if minute >= 50 and red_cards >= 1:
        score = 0.72
        confidence, stake = confidence_and_stake(score)
        return {
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "bet": "Siguiente gol del equipo con superioridad numérica",
            "min_odds": 1.85,
            "confidence": confidence,
            "stake": stake,
            "reason": "Tarjeta roja detectada, cambia fuerte la dinámica del partido.",
        }

    return None

# =========================================================
# SCAN + SEND
# =========================================================

async def alert_if_odds_credits_empty(bot: Bot, remaining_values: list):
    global odds_credits_alert_sent_for_day

    if not remaining_values:
        return

    today = today_local_str()
    min_remaining = min(remaining_values)

    if min_remaining <= 0 and odds_credits_alert_sent_for_day != today:
        await send_telegram(bot, format_odds_credits_empty())
        odds_credits_alert_sent_for_day = today

async def send_no_fixtures_if_needed(bot: Bot, fixtures: list):
    global no_fixtures_alert_sent_for_day

    now = now_local()
    today = today_local_str()

    if now.hour < NO_FIXTURES_ALERT_HOUR:
        return

    if no_fixtures_alert_sent_for_day == today:
        return

    if not fixtures:
        await send_telegram(bot, format_no_fixtures_message())
        no_fixtures_alert_sent_for_day = today

async def send_upcoming_match_alerts(bot: Bot, fixtures: list):
    current_utc = now_utc()

    for match in fixtures:
        if not is_today_local(match["start_time_utc"]):
            continue

        diff_hours = (match["start_time_utc"] - current_utc).total_seconds() / 3600
        if not (0 <= diff_hours <= PREMATCH_WINDOW_HOURS):
            continue

        key = f"upcoming|{match['id']}"
        if key in sent_upcoming_match_alerts:
            continue

        await send_telegram(bot, format_upcoming_match_message(match))
        sent_upcoming_match_alerts.add(key)

async def scan_prematch_and_send(bot: Bot, fixtures: list, odds_data_by_fixture: dict):
    current_utc = now_utc()

    for match in fixtures:
        if not is_today_local(match["start_time_utc"]):
            continue

        if not should_send_prematch(match["start_time_utc"], current_utc):
            continue

        key = prematch_key(match["id"])
        if key in sent_prematch_signals:
            continue

        market_data = odds_data_by_fixture.get(match["id"])
        if not market_data:
            continue

        signal = build_prematch_pick(match, market_data)
        if not signal:
            continue

        await send_telegram(bot, format_prematch_pick(signal))
        sent_prematch_signals.add(key)

        parley_signal = build_prematch_parley(match, market_data)
        if parley_signal:
            p_key = parley_key(match["id"])
            if p_key not in sent_prematch_signals:
                await send_telegram(bot, format_parley_pick(parley_signal))
                sent_prematch_signals.add(p_key)

async def scan_live_and_send(bot: Bot, live_matches: list):
    for match in live_matches:
        signal = build_live_pick(match)
        if not signal:
            continue

        key = market_pick_key(match["id"], signal["bet"], signal["reason"])
        if key in sent_live_signals:
            continue

        await send_telegram(bot, format_live_pick(signal))
        sent_live_signals.add(key)

# =========================================================
# MAIN LOOP
# =========================================================

async def run_cycle(bot: Bot):
    async with httpx.AsyncClient() as client:
        fixtures = await fetch_today_fixtures(client)
        odds_markets, remaining_values = await fetch_all_odds(client)
        odds_data_by_fixture = map_odds_to_fixtures(fixtures, odds_markets)

        await alert_if_odds_credits_empty(bot, remaining_values)
        await send_no_fixtures_if_needed(bot, fixtures)
        await send_upcoming_match_alerts(bot, fixtures)
        await scan_prematch_and_send(bot, fixtures, odds_data_by_fixture)

        live_matches = await fetch_live_fixtures(client)
        await scan_live_and_send(bot, live_matches)

async def main():
    if not BOT_TOKEN:
        raise ValueError("Falta BOT_TOKEN")
    if not CHAT_ID:
        raise ValueError("Falta CHAT_ID")
    if not ODDS_API_KEY:
        raise ValueError("Falta ODDS_API_KEY")
    if not API_FOOTBALL_KEY:
        raise ValueError("Falta API_FOOTBALL_KEY")

    bot = Bot(token=BOT_TOKEN)

    logging.info("Bot iniciado correctamente.")

    while True:
        try:
            if is_working_hours():
                logging.info("Ejecutando ciclo...")
                await run_cycle(bot)
            else:
                logging.info("Fuera de horario de trabajo. En espera...")
        except Exception as e:
            logging.exception("Error en ciclo principal: %s", e)

        await asyncio.sleep(CYCLE_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())
