import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =========================================================
# V15 PRO - FOOTBALL BETTING ALERTS BOT
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# =========================================================
# VARIABLES DE ENTORNO
# =========================================================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

TZ_NAME = os.getenv("TZ_NAME", "America/Mexico_City").strip()
LOCAL_TZ = ZoneInfo(TZ_NAME)

# =========================================================
# CONFIG
# =========================================================

CYCLE_INTERVAL = int(os.getenv("CYCLE_INTERVAL", "900"))  # 15 min
PRE_MATCH_WINDOW_HOURS = int(os.getenv("PRE_MATCH_WINDOW_HOURS", "24"))
MIN_VALUE_EDGE = float(os.getenv("MIN_VALUE_EDGE", "0.03"))
WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "7"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "22"))

ODDS_REGIONS = os.getenv("ODDS_REGIONS", "uk")
ODDS_MARKETS = os.getenv("ODDS_MARKETS", "h2h")
ODDS_BOOKMAKERS = os.getenv("ODDS_BOOKMAKERS", "").strip()

ENABLE_PREMATCH = os.getenv("ENABLE_PREMATCH", "true").lower() == "true"
ENABLE_LIVE = os.getenv("ENABLE_LIVE", "true").lower() == "true"
ENABLE_PARLAY = os.getenv("ENABLE_PARLAY", "true").lower() == "true"
ENABLE_DAILY_STATUS = os.getenv("ENABLE_DAILY_STATUS", "true").lower() == "true"

PARLAY_MIN_COMBINED = float(os.getenv("PARLAY_MIN_COMBINED", "2.00"))
PARLAY_MAX_COMBINED = float(os.getenv("PARLAY_MAX_COMBINED", "4.50"))

HTTP_TIMEOUT = 25.0

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

# =========================================================
# RUNTIME STATE
# =========================================================

bot = Bot(token=BOT_TOKEN)

sent_pre_signals = set()
sent_live_signals = set()
sent_parlay_signals = set()
sent_upcoming_alerts = set()
sent_no_fixture_days = set()
odds_credits_alert_sent = False

# =========================================================
# UTILS
# =========================================================

def now_local():
    return datetime.now(LOCAL_TZ)

def in_work_window() -> bool:
    n = now_local()
    return WORK_START_HOUR <= n.hour < WORK_END_HOUR

def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")

def safe_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def implied_probability(decimal_odds: float) -> float:
    if not decimal_odds or decimal_odds <= 1:
        return 0.0
    return 1.0 / decimal_odds

def format_local_time(dt_str: str) -> str:
    try:
        dt_utc = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt_utc.astimezone(LOCAL_TZ).strftime("%d/%m %I:%M %p")
    except Exception:
        return dt_str

def normalize_text(s: str) -> str:
    return (s or "").strip().lower()

async def send_telegram(text: str):
    if not CHAT_ID:
        logging.warning("CHAT_ID vacío, no se pudo enviar mensaje.")
        return
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
    except TelegramError as e:
        logging.error(f"Error enviando Telegram: {e}")

def get_env_or_raise():
    missing = []
    for name, value in {
        "BOT_TOKEN": BOT_TOKEN,
        "CHAT_ID": CHAT_ID,
        "ODDS_API_KEY": ODDS_API_KEY,
        "API_FOOTBALL_KEY": API_FOOTBALL_KEY,
    }.items():
        if not value:
            missing.append(name)
    if missing:
        raise RuntimeError(f"Faltan variables de entorno: {', '.join(missing)}")

# =========================================================
# TRADUCCIONES / TEXTOS
# =========================================================

def confidence_label(score: float) -> str:
    if score >= 8:
        return "Alta"
    if score >= 6:
        return "Media"
    return "Baja"

def stake_label(score: float) -> str:
    if score >= 8:
        return "3/10"
    if score >= 6:
        return "2/10"
    return "1/10"

def fmt_pre_match_signal(league, home, away, kickoff, pick, odds, edge, conf, stake, reason):
    vip = "💎 VIP SIGNAL" if conf == "Alta" else "📊 BETTING SIGNAL"
    return (
        f"{vip}\n\n"
        f"🏆 {league}\n"
        f"⚽ {home} vs {away}\n"
        f"🕒 Inicio: {kickoff}\n\n"
        f"🎯 Pick: {pick}\n"
        f"💰 Cuota: {odds:.2f}\n"
        f"📈 Edge estimado: {edge*100:.1f}%\n"
        f"🔥 Confianza: {conf}\n"
        f"📦 Stake: {stake}\n\n"
        f"🧠 Motivo: {reason}"
    )

def fmt_live_signal(league, home, away, minute, score, pick, min_odds, conf, stake, reason):
    vip = "💎 SEÑAL VIP EN VIVO" if conf == "Alta" else "🔴 PICK EN VIVO"
    return (
        f"{vip}\n\n"
        f"🏆 {league}\n"
        f"⚽ {home} vs {away}\n"
        f"⏱ Minuto: {minute}'\n"
        f"📊 Marcador: {score}\n\n"
        f"🎯 Apuesta: {pick}\n"
        f"💰 Cuota mínima recomendada: {min_odds:.2f}\n"
        f"🔥 Confianza: {conf}\n"
        f"📦 Stake: {stake}\n\n"
        f"🧠 Motivo: {reason}"
    )

def fmt_parlay_signal(legs, combined_odds):
    lines = ["🔥 PARLAY PRE-PARTIDO\n"]
    for i, leg in enumerate(legs, start=1):
        lines.append(
            f"{i}. 🏆 {leg['league']}\n"
            f"   ⚽ {leg['match']}\n"
            f"   🎯 {leg['pick']}\n"
            f"   💰 {leg['odds']:.2f}\n"
        )
    lines.append(f"💸 Cuota combinada estimada: {combined_odds:.2f}")
    lines.append("📦 Stake sugerido: 1/10")
    return "\n".join(lines)

# =========================================================
# REQUESTS
# =========================================================

async def fetch_odds_sport(client: httpx.AsyncClient, sport_key: str):
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_MARKETS,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    if ODDS_BOOKMAKERS:
        params["bookmakers"] = ODDS_BOOKMAKERS

    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/"
    resp = await client.get(url, params=params)
    credits_remaining = resp.headers.get("x-requests-remaining")
    credits_used = resp.headers.get("x-requests-used")
    return resp, credits_remaining, credits_used

async def fetch_live_fixtures(client: httpx.AsyncClient, league_id: int):
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"live": "all", "league": league_id}
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    resp = await client.get(url, params=params, headers=headers)
    return resp

async def fetch_today_fixtures(client: httpx.AsyncClient, league_id: int, date_str: str):
    url = "https://v3.football.api-sports.io/fixtures"
    params = {"league": league_id, "date": date_str, "timezone": TZ_NAME}
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    resp = await client.get(url, params=params, headers=headers)
    return resp

# =========================================================
# PREMATCH LOGIC
# =========================================================

def get_best_h2h_offer(event: dict):
    home = event.get("home_team")
    away = event.get("away_team")
    outcomes_map = {home: [], away: [], "Draw": []}

    for bookmaker in event.get("bookmakers", []):
        for market in bookmaker.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for outcome in market.get("outcomes", []):
                name = outcome.get("name")
                price = safe_float(outcome.get("price"))
                if name in outcomes_map and price:
                    outcomes_map[name].append(price)

    if not any(outcomes_map.values()):
        return None

    fair_probs = {}
    best_odds = {}

    for side, prices in outcomes_map.items():
        if prices:
            avg_imp = mean(implied_probability(p) for p in prices if p and p > 1)
            fair_probs[side] = avg_imp
            best_odds[side] = max(prices)

    if not fair_probs or not best_odds:
        return None

    edges = {}
    for side, fair_prob in fair_probs.items():
        offered = best_odds.get(side)
        if not offered:
            continue
        market_prob = implied_probability(offered)
        edge = fair_prob - market_prob
        edges[side] = edge

    if not edges:
        return None

    best_side = max(edges, key=edges.get)
    best_edge = edges[best_side]
    best_price = best_odds[best_side]

    return {
        "pick": best_side,
        "odds": best_price,
        "edge": best_edge,
    }

def prematch_score(edge: float, odds: float) -> float:
    score = 5.0
    score += min(max(edge * 100, 0), 6)
    if 1.50 <= odds <= 2.30:
        score += 1.2
    if edge >= 0.05:
        score += 1.0
    return min(score, 10.0)

def translate_pick(side: str, home: str, away: str) -> str:
    if side == home:
        return f"Gana {home}"
    if side == away:
        return f"Gana {away}"
    if normalize_text(side) == "draw":
        return "Empate"
    return side

def build_prematch_reason(side: str, edge: float, odds: float, home: str, away: str) -> str:
    translated = translate_pick(side, home, away)
    return f"Valor detectado para {translated} con cuota competitiva y edge positivo frente al promedio del mercado."

async def process_prematch():
    global odds_credits_alert_sent

    if not ENABLE_PREMATCH and not ENABLE_PARLAY:
        return

    now = now_local()
    pre_match_candidates = []

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for sport_key in SPORT_KEYS:
            try:
                resp, credits_remaining, credits_used = await fetch_odds_sport(client, sport_key)

                if credits_remaining is not None:
                    logging.info(f"{sport_key} | Odds remaining={credits_remaining} used={credits_used}")
                    try:
                        if int(credits_remaining) <= 0 and not odds_credits_alert_sent:
                            odds_credits_alert_sent = True
                            await send_telegram("🚨 Se acabaron los créditos de The Odds API.")
                    except Exception:
                        pass

                if resp.status_code != 200:
                    logging.warning(f"Odds API error {sport_key}: {resp.status_code} | {resp.text[:300]}")
                    continue

                events = resp.json()
                logging.info(f"{sport_key} | Partidos encontrados: {len(events)}")

                for event in events:
                    commence = event.get("commence_time")
                    if not commence:
                        continue

                    try:
                        kickoff_utc = datetime.fromisoformat(commence.replace("Z", "+00:00"))
                        kickoff_local = kickoff_utc.astimezone(LOCAL_TZ)
                    except Exception:
                        continue

                    hours_to_kickoff = (kickoff_local - now).total_seconds() / 3600
                    if hours_to_kickoff < 0 or hours_to_kickoff > PRE_MATCH_WINDOW_HOURS:
                        continue

                    result = get_best_h2h_offer(event)
                    if not result:
                        continue

                    if result["edge"] < MIN_VALUE_EDGE:
                        continue

                    home = event.get("home_team", "Local")
                    away = event.get("away_team", "Visitante")
                    league = event.get("sport_title", sport_key)
                    pick_text = translate_pick(result["pick"], home, away)
                    odds = result["odds"]
                    edge = result["edge"]

                    score = prematch_score(edge, odds)
                    conf = confidence_label(score)
                    stake = stake_label(score)
                    reason = build_prematch_reason(result["pick"], edge, odds, home, away)

                    signal_id = f"PRE|{sport_key}|{home}|{away}|{kickoff_local.isoformat()}|{pick_text}"
                    candidate = {
                        "signal_id": signal_id,
                        "league": league,
                        "home": home,
                        "away": away,
                        "kickoff": kickoff_local.strftime("%d/%m %I:%M %p"),
                        "pick": pick_text,
                        "odds": odds,
                        "edge": edge,
                        "conf": conf,
                        "stake": stake,
                        "reason": reason,
                        "score": score,
                        "match_text": f"{home} vs {away}",
                    }
                    pre_match_candidates.append(candidate)

            except Exception as e:
                logging.exception(f"Error process_prematch sport={sport_key}: {e}")

    # Enviar picks individuales
    if ENABLE_PREMATCH:
        pre_match_candidates.sort(key=lambda x: x["score"], reverse=True)
        for c in pre_match_candidates[:5]:
            if c["signal_id"] in sent_pre_signals:
                continue
            msg = fmt_pre_match_signal(
                c["league"], c["home"], c["away"], c["kickoff"],
                c["pick"], c["odds"], c["edge"], c["conf"], c["stake"], c["reason"]
            )
            await send_telegram(msg)
            sent_pre_signals.add(c["signal_id"])

    # Enviar parley
    if ENABLE_PARLAY:
        usable = []
        used_matches = set()

        for c in sorted(pre_match_candidates, key=lambda x: x["score"], reverse=True):
            if c["match_text"] in used_matches:
                continue
            if 1.45 <= c["odds"] <= 2.10:
                usable.append(c)
                used_matches.add(c["match_text"])
            if len(usable) >= 3:
                break

        if len(usable) >= 2:
            combined = 1.0
            legs = []
            for u in usable:
                combined *= u["odds"]
                legs.append({
                    "league": u["league"],
                    "match": u["match_text"],
                    "pick": u["pick"],
                    "odds": u["odds"],
                })

            if PARLAY_MIN_COMBINED <= combined <= PARLAY_MAX_COMBINED:
                parlay_id = "PARLAY|" + "|".join(f"{l['match']}|{l['pick']}" for l in legs)
                if parlay_id not in sent_parlay_signals:
                    await send_telegram(fmt_parlay_signal(legs, combined))
                    sent_parlay_signals.add(parlay_id)

# =========================================================
# LIVE LOGIC
# =========================================================

def evaluate_live_signal(fixture: dict):
    fixture_info = fixture.get("fixture", {})
    teams = fixture.get("teams", {})
    goals = fixture.get("goals", {})
    score_info = fixture.get("score", {})
    league_info = fixture.get("league", {})

    home = teams.get("home", {}).get("name", "Local")
    away = teams.get("away", {}).get("name", "Visitante")
    league = league_info.get("name", "Liga")

    status_elapsed = fixture_info.get("status", {}).get("elapsed")
    minute = int(status_elapsed or 0)

    home_goals = goals.get("home", 0) or 0
    away_goals = goals.get("away", 0) or 0
    total_goals = home_goals + away_goals
    diff = abs(home_goals - away_goals)

    score = f"{home} {home_goals}-{away_goals} {away}"

    # Regla 1: 0-0 tarde
    if minute >= 70 and total_goals == 0:
        return {
            "pick": "Under 0.5 goles live",
            "min_odds": 1.70,
            "reason": "Sigue 0-0 en tramo avanzado del partido.",
            "confidence_score": 7.0,
            "signal_key": f"late00_{minute//5}",
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "score": score,
        }

    # Regla 2: un gol de diferencia en tramo avanzado
    if minute >= 60 and diff == 1 and total_goals >= 2:
        return {
            "pick": "Over 3.5 goles live",
            "min_odds": 1.80,
            "reason": "Diferencia de un gol en tramo avanzado y ritmo favorable para otro gol.",
            "confidence_score": 6.5,
            "signal_key": f"onegoal_{minute//5}",
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "score": score,
        }

    # Regla 3: 1-1 o 2-2 pasada la hora
    if minute >= 60 and home_goals == away_goals and total_goals >= 2:
        return {
            "pick": "Over 2.5 goles live",
            "min_odds": 1.60,
            "reason": "Empate con goles y partido abierto en segunda mitad.",
            "confidence_score": 7.5,
            "signal_key": f"drawgoals_{minute//5}",
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "score": score,
        }

    # Regla 4: roja + empate o un gol
    events = fixture.get("events", [])
    red_cards = 0
    for ev in events:
        detail = normalize_text(ev.get("detail", ""))
        if "red card" in detail or "tarjeta roja" in detail:
            red_cards += 1

    if minute >= 55 and red_cards >= 1 and (diff <= 1):
        return {
            "pick": "Próximo gol en el partido",
            "min_odds": 1.65,
            "reason": "Hay tarjeta roja y el juego sigue cerrado; suele abrir espacios.",
            "confidence_score": 8.0,
            "signal_key": f"redcard_{minute//5}",
            "league": league,
            "home": home,
            "away": away,
            "minute": minute,
            "score": score,
        }

    return None

async def process_live():
    if not ENABLE_LIVE:
        return

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for league_id, league_name in API_FOOTBALL_LEAGUES.items():
            try:
                resp = await fetch_live_fixtures(client, league_id)
                if resp.status_code != 200:
                    logging.warning(f"API-Football live {league_name}: {resp.status_code} | {resp.text[:250]}")
                    continue

                data = resp.json()
                fixtures = data.get("response", [])
                logging.info(f"{league_name} | Live fixtures: {len(fixtures)}")

                for fixture in fixtures:
                    signal = evaluate_live_signal(fixture)
                    if not signal:
                        continue

                    fixture_id = fixture.get("fixture", {}).get("id")
                    if not fixture_id:
                        continue

                    dedupe_id = f"LIVE|{fixture_id}|{signal['pick']}|{signal['signal_key']}"
                    if dedupe_id in sent_live_signals:
                        continue

                    conf = confidence_label(signal["confidence_score"])
                    stake = stake_label(signal["confidence_score"])

                    msg = fmt_live_signal(
                        signal["league"],
                        signal["home"],
                        signal["away"],
                        signal["minute"],
                        signal["score"],
                        signal["pick"],
                        signal["min_odds"],
                        conf,
                        stake,
                        signal["reason"]
                    )

                    await send_telegram(msg)
                    sent_live_signals.add(dedupe_id)

            except Exception as e:
                logging.exception(f"Error process_live league={league_name}: {e}")

# =========================================================
# DAILY STATUS
# =========================================================

async def process_daily_status():
    if not ENABLE_DAILY_STATUS:
        return

    now = now_local()
    current_day = now.strftime("%Y-%m-%d")

    # Solo avisar una vez por día, entre 7:00 y 8:00 aprox
    if now.hour != WORK_START_HOUR:
        return

    day_alert_key = f"DAILY-{current_day}"
    if day_alert_key in sent_upcoming_alerts:
        return

    all_matches = []

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
        for league_id, league_name in API_FOOTBALL_LEAGUES.items():
            try:
                resp = await fetch_today_fixtures(client, league_id, current_day)
                if resp.status_code != 200:
                    logging.warning(f"Daily fixtures {league_name}: {resp.status_code}")
                    continue

                data = resp.json()
                fixtures = data.get("response", [])

                for f in fixtures:
                    fixture_info = f.get("fixture", {})
                    teams = f.get("teams", {})
                    status_short = normalize_text(fixture_info.get("status", {}).get("short", ""))

                    # Solo próximos o no iniciados
                    if status_short in {"ft", "aet", "p", "canc", "pst"}:
                        continue

                    home = teams.get("home", {}).get("name", "Local")
                    away = teams.get("away", {}).get("name", "Visitante")
                    date_iso = fixture_info.get("date", "")
                    all_matches.append({
                        "league": league_name,
                        "match": f"{home} vs {away}",
                        "time": format_local_time(date_iso),
                    })

            except Exception as e:
                logging.exception(f"Error process_daily_status league={league_name}: {e}")

    if not all_matches:
        no_fixture_key = f"NOFIX-{current_day}"
        if no_fixture_key not in sent_no_fixture_days:
            await send_telegram("😴 Hoy no encontré partidos en tus ligas configuradas. El bot seguirá en espera y volverá a revisar más tarde.")
            sent_no_fixture_days.add(no_fixture_key)
        sent_upcoming_alerts.add(day_alert_key)
        return

    lines = ["📅 PARTIDOS DETECTADOS PARA HOY\n"]
    for m in all_matches[:12]:
        lines.append(f"🏆 {m['league']}\n⚽ {m['match']}\n🕒 {m['time']}\n")
    lines.append("👀 Estaré revisando pre-partidos y live durante el día.")
    await send_telegram("\n".join(lines))
    sent_upcoming_alerts.add(day_alert_key)

# =========================================================
# CLEANUP
# =========================================================

def cleanup_old_signals():
    # limpieza simple cuando ya crezcan mucho
    if len(sent_pre_signals) > 2000:
        sent_pre_signals.clear()
    if len(sent_live_signals) > 4000:
        sent_live_signals.clear()
    if len(sent_parlay_signals) > 1000:
        sent_parlay_signals.clear()
    if len(sent_upcoming_alerts) > 60:
        sent_upcoming_alerts.clear()
    if len(sent_no_fixture_days) > 60:
        sent_no_fixture_days.clear()

# =========================================================
# MAIN LOOP
# =========================================================

async def main_loop():
    get_env_or_raise()
    await send_telegram("✅ Bot V15 PRO iniciado correctamente.")

    while True:
        try:
            cleanup_old_signals()

            if in_work_window():
                logging.info("Dentro del horario de trabajo.")
                await process_daily_status()
                await process_prematch()
                await process_live()
            else:
                logging.info("Fuera del horario de trabajo. Bot en espera.")

            await asyncio.sleep(CYCLE_INTERVAL)

        except Exception as e:
            logging.exception(f"Error general en main_loop: {e}")
            try:
                await send_telegram(f"⚠️ Error en el bot: {e}")
            except Exception:
                pass
            await asyncio.sleep(CYCLE_INTERVAL)

# =========================================================
# ENTRYPOINT
# =========================================================

if __name__ == "__main__":
    asyncio.run(main_loop())
