"""
Football Betting Alerts Bot - V5
--------------------------------
Incluye:
- Pre-match con edge (The Odds API)
- Live stats y scoring (API-Football)
- Live odds + edge estimado
- Modo dinero
- Historial JSON
- Liquidación automática:
    * Pre-match 1X2
    * Live over goals
    * Live over corners
    * Live over tarjetas
"""

import asyncio
import json
import logging
from datetime import datetime, timezone, timedelta
from statistics import mean
from pathlib import Path
import re

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

PREMATCH_SCAN_INTERVAL = 180
LIVE_SCAN_INTERVAL = 60
UPCOMING_SCAN_INTERVAL = 300
SETTLEMENT_SCAN_INTERVAL = 300
MAIN_LOOP_SLEEP = 60

PREMATCH_MIN_MINUTES = 15
PREMATCH_MAX_MINUTES = 120
UPCOMING_LOOKAHEAD_HOURS = 12
MIN_VALUE_EDGE = 0.06

MAX_PREMATCH_ALERTS_PER_CYCLE = 5
MAX_LIVE_ALERTS_PER_CYCLE = 6

ODDS_MARKETS = "h2h"
ODDS_REGIONS = "uk"
ODDS_API_REQUEST_DELAY = 1

LIVE_MIN_MINUTE = 8
LIVE_GOAL_MIN_SCORE = 7
LIVE_CORNERS_MIN_SCORE = 6
LIVE_CARDS_MIN_SCORE = 6
MAX_STATS_REQUESTS_PER_CYCLE = 5

ENABLE_V4_LIVE_ODDS = True
LIVE_ODDS_MIN_EDGE = 0.08

LIVE_GOAL_MIN_ODDS = 1.70
LIVE_GOAL_MAX_ODDS = 3.50

LIVE_CORNERS_MIN_ODDS = 1.60
LIVE_CORNERS_MAX_ODDS = 2.80

LIVE_CARDS_MIN_ODDS = 1.60
LIVE_CARDS_MAX_ODDS = 2.80

BANKROLL_INICIAL = 1000.00
BANKROLL_STATE_FILE = Path("bankroll_state.json")
BET_HISTORY_FILE = Path("bet_history.json")

DAILY_SUMMARY_HOUR = 21
DAILY_SUMMARY_MINUTE = 55

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
# JSON state helpers
# ---------------------------------------------------------------------------

def ensure_json_file(path: Path, default_data):
    if not path.exists():
        path.write_text(json.dumps(default_data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path, default_data):
    ensure_json_file(path, default_data)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("No se pudo leer %s; recreando archivo.", path)
        path.write_text(json.dumps(default_data, ensure_ascii=False, indent=2), encoding="utf-8")
        return default_data


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_bankroll_state():
    default_data = {
        "bankroll_inicial": BANKROLL_INICIAL,
        "last_daily_summary_date": None,
    }
    data = load_json(BANKROLL_STATE_FILE, default_data)
    if "bankroll_inicial" not in data:
        data["bankroll_inicial"] = BANKROLL_INICIAL
    if "last_daily_summary_date" not in data:
        data["last_daily_summary_date"] = None
    return data


def save_bankroll_state(state: dict):
    save_json(BANKROLL_STATE_FILE, state)


def load_bet_history():
    return load_json(BET_HISTORY_FILE, [])


def save_bet_history(history: list[dict]):
    save_json(BET_HISTORY_FILE, history)

# ---------------------------------------------------------------------------
# General helpers
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


def odd_to_float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def extract_line_from_label(label: str) -> float | None:
    if not label:
        return None

    match = re.search(r"(\d+(?:\.\d+)?)", label)
    if not match:
        return None

    try:
        return float(match.group(1))
    except ValueError:
        return None


def get_stake_by_score(score: int, bankroll_actual: float) -> float:
    if score >= 9:
        return round(bankroll_actual * 0.03, 2)
    if score >= 7:
        return round(bankroll_actual * 0.02, 2)
    if score >= 6:
        return round(bankroll_actual * 0.01, 2)
    return 0.0


def bankroll_summary(history: list[dict], bankroll_inicial: float) -> dict:
    settled = [b for b in history if b.get("resultado") in {"ganada", "perdida"}]

    total_bets = len(settled)
    wins = len([b for b in settled if b["resultado"] == "ganada"])
    losses = len([b for b in settled if b["resultado"] == "perdida"])
    pending = len([b for b in history if b.get("resultado") == "pendiente"])

    total_staked = sum(float(b.get("stake", 0.0)) for b in settled)
    total_profit = sum(float(b.get("profit", 0.0)) for b in settled)

    roi = (total_profit / total_staked * 100) if total_staked > 0 else 0.0
    bankroll_actual = bankroll_inicial + total_profit

    return {
        "total_bets": total_bets,
        "wins": wins,
        "losses": losses,
        "pending": pending,
        "total_staked": round(total_staked, 2),
        "profit": round(total_profit, 2),
        "roi": round(roi, 2),
        "bankroll_actual": round(bankroll_actual, 2),
    }


def current_bankroll(history: list[dict], bankroll_inicial: float) -> float:
    return bankroll_summary(history, bankroll_inicial)["bankroll_actual"]


def format_money(x: float) -> str:
    sign = "+" if x > 0 else ""
    return f"{sign}{x:.2f}"


def format_daily_summary_text(summary: dict) -> str:
    return (
        "💰 REPORTE DEL BOT\n\n"
        f"✅ Apuestas cerradas: {summary['total_bets']}\n"
        f"🟢 Ganadas: {summary['wins']}\n"
        f"🔴 Perdidas: {summary['losses']}\n"
        f"⏳ Pendientes: {summary['pending']}\n"
        f"💸 Total apostado: {summary['total_staked']:.2f}\n"
        f"📈 Profit neto: {format_money(summary['profit'])}\n"
        f"📊 ROI: {summary['roi']:.2f}%\n"
        f"🏦 Bank actual: {summary['bankroll_actual']:.2f}"
    )


def register_bet(history: list[dict], bet: dict) -> bool:
    bet_id = bet["bet_id"]
    if any(item.get("bet_id") == bet_id for item in history):
        return False
    history.append(bet)
    save_bet_history(history)
    return True

# ---------------------------------------------------------------------------
# Signal scoring
# ---------------------------------------------------------------------------

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
# Live odds helpers
# ---------------------------------------------------------------------------

def find_best_live_odd(live_odds: dict, bet_names: list[str], labels: list[str]) -> dict | None:
    if not live_odds:
        return None

    bet_names_norm = [x.lower() for x in bet_names]
    labels_norm = [x.lower() for x in labels]

    best = None

    for item in live_odds.get("values", []):
        bet_name = (item.get("bet_name") or "").strip().lower()
        label = (item.get("label") or "").strip().lower()
        odd = odd_to_float(item.get("odd"))

        if odd is None:
            continue

        if bet_name in bet_names_norm and label in labels_norm:
            if best is None or odd > best["odd"]:
                best = {
                    "bookmaker": item.get("bookmaker"),
                    "bet_name": item.get("bet_name"),
                    "label": item.get("label"),
                    "odd": odd,
                }

    return best


def estimate_fair_odds_from_score(score: int, signal_type: str) -> float:
    score = max(1, min(10, score))

    if signal_type == "goal":
        mapping = {6: 2.30, 7: 2.05, 8: 1.85, 9: 1.70, 10: 1.60}
    elif signal_type == "corners":
        mapping = {6: 2.10, 7: 1.95, 8: 1.80, 9: 1.70, 10: 1.60}
    else:
        mapping = {6: 2.15, 7: 2.00, 8: 1.85, 9: 1.75, 10: 1.65}

    if score <= 5:
        return 2.50

    return mapping.get(score, 1.60)


def compute_live_edge(live_odd: float, fair_odd: float) -> float:
    if not live_odd or not fair_odd or fair_odd <= 0:
        return 0.0
    return (live_odd / fair_odd) - 1.0

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
        f"🕒 Hora: {format_local_hour(alert['commence_time'])}\n"
        f"💵 Stake sugerido: {alert['stake']:.2f}"
    )


def format_live_alert(alert: dict) -> str:
    badge = {
        "VIP": "💎",
        "PRO": "🔥",
        "BUENA": "📈",
        "NORMAL": "📊",
    }.get(alert["tier"], "📊")

    market_line = f"\n📍 Mercado: {alert['market_name']}" if alert.get("market_name") else ""
    label_line = f"\n🏷 Línea: {alert['market_label']}" if alert.get("market_label") else ""

    odds_line = ""
    if alert.get("live_odd") is not None:
        odds_line = (
            f"\n💰 Cuota live: {alert['live_odd']:.2f}"
            f"\n📐 Cuota justa estimada: {alert['fair_odd']:.2f}"
            f"\n🔥 Edge live: {alert['live_edge_pct']:.2f}%"
        )

    stake_line = f"\n💵 Stake sugerido: {alert['stake']:.2f}" if alert.get("stake") is not None else ""

    return (
        f"{badge} ALERTA EN VIVO {alert['tier']}\n\n"
        f"⚽ {alert['home']} vs {alert['away']}\n"
        f"🏆 Liga: {alert['league']}\n"
        f"📊 Marcador: {alert['score']}\n"
        f"⏱ Minuto: {alert['minute']}\n"
        f"🎯 Señal: {alert['signal_type']}\n"
        f"⭐ Score: {alert['signal_score']}/10"
        f"{market_line}"
        f"{label_line}"
        f"{odds_line}"
        f"{stake_line}\n"
        f"📌 Motivo: {alert['reason']}"
    )

# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

async def send_message(bot: Bot, text: str) -> None:
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
        logger.info("Mensaje enviado (%d chars)", len(text))
    except TelegramError as exc:
        logger.error("Telegram error: %s", exc)

# ---------------------------------------------------------------------------
# API fetchers
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


async def fetch_pre_match_alerts(bot: Bot, client: httpx.AsyncClient, bankroll_actual: float) -> list[dict]:
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
                    await send_message(bot, "⚠️ Aviso: te quedaste sin créditos en The Odds API.")
                    odds_credits_alert_sent = True

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
                            if name and isinstance(price, (int, float)):
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

                    score = 7 if edge_decimal < 0.10 else 8 if edge_decimal < 0.15 else 9
                    stake = get_stake_by_score(score, bankroll_actual)

                    alerts.append({
                        "match_key": match_key,
                        "league": league_name,
                        "sport_key": sport_key,
                        "home_team": home_team,
                        "away_team": away_team,
                        "pick": outcome_name,
                        "pick_text": translate_pick(outcome_name, home_team, away_team),
                        "best_odds": float(best_odds),
                        "avg_odds": float(avg_odds),
                        "edge_pct": edge_decimal * 100,
                        "minutes_to_start": get_minutes_to_start(commence_time),
                        "commence_time": commence_time,
                        "stake": stake,
                        "score": score,
                    })

            await asyncio.sleep(ODDS_API_REQUEST_DELAY)

        except Exception as exc:
            logger.warning("Error consultando %s: %s", sport_key, exc)
            await asyncio.sleep(ODDS_API_REQUEST_DELAY)

    alerts.sort(key=lambda x: (-x["edge_pct"], x["minutes_to_start"]))
    return alerts[:MAX_PREMATCH_ALERTS_PER_CYCLE]


async def fetch_match_stats(client: httpx.AsyncClient, fixture_id: int):
    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures/statistics",
            params={"fixture": fixture_id},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=15,
        )

        if r.status_code != 200:
            return None

        response = r.json().get("response", [])
        if len(response) < 2:
            return None

        def parse_statistics(team_block: dict) -> dict:
            parsed = {}
            for item in team_block.get("statistics", []):
                parsed[item.get("type")] = item.get("value")
            return parsed

        return {
            "home": parse_statistics(response[0]),
            "away": parse_statistics(response[1]),
        }

    except Exception as exc:
        logger.warning("Error obteniendo stats fixture=%s: %s", fixture_id, exc)
        return None


async def fetch_live_odds(client: httpx.AsyncClient, fixture_id: int):
    try:
        r = await client.get(
            "https://v3.football.api-sports.io/odds/live",
            params={"fixture": fixture_id},
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            timeout=15,
        )

        if r.status_code != 200:
            return None

        response = r.json().get("response", [])
        if not response:
            return None

        item = response[0]
        values = []

        for bookmaker_block in item.get("bookmakers", []):
            for bet_block in bookmaker_block.get("bets", []):
                bet_name = bet_block.get("name", "")
                for value_block in bet_block.get("values", []):
                    values.append({
                        "bookmaker": bookmaker_block.get("name"),
                        "bet_name": bet_name,
                        "label": value_block.get("value"),
                        "odd": value_block.get("odd"),
                    })

        return {
            "fixture_id": fixture_id,
            "values": values,
        }

    except Exception as exc:
        logger.warning("Error obteniendo live odds fixture=%s: %s", fixture_id, exc)
        return None

# ---------------------------------------------------------------------------
# Live alert generation
# ---------------------------------------------------------------------------

async def fetch_live_alerts(client: httpx.AsyncClient, bankroll_actual: float) -> list[dict]:
    alerts: list[dict] = []

    try:
        r = await client.get(
            "https://v3.football.api-sports.io/fixtures",
            params={"live": "all", "timezone": "America/Mexico_City"},
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

            live_odds_payload = None
            if ENABLE_V4_LIVE_ODDS:
                live_odds_payload = await fetch_live_odds(client, fixture_id)

            home_stats = extract_live_stat_block(stats_payload["home"])
            away_stats = extract_live_stat_block(stats_payload["away"])

            # GOAL probable local -> prefer Over goals market for settlement
            home_goal_score, home_goal_reasons = score_goal_signal(
                minute=minute,
                team_stats=home_stats,
                opp_stats=away_stats,
                team_goals=home_goals,
                opp_goals=away_goals,
            )

            if home_goal_score >= LIVE_GOAL_MIN_SCORE:
                fair_odd = estimate_fair_odds_from_score(home_goal_score, "goal")
                best_live = None
                live_edge = 0.0

                if live_odds_payload:
                    best_live = find_best_live_odd(
                        live_odds_payload,
                        bet_names=["Goals Over/Under", "Over/Under"],
                        labels=["Over 1.5", "Over 2.5", "Over 3.5"],
                    )
                    if best_live:
                        live_edge = compute_live_edge(best_live["odd"], fair_odd)

                if (not ENABLE_V4_LIVE_ODDS) or (
                    best_live
                    and LIVE_GOAL_MIN_ODDS <= best_live["odd"] <= LIVE_GOAL_MAX_ODDS
                    and live_edge >= LIVE_ODDS_MIN_EDGE
                ):
                    stake = get_stake_by_score(home_goal_score, bankroll_actual)
                    alerts.append({
                        "signal_key": build_live_key(fixture_id, minute, "goals_over"),
                        "league": league.get("name", "Liga"),
                        "home": home,
                        "away": away,
                        "minute": minute,
                        "score": f"{home_goals}-{away_goals}",
                        "signal_type": "Over goles live",
                        "signal_score": home_goal_score,
                        "tier": classify_live_tier(home_goal_score),
                        "reason": ", ".join(home_goal_reasons),
                        "market_name": best_live["bet_name"] if best_live else None,
                        "market_label": best_live["label"] if best_live else None,
                        "live_odd": best_live["odd"] if best_live else None,
                        "fair_odd": fair_odd if best_live else None,
                        "live_edge_pct": live_edge * 100 if best_live else None,
                        "stake": stake,
                        "fixture_id": fixture_id,
                    })

            # Corners
            corners_score, corners_reasons = score_corners_signal(
                minute=minute,
                home_stats=home_stats,
                away_stats=away_stats,
            )

            if corners_score >= LIVE_CORNERS_MIN_SCORE:
                fair_odd = estimate_fair_odds_from_score(corners_score, "corners")
                best_live = None
                live_edge = 0.0

                if live_odds_payload:
                    best_live = find_best_live_odd(
                        live_odds_payload,
                        bet_names=["Corners Over Under", "Over/Under - Corners", "Corners"],
                        labels=["Over 8.5", "Over 9.5", "Over 10.5"],
                    )
                    if best_live:
                        live_edge = compute_live_edge(best_live["odd"], fair_odd)

                if (not ENABLE_V4_LIVE_ODDS) or (
                    best_live
                    and LIVE_CORNERS_MIN_ODDS <= best_live["odd"] <= LIVE_CORNERS_MAX_ODDS
                    and live_edge >= LIVE_ODDS_MIN_EDGE
                ):
                    stake = get_stake_by_score(corners_score, bankroll_actual)
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
                        "market_name": best_live["bet_name"] if best_live else None,
                        "market_label": best_live["label"] if best_live else None,
                        "live_odd": best_live["odd"] if best_live else None,
                        "fair_odd": fair_odd if best_live else None,
                        "live_edge_pct": live_edge * 100 if best_live else None,
                        "stake": stake,
                        "fixture_id": fixture_id,
                    })

            # Cards
            cards_score, cards_reasons = score_cards_signal(
                minute=minute,
                home_stats=home_stats,
                away_stats=away_stats,
            )

            if cards_score >= LIVE_CARDS_MIN_SCORE:
                fair_odd = estimate_fair_odds_from_score(cards_score, "cards")
                best_live = None
                live_edge = 0.0

                if live_odds_payload:
                    best_live = find_best_live_odd(
                        live_odds_payload,
                        bet_names=["Cards Over/Under", "Bookings", "Over/Under - Cards"],
                        labels=["Over 3.5", "Over 4.5", "Over 5.5"],
                    )
                    if best_live:
                        live_edge = compute_live_edge(best_live["odd"], fair_odd)

                if (not ENABLE_V4_LIVE_ODDS) or (
                    best_live
                    and LIVE_CARDS_MIN_ODDS <= best_live["odd"] <= LIVE_CARDS_MAX_ODDS
                    and live_edge >= LIVE_ODDS_MIN_EDGE
                ):
                    stake = get_stake_by_score(cards_score, bankroll_actual)
                    alerts.append({
                        "signal_key": build_live_key(fixture_id, minute, "cards"),
                        "league": league.get("name", "Liga"),
                        "home": home,
                        "away": away,
                        "minute": minute,
                        "score": f"{home_goals}-{away_goals}",
                        "signal_type": "Over tarjetas live",
                        "signal_score": cards_score,
                        "tier": classify_live_tier(cards_score),
                        "reason": ", ".join(cards_reasons),
                        "market_name": best_live["bet_name"] if best_live else None,
                        "market_label": best_live["label"] if best_live else None,
                        "live_odd": best_live["odd"] if best_live else None,
                        "fair_odd": fair_odd if best_live else None,
                        "live_edge_pct": live_edge * 100 if best_live else None,
                        "stake": stake,
                        "fixture_id": fixture_id,
                    })

        alerts.sort(key=lambda x: (-x["signal_score"], x["minute"]))
        return alerts[:MAX_LIVE_ALERTS_PER_CYCLE]

    except Exception as exc:
        logger.warning("Error en live fetch: %s", exc)
        return alerts

# ---------------------------------------------------------------------------
# Bet entry builders
# ---------------------------------------------------------------------------

def create_prematch_bet_entry(alert: dict) -> dict:
    return {
        "bet_id": f"prematch|{alert['match_key']}",
        "tipo": "prematch",
        "subtipo": "h2h",
        "fecha_alerta": now_local().isoformat(),
        "league": alert["league"],
        "home_team": alert["home_team"],
        "away_team": alert["away_team"],
        "pick": alert["pick"],
        "pick_text": alert["pick_text"],
        "odd": alert["best_odds"],
        "stake": alert["stake"],
        "resultado": "pendiente",
        "profit": 0.0,
        "settled_at": None,
    }


def create_live_bet_entry(alert: dict) -> dict:
    market_label = alert.get("market_label")
    line = extract_line_from_label(market_label) if market_label else None

    return {
        "bet_id": f"live|{alert['signal_key']}",
        "tipo": "live",
        "subtipo": normalize_text(alert["signal_type"]),
        "fixture_id": alert.get("fixture_id"),
        "fecha_alerta": now_local().isoformat(),
        "league": alert["league"],
        "home_team": alert["home"],
        "away_team": alert["away"],
        "market_name": alert.get("market_name"),
        "market_label": market_label,
        "line": line,
        "pick": alert["signal_type"],
        "pick_text": alert["signal_type"],
        "odd": alert.get("live_odd"),
        "stake": alert["stake"],
        "resultado": "pendiente",
        "profit": 0.0,
        "settled_at": None,
        "reason": alert.get("reason"),
    }

# ---------------------------------------------------------------------------
# Settlement
# ---------------------------------------------------------------------------

async def settle_prematch_bets(client: httpx.AsyncClient, history: list[dict]) -> bool:
    changed = False

    for bet in history:
        if bet.get("tipo") != "prematch":
            continue
        if bet.get("resultado") != "pendiente":
            continue

        home_team = bet.get("home_team")
        away_team = bet.get("away_team")
        alert_time_str = bet.get("fecha_alerta")
        if not home_team or not away_team or not alert_time_str:
            continue

        try:
            alert_dt = datetime.fromisoformat(alert_time_str)
        except Exception:
            continue

        date_str = alert_dt.astimezone(LOCAL_TZ).strftime("%Y-%m-%d")
        found_result = None

        for league_id in ALLOWED_LEAGUE_IDS:
            try:
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
                    continue

                response = r.json().get("response", [])

                for item in response:
                    teams = item.get("teams", {})
                    fixture = item.get("fixture", {})
                    goals = item.get("goals", {})

                    h = teams.get("home", {}).get("name")
                    a = teams.get("away", {}).get("name")
                    status = fixture.get("status", {}).get("short")

                    if h != home_team or a != away_team:
                        continue
                    if status not in {"FT", "AET", "PEN"}:
                        continue

                    hg = goals.get("home")
                    ag = goals.get("away")
                    if hg is None or ag is None:
                        continue

                    found_result = (hg, ag)
                    break

                if found_result:
                    break

            except Exception:
                continue

        if not found_result:
            continue

        hg, ag = found_result
        pick = bet.get("pick")
        odd = float(bet.get("odd") or 0.0)
        stake = float(bet.get("stake") or 0.0)

        won = False
        if pick == home_team and hg > ag:
            won = True
        elif pick == away_team and ag > hg:
            won = True
        elif normalize_text(pick) == "draw" and hg == ag:
            won = True

        if won:
            bet["resultado"] = "ganada"
            bet["profit"] = round(stake * (odd - 1), 2)
        else:
            bet["resultado"] = "perdida"
            bet["profit"] = round(-stake, 2)

        bet["settled_at"] = now_local().isoformat()
        changed = True

    if changed:
        save_bet_history(history)

    return changed


async def settle_live_bets(client: httpx.AsyncClient, history: list[dict]) -> bool:
    changed = False

    for bet in history:
        if bet.get("tipo") != "live":
            continue
        if bet.get("resultado") != "pendiente":
            continue

        fixture_id = bet.get("fixture_id")
        if not fixture_id:
            continue

        try:
            r = await client.get(
                "https://v3.football.api-sports.io/fixtures",
                params={"id": fixture_id, "timezone": "America/Mexico_City"},
                headers={"x-apisports-key": FOOTBALL_API_KEY},
                timeout=20,
            )

            if r.status_code != 200:
                continue

            response = r.json().get("response", [])
            if not response:
                continue

            match = response[0]
            status = match.get("fixture", {}).get("status", {}).get("short")
            if status not in {"FT", "AET", "PEN"}:
                continue

            goals = match.get("goals", {})
            hg = goals.get("home", 0) or 0
            ag = goals.get("away", 0) or 0

            stats_payload = await fetch_match_stats(client, fixture_id)
            if not stats_payload:
                continue

            home_stats = extract_live_stat_block(stats_payload["home"])
            away_stats = extract_live_stat_block(stats_payload["away"])

            total_goals = hg + ag
            total_corners = home_stats["corner_kicks"] + away_stats["corner_kicks"]
            total_cards = (
                home_stats["yellow_cards"] + away_stats["yellow_cards"] +
                home_stats["red_cards"] + away_stats["red_cards"]
            )

            market_name = normalize_text(bet.get("market_name"))
            market_label = normalize_text(bet.get("market_label"))
            line = bet.get("line")
            odd = float(bet.get("odd") or 0.0)
            stake = float(bet.get("stake") or 0.0)

            if line is None or odd <= 1 or stake <= 0:
                continue

            won = None

            if "corner" in market_name and "over" in market_label:
                won = total_corners > line
            elif ("card" in market_name or "booking" in market_name) and "over" in market_label:
                won = total_cards > line
            elif "over" in market_label and ("goal" in market_name or "over/under" in market_name):
                won = total_goals > line

            if won is None:
                continue

            if won:
                bet["resultado"] = "ganada"
                bet["profit"] = round(stake * (odd - 1), 2)
            else:
                bet["resultado"] = "perdida"
                bet["profit"] = round(-stake, 2)

            bet["settled_at"] = now_local().isoformat()
            changed = True

        except Exception as exc:
            logger.warning("Error liquidando live fixture=%s: %s", fixture_id, exc)
            continue

    if changed:
        save_bet_history(history)

    return changed

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

    bankroll_state = load_bankroll_state()
    bankroll_inicial = float(bankroll_state["bankroll_inicial"])
    history = load_bet_history()

    bot = Bot(token=BOT_TOKEN)
    info = await bot.get_me()
    logger.info("Bot online: @%s", info.username)

    last_upcoming_scan = datetime.min.replace(tzinfo=timezone.utc)
    last_prematch_scan = datetime.min.replace(tzinfo=timezone.utc)
    last_live_scan = datetime.min.replace(tzinfo=timezone.utc)
    last_settlement_scan = datetime.min.replace(tzinfo=timezone.utc)

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

                history = load_bet_history()
                bankroll_actual = current_bankroll(history, bankroll_inicial)

                if (current_utc - last_upcoming_scan).total_seconds() >= UPCOMING_SCAN_INTERVAL:
                    upcoming_matches = await fetch_upcoming_matches(client)
                    logger.info("Próximos partidos detectados: %s", len(upcoming_matches))

                    for match in upcoming_matches:
                        if match["match_key"] in sent_upcoming_match_alerts:
                            continue
                        await send_message(bot, format_upcoming_match_alert(match))
                        sent_upcoming_match_alerts.add(match["match_key"])

                    last_upcoming_scan = current_utc

                if (current_utc - last_prematch_scan).total_seconds() >= PREMATCH_SCAN_INTERVAL:
                    prematch_alerts = await fetch_pre_match_alerts(bot, client, bankroll_actual)
                    logger.info("Alertas pre-partido detectadas: %s", len(prematch_alerts))

                    for alert in prematch_alerts:
                        if alert["match_key"] in sent_pre_match_signals:
                            continue

                        await send_message(bot, format_pre_match_alert(alert))
                        sent_pre_match_signals.add(alert["match_key"])

                        history = load_bet_history()
                        register_bet(history, create_prematch_bet_entry(alert))

                    last_prematch_scan = current_utc

                if (current_utc - last_live_scan).total_seconds() >= LIVE_SCAN_INTERVAL:
                    history = load_bet_history()
                    bankroll_actual = current_bankroll(history, bankroll_inicial)

                    live_alerts = await fetch_live_alerts(client, bankroll_actual)
                    logger.info("Alertas live detectadas: %s", len(live_alerts))

                    for alert in live_alerts:
                        if alert["signal_key"] in sent_live_signals:
                            continue

                        await send_message(bot, format_live_alert(alert))
                        sent_live_signals.add(alert["signal_key"])

                        history = load_bet_history()
                        register_bet(history, create_live_bet_entry(alert))

                    last_live_scan = current_utc

                if (current_utc - last_settlement_scan).total_seconds() >= SETTLEMENT_SCAN_INTERVAL:
                    history = load_bet_history()
                    changed_pre = await settle_prematch_bets(client, history)
                    changed_live = await settle_live_bets(client, history)

                    if changed_pre or changed_live:
                        summary = bankroll_summary(history, bankroll_inicial)
                        logger.info(
                            "Bets liquidadas. Profit=%s ROI=%s Bank=%s",
                            summary["profit"],
                            summary["roi"],
                            summary["bankroll_actual"],
                        )

                    last_settlement_scan = current_utc

                bankroll_state = load_bankroll_state()
                last_summary_date = bankroll_state.get("last_daily_summary_date")
                today_str = current_local.strftime("%Y-%m-%d")

                if (
                    current_local.hour == DAILY_SUMMARY_HOUR
                    and current_local.minute >= DAILY_SUMMARY_MINUTE
                    and last_summary_date != today_str
                ):
                    history = load_bet_history()
                    summary = bankroll_summary(history, bankroll_inicial)
                    await send_message(bot, format_daily_summary_text(summary))

                    bankroll_state["last_daily_summary_date"] = today_str
                    save_bankroll_state(bankroll_state)

                await asyncio.sleep(MAIN_LOOP_SLEEP)

            except Exception as exc:
                logger.error("Cycle error: %s", exc)
                await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
