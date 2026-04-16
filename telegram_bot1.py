"""
V13 PRO INTELIGENTE - Telegram Betting Bot
------------------------------------------
Usa:
- The Odds API -> cuotas y mercados reales
- API-Football -> inteligencia previa (predictions)

Objetivo:
- Prioridad total a señales PREPARTIDO
- Construir picks más lógicos antes de usar cuotas
- Combinar 2 o 3 selecciones
- Evitar duplicados
"""

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

import os

print("DEBUG ENV:", os.environ)
# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("v13_pro")

# =========================================================
# VARIABLES DE ENTORNO
# =========================================================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

if not BOT_TOKEN:
    raise ValueError("Falta BOT_TOKEN")
if not CHAT_ID:
    raise ValueError("Falta CHAT_ID")
if not ODDS_API_KEY:
    raise ValueError("Falta ODDS_API_KEY")
if not API_FOOTBALL_KEY:
    raise ValueError("Falta API_FOOTBALL_KEY")

# =========================================================
# CONFIG
# =========================================================

TZ = ZoneInfo("America/Mexico_City")

LEAGUES = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_uefa_champs_league",
]

# Mapeo a API-Football
API_FOOTBALL_LEAGUE_MAP = {
    "soccer_mexico_ligamx": {"league_id": 262, "season": 2025},
    "soccer_epl": {"league_id": 39, "season": 2025},
    "soccer_uefa_champs_league": {"league_id": 2, "season": 2025},
}

SCAN_INTERVAL_SECONDS = 900
PREMATCH_WINDOW_HOURS = 18
MAX_EVENTS_TO_DEEP_SCAN_PER_CYCLE = 8
MAX_MESSAGES_PER_CYCLE = 3

ODDS_REGION = "uk"

EVENT_MARKETS = "double_chance,alternate_totals_corners,alternate_totals_cards"

MIN_PICK_ODDS = 1.30
MAX_PICK_ODDS = 2.20
MIN_COMBO_ODDS = 2.00
MAX_COMBO_ODDS = 4.50
BASE_MARKETS = "h2h,totals"
PREFERRED_GOAL_LINES = [
    ("Under", 3.5),
    ("Over", 1.5),
    ("Over", 2.5),
]

PREFERRED_CORNERS_LINES = [
    ("Over", 7.5),
    ("Over", 8.5),
    ("Over", 9.5),
]

PREFERRED_CARDS_LINES = [
    ("Over", 3.5),
    ("Over", 4.5),
    ("Over", 5.5),
]

ENABLE_LIVE_SIGNALS = False

# =========================================================
# STATE
# =========================================================

bot = Bot(token=BOT_TOKEN)
sent_signal_keys = set()

# =========================================================
# HELPERS
# =========================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def parse_dt(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))

def format_local_time(dt_str: str) -> str:
    dt = parse_dt(dt_str).astimezone(TZ)
    return dt.strftime("%d/%m %I:%M %p")

def is_within_window(commence_time: str, hours: int) -> bool:
    diff = (parse_dt(commence_time) - now_utc()).total_seconds()
    return 0 < diff <= hours * 3600

def valid_pick_odds(price: float) -> bool:
    return MIN_PICK_ODDS <= price <= MAX_PICK_ODDS

def combo_odds(picks: List[Dict[str, Any]]) -> float:
    total = 1.0
    for p in picks:
        total *= float(p["odds"])
    return round(total, 2)

def valid_combo_odds(price: float) -> bool:
    return MIN_COMBO_ODDS <= price <= MAX_COMBO_ODDS

def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None

def fixture_name(game: Dict[str, Any]) -> str:
    return f"{game.get('home_team', 'Local')} vs {game.get('away_team', 'Visitante')}"

def dedupe_key(game: Dict[str, Any], picks: List[Dict[str, Any]]) -> str:
    labels = "|".join(sorted([p["label"] for p in picks]))
    return f"{game.get('id', 'noid')}::{labels}"

def market_outcomes_by_key(bookmakers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    merged: Dict[str, List[Dict[str, Any]]] = {}
    for book in bookmakers or []:
        for market in book.get("markets", []):
            key = market.get("key")
            if not key:
                continue
            merged.setdefault(key, [])
            for outcome in market.get("outcomes", []):
                item = dict(outcome)
                item["_bookmaker"] = book.get("title", "")
                merged[key].append(item)
    return merged

def pick_best_outcome(
    outcomes: List[Dict[str, Any]],
    *,
    name: Optional[str] = None,
    point: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    candidates = []
    for o in outcomes:
        if name is not None and str(o.get("name")) != str(name):
            continue
        if point is not None:
            p = safe_float(o.get("point"))
            if p is None or abs(p - point) > 1e-9:
                continue
        price = safe_float(o.get("price"))
        if price is None:
            continue
        candidates.append(o)

    if not candidates:
        return None

    return max(candidates, key=lambda x: safe_float(x.get("price")) or 0.0)

# =========================================================
# HTTP
# =========================================================

async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
) -> Tuple[Optional[Any], Dict[str, str]]:
    try:
        response = await client.get(url, params=params, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json(), dict(response.headers)
    except httpx.HTTPStatusError as e:
        logger.error("HTTP %s en %s | %s", e.response.status_code, url, e.response.text[:500])
        return None, dict(getattr(e.response, "headers", {}))
    except Exception as e:
        logger.exception("Error consultando %s: %s", url, e)
        return None, {}

def read_odds_headers(headers: Dict[str, str]) -> None:
    remaining = headers.get("x-requests-remaining") or headers.get("X-Requests-Remaining")
    used = headers.get("x-requests-used") or headers.get("X-Requests-Used")
    if remaining or used:
        logger.info("Odds API | remaining=%s used=%s", remaining, used)

# =========================================================
# THE ODDS API
# =========================================================

async def get_upcoming_odds_for_league(
    client: httpx.AsyncClient,
    sport_key: str
) -> List[Dict[str, Any]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGION,
        "markets": BASE_MARKETS,
        "oddsFormat": "decimal",
    }
    data, headers = await fetch_json(client, url, params=params)
    read_odds_headers(headers)
    return data if isinstance(data, list) else []

async def get_event_extra_markets(
    client: httpx.AsyncClient,
    sport_key: str,
    event_id: str
) -> Optional[Dict[str, Any]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGION,
        "markets": EVENT_MARKETS,
        "oddsFormat": "decimal",
    }
    data, headers = await fetch_json(client, url, params=params)
    read_odds_headers(headers)
    return data if isinstance(data, dict) else None

# =========================================================
# API-FOOTBALL
# =========================================================

API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

def api_football_headers() -> Dict[str, str]:
    return {
        "x-apisports-key": API_FOOTBALL_KEY
    }

async def find_fixture_id_api_football(
    client: httpx.AsyncClient,
    league_id: int,
    season: int,
    home_team: str,
    away_team: str,
    event_dt: str
) -> Optional[int]:
    """
    Busca fixture cercano por liga/temporada/fecha aproximada.
    """
    date_str = parse_dt(event_dt).astimezone(TZ).strftime("%Y-%m-%d")
    url = f"{API_FOOTBALL_BASE}/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "date": date_str,
    }

    data, _ = await fetch_json(client, url, params=params, headers=api_football_headers())
    if not isinstance(data, dict):
        return None

    response = data.get("response", []) or []
    if not response:
        return None

    home_lower = home_team.lower().strip()
    away_lower = away_team.lower().strip()

    for item in response:
        teams = item.get("teams", {})
        home = teams.get("home", {}).get("name", "").lower().strip()
        away = teams.get("away", {}).get("name", "").lower().strip()

        if home == home_lower and away == away_lower:
            return item.get("fixture", {}).get("id")

    # fallback flexible
    for item in response:
        teams = item.get("teams", {})
        home = teams.get("home", {}).get("name", "").lower().strip()
        away = teams.get("away", {}).get("name", "").lower().strip()

        if home_lower in home or home in home_lower:
            if away_lower in away or away in away_lower:
                return item.get("fixture", {}).get("id")

    return None

async def get_prediction_api_football(
    client: httpx.AsyncClient,
    fixture_id: int
) -> Optional[Dict[str, Any]]:
    url = f"{API_FOOTBALL_BASE}/predictions"
    params = {"fixture": fixture_id}
    data, _ = await fetch_json(client, url, params=params, headers=api_football_headers())

    if not isinstance(data, dict):
        return None

    response = data.get("response", []) or []
    if not response:
        return None

    return response[0]

# =========================================================
# INTELIGENCIA API-FOOTBALL
# =========================================================

def extract_prediction_signals(pred: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Saca señales útiles del endpoint predictions.
    """
    result = {
        "winner_name": None,
        "winner_comment": None,
        "advice": None,
        "goals_home": None,
        "goals_away": None,
        "under_over": None,
        "percent_home": None,
        "percent_draw": None,
        "percent_away": None,
    }

    if not pred:
        return result

    predictions = pred.get("predictions", {}) or {}
    winner = predictions.get("winner", {}) or {}
    goals = predictions.get("goals", {}) or {}
    percent = predictions.get("percent", {}) or {}

    result["winner_name"] = winner.get("name")
    result["winner_comment"] = winner.get("comment")
    result["advice"] = predictions.get("advice")
    result["goals_home"] = goals.get("home")
    result["goals_away"] = goals.get("away")
    result["under_over"] = predictions.get("under_over")
    result["percent_home"] = percent.get("home")
    result["percent_draw"] = percent.get("draw")
    result["percent_away"] = percent.get("away")

    return result

def prediction_boost_for_pick(pick: Dict[str, Any], intel: Dict[str, Any], home_team: str, away_team: str) -> float:
    """
    Sube o baja score del pick según la predicción.
    """
    score = 0.0
    label = pick["label"].lower()

    advice = (intel.get("advice") or "").lower()
    winner_name = (intel.get("winner_name") or "").lower()
    under_over = str(intel.get("under_over") or "").lower()

    home_lower = home_team.lower()
    away_lower = away_team.lower()

    # Ganador / doble oportunidad
    if home_lower in label and winner_name == home_lower:
        score += 12
    if away_lower in label and winner_name == away_lower:
        score += 12

    if "o empate" in label:
        if home_lower in label and winner_name == home_lower:
            score += 10
        if away_lower in label and winner_name == away_lower:
            score += 10

    # Under/over goles
    if "under 3.5 goles" in label:
        if "under" in under_over:
            score += 10
    if "over 1.5 goles" in label:
        if "over" in under_over:
            score += 8
    if "over 2.5 goles" in label:
        if "over" in under_over:
            score += 10

    # Ambos anotan usando advice como ayuda ligera
    if "ambos anotan" in label and "goals" in advice:
        score += 4
    if "ambos no anotan" in label and "under" in advice:
        score += 4

    # Tarjetas/corners: sin predicción directa, solo boost pequeño si el juego pinta competido
    pct_home = percent_to_float(intel.get("percent_home"))
    pct_away = percent_to_float(intel.get("percent_away"))
    pct_draw = percent_to_float(intel.get("percent_draw"))

    # Partido cerrado = más posibilidad de tarjetas
    if "tarjetas" in label and pct_home is not None and pct_away is not None:
        if abs(pct_home - pct_away) <= 12:
            score += 5

    # Partido abierto = más corners potenciales
    if "corners" in label and "over" in under_over:
        score += 5

    return score

def percent_to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    s = str(v).replace("%", "").strip()
    try:
        return float(s)
    except Exception:
        return None

def base_pick_score(pick: Dict[str, Any]) -> float:
    odds = float(pick["odds"])
    market = pick.get("market", "")
    score = 100.0

    score -= abs(odds - 1.65) * 20

    if market == "double_chance":
        score += 10
    elif market == "totals_goals":
        score += 9
    elif market == "btts":
        score += 7
    elif market == "cards":
        score += 6
    elif market == "corners":
        score += 5
    elif market == "h2h":
        score += 4

    if odds < 1.35:
        score -= 8
    if odds > 2.10:
        score -= 10

    return round(score, 2)

# =========================================================
# BUILD PICKS
# =========================================================

def build_goal_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    totals = merged_markets.get("totals", []) + merged_markets.get("alternate_totals", [])

    for side, line in PREFERRED_GOAL_LINES:
        outcome = pick_best_outcome(totals, name=side, point=line)
        if not outcome:
            continue
        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        reason = "Línea de goles utilizable para previa."
        if side == "Under":
            reason = "Partido con ruta razonable a pocos goles."
        elif line == 1.5:
            reason = "Línea accesible para que el juego tenga al menos dos goles."
        elif line == 2.5:
            reason = "Partido con opción real de superar la línea de goles."

        picks.append({
            "market": "totals_goals",
            "label": f"{side} {line:g} goles",
            "odds": price,
            "reason_es": reason,
        })
    return picks

def build_btts_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    outcomes = merged_markets.get("btts", [])

    yes = pick_best_outcome(outcomes, name="Yes")
    no = pick_best_outcome(outcomes, name="No")

    if yes:
        price = safe_float(yes.get("price"))
        if price is not None and valid_pick_odds(price):
            picks.append({
                "market": "btts",
                "label": "Ambos anotan",
                "odds": price,
                "reason_es": "Ambos equipos tienen camino razonable al gol.",
            })

    if no:
        price = safe_float(no.get("price"))
        if price is not None and valid_pick_odds(price):
            picks.append({
                "market": "btts",
                "label": "Ambos NO anotan",
                "odds": price,
                "reason_es": "Existe opción de que uno de los dos se quede sin marcar.",
            })

    return picks

def build_h2h_picks(merged_markets: Dict[str, List[Dict[str, Any]]], home: str, away: str) -> List[Dict[str, Any]]:
    picks = []
    outcomes = merged_markets.get("h2h", [])

    for team in [home, away]:
        outcome = pick_best_outcome(outcomes, name=team)
        if not outcome:
            continue
        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "h2h",
            "label": f"{team} gana",
            "odds": price,
            "reason_es": f"{team} sale con cuota utilizable como favorito ligero.",
        })

    return picks

def build_double_chance_picks(merged_markets: Dict[str, List[Dict[str, Any]]], home: str, away: str) -> List[Dict[str, Any]]:
    picks = []
    outcomes = merged_markets.get("double_chance", [])

    targets = [
        (f"{home} or Draw", f"{home} o empate", f"{home} tiene respaldo cubriendo también el empate."),
        (f"{away} or Draw", f"{away} o empate", f"{away} tiene respaldo cubriendo también el empate."),
    ]

    for raw_name, label, reason in targets:
        outcome = pick_best_outcome(outcomes, name=raw_name)
        if not outcome:
            continue
        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "double_chance",
            "label": label,
            "odds": price,
            "reason_es": reason,
        })

    return picks

def build_corners_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    outcomes = merged_markets.get("alternate_totals_corners", [])

    for side, line in PREFERRED_CORNERS_LINES:
        outcome = pick_best_outcome(outcomes, name=side, point=line)
        if not outcome:
            continue
        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "corners",
            "label": f"{side} {line:g} corners",
            "odds": price,
            "reason_es": "Línea de corners interesante para previa.",
        })

    return picks

def build_cards_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    outcomes = merged_markets.get("alternate_totals_cards", [])

    for side, line in PREFERRED_CARDS_LINES:
        outcome = pick_best_outcome(outcomes, name=side, point=line)
        if not outcome:
            continue
        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "cards",
            "label": f"{side} {line:g} tarjetas",
            "odds": price,
            "reason_es": "Línea de tarjetas atractiva para previa.",
        })

    return picks

def build_all_candidate_picks(
    game: Dict[str, Any],
    base_bookmakers: List[Dict[str, Any]],
    extra_bookmakers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    merged = market_outcomes_by_key(base_bookmakers)
    merged_extra = market_outcomes_by_key(extra_bookmakers)

    for k, v in merged_extra.items():
        merged.setdefault(k, [])
        merged[k].extend(v)

    home = game.get("home_team", "")
    away = game.get("away_team", "")

    picks: List[Dict[str, Any]] = []
    picks.extend(build_goal_picks(merged))
    picks.extend(build_btts_picks(merged))
    picks.extend(build_h2h_picks(merged, home, away))
    picks.extend(build_double_chance_picks(merged, home, away))
    picks.extend(build_corners_picks(merged))
    picks.extend(build_cards_picks(merged))

    return picks

# =========================================================
# COMBOS
# =========================================================

def picks_conflict(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    la = a["label"].lower()
    lb = b["label"].lower()

    if la == lb:
        return True

    if ("ambos anotan" in la and "ambos no anotan" in lb) or ("ambos anotan" in lb and "ambos no anotan" in la):
        return True

    if ("gana" in la and "o empate" in lb) or ("gana" in lb and "o empate" in la):
        return True

    if a.get("market") == b.get("market") and a.get("market") in {"cards", "corners", "double_chance"}:
        return True

    return False

def combo_is_valid(picks: List[Dict[str, Any]]) -> bool:
    for i in range(len(picks)):
        for j in range(i + 1, len(picks)):
            if picks_conflict(picks[i], picks[j]):
                return False
    return True

def choose_best_combo(picks: List[Dict[str, Any]]) -> Optional[Tuple[List[Dict[str, Any]], float]]:
    best_combo = None
    best_score = -1e9

    # 2 picks
    for i in range(len(picks)):
        for j in range(i + 1, len(picks)):
            combo = [picks[i], picks[j]]
            if not combo_is_valid(combo):
                continue
            total = combo_odds(combo)
            if not valid_combo_odds(total):
                continue
            score = sum(p["score"] for p in combo)
            if 2.10 <= total <= 3.80:
                score += 10
            if score > best_score:
                best_score = score
                best_combo = (combo, total)

    # 3 picks
    for i in range(len(picks)):
        for j in range(i + 1, len(picks)):
            for k in range(j + 1, len(picks)):
                combo = [picks[i], picks[j], picks[k]]
                if not combo_is_valid(combo):
                    continue
                total = combo_odds(combo)
                if not valid_combo_odds(total):
                    continue
                score = sum(p["score"] for p in combo)
                if 2.30 <= total <= 4.20:
                    score += 14
                if score > best_score:
                    best_score = score
                    best_combo = (combo, total)

    return best_combo

# =========================================================
# TELEGRAM
# =========================================================

def format_signal(game: Dict[str, Any], picks: List[Dict[str, Any]], total_price: float, intel: Dict[str, Any]) -> str:
    lines = []
    lines.append("📊 SEÑAL PREPARTIDO PRO")
    lines.append("")
    lines.append(f"🏟 Partido: {fixture_name(game)}")
    lines.append(f"🏆 Liga: {game.get('sport_title', game.get('sport_key', ''))}")
    lines.append(f"🕒 Hora: {format_local_time(game['commence_time'])}")
    lines.append("")

    if intel.get("advice"):
        lines.append(f"🧠 Lectura API-Football: {intel['advice']}")
        lines.append("")

    lines.append("✅ Picks:")
    for p in picks:
        lines.append(f"- {p['label']} @ {p['odds']:.2f}")

    lines.append("")
    lines.append(f"🎯 Cuota combinada: {total_price:.2f}")
    lines.append("")
    lines.append("📝 Motivos:")
    for p in picks:
        lines.append(f"• {p['reason_es']}")
    lines.append("")
    lines.append("💰 Stake sugerido: 0.5u a 1u")

    return "\n".join(lines)

async def send_text(text: str) -> bool:
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
        return True
    except TelegramError as e:
        logger.error("TelegramError: %s", e)
        return False
    except Exception as e:
        logger.exception("Error inesperado enviando Telegram: %s", e)
        return False

# =========================================================
# FLUJO PRINCIPAL
# =========================================================

async def collect_upcoming_games(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    all_games = []

    for league in LEAGUES:
        games = await get_upcoming_odds_for_league(client, league)
        for game in games:
            if is_within_window(game.get("commence_time", ""), PREMATCH_WINDOW_HOURS):
                all_games.append(game)

    all_games.sort(key=lambda g: g.get("commence_time", "9999"))
    return all_games

async def enrich_with_api_football_intel(
    client: httpx.AsyncClient,
    game: Dict[str, Any]
) -> Dict[str, Any]:
    sport_key = game.get("sport_key")
    mapping = API_FOOTBALL_LEAGUE_MAP.get(sport_key)
    if not mapping:
        return {}

    fixture_id = await find_fixture_id_api_football(
        client=client,
        league_id=mapping["league_id"],
        season=mapping["season"],
        home_team=game.get("home_team", ""),
        away_team=game.get("away_team", ""),
        event_dt=game.get("commence_time", ""),
    )
    if not fixture_id:
        return {}

    pred = await get_prediction_api_football(client, fixture_id)
    return extract_prediction_signals(pred)

async def build_signal_for_game(
    client: httpx.AsyncClient,
    game: Dict[str, Any]
) -> Optional[Tuple[List[Dict[str, Any]], float, Dict[str, Any]]]:
    sport_key = game.get("sport_key")
    event_id = game.get("id")
    if not sport_key or not event_id:
        return None

    event_data = await get_event_extra_markets(client, sport_key, event_id)
    extra_bookmakers = event_data.get("bookmakers", []) if isinstance(event_data, dict) else []
    base_bookmakers = game.get("bookmakers", []) or []

    candidate_picks = build_all_candidate_picks(game, base_bookmakers, extra_bookmakers)
    if len(candidate_picks) < 2:
        return None

    intel = await enrich_with_api_football_intel(client, game)

    # score final: odds + intelligence
    home = game.get("home_team", "")
    away = game.get("away_team", "")

    for p in candidate_picks:
        p["score"] = base_pick_score(p) + prediction_boost_for_pick(p, intel, home, away)

    candidate_picks.sort(key=lambda x: x["score"], reverse=True)

    combo = choose_best_combo(candidate_picks)
    if not combo:
        return None

    picks, total = combo
    return picks, total, intel

async def run_cycle() -> None:
    logger.info("Iniciando ciclo V13 PRO...")
    sent_count = 0

    async with httpx.AsyncClient() as client:
        games = await collect_upcoming_games(client)
        if not games:
            logger.info("No hay partidos en ventana.")
            return

        games = games[:MAX_EVENTS_TO_DEEP_SCAN_PER_CYCLE]

        for game in games:
            if sent_count >= MAX_MESSAGES_PER_CYCLE:
                logger.info("Límite de mensajes por ciclo alcanzado.")
                break

            try:
                result = await build_signal_for_game(client, game)
                if not result:
                    continue

                picks, total, intel = result
                key = dedupe_key(game, picks)
                if key in sent_signal_keys:
                    logger.info("Señal duplicada omitida: %s", key)
                    continue

                msg = format_signal(game, picks, total, intel)
                ok = await send_text(msg)
                if ok:
                    sent_signal_keys.add(key)
                    sent_count += 1
                    logger.info("Señal enviada: %s", fixture_name(game))

            except Exception as e:
                logger.exception("Error procesando %s: %s", fixture_name(game), e)

async def main() -> None:
    logger.info("Bot V13 PRO INTELIGENTE iniciado")
    logger.info("Live habilitado: %s", ENABLE_LIVE_SIGNALS)

    while True:
        try:
            await run_cycle()
        except Exception as e:
            logger.exception("Error en loop principal: %s", e)

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

if __name__ == "__main__":
    asyncio.run(main())
