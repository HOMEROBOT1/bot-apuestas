"""
V12 PRO PREMATCH - Telegram Betting Bot
---------------------------------------
Enfoque:
- Prioridad total a señales PREPARTIDO
- Construye combinadas automáticas de 2 o 3 picks
- Usa The Odds API V4
- Mercados base por liga:
    * h2h
    * totals
    * btts
- Mercados extra por evento:
    * double_chance
    * alternate_totals_corners
    * alternate_totals_cards

Notas:
- Los mercados adicionales deben consultarse uno por evento usando
  /v4/sports/{sport}/events/{eventId}/odds
- Esto consume más créditos, así que el bot filtra por ventana horaria
  y máximo de partidos por ciclo.
"""

import asyncio
import logging
import math
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("v12_prematch")

# =========================================================
# ENV VARS
# =========================================================

BOT_TOKEN = os.getenv("8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ", "").strip()
CHAT_ID = os.getenv("1983622390", "").strip()
ODDS_API_KEY = os.getenv("92f3a8c48fe9834c7b1e6bbf38346064", "").strip()

if not BOT_TOKEN:
    raise ValueError("Falta BOT_TOKEN en variables de entorno.")
if not CHAT_ID:
    raise ValueError("Falta CHAT_ID en variables de entorno.")
if not ODDS_API_KEY:
    raise ValueError("Falta ODDS_API_KEY en variables de entorno.")

# =========================================================
# CONFIG GENERAL
# =========================================================

TZ = ZoneInfo("America/Mexico_City")

LEAGUES = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_uefa_champs_league",
]

# Cada cuánto revisa
SCAN_INTERVAL_SECONDS = 900  # 15 min

# Solo revisar partidos que inicien dentro de esta ventana
PREMATCH_WINDOW_HOURS = 18

# Limita cuántos partidos va a profundizar por ciclo para ahorrar créditos
MAX_EVENTS_TO_DEEP_SCAN_PER_CYCLE = 8

# Regiones a consultar
ODDS_REGION = "uk"

# Endpoint general: mercados destacados/más baratos
BASE_MARKETS = "h2h,totals,btts"

# Endpoint por evento: mercados extra
EVENT_MARKETS = "double_chance,alternate_totals_corners,alternate_totals_cards"

# Rango de cuota individual permitido
MIN_PICK_ODDS = 1.30
MAX_PICK_ODDS = 2.20

# Rango de cuota total de la combinada
MIN_COMBINED_ODDS = 2.00
MAX_COMBINED_ODDS = 4.50

# Máximo picks por señal
MIN_LEGS = 2
MAX_LEGS = 3

# Para no saturar
MAX_MESSAGES_PER_CYCLE = 3

# Preferencias de líneas
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

# Si quieres desactivar por completo live en tu estructura vieja:
ENABLE_LIVE_SIGNALS = False

# =========================================================
# STATE
# =========================================================

bot = Bot(token=BOT_TOKEN)

# Dedupe en memoria
sent_signal_keys = set()

# Para evitar alertar lo mismo de créditos en cada ciclo
credits_alert_sent = False

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
    start = parse_dt(commence_time)
    diff = (start - now_utc()).total_seconds()
    return 0 < diff <= hours * 3600

def valid_pick_odds(price: float) -> bool:
    return MIN_PICK_ODDS <= price <= MAX_PICK_ODDS

def combo_odds(picks: List[Dict[str, Any]]) -> float:
    result = 1.0
    for p in picks:
        result *= float(p["odds"])
    return round(result, 2)

def valid_combo_odds(price: float) -> bool:
    return MIN_COMBINED_ODDS <= price <= MAX_COMBINED_ODDS

def safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None

def compact_team_name(name: str) -> str:
    return (name or "").strip()

def fixture_name(game: Dict[str, Any]) -> str:
    return f"{compact_team_name(game.get('home_team', 'Local'))} vs {compact_team_name(game.get('away_team', 'Visitante'))}"

def signal_dedupe_key(game: Dict[str, Any], picks: List[Dict[str, Any]]) -> str:
    labels = "|".join(sorted([p["label"] for p in picks]))
    return f"{game.get('id', 'noid')}::{labels}"

def market_outcomes_by_key(bookmakers: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Combina outcomes por market key tomando múltiples bookmakers.
    """
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
    """
    Devuelve el outcome con mejor precio para un nombre y punto dado.
    """
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

def implied_probability(decimal_odds: float) -> float:
    if decimal_odds <= 0:
        return 0.0
    return 1.0 / decimal_odds

def score_pick(pick: Dict[str, Any]) -> float:
    """
    Score simple para ordenar picks:
    - premia cuotas razonables
    - premia picks que suelen ser más "seguros" dentro del rango
    """
    odds = float(pick["odds"])
    market = pick.get("market", "")

    base = 100.0

    # Centro preferido alrededor de 1.45 - 1.85
    base -= abs(odds - 1.65) * 20

    # Prioridades
    if market == "double_chance":
        base += 10
    elif market == "totals_goals":
        base += 9
    elif market == "btts":
        base += 7
    elif market == "cards":
        base += 6
    elif market == "corners":
        base += 5
    elif market == "h2h":
        base += 3

    # Penaliza picks muy justitos
    if odds < 1.35:
        base -= 8
    if odds > 2.10:
        base -= 10

    return round(base, 2)

def picks_conflict(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """
    Evita combinaciones absurdas o duplicadas.
    """
    la = a["label"].lower()
    lb = b["label"].lower()

    # mismo pick
    if la == lb:
        return True

    # under/over contradictorios de goles
    if "under 3.5 goles" in la and "over 2.5 goles" in lb:
        return False  # esta sí puede coexistir
    if "under 3.5 goles" in lb and "over 2.5 goles" in la:
        return False

    # ambos anotan vs ambos no anotan
    if ("ambos anotan" in la and "ambos no anotan" in lb) or ("ambos anotan" in lb and "ambos no anotan" in la):
        return True

    # ganador directo del mismo equipo con doble oportunidad redundante
    if ("gana" in la and "o empate" in lb) or ("gana" in lb and "o empate" in la):
        # no siempre es contradicción, pero la evitamos por redundancia
        return True

    # dos picks iguales del mismo mercado con líneas distintas muy redundantes
    if a.get("market") == b.get("market") and a.get("market") in {"cards", "corners"}:
        return True

    if a.get("market") == b.get("market") and a.get("market") in {"double_chance"}:
        return True

    return False

def combo_is_coherent(picks: List[Dict[str, Any]]) -> bool:
    for i in range(len(picks)):
        for j in range(i + 1, len(picks)):
            if picks_conflict(picks[i], picks[j]):
                return False
    return True

def pretty_pick_line(pick: Dict[str, Any]) -> str:
    return f"- {pick['label']} @ {pick['odds']:.2f}"

def format_signal_message(game: Dict[str, Any], picks: List[Dict[str, Any]], total_price: float) -> str:
    match_name = fixture_name(game)
    local_time = format_local_time(game["commence_time"])
    league = game.get("sport_title", game.get("sport_key", ""))

    lines = []
    lines.append("📊 SEÑAL PREPARTIDO")
    lines.append("")
    lines.append(f"🏟 Partido: {match_name}")
    lines.append(f"🏆 Liga: {league}")
    lines.append(f"🕒 Hora: {local_time}")
    lines.append("")
    lines.append("✅ Picks:")
    for pick in picks:
        lines.append(pretty_pick_line(pick))
    lines.append("")
    lines.append(f"🎯 Cuota combinada: {total_price:.2f}")
    lines.append("")
    lines.append("🧠 Motivo:")
    for pick in picks:
        lines.append(f"• {pick['reason_es']}")
    lines.append("")
    lines.append("💰 Stake sugerido: 0.5u a 1u")
    return "\n".join(lines)

# =========================================================
# HTTP
# =========================================================

async def fetch_json(
    client: httpx.AsyncClient,
    url: str,
    params: Dict[str, Any]
) -> Tuple[Optional[Any], Dict[str, str]]:
    try:
        response = await client.get(url, params=params, timeout=30.0)
        headers = {k: v for k, v in response.headers.items()}

        if response.status_code == 429:
            logger.warning("Rate limit alcanzado (429).")
            return None, headers

        response.raise_for_status()

        data = response.json()
        return data, headers

    except httpx.HTTPStatusError as e:
        logger.error("HTTP error %s en %s | %s", e.response.status_code, url, e.response.text[:500])
        return None, {}
    except Exception as e:
        logger.exception("Error al consultar %s: %s", url, e)
        return None, {}

def read_remaining_credits(headers: Dict[str, str]) -> Optional[str]:
    """
    The Odds API suele devolver info de créditos en headers.
    El nombre exacto puede variar por entorno; intentamos varios.
    """
    for key in [
        "x-requests-remaining",
        "X-Requests-Remaining",
        "x-requests-used",
        "X-Requests-Used",
        "x-requests-last",
        "X-Requests-Last",
    ]:
        if key in headers:
            return str(headers[key])
    return None

# =========================================================
# ODDS API
# =========================================================

async def get_upcoming_odds_for_league(
    client: httpx.AsyncClient,
    sport_key: str
) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGION,
        "markets": BASE_MARKETS,
        "oddsFormat": "decimal",
    }
    data, headers = await fetch_json(client, url, params)
    if not isinstance(data, list):
        return [], headers
    return data, headers

async def get_event_extra_markets(
    client: httpx.AsyncClient,
    sport_key: str,
    event_id: str
) -> Tuple[Optional[Dict[str, Any]], Dict[str, str]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGION,
        "markets": EVENT_MARKETS,
        "oddsFormat": "decimal",
    }
    data, headers = await fetch_json(client, url, params)
    if not isinstance(data, dict):
        return None, headers
    return data, headers

# =========================================================
# PICK BUILDERS
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

        label = f"{side} {line:g} goles"
        if side == "Under":
            reason = "Partido con perfil más cerrado y línea manejable para goles."
        else:
            reason = "Línea accesible de goles para un partido con opción de movimiento ofensivo."

        picks.append({
            "market": "totals_goals",
            "label": label,
            "odds": price,
            "reason_es": reason,
            "bookmaker": outcome.get("_bookmaker", ""),
        })

    return picks

def build_btts_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    btts = merged_markets.get("btts", [])

    yes = pick_best_outcome(btts, name="Yes")
    no = pick_best_outcome(btts, name="No")

    if yes:
        price = safe_float(yes.get("price"))
        if price is not None and valid_pick_odds(price):
            picks.append({
                "market": "btts",
                "label": "Ambos anotan",
                "odds": price,
                "reason_es": "Ambos equipos tienen ruta razonable para encontrar al menos un gol.",
                "bookmaker": yes.get("_bookmaker", ""),
            })

    if no:
        price = safe_float(no.get("price"))
        if price is not None and valid_pick_odds(price):
            picks.append({
                "market": "btts",
                "label": "Ambos NO anotan",
                "odds": price,
                "reason_es": "Uno de los dos puede quedarse corto en ataque o el partido puede trabarse.",
                "bookmaker": no.get("_bookmaker", ""),
            })

    return picks

def build_h2h_picks(
    merged_markets: Dict[str, List[Dict[str, Any]]],
    home_team: str,
    away_team: str
) -> List[Dict[str, Any]]:
    picks = []
    h2h = merged_markets.get("h2h", [])

    for team in [home_team, away_team]:
        outcome = pick_best_outcome(h2h, name=team)
        if not outcome:
            continue
        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "h2h",
            "label": f"{team} gana",
            "odds": price,
            "reason_es": f"{team} aparece con una cuota utilizable y ligera ventaja de mercado.",
            "bookmaker": outcome.get("_bookmaker", ""),
        })

    return picks

def build_double_chance_picks(
    merged_markets: Dict[str, List[Dict[str, Any]]],
    home_team: str,
    away_team: str
) -> List[Dict[str, Any]]:
    picks = []
    dc = merged_markets.get("double_chance", [])

    targets = [
        f"{home_team} or Draw",
        f"{away_team} or Draw",
    ]

    for target in targets:
        outcome = pick_best_outcome(dc, name=target)
        if not outcome:
            continue

        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        if target.startswith(home_team):
            label = f"{home_team} o empate"
            reason = f"{home_team} tiene respaldo extra al cubrir también el empate."
        else:
            label = f"{away_team} o empate"
            reason = f"{away_team} tiene respaldo extra al cubrir también el empate."

        picks.append({
            "market": "double_chance",
            "label": label,
            "odds": price,
            "reason_es": reason,
            "bookmaker": outcome.get("_bookmaker", ""),
        })

    return picks

def build_corners_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    corners = merged_markets.get("alternate_totals_corners", [])

    for side, line in PREFERRED_CORNERS_LINES:
        outcome = pick_best_outcome(corners, name=side, point=line)
        if not outcome:
            continue

        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "corners",
            "label": f"{side} {line:g} corners",
            "odds": price,
            "reason_es": "Línea de corners utilizable para un partido con volumen ofensivo razonable.",
            "bookmaker": outcome.get("_bookmaker", ""),
        })

    return picks

def build_cards_picks(merged_markets: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    picks = []
    cards = merged_markets.get("alternate_totals_cards", [])

    for side, line in PREFERRED_CARDS_LINES:
        outcome = pick_best_outcome(cards, name=side, point=line)
        if not outcome:
            continue

        price = safe_float(outcome.get("price"))
        if price is None or not valid_pick_odds(price):
            continue

        picks.append({
            "market": "cards",
            "label": f"{side} {line:g} tarjetas",
            "odds": price,
            "reason_es": "Línea de tarjetas atractiva para un juego con margen de fricción y faltas.",
            "bookmaker": outcome.get("_bookmaker", ""),
        })

    return picks

def build_all_candidate_picks(
    game: Dict[str, Any],
    base_bookmakers: List[Dict[str, Any]],
    event_bookmakers: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    merged_base = market_outcomes_by_key(base_bookmakers)
    merged_event = market_outcomes_by_key(event_bookmakers)
    merged_all = {}
    merged_all.update(merged_base)
    for k, v in merged_event.items():
        merged_all.setdefault(k, [])
        merged_all[k].extend(v)

    home = game.get("home_team", "")
    away = game.get("away_team", "")

    picks: List[Dict[str, Any]] = []
    picks.extend(build_goal_picks(merged_all))
    picks.extend(build_btts_picks(merged_all))
    picks.extend(build_h2h_picks(merged_all, home, away))
    picks.extend(build_double_chance_picks(merged_all, home, away))
    picks.extend(build_corners_picks(merged_all))
    picks.extend(build_cards_picks(merged_all))

    # Score y orden
    for p in picks:
        p["score"] = score_pick(p)

    picks.sort(key=lambda x: x["score"], reverse=True)
    return picks

# =========================================================
# COMBO ENGINE
# =========================================================

def choose_best_combo(candidate_picks: List[Dict[str, Any]]) -> Optional[Tuple[List[Dict[str, Any]], float]]:
    if len(candidate_picks) < MIN_LEGS:
        return None

    best_combo = None
    best_score = -1e9

    # 2 legs
    for i in range(len(candidate_picks)):
        for j in range(i + 1, len(candidate_picks)):
            combo = [candidate_picks[i], candidate_picks[j]]
            if not combo_is_coherent(combo):
                continue

            total = combo_odds(combo)
            if not valid_combo_odds(total):
                continue

            score = sum(p["score"] for p in combo) + (10 if 2.10 <= total <= 3.80 else 0)
            if score > best_score:
                best_score = score
                best_combo = (combo, total)

    # 3 legs
    if MAX_LEGS >= 3:
        for i in range(len(candidate_picks)):
            for j in range(i + 1, len(candidate_picks)):
                for k in range(j + 1, len(candidate_picks)):
                    combo = [candidate_picks[i], candidate_picks[j], candidate_picks[k]]
                    if not combo_is_coherent(combo):
                        continue

                    total = combo_odds(combo)
                    if not valid_combo_odds(total):
                        continue

                    score = sum(p["score"] for p in combo) + (14 if 2.30 <= total <= 4.20 else 0)
                    if score > best_score:
                        best_score = score
                        best_combo = (combo, total)

    return best_combo

# =========================================================
# TELEGRAM
# =========================================================

async def send_text(text: str) -> bool:
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
        return True
    except TelegramError as e:
        logger.error("TelegramError al enviar mensaje: %s", e)
        return False
    except Exception as e:
        logger.exception("Error inesperado enviando mensaje: %s", e)
        return False

# =========================================================
# MAIN PREMATCH FLOW
# =========================================================

async def collect_upcoming_games(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    all_games: List[Dict[str, Any]] = []

    for league in LEAGUES:
        games, headers = await get_upcoming_odds_for_league(client, league)
        remaining = read_remaining_credits(headers)
        if remaining is not None:
            logger.info("Créditos/header (%s): %s", league, remaining)

        for game in games:
            if is_within_window(game.get("commence_time", ""), PREMATCH_WINDOW_HOURS):
                all_games.append(game)

    # Orden por hora de inicio
    all_games.sort(key=lambda g: g.get("commence_time", "9999"))
    return all_games

async def build_prematch_signal_for_game(
    client: httpx.AsyncClient,
    game: Dict[str, Any]
) -> Optional[Tuple[List[Dict[str, Any]], float]]:
    sport_key = game.get("sport_key")
    event_id = game.get("id")

    if not sport_key or not event_id:
        return None

    event_data, headers = await get_event_extra_markets(client, sport_key, event_id)
    remaining = read_remaining_credits(headers)
    if remaining is not None:
        logger.info("Créditos/header (evento %s): %s", event_id[:8], remaining)

    event_bookmakers = []
    if event_data and isinstance(event_data, dict):
        event_bookmakers = event_data.get("bookmakers", []) or []

    base_bookmakers = game.get("bookmakers", []) or []

    candidate_picks = build_all_candidate_picks(game, base_bookmakers, event_bookmakers)
    if len(candidate_picks) < 2:
        logger.info("Sin picks suficientes para %s", fixture_name(game))
        return None

    combo = choose_best_combo(candidate_picks)
    return combo

async def run_prematch_cycle() -> None:
    global credits_alert_sent

    logger.info("Iniciando ciclo prepartido...")
    messages_sent = 0

    async with httpx.AsyncClient() as client:
        upcoming_games = await collect_upcoming_games(client)

        if not upcoming_games:
            logger.info("No hay partidos próximos en ventana.")
            return

        # Solo profundiza algunos por ciclo para cuidar créditos
        games_to_scan = upcoming_games[:MAX_EVENTS_TO_DEEP_SCAN_PER_CYCLE]
        logger.info("Partidos a revisar a detalle: %s", len(games_to_scan))

        for game in games_to_scan:
            if messages_sent >= MAX_MESSAGES_PER_CYCLE:
                logger.info("Límite de mensajes por ciclo alcanzado.")
                break

            try:
                combo_result = await build_prematch_signal_for_game(client, game)
                if not combo_result:
                    continue

                picks, total_price = combo_result
                key = signal_dedupe_key(game, picks)

                if key in sent_signal_keys:
                    logger.info("Señal duplicada omitida: %s", key)
                    continue

                sent_signal_keys.add(key)
                msg = format_signal_message(game, picks, total_price)
                ok = await send_text(msg)

                if ok:
                    messages_sent += 1
                    logger.info("Señal enviada para %s", fixture_name(game))

            except Exception as e:
                logger.exception("Error procesando juego %s: %s", fixture_name(game), e)

# =========================================================
# LOOP
# =========================================================

async def main() -> None:
    logger.info("Bot V12 PRO PREMATCH iniciado.")
    logger.info("Live signals habilitadas: %s", ENABLE_LIVE_SIGNALS)

    while True:
        try:
            await run_prematch_cycle()
        except Exception as e:
            logger.exception("Error en ciclo principal: %s", e)

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    asyncio.run(main())
