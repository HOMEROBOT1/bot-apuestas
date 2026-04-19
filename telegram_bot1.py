import asyncio
import logging
import os
from datetime import datetime, timedelta
from statistics import mean
from zoneinfo import ZoneInfo

import httpx
from telegram import Bot
from telegram.error import TelegramError

# =============================================================================
# CONFIG
# =============================================================================

BOT_TOKEN = "8713741185:AAFqvoZ0Ji3xWw2FsA8BuMslfCGhQ0tMzCQ"
CHAT_ID = "1983622390"
ODDS_API_KEY = "92f3a8c48fe9834c7b1e6bbf38346064"
API_FOOTBALL_KEY = "c455630d0023ef208f93dd0567164905"

LOCAL_TZ = ZoneInfo(os.getenv("LOCAL_TZ", "America/Mexico_City"))

ACTIVE_START_HOUR = int(os.getenv("ACTIVE_START_HOUR", "7"))
ACTIVE_END_HOUR = int(os.getenv("ACTIVE_END_HOUR", "22"))

SEND_BEFORE_MIN = int(os.getenv("SEND_BEFORE_MIN", "10"))
CYCLE_INTERVAL = int(os.getenv("CYCLE_INTERVAL", "60"))

MIN_EDGE_H2H = float(os.getenv("MIN_EDGE_H2H", "0.06"))
MIN_EDGE_TOTALS = float(os.getenv("MIN_EDGE_TOTALS", "0.06"))
MIN_EDGE_BTTS = float(os.getenv("MIN_EDGE_BTTS", "0.06"))
MIN_EDGE_CORNERS = float(os.getenv("MIN_EDGE_CORNERS", "0.04"))
MIN_EDGE_CARDS = float(os.getenv("MIN_EDGE_CARDS", "0.04"))

ODDS_REGIONS = os.getenv("ODDS_REGIONS", "uk")
ODDS_BOOKMAKERS = os.getenv("ODDS_BOOKMAKERS", "").strip()
ODDS_FEATURED_MARKETS = "h2h,totals,btts"
ODDS_ADDITIONAL_MARKETS = "alternate_totals_corners,alternate_totals_cards"

ENABLE_PREMATCH = os.getenv("ENABLE_PREMATCH", "true").lower() == "true"
ENABLE_LIVE = os.getenv("ENABLE_LIVE", "true").lower() == "true"
ENABLE_PARLAY = os.getenv("ENABLE_PARLAY", "true").lower() == "true"

SPORT_KEYS = [
    "soccer_mexico_ligamx",
    "soccer_epl",
    "soccer_uefa_champs_league",
]

API_FOOTBALL_LEAGUES = {
    262: {"name": "Liga MX", "season": 2025},
    39: {"name": "Premier League", "season": 2025},
    2: {"name": "Champions League", "season": 2025},
}

# live thresholds
LIVE_MINUTE_MIN = 55
LIVE_GOAL_PRESSURE_MINUTE = 60
LIVE_CORNERS_MINUTE = 55
LIVE_CARDS_MINUTE = 50

# =============================================================================
# SETUP
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

required = {
    "BOT_TOKEN": BOT_TOKEN,
    "CHAT_ID": CHAT_ID,
    "ODDS_API_KEY": ODDS_API_KEY,
    "API_FOOTBALL_KEY": API_FOOTBALL_KEY,
}
missing = [k for k, v in required.items() if not v]
if missing:
    raise RuntimeError(f"Faltan variables: {', '.join(missing)}")

bot = Bot(token=BOT_TOKEN)

# estado en memoria
scheduled_picks = {}
scheduled_parlay_date = None

sent_picks = set()
sent_parlays = set()
sent_live_signals = set()
sent_daily_status = set()

# =============================================================================
# UTILS
# =============================================================================

def now_local() -> datetime:
    return datetime.now(LOCAL_TZ)

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def implied_prob(decimal_odds: float):
    if not decimal_odds or decimal_odds <= 1:
        return None
    return 1 / decimal_odds

def decimal_from_prob(prob: float):
    if not prob or prob <= 0:
        return None
    return 1 / prob

def to_local(dt_str: str | None):
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
    except Exception:
        return None

def fixture_key(home: str, away: str, kickoff: datetime | None):
    base = f"{(home or '').strip().lower()}|{(away or '').strip().lower()}"
    if kickoff:
        base += "|" + kickoff.strftime("%Y-%m-%d %H:%M")
    return base

def format_match_time(dt: datetime | None):
    if not dt:
        return "Hora no disponible"
    return dt.strftime("%d/%m %I:%M %p")

async def safe_send(text: str):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
    except TelegramError as e:
        logging.error(f"Telegram error: {e}")

def active_window_open() -> bool:
    h = now_local().hour
    return ACTIVE_START_HOUR <= h < ACTIVE_END_HOUR

# =============================================================================
# HTTP
# =============================================================================

async def get_json(url: str, headers=None, params=None):
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers=headers, params=params)
        r.raise_for_status()
        return r.json(), r.headers

# =============================================================================
# ODDS API
# =============================================================================

async def get_featured_odds_for_sport(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_FEATURED_MARKETS,
        "oddsFormat": "decimal",
    }
    if ODDS_BOOKMAKERS:
        params["bookmakers"] = ODDS_BOOKMAKERS
    data, headers = await get_json(url, params=params)
    return data, headers

async def get_event_additional_markets(sport_key: str, event_id: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/events/{event_id}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": ODDS_ADDITIONAL_MARKETS,
        "oddsFormat": "decimal",
    }
    if ODDS_BOOKMAKERS:
        params["bookmakers"] = ODDS_BOOKMAKERS
    try:
        data, _ = await get_json(url, params=params)
        return data
    except Exception as e:
        logging.info(f"Mercados adicionales no disponibles para event_id={event_id}: {e}")
        return None

# =============================================================================
# API FOOTBALL
# =============================================================================

API_FOOTBALL_BASE = "https://v3.football.api-sports.io"

async def api_football_get(path: str, params=None):
    headers = {"x-apisports-key": API_FOOTBALL_KEY}
    url = f"{API_FOOTBALL_BASE}{path}"
    data, _ = await get_json(url, headers=headers, params=params)
    return data

async def get_today_fixtures():
    today = now_local().strftime("%Y-%m-%d")
    all_fx = []

    for league_id, cfg in API_FOOTBALL_LEAGUES.items():
        params = {
            "league": league_id,
            "season": cfg["season"],
            "date": today,
            "timezone": "America/Mexico_City",
        }
        try:
            data = await api_football_get("/fixtures", params=params)
            resp = data.get("response", [])
            logging.info(f"{cfg['name']} hoy: {len(resp)} fixtures")
            for item in resp:
                fx = item.get("fixture", {})
                teams = item.get("teams", {})
                status = fx.get("status", {})
                all_fx.append({
                    "fixture_id": fx.get("id"),
                    "league_id": league_id,
                    "league_name": cfg["name"],
                    "home": teams.get("home", {}).get("name"),
                    "away": teams.get("away", {}).get("name"),
                    "date_local": to_local(fx.get("date")),
                    "status_short": status.get("short"),
                })
        except Exception as e:
            logging.warning(f"Error fixtures {cfg['name']}: {e}")

    return all_fx

async def get_live_fixtures():
    all_fx = []

    for league_id, cfg in API_FOOTBALL_LEAGUES.items():
        params = {
            "live": "all",
            "league": league_id,
            "season": cfg["season"],
            "timezone": "America/Mexico_City",
        }
        try:
            data = await api_football_get("/fixtures", params=params)
            resp = data.get("response", [])
            for item in resp:
                fx = item.get("fixture", {})
                teams = item.get("teams", {})
                goals = item.get("goals", {})
                status = fx.get("status", {})
                all_fx.append({
                    "fixture_id": fx.get("id"),
                    "league_name": cfg["name"],
                    "home": teams.get("home", {}).get("name"),
                    "away": teams.get("away", {}).get("name"),
                    "minute": status.get("elapsed") or 0,
                    "status_short": status.get("short"),
                    "home_goals": goals.get("home", 0) or 0,
                    "away_goals": goals.get("away", 0) or 0,
                })
        except Exception as e:
            logging.warning(f"Error live fixtures {cfg['name']}: {e}")

    return all_fx

async def get_fixture_events(fixture_id: int):
    try:
        data = await api_football_get("/fixtures/events", params={"fixture": fixture_id})
        return data.get("response", [])
    except Exception as e:
        logging.warning(f"Error events {fixture_id}: {e}")
        return []

async def get_fixture_statistics(fixture_id: int):
    try:
        data = await api_football_get("/fixtures/statistics", params={"fixture": fixture_id})
        return data.get("response", [])
    except Exception as e:
        logging.warning(f"Error statistics {fixture_id}: {e}")
        return []

# =============================================================================
# ANALYSIS HELPERS
# =============================================================================

def best_price_and_probs_h2h(match):
    home = match.get("home_team")
    away = match.get("away_team")
    probs = {"home": [], "draw": [], "away": []}
    best = {"home": None, "draw": None, "away": None}

    for bm in match.get("bookmakers", []):
        for market in bm.get("markets", []):
            if market.get("key") != "h2h":
                continue
            for oc in market.get("outcomes", []):
                name = (oc.get("name") or "").strip().lower()
                price = safe_float(oc.get("price"))
                if not price:
                    continue
                p = implied_prob(price)
                if p is None:
                    continue

                if name == (home or "").lower():
                    probs["home"].append(p)
                    best["home"] = price if best["home"] is None else max(best["home"], price)
                elif name == (away or "").lower():
                    probs["away"].append(p)
                    best["away"] = price if best["away"] is None else max(best["away"], price)
                elif name in ["draw", "empate"]:
                    probs["draw"].append(p)
                    best["draw"] = price if best["draw"] is None else max(best["draw"], price)

    return probs, best

def best_price_and_probs_totals(match):
    totals = {}
    for bm in match.get("bookmakers", []):
        for market in bm.get("markets", []):
            if market.get("key") != "totals":
                continue
            for oc in market.get("outcomes", []):
                side = (oc.get("name") or "").strip().lower()
                point = safe_float(oc.get("point"))
                price = safe_float(oc.get("price"))
                if side not in ["over", "under"] or point is None or not price:
                    continue
                key = (point, side)
                if key not in totals:
                    totals[key] = {"probs": [], "best": None}
                p = implied_prob(price)
                if p is None:
                    continue
                totals[key]["probs"].append(p)
                totals[key]["best"] = price if totals[key]["best"] is None else max(totals[key]["best"], price)
    return totals

def best_price_and_probs_btts(match):
    probs = {"yes": [], "no": []}
    best = {"yes": None, "no": None}
    for bm in match.get("bookmakers", []):
        for market in bm.get("markets", []):
            if market.get("key") != "btts":
                continue
            for oc in market.get("outcomes", []):
                side = (oc.get("name") or "").strip().lower()
                price = safe_float(oc.get("price"))
                if not price:
                    continue
                norm = "yes" if side in ["yes", "si", "sí"] else "no"
                if norm not in ["yes", "no"]:
                    continue
                p = implied_prob(price)
                if p is None:
                    continue
                probs[norm].append(p)
                best[norm] = price if best[norm] is None else max(best[norm], price)
    return probs, best

def parse_event_additional_markets(event_data):
    """
    Devuelve candidatos corners/cards si existen en el event odds endpoint.
    """
    candidates = []

    if not event_data:
        return candidates

    for bm in event_data.get("bookmakers", []):
        for market in bm.get("markets", []):
            key = market.get("key")
            if key not in ["alternate_totals_corners", "alternate_totals_cards"]:
                continue
            for oc in market.get("outcomes", []):
                side = (oc.get("name") or "").strip().lower()
                point = safe_float(oc.get("point"))
                price = safe_float(oc.get("price"))
                if side not in ["over", "under"] or point is None or not price:
                    continue

                if key == "alternate_totals_corners":
                    if (point, side) in [(8.5, "over"), (9.5, "over"), (10.5, "under")]:
                        candidates.append({
                            "market": "corners",
                            "pick": f"{'Over' if side == 'over' else 'Under'} {point} córners",
                            "offered_odds": round(price, 2),
                            "edge": 0.0,
                            "score_hint": price,
                        })

                if key == "alternate_totals_cards":
                    if (point, side) in [(3.5, "over"), (4.5, "over"), (6.5, "under")]:
                        candidates.append({
                            "market": "cards",
                            "pick": f"{'Over' if side == 'over' else 'Under'} {point} tarjetas",
                            "offered_odds": round(price, 2),
                            "edge": 0.0,
                            "score_hint": price,
                        })

    return candidates

def stake_from_edge(edge: float):
    if edge >= 0.14:
        return 3
    if edge >= 0.09:
        return 2
    return 1

def analyze_prematch(match, additional_event_data=None):
    home = match.get("home_team")
    away = match.get("away_team")
    candidates = []

    # H2H
    probs_h2h, best_h2h = best_price_and_probs_h2h(match)
    for side in ["home", "draw", "away"]:
        if not probs_h2h[side] or not best_h2h[side]:
            continue
        fair_prob = mean(probs_h2h[side])
        fair_odds = decimal_from_prob(fair_prob)
        offered = best_h2h[side]
        edge = (offered / fair_odds) - 1
        if edge >= MIN_EDGE_H2H:
            pick = {
                "home": f"Gana {home}",
                "draw": "Empate",
                "away": f"Gana {away}",
            }[side]
            candidates.append({
                "market": "h2h",
                "pick": pick,
                "fair_odds": round(fair_odds, 2),
                "offered_odds": round(offered, 2),
                "edge": edge,
            })

    # totals
    totals = best_price_and_probs_totals(match)
    desired_totals = {
        (1.5, "over"): "Over 1.5 goles",
        (2.5, "over"): "Over 2.5 goles",
        (3.5, "under"): "Under 3.5 goles",
    }
    for key, label in desired_totals.items():
        if key not in totals:
            continue
        fair_prob = mean(totals[key]["probs"])
        fair_odds = decimal_from_prob(fair_prob)
        offered = totals[key]["best"]
        edge = (offered / fair_odds) - 1
        if edge >= MIN_EDGE_TOTALS:
            candidates.append({
                "market": "totals",
                "pick": label,
                "fair_odds": round(fair_odds, 2),
                "offered_odds": round(offered, 2),
                "edge": edge,
            })

    # btts
    probs_btts, best_btts = best_price_and_probs_btts(match)
    if probs_btts["yes"] and best_btts["yes"]:
        fair_prob = mean(probs_btts["yes"])
        fair_odds = decimal_from_prob(fair_prob)
        offered = best_btts["yes"]
        edge = (offered / fair_odds) - 1
        if edge >= MIN_EDGE_BTTS:
            candidates.append({
                "market": "btts",
                "pick": "Ambos anotan: Sí",
                "fair_odds": round(fair_odds, 2),
                "offered_odds": round(offered, 2),
                "edge": edge,
            })

    # corners/cards adicionales
    extra_candidates = parse_event_additional_markets(additional_event_data)
    for c in extra_candidates:
        threshold = MIN_EDGE_CORNERS if c["market"] == "corners" else MIN_EDGE_CARDS
        # Aquí no hay fair line real sin una segunda comparación estable.
        # Usamos filtro conservador por cuota para no mandar basura.
        if 1.60 <= c["offered_odds"] <= 2.10 and threshold <= 0.05:
            c["fair_odds"] = None
            c["edge"] = 0.05
            candidates.append(c)

    if not candidates:
        return None

    candidates.sort(key=lambda x: (x["edge"], x["offered_odds"]), reverse=True)
    best = candidates[0]
    best["stake"] = stake_from_edge(best["edge"])
    return best

def format_prematch_signal(match, analysis, kickoff_local):
    market_map = {
        "h2h": "Ganador / Empate",
        "totals": "Over / Under goles",
        "btts": "Ambos anotan",
        "corners": "Córners",
        "cards": "Tarjetas",
    }
    label = "💎 PICK ELITE" if analysis["stake"] >= 2 else "📊 PICK"
    lines = [
        label + " (10 MIN)",
        "",
        f"⚽ {match['home_team']} vs {match['away_team']}",
        f"🕒 Hora: {format_match_time(kickoff_local)}",
        f"📌 Mercado: {market_map.get(analysis['market'], analysis['market'])}",
        f"🎯 Pick: {analysis['pick']}",
        f"💰 Cuota: {analysis['offered_odds']}",
    ]
    if analysis.get("fair_odds"):
        lines.append(f"📉 Cuota justa: {analysis['fair_odds']}")
    lines.append(f"🔥 Edge: {round(analysis['edge'] * 100, 1)}%")
    lines.append(f"📊 Stake: {analysis['stake']}/10")
    return "\n".join(lines)

# =============================================================================
# SCHEDULING PREMATCH
# =============================================================================

async def build_daily_schedule():
    if not ENABLE_PREMATCH:
        return

    all_matches = []

    for sport_key in SPORT_KEYS:
        try:
            featured, _ = await get_featured_odds_for_sport(sport_key)
            for match in featured:
                kickoff = to_local(match.get("commence_time"))
                if not kickoff:
                    continue
                key = fixture_key(match.get("home_team"), match.get("away_team"), kickoff)
                if key in scheduled_picks or key in sent_picks:
                    continue

                # agenda solo partidos del día y futuros
                if kickoff.date() != now_local().date():
                    continue
                if kickoff <= now_local():
                    continue

                scheduled_picks[key] = {
                    "sport_key": sport_key,
                    "match": match,
                    "kickoff": kickoff,
                    "send_time": kickoff - timedelta(minutes=SEND_BEFORE_MIN),
                }
                all_matches.append((key, match, kickoff))
        except Exception as e:
            logging.warning(f"Error odds schedule {sport_key}: {e}")

    logging.info(f"Partidos programados hoy: {len(scheduled_picks)}")

async def send_due_picks():
    due_for_parlay = []

    for key, item in list(scheduled_picks.items()):
        if key in sent_picks:
            continue

        if now_local() < item["send_time"]:
            continue

        match = item["match"]
        kickoff = item["kickoff"]
        event_id = match.get("id")
        sport_key = item["sport_key"]

        additional = None
        if event_id:
            additional = await get_event_additional_markets(sport_key, event_id)

        analysis = analyze_prematch(match, additional_event_data=additional)
        if not analysis:
            sent_picks.add(key)
            continue

        msg = format_prematch_signal(match, analysis, kickoff)
        await safe_send(msg)
        sent_picks.add(key)
        due_for_parlay.append({
            "fixture_key": key,
            "home": match.get("home_team"),
            "away": match.get("away_team"),
            "pick": analysis["pick"],
            "odds": analysis["offered_odds"],
            "edge": analysis["edge"],
            "stake": analysis["stake"],
            "kickoff": kickoff,
        })

    if ENABLE_PARLAY and due_for_parlay:
        await maybe_send_parlay(due_for_parlay)

def build_parlay(candidates):
    """
    2-3 legs, cuota total 2.00 a 4.50
    """
    cands = sorted(candidates, key=lambda x: (x["edge"], x["stake"]), reverse=True)
    chosen = []
    total = 1.0

    for c in cands:
        if len(chosen) >= 3:
            break
        projected = total * c["odds"]
        if projected <= 4.50:
            chosen.append(c)
            total = projected

    if len(chosen) < 2:
        return None
    if total < 2.00 or total > 4.50:
        return None

    return {"legs": chosen, "total_odds": round(total, 2)}

async def maybe_send_parlay(candidates):
    global scheduled_parlay_date

    today_str = now_local().strftime("%Y-%m-%d")
    if scheduled_parlay_date == today_str:
        return

    parlay = build_parlay(candidates)
    if not parlay:
        return

    parlay_key = "|".join([x["fixture_key"] for x in parlay["legs"]])
    if parlay_key in sent_parlays:
        return

    lines = ["🔥 PARLAY ELITE", ""]
    for i, leg in enumerate(parlay["legs"], start=1):
        lines.append(f"{i}. {leg['home']} vs {leg['away']}")
        lines.append(f"   🎯 {leg['pick']} @ {leg['odds']}")
    lines.append("")
    lines.append(f"💰 Cuota total: {parlay['total_odds']}")
    await safe_send("\n".join(lines))

    sent_parlays.add(parlay_key)
    scheduled_parlay_date = today_str

# =============================================================================
# LIVE SIGNALS
# =============================================================================

def stats_to_map(stats_response):
    """
    Convierte stats de API-Football a dict por equipo.
    """
    team_stats = {}
    for block in stats_response:
        team_name = block.get("team", {}).get("name")
        stats = {}
        for item in block.get("statistics", []):
            stats[item.get("type")] = item.get("value")
        team_stats[team_name] = stats
    return team_stats

def parse_int_stat(v):
    if v is None:
        return 0
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        txt = v.replace("%", "").strip()
        try:
            return int(txt)
        except Exception:
            return 0
    return 0

async def analyze_live_fixture(fx):
    minute = fx["minute"]
    if minute < LIVE_MINUTE_MIN:
        return []

    fixture_id = fx["fixture_id"]
    home = fx["home"]
    away = fx["away"]
    hg = fx["home_goals"]
    ag = fx["away_goals"]

    signals = []

    stats_raw = await get_fixture_statistics(fixture_id)
    events = await get_fixture_events(fixture_id)
    stats = stats_to_map(stats_raw)

    hs = stats.get(home, {})
    as_ = stats.get(away, {})

    home_shots = parse_int_stat(hs.get("Shots on Goal")) + parse_int_stat(hs.get("Total Shots"))
    away_shots = parse_int_stat(as_.get("Shots on Goal")) + parse_int_stat(as_.get("Total Shots"))

    home_corners = parse_int_stat(hs.get("Corner Kicks"))
    away_corners = parse_int_stat(as_.get("Corner Kicks"))

    home_yellow = parse_int_stat(hs.get("Yellow Cards"))
    away_yellow = parse_int_stat(as_.get("Yellow Cards"))

    # roja
    red_found = False
    for ev in events:
        detail = (ev.get("detail") or "").lower()
        typ = (ev.get("type") or "").lower()
        if "red card" in detail or "red card" in typ:
            red_found = True
            break

    # señal de gol/over live
    total_goals = hg + ag
    if minute >= LIVE_GOAL_PRESSURE_MINUTE and total_goals <= 2 and (home_shots + away_shots) >= 18:
        signals.append({
            "reason": "goal_pressure",
            "text": f"Mucho volumen ofensivo ({home_shots + away_shots} tiros aprox.). Posible gol tardío / over live."
        })

    # corners live
    total_corners = home_corners + away_corners
    if minute >= LIVE_CORNERS_MINUTE and total_corners >= 8:
        signals.append({
            "reason": "corners_pressure",
            "text": f"Ya van {total_corners} córners. Posible over córners live."
        })

    # cards live
    total_yellow = home_yellow + away_yellow
    if minute >= LIVE_CARDS_MINUTE and total_yellow >= 4:
        signals.append({
            "reason": "cards_pressure",
            "text": f"Partido caliente con {total_yellow} amarillas aprox. Posible over tarjetas live."
        })

    # roja
    if red_found and minute >= LIVE_MINUTE_MIN:
        signals.append({
            "reason": "red_card",
            "text": "Se detectó tarjeta roja. Puede abrir valor en gol, córners o siguiente gol."
        })

    # one goal margin late
    if minute >= 65 and abs(hg - ag) == 1:
        signals.append({
            "reason": "one_goal_margin",
            "text": "Diferencia de un gol en tramo avanzado. Ojo con gol tardío / empate / siguiente gol."
        })

    return signals

async def process_live():
    if not ENABLE_LIVE:
        return

    fixtures = await get_live_fixtures()
    for fx in fixtures:
        try:
            signals = await analyze_live_fixture(fx)
            for sig in signals:
                dedupe_key = f"{fx['fixture_id']}|{sig['reason']}"
                if dedupe_key in sent_live_signals:
                    continue

                msg = (
                    f"🔴 SEÑAL EN VIVO\n\n"
                    f"🏆 {fx['league_name']}\n"
                    f"⚽ {fx['home']} vs {fx['away']}\n"
                    f"⏱ Minuto: {fx['minute']}'\n"
                    f"📊 Marcador: {fx['home']} {fx['home_goals']}-{fx['away_goals']} {fx['away']}\n"
                    f"🧠 Motivo: {sig['text']}"
                )
                await safe_send(msg)
                sent_live_signals.add(dedupe_key)
        except Exception as e:
            logging.warning(f"Error live fixture {fx.get('fixture_id')}: {e}")

# =============================================================================
# DAILY STATUS
# =============================================================================

async def send_daily_status():
    today_str = now_local().strftime("%Y-%m-%d")
    if today_str in sent_daily_status:
        return

    fixtures = await get_today_fixtures()
    if not fixtures:
        await safe_send("📭 Hoy no encontré partidos en tus ligas configuradas.")
        sent_daily_status.add(today_str)
        return

    leagues = sorted(list({f['league_name'] for f in fixtures}))
    await safe_send(f"📅 Hoy sí hay partidos en: {', '.join(leagues)}. El bot queda atento y mandará picks 10 min antes.")
    sent_daily_status.add(today_str)

# =============================================================================
# LOOP
# =============================================================================

async def run_cycle():
    if not active_window_open():
        return

    await send_daily_status()
    await build_daily_schedule()
    await send_due_picks()
    await process_live()

async def main():
    logging.info("Iniciando V17 NIVEL DIOS...")
    while True:
        try:
            await run_cycle()
            await asyncio.sleep(CYCLE_INTERVAL)
        except Exception as e:
            logging.exception(f"Error loop principal: {e}")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
