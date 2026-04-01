import logging
import re
import unicodedata
from datetime import date, datetime, timedelta
from difflib import SequenceMatcher

import requests

logger = logging.getLogger(__name__)

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/all/scoreboard"
_SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/all/summary"
_MIN_MATCH_SCORE = 0.78
_MIN_MARGIN = 0.08
_TIMEOUT = 20


def _normalizar_texto(valor: str | None) -> str:
    if not valor:
        return ""
    valor = unicodedata.normalize("NFKD", valor)
    valor = "".join(ch for ch in valor if not unicodedata.combining(ch))
    valor = valor.lower()
    valor = re.sub(r"[^a-z0-9]+", " ", valor)
    return " ".join(valor.split())


def _parsear_fecha(valor: str | date | None) -> date | None:
    if isinstance(valor, date):
        return valor
    if not valor:
        return None
    try:
        return datetime.strptime(str(valor), "%Y-%m-%d").date()
    except ValueError:
        return None


def _dividir_partido(partido: str | None) -> tuple[str, str] | None:
    if not partido:
        return None

    separadores = [r"\s+vs\.?\s+", r"\s+v\s+", r"\s+-\s+", r"\s+@\s+"]
    for patron in separadores:
        partes = re.split(patron, partido, maxsplit=1, flags=re.IGNORECASE)
        if len(partes) == 2:
            izquierda = partes[0].strip()
            derecha = partes[1].strip()
            if izquierda and derecha:
                return izquierda, derecha
    return None


def _candidatos_nombre_equipo(team: dict | None) -> list[str]:
    if not team:
        return []
    nombres = [
        team.get("displayName"),
        team.get("shortDisplayName"),
        team.get("name"),
        team.get("location"),
        team.get("abbreviation"),
    ]
    return [nombre for nombre in nombres if nombre]


def _similitud(a: str, b: str) -> float:
    a_norm = _normalizar_texto(a)
    b_norm = _normalizar_texto(b)
    if not a_norm or not b_norm:
        return 0.0
    if a_norm == b_norm:
        return 1.0
    return SequenceMatcher(None, a_norm, b_norm).ratio()


def _mejor_similitud_equipo(nombre: str, team: dict | None) -> float:
    return max((_similitud(nombre, candidato) for candidato in _candidatos_nombre_equipo(team)), default=0.0)


def _puntuacion_evento(pick: dict, event: dict) -> float:
    equipos_pick = _dividir_partido(pick.get("partido"))
    if not equipos_pick:
        return 0.0

    competencia = (event.get("competitions") or [{}])[0]
    competidores = competencia.get("competitors") or []
    if len(competidores) < 2:
        return 0.0

    home = next((c for c in competidores if c.get("homeAway") == "home"), competidores[0])
    away = next((c for c in competidores if c.get("homeAway") == "away"), competidores[1])

    pick_a, pick_b = equipos_pick
    score_directo = (
        _mejor_similitud_equipo(pick_a, home.get("team"))
        + _mejor_similitud_equipo(pick_b, away.get("team"))
    ) / 2
    score_inverso = (
        _mejor_similitud_equipo(pick_a, away.get("team"))
        + _mejor_similitud_equipo(pick_b, home.get("team"))
    ) / 2
    score = max(score_directo, score_inverso)

    liga_pick = _normalizar_texto(pick.get("liga"))
    liga_evento = _normalizar_texto(
        competencia.get("league", {}).get("name")
        or competencia.get("name")
        or event.get("league", {}).get("name")
    )
    if liga_pick and liga_evento:
        liga_score = _similitud(liga_pick, liga_evento)
        if liga_score >= 0.65:
            score += 0.05

    return min(score, 1.0)


def _get_json(session: requests.Session, url: str, params: dict[str, str]) -> dict:
    response = session.get(url, params=params, timeout=_TIMEOUT)
    response.raise_for_status()
    return response.json()


def _cargar_eventos_fecha(session: requests.Session, fecha: date) -> list[dict]:
    payload = _get_json(
        session,
        _SCOREBOARD_URL,
        {"dates": fecha.strftime("%Y%m%d"), "limit": "1000"},
    )
    return payload.get("events") or []


def _buscar_mejor_evento(session: requests.Session, pick: dict) -> tuple[dict | None, float, float]:
    fecha_pick = _parsear_fecha(pick.get("fecha"))
    if not fecha_pick:
        return None, 0.0, 0.0

    eventos: list[dict] = []
    for delta in (-1, 0, 1):
        fecha_consulta = fecha_pick + timedelta(days=delta)
        try:
            eventos.extend(_cargar_eventos_fecha(session, fecha_consulta))
        except Exception as e:
            logger.warning(f"No se pudo consultar ESPN para {fecha_consulta}: {e}")

    puntuados: list[tuple[float, dict]] = []
    for event in eventos:
        score = _puntuacion_evento(pick, event)
        if score > 0:
            puntuados.append((score, event))

    if not puntuados:
        return None, 0.0, 0.0

    puntuados.sort(key=lambda item: item[0], reverse=True)
    mejor_score, mejor_evento = puntuados[0]
    segundo_score = puntuados[1][0] if len(puntuados) > 1 else 0.0
    return mejor_evento, mejor_score, mejor_score - segundo_score


def _corner_total_ft(summary: dict) -> int | None:
    equipos = summary.get("boxscore", {}).get("teams") or []
    if len(equipos) < 2:
        return None

    total = 0
    for equipo in equipos:
        stats = equipo.get("statistics") or []
        valor = next(
            (
                stat.get("displayValue")
                for stat in stats
                if stat.get("label") == "Corner Kicks" or stat.get("name") == "wonCorners"
            ),
            None,
        )
        if valor is None:
            return None
        try:
            total += int(str(valor).strip())
        except ValueError:
            return None
    return total


def _corner_total_ht(summary: dict) -> int | None:
    comentarios = summary.get("commentary") or []
    total = 0
    encontrado = False

    for item in comentarios:
        play = item.get("play") or {}
        tipo = (play.get("type") or {}).get("type")
        periodo = (play.get("period") or {}).get("number")
        texto = item.get("text") or play.get("text") or ""

        es_corner = tipo == "corner-kick" or texto.startswith("Corner,")
        if not es_corner:
            continue

        encontrado = True
        if periodo == 1:
            total += 1

    if not encontrado:
        return None
    return total


def _estado_completado(event: dict) -> bool:
    competencia = (event.get("competitions") or [{}])[0]
    status = competencia.get("status", {})
    tipo = status.get("type", {})
    return bool(tipo.get("completed")) or tipo.get("state") == "post"


async def resolver_corner_pick_espn(pick: dict) -> dict | None:
    periodo = (pick.get("periodo_codigo") or "FT").upper()
    if periodo not in ("FT", "HT"):
        return None

    with requests.Session() as session:
        session.headers.update({"User-Agent": _USER_AGENT, "Accept": "application/json"})
        mejor_evento, score, margin = _buscar_mejor_evento(session, pick)
        if not mejor_evento:
            return None
        if score < _MIN_MATCH_SCORE or margin < _MIN_MARGIN:
            logger.info(
                "ESPN ambiguo para %s | score=%.3f margin=%.3f",
                pick.get("partido"),
                score,
                margin,
            )
            return None
        if not _estado_completado(mejor_evento):
            return None

        event_id = mejor_evento.get("id")
        if not event_id:
            return None

        try:
            summary = _get_json(session, _SUMMARY_URL, {"event": str(event_id)})
        except Exception as e:
            logger.warning(f"No se pudo cargar resumen ESPN para {event_id}: {e}")
            return None

    if periodo == "HT":
        total = _corner_total_ht(summary)
    else:
        total = _corner_total_ft(summary)

    if total is None:
        return None

    competencia = (mejor_evento.get("competitions") or [{}])[0]
    competidores = competencia.get("competitors") or []
    equipos_evento = " vs ".join(
        c.get("team", {}).get("displayName", "?") for c in competidores[:2]
    )
    return {
        "event_id": str(event_id),
        "periodo": periodo,
        "corners_final_total": total,
        "match_score": score,
        "match_margin": margin,
        "partido_espn": equipos_evento,
        "estado": (competencia.get("status", {}).get("type", {}) or {}).get("shortDetail"),
    }
