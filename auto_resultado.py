"""
auto_resultado.py
─────────────────
Job automático que resuelve picks pendientes consultando API-Football v3.

Lógica de resolución:
  GOL (live)     → cuenta goles después del minuto del pick.
                   HIT si ocurrieron >= N goles (N = línea del pick).
                   MISS si el partido terminó y no se llegó.

  CORNER (live)  → compara corners totales finales con corners_entrada_total.
                   HIT si la diferencia >= N (N = línea del pick).
                   MISS si el partido terminó y no se llegó.
                   Nota: la API free no devuelve eventos de corner por minuto,
                   solo totales al final, por eso se usa la diferencia total.

  PREPARTIDO     → picks con código PRE_*. Se busca el fixture en una ventana
                   de AUTO_PRE_VENTANA días a partir de la fecha de publicación
                   (el partido puede ser hasta varios días después del pick).
                   GOL prepartido: cuenta goles totales del partido (minuto=0).
                   CORNER prepartido: cuenta corners totales del partido.

  VOID   → si el partido fue cancelado, aplazado o abandonado.

Variables de entorno:
  API_FOOTBALL_KEY  — clave de API-Football (v3.football.api-sports.io)

Configuración:
  AUTO_MIN_HORAS    — mínimo de horas tras publicación para intentar resolver (default 2)
  AUTO_MAX_DIAS     — máximo de días hacia atrás que se revisan (default 10)
  AUTO_MIN_SCORE    — similitud mínima (0-1) para aceptar un partido como coincidencia (default 0.55)
  AUTO_PRE_VENTANA  — días hacia adelante para buscar el partido de un pick prepartido (default 8)
"""

import datetime
import logging
import os
import re
import difflib

import requests

from db import db_actualizar_resultado_confirmado, db_picks_pendientes_revision

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuración
# ──────────────────────────────────────────────────────────────────────────────

API_KEY  = os.getenv("API_FOOTBALL_KEY", "")
API_BASE = "https://v3.football.api-sports.io"

AUTO_MIN_HORAS   = int(os.getenv("AUTO_MIN_HORAS",   "2"))
AUTO_MAX_DIAS    = int(os.getenv("AUTO_MAX_DIAS",    "10"))   # ↑ de 3 a 10 para cubrir prepartidos
AUTO_MIN_SCORE   = float(os.getenv("AUTO_MIN_SCORE", "0.55"))
AUTO_PRE_VENTANA = int(os.getenv("AUTO_PRE_VENTANA", "8"))    # días a buscar hacia adelante para PRE

# Partidos terminados de forma oficial
_STATUS_TERMINADO = {"FT", "AET", "PEN", "AWD"}
# Partidos anulados (→ VOID)
_STATUS_VOID      = {"CANC", "PST", "ABD"}

# Cache en memoria: fecha_str → lista de fixtures
# Se reinicia con cada despliegue; evita llamadas repetidas en la misma sesión.
_cache_fixtures: dict[str, list[dict]] = {}
# Cache de eventos y estadísticas por fixture_id
_cache_eventos:  dict[int, list[dict]] = {}
_cache_stats:    dict[int, dict]       = {}


# ──────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────

def _headers() -> dict:
    return {
        "x-rapidapi-key":  API_KEY,
        "x-rapidapi-host": "v3.football.api-sports.io",
    }


def _get(endpoint: str, params: dict) -> dict:
    """Llamada GET a la API con timeout y manejo de errores."""
    url = f"{API_BASE}/{endpoint}"
    try:
        r = requests.get(url, params=params, headers=_headers(), timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.error("API-Football [%s] error: %s", endpoint, e)
        return {}


# ──────────────────────────────────────────────────────────────────────────────
# Búsqueda de partidos
# ──────────────────────────────────────────────────────────────────────────────

def _fixtures_por_fecha(fecha_str: str) -> list[dict]:
    """
    Devuelve todos los fixtures de una fecha (YYYY-MM-DD).
    La primera llamada consulta la API; las siguientes usan el caché.
    """
    if fecha_str in _cache_fixtures:
        return _cache_fixtures[fecha_str]

    data = _get("fixtures", {"date": fecha_str})
    fixtures = data.get("response", [])
    _cache_fixtures[fecha_str] = fixtures
    logger.info("API-Football: %d partidos para %s", len(fixtures), fecha_str)
    return fixtures


def _normalizar(nombre: str) -> str:
    """Normaliza nombre de equipo para comparación fuzzy."""
    return re.sub(r"[^a-z0-9 ]", "", nombre.lower())


def _similitud(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, _normalizar(a), _normalizar(b)).ratio()


def _buscar_fixture(partido: str, fixtures: list[dict]) -> dict | None:
    """
    Busca el fixture que mejor coincide con el campo 'partido' del pick.
    Acepta formatos "Equipo A vs Equipo B" y "Equipo A - Equipo B".
    Devuelve None si la mejor coincidencia está por debajo del umbral.
    """
    if " vs " in partido:
        local, visitante = partido.split(" vs ", 1)
    elif " - " in partido:
        local, visitante = partido.split(" - ", 1)
    else:
        logger.warning("Formato de partido no reconocido: %r", partido)
        return None

    local, visitante = local.strip(), visitante.strip()
    mejor_score   = 0.0
    mejor_fixture = None

    for f in fixtures:
        home = f["teams"]["home"]["name"]
        away = f["teams"]["away"]["name"]

        score = max(
            (_similitud(local, home) + _similitud(visitante, away)) / 2,
            (_similitud(local, away) + _similitud(visitante, home)) / 2,
        )

        if score > mejor_score:
            mejor_score   = score
            mejor_fixture = f

    if mejor_score >= AUTO_MIN_SCORE and mejor_fixture:
        logger.info(
            "Partido encontrado para '%s': %s vs %s (score=%.2f)",
            partido,
            mejor_fixture["teams"]["home"]["name"],
            mejor_fixture["teams"]["away"]["name"],
            mejor_score,
        )
        return mejor_fixture

    logger.warning(
        "Sin coincidencia para '%s' (mejor score=%.2f < %.2f)",
        partido, mejor_score, AUTO_MIN_SCORE,
    )
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Datos del partido
# ──────────────────────────────────────────────────────────────────────────────

def _eventos_gol(fixture_id: int) -> list[dict]:
    """Devuelve los eventos de tipo Goal del partido (con caché)."""
    if fixture_id in _cache_eventos:
        return _cache_eventos[fixture_id]

    data   = _get("fixtures/events", {"fixture": fixture_id, "type": "Goal"})
    eventos = [
        e for e in data.get("response", [])
        if e.get("detail") not in ("Missed Penalty",)
    ]
    _cache_eventos[fixture_id] = eventos
    return eventos


def _estadisticas(fixture_id: int) -> dict:
    """
    Devuelve estadísticas totales (home + away) del partido (con caché).
    Ejemplo: {"Corner Kicks": 9, "Total Shots": 24, ...}
    """
    if fixture_id in _cache_stats:
        return _cache_stats[fixture_id]

    data     = _get("fixtures/statistics", {"fixture": fixture_id})
    totales: dict[str, int] = {}
    for team_data in data.get("response", []):
        for stat in team_data.get("statistics", []):
            tipo = stat["type"]
            val  = stat["value"]
            totales[tipo] = totales.get(tipo, 0) + (int(val) if val else 0)

    _cache_stats[fixture_id] = totales
    return totales


# ──────────────────────────────────────────────────────────────────────────────
# Parseo de línea
# ──────────────────────────────────────────────────────────────────────────────

def _parse_linea(linea_codigo: str | None) -> int:
    """
    Extrae el número de ocurrencias necesarias del campo linea_codigo.

    Ejemplos:
      "LÍNEA 1" / "LINEA 1"  → 1
      "LÍNEA 2"               → 2
      "OVER 0.5" / "+0.5"    → 1   (necesitas al menos 1)
      "OVER 1.5" / "+1.5"    → 2
      "OVER 2.5" / "+2.5"    → 3
      "CF2" / "GF2"          → 2   (número del código)
    """
    if not linea_codigo:
        return 1

    texto = linea_codigo.upper()

    # "LÍNEA N" o "LINEA N"
    m = re.search(r"L[IÍ]NEA\s+(\d+)", texto)
    if m:
        return int(m.group(1))

    # "OVER N.5" o "+N.5"
    m = re.search(r"(?:OVER|\+)\s*(\d+)\.5", texto)
    if m:
        return int(m.group(1)) + 1

    # Número al final de un código alfanumérico (CF2, GF3, UGM2…)
    m = re.search(r"[A-Z]+(\d+)$", texto)
    if m:
        return int(m.group(1))

    return 1


# ──────────────────────────────────────────────────────────────────────────────
# Resolución por tipo
# ──────────────────────────────────────────────────────────────────────────────

def _resolver_gol(pick: dict, fixture: dict) -> str | None:
    """
    Devuelve "HIT", "MISS", "VOID" o None (partido no terminado aún).
    """
    status = fixture["fixture"]["status"]["short"]

    if status in _STATUS_VOID:
        return "VOID"
    if status not in _STATUS_TERMINADO:
        return None   # partido en curso o no empezado

    fixture_id    = fixture["fixture"]["id"]
    minuto        = pick.get("minuto_alerta") or 0
    linea         = _parse_linea(pick.get("linea_codigo"))
    eventos       = _eventos_gol(fixture_id)

    goles_despues = sum(
        1 for e in eventos
        if (e.get("time", {}).get("elapsed") or 0) > minuto
    )

    resultado = "HIT" if goles_despues >= linea else "MISS"
    logger.debug(
        "GOL | %s | minuto=%s linea=%s goles_despues=%s → %s",
        pick.get("partido"), minuto, linea, goles_despues, resultado,
    )
    return resultado


def _resolver_corner(pick: dict, fixture: dict) -> str | None:
    """
    Devuelve "HIT", "MISS", "VOID" o None (partido no terminado aún).

    Nota: la API free no da eventos de corner por minuto; usamos la
    diferencia entre el total final y los corners en el momento del pick.
    Esto puede dar HIT en picks tardíos donde todos los corners ya
    habían ocurrido, pero es la mejor aproximación disponible sin
    datos granulares.
    """
    status = fixture["fixture"]["status"]["short"]

    if status in _STATUS_VOID:
        return "VOID"
    if status not in _STATUS_TERMINADO:
        return None

    fixture_id      = fixture["fixture"]["id"]
    corners_entrada = pick.get("corners_entrada_total") or 0
    linea           = _parse_linea(pick.get("linea_codigo"))
    stats           = _estadisticas(fixture_id)

    total_corners   = stats.get("Corner Kicks", 0)
    corners_despues = max(0, total_corners - corners_entrada)

    resultado = "HIT" if corners_despues >= linea else "MISS"
    logger.debug(
        "CORNER | %s | corners_entrada=%s total=%s linea=%s corners_despues=%s → %s",
        pick.get("partido"), corners_entrada, total_corners, linea, corners_despues, resultado,
    )
    return resultado


# ──────────────────────────────────────────────────────────────────────────────
# Helpers prepartido
# ──────────────────────────────────────────────────────────────────────────────

def _es_prepartido(pick: dict) -> bool:
    """
    Detecta si el pick es prepartido (código empieza por PRE).
    Los picks prepartido no tienen minuto_alerta ni stats de entrada.
    """
    codigo = (pick.get("codigo") or "").upper()
    return codigo.startswith("PRE")


def _buscar_fixture_ventana(partido: str, fecha_inicio_str: str) -> dict | None:
    """
    Para picks prepartido: busca el fixture en una ventana de AUTO_PRE_VENTANA días.
    Empieza en fecha_inicio y avanza día a día hasta encontrar coincidencia.

    Usa el caché de _fixtures_por_fecha para no duplicar llamadas a la API
    cuando el mismo día ya fue consultado en esta sesión.
    """
    try:
        fecha = datetime.date.fromisoformat(fecha_inicio_str)
    except ValueError:
        logger.warning("Fecha inválida para búsqueda ventana: %r", fecha_inicio_str)
        return None

    for delta in range(AUTO_PRE_VENTANA + 1):
        fecha_str = str(fecha + datetime.timedelta(days=delta))
        fixtures  = _fixtures_por_fecha(fecha_str)
        if not fixtures:
            continue
        fixture = _buscar_fixture(partido, fixtures)
        if fixture:
            logger.info(
                "Prepartido encontrado en fecha %s (delta +%d días desde publicación)",
                fecha_str, delta,
            )
            return fixture

    logger.warning(
        "Sin fixture para prepartido '%s' en ventana de %d días desde %s",
        partido, AUTO_PRE_VENTANA, fecha_inicio_str,
    )
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Job principal
# ──────────────────────────────────────────────────────────────────────────────

async def job_auto_resultado(context) -> None:
    """
    Job periódico (recomendado: cada hora) que resuelve picks pendientes
    consultando API-Football.
    """
    if not API_KEY:
        logger.warning(
            "API_FOOTBALL_KEY no configurada — auto-resolución desactivada. "
            "Añade la variable de entorno en Railway para activarla."
        )
        return

    picks = db_picks_pendientes_revision(
        max_dias  = AUTO_MAX_DIAS,
        min_horas = AUTO_MIN_HORAS,
    )

    if not picks:
        return

    logger.info("Auto-resultado: %d picks pendientes.", len(picks))

    resueltos = 0
    sin_partido = 0
    en_curso = 0
    errores = 0

    for pick in picks:
        try:
            fecha_str   = str(pick["fecha"])
            es_pre      = _es_prepartido(pick)

            if es_pre:
                # Prepartido: el partido ocurre DESPUÉS de la publicación.
                # Buscamos en una ventana de días hacia adelante.
                fixture = _buscar_fixture_ventana(pick.get("partido", ""), fecha_str)
            else:
                # Live: el partido es el mismo día que la alerta.
                fixtures = _fixtures_por_fecha(fecha_str)
                if not fixtures:
                    sin_partido += 1
                    continue
                fixture = _buscar_fixture(pick.get("partido", ""), fixtures)

            if not fixture:
                sin_partido += 1
                continue

            tipo = pick.get("tipo_pick")
            if tipo == "gol":
                resultado = _resolver_gol(pick, fixture)
            elif tipo == "corner":
                resultado = _resolver_corner(pick, fixture)
            else:
                logger.debug("Tipo de pick desconocido: %s", tipo)
                continue

            if resultado is None:
                en_curso += 1
                continue

            ok = db_actualizar_resultado_confirmado(
                pick["message_id_origen"], resultado
            )
            if ok:
                resueltos += 1
                logger.info(
                    "Auto-resultado ✅ | %s → %s | %s (min %s, línea %s)",
                    pick["message_id_origen"],
                    resultado,
                    pick.get("partido"),
                    pick.get("minuto_alerta"),
                    pick.get("linea_codigo"),
                )

        except Exception as e:
            errores += 1
            logger.error(
                "Error auto-resolviendo pick %s: %s",
                pick.get("message_id_origen"), e,
            )

    logger.info(
        "Auto-resultado completado: %d resueltos | %d en curso | %d sin partido | %d errores",
        resueltos, en_curso, sin_partido, errores,
    )
