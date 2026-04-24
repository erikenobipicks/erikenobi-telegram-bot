"""
clasificador_alertas.py
───────────────────────
Clasifica una alerta (datos dict del extractor) en 4 niveles:

  3 → ÉLITE     🔵  stake 3u
  2 → ALTO      🟢  stake 2u
  1 → FAVORABLE 🟡  stake 1u
  0 → BAJO      🔴  stake 0.5u (o 0u si cuota baja)

Usa directamente el dict devuelto por extraer_datos() y las funciones
de detección de extractor.py — sin parsers ni dependencias adicionales.

Tras la clasificación por reglas, el motor estadístico interno (scorer.py)
calcula un score 0–100 basado en el historial de la DB y ajusta el stake
±0.5u cuando la confianza es media/alta.
"""

import logging
import re

from utils import parse_percent, parse_dupla_numerica
from extractor import (
    detectar_linea_por_codigo,
    detectar_modo_por_codigo,
)
from config import CUOTA_MIN_BAJO

logger = logging.getLogger(__name__)

# ── Metadatos históricos ───────────────────────────────────────────────────────
_NIVELES = {
    3: {"nombre": "ÉLITE",     "emoji": "🔵", "wr": 96.0, "n": 25},
    2: {"nombre": "ALTO",      "emoji": "🟢", "wr": 86.3, "n": 51},
    1: {"nombre": "FAVORABLE", "emoji": "🟡", "wr": 72.4, "n": 243},
    0: {"nombre": "BAJO",      "emoji": "🔴", "wr": 60.2, "n": 201},
}

# Términos que activan nivel ALTO (sobre el texto efectivo = codigo+linea+modo)
_TERMINOS_ALTO = ["línea 3", "línea 4", "+1.5", "over 1.5", "cf3", "gf3", "cf4", "gf4"]

# Términos que excluyen nivel FAVORABLE
_TERMINOS_EXCLUIDOS = ["línea 1", "over 0.5", "+0.5", "cf1", "gf1"]

# ── Umbrales de clasificación — extraídos como constantes para facilitar
#    su ajuste sin modificar la lógica interna. ────────────────────────────────

# ÉLITE
_ELITE_SL_MIN    = 80    # strike_liga mínimo (%)
_ELITE_SL_MAX    = 90    # strike_liga máximo (%) — evita outliers estadísticos
_ELITE_SA_MIN    = 77    # strike_alerta mínimo (%)
_ELITE_MIN_MIN   = 45    # minuto mínimo
_ELITE_MIN_MAX   = 72    # minuto máximo

# ALTO
_ALTO_CUOTA_MIN  = 1.40  # cuota local mínima
_ALTO_CUOTA_MAX  = 2.20  # cuota local máxima

# FAVORABLE
_FAV_MOMENTUM_MIN = -10  # diferencia momentum (local − visitante) mínima
_FAV_MIN_MAX      = 85   # minuto máximo
_FAV_CORNER_SL_MIN = 75  # strike_liga mínimo para que un corner ignore el límite de minuto


# ══════════════════════════════════════════════════════════════════════════════
# NG1 — CLASIFICADOR ESPECÍFICO
# ══════════════════════════════════════════════════════════════════════════════

# Umbrales NG1
_NG1_ELITE_MIN_MIN        = 50
_NG1_ELITE_MIN_MAX        = 58
_NG1_ELITE_XG_MIN         = 0.3
_NG1_ELITE_CUOTA_EXCL_MIN = 1.25   # rango de cuota excluido en ÉLITE (yield negativo)
_NG1_ELITE_CUOTA_EXCL_MAX = 1.35

_NG1_ALTO_MIN_MIN  = 50
_NG1_ALTO_MIN_MAX  = 62
_NG1_ALTO_XG_MIN   = 0.0

_NG1_DESCARTE_CUOTA_MIN = 1.25    # rango de cuota → yield -15% confirmado
_NG1_DESCARTE_CUOTA_MAX = 1.35

_NG1_DOBLE_CUOTA_MIN = 1.38       # cuota +0.5 mínima para sugerir doble entrada
_NG1_DOBLE_CUOTA_MAX = 1.45       # cuota +0.5 máxima para sugerir doble entrada

# Win rates históricos NG1 (807 picks, 197 días)
_NG1_NIVELES = {
    "ÉLITE":     {"emoji": "🔵", "wr": 86.31, "n": 807},
    "ALTO":      {"emoji": "🟢", "wr": 86.31, "n": 807},
    "FAVORABLE": {"emoji": "🟡", "wr": 83.98, "n": 807},
    "BAJO":      {"emoji": "🔴", "wr": 81.29, "n": 807},
}


def _es_ng1(datos: dict) -> bool:
    return (datos.get("codigo") or "").upper() == "NG1"


def _liga_en_blacklist_ng1(liga: str | None) -> bool:
    if not liga:
        return False
    liga_lower = liga.lower()
    return any(bl in liga_lower for bl in BLACKLIST_NG1)


def _xg_diff_ng1(datos: dict) -> float | None:
    """Devuelve xG_home - xG_away o None si no hay datos."""
    xg_home = datos.get("xg_home")
    xg_away = datos.get("xg_away")
    if xg_home is not None and xg_away is not None:
        return round(float(xg_home) - float(xg_away), 3)
    return None


def _cuota_over05(datos: dict) -> float | None:
    """Extrae la cuota del mercado Over 0.5 (línea +0.5 asiática en NG1)."""
    raw = datos.get("odds_over_0_5")
    if not raw:
        return None
    try:
        partes = re.split(r"[\s|]+", str(raw).strip())
        return float(partes[0].replace(",", ".")) if partes else None
    except (ValueError, IndexError):
        return None


def _cuota_over15(datos: dict) -> float | None:
    """Extrae la cuota del mercado Over 1.5 (proxy de línea +1 asiática en NG1)."""
    raw = datos.get("odds_over_1_5")
    if not raw:
        return None
    try:
        partes = re.split(r"[\s|]+", str(raw).strip())
        return float(partes[0].replace(",", ".")) if partes else None
    except (ValueError, IndexError):
        return None


def _score_empatado_ng1(datos: dict) -> bool:
    """True si el marcador actual es 1-1 o 2-2 (mayor probabilidad de gol extra)."""
    goles_raw = datos.get("goles") or ""
    partes = [p.strip() for p in goles_raw.replace("-", " ").split() if p.strip().isdigit()]
    if len(partes) >= 2:
        try:
            loc, vis = int(partes[0]), int(partes[1])
            return loc == vis and loc in (1, 2)
        except ValueError:
            pass
    return False


def _calcular_doble_entrada_ng1(datos: dict, nivel_nombre: str) -> str | None:
    """
    Devuelve el texto de sugerencia de doble entrada o None.
    Solo aplica en ÉLITE/ALTO + cuota_05 en [1.38, 1.45) + marcador 1-1 ó 2-2.
    """
    if nivel_nombre not in ("ÉLITE", "ALTO"):
        return None

    cuota_05 = _cuota_over05(datos)
    if cuota_05 is None:
        return None

    if cuota_05 < _NG1_DOBLE_CUOTA_MIN:
        # Cuota baja → solo línea +1 (no recomendar doble)
        return None
    if cuota_05 >= _NG1_DOBLE_CUOTA_MAX:
        # Cuota alta → solo +0.5 paga más (no recomendar doble)
        return None

    if not _score_empatado_ng1(datos):
        return None

    cuota_1   = _cuota_over15(datos)
    cuota_1_txt  = f"{cuota_1:.2f}" if cuota_1 else "?"
    cuota_05_txt = f"{cuota_05:.2f}"

    return (
        f"💡 Sugerencia: Doble entrada posible\n"
        f"    - Línea +1 a {cuota_1_txt}: 0.7u\n"
        f"    - Línea +0.5 a {cuota_05_txt}: 0.7u"
    )


# Mapa de nivel numérico NG1: coincide con el convenio general (3=máximo, 0=no apostar)
_NG1_NIVEL_NUM = {"ÉLITE": 3, "ALTO": 2, "FAVORABLE": 1, "BAJO": 0}


def _resultado_ng1(nombre: str, stake: float, razones: list[str],
                   xg_diff, datos: dict, advertencia: str | None = None) -> dict:
    info = _NG1_NIVELES[nombre]
    return {
        "nivel":        _NG1_NIVEL_NUM.get(nombre, 1),
        "nombre":       nombre,
        "emoji":        info["emoji"],
        "stake":        stake,
        "razones":      razones,
        "wr":           info["wr"],
        "n":            info["n"],
        "advertencia":  advertencia,
        "xg_diff":      xg_diff,
        "doble_entrada": _calcular_doble_entrada_ng1(datos, nombre),
        "score_info":   None,
        "es_ng1":       True,
    }


def clasificar_ng1(datos: dict) -> dict:
    """
    Clasificador override para picks NG1 (Next Goal, Over 0.5 desde la alerta).

    Niveles:
      ÉLITE     → 1.5u  (Timer 50-58, xG diff ≥ 0.3, fuera de zona muerta, liga OK)
      ALTO      → 1.2u  (Timer 50-62, xG diff ≥ 0, liga OK)
      FAVORABLE → 1.0u  (fallback — filtros básicos InPlayGuru)
      BAJO      → 0u    (cuota en zona yield -15%)

    El filtro de liga se aplica DESPUÉS de esta función mediante el sistema
    dinámico de estrategia_liga_stats (multiplicador de tier por liga).
    """
    liga    = datos.get("liga")
    minuto  = datos.get("minuto") or 0
    cuota   = _cuota_over05(datos)
    xg_diff = _xg_diff_ng1(datos)

    # ── DESCARTE: cuota en zona muerta (yield -15% confirmado) ────────
    # El filtro de liga ya no es estático aquí; lo gestiona el sistema dinámico.
    if cuota is not None and _NG1_DESCARTE_CUOTA_MIN <= cuota <= _NG1_DESCARTE_CUOTA_MAX:
        return _resultado_ng1(
            "BAJO", 0.0,
            [f"Cuota Over 0.5 ({cuota}) en zona de yield negativo "
             f"({_NG1_DESCARTE_CUOTA_MIN}-{_NG1_DESCARTE_CUOTA_MAX})"],
            xg_diff, datos,
            advertencia="yield -15% confirmado en esta franja de cuotas",
        )

    # ── ÉLITE ─────────────────────────────────────────────────────────
    cuota_fuera_zona = cuota is None or not (_NG1_ELITE_CUOTA_EXCL_MIN <= cuota <= _NG1_ELITE_CUOTA_EXCL_MAX)
    if (
        _NG1_ELITE_MIN_MIN <= minuto <= _NG1_ELITE_MIN_MAX
        and xg_diff is not None and xg_diff >= _NG1_ELITE_XG_MIN
        and cuota_fuera_zona
    ):
        razones = [
            f"Timer {minuto}' (rango {_NG1_ELITE_MIN_MIN}-{_NG1_ELITE_MIN_MAX}')",
            f"xG diff: +{xg_diff:.2f} (Home favorito ≥ {_NG1_ELITE_XG_MIN})",
        ]
        if cuota:
            razones.append(f"Cuota {cuota:.2f} fuera de zona muerta")
        return _resultado_ng1("ÉLITE", 1.5, razones, xg_diff, datos)

    # ── ALTO ──────────────────────────────────────────────────────────
    if (
        _NG1_ALTO_MIN_MIN <= minuto <= _NG1_ALTO_MIN_MAX
        and xg_diff is not None and xg_diff >= _NG1_ALTO_XG_MIN
    ):
        razones = [
            f"Timer {minuto}' (rango {_NG1_ALTO_MIN_MIN}-{_NG1_ALTO_MIN_MAX}')",
            f"xG diff: {xg_diff:+.2f}",
        ]
        return _resultado_ng1("ALTO", 1.2, razones, xg_diff, datos)

    # ── FAVORABLE (fallback) ──────────────────────────────────────────
    razones_fav = ["Filtros básicos InPlayGuru"]
    if xg_diff is None:
        razones_fav.append("Sin dato xG disponible")
    elif minuto < _NG1_ALTO_MIN_MIN or minuto > _NG1_ALTO_MIN_MAX:
        razones_fav.append(f"Timer {minuto}' fuera del rango óptimo ({_NG1_ALTO_MIN_MIN}-{_NG1_ALTO_MIN_MAX}')")
    elif xg_diff < _NG1_ALTO_XG_MIN:
        razones_fav.append(f"xG diff {xg_diff:+.2f} por debajo del umbral")
    return _resultado_ng1("FAVORABLE", 1.0, razones_fav, xg_diff, datos)


# ══════════════════════════════════════════════════════════════════════════════
# API pública
# ══════════════════════════════════════════════════════════════════════════════

def clasificar_alerta(datos: dict, tipo_pick: str) -> dict:
    """
    Clasifica la alerta y devuelve un dict con:
        nivel       int         0-3
        nombre      str         ÉLITE / ALTO / FAVORABLE / BAJO
        emoji       str         🔵 🟢 🟡 🔴
        stake       float       unidades sugeridas (ajustado por score)
        razones     list[str]   explicación del nivel asignado
        wr          float       win rate histórico (%)
        n           int         tamaño de muestra
        advertencia str | None  aviso adicional
        score_info  dict | None resultado del motor estadístico interno

    Para picks NG1 se usa el clasificador específico (sin score estadístico).
    """
    # ── NG1: clasificador override ────────────────────────────────────
    if _es_ng1(datos):
        return clasificar_ng1(datos)

    resultado = _clasificar_base(datos, tipo_pick)

    # ── Motor estadístico interno ─────────────────────────────────────
    # Importación diferida para evitar ciclos en el arranque
    try:
        from scorer import calcular_score
        score_info = calcular_score(datos, tipo_pick)
        resultado["score_info"] = score_info

        delta = score_info.get("stake_delta", 0.0)
        if delta != 0.0:
            nuevo_stake = max(0.0, min(3.0, resultado["stake"] + delta))
            resultado["stake"] = round(nuevo_stake, 1)
            logger.debug(
                "Score %s (%s) → stake ajustado %+.1fu → %.1fu",
                score_info["score"],
                score_info["confianza"],
                delta,
                resultado["stake"],
            )
    except Exception as e:
        logger.warning("Error en motor estadístico: %s", e)
        resultado["score_info"] = None

    return resultado


def _clasificar_base(datos: dict, tipo_pick: str) -> dict:
    """Clasificación por reglas fijas (sin score estadístico)."""
    texto_ef = _texto_efectivo(datos)
    minuto   = datos.get("minuto") or 0

    sa       = parse_percent(datos.get("strike_alerta"))   # int o None
    sl       = parse_percent(datos.get("strike_liga"))     # int o None
    cuota_1  = _parse_cuota_1(datos)
    ml, mv   = _parse_momentum(datos)

    momentum_diff = (ml - mv) if (ml is not None and mv is not None) else 0

    # ── ÉLITE — nivel 3 ───────────────────────────────────────────────
    if (
        sl is not None and _ELITE_SL_MIN <= sl <= _ELITE_SL_MAX
        and sa is not None and sa >= _ELITE_SA_MIN
        and _ELITE_MIN_MIN <= minuto <= _ELITE_MIN_MAX
    ):
        razones = [
            f"Strike liga: {sl}% (rango {_ELITE_SL_MIN}-{_ELITE_SL_MAX}%)",
            f"Strike alerta: {sa}% (≥ {_ELITE_SA_MIN}%)",
            f"Minuto: {minuto}' (rango {_ELITE_MIN_MIN}-{_ELITE_MIN_MAX}')",
        ]
        return _resultado(3, 3.0, razones)

    # ── ALTO — nivel 2 ────────────────────────────────────────────────
    termino_alto = next((t for t in _TERMINOS_ALTO if t in texto_ef), None)
    if (
        termino_alto is not None
        and cuota_1 is not None
        and _ALTO_CUOTA_MIN <= cuota_1 <= _ALTO_CUOTA_MAX
    ):
        razones = [
            f"Apuesta de alto valor: '{termino_alto}'",
            f"Cuota local: {cuota_1} (rango {_ALTO_CUOTA_MIN}-{_ALTO_CUOTA_MAX})",
        ]
        return _resultado(2, 2.0, razones)

    # ── FAVORABLE — nivel 1 ───────────────────────────────────────────
    excluido = next((t for t in _TERMINOS_EXCLUIDOS if t in texto_ef), None)
    # Los corners con strike_liga alto son válidos a cualquier minuto:
    # el historial de la liga predice el resultado mejor que el minuto.
    corner_alta_confianza = (
        tipo_pick == "corner"
        and sl is not None
        and sl >= _FAV_CORNER_SL_MIN
    )
    if (
        excluido is None
        and momentum_diff >= _FAV_MOMENTUM_MIN
        and (minuto <= _FAV_MIN_MAX or corner_alta_confianza)
    ):
        if corner_alta_confianza and minuto > _FAV_MIN_MAX:
            razones = [f"Corner alta confianza: strike_liga {sl}% (≥ {_FAV_CORNER_SL_MIN}%) — minuto ignorado"]
        else:
            razones = [f"Minuto: {minuto}' (≤ {_FAV_MIN_MAX}')"]
        if ml is not None:
            razones.append(
                f"Momentum: {ml}-{mv} (diferencia {momentum_diff:+})"
            )
        else:
            razones.append("Momentum sin dato — se acepta")
        return _resultado(1, 1.0, razones)

    # ── BAJO — nivel 0 ────────────────────────────────────────────────
    advert = None
    if cuota_1 is None:
        stake, advert = 0.5, "verificar cuota antes de apostar"
    elif cuota_1 >= CUOTA_MIN_BAJO:
        stake = 0.5
    else:
        stake = 0.0

    razones_bajo = ["No cumple criterios de niveles superiores"]
    if excluido:
        razones_bajo.append(f"Apuesta excluida: '{excluido}'")
    if minuto > _FAV_MIN_MAX:
        razones_bajo.append(f"Minuto {minuto}' fuera de rango (> {_FAV_MIN_MAX}')")
    if momentum_diff < _FAV_MOMENTUM_MIN:
        razones_bajo.append(f"Momentum desfavorable ({momentum_diff:+})")

    return _resultado(0, stake, razones_bajo, advertencia=advert)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _texto_efectivo(datos: dict) -> str:
    """
    Texto combinado para buscar términos de clasificación.
    Combina: código de modelo + línea detectada + modo detectado.
    """
    codigo = (datos.get("codigo") or "").lower()
    linea  = (detectar_linea_por_codigo(datos) or "").lower()
    modo   = (detectar_modo_por_codigo(datos) or "").lower()
    return f"{codigo} {linea} {modo}"


def _parse_cuota_1(datos: dict) -> float | None:
    """Extrae la primera cuota (local) del campo odds_1x2."""
    odds_raw = datos.get("odds_1x2")
    if not odds_raw:
        return None
    # Acepta formatos: "1.80 - 3.50 - 4.20" | "1.80 | 3.50 | 4.20"
    m = re.match(r"([0-9]+(?:[.,][0-9]+)?)", str(odds_raw).strip())
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    return None


def _parse_momentum(datos: dict) -> tuple[int | None, int | None]:
    """Extrae (local, visitante) del campo momentum."""
    dupla = parse_dupla_numerica(datos.get("momentum") or "")
    if dupla:
        return dupla
    return None, None


def _resultado(
    nivel: int,
    stake: float,
    razones: list[str],
    advertencia: str | None = None,
) -> dict:
    info = _NIVELES[nivel]
    return {
        "nivel":       nivel,
        "nombre":      info["nombre"],
        "emoji":       info["emoji"],
        "stake":       stake,
        "razones":     razones,
        "wr":          info["wr"],
        "n":           info["n"],
        "advertencia": advertencia,
    }
