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
"""

import re

from utils import parse_percent, parse_dupla_numerica
from extractor import (
    detectar_linea_por_codigo,
    detectar_modo_por_codigo,
)
from config import CUOTA_MIN_BAJO

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


# ══════════════════════════════════════════════════════════════════════════════
# API pública
# ══════════════════════════════════════════════════════════════════════════════

def clasificar_alerta(datos: dict, tipo_pick: str) -> dict:
    """
    Clasifica la alerta y devuelve un dict con:
        nivel       int         0-3
        nombre      str         ÉLITE / ALTO / FAVORABLE / BAJO
        emoji       str         🔵 🟢 🟡 🔴
        stake       float       unidades sugeridas
        razones     list[str]   explicación del nivel asignado
        wr          float       win rate histórico (%)
        n           int         tamaño de muestra
        advertencia str | None  aviso adicional
    """
    texto_ef = _texto_efectivo(datos)
    minuto   = datos.get("minuto") or 0

    sa       = parse_percent(datos.get("strike_alerta"))   # int o None
    sl       = parse_percent(datos.get("strike_liga"))     # int o None
    cuota_1  = _parse_cuota_1(datos)
    ml, mv   = _parse_momentum(datos)

    momentum_diff = (ml - mv) if (ml is not None and mv is not None) else 0

    # ── ÉLITE — nivel 3 ───────────────────────────────────────────────
    if (
        sl is not None and 80 <= sl <= 90
        and sa is not None and sa >= 77
        and 45 <= minuto <= 72
    ):
        razones = [
            f"Strike liga: {sl}% (rango 80-90%)",
            f"Strike alerta: {sa}% (≥ 77%)",
            f"Minuto: {minuto}' (rango 45-72')",
        ]
        return _resultado(3, 3.0, razones)

    # ── ALTO — nivel 2 ────────────────────────────────────────────────
    termino_alto = next((t for t in _TERMINOS_ALTO if t in texto_ef), None)
    if (
        termino_alto is not None
        and cuota_1 is not None
        and 1.40 <= cuota_1 <= 2.20
    ):
        razones = [
            f"Apuesta de alto valor: '{termino_alto}'",
            f"Cuota local: {cuota_1} (rango 1.40-2.20)",
        ]
        return _resultado(2, 2.0, razones)

    # ── FAVORABLE — nivel 1 ───────────────────────────────────────────
    excluido = next((t for t in _TERMINOS_EXCLUIDOS if t in texto_ef), None)
    if (
        excluido is None
        and momentum_diff >= -10
        and minuto <= 72
    ):
        razones = [f"Minuto: {minuto}' (≤ 72')"]
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
    if minuto > 72:
        razones_bajo.append(f"Minuto {minuto}' fuera de rango (> 72')")
    if momentum_diff < -10:
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
