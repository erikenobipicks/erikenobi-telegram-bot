"""
scorer.py
─────────
Motor estadístico interno (Opción A) — sin IA externa.

Calcula un score dinámico 0–100 basado en el historial de la DB,
usando shrinkage bayesiana para estabilizar estimaciones con muestras
pequeñas (regresión al prior del 70 %).

Las 4 dimensiones y sus pesos:
  código   40 %  — el modelo específico es la señal más fiable
  liga     30 %  — algunas ligas tienen patrones distintos
  hora     15 %  — franja horaria de la alerta (hora de Madrid)
  minuto   15 %  — bucket de minuto dentro del partido

Si una dimensión no alcanza la muestra mínima (_MIN_MUESTRA), su peso
se redistribuye proporcionalmente entre las dimensiones con suficiente
historial. Si ninguna dimensión es fiable, devuelve score = prior y
confianza = "baja".
"""

import logging

from utils import ahora_madrid
from db import db_score_por_dimension

logger = logging.getLogger(__name__)


# ── Hiperparámetros ───────────────────────────────────────────────────────────

_PRIOR_WR   = 0.70   # tasa de acierto a priori (prior bayesiano)
_PRIOR_N    = 10     # fuerza del prior (picks ficticios equivalentes)
_MIN_MUESTRA = 8     # mínimo de picks resueltos para confiar en la dimensión
_SCORE_UP   = 78     # score ≥ este umbral → stake +0.5u
_SCORE_DOWN = 58     # score < este umbral → stake −0.5u (solo si confianza media/alta)
_DIAS       = 90     # ventana de lookback en días

# Pesos nominales por dimensión (deben sumar 1.0)
_PESOS = {
    "codigo":  0.40,
    "liga":    0.30,
    "hora":    0.15,
    "minuto":  0.15,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bayes(hits: int, total: int) -> float:
    """
    Shrinkage bayesiana: combina los datos observados con un prior.
    Devuelve el WR estimado en porcentaje (0–100).
    """
    return (_PRIOR_WR * _PRIOR_N + hits) / (_PRIOR_N + total) * 100


def _bucket_minuto(minuto: int) -> tuple[int, int]:
    """Asigna el minuto a un bucket de ~15 min y devuelve (min, max)."""
    if minuto <= 20:  return (0,  20)
    if minuto <= 35:  return (21, 35)
    if minuto <= 50:  return (36, 50)
    if minuto <= 65:  return (51, 65)
    if minuto <= 80:  return (66, 80)
    return (81, 120)


# ══════════════════════════════════════════════════════════════════════════════
# API pública
# ══════════════════════════════════════════════════════════════════════════════

def calcular_score(datos: dict, tipo_pick: str) -> dict:
    """
    Calcula el score estadístico de la alerta a partir del historial de la DB.

    Parámetros:
        datos      — dict devuelto por extraer_datos()
        tipo_pick  — "gol" | "corner"

    Devuelve un dict con:
        score       int         0–100
        confianza   str         "alta" | "media" | "baja"
        señales     list[str]   líneas explicativas por dimensión
        stake_delta float       ajuste sugerido al stake: +0.5, 0.0 o −0.5
        n_total     int         número total de picks usados en el cálculo
    """
    codigo = datos.get("codigo")
    liga   = datos.get("liga")
    minuto = datos.get("minuto") or 0
    hora   = ahora_madrid().hour

    min_min, min_max = _bucket_minuto(minuto)

    # ── 1. Consultar cada dimensión ───────────────────────────────────────────
    raw: dict[str, tuple[int, int]] = {}

    if codigo:
        raw["codigo"] = db_score_por_dimension(tipo_pick, codigo=codigo, dias=_DIAS)

    if liga:
        raw["liga"] = db_score_por_dimension(tipo_pick, liga=liga, dias=_DIAS)

    raw["hora"] = db_score_por_dimension(tipo_pick, hora=hora, dias=_DIAS)

    if minuto:
        raw["minuto"] = db_score_por_dimension(
            tipo_pick, minuto_min=min_min, minuto_max=min_max, dias=_DIAS
        )

    # ── 2. Filtrar dimensiones con muestra suficiente ─────────────────────────
    activas: dict[str, float] = {}   # dim → score bayesiano (0–100)
    pesos_activos: dict[str, float] = {}
    señales: list[str] = []
    n_total = 0

    for dim, (hits, total) in raw.items():
        n_total += total
        if total >= _MIN_MUESTRA:
            wr = _bayes(hits, total)
            activas[dim]       = wr
            pesos_activos[dim] = _PESOS.get(dim, 0.0)
            señales.append(f"{dim.capitalize()}: {wr:.0f}% ({hits}/{total})")

    # ── 3. Sin historial suficiente en ninguna dimensión ─────────────────────
    if not activas:
        return {
            "score":       int(round(_PRIOR_WR * 100)),
            "confianza":   "baja",
            "señales":     ["Sin historial suficiente"],
            "stake_delta": 0.0,
            "n_total":     n_total,
        }

    # ── 4. Normalizar pesos y calcular score ponderado ────────────────────────
    suma_pesos = sum(pesos_activos.values())
    score_f = sum(
        activas[d] * (pesos_activos[d] / suma_pesos)
        for d in activas
    )
    score = int(round(score_f))

    # ── 5. Nivel de confianza ─────────────────────────────────────────────────
    n_dims = len(activas)
    n_picks_usados = sum(raw[d][1] for d in activas)

    if n_dims >= 3 or n_picks_usados >= 25:
        confianza = "alta"
    elif n_dims >= 2 or n_picks_usados >= _MIN_MUESTRA:
        confianza = "media"
    else:
        confianza = "baja"

    # ── 6. Ajuste de stake ────────────────────────────────────────────────────
    stake_delta = 0.0
    if confianza in ("alta", "media"):
        if score >= _SCORE_UP:
            stake_delta = +0.5
        elif score < _SCORE_DOWN:
            stake_delta = -0.5

    return {
        "score":       score,
        "confianza":   confianza,
        "señales":     señales,
        "stake_delta": stake_delta,
        "n_total":     n_total,
    }
