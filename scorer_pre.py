"""
scorer_pre.py
─────────────
Motor estadístico para picks prepartido (PRE_*).

Misma arquitectura bayesiana que scorer.py pero adaptada a prepartido:
  - Sin dimensión de minuto (PRE no tiene minuto en directo).
  - Añade dimensión de rango de cuota (odds_range).
  - Ventana temporal más larga (365 días) porque se generan menos picks.
  - Prior más conservador (~55 % WR esperado en Over 2.5 prepartido).

Dimensiones y pesos:
  codigo      45 %  — rendimiento global de la estrategia PRE
  liga        35 %  — esa estrategia funciona en esta liga concreta?
  odds_range  20 %  — el rango de cuota marca diferencias?

El stake base siempre es 1u. El scorer ajusta ±0.5u cuando la confianza
es media o alta, con un suelo de 0.5u y un techo de 1.5u.
El guardián de ROI negativo (en estadisticas.py) devuelve 0u antes de
llegar aquí si la estrategia acumula pérdidas sostenidas.
"""

import logging

from db import db_score_pre_por_dimension

logger = logging.getLogger(__name__)


# ── Hiperparámetros ───────────────────────────────────────────────────────────

_PRIOR_WR    = 0.55   # WR a priori para PRE Over 2.5 (~55 % en general)
_PRIOR_N     = 8      # fuerza del prior (picks ficticios equivalentes)
_MIN_MUESTRA = 6      # mínimo de picks resueltos para confiar en una dimensión
_SCORE_UP    = 72     # score ≥ este umbral → stake +0.5u
_SCORE_DOWN  = 48     # score < este umbral → stake −0.5u
_DIAS        = 365    # ventana anual

_PESOS = {
    "codigo":     0.45,
    "liga":       0.35,
    "odds_range": 0.20,
}

# Buckets de cuota: (min_inclusive, max_exclusive)  — None = sin techo
_ODDS_BUCKETS: list[tuple[float, float | None]] = [
    (1.40, 1.70),
    (1.70, 1.85),
    (1.85, 2.00),
    (2.00, 2.50),
    (2.50, None),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _bayes(hits: int, total: int) -> float:
    """Shrinkage bayesiana: combina datos reales con el prior."""
    return (_PRIOR_WR * _PRIOR_N + hits) / (_PRIOR_N + total) * 100


def _odds_bucket(odds: float) -> tuple[float, float | None]:
    """Devuelve el bucket (min, max) al que pertenece la cuota."""
    for mn, mx in _ODDS_BUCKETS:
        if mx is None or odds < mx:
            return (mn, mx)
    return _ODDS_BUCKETS[-1]


# ══════════════════════════════════════════════════════════════════════════════
# API pública
# ══════════════════════════════════════════════════════════════════════════════

def calcular_score_pre(
    codigo: str,
    tipo_pick: str,
    liga: str | None = None,
    odds: float | None = None,
) -> dict:
    """
    Calcula el score estadístico de un pick prepartido a partir del historial.

    Parámetros:
        codigo    — código PRE exacto, ej. "PRE_O25FT"
        tipo_pick — "gol" | "corner"
        liga      — nombre de la liga (opcional)
        odds      — cuota real del pick (opcional)

    Devuelve un dict con:
        score       int         0–100
        confianza   str         "alta" | "media" | "baja"
        senales     list[str]   líneas explicativas por dimensión
        stake_delta float       ajuste sugerido: +0.5, 0.0 o −0.5
        n_total     int         picks usados en el cálculo
    """
    raw: dict[str, tuple[int, int]] = {}

    # Dimensión código: rendimiento global de la estrategia
    raw["codigo"] = db_score_pre_por_dimension(codigo=codigo, dias=_DIAS)

    # Dimensión liga: esa estrategia en esa liga concreta
    if liga:
        raw["liga"] = db_score_pre_por_dimension(
            codigo=codigo, liga=liga, dias=_DIAS
        )

    # Dimensión odds_range: bucket de cuota
    if odds is not None:
        mn, mx = _odds_bucket(odds)
        raw["odds_range"] = db_score_pre_por_dimension(
            codigo=codigo, odds_min=mn, odds_max=mx, dias=_DIAS
        )

    # ── Filtrar dimensiones con muestra suficiente ─────────────────────────
    activas: dict[str, float] = {}
    pesos_activos: dict[str, float] = {}
    senales: list[str] = []
    n_total = 0

    for dim, (hits, total) in raw.items():
        n_total += total
        if total >= _MIN_MUESTRA:
            wr = _bayes(hits, total)
            activas[dim]       = wr
            pesos_activos[dim] = _PESOS.get(dim, 0.0)
            senales.append(f"{dim}: {wr:.0f}% ({hits}/{total})")

    # ── Sin historial suficiente ───────────────────────────────────────────
    if not activas:
        return {
            "score":       int(round(_PRIOR_WR * 100)),
            "confianza":   "baja",
            "senales":     ["Sin historial PRE suficiente"],
            "stake_delta": 0.0,
            "n_total":     n_total,
        }

    # ── Score ponderado ────────────────────────────────────────────────────
    suma_pesos = sum(pesos_activos.values())
    score_f = sum(
        activas[d] * (pesos_activos[d] / suma_pesos)
        for d in activas
    )
    score = int(round(score_f))

    # ── Nivel de confianza ─────────────────────────────────────────────────
    n_dims   = len(activas)
    n_picks  = sum(raw[d][1] for d in activas)

    if n_dims >= 3 or n_picks >= 25:
        confianza = "alta"
    elif n_dims >= 2 or n_picks >= _MIN_MUESTRA:
        confianza = "media"
    else:
        confianza = "baja"

    # ── Ajuste de stake ────────────────────────────────────────────────────
    stake_delta = 0.0
    if confianza in ("alta", "media"):
        if score >= _SCORE_UP:
            stake_delta = +0.5
        elif score < _SCORE_DOWN:
            stake_delta = -0.5

    logger.debug(
        "scorer_pre | %s | score=%d | confianza=%s | delta=%+.1f | dims=%s",
        codigo, score, confianza, stake_delta, senales,
    )

    return {
        "score":       score,
        "confianza":   confianza,
        "senales":     senales,
        "stake_delta": stake_delta,
        "n_total":     n_total,
    }
