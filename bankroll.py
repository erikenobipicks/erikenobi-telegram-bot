"""
bankroll.py
───────────
Gestión del bankroll y cálculo de stake para picks prepartido.

Sistema activo: Ganador Local (1X prepartido)
  • Stake base:  2% del bankroll
  • Rango válido: 1.70 – 2.60
  • Multiplicadores por rango de cuota:
      < 1.70        → fuera de rango (no se muestra stake)
      1.70 – 1.80   → ×1.0
      1.80 – 1.90   → ×0.5  (rango problemático)
      1.90 – 2.60   → ×1.0
      > 2.60        → fuera de rango (no se muestra stake)

Bankroll actualizable con:
  • Comando /bankroll <importe>  (solo admins)
  • Subiendo un Excel al bot en chat privado (admin)
    El Excel debe tener el bankroll en la celda A1 de la primera hoja.
"""

import json
import logging
import os

logger = logging.getLogger(__name__)

BANKROLL_FILE    = "bankroll.json"
DEFAULT_BANKROLL = 1000.0
STAKE_PCT        = 0.02   # 2%

# Rango operativo
CUOTA_MIN = 1.70
CUOTA_MAX = 2.60

# Multiplicadores: lista de (limite_superior_exclusivo, multiplicador)
# El último tramo usa CUOTA_MAX como límite.
_TRAMOS = [
    (1.70, 0.0),   # < 1.70 → fuera de rango
    (1.80, 1.0),   # 1.70–1.80
    (1.90, 0.5),   # 1.80–1.90  ← rango problemático
    (2.60, 1.0),   # 1.90–2.60
]


# ══════════════════════════════════════════════════════
# PERSISTENCIA DEL BANKROLL
# ══════════════════════════════════════════════════════

def get_bankroll() -> float:
    """Lee el bankroll desde PostgreSQL. Fallback al archivo local si la DB falla."""
    try:
        from db import db_get_bankroll
        return db_get_bankroll(default=DEFAULT_BANKROLL)
    except Exception as e:
        logger.warning(f"No se pudo leer bankroll de DB, usando archivo local: {e}")
    try:
        with open(BANKROLL_FILE, "r") as f:
            data = json.load(f)
        return float(data.get("bankroll", DEFAULT_BANKROLL))
    except FileNotFoundError:
        return DEFAULT_BANKROLL
    except Exception as e:
        logger.error(f"Error leyendo bankroll: {e}")
        return DEFAULT_BANKROLL


def set_bankroll(valor: float) -> None:
    """Guarda el bankroll en PostgreSQL y también en archivo local como backup."""
    try:
        from db import db_set_bankroll
        db_set_bankroll(valor)
    except Exception as e:
        logger.error(f"Error guardando bankroll en DB: {e}")
    try:
        with open(BANKROLL_FILE, "w") as f:
            json.dump({"bankroll": round(valor, 2)}, f)
        logger.info(f"Bankroll actualizado: {valor}€")
    except Exception as e:
        logger.error(f"Error guardando bankroll en archivo local: {e}")


def leer_bankroll_excel(ruta: str) -> float | None:
    """
    Lee el bankroll desde la celda A1 de la primera hoja de un Excel.
    Devuelve None si no puede leerlo.
    """
    try:
        import openpyxl
        wb = openpyxl.load_workbook(ruta, read_only=True, data_only=True)
        ws = wb.active
        valor = ws["A1"].value
        wb.close()
        if valor is None:
            logger.warning("Celda A1 vacía en el Excel de bankroll.")
            return None
        return float(str(valor).replace("€", "").replace(",", ".").strip())
    except ImportError:
        logger.error("openpyxl no instalado. Añade 'openpyxl' a requirements.txt")
        return None
    except Exception as e:
        logger.error(f"Error leyendo Excel de bankroll: {e}")
        return None


# ══════════════════════════════════════════════════════
# CÁLCULO DE STAKE
# ══════════════════════════════════════════════════════

def calcular_stake_1x(cuota_local: float) -> dict | None:
    """
    Calcula el stake para un pick de Ganador Local.

    Devuelve un dict con:
        stake          → importe en euros
        multiplicador  → el aplicado
        bankroll       → bankroll actual
    Devuelve None si la cuota está fuera del rango operativo.
    """
    if cuota_local < CUOTA_MIN or cuota_local > CUOTA_MAX:
        logger.debug(f"Cuota {cuota_local} fuera de rango [{CUOTA_MIN}–{CUOTA_MAX}]")
        return None

    multiplicador = 1.0
    for limite, mult in _TRAMOS:
        if cuota_local < limite:
            multiplicador = mult
            break

    if multiplicador == 0.0:
        return None

    bankroll   = get_bankroll()
    stake_base = bankroll * STAKE_PCT
    stake      = round(stake_base * multiplicador, 2)

    return {
        "stake":         stake,
        "multiplicador": multiplicador,
        "stake_base":    round(stake_base, 2),
        "bankroll":      bankroll,
    }


# Niveles de stake visibles al usuario
# mult 0.5 → "Stake 1"  (rango problemático, stake reducido)
# mult 1.0 → "Stake 2"  (stake normal)
_NIVEL_STAKE = {
    0.5: "Stake 1",
    1.0: "Stake 2",
}


def construir_linea_stake_pre(cuota_local_str: str | None) -> str:
    """
    Devuelve la línea HTML lista para insertar en el mensaje de Telegram.
    Ejemplo: "📊 Stake 2"  o  "📊 Stake 1  <i>(cuota en rango reducido)</i>"
    Devuelve cadena vacía si no aplica.
    """
    if not cuota_local_str:
        return ""
    try:
        cuota = float(str(cuota_local_str).replace(",", ".").strip())
    except ValueError:
        return ""

    resultado = calcular_stake_1x(cuota)
    if not resultado:
        return ""

    mult  = resultado["multiplicador"]
    nivel = _NIVEL_STAKE.get(mult, f"×{mult}")

    if mult < 1.0:
        return f"📊 {nivel}  <i>(rango de cuota reducido)</i>"
    return f"📊 {nivel}"
