import logging

from config import (
    FREE_HORA_INICIO, FREE_HORA_FIN, FREE_TIMEZONE,
    MAX_FREE_GOLES, MAX_FREE_CORNERS, MAX_FREE_TOTAL,
)
from utils import ahora_madrid, hoy_str, clave_hora_actual_free, parse_percent
from state import STATE, save_state

logger = logging.getLogger(__name__)


# ==============================
# HORARIO
# ==============================

def esta_en_horario_free() -> bool:
    hora = ahora_madrid().hour
    return FREE_HORA_INICIO <= hora < FREE_HORA_FIN


# ==============================
# RESET DIARIO
# ==============================

def reset_free_state_si_toca() -> None:
    """Reinicia los contadores FREE si ha cambiado el día (hora Madrid)."""
    fs = STATE["free_state"]
    hoy = hoy_str()
    if fs["fecha"] != hoy:
        fs.update({
            "fecha": hoy,
            "goles_enviados": 0,
            "corners_enviados": 0,
            "ultimo_score_gol": -1,
            "ultimo_score_corner": -1,
            "ultima_hora_envio": None,
        })
        logger.info("Estado FREE reiniciado para el día de hoy.")


# ==============================
# CONTADORES
# ==============================

def total_free_enviados() -> int:
    fs = STATE["free_state"]
    return fs["goles_enviados"] + fs["corners_enviados"]


def score_para_free(datos: dict) -> int:
    """Calcula un score de prioridad para seleccionar el mejor pick FREE del día."""
    strike_alerta = parse_percent(datos.get("strike_alerta"))
    strike_liga   = parse_percent(datos.get("strike_liga"))

    if strike_alerta is None and strike_liga is None:
        return -1
    if strike_alerta is not None and strike_liga is not None:
        return strike_alerta * 1000 + strike_liga
    if strike_alerta is not None:
        return strike_alerta * 1000
    return strike_liga if strike_liga is not None else -1


# ==============================
# DECISIÓN DE ENVÍO
# ==============================

def debe_enviar_a_free(tipo_pick: str, datos: dict) -> tuple[bool, str]:
    """
    Devuelve (True, "OK") si el pick cumple todos los criterios para el canal FREE,
    o (False, motivo) en caso contrario.
    """
    reset_free_state_si_toca()
    fs = STATE["free_state"]

    if not esta_en_horario_free():
        return False, f"Fuera de horario FREE ({FREE_HORA_INICIO}:00–{FREE_HORA_FIN}:00 {FREE_TIMEZONE})"

    hora_actual = clave_hora_actual_free()
    if fs.get("ultima_hora_envio") == hora_actual:
        return False, "Ya se ha enviado un pick FREE en esta hora"

    if total_free_enviados() >= MAX_FREE_TOTAL:
        return False, f"FREE diario completo ({MAX_FREE_TOTAL})"

    score = score_para_free(datos)

    if tipo_pick == "gol":
        if fs["goles_enviados"] >= MAX_FREE_GOLES:
            return False, f"FREE goles completo ({MAX_FREE_GOLES})"
        if score <= fs["ultimo_score_gol"]:
            return False, f"Score gol insuficiente ({score})"
        return True, "OK"

    if tipo_pick == "corner":
        if fs["corners_enviados"] >= MAX_FREE_CORNERS:
            return False, f"FREE corners completo ({MAX_FREE_CORNERS})"
        if score <= fs["ultimo_score_corner"]:
            return False, f"Score corner insuficiente ({score})"
        return True, "OK"

    return False, "Tipo no válido"


def registrar_envio_free(tipo_pick: str, datos: dict) -> None:
    """Actualiza los contadores y el score tras un envío al canal FREE."""
    fs = STATE["free_state"]
    score = score_para_free(datos)
    fs["ultima_hora_envio"] = clave_hora_actual_free()

    if tipo_pick == "gol":
        fs["goles_enviados"] += 1
        fs["ultimo_score_gol"] = score
    elif tipo_pick == "corner":
        fs["corners_enviados"] += 1
        fs["ultimo_score_corner"] = score

    logger.info(f"Envío FREE registrado — tipo: {tipo_pick} | score: {score}")
