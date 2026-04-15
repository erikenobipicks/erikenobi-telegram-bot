import logging

from config import (
    FREE_HORA_INICIO, FREE_HORA_FIN, FREE_TIMEZONE,
    MAX_FREE_GOLES, MAX_FREE_CORNERS, MAX_FREE_TOTAL,
)
from utils import ahora_madrid, hoy_str, clave_hora_actual_free, parse_percent
from db import db_leer_free_state, db_guardar_free_state

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

def reset_free_state_si_toca(fs: dict) -> dict:
    """
    Reinicia los contadores FREE si ha cambiado el día (hora Madrid).
    Modifica fs in-place y devuelve el mismo dict.
    """
    hoy = hoy_str()
    if fs.get("fecha") != hoy:
        fs.update({
            "fecha":               hoy,
            "goles_enviados":      0,
            "corners_enviados":    0,
            "ultimo_score_gol":    -1,
            "ultimo_score_corner": -1,
            "ultima_hora_envio":   None,
        })
        db_guardar_free_state(fs)
        logger.info("Estado FREE reiniciado para el día de hoy.")
    return fs


# ==============================
# CONTADORES
# ==============================

def total_free_enviados(fs: dict) -> int:
    return fs.get("goles_enviados", 0) + fs.get("corners_enviados", 0)


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
    Lee y escribe el estado desde PostgreSQL para sobrevivir reinicios.
    """
    fs = db_leer_free_state()
    fs = reset_free_state_si_toca(fs)

    if not esta_en_horario_free():
        return False, f"Fuera de horario FREE ({FREE_HORA_INICIO}:00–{FREE_HORA_FIN}:00 {FREE_TIMEZONE})"

    hora_actual = clave_hora_actual_free()
    if fs.get("ultima_hora_envio") == hora_actual:
        return False, "Ya se ha enviado un pick FREE en esta hora"

    if total_free_enviados(fs) >= MAX_FREE_TOTAL:
        return False, f"FREE diario completo ({MAX_FREE_TOTAL})"

    if tipo_pick == "gol":
        if fs.get("goles_enviados", 0) >= MAX_FREE_GOLES:
            return False, f"FREE goles completo ({MAX_FREE_GOLES})"
        return True, "OK"

    if tipo_pick == "corner":
        if fs.get("corners_enviados", 0) >= MAX_FREE_CORNERS:
            return False, f"FREE corners completo ({MAX_FREE_CORNERS})"
        return True, "OK"

    return False, "Tipo no válido"


def registrar_envio_free(tipo_pick: str, datos: dict) -> None:
    """Actualiza los contadores en DB tras un envío al canal FREE."""
    fs = db_leer_free_state()
    fs = reset_free_state_si_toca(fs)

    fs["ultima_hora_envio"] = clave_hora_actual_free()

    if tipo_pick == "gol":
        fs["goles_enviados"] = fs.get("goles_enviados", 0) + 1
    elif tipo_pick == "corner":
        fs["corners_enviados"] = fs.get("corners_enviados", 0) + 1

    db_guardar_free_state(fs)
    logger.info(
        "Envío FREE registrado — tipo: %s | goles: %s/%s | corners: %s/%s",
        tipo_pick,
        fs["goles_enviados"], MAX_FREE_GOLES,
        fs["corners_enviados"], MAX_FREE_CORNERS,
    )
