import json
import copy
import logging

from config import STATE_FILE, DEFAULT_STATE

logger = logging.getLogger(__name__)

STATE: dict = copy.deepcopy(DEFAULT_STATE)


def save_state() -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
        logger.debug("Estado guardado en disco.")
    except Exception as e:
        logger.error(f"Error guardando estado: {e}")


def load_state() -> None:
    global STATE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        # Migración suave: si el estado guardado usa el formato antiguo
        # (resumen_control con keys fijas), lo convertimos al nuevo dict plano
        control_antiguo = loaded.get("resumen_control", {})
        if isinstance(control_antiguo, dict) and (
            "ultimo_resumen_dia" in control_antiguo
            or "ultimo_resumen_semana" in control_antiguo
        ):
            logger.info("Migrando resumen_control al nuevo formato...")
            loaded["resumen_control"] = {}

        STATE = loaded
        logger.info("Estado cargado desde disco.")
    except FileNotFoundError:
        logger.info("No existe estado previo — se usará estado inicial.")
    except Exception as e:
        logger.error(f"Error cargando estado: {e}")
