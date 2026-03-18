import json
import copy
import logging

from config import STATE_FILE, DEFAULT_STATE

logger = logging.getLogger(__name__)

# Estado en memoria — se inicializa con la plantilla por defecto
STATE: dict = copy.deepcopy(DEFAULT_STATE)


def save_state() -> None:
    """Persiste el estado actual en disco."""
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
        logger.debug("Estado guardado en disco.")
    except Exception as e:
        logger.error(f"Error guardando estado: {e}")


def load_state() -> None:
    """Carga el estado desde disco. Si no existe, mantiene el estado por defecto."""
    global STATE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            STATE = json.load(f)
        logger.info("Estado cargado desde disco.")
    except FileNotFoundError:
        logger.info("No existe estado previo — se usará estado inicial.")
    except Exception as e:
        logger.error(f"Error cargando estado: {e}")
