import json
import copy
import logging

from config import STATE_FILE, DEFAULT_STATE

logger = logging.getLogger(__name__)

# Solo guardamos en JSON lo que es volátil e intradía:
# - mensajes_publicados: para poder editar mensajes ya enviados
# - free_state: cupos y scores del día del canal FREE
# Las estadísticas y resumen_control van a PostgreSQL (ver db.py)

STATE: dict = copy.deepcopy(DEFAULT_STATE)


def save_state() -> None:
    """Persiste el estado volátil en disco."""
    try:
        # Guardamos solo las claves ligeras, no las estadísticas (van a DB)
        ligero = {
            "mensajes_publicados": STATE.get("mensajes_publicados", {}),
            "free_state":          STATE.get("free_state", {}),
            "alertas_recientes":   STATE.get("alertas_recientes", {}),
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(ligero, f, ensure_ascii=False, indent=2)
        logger.debug("Estado volátil guardado en disco.")
    except Exception as e:
        logger.error(f"Error guardando estado: {e}")


def load_state() -> None:
    """Carga el estado volátil desde disco."""
    global STATE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        # Fusionamos con el DEFAULT para garantizar todas las claves
        STATE = copy.deepcopy(DEFAULT_STATE)
        STATE["mensajes_publicados"] = loaded.get("mensajes_publicados", {})
        STATE["free_state"]          = loaded.get("free_state", DEFAULT_STATE["free_state"])
        STATE["alertas_recientes"]   = loaded.get("alertas_recientes", DEFAULT_STATE.get("alertas_recientes", {}))
        STATE["estadisticas"]        = loaded.get("estadisticas", DEFAULT_STATE["estadisticas"])

        logger.info("Estado volátil cargado desde disco.")
    except FileNotFoundError:
        logger.info("No existe estado previo — se usará estado inicial.")
    except Exception as e:
        logger.error(f"Error cargando estado: {e}")
