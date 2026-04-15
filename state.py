import json
import copy
import logging

from config import STATE_FILE, DEFAULT_STATE

logger = logging.getLogger(__name__)

# Estado volátil en memoria:
# - mensajes_publicados: para poder editar mensajes ya enviados (JSON local + DB fallback)
# - alertas_recientes:   anti-duplicado de picks (DB primario, JSON backup)
# - free_state:          cupos del canal FREE (gestionado por free.py vía DB)
# Las estadísticas y resumen_control van a PostgreSQL (ver db.py)

STATE: dict = copy.deepcopy(DEFAULT_STATE)


def save_state() -> None:
    """Persiste el estado volátil: alertas_recientes en DB y JSON local como backup."""
    # Guardar alertas_recientes en DB (sobrevive reinicios en Railway)
    try:
        from db import db_guardar_alertas_recientes
        db_guardar_alertas_recientes(STATE.get("alertas_recientes", {}))
    except Exception as e:
        logger.error(f"Error guardando alertas_recientes en DB: {e}")

    # Backup en JSON local
    try:
        ligero = {
            "mensajes_publicados": STATE.get("mensajes_publicados", {}),
            "alertas_recientes":   STATE.get("alertas_recientes", {}),
        }
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(ligero, f, ensure_ascii=False, indent=2)
        logger.debug("Estado volátil guardado en disco.")
    except Exception as e:
        logger.error(f"Error guardando estado en disco: {e}")


def load_state() -> None:
    """Carga el estado volátil. alertas_recientes se lee desde DB primero."""
    global STATE
    STATE = copy.deepcopy(DEFAULT_STATE)

    # Cargar alertas_recientes desde DB (fuente primaria)
    try:
        from db import db_leer_alertas_recientes
        alertas_db = db_leer_alertas_recientes()
        if alertas_db:
            STATE["alertas_recientes"] = alertas_db
            logger.info(f"alertas_recientes cargadas desde DB ({len(alertas_db)} entradas).")
    except Exception as e:
        logger.warning(f"No se pudo leer alertas_recientes de DB: {e}")

    # Cargar el resto desde JSON local
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            loaded = json.load(f)

        STATE["mensajes_publicados"] = loaded.get("mensajes_publicados", {})
        # Solo usar alertas del JSON si la DB no devolvió nada
        if not STATE["alertas_recientes"]:
            STATE["alertas_recientes"] = loaded.get("alertas_recientes", {})
        STATE["estadisticas"] = loaded.get("estadisticas", DEFAULT_STATE["estadisticas"])

        logger.info("Estado local cargado desde disco.")
    except FileNotFoundError:
        logger.info("No existe estado previo en disco — se usará estado inicial.")
    except Exception as e:
        logger.error(f"Error cargando estado desde disco: {e}")
