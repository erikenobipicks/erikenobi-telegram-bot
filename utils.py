import re
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from config import FREE_TIMEZONE

logger = logging.getLogger(__name__)


# ==============================
# FECHA / HORA
# ==============================

def ahora_madrid() -> datetime:
    """Devuelve el datetime actual en zona horaria Europe/Madrid."""
    return datetime.now(ZoneInfo(FREE_TIMEZONE))


def hoy_str() -> str:
    """Fecha de hoy en Madrid (YYYY-MM-DD). Evita desfases con servidores UTC."""
    return ahora_madrid().strftime("%Y-%m-%d")


def ahora_str() -> str:
    """Datetime actual en Madrid como string legible."""
    return ahora_madrid().strftime("%Y-%m-%d %H:%M:%S")


def semana_str() -> str:
    """Identificador de semana ISO basado en la hora de Madrid."""
    year, week, _ = ahora_madrid().isocalendar()
    return f"{year}-W{week}"


def clave_hora_actual_free() -> str:
    """Clave única por hora del día en Madrid (YYYY-MM-DD HH)."""
    return ahora_madrid().strftime("%Y-%m-%d %H")


# ==============================
# PARSEO
# ==============================

def parse_percent(valor) -> int | None:
    """Convierte un valor porcentual a entero. Devuelve None si no es válido."""
    if valor is None:
        return None
    valor = str(valor).strip().replace("%", "")
    if valor.upper() == "N/A":
        return None
    try:
        return int(valor)
    except Exception:
        return None


def parse_marcador_total(valor: str) -> int | None:
    """Extrae la suma de un marcador tipo '2-1'. Devuelve None si no parsea."""
    if not valor:
        return None
    m = re.match(r"\s*(\d+)\s*-\s*(\d+)\s*$", valor)
    if not m:
        return None
    return int(m.group(1)) + int(m.group(2))


def normalizar_codigo(codigo: str) -> str:
    """Normaliza un código de pick: mayúsculas, sin espacios ni guiones bajos."""
    if not codigo:
        return ""
    return codigo.upper().replace("_", "").replace(" ", "")
