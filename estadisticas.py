import logging
from datetime import timedelta

from utils import hoy_str, ahora_str, ahora_madrid, semana_str
from state import STATE, save_state

logger = logging.getLogger(__name__)


# ==============================
# REGISTRO
# ==============================

def registrar_pick_estadistica(message_id_origen, datos: dict, tipo_pick: str) -> None:
    registro = {
        "message_id_origen": str(message_id_origen),
        "codigo":       datos.get("codigo"),
        "tipo_pick":    tipo_pick,
        "liga":         datos.get("liga"),
        "partido":      datos.get("partido"),
        "strike_alerta": datos.get("strike_alerta"),
        "strike_liga":  datos.get("strike_liga"),
        "resultado":    None,
        "fecha":        hoy_str(),
        "fecha_hora":   ahora_str(),
    }
    STATE["estadisticas"].append(registro)
    logger.debug(f"Pick registrado: {message_id_origen} | {tipo_pick} | {datos.get('partido')}")


def actualizar_resultado_estadistica(message_id_origen, resultado: str) -> None:
    for item in STATE["estadisticas"]:
        if str(item.get("message_id_origen")) == str(message_id_origen):
            item["resultado"] = resultado
            logger.debug(f"Resultado actualizado: {message_id_origen} → {resultado}")
            break


# ==============================
# FILTROS
# ==============================

def filtrar_estadisticas_hoy() -> list:
    hoy = hoy_str()
    return [x for x in STATE["estadisticas"] if x.get("fecha") == hoy]


def filtrar_estadisticas_semana() -> list:
    ahora = ahora_madrid()
    inicio_semana = ahora - timedelta(days=ahora.weekday())
    inicio_semana_str = inicio_semana.strftime("%Y-%m-%d")
    return [x for x in STATE["estadisticas"] if x.get("fecha", "") >= inicio_semana_str]


# ==============================
# RESUMEN
# ==============================

def construir_resumen(lista: list, titulo: str) -> str:
    total      = len(lista)
    hits       = sum(1 for x in lista if x.get("resultado") == "HIT")
    miss       = sum(1 for x in lista if x.get("resultado") == "MISS")
    voids      = sum(1 for x in lista if x.get("resultado") == "VOID")
    pendientes = total - hits - miss - voids

    goles   = [x for x in lista if x.get("tipo_pick") == "gol"]
    corners = [x for x in lista if x.get("tipo_pick") == "corner"]

    goles_hits  = sum(1 for x in goles   if x.get("resultado") == "HIT")
    goles_miss  = sum(1 for x in goles   if x.get("resultado") == "MISS")
    goles_void  = sum(1 for x in goles   if x.get("resultado") == "VOID")
    corners_hits = sum(1 for x in corners if x.get("resultado") == "HIT")
    corners_miss = sum(1 for x in corners if x.get("resultado") == "MISS")
    corners_void = sum(1 for x in corners if x.get("resultado") == "VOID")

    resueltos = hits + miss
    strike = round((hits / resueltos) * 100, 1) if resueltos > 0 else 0

    lineas = [
        f"📊 {titulo}", "",
        f"Total picks: {total}",
        f"✅ Hits: {hits}",
        f"❌ Miss: {miss}",
        f"⚪ Nulos: {voids}",
        f"⏳ Pendientes: {pendientes}",
        f"📈 Strike: {strike}%", "",
        f"⚽ Goles: {len(goles)} | ✅ {goles_hits} | ❌ {goles_miss} | ⚪ {goles_void}",
        f"🚩 Corners: {len(corners)} | ✅ {corners_hits} | ❌ {corners_miss} | ⚪ {corners_void}",
    ]
    return "\n".join(lineas)


# ==============================
# PUBLICACIÓN AUTOMÁTICA
# ==============================

async def publicar_resumen_diario_si_toca(context) -> None:
    from config import CANAL_RESUMEN_ID
    control = STATE["resumen_control"]
    hoy     = hoy_str()

    if ahora_madrid().hour < 22:
        return
    if control["ultimo_resumen_dia"] == hoy:
        return

    lista = filtrar_estadisticas_hoy()
    if not lista:
        return

    texto = construir_resumen(lista, "RESUMEN DEL DÍA")
    try:
        await context.bot.send_message(chat_id=CANAL_RESUMEN_ID, text=texto)
        control["ultimo_resumen_dia"] = hoy
        save_state()
        logger.info("Resumen diario publicado.")
    except Exception as e:
        logger.error(f"Error publicando resumen diario: {e}")


async def publicar_resumen_semanal_si_toca(context) -> None:
    from config import CANAL_RESUMEN_ID
    control = STATE["resumen_control"]
    ahora   = ahora_madrid()
    semana  = semana_str()

    if ahora.weekday() != 6 or ahora.hour < 22:
        return
    if control["ultimo_resumen_semana"] == semana:
        return

    lista = filtrar_estadisticas_semana()
    if not lista:
        return

    texto = construir_resumen(lista, "RESUMEN SEMANAL")
    try:
        await context.bot.send_message(chat_id=CANAL_RESUMEN_ID, text=texto)
        control["ultimo_resumen_semana"] = semana
        save_state()
        logger.info("Resumen semanal publicado.")
    except Exception as e:
        logger.error(f"Error publicando resumen semanal: {e}")
