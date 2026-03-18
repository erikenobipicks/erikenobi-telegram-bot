import logging

from telegram import Update
from telegram.ext import ContextTypes

from config import (
    CANAL_ORIGEN_ID,
    CANAL_CORNERS_ID, CANAL_GOLES_ID, CANAL_GENERAL_ID, CANAL_FREE_ID,
    ENVIAR_A_GENERAL,
)
from state import STATE, save_state
from extractor import extraer_datos, detectar_tipo_pick_por_codigo, pasa_filtro_strike_liga
from formateador import construir_mensaje_base, construir_mensaje_editado
from free import debe_enviar_a_free, registrar_envio_free
from estadisticas import (
    registrar_pick_estadistica,
    actualizar_resultado_estadistica,
    filtrar_estadisticas_hoy,
    filtrar_estadisticas_semana,
    construir_resumen,
    publicar_resumen_diario_si_toca,
    publicar_resumen_semanal_si_toca,
)

logger = logging.getLogger(__name__)


# ==============================
# ENVÍO / EDICIÓN TELEGRAM
# ==============================

async def enviar_mensaje(context: ContextTypes.DEFAULT_TYPE, canal_id: int, texto: str):
    return await context.bot.send_message(
        chat_id=canal_id,
        text=texto,
        parse_mode="HTML",
    )


async def editar_mensaje(
    context: ContextTypes.DEFAULT_TYPE,
    canal_id: int,
    message_id: int,
    texto_nuevo: str,
) -> None:
    try:
        await context.bot.edit_message_text(
            chat_id=canal_id,
            message_id=message_id,
            text=texto_nuevo,
            parse_mode="HTML",
        )
        logger.info(f"Mensaje editado en canal {canal_id} (msg {message_id})")
    except Exception as e:
        logger.error(f"Error editando mensaje en {canal_id}: {e}")


# ==============================
# PROCESAR MENSAJE NUEVO
# ==============================

async def procesar_nuevo_mensaje(mensaje, context: ContextTypes.DEFAULT_TYPE) -> None:
    texto   = mensaje.text or mensaje.caption or ""
    chat_id = mensaje.chat_id
    msg_id  = mensaje.message_id

    if chat_id != CANAL_ORIGEN_ID:
        return

    datos = extraer_datos(texto)

    if not pasa_filtro_strike_liga(datos):
        return

    tipo_pick = detectar_tipo_pick_por_codigo(datos)
    if not tipo_pick:
        logger.info("Ignorado: no se detecta GOAL/CORNER por código.")
        return

    # Canales destino
    canales_destino = []
    if tipo_pick == "corner":
        canales_destino.append(CANAL_CORNERS_ID)
    elif tipo_pick == "gol":
        canales_destino.append(CANAL_GOLES_ID)
    if ENVIAR_A_GENERAL:
        canales_destino.append(CANAL_GENERAL_ID)

    mensaje_base      = construir_mensaje_base(datos, tipo_pick)
    mensaje_base_free = construir_mensaje_base(datos, tipo_pick, para_free=True)

    logger.info(f"NUEVO | tipo: {tipo_pick} | origen msg_id: {msg_id} | destinos: {canales_destino}")

    destinos_publicados: dict[str, int] = {}

    for canal_id in canales_destino:
        try:
            enviado = await enviar_mensaje(context, canal_id, mensaje_base)
            destinos_publicados[str(canal_id)] = enviado.message_id
            logger.info(f"Enviado a {canal_id} (msg {enviado.message_id})")
        except Exception as e:
            logger.error(f"Error enviando a {canal_id}: {e}")

    # Canal FREE
    ok_free, motivo_free = debe_enviar_a_free(tipo_pick, datos)
    if ok_free:
        try:
            enviado_free = await enviar_mensaje(context, CANAL_FREE_ID, mensaje_base_free)
            destinos_publicados[str(CANAL_FREE_ID)] = enviado_free.message_id
            registrar_envio_free(tipo_pick, datos)
            logger.info(f"Enviado a FREE {CANAL_FREE_ID} (msg {enviado_free.message_id})")
        except Exception as e:
            logger.error(f"Error enviando a FREE: {e}")
    else:
        logger.info(f"No enviado a FREE: {motivo_free}")

    # Guardar en estado
    STATE["mensajes_publicados"][str(msg_id)] = {
        "tipo_pick":        tipo_pick,
        "mensaje_base":     mensaje_base,
        "mensaje_base_free": mensaje_base_free,
        "destinos":         destinos_publicados,
    }

    registrar_pick_estadistica(msg_id, datos, tipo_pick)
    save_state()

    await publicar_resumen_diario_si_toca(context)
    await publicar_resumen_semanal_si_toca(context)


# ==============================
# PROCESAR MENSAJE EDITADO
# ==============================

async def procesar_mensaje_editado(mensaje, context: ContextTypes.DEFAULT_TYPE) -> None:
    texto   = mensaje.text or mensaje.caption or ""
    chat_id = mensaje.chat_id
    msg_id  = mensaje.message_id

    if chat_id != CANAL_ORIGEN_ID:
        return

    key = str(msg_id)
    if key not in STATE["mensajes_publicados"]:
        logger.info("Editado ignorado: no tenemos referencia del original.")
        return

    registro  = STATE["mensajes_publicados"][key]
    tipo_pick = registro["tipo_pick"]
    destinos  = registro["destinos"]

    datos = extraer_datos(texto)

    if not datos.get("resultado"):
        logger.info("Editado detectado, pero sin Hit/Miss/Void todavía.")
        return

    logger.info(f"EDITADO | tipo: {tipo_pick} | resultado: {datos.get('resultado')}")

    for canal_id_str, msg_id_publicado in destinos.items():
        base = (
            registro["mensaje_base_free"]
            if str(canal_id_str) == str(CANAL_FREE_ID)
            else registro["mensaje_base"]
        )
        texto_editado = construir_mensaje_editado(base, datos, tipo_pick)
        await editar_mensaje(context, int(canal_id_str), msg_id_publicado, texto_editado)

    actualizar_resultado_estadistica(msg_id, datos.get("resultado"))
    save_state()

    await publicar_resumen_diario_si_toca(context)
    await publicar_resumen_semanal_si_toca(context)


# ==============================
# HANDLERS TELEGRAM
# ==============================

async def handler_nuevo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.channel_post:
        await procesar_nuevo_mensaje(update.channel_post, context)


async def handler_editado(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.edited_channel_post:
        await procesar_mensaje_editado(update.edited_channel_post, context)


# ==============================
# COMANDOS DE CONSULTA
# ==============================

async def cmd_resumen_hoy(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lista = filtrar_estadisticas_hoy()
    texto = construir_resumen(lista, "RESUMEN DEL DÍA")
    await update.message.reply_text(texto)


async def cmd_resumen_semana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lista = filtrar_estadisticas_semana()
    texto = construir_resumen(lista, "RESUMEN SEMANAL")
    await update.message.reply_text(texto)
