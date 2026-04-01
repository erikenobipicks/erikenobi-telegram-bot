import logging
import os
import tempfile

from telegram import Update
from telegram.ext import ContextTypes

from config import (
    CANAL_ORIGEN_ID,
    CANAL_CORNERS_ID, CANAL_GOLES_ID, CANAL_GENERAL_ID, CANAL_FREE_ID, CANAL_PRE_ID,
    ENVIAR_A_GENERAL,
    ADMIN_IDS,
)
from state import STATE, save_state
from extractor import (
    extraer_datos,
    detectar_tipo_pick_por_codigo,
    detectar_fase_por_codigo,
    pasa_filtro_strike_liga,
)
from bankroll import get_bankroll, set_bankroll, leer_bankroll_excel
from formateador import construir_mensaje_base, construir_mensaje_editado
from free import debe_enviar_a_free, registrar_envio_free
from estadisticas import (
    registrar_pick_estadistica,
    actualizar_resultado_estadistica,
    resolver_resultado_corner_mas_uno,
    enviar_resumenes_comando,
    enviar_resumen_anual_comando,
    enviar_resumen_liga_comando,
    enviar_resumen_prepartido_comando,
    enviar_resumen_codigo_comando,
    publicar_resumen_diario_si_toca,
    publicar_resumen_semanal_si_toca,
    publicar_resumen_mensual_si_toca,
    notificar_picks_pendientes_si_toca,
    verificar_racha_y_notificar,
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

    fase = detectar_fase_por_codigo(datos)
    es_prepartido = (fase == "PRE")

    # ── Picks PREPARTIDO → canal propio, sin FREE ni canales live ────
    if es_prepartido:
        if not CANAL_PRE_ID:
            logger.warning("Pick prepartido recibido pero CANAL_PRE_ID no configurado.")
            return

        mensaje_pre = construir_mensaje_base(datos, tipo_pick)
        logger.info(f"PREPARTIDO | tipo: {tipo_pick} | origen msg_id: {msg_id}")

        destinos_publicados: dict[str, int] = {}
        try:
            enviado = await enviar_mensaje(context, CANAL_PRE_ID, mensaje_pre)
            destinos_publicados[str(CANAL_PRE_ID)] = enviado.message_id
            logger.info(f"Prepartido enviado a {CANAL_PRE_ID} (msg {enviado.message_id})")
        except Exception as e:
            logger.error(f"Error enviando prepartido a {CANAL_PRE_ID}: {e}")

        STATE["mensajes_publicados"][str(msg_id)] = {
            "tipo_pick":         tipo_pick,
            "mensaje_base":      mensaje_pre,
            "mensaje_base_free": mensaje_pre,
            "destinos":          destinos_publicados,
        }
        registrar_pick_estadistica(msg_id, datos, tipo_pick, enviado_a_free=False)
        save_state()
        return

    # ── Picks LIVE — flujo normal ─────────────────────────────────────
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
    enviado_a_free = False
    ok_free, motivo_free = debe_enviar_a_free(tipo_pick, datos)
    if ok_free:
        try:
            enviado_free = await enviar_mensaje(context, CANAL_FREE_ID, mensaje_base_free)
            destinos_publicados[str(CANAL_FREE_ID)] = enviado_free.message_id
            registrar_envio_free(tipo_pick, datos)
            enviado_a_free = True
            logger.info(f"Enviado a FREE {CANAL_FREE_ID} (msg {enviado_free.message_id})")
        except Exception as e:
            logger.error(f"Error enviando a FREE: {e}")
    else:
        logger.info(f"No enviado a FREE: {motivo_free}")

    # Guardar en estado
    STATE["mensajes_publicados"][str(msg_id)] = {
        "tipo_pick":         tipo_pick,
        "mensaje_base":      mensaje_base,
        "mensaje_base_free": mensaje_base_free,
        "destinos":          destinos_publicados,
    }

    # Registrar estadística con el flag de free
    registrar_pick_estadistica(msg_id, datos, tipo_pick, enviado_a_free=enviado_a_free)
    save_state()

    await publicar_resumen_diario_si_toca(context)
    await publicar_resumen_semanal_si_toca(context)
    await publicar_resumen_mensual_si_toca(context)
    await notificar_picks_pendientes_si_toca(context)


# ==============================
# PROCESAR MENSAJE EDITADO
# ==============================

async def procesar_mensaje_editado(mensaje, context: ContextTypes.DEFAULT_TYPE) -> None:
    texto   = mensaje.text or mensaje.caption or ""
    chat_id = mensaje.chat_id
    msg_id  = mensaje.message_id

    if chat_id != CANAL_ORIGEN_ID:
        return

    datos = extraer_datos(texto)

    if not datos.get("resultado"):
        logger.info("Editado detectado, pero sin Hit/Miss/Void todavía.")
        return

    key = str(msg_id)

    # ── Caso normal: tenemos el registro en STATE ─────────────────────
    if key in STATE["mensajes_publicados"]:
        registro  = STATE["mensajes_publicados"][key]
        tipo_pick = registro["tipo_pick"]
        destinos  = registro["destinos"]

        logger.info(f"EDITADO | tipo: {tipo_pick} | resultado: {datos.get('resultado')}")

        for canal_id_str, msg_id_publicado in destinos.items():
            base = (
                registro["mensaje_base_free"]
                if str(canal_id_str) == str(CANAL_FREE_ID)
                else registro["mensaje_base"]
            )
            texto_editado = construir_mensaje_editado(base, datos, tipo_pick)
            await editar_mensaje(context, int(canal_id_str), msg_id_publicado, texto_editado)

    # ── Caso tardío: el bot se reinició y perdimos el STATE ───────────
    # No podemos editar los mensajes en los canales destino porque no
    # tenemos los message_id, pero sí podemos actualizar la estadística
    # en la base de datos para que los resúmenes sean correctos.
    else:
        tipo_pick = detectar_tipo_pick_por_codigo(datos) or "desconocido"
        logger.warning(
            f"EDITADO TARDÍO | msg_id: {msg_id} | resultado: {datos.get('resultado')} | "
            f"Sin referencia en STATE — solo se actualiza la DB."
        )

    actualizado = actualizar_resultado_estadistica(msg_id, datos.get("resultado"))
    if not actualizado:
        logger.warning(f"No se pudo actualizar el resultado en DB para msg_id {msg_id}")
    save_state()

    # Comprobar racha solo cuando el resultado es HIT
    if datos.get("resultado") == "HIT":
        await verificar_racha_y_notificar(context, tipo_pick)

    await publicar_resumen_diario_si_toca(context)
    await publicar_resumen_semanal_si_toca(context)
    await publicar_resumen_mensual_si_toca(context)
    await notificar_picks_pendientes_si_toca(context)


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
    await enviar_resumenes_comando(update, "dia")


async def cmd_resumen_semana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await enviar_resumenes_comando(update, "semana")


async def cmd_resumen_mes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await enviar_resumenes_comando(update, "mes")


async def cmd_resumen_anual(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Resumen del año en curso con desglose mensual."""
    await enviar_resumen_anual_comando(update)


async def cmd_resumen_liga(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Uso: /resumen_liga <nombre_liga>
    Ejemplo: /resumen_liga LALIGA
    """
    if not context.args:
        await update.message.reply_text(
            "Uso: /resumen_liga <nombre>\nEjemplo: /resumen_liga LALIGA"
        )
        return
    liga = " ".join(context.args)
    await enviar_resumen_liga_comando(update, liga)


async def cmd_resumen_codigo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Uso: /resumen_codigo <codigo>
    Ejemplo: /resumen_codigo CM02
    """
    if not context.args:
        await update.message.reply_text(
            "Uso: /resumen_codigo <codigo>\nEjemplo: /resumen_codigo CM02"
        )
        return
    codigo = context.args[0]
    await enviar_resumen_codigo_comando(update, codigo)


async def cmd_resumen_pre(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Resumen de estrategias prepartido con profit/ROI."""
    await enviar_resumen_prepartido_comando(update)


# ==============================
# COMANDO /resultado — CORRECCIÓN MANUAL
# ==============================

async def cmd_resultado(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Permite corregir o asignar manualmente el resultado de un pick en la DB.
    Solo accesible para admins.

    Uso: /resultado <message_id_origen> <HIT|MISS|VOID>
    Ejemplo: /resultado 12345 HIT

    message_id_origen es el ID del mensaje en el canal origen (el número
    que aparece en la URL del mensaje de Telegram).
    """
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Uso: /resultado <message_id_origen> <HIT|MISS|VOID>\n"
            "Ejemplo: /resultado 12345 HIT"
        )
        return

    msg_id_str = context.args[0]
    resultado  = context.args[1].upper()

    if resultado not in ("HIT", "MISS", "VOID"):
        await update.message.reply_text(
            "Resultado no válido. Usa: HIT, MISS o VOID"
        )
        return

    actualizado = actualizar_resultado_estadistica(msg_id_str, resultado)
    if not actualizado:
        await update.message.reply_text(
            f"No se encontrÃ³ ningÃºn pick con el message_id {msg_id_str} en la DB."
        )
        return

    logger.info(
        f"Resultado actualizado manualmente por admin {user.id}: "
        f"msg {msg_id_str} → {resultado}"
    )
    await update.message.reply_text(
        f"✅ Resultado actualizado en la DB:\n"
        f"Pick {msg_id_str} → {resultado}\n\n"
        f"Los próximos resúmenes ya reflejarán este cambio."
    )


# ==============================
# BANKROLL — CONSULTA Y ACTUALIZACIÓN
# ==============================

async def cmd_resultado_corner(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Calcula el resultado de un pick de corners +1 usando el total final del periodo.
    Solo accesible para admins.

    Uso: /resultado_corner <message_id_origen> <corners_finales_del_periodo>
    Ejemplo: /resultado_corner 12345 8
    """
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if len(context.args) < 2:
        await update.message.reply_text(
            "Uso: /resultado_corner <message_id_origen> <corners_finales_del_periodo>\n"
            "Ejemplo: /resultado_corner 12345 8"
        )
        return

    msg_id_str = context.args[0]

    try:
        corners_finales = int(context.args[1])
    except ValueError:
        await update.message.reply_text(
            "El total final de corners debe ser un numero entero."
        )
        return

    ok, mensaje = resolver_resultado_corner_mas_uno(msg_id_str, corners_finales)
    if not ok:
        await update.message.reply_text(mensaje)
        return

    logger.info(
        f"Resultado de corners +1 actualizado manualmente por admin {user.id}: "
        f"msg {msg_id_str} | corners_finales={corners_finales}"
    )
    await update.message.reply_text(mensaje)


async def cmd_bankroll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Consulta o actualiza el bankroll.
    Uso: /bankroll          → muestra el bankroll actual
         /bankroll 1500     → actualiza a 1500€
    Solo admins.
    """
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    if not context.args:
        actual = get_bankroll()
        stake_base = round(actual * 0.02, 2)
        await update.message.reply_text(
            f"💰 Bankroll actual: <b>{actual}€</b>\n"
            f"Stake base (2%): <b>{stake_base}€</b>\n\n"
            f"Para actualizar: /bankroll 1500\n"
            f"O sube un Excel con el valor en la celda A1.",
            parse_mode="HTML",
        )
        return

    try:
        nuevo = float(context.args[0].replace(",", ".").replace("€", "").strip())
        if nuevo <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("Valor no válido. Ejemplo: /bankroll 1500")
        return

    set_bankroll(nuevo)
    stake_base = round(nuevo * 0.02, 2)
    await update.message.reply_text(
        f"✅ Bankroll actualizado: <b>{nuevo}€</b>\n"
        f"Stake base (2%): <b>{stake_base}€</b>",
        parse_mode="HTML",
    )
    logger.info(f"Bankroll actualizado por admin {user.id}: {nuevo}€")


async def handler_excel_bankroll(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Procesa un Excel subido en chat privado para actualizar el bankroll.
    El Excel debe tener el valor en la celda A1 de la primera hoja.
    Solo admins.
    """
    user    = update.effective_user
    chat    = update.effective_chat
    message = update.effective_message

    if not user or user.id not in ADMIN_IDS:
        return
    if not chat or chat.type != "private":
        return
    if not message or not message.document:
        return

    nombre = message.document.file_name or ""
    if not nombre.lower().endswith((".xlsx", ".xls")):
        return

    await message.reply_text("📊 Procesando Excel...")

    try:
        archivo = await context.bot.get_file(message.document.file_id)
        ruta    = os.path.join(tempfile.gettempdir(), f"bankroll_{user.id}.xlsx")
        await archivo.download_to_drive(ruta)
    except Exception as e:
        logger.error(f"Error descargando Excel de bankroll: {e}")
        await message.reply_text("❌ Error descargando el archivo.")
        return

    valor = leer_bankroll_excel(ruta)
    if valor is None:
        await message.reply_text(
            "❌ No se pudo leer el bankroll del Excel.\n"
            "Asegúrate de que el valor esté en la celda <b>A1</b> de la primera hoja.",
            parse_mode="HTML",
        )
        return

    set_bankroll(valor)
    stake_base = round(valor * 0.02, 2)
    await message.reply_text(
        f"✅ Bankroll actualizado desde Excel: <b>{valor}€</b>\n"
        f"Stake base (2%): <b>{stake_base}€</b>",
        parse_mode="HTML",
    )
    logger.info(f"Bankroll actualizado desde Excel por admin {user.id}: {valor}€")
