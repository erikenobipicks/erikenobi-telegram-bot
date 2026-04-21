import asyncio
import logging
import os
import tempfile
import json
from datetime import datetime, timezone

from telegram import Update, error as tg_error
from telegram.ext import ContextTypes

from config import (
    CANAL_ORIGEN_ID,
    CANAL_CORNERS_ID, CANAL_GOLES_ID, CANAL_GENERAL_ID, CANAL_FREE_ID,
    CANAL_PRE_ID, CANAL_PRE_GENERAL_ID,
    ENVIAR_A_GENERAL,
    ADMIN_IDS,
    DUPLICADO_VENTANA_MINUTOS,
    FINDE_CORNER_HORA_INICIO, FINDE_CORNER_HORA_FIN, FINDE_CORNER_STRIKE_MIN,
    RACHA_MISS_LIMITE, RACHA_MISS_PAUSA_MIN,
    RAFAGA_PICKS_LIMITE, RAFAGA_VENTANA_MIN, RAFAGA_PAUSA_MIN,
)
from state import STATE, save_state
from extractor import (
    extraer_datos,
    detectar_tipo_pick_por_codigo,
    detectar_fase_por_codigo,
    detectar_periodo_por_codigo,
    detectar_modo_por_codigo,
    detectar_linea_por_codigo,
    pasa_filtro_strike_liga,
)
from bankroll import get_bankroll, set_bankroll, leer_bankroll_excel
from formateador import construir_mensaje_base, construir_mensaje_editado
from clasificador_alertas import clasificar_alerta
from free import intentar_envio_free
from utils import hoy_str, ahora_madrid, parse_percent
from db import db_guardar_publicacion, db_pick_por_message_id, db_racha_miss_actual
from estadisticas import (
    registrar_pick_estadistica,
    actualizar_resultado_estadistica,
    resolver_resultado_corner_mas_uno,
    enviar_resumenes_comando,
    enviar_resumen_anual_comando,
    enviar_analisis_filtros_comando,
    enviar_resumen_liga_comando,
    enviar_resumen_prepartido_comando,
    enviar_resumen_codigo_comando,
    publicar_resumen_diario_si_toca,
    publicar_resumen_semanal_si_toca,
    publicar_resumen_mensual_si_toca,
    notificar_picks_pendientes_si_toca,
    verificar_racha_y_notificar,
    calcular_stake_pre,
    propagar_resultado_rem_a_pre,
)

logger = logging.getLogger(__name__)


def _extraer_odds_pre(datos: dict, codigo: str) -> float | None:
    """
    Extrae la cuota numérica de un pick prepartido a partir del código.
    Usada para pasar odds al scorer_pre y calcular el stake dinámico.
    """
    codigo_up = codigo.upper()
    odds_raw = ""
    if "O25" in codigo_up or "OVER2.5" in codigo_up:
        odds_raw = datos.get("odds_over_2_5") or ""
    elif "O15" in codigo_up or "OVER1.5" in codigo_up:
        odds_raw = datos.get("odds_over_1_5") or ""
    elif "O05" in codigo_up or "OVER0.5" in codigo_up:
        odds_raw = datos.get("odds_over_0_5") or ""
    elif "1X" in codigo_up:
        raw_1x2 = datos.get("odds_1x2") or ""
        odds_raw = raw_1x2.split()[0] if raw_1x2.split() else ""

    partes_odds = [p.strip() for p in odds_raw.replace("|", " ").split() if p.strip()]
    if partes_odds:
        try:
            return float(partes_odds[0].replace(",", "."))
        except ValueError:
            pass
    return None


def _es_carlos_mollar(datos: dict) -> bool:
    """
    Devuelve True si el pick pertenece al sistema Carlos Mollar.
    Se detecta por la etiqueta 'sistema' extraída de partes[6] de la alerta.
    """
    return "CARLOS" in (datos.get("sistema") or "").upper()


# ══════════════════════════════════════════════════════════════════════
# BUFFER CARLOS MOLLAR — ventana STK5
# ══════════════════════════════════════════════════════════════════════
#
# Cuando llegan varias alertas CM del mismo partido en ráfaga rápida
# (ej: una sin STK5 y otra con STK5), el buffer espera _CM_BUFFER_SEG
# segundos antes de decidir cuál publicar.
#
# Regla de selección:
#   - Si alguna alerta tiene STK5 → publicar ESA (stake 5u), descartar resto
#   - Si ninguna tiene STK5     → publicar la primera llegada (stake 3u)
#
# Las alertas CM siempre van a CANAL_PRE_ID (no pasan por el routing general).
# ══════════════════════════════════════════════════════════════════════

_CM_BUFFER_SEG = 8   # segundos de ventana para agrupar alertas del mismo partido

# buffer_key → {"alertas": [...], "task": asyncio.Task | None}
_cm_buffer: dict[str, dict] = {}


def _cm_stake(datos: dict) -> float:
    """Stake fijo Carlos Mollar: STK5 → 5.0u, sin STK5 → 3.0u."""
    return 5.0 if datos.get("stk5") else 3.0


def _cm_key(datos: dict, tipo_pick: str, fase: str) -> str:
    """Clave de buffer: agrupa alertas CM del mismo partido/tipo/fase."""
    partido = " ".join((datos.get("partido") or "").upper().split())
    return f"{fase}|{tipo_pick}|{partido}"


def _normalizar_alerta(valor: str | None) -> str:
    if not valor:
        return ""
    return " ".join(str(valor).strip().upper().split())


def _clave_duplicado(datos: dict, tipo_pick: str) -> str:
    """
    Clave laxa para la ventana de DUPLICADO_VENTANA_MINUTOS minutos.
    Bloquea cualquier pick del mismo tipo y periodo para el mismo partido,
    independientemente del modelo o mercado concreto. Así se evita que
    varios modelos disparen alertas del mismo partido en un intervalo corto.
    """
    partido = _normalizar_alerta(datos.get("partido"))
    periodo = _normalizar_alerta(detectar_periodo_por_codigo(datos))
    return "|".join([tipo_pick.upper(), periodo, partido])


def _clave_alerta_reciente(datos: dict, tipo_pick: str) -> str:
    """
    Clave exacta para el bloqueo de 24h.
    Incluye el mercado concreto (modo+línea) pero NO la liga, porque el texto
    de la liga puede variar ligeramente entre modelos para el mismo partido.
    """
    partido = _normalizar_alerta(datos.get("partido"))
    fase    = _normalizar_alerta(detectar_fase_por_codigo(datos))
    periodo = _normalizar_alerta(detectar_periodo_por_codigo(datos))
    modo    = _normalizar_alerta(detectar_modo_por_codigo(datos))
    linea   = _normalizar_alerta(detectar_linea_por_codigo(datos))
    return "|".join([tipo_pick.upper(), fase, periodo, modo, linea, partido])


def _purga_alertas_recientes() -> None:
    """Elimina entradas con timestamp de hace más de 24h."""
    ahora_ts = datetime.now(timezone.utc).timestamp()
    recientes = STATE.setdefault("alertas_recientes", {})
    for clave, valor in list(recientes.items()):
        # valor puede ser string fecha (sistema antiguo) o float timestamp
        if isinstance(valor, float) and ahora_ts - valor > 86400:
            recientes.pop(clave, None)
        elif isinstance(valor, str):
            # entrada del sistema antiguo: purgar siempre
            recientes.pop(clave, None)


def purgar_alertas_recientes() -> None:
    """
    Versión pública de _purga_alertas_recientes.
    Llamada periódicamente desde main.py para limpiar el dict incluso
    cuando no llegan picks nuevos (ej: noches sin actividad).
    """
    _purga_alertas_recientes()


def _es_alerta_duplicada(datos: dict, tipo_pick: str) -> bool:
    """
    Devuelve True si:
    - Misma clave exacta (mismo partido+periodo+modo+línea) ya publicada hoy, O
    - Misma clave laxa (mismo partido+periodo+tipo) publicada en los últimos
      DUPLICADO_VENTANA_MINUTOS minutos.
    """
    _purga_alertas_recientes()
    recientes = STATE.setdefault("alertas_recientes", {})
    ahora_ts  = datetime.now(timezone.utc).timestamp()
    ventana   = DUPLICADO_VENTANA_MINUTOS * 60

    # Comprobación exacta (misma alerta idéntica)
    clave_exacta = _clave_alerta_reciente(datos, tipo_pick)
    if clave_exacta.replace("|", "").strip():
        ts = recientes.get(clave_exacta)
        if isinstance(ts, float) and ahora_ts - ts < 86400:
            return True

    # Comprobación laxa (mismo partido+periodo+tipo en ventana de 15 min)
    clave_laxa = _clave_duplicado(datos, tipo_pick)
    if clave_laxa.replace("|", "").strip():
        ts = recientes.get(clave_laxa)
        if isinstance(ts, float) and ahora_ts - ts < ventana:
            return True

    return False


def _registrar_alerta_reciente(datos: dict, tipo_pick: str) -> None:
    _purga_alertas_recientes()
    ahora_ts = datetime.now(timezone.utc).timestamp()
    recientes = STATE.setdefault("alertas_recientes", {})

    clave_exacta = _clave_alerta_reciente(datos, tipo_pick)
    if clave_exacta.replace("|", "").strip():
        recientes[clave_exacta] = ahora_ts

    clave_laxa = _clave_duplicado(datos, tipo_pick)
    if clave_laxa.replace("|", "").strip():
        recientes[clave_laxa] = ahora_ts


# ══════════════════════════════════════════════════════════════════════
# PAUSAS AUTOMÁTICAS — racha de rojos y ráfaga de picks
# ══════════════════════════════════════════════════════════════════════

def _obtener_pausa_activa(tipo_pick: str) -> dict | None:
    """
    Devuelve info de la pausa activa si existe, None si no hay pausa.
    Comprueba tanto racha_miss como ráfaga.
    """
    ahora_ts = datetime.now(timezone.utc).timestamp()
    pausas   = STATE.setdefault("pausas", {"racha_miss": {}, "rafaga": {}})
    for motivo in ("racha_miss", "rafaga"):
        fin = pausas.get(motivo, {}).get(tipo_pick)
        if isinstance(fin, float) and fin > ahora_ts:
            minutos_restantes = max(1, round((fin - ahora_ts) / 60))
            return {"motivo": motivo, "hasta": fin, "minutos_restantes": minutos_restantes}
    return None


def _activar_pausa(tipo_pick: str, motivo: str, minutos: int) -> None:
    """Activa una pausa para tipo_pick durante `minutos` minutos."""
    ahora_ts = datetime.now(timezone.utc).timestamp()
    pausas   = STATE.setdefault("pausas", {"racha_miss": {}, "rafaga": {}})
    pausas.setdefault(motivo, {})[tipo_pick] = ahora_ts + minutos * 60
    save_state()
    logger.warning(
        "PAUSA activada | motivo: %s | tipo: %s | duración: %d min",
        motivo, tipo_pick, minutos,
    )


def _registrar_pick_reciente(tipo_pick: str) -> None:
    """Registra el timestamp del pick publicado para detección de ráfaga."""
    ahora_ts  = datetime.now(timezone.utc).timestamp()
    recientes = STATE.setdefault("picks_recientes_ts", {})
    lista     = recientes.setdefault(tipo_pick, [])
    lista.append(ahora_ts)
    # Conservar solo los últimos 30 timestamps para no crecer indefinidamente
    recientes[tipo_pick] = lista[-30:]


def _verificar_rafaga(tipo_pick: str) -> None:
    """
    Tras publicar un pick, comprueba si se ha alcanzado el límite de ráfaga.
    Si RAFAGA_PICKS_LIMITE picks del mismo tipo se publicaron en los últimos
    RAFAGA_VENTANA_MIN minutos, activa una pausa de RAFAGA_PAUSA_MIN minutos.
    """
    ahora_ts  = datetime.now(timezone.utc).timestamp()
    ventana   = RAFAGA_VENTANA_MIN * 60
    recientes = STATE.get("picks_recientes_ts", {}).get(tipo_pick, [])
    en_ventana = sum(1 for ts in recientes if ahora_ts - ts <= ventana)

    if en_ventana >= RAFAGA_PICKS_LIMITE:
        _activar_pausa(tipo_pick, "rafaga", RAFAGA_PAUSA_MIN)
        logger.warning(
            "RÁFAGA detectada | tipo: %s | %d picks en %d min → pausa %d min",
            tipo_pick, en_ventana, RAFAGA_VENTANA_MIN, RAFAGA_PAUSA_MIN,
        )


def _verificar_racha_miss(tipo_pick: str) -> None:
    """
    Tras registrar un MISS, comprueba si hay racha de rojos.
    Si hay RACHA_MISS_LIMITE MISS consecutivos, activa pausa de RACHA_MISS_PAUSA_MIN min.
    Los VOIDs no interrumpen ni suman a la racha.
    """
    try:
        racha = db_racha_miss_actual(tipo_pick)
    except Exception as e:
        logger.error("Error consultando racha MISS: %s", e)
        return

    if racha >= RACHA_MISS_LIMITE:
        _activar_pausa(tipo_pick, "racha_miss", RACHA_MISS_PAUSA_MIN)
        logger.warning(
            "RACHA MISS | tipo: %s | %d MISS consecutivos → pausa %d min",
            tipo_pick, racha, RACHA_MISS_PAUSA_MIN,
        )


# ── Límite de tamaño del STATE ────────────────────────────────────────────────

_MAX_MENSAJES_PUBLICADOS = 2000


def limpiar_mensajes_publicados() -> None:
    """
    Recorta STATE['mensajes_publicados'] a las últimas _MAX_MENSAJES_PUBLICADOS
    entradas para evitar crecimiento ilimitado en memoria.
    Los dicts de Python mantienen orden de inserción (≥3.7), así que eliminamos
    las claves más antiguas (al principio del dict).
    """
    publicados = STATE.get("mensajes_publicados", {})
    exceso = len(publicados) - _MAX_MENSAJES_PUBLICADOS
    if exceso > 0:
        for clave in list(publicados.keys())[:exceso]:
            del publicados[clave]
        logger.debug("STATE limpiado: %d entradas antiguas de mensajes_publicados eliminadas.", exceso)


def _registro_publicado_desde_db(message_id_origen: int | str) -> dict | None:
    pick = db_pick_por_message_id(str(message_id_origen))
    if not pick:
        return None

    destinos_raw = pick.get("destinos_json") or ""
    try:
        destinos = json.loads(destinos_raw) if destinos_raw else {}
    except Exception:
        logger.warning(
            "No se pudo parsear destinos_json para msg_id %s",
            message_id_origen,
        )
        destinos = {}

    if not destinos:
        return None

    return {
        "tipo_pick": pick.get("tipo_pick") or "desconocido",
        "mensaje_base": pick.get("mensaje_base") or "",
        "mensaje_base_free": pick.get("mensaje_base_free") or pick.get("mensaje_base") or "",
        "destinos": destinos,
    }


# ==============================
# ENVÍO / EDICIÓN TELEGRAM
# ==============================

_TELEGRAM_MAX_LEN = 4096   # límite de Telegram para mensajes de texto


def _truncar_mensaje(texto: str) -> str:
    """Recorta el mensaje a _TELEGRAM_MAX_LEN y añade aviso si se truncó."""
    if len(texto) <= _TELEGRAM_MAX_LEN:
        return texto
    aviso = "\n\n⚠️ <i>[mensaje truncado]</i>"
    return texto[: _TELEGRAM_MAX_LEN - len(aviso)] + aviso


async def enviar_mensaje(
    context: ContextTypes.DEFAULT_TYPE,
    canal_id: int,
    texto: str,
    max_intentos: int = 3,
):
    """
    Envía un mensaje con reintentos automáticos:
    - Trunca a 4096 chars antes de enviar (límite de Telegram).
    - RetryAfter (rate-limit de Telegram): espera el tiempo indicado y reintenta.
    - NetworkError: backoff exponencial (1s, 2s) entre intentos.
    """
    texto = _truncar_mensaje(texto)
    for intento in range(max_intentos):
        try:
            return await context.bot.send_message(
                chat_id=canal_id,
                text=texto,
                parse_mode="HTML",
            )
        except tg_error.RetryAfter as e:
            espera = int(e.retry_after) + 1
            logger.warning(
                "Rate limit Telegram en canal %s — esperando %ds (intento %d/%d)",
                canal_id, espera, intento + 1, max_intentos,
            )
            await asyncio.sleep(espera)
        except tg_error.NetworkError as e:
            if intento < max_intentos - 1:
                espera = 2 ** intento
                logger.warning(
                    "Error de red enviando a %s: %s — reintentando en %ds (intento %d/%d)",
                    canal_id, e, espera, intento + 1, max_intentos,
                )
                await asyncio.sleep(espera)
            else:
                raise


# Mensajes de Telegram que indican que ya no es posible editar (>48h o borrado).
# Son situaciones normales y esperadas — se loguean como INFO, no como ERROR.
_ERRORES_EDICION_ESPERADOS = (
    "message can't be edited",
    "message to edit not found",
    "message is not modified",
    "there is no text in the message to edit",
)


async def editar_mensaje(
    context: ContextTypes.DEFAULT_TYPE,
    canal_id: int,
    message_id: int,
    texto_nuevo: str,
) -> None:
    texto_nuevo = _truncar_mensaje(texto_nuevo)
    try:
        await context.bot.edit_message_text(
            chat_id=canal_id,
            message_id=message_id,
            text=texto_nuevo,
            parse_mode="HTML",
        )
        logger.info("Mensaje editado en canal %s (msg %s)", canal_id, message_id)
    except tg_error.BadRequest as e:
        msg_lower = str(e).lower()
        if any(esperado in msg_lower for esperado in _ERRORES_EDICION_ESPERADOS):
            # Situación normal: el mensaje es demasiado antiguo, fue borrado,
            # o el texto ya era idéntico. No es un error de programación.
            logger.info(
                "Mensaje %s en canal %s no editable (>48h o sin cambios): %s",
                message_id, canal_id, e,
            )
        else:
            logger.error(
                "Error inesperado editando msg %s en canal %s: %s",
                message_id, canal_id, e,
            )
    except Exception as e:
        logger.error(
            "Error editando msg %s en canal %s: %s",
            message_id, canal_id, e,
        )


# ══════════════════════════════════════════════════════════════════════
# PUBLICACIÓN CARLOS MOLLAR — llamadas desde el buffer
# ══════════════════════════════════════════════════════════════════════

async def _publicar_cm_rem(
    context: ContextTypes.DEFAULT_TYPE,
    datos: dict,
    tipo_pick: str,
    msg_id: int,
    stake: float,
) -> None:
    """Publica el recordatorio REM de Carlos Mollar elegido por el buffer."""
    codigo_rem = (datos.get("codigo") or "").upper()
    codigo_pre = "PRE_" + codigo_rem[4:]

    mensaje_rem = construir_mensaje_base(datos, tipo_pick, rem_stake=stake)

    logger.info(
        "CM-REM | tipo: %s | codigo_pre: %s | stake: %.1fu | stk5: %s | msg_id: %s",
        tipo_pick, codigo_pre, stake, datos.get("stk5"), msg_id,
    )

    destinos_publicados: dict[str, int] = {}
    try:
        enviado = await enviar_mensaje(context, CANAL_PRE_ID, mensaje_rem)
        destinos_publicados[str(CANAL_PRE_ID)] = enviado.message_id
        logger.info("CM-REM enviado a %s (msg %s)", CANAL_PRE_ID, enviado.message_id)
    except Exception as e:
        logger.error("Error enviando CM-REM a %s: %s", CANAL_PRE_ID, e)

    if destinos_publicados:
        _registrar_alerta_reciente(datos, tipo_pick)
        STATE["mensajes_publicados"][str(msg_id)] = {
            "tipo_pick":         tipo_pick,
            "mensaje_base":      mensaje_rem,
            "mensaje_base_free": mensaje_rem,
            "destinos":          destinos_publicados,
            "es_recordatorio":   True,
            "codigo_pre":        codigo_pre,
            "partido":           datos.get("partido") or "",
        }
        db_guardar_publicacion(
            str(msg_id), mensaje_rem, mensaje_rem,
            json.dumps(destinos_publicados, ensure_ascii=False),
        )
    save_state()


async def _publicar_cm_pre(
    context: ContextTypes.DEFAULT_TYPE,
    datos: dict,
    tipo_pick: str,
    msg_id: int,
    stake: float,
) -> None:
    """Publica el pick PRE de Carlos Mollar elegido por el buffer."""
    datos["stake"] = stake   # el formateador lo muestra en el mensaje
    mensaje_pre = construir_mensaje_base(datos, tipo_pick)

    logger.info(
        "CM-PRE | tipo: %s | codigo: %s | stake: %.1fu | stk5: %s | msg_id: %s",
        tipo_pick, datos.get("codigo"), stake, datos.get("stk5"), msg_id,
    )

    destinos_publicados: dict[str, int] = {}
    try:
        enviado = await enviar_mensaje(context, CANAL_PRE_ID, mensaje_pre)
        destinos_publicados[str(CANAL_PRE_ID)] = enviado.message_id
        logger.info("CM-PRE enviado a %s (msg %s)", CANAL_PRE_ID, enviado.message_id)
    except Exception as e:
        logger.error("Error enviando CM-PRE a %s: %s", CANAL_PRE_ID, e)

    if destinos_publicados:
        _registrar_alerta_reciente(datos, tipo_pick)
        registrar_pick_estadistica(msg_id, datos, tipo_pick, enviado_a_free=False)
        db_guardar_publicacion(
            str(msg_id), mensaje_pre, mensaje_pre,
            json.dumps(destinos_publicados, ensure_ascii=False),
        )
        STATE["mensajes_publicados"][str(msg_id)] = {
            "tipo_pick":         tipo_pick,
            "mensaje_base":      mensaje_pre,
            "mensaje_base_free": mensaje_pre,
            "destinos":          destinos_publicados,
        }
    save_state()


async def _despachar_cm(
    context: ContextTypes.DEFAULT_TYPE,
    buffer_key: str,
) -> None:
    """
    Espera _CM_BUFFER_SEG segundos y publica el pick Carlos Mollar
    de mayor prioridad del buffer:
      - Si existe alguno con STK5 → se publica ESE (stake 5u).
      - Si no hay STK5 → se publica el primero en llegar (stake 3u).
    El resto de alertas del buffer se descartan silenciosamente.
    """
    await asyncio.sleep(_CM_BUFFER_SEG)

    entry = _cm_buffer.pop(buffer_key, None)
    if not entry:
        return

    alertas = entry["alertas"]

    # Preferencia: STK5 primero; si no hay, la primera llegada
    mejor = next((a for a in alertas if a["datos"].get("stk5")), alertas[0])
    descartadas = len(alertas) - 1
    if descartadas > 0:
        logger.info(
            "CM buffer: %d alerta(s) descartada(s) para '%s' — publicando msg %s (stk5=%s)",
            descartadas,
            mejor["datos"].get("partido"),
            mejor["msg_id"],
            mejor["datos"].get("stk5"),
        )

    stake = _cm_stake(mejor["datos"])

    if mejor["fase"] == "REM":
        await _publicar_cm_rem(
            context, mejor["datos"], mejor["tipo_pick"], mejor["msg_id"], stake,
        )
    elif mejor["fase"] == "PRE":
        await _publicar_cm_pre(
            context, mejor["datos"], mejor["tipo_pick"], mejor["msg_id"], stake,
        )


async def _encolar_cm(
    context: ContextTypes.DEFAULT_TYPE,
    datos: dict,
    tipo_pick: str,
    msg_id: int,
    fase: str,
) -> None:
    """
    Añade el pick Carlos Mollar al buffer y lanza el temporizador de despacho
    si aún no estaba activo para este partido/tipo/fase.
    Las alertas posteriores del mismo grupo se añaden al buffer existente
    sin reiniciar el temporizador.
    """
    key = _cm_key(datos, tipo_pick, fase)

    if key not in _cm_buffer:
        _cm_buffer[key] = {"alertas": [], "task": None}

    _cm_buffer[key]["alertas"].append({
        "datos":     datos,
        "tipo_pick": tipo_pick,
        "msg_id":    msg_id,
        "fase":      fase,
    })

    n = len(_cm_buffer[key]["alertas"])
    logger.info(
        "CM buffer | fase: %s | partido: %s | stk5: %s | msg_id: %s | alertas en buffer: %d",
        fase, datos.get("partido"), datos.get("stk5"), msg_id, n,
    )

    # El temporizador se crea solo una vez (al llegar la primera alerta del grupo).
    # Las siguientes se añaden al buffer mientras el timer está corriendo.
    existing = _cm_buffer[key].get("task")
    if existing is None or existing.done():
        _cm_buffer[key]["task"] = asyncio.create_task(_despachar_cm(context, key))


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
    es_prepartido   = (fase == "PRE")
    es_recordatorio = (fase == "REM")

    # ── Carlos Mollar PRE/REM → buffer con ventana STK5 ──────────────
    # Bypassa el check de duplicados estándar: el buffer gestiona la
    # selección interna (STK5 > no-STK5) y registra la alerta como
    # "reciente" solo cuando realmente se publica.
    if (es_prepartido or es_recordatorio) and _es_carlos_mollar(datos):
        await _encolar_cm(context, datos, tipo_pick, msg_id, fase)
        return

    if _es_alerta_duplicada(datos, tipo_pick):
        logger.info(
            "Ignorada por duplicada | tipo: %s | codigo: %s | partido: %s",
            tipo_pick,
            datos.get("codigo"),
            datos.get("partido"),
        )
        return

    # ── Picks RECORDATORIO (REM_*) → canal PRE según sistema, vincula con PRE original ─
    if es_recordatorio:
        codigo_rem = (datos.get("codigo") or "").upper()   # REM_O25FT
        codigo_pre = "PRE_" + codigo_rem[4:]               # PRE_O25FT

        # Routing: Carlos Mollar → CANAL_PRE_ID; resto → CANAL_PRE_GENERAL_ID
        canal_rem = CANAL_PRE_ID if _es_carlos_mollar(datos) else CANAL_PRE_GENERAL_ID

        if not canal_rem:
            logger.warning("Pick recordatorio recibido pero el canal destino no está configurado.")
            return

        # Stake dinámico: scorer_pre usa liga y cuota real del alert
        _liga_rem = datos.get("liga")
        _odds_rem = _extraer_odds_pre(datos, codigo_pre)
        stake     = calcular_stake_pre(codigo_pre, tipo_pick, liga=_liga_rem, odds=_odds_rem)

        mensaje_rem = construir_mensaje_base(
            datos, tipo_pick,
            rem_stake=stake,
        )

        logger.info(
            "RECORDATORIO | tipo: %s | codigo_pre: %s | stake: %su | canal: %s | origen msg_id: %s",
            tipo_pick, codigo_pre, stake, canal_rem, msg_id,
        )

        destinos_publicados: dict[str, int] = {}
        try:
            enviado = await enviar_mensaje(context, canal_rem, mensaje_rem)
            destinos_publicados[str(canal_rem)] = enviado.message_id
            logger.info(f"Recordatorio enviado a {canal_rem} (msg {enviado.message_id})")
        except Exception as e:
            logger.error(f"Error enviando recordatorio a {canal_rem}: {e}")

        # Guardamos en STATE para poder editar el mensaje cuando llegue el resultado.
        # NO registramos en DB como estadística (evita duplicar con el PRE original).
        STATE["mensajes_publicados"][str(msg_id)] = {
            "tipo_pick":         tipo_pick,
            "mensaje_base":      mensaje_rem,
            "mensaje_base_free": mensaje_rem,
            "destinos":          destinos_publicados,
            "es_recordatorio":   True,
            "codigo_pre":        codigo_pre,
            "partido":           datos.get("partido") or "",
        }

        if destinos_publicados:
            _registrar_alerta_reciente(datos, tipo_pick)
            db_guardar_publicacion(
                str(msg_id),
                mensaje_rem,
                mensaje_rem,
                json.dumps(destinos_publicados, ensure_ascii=False),
            )

        save_state()
        return

    # ── Picks PREPARTIDO → canal PRE según sistema, sin FREE ni canales live ────
    if es_prepartido:
        # Routing: Carlos Mollar → CANAL_PRE_ID; resto → CANAL_PRE_GENERAL_ID
        canal_pre = CANAL_PRE_ID if _es_carlos_mollar(datos) else CANAL_PRE_GENERAL_ID

        if not canal_pre:
            logger.warning("Pick prepartido recibido pero el canal destino no está configurado.")
            return

        # Stake dinámico con scorer_pre (liga + cuota real del alert)
        _codigo_pre = (datos.get("codigo") or "").upper()
        _odds_pre   = _extraer_odds_pre(datos, _codigo_pre)
        _liga_pre   = datos.get("liga")
        _stake_pre  = calcular_stake_pre(_codigo_pre, tipo_pick, liga=_liga_pre, odds=_odds_pre)
        datos["stake"] = _stake_pre   # formateador lo usa para mostrar en el mensaje

        mensaje_pre = construir_mensaje_base(datos, tipo_pick)
        logger.info(
            "PREPARTIDO | tipo: %s | codigo: %s | stake: %.1fu | canal: %s | origen msg_id: %s",
            tipo_pick, _codigo_pre, _stake_pre, canal_pre, msg_id,
        )

        destinos_publicados: dict[str, int] = {}
        try:
            enviado = await enviar_mensaje(context, canal_pre, mensaje_pre)
            destinos_publicados[str(canal_pre)] = enviado.message_id
            logger.info(f"Prepartido enviado a {canal_pre} (msg {enviado.message_id})")
        except Exception as e:
            logger.error(f"Error enviando prepartido a {canal_pre}: {e}")

        STATE["mensajes_publicados"][str(msg_id)] = {
            "tipo_pick":         tipo_pick,
            "mensaje_base":      mensaje_pre,
            "mensaje_base_free": mensaje_pre,
            "destinos":          destinos_publicados,
        }
        if destinos_publicados:
            _registrar_alerta_reciente(datos, tipo_pick)
            registrar_pick_estadistica(msg_id, datos, tipo_pick, enviado_a_free=False)
            db_guardar_publicacion(
                str(msg_id),
                mensaje_pre,
                mensaje_pre,
                json.dumps(destinos_publicados, ensure_ascii=False),
            )
        save_state()
        return

    # ── Comprobación de pausas activas (racha MISS o ráfaga) ─────────
    # Las pausas solo aplican a picks LIVE; los prepartidos no se ven afectados.
    pausa = _obtener_pausa_activa(tipo_pick)
    if pausa:
        logger.info(
            "Pick bloqueado por pausa '%s' | tipo: %s | %d min restantes | partido: %s",
            pausa["motivo"], tipo_pick, pausa["minutos_restantes"], datos.get("partido"),
        )
        return

    # ── Filtro strike corners — sábado 17-22h ────────────────────────
    if tipo_pick == "corner" and not es_prepartido:
        ahora = ahora_madrid()
        if ahora.weekday() == 5 and FINDE_CORNER_HORA_INICIO <= ahora.hour < FINDE_CORNER_HORA_FIN:
            sa = parse_percent(datos.get("strike_alerta"))
            if sa is not None and sa < FINDE_CORNER_STRIKE_MIN:
                logger.info(
                    "Corner descartado por strike bajo en sábado tarde | "
                    "strike_alerta: %s%% < %s%% | partido: %s",
                    sa, FINDE_CORNER_STRIKE_MIN, datos.get("partido"),
                )
                return

    # ── Picks LIVE — clasificar antes de enrutar ──────────────────────
    clasificacion = clasificar_alerta(datos, tipo_pick)
    nivel         = clasificacion["nivel"]

    logger.info(
        "CLASIFICACIÓN | %s | nivel=%s (%s) | stake=%.1fu | partido=%s",
        tipo_pick,
        nivel,
        clasificacion["nombre"],
        clasificacion["stake"],
        datos.get("partido"),
    )

    # ── Filtro stake 0 — no apostar ───────────────────────────────────
    # Si el clasificador asigna stake=0 significa que las condiciones no
    # son favorables para apostar. Se descarta sin publicar nada.
    if clasificacion["stake"] == 0:
        logger.info(
            "Pick con stake 0 (no apostar) — descartado | partido: %s",
            datos.get("partido"),
        )
        return

    # ── Filtro de bajo perfil en fin de semana ────────────────────────
    # En sábado y domingo hay más volumen de picks de calidad, así que
    # los picks BAJO (nivel 0) se descartan para no saturar los canales.
    # Entre semana sí se publican porque hay menos actividad.
    # weekday(): 0=lunes … 4=viernes, 5=sábado, 6=domingo
    if nivel == 0 and ahora_madrid().weekday() >= 5:
        logger.info(
            "Pick BAJO en fin de semana — descartado para reducir volumen "
            "(partido: %s)",
            datos.get("partido"),
        )
        return

    # Canal principal por tipo de pick
    canales_destino = []
    if tipo_pick == "corner":
        canales_destino.append(CANAL_CORNERS_ID)
    elif tipo_pick == "gol":
        canales_destino.append(CANAL_GOLES_ID)

    # Canal general — solo para picks FAVORABLE o superior (nivel >= 1)
    if ENVIAR_A_GENERAL and nivel >= 1:
        canales_destino.append(CANAL_GENERAL_ID)

    mensaje_base      = construir_mensaje_base(datos, tipo_pick,                clasificacion=clasificacion)
    mensaje_base_free = construir_mensaje_base(datos, tipo_pick, para_free=True, clasificacion=clasificacion)

    logger.info(f"NUEVO | tipo: {tipo_pick} | nivel: {clasificacion['nombre']} | origen msg_id: {msg_id} | destinos: {canales_destino}")

    destinos_publicados: dict[str, int] = {}

    for canal_id in canales_destino:
        try:
            enviado = await enviar_mensaje(context, canal_id, mensaje_base)
            destinos_publicados[str(canal_id)] = enviado.message_id
            logger.info(f"Enviado a {canal_id} (msg {enviado.message_id})")
        except Exception as e:
            logger.error(f"Error enviando a {canal_id}: {e}")

    # Canal FREE — solo para picks FAVORABLE o superior (nivel >= 1)
    # intentar_envio_free hace el check+register de forma atómica (asyncio.Lock)
    # para evitar que dos picks simultáneos burlen el límite de cupos.
    enviado_a_free = False
    if nivel >= 1:
        ok_free, motivo_free = await intentar_envio_free(tipo_pick, datos)
        if ok_free:
            try:
                enviado_free = await enviar_mensaje(context, CANAL_FREE_ID, mensaje_base_free)
                destinos_publicados[str(CANAL_FREE_ID)] = enviado_free.message_id
                enviado_a_free = True
                logger.info(f"Enviado a FREE {CANAL_FREE_ID} (msg {enviado_free.message_id})")
            except Exception as e:
                logger.error(f"Error enviando a FREE: {e}")
        else:
            logger.info(f"No enviado a FREE: {motivo_free}")
    else:
        logger.info(f"Pick BAJO (nivel 0) — no se envía a GENERAL ni a FREE")

    # Guardar en estado
    STATE["mensajes_publicados"][str(msg_id)] = {
        "tipo_pick":         tipo_pick,
        "mensaje_base":      mensaje_base,
        "mensaje_base_free": mensaje_base_free,
        "destinos":          destinos_publicados,
    }
    if destinos_publicados:
        _registrar_alerta_reciente(datos, tipo_pick)
        # Solo registrar en estadísticas si el pick llegó a al menos un canal,
        # igual que en el flujo prepartido. Evita contar picks cuyo envío falló.
        registrar_pick_estadistica(msg_id, datos, tipo_pick, enviado_a_free=enviado_a_free, nivel=clasificacion["nombre"])
        # Registrar timestamp para detección de ráfaga y verificar si se activa pausa
        _registrar_pick_reciente(tipo_pick)
        _verificar_rafaga(tipo_pick)

    db_guardar_publicacion(
        str(msg_id),
        mensaje_base,
        mensaje_base_free,
        json.dumps(destinos_publicados, ensure_ascii=False),
    )
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

    # tipo_pick se determina según la fuente disponible; siempre se inicializa
    tipo_pick = detectar_tipo_pick_por_codigo(datos) or "desconocido"

    # ── Caso 1: registro en STATE (arranque normal) ───────────────────
    if key in STATE["mensajes_publicados"]:
        registro  = STATE["mensajes_publicados"][key]
        tipo_pick = registro["tipo_pick"]
        destinos  = registro["destinos"]

        logger.info(
            "EDITADO | tipo: %s | resultado: %s | msg_id: %s",
            tipo_pick, datos.get("resultado"), msg_id,
        )
        for canal_id_str, msg_id_publicado in destinos.items():
            base = (
                registro["mensaje_base_free"]
                if str(canal_id_str) == str(CANAL_FREE_ID)
                else registro["mensaje_base"]
            )
            texto_editado = construir_mensaje_editado(base, datos, tipo_pick)
            await editar_mensaje(context, int(canal_id_str), msg_id_publicado, texto_editado)

    # ── Caso 2: bot se reinició y perdimos el STATE → recuperar de DB ─
    else:
        registro_db = _registro_publicado_desde_db(msg_id)
        if registro_db:
            tipo_pick = registro_db["tipo_pick"]
            for canal_id_str, msg_id_publicado in registro_db["destinos"].items():
                base = (
                    registro_db["mensaje_base_free"]
                    if str(canal_id_str) == str(CANAL_FREE_ID)
                    else registro_db["mensaje_base"]
                )
                texto_editado = construir_mensaje_editado(base, datos, tipo_pick)
                await editar_mensaje(context, int(canal_id_str), int(msg_id_publicado), texto_editado)
            STATE["mensajes_publicados"][key] = registro_db
            logger.warning(
                "EDITADO RECUPERADO DE DB | msg_id: %s | resultado: %s",
                msg_id, datos.get("resultado"),
            )
        else:
            logger.warning(
                "EDITADO SIN REFERENCIA | msg_id: %s | resultado: %s | "
                "Sin registro en STATE ni en DB — solo se actualiza la estadística.",
                msg_id, datos.get("resultado"),
            )

    actualizado = actualizar_resultado_estadistica(msg_id, datos.get("resultado"))
    if not actualizado:
        # Puede ser normal si es un REM (no registrado en DB como estadística)
        logger.info(
            "Pick %s no actualizado en DB — puede ser un recordatorio REM (esperado).",
            msg_id,
        )
    save_state()

    # ── Propagación REM → PRE ─────────────────────────────────────────
    # Si el mensaje editado es un recordatorio (REM_*), propaga el resultado
    # al pick PRE original usando fuzzy match de nombre de partido.
    codigo_edit = (datos.get("codigo") or "").upper()
    if codigo_edit.startswith("REM_") and datos.get("resultado") and datos.get("partido"):
        codigo_pre  = "PRE_" + codigo_edit[4:]
        propagar_resultado_rem_a_pre(
            rem_message_id = msg_id,
            codigo_pre     = codigo_pre,
            partido_rem    = datos.get("partido", ""),
            tipo_pick      = tipo_pick,
            resultado      = datos.get("resultado"),
        )

    resultado_actual = datos.get("resultado", "").upper()

    # Comprobar racha solo cuando el resultado es HIT
    if resultado_actual == "HIT":
        await verificar_racha_y_notificar(context, tipo_pick)

    # Comprobar racha de MISS y activar pausa si es necesario
    if resultado_actual == "MISS":
        _verificar_racha_miss(tipo_pick)

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

async def cmd_analisis_filtros(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Analiza el historico resuelto y propone patrones candidatos a filtro.
    Uso:
      /analisis_filtros
      /analisis_filtros CM01
      /analisis_filtros gol
    Solo admins.
    """
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        await update.message.reply_text("No tienes permisos para usar este comando.")
        return

    filtro = " ".join(context.args).strip() if context.args else None
    await enviar_analisis_filtros_comando(update, filtro=filtro)


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
            f"No se encontró ningún pick con el message_id {msg_id_str} en la DB."
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
        if not (0 <= corners_finales <= 150):
            raise ValueError("fuera de rango")
    except ValueError:
        await update.message.reply_text(
            "El total final de corners debe ser un número entero entre 0 y 150."
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
