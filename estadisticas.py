import logging
from datetime import timedelta

from utils import hoy_str, ahora_str, semana_str, ahora_madrid
from db import (
    db_registrar_pick,
    db_actualizar_resultado,
    db_picks_por_periodo,
    db_picks_filtrados,
    db_calcular_racha_actual,
    db_ya_publicado,
    db_marcar_publicado,
    db_stats_prepartido_por_mes,
    db_stats_prepartido_global,
)
from config import RESUMENES_CONFIG, RACHA_MINIMA, CANAL_RACHA_ID

logger = logging.getLogger(__name__)


# ==============================
# REGISTRO DE PICKS
# ==============================

def registrar_pick_estadistica(
    message_id_origen,
    datos: dict,
    tipo_pick: str,
    enviado_a_free: bool = False,
) -> None:
    # Extraer cuota para picks prepartido (PRE_1X usa cuota local = primera de 1X2)
    odds = None
    codigo = datos.get("codigo") or ""
    if codigo.upper().startswith("PRE_"):
        odds_raw = datos.get("odds_1x2") or ""
        partes = [p.strip() for p in odds_raw.replace("|", " ").split() if p.strip()]
        if partes:
            try:
                odds = float(partes[0].replace(",", "."))
            except ValueError:
                pass

    db_registrar_pick(
        message_id_origen = str(message_id_origen),
        codigo            = datos.get("codigo"),
        tipo_pick         = tipo_pick,
        liga              = datos.get("liga"),
        partido           = datos.get("partido"),
        strike_alerta     = datos.get("strike_alerta"),
        strike_liga       = datos.get("strike_liga"),
        enviado_a_free    = enviado_a_free,
        fecha             = hoy_str(),
        fecha_hora        = ahora_str(),
        odds              = odds,
    )


def actualizar_resultado_estadistica(message_id_origen, resultado: str) -> None:
    db_actualizar_resultado(str(message_id_origen), resultado)


# ==============================
# FILTRO POR TIPO
# ==============================

def filtrar_por_tipo(lista: list, tipo_pick) -> list:
    """
    tipo_pick: "gol" | "corner" | None (todos) | "free" (solo enviados al free)
    """
    if tipo_pick is None:
        return lista
    if tipo_pick == "free":
        return [x for x in lista if x.get("enviado_a_free")]
    return [x for x in lista if x.get("tipo_pick") == tipo_pick]


# ==============================
# CONSTRUCCIÓN DEL TEXTO DE RESUMEN
# ==============================

def construir_resumen(lista: list, titulo: str) -> str:
    total      = len(lista)
    hits       = sum(1 for x in lista if x.get("resultado") == "HIT")
    miss       = sum(1 for x in lista if x.get("resultado") == "MISS")
    voids      = sum(1 for x in lista if x.get("resultado") == "VOID")
    pendientes = total - hits - miss - voids
    resueltos  = hits + miss
    strike     = round((hits / resueltos) * 100, 1) if resueltos > 0 else 0

    goles   = [x for x in lista if x.get("tipo_pick") == "gol"]
    corners = [x for x in lista if x.get("tipo_pick") == "corner"]

    def stats(subset):
        h = sum(1 for x in subset if x.get("resultado") == "HIT")
        m = sum(1 for x in subset if x.get("resultado") == "MISS")
        v = sum(1 for x in subset if x.get("resultado") == "VOID")
        return h, m, v

    gh, gm, gv = stats(goles)
    ch, cm, cv = stats(corners)

    lineas = [
        f"📊 {titulo}", "",
        f"Total picks: {total}",
        f"✅ Hits: {hits}",
        f"❌ Miss: {miss}",
        f"⚪ Nulos: {voids}",
        f"⏳ Pendientes: {pendientes}",
        f"📈 Strike: {strike}%",
    ]

    if goles:
        lineas.append(f"\n⚽ Goles: {len(goles)} | ✅ {gh} | ❌ {gm} | ⚪ {gv}")
    if corners:
        lineas.append(f"🚩 Corners: {len(corners)} | ✅ {ch} | ❌ {cm} | ⚪ {cv}")

    return "\n".join(lineas)


def _construir_resumen_anual(lista: list) -> str:
    """
    Resumen anual con desglose mes a mes.
    """
    from collections import defaultdict

    anio_actual = ahora_madrid().year
    texto_base  = construir_resumen(lista, f"RESUMEN ANUAL {anio_actual}")

    # Desglose por mes
    por_mes: dict[str, list] = defaultdict(list)
    for pick in lista:
        fecha = pick.get("fecha")
        if fecha:
            mes_key = str(fecha)[:7]  # "YYYY-MM"
            por_mes[mes_key].append(pick)

    if not por_mes:
        return texto_base

    # Nombres de mes en español
    meses_es = {
        "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
    }

    lineas_mes = ["\n📆 Desglose mensual:"]
    for mes_key in sorted(por_mes.keys(), reverse=True):
        picks_mes  = por_mes[mes_key]
        hits_m     = sum(1 for x in picks_mes if x.get("resultado") == "HIT")
        misses_m   = sum(1 for x in picks_mes if x.get("resultado") == "MISS")
        resueltos  = hits_m + misses_m
        strike_m   = round((hits_m / resueltos) * 100, 1) if resueltos > 0 else 0
        nombre_mes = meses_es.get(mes_key[5:], mes_key[5:])
        lineas_mes.append(
            f"  {nombre_mes}: {len(picks_mes)} picks | {strike_m}% strike"
        )

    return texto_base + "\n".join(lineas_mes)


# ==============================
# CLAVES Y CONDICIONES DE PUBLICACIÓN
# ==============================

def _clave_periodo(periodo: str) -> str:
    ahora = ahora_madrid()
    if periodo == "dia":
        return hoy_str()
    if periodo == "semana":
        return semana_str()
    if periodo == "mes":
        primer_dia = ahora.replace(day=1)
        mes_anterior = primer_dia - timedelta(days=1)
        return mes_anterior.strftime("%Y-%m")
    return ""


def _debe_publicar_ahora(periodo: str) -> bool:
    ahora = ahora_madrid()
    if periodo == "dia":
        return ahora.hour >= 22
    if periodo == "semana":
        return ahora.weekday() == 6 and ahora.hour >= 22
    if periodo == "mes":
        return ahora.day == 1 and ahora.hour >= 10
    return False


_TITULOS = {
    ("dia",    "gol"):    "RESUMEN DEL DÍA — GOLES",
    ("dia",    "corner"): "RESUMEN DEL DÍA — CORNERS",
    ("dia",    None):     "RESUMEN DEL DÍA — GENERAL",
    ("dia",    "free"):   "RESUMEN DEL DÍA — CANAL FREE",
    ("semana", "gol"):    "RESUMEN SEMANAL — GOLES",
    ("semana", "corner"): "RESUMEN SEMANAL — CORNERS",
    ("semana", None):     "RESUMEN SEMANAL — GENERAL",
    ("semana", "free"):   "RESUMEN SEMANAL — CANAL FREE",
    ("mes",    "gol"):    "RESUMEN MENSUAL — GOLES",
    ("mes",    "corner"): "RESUMEN MENSUAL — CORNERS",
    ("mes",    None):     "RESUMEN MENSUAL — GENERAL",
    ("mes",    "free"):   "RESUMEN MENSUAL — CANAL FREE",
}

_PERIODO_DB = {
    "dia":    "dia",
    "semana": "semana",
    "mes":    "mes_anterior",
    "anio":   "anio",
}


def _titulo_resumen(periodo: str, tipo_pick, label: str) -> str:
    return _TITULOS.get((periodo, tipo_pick), f"RESUMEN — {label}")


# ==============================
# PUBLICACIÓN AUTOMÁTICA
# ==============================

async def publicar_resumenes_si_toca(context, periodo: str) -> None:
    if not _debe_publicar_ahora(periodo):
        return

    clave_valor = _clave_periodo(periodo)
    periodo_db  = _PERIODO_DB.get(periodo, periodo)
    lista_base  = db_picks_por_periodo(periodo_db)

    for cfg in RESUMENES_CONFIG:
        resumen_id    = cfg["id"]
        canal_id      = cfg["canal_id"]
        tipo_pick     = cfg["tipo_pick"]
        label         = cfg["label"]
        clave_control = f"{resumen_id}_{periodo}"

        if db_ya_publicado(clave_control, clave_valor):
            continue

        lista = filtrar_por_tipo(lista_base, tipo_pick)

        if not lista:
            logger.info(f"Resumen {resumen_id}/{periodo}: sin picks, se omite.")
            db_marcar_publicado(clave_control, clave_valor)
            continue

        titulo = _titulo_resumen(periodo, tipo_pick, label)
        texto  = construir_resumen(lista, titulo)

        try:
            await context.bot.send_message(chat_id=canal_id, text=texto)
            db_marcar_publicado(clave_control, clave_valor)
            logger.info(f"Resumen publicado: {resumen_id}/{periodo} → canal {canal_id}")
        except Exception as e:
            logger.error(f"Error publicando resumen {resumen_id}/{periodo}: {e}")


async def publicar_resumen_diario_si_toca(context) -> None:
    await publicar_resumenes_si_toca(context, "dia")


async def publicar_resumen_semanal_si_toca(context) -> None:
    await publicar_resumenes_si_toca(context, "semana")


async def publicar_resumen_mensual_si_toca(context) -> None:
    await publicar_resumenes_si_toca(context, "mes")


# ==============================
# RACHA — NOTIFICACIÓN AUTOMÁTICA
# ==============================

async def verificar_racha_y_notificar(context, tipo_pick: str) -> None:
    """
    Comprueba la racha de HITs consecutivos tras actualizar un resultado.
    Notifica al canal CANAL_RACHA_ID cuando la racha alcanza un múltiplo
    de RACHA_MINIMA (5, 10, 15...).
    Los VOIDs no cortan la racha.
    """
    try:
        racha = db_calcular_racha_actual()
        if racha < RACHA_MINIMA or racha % RACHA_MINIMA != 0:
            return

        emoji_tipo = "⚽" if tipo_pick == "gol" else "🚩"
        tipo_nombre = "Goles" if tipo_pick == "gol" else "Corners"

        texto = (
            f"🔥 ¡RACHA ACTIVA!\n\n"
            f"{emoji_tipo} {racha} HITs consecutivos en el canal\n"
            f"(goles + corners combinados)\n\n"
            f"El último resultado ha sido {tipo_nombre}.\n"
            f"¡Seguimos! 💪"
        )

        await context.bot.send_message(chat_id=CANAL_RACHA_ID, text=texto)
        logger.info(f"Notificación de racha enviada: {racha} HITs consecutivos")
    except Exception as e:
        logger.error(f"Error en verificar_racha_y_notificar: {e}")


# ==============================
# RESÚMENES BAJO DEMANDA (comandos)
# ==============================

async def enviar_resumenes_comando(update, periodo: str) -> None:
    periodo_db = _PERIODO_DB.get(periodo, periodo)
    lista_base = db_picks_por_periodo(periodo_db)

    nombres = {"dia": "hoy", "semana": "esta semana", "mes": "el mes pasado"}
    nombre  = nombres.get(periodo, periodo)

    if not lista_base:
        await update.message.reply_text(f"No hay picks registrados para {nombre}.")
        return

    for cfg in RESUMENES_CONFIG:
        lista  = filtrar_por_tipo(lista_base, cfg["tipo_pick"])
        titulo = _titulo_resumen(periodo, cfg["tipo_pick"], cfg["label"])
        texto  = (
            construir_resumen(lista, titulo)
            if lista
            else f"📊 {titulo}\n\nSin picks para {nombre}."
        )
        await update.message.reply_text(texto)


async def enviar_resumen_anual_comando(update) -> None:
    """
    Resumen del año en curso con desglose mes a mes, enviado como respuesta
    al comando /resumen_anual.
    """
    lista = db_picks_por_periodo("anio")

    if not lista:
        await update.message.reply_text("No hay picks registrados para este año.")
        return

    texto = _construir_resumen_anual(lista)
    await update.message.reply_text(texto)


async def enviar_resumen_liga_comando(update, liga: str) -> None:
    """
    Resumen de todos los picks de una liga específica.
    Uso: /resumen_liga LALIGA
    """
    lista = db_picks_filtrados(liga=liga)

    if not lista:
        await update.message.reply_text(
            f"No se encontraron picks para la liga: {liga}"
        )
        return

    titulo = f"RESUMEN — Liga: {liga.upper()}"
    texto  = construir_resumen(lista, titulo)
    await update.message.reply_text(texto)


async def enviar_resumen_prepartido_comando(update) -> None:
    """
    Resumen de picks prepartido con desglose mes a mes por estrategia.
    Incluye profit en unidades y ROI.
    Uso: /resumen_pre
    """
    _NOMBRES = {
        "PRE_O25FT": "Over 2.5 FT",
        "PRE_1X":    "Ganador Local",
        "PRE_O15FT": "Over 1.5 FT",
        "PRE_O05FT": "Over 0.5 FT",
    }

    meses_es = {
        "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
        "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
        "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
    }

    global_rows = db_stats_prepartido_global()
    mes_rows    = db_stats_prepartido_por_mes()

    if not global_rows:
        await update.message.reply_text("No hay picks prepartido registrados aún.")
        return

    from collections import defaultdict
    por_codigo: dict[str, list] = defaultdict(list)
    for row in mes_rows:
        por_codigo[row["codigo"]].append(row)

    lineas = ["📊 RESUMEN PREPARTIDO\n"]

    for g in global_rows:
        codigo    = g["codigo"]
        nombre    = _NOMBRES.get(codigo, codigo)
        total     = g["total"]
        hits      = g["hits"]
        misses    = g["misses"]
        voids     = g["voids"]
        resueltos = hits + misses
        strike    = round(hits / resueltos * 100, 1) if resueltos > 0 else 0

        # Profit y ROI globales
        profit_u  = float(g["profit_units"]  or 0)
        staked_u  = float(g["staked_units"]  or 0)
        con_odds  = int(g["picks_con_odds"]  or 0)
        roi       = round(profit_u / staked_u * 100, 1) if staked_u > 0 else 0
        profit_signo = f"+{profit_u:.2f}" if profit_u >= 0 else f"{profit_u:.2f}"
        roi_signo    = f"+{roi}%" if roi >= 0 else f"{roi}%"
        roi_emoji    = "📈" if roi >= 0 else "📉"

        lineas.append("━━━━━━━━━━━━━━━━━━━━")
        lineas.append(f"📌 {nombre}  ({codigo})")
        lineas.append(
            f"Total: {total} | ✅ {hits} | ❌ {misses} | ⚪ {voids} | "
            f"Strike: {strike}%"
        )
        if con_odds > 0:
            lineas.append(
                f"{roi_emoji} Profit: {profit_signo}u | ROI: {roi_signo}  "
                f"<i>({con_odds} picks con cuota)</i>"
            )

        # Desglose mensual
        meses = por_codigo.get(codigo, [])
        if meses:
            lineas.append("📆 Mes a mes:")
            for m in meses:
                mes_key    = m["mes"]
                nombre_mes = meses_es.get(mes_key[5:], mes_key[5:])
                h  = m["hits"]
                mi = m["misses"]
                v  = m["voids"]
                res_m   = h + mi
                strike_m = round(h / res_m * 100, 1) if res_m > 0 else 0
                pend_m   = m["total"] - res_m - v
                pend_txt = f" ⏳{pend_m}" if pend_m > 0 else ""

                # Profit mensual
                p_u = float(m["profit_units"] or 0)
                c_o = int(m["picks_con_odds"] or 0)
                if c_o > 0:
                    p_signo = f"+{p_u:.2f}u" if p_u >= 0 else f"{p_u:.2f}u"
                    profit_txt = f" | {p_signo}"
                else:
                    profit_txt = ""

                lineas.append(
                    f"  {nombre_mes} {mes_key[:4]}: "
                    f"✅{h} ❌{mi} ⚪{v}{pend_txt} "
                    f"→ {strike_m}%{profit_txt}"
                )

        lineas.append("")

    await update.message.reply_text("\n".join(lineas), parse_mode="HTML")


async def enviar_resumen_codigo_comando(update, codigo: str) -> None:
    """
    Resumen de todos los picks de un código específico.
    Uso: /resumen_codigo CM02
    """
    lista = db_picks_filtrados(codigo=codigo)

    if not lista:
        await update.message.reply_text(
            f"No se encontraron picks con el código: {codigo}"
        )
        return

    titulo = f"RESUMEN — Código: {codigo.upper()}"
    texto  = construir_resumen(lista, titulo)
    await update.message.reply_text(texto)
