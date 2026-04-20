import logging
from collections import defaultdict
from datetime import timedelta

from extractor import (
    detectar_linea_por_codigo,
    detectar_modo_por_codigo,
    detectar_periodo_por_codigo,
)
from espn import resolver_corner_pick_espn
from utils import (
    hoy_str,
    ahora_str,
    semana_str,
    ahora_madrid,
    parse_dupla_numerica,
    parse_marcador_total,
    parse_percent,
)
from db import (
    db_registrar_pick,
    db_actualizar_resultado_confirmado,
    db_pick_por_message_id,
    db_picks_por_periodo,
    db_picks_filtrados,
    db_picks_para_analisis,
    db_picks_pendientes_revision,
    db_calcular_racha_actual,
    db_ya_publicado,
    db_marcar_publicado,
    db_stats_prepartido_por_mes,
    db_stats_prepartido_global,
    db_buscar_pre_para_rem,
)
from config import RESUMENES_CONFIG, RACHA_MINIMA, CANAL_RACHA_ID, ADMIN_IDS

logger = logging.getLogger(__name__)


# ==============================
# REGISTRO DE PICKS
# ==============================

def calcular_stake_pre(codigo_pre: str, tipo_pick: str) -> float:
    """
    Calcula el stake recomendado para un pick PRE en base al historial acumulado en DB.

    Modelo basado en ROI a cuota 1.70 de referencia:
      < 10 picks resueltos  → 1.0u  (sin muestra suficiente)
      ROI < -10%            → 0.0u  (en pérdidas, pausar)
      ROI  0%-10%           → 1.0u
      ROI 10%-20%, WR≥60%  → 1.5u
      ROI > 20%,  WR≥65%   → 2.0u
    """
    picks = db_picks_para_analisis(codigo=codigo_pre, tipo_pick=tipo_pick, dias=180)
    resueltos = [p for p in picks if p.get("resultado") in ("HIT", "MISS", "VOID")]

    n = len(resueltos)
    if n < 10:
        return 1.0  # muestra insuficiente → stake base

    hits   = sum(1 for p in resueltos if p.get("resultado") == "HIT")
    misses = sum(1 for p in resueltos if p.get("resultado") == "MISS")
    voids  = sum(1 for p in resueltos if p.get("resultado") == "VOID")
    base   = hits + misses + voids
    wr     = hits / base if base > 0 else 0.0
    profit = hits * 0.70 - misses * 1.0
    roi    = profit / base if base > 0 else 0.0

    if roi < -0.10:
        return 0.0
    if roi >= 0.20 and wr >= 0.65:
        return 2.0
    if roi >= 0.10 and wr >= 0.60:
        return 1.5
    return 1.0


def historial_pre_str(codigo_pre: str, tipo_pick: str) -> str:
    """
    Devuelve una cadena corta con el historial del código PRE para mostrar
    en el mensaje del recordatorio.
    Ejemplo: '8✅ 3❌ 1⚪ de 12 (66.7% | ROI +3.5%)'
    """
    picks = db_picks_para_analisis(codigo=codigo_pre, tipo_pick=tipo_pick, dias=180)
    resueltos = [p for p in picks if p.get("resultado") in ("HIT", "MISS", "VOID")]

    n = len(resueltos)
    if n == 0:
        return "Sin historial aún"

    hits   = sum(1 for p in resueltos if p.get("resultado") == "HIT")
    misses = sum(1 for p in resueltos if p.get("resultado") == "MISS")
    voids  = sum(1 for p in resueltos if p.get("resultado") == "VOID")
    base   = hits + misses + voids
    wr     = round(hits / base * 100, 1) if base > 0 else 0.0
    profit = hits * 0.70 - misses * 1.0
    roi    = round(profit / base * 100, 1) if base > 0 else 0.0
    roi_str = f"+{roi:.1f}%" if roi >= 0 else f"{roi:.1f}%"

    void_txt = f" {voids}⚪" if voids > 0 else ""
    return f"{hits}✅ {misses}❌{void_txt} de {n}  ({wr}% | ROI {roi_str})"


def propagar_resultado_rem_a_pre(
    rem_message_id,
    codigo_pre: str,
    partido_rem: str,
    tipo_pick: str,
    resultado: str,
) -> None:
    """
    Cuando un recordatorio REM recibe resultado de inplayguru, lo propaga
    al pick PRE original usando fuzzy matching de nombre de partido.

    Solo actualiza el PRE si el matching supera el umbral de 0.80 de similitud,
    para evitar actualizaciones incorrectas.
    """
    import re
    import difflib

    candidatos = db_buscar_pre_para_rem(codigo_pre, tipo_pick)
    if not candidatos:
        logger.info(
            "REM→PRE: sin picks %s pendientes para propagar resultado (msg_rem=%s).",
            codigo_pre, rem_message_id,
        )
        return

    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9 ]", "", s.lower())

    mejor_score = 0.0
    mejor_pick  = None
    for pick in candidatos:
        partido_pre = pick.get("partido") or ""
        score = difflib.SequenceMatcher(
            None, _norm(partido_rem), _norm(partido_pre)
        ).ratio()
        if score > mejor_score:
            mejor_score = score
            mejor_pick  = pick

    if mejor_score < 0.80 or not mejor_pick:
        logger.warning(
            "REM→PRE: no se encontró PRE con suficiente similitud para '%s' "
            "(mejor score=%.2f < 0.80). No se propaga resultado.",
            partido_rem, mejor_score,
        )
        return

    pre_id = mejor_pick["message_id_origen"]
    ok = db_actualizar_resultado_confirmado(pre_id, resultado)
    if ok:
        logger.info(
            "REM→PRE ✅ | msg_rem=%s → %s | PRE %s '%s' (score=%.2f)",
            rem_message_id, resultado, pre_id,
            mejor_pick.get("partido", ""), mejor_score,
        )
    else:
        logger.warning(
            "REM→PRE: no se pudo actualizar PRE %s con resultado %s",
            pre_id, resultado,
        )


def registrar_pick_estadistica(
    message_id_origen,
    datos: dict,
    tipo_pick: str,
    enviado_a_free: bool = False,
    nivel: str | None = None,
) -> None:
    # Extraer cuota para picks prepartido
    # PRE_1X  → primera cuota de 1X2 (cuota local)
    # PRE_O25FT → primera cuota de Over/Under 2.50 (cuota over 2.5)
    odds = None
    codigo = datos.get("codigo") or ""
    periodo_codigo = detectar_periodo_por_codigo(datos)
    modo_codigo = detectar_modo_por_codigo(datos)
    linea_codigo = detectar_linea_por_codigo(datos)
    strike_alerta_pct = parse_percent(datos.get("strike_alerta"))
    strike_liga_pct = parse_percent(datos.get("strike_liga"))
    minuto_alerta = datos.get("minuto")
    goles_entrada_total = parse_marcador_total(datos.get("goles"))
    corners_entrada_total = None
    red_cards_entrada_total = parse_marcador_total(datos.get("red_cards"))
    momentum_local = None
    momentum_visitante = None

    if codigo.upper().startswith("PRE_"):
        codigo_up = codigo.upper()
        odds_raw = ""
        if codigo_up == "PRE_1X":
            odds_raw = datos.get("odds_1x2") or ""
        elif "O25" in codigo_up or "OVER2.5" in codigo_up:
            odds_raw = datos.get("odds_over_2_5") or ""
        elif "O15" in codigo_up or "OVER1.5" in codigo_up:
            odds_raw = datos.get("odds_over_1_5") or ""
        elif "O05" in codigo_up or "OVER0.5" in codigo_up:
            odds_raw = datos.get("odds_over_0_5") or ""

        partes = [p.strip() for p in odds_raw.replace("|", " ").split() if p.strip()]
        if partes:
            try:
                odds = float(partes[0].replace(",", "."))
            except ValueError:
                pass

    if tipo_pick == "corner":
        corners_entrada_total = parse_marcador_total(datos.get("corners"))

    momentum_partes = parse_dupla_numerica(datos.get("momentum"))
    if momentum_partes:
        momentum_local, momentum_visitante = momentum_partes

    db_registrar_pick(
        message_id_origen = str(message_id_origen),
        codigo            = datos.get("codigo"),
        tipo_pick         = tipo_pick,
        periodo_codigo    = periodo_codigo,
        modo_codigo       = modo_codigo,
        linea_codigo      = linea_codigo,
        liga              = datos.get("liga"),
        partido           = datos.get("partido"),
        strike_alerta     = datos.get("strike_alerta"),
        strike_liga       = datos.get("strike_liga"),
        enviado_a_free    = enviado_a_free,
        strike_alerta_pct = strike_alerta_pct,
        strike_liga_pct   = strike_liga_pct,
        minuto_alerta     = minuto_alerta,
        goles_entrada_total = goles_entrada_total,
        corners_entrada_total = corners_entrada_total,
        red_cards_entrada_total = red_cards_entrada_total,
        momentum_local    = momentum_local,
        momentum_visitante = momentum_visitante,
        fecha             = hoy_str(),
        fecha_hora        = ahora_str(),
        odds              = odds,
        nivel             = nivel,
    )


def actualizar_resultado_estadistica(message_id_origen, resultado: str) -> bool:
    return db_actualizar_resultado_confirmado(str(message_id_origen), resultado)


def es_pick_corner_mas_uno(pick: dict | None) -> bool:
    if not pick or pick.get("tipo_pick") != "corner":
        return False

    modo = (pick.get("modo_codigo") or "").upper()
    linea = (pick.get("linea_codigo") or "").upper()
    return (
        ("ASIAN" in modo and "+1" in linea)
        or modo == "+1"
        or ("SINGLE" in modo and "+1" in linea)
    )


def calcular_resultado_corner_mas_uno(
    corners_entrada_total: int,
    corners_final_total: int,
) -> str:
    delta = corners_final_total - corners_entrada_total
    if delta <= 0:
        return "MISS"
    if delta == 1:
        return "VOID"
    return "HIT"


def resolver_resultado_corner_mas_uno(
    message_id_origen: str,
    corners_final_total: int,
) -> tuple[bool, str]:
    pick = db_pick_por_message_id(str(message_id_origen))
    if not pick:
        return False, "No encontré ese pick en la base de datos."

    if not es_pick_corner_mas_uno(pick):
        return False, "Ese pick no es un córner +1 compatible con este comando."

    corners_entrada_total = pick.get("corners_entrada_total")
    if corners_entrada_total is None:
        return False, "Ese pick no tiene guardado el total de córners de entrada."

    if corners_final_total < corners_entrada_total:
        return (
            False,
            f"El total final ({corners_final_total}) no puede ser menor que el de entrada ({corners_entrada_total}).",
        )

    resultado = calcular_resultado_corner_mas_uno(
        corners_entrada_total=corners_entrada_total,
        corners_final_total=corners_final_total,
    )
    actualizado = actualizar_resultado_estadistica(message_id_origen, resultado)
    if not actualizado:
        return False, "No pude actualizar el resultado en la base de datos."

    periodo = (pick.get("periodo_codigo") or "FT").upper()
    extras = corners_final_total - corners_entrada_total
    return (
        True,
        f"Pick {message_id_origen} actualizado a {resultado}. "
        f"Entrada: {corners_entrada_total} córners | Final {periodo}: {corners_final_total} | "
        f"Córners tras la entrada: {extras}.",
    )

async def auto_resolver_pick_corner_mas_uno(pick: dict) -> tuple[bool, str]:
    if not es_pick_corner_mas_uno(pick):
        return False, "No es un pick de corners +1 compatible."

    if pick.get("corners_entrada_total") is None:
        return False, "No tiene guardado el total de corners de entrada."

    datos_espn = await resolver_corner_pick_espn(pick)
    if not datos_espn:
        return False, "No se pudo localizar el partido con suficiente confianza en ESPN."

    ok, mensaje = resolver_resultado_corner_mas_uno(
        str(pick.get("message_id_origen")),
        int(datos_espn["corners_final_total"]),
    )
    if not ok:
        return False, mensaje

    return (
        True,
        f"{mensaje} Fuente: ESPN ({datos_espn['partido_espn']}, event {datos_espn['event_id']}).",
    )


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

_DIAS_ES = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
_MESES_ES_LARGO = {
    1: "enero", 2: "febrero", 3: "marzo", 4: "abril",
    5: "mayo", 6: "junio", 7: "julio", 8: "agosto",
    9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
}

# Cuota de referencia fija para calcular profit/ROI en picks live
_CUOTA_LIVE = 1.70


def _profit_live(subset: list) -> tuple[float, float, int]:
    """
    Calcula profit y ROI a cuota fija _CUOTA_LIVE para una lista de picks live.
    Los picks prepartido (código PRE_*) se excluyen — tienen cuota real propia.

    Reglas:
      HIT  → +(_CUOTA_LIVE - 1) unidades  (ej: +0.70u a 1.70)
      MISS → -1.00u
      VOID → 0u (stake devuelto, pero cuenta como pick jugado en el denominador)
      Pendiente → no se incluye

    Devuelve (profit_units, roi_pct, picks_contabilizados).
    """
    profit = 0.0
    staked = 0
    for x in subset:
        if (x.get("codigo") or "").upper().startswith("PRE"):
            continue
        r = x.get("resultado")
        if r == "HIT":
            profit += _CUOTA_LIVE - 1.0
            staked += 1
        elif r == "MISS":
            profit -= 1.0
            staked += 1
        elif r == "VOID":
            staked += 1   # stake devuelto → profit 0, pero pick jugado
    roi = round(profit / staked * 100, 1) if staked > 0 else 0.0
    return round(profit, 2), roi, staked


def _fecha_legible() -> str:
    ahora = ahora_madrid()
    dia_semana = _DIAS_ES[ahora.weekday()]
    mes = _MESES_ES_LARGO[ahora.month]
    return f"{dia_semana} {ahora.day} de {mes}"


def _mensaje_motivacional(strike: float, resueltos: int) -> str:
    if resueltos == 0:
        return "📋 Sin picks resueltos aún."
    if strike >= 80:
        return "🔥 ¡Día espectacular! Rendimiento elite."
    if strike >= 70:
        return "💪 ¡Gran jornada! Por encima del 70%."
    if strike >= 60:
        return "📈 Buen rendimiento. El sistema funciona."
    if strike >= 50:
        return "⚖️ Día ajustado. El largo plazo manda."
    return "📉 Día difícil. La tendencia sigue siendo positiva."


def _fmt_profit(profit: float, roi: float) -> str:
    """Formatea profit y ROI con signo y emoji de tendencia."""
    p_str   = f"+{profit:.2f}u" if profit >= 0 else f"{profit:.2f}u"
    r_str   = f"+{roi:.1f}%" if roi >= 0 else f"{roi:.1f}%"
    emoji   = "📈" if profit >= 0 else "📉"
    return f"{emoji} {p_str}  ·  ROI {r_str}"


def construir_resumen(lista: list, titulo: str) -> str:
    total      = len(lista)
    hits       = sum(1 for x in lista if x.get("resultado") == "HIT")
    miss       = sum(1 for x in lista if x.get("resultado") == "MISS")
    voids      = sum(1 for x in lista if x.get("resultado") == "VOID")
    pendientes = total - hits - miss - voids

    # Strike: HIT / (HIT + MISS + VOID) — nulos cuentan como pick jugado
    base_strike = hits + miss + voids
    strike      = round((hits / base_strike) * 100, 1) if base_strike > 0 else 0.0

    goles   = [x for x in lista if x.get("tipo_pick") == "gol"]
    corners = [x for x in lista if x.get("tipo_pick") == "corner"]

    def stats(subset):
        h = sum(1 for x in subset if x.get("resultado") == "HIT")
        m = sum(1 for x in subset if x.get("resultado") == "MISS")
        v = sum(1 for x in subset if x.get("resultado") == "VOID")
        return h, m, v

    gh, gm, gv = stats(goles)
    ch, cm, cv = stats(corners)

    # Profit a cuota fija 1.70 (solo picks live, excluye PRE_*)
    profit_tot, roi_tot, staked_tot = _profit_live(lista)
    gp, gr, gs = _profit_live(goles)
    cp, cr, cs = _profit_live(corners)

    strike_txt     = f"<b>{strike}%</b>" if base_strike > 0 else "<b>—</b>"
    pendientes_txt = f"  ·  ⏳ <i>{pendientes} pend.</i>" if pendientes > 0 else ""
    motivo         = _mensaje_motivacional(strike, base_strike)

    lineas = [
        f"<b>{titulo}</b>",
        f"📅 {_fecha_legible()}",
        "──────────────────",
        "",
        f"🎯 Strike: {strike_txt}",
        f"✅ <b>{hits}</b> Hits  ·  ❌ <b>{miss}</b> Miss  ·  ⚪ <b>{voids}</b> Nulos{pendientes_txt}",
        f"📦 {total} picks en total",
    ]

    if goles and corners:
        lineas.append("")
        g_profit_txt = f"  <i>{_fmt_profit(gp, gr)}</i>" if gs > 0 else ""
        c_profit_txt = f"  <i>{_fmt_profit(cp, cr)}</i>" if cs > 0 else ""
        lineas.append(f"⚽ Goles:   ✅ {gh}  ❌ {gm}  ⚪ {gv}  <i>({len(goles)} picks)</i>{g_profit_txt}")
        lineas.append(f"🚩 Córners: ✅ {ch}  ❌ {cm}  ⚪ {cv}  <i>({len(corners)} picks)</i>{c_profit_txt}")
    elif goles:
        lineas.append(f"\n⚽ Goles:   ✅ {gh}  ❌ {gm}  ⚪ {gv}")
    elif corners:
        lineas.append(f"\n🚩 Córners: ✅ {ch}  ❌ {cm}  ⚪ {cv}")

    if staked_tot > 0:
        lineas.append("")
        lineas.append(f"💰 Profit @{_CUOTA_LIVE}: <b>{_fmt_profit(profit_tot, roi_tot)}</b>  <i>({staked_tot} picks)</i>")

    lineas.append("")
    lineas.append(motivo)

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
        voids_m    = sum(1 for x in picks_mes if x.get("resultado") == "VOID")
        base_m     = hits_m + misses_m + voids_m
        strike_m   = round((hits_m / base_m) * 100, 1) if base_m > 0 else 0
        nombre_mes = meses_es.get(mes_key[5:], mes_key[5:])
        profit_m, roi_m, staked_m = _profit_live(picks_mes)
        profit_txt = f" | {_fmt_profit(profit_m, roi_m)}" if staked_m > 0 else ""
        lineas_mes.append(
            f"  {nombre_mes}: {len(picks_mes)} picks | {strike_m}% strike{profit_txt}"
        )

    return texto_base + "\n".join(lineas_mes)


# ==============================
# CLAVES Y CONDICIONES DE PUBLICACIÓN
# ==============================

def _clave_periodo(periodo: str) -> str:
    ahora = ahora_madrid()
    if periodo == "dia":
        return ahora.strftime("%Y-%m-%d")
    if periodo == "semana":
        year, week, _ = ahora.isocalendar()
        return f"{year}-W{week}"
    if periodo == "mes":
        return ahora.strftime("%Y-%m")
    return ""



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

_PERIODO_DB_AUTO = {
    "dia":    "dia",
    "semana": "semana_anterior",   # el job semanal corre el lunes: muestra la semana que acaba de cerrar
    "mes":    "mes_anterior",
    "anio":   "anio",
}


def _titulo_resumen(periodo: str, tipo_pick, label: str) -> str:
    return _TITULOS.get((periodo, tipo_pick), f"RESUMEN — {label}")


# ==============================
# PUBLICACIÓN AUTOMÁTICA
# ==============================

async def publicar_resumenes_si_toca(context, periodo: str) -> None:
    # ── Compuerta horaria ────────────────────────────────────────────
    # Evita publicar resúmenes parciales si el job se dispara fuera de hora.
    ahora_local = ahora_madrid()
    if periodo == "dia" and ahora_local.hour < 23:
        logger.info("Resumen diario: hora %dh < 23h, se pospone.", ahora_local.hour)
        return
    if periodo == "semana" and (ahora_local.weekday() != 0 or ahora_local.hour < 23):
        logger.info(
            "Resumen semanal: no es lunes ≥ 23h (weekday=%d, hora=%dh), se pospone.",
            ahora_local.weekday(), ahora_local.hour,
        )
        return
    if periodo == "mes" and (ahora_local.day != 1 or ahora_local.hour < 9):
        logger.info(
            "Resumen mensual: no es día 1 ≥ 9h (día=%d, hora=%dh), se pospone.",
            ahora_local.day, ahora_local.hour,
        )
        return
    # ────────────────────────────────────────────────────────────────

    clave_valor = _clave_periodo(periodo)
    periodo_db  = _PERIODO_DB_AUTO.get(periodo, periodo)
    lista_base  = db_picks_por_periodo(periodo_db)
    logger.info(
        "Resumen %s: compuerta abierta — %d picks encontrados en DB (periodo_db=%s).",
        periodo, len(lista_base), periodo_db,
    )

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
            continue

        titulo = _titulo_resumen(periodo, tipo_pick, label)
        texto  = construir_resumen(lista, titulo)

        try:
            await context.bot.send_message(chat_id=canal_id, text=texto, parse_mode="HTML")
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


def _debe_notificar_pendientes_ahora() -> bool:
    ahora = ahora_madrid()
    return (ahora.hour, ahora.minute) >= (8, 0)


def _partir_mensajes(texto_base: str, lineas: list[str], max_len: int = 3900) -> list[str]:
    mensajes = []
    actual = texto_base

    for linea in lineas:
        candidata = f"{actual}\n{linea}" if actual else linea
        if len(candidata) > max_len and actual:
            mensajes.append(actual)
            actual = f"{texto_base}\n{linea}"
        else:
            actual = candidata

    if actual:
        mensajes.append(actual)

    return mensajes


async def notificar_picks_pendientes_si_toca(context) -> None:
    if not _debe_notificar_pendientes_ahora():
        return

    clave_valor = _clave_periodo("dia")
    clave_control = "revision_pendientes_dia"

    if db_ya_publicado(clave_control, clave_valor):
        return

    pendientes = [
        pick for pick in db_picks_por_periodo("ayer")
        if not pick.get("resultado")
    ]

    auto_resueltos: list[str] = []
    pendientes_finales: list[dict] = []

    for pick in pendientes:
        if es_pick_corner_mas_uno(pick):
            ok, mensaje = await auto_resolver_pick_corner_mas_uno(pick)
            if ok:
                auto_resueltos.append(mensaje)
                continue
        pendientes_finales.append(pick)

    pendientes = pendientes_finales

    if not pendientes and not auto_resueltos:
        db_marcar_publicado(clave_control, clave_valor)
        logger.info("Revision de pendientes: no hay picks sin resultado del dia anterior.")
        return

    mensajes: list[str] = []

    if auto_resueltos:
        encabezado_auto = (
            f"Revision automatica corners +1 - {clave_valor}\n\n"
            f"Se han resuelto automaticamente {len(auto_resueltos)} pick(s) usando ESPN:\n"
        )
        mensajes.extend(_partir_mensajes(encabezado_auto, [f"- {linea}" for linea in auto_resueltos]))

    encabezado = (
        f"Revision manual pendiente - {clave_valor}\n\n"
        f"Hay {len(pendientes)} pick(s) de ayer sin resultado final en la base de datos.\n"
        "Verificalos manualmente con /resultado <message_id> HIT|MISS|VOID.\n"
        "Para corners +1 puedes usar /resultado_corner <message_id> <corners_finales_del_periodo>.\n"
    )

    lineas = []
    for pick in pendientes:
        tipo = (pick.get("tipo_pick") or "?").upper()
        codigo = pick.get("codigo") or "-"
        liga = pick.get("liga") or "-"
        partido = pick.get("partido") or "-"
        msg_id = pick.get("message_id_origen") or "-"
        if es_pick_corner_mas_uno(pick) and pick.get("corners_entrada_total") is not None:
            periodo = (pick.get("periodo_codigo") or "FT").upper()
            entrada = pick.get("corners_entrada_total")
            lineas.append(
                f"- {msg_id} | {tipo} | {codigo} | {liga} | {partido} | entrada={entrada} | "
                f"usa: /resultado_corner {msg_id} <corners_finales_{periodo}>"
            )
        else:
            lineas.append(f"- {msg_id} | {tipo} | {codigo} | {liga} | {partido}")

    if pendientes:
        mensajes.extend(_partir_mensajes(encabezado, lineas))

    enviado_ok = False
    for admin_id in ADMIN_IDS:
        try:
            for mensaje in mensajes:
                await context.bot.send_message(chat_id=admin_id, text=mensaje)
            enviado_ok = True
        except Exception as e:
            logger.error(f"Error enviando revision de pendientes al admin {admin_id}: {e}")

    if enviado_ok:
        db_marcar_publicado(clave_control, clave_valor)


async def notificar_picks_pendientes_retrasados(context) -> None:
    """
    Recordatorio periodico para picks que siguen pendientes demasiadas horas o dias.
    No sustituye al aviso diario de ayer: lo complementa para que no se pierdan.
    """
    ahora = ahora_madrid().strftime("%Y-%m-%d %H")
    clave_control = "revision_pendientes_retrasados"

    if db_ya_publicado(clave_control, ahora):
        return

    pendientes = db_picks_pendientes_revision(max_dias=7, min_horas=6)
    if not pendientes:
        db_marcar_publicado(clave_control, ahora)
        return

    encabezado = (
        "Recordatorio de pendientes retrasados\n\n"
        f"Hay {len(pendientes)} pick(s) sin resultado desde hace al menos 6 horas.\n"
        "Revisa si el canal origen ya fue editado o corrigelos manualmente con "
        "/resultado <message_id> HIT|MISS|VOID.\n"
        "Para corners +1 puedes usar /resultado_corner <message_id> <corners_finales_del_periodo>.\n"
    )

    lineas: list[str] = []
    for pick in pendientes:
        tipo = (pick.get("tipo_pick") or "?").upper()
        codigo = pick.get("codigo") or "-"
        liga = pick.get("liga") or "-"
        partido = pick.get("partido") or "-"
        msg_id = pick.get("message_id_origen") or "-"
        horas = pick.get("horas_pendiente")
        etiqueta_horas = f"{horas}h" if horas is not None else "?"
        linea = f"- {msg_id} | {tipo} | {codigo} | {liga} | {partido} | {etiqueta_horas}"
        if es_pick_corner_mas_uno(pick) and pick.get("corners_entrada_total") is not None:
            periodo = (pick.get("periodo_codigo") or "FT").upper()
            entrada = pick.get("corners_entrada_total")
            linea += f" | Entrada corners {periodo}: {entrada}"
        lineas.append(linea)

    mensajes = _partir_mensajes(encabezado, lineas)

    enviado_ok = False
    for admin_id in ADMIN_IDS:
        try:
            for mensaje in mensajes:
                await context.bot.send_message(chat_id=admin_id, text=mensaje)
            enviado_ok = True
        except Exception as e:
            logger.error(
                f"Error enviando recordatorio de pendientes retrasados al admin {admin_id}: {e}"
            )

    if enviado_ok:
        db_marcar_publicado(clave_control, ahora)


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
# ANALISIS DE FILTROS
# ==============================

def _bucket_strike(valor: int | None, label: str) -> str | None:
    if valor is None:
        return None
    if valor < 65:
        return f"{label} <65"
    if valor < 70:
        return f"{label} 65-69"
    if valor < 75:
        return f"{label} 70-74"
    return f"{label} 75+"


def _bucket_minuto(valor: int | None) -> str | None:
    if valor is None:
        return None
    if valor <= 20:
        return "Minuto 0-20"
    if valor <= 30:
        return "Minuto 21-30"
    if valor <= 40:
        return "Minuto 31-40"
    if valor <= 50:
        return "Minuto 41-50"
    if valor <= 60:
        return "Minuto 51-60"
    return "Minuto 61+"


def _bucket_goles(valor: int | None) -> str | None:
    if valor is None:
        return None
    if valor <= 0:
        return "Marcador entrada 0 goles"
    if valor == 1:
        return "Marcador entrada 1 gol"
    if valor == 2:
        return "Marcador entrada 2 goles"
    return "Marcador entrada 3+ goles"


def _bucket_rojas(valor: int | None) -> str | None:
    if valor is None:
        return None
    if valor == 0:
        return "Sin rojas"
    if valor == 1:
        return "1 roja total"
    return "2+ rojas totales"


def _bucket_momentum(local: int | None, visitante: int | None) -> str | None:
    if local is None or visitante is None:
        return None
    diff = local - visitante
    if diff >= 20:
        return "Momentum local +20"
    if diff >= 8:
        return "Momentum local +8"
    if diff <= -20:
        return "Momentum rival +20"
    if diff <= -8:
        return "Momentum rival +8"
    return "Momentum equilibrado"


def _bucket_odds(valor: float | None) -> str | None:
    if valor is None:
        return None
    if valor < 1.70:
        return "Cuota <1.70"
    if valor < 1.85:
        return "Cuota 1.70-1.84"
    if valor < 2.00:
        return "Cuota 1.85-1.99"
    if valor <= 2.60:
        return "Cuota 2.00-2.60"
    return "Cuota >2.60"


def _candidate_dimensions(row: dict) -> dict[str, str]:
    dimensiones: dict[str, str] = {}
    if row.get("codigo"):
        dimensiones["codigo"] = f"Código {row['codigo']}"
    if row.get("liga"):
        dimensiones["liga"] = f"Liga {row['liga']}"
    if row.get("periodo_codigo"):
        dimensiones["periodo"] = f"Periodo {row['periodo_codigo']}"
    if row.get("modo_codigo"):
        dimensiones["modo"] = f"Modo {row['modo_codigo']}"
    if row.get("linea_codigo"):
        dimensiones["linea"] = f"Línea {row['linea_codigo']}"

    for key, label in (
        ("strike_alerta_pct", _bucket_strike(row.get("strike_alerta_pct"), "Strike alerta")),
        ("strike_liga_pct", _bucket_strike(row.get("strike_liga_pct"), "Strike liga")),
        ("minuto_alerta", _bucket_minuto(row.get("minuto_alerta"))),
        ("goles_entrada_total", _bucket_goles(row.get("goles_entrada_total"))),
        ("red_cards_entrada_total", _bucket_rojas(row.get("red_cards_entrada_total"))),
        ("odds", _bucket_odds(float(row["odds"]) if row.get("odds") is not None else None)),
    ):
        if label:
            dimensiones[key] = label

    momentum_label = _bucket_momentum(row.get("momentum_local"), row.get("momentum_visitante"))
    if momentum_label:
        dimensiones["momentum"] = momentum_label

    return dimensiones


def _analizar_patrones(rows: list[dict], min_muestra: int) -> tuple[float, list[dict], list[dict]]:
    total = len(rows)
    hits = sum(1 for row in rows if row.get("resultado") == "HIT")
    base_strike = round(hits / total * 100, 1) if total > 0 else 0.0

    acumulado: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"hits": 0, "total": 0})
    for row in rows:
        for dimension, etiqueta in _candidate_dimensions(row).items():
            key = (dimension, etiqueta)
            acumulado[key]["total"] += 1
            if row.get("resultado") == "HIT":
                acumulado[key]["hits"] += 1

    patrones = []
    for (dimension, etiqueta), datos in acumulado.items():
        total_patron = datos["total"]
        if total_patron < min_muestra:
            continue
        strike = round(datos["hits"] / total_patron * 100, 1) if total_patron > 0 else 0.0
        delta = round(strike - base_strike, 1)
        if abs(delta) < 8:
            continue
        patrones.append(
            {
                "dimension": dimension,
                "etiqueta": etiqueta,
                "total": total_patron,
                "hits": datos["hits"],
                "strike": strike,
                "delta": delta,
            }
        )

    mejores = sorted(
        [p for p in patrones if p["delta"] > 0],
        key=lambda p: (p["delta"], p["total"]),
        reverse=True,
    )[:6]
    peores = sorted(
        [p for p in patrones if p["delta"] < 0],
        key=lambda p: (p["delta"], -p["total"]),
    )[:6]
    return base_strike, mejores, peores


async def enviar_analisis_filtros_comando(update, filtro: str | None = None) -> None:
    """
    Analiza el histórico resuelto y sugiere patrones candidatos a filtro.
    Uso:
      /analisis_filtros
      /analisis_filtros CM01
      /analisis_filtros gol
    """
    filtro_norm = (filtro or "").strip()
    tipo_pick = filtro_norm.lower() if filtro_norm.lower() in ("gol", "corner") else None
    codigo = filtro_norm.upper() if filtro_norm and not tipo_pick else None

    rows = db_picks_para_analisis(codigo=codigo, tipo_pick=tipo_pick, dias=180)
    if not rows:
        await update.message.reply_text("No hay picks resueltos suficientes para analizar filtros.")
        return

    min_muestra = 6 if (codigo or tipo_pick) else 10
    base_strike, mejores, peores = _analizar_patrones(rows, min_muestra=min_muestra)

    alcance = codigo or (tipo_pick.upper() if tipo_pick else "GLOBAL")
    lineas = [
        f"📊 ANÁLISIS DE FILTROS — {alcance}",
        "",
        f"Muestra analizada: {len(rows)} picks resueltos (últimos 180 días).",
        f"Strike base: {base_strike}%",
        f"Mínimo por patrón: {min_muestra} picks.",
        "",
    ]

    if mejores:
        lineas.append("🟢 Patrones que mejoran el strike:")
        for item in mejores:
            lineas.append(
                f"- {item['etiqueta']} → {item['strike']}% "
                f"({item['hits']}/{item['total']}, {item['delta']:+} pts)"
            )
        lineas.append("")

    if peores:
        lineas.append("🔴 Patrones que empeoran el strike:")
        for item in peores:
            lineas.append(
                f"- {item['etiqueta']} → {item['strike']}% "
                f"({item['hits']}/{item['total']}, {item['delta']:+} pts)"
            )
        lineas.append("")

    if not mejores and not peores:
        lineas.append(
            "No han salido patrones suficientemente claros todavía. "
            "Hace falta más muestra o más contexto guardado."
        )
    else:
        lineas.append(
            "Úsalo como guía, no como filtro automático inmediato: conviene validar los patrones con muestra adicional."
        )

    await update.message.reply_text("\n".join(lineas))


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
            else f"📊 <b>{titulo}</b>\n\nSin picks para {nombre}."
        )
        await update.message.reply_text(texto, parse_mode="HTML")


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
    await update.message.reply_text(texto, parse_mode="HTML")


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
    await update.message.reply_text(texto, parse_mode="HTML")


async def enviar_resumen_prepartido_comando(update) -> None:
    """
    Resumen de picks prepartido con desglose mes a mes por estrategia.
    Incluye profit en unidades y ROI.
    Uso: /resumen_pre
    """
    _NOMBRES = {
        "PRE_O25FT": "Over 2.5 FT",
        "PRE_1X":    "Ganador Local",
        "PRE_1XHT":  "Ganador Local 1ª Mitad",
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
    await update.message.reply_text(texto, parse_mode="HTML")
