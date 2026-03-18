import logging
from datetime import timedelta

from utils import hoy_str, ahora_str, ahora_madrid, semana_str
from state import STATE, save_state
from config import RESUMENES_CONFIG

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
    registro = {
        "message_id_origen": str(message_id_origen),
        "codigo":        datos.get("codigo"),
        "tipo_pick":     tipo_pick,
        "liga":          datos.get("liga"),
        "partido":       datos.get("partido"),
        "strike_alerta": datos.get("strike_alerta"),
        "strike_liga":   datos.get("strike_liga"),
        "resultado":     None,
        "fecha":         hoy_str(),
        "fecha_hora":    ahora_str(),
        "enviado_a_free": enviado_a_free,   # ← nuevo campo para filtrar resumen FREE
    }
    STATE["estadisticas"].append(registro)
    logger.debug(f"Pick registrado: {message_id_origen} | {tipo_pick} | free={enviado_a_free}")


def actualizar_resultado_estadistica(message_id_origen, resultado: str) -> None:
    for item in STATE["estadisticas"]:
        if str(item.get("message_id_origen")) == str(message_id_origen):
            item["resultado"] = resultado
            logger.debug(f"Resultado actualizado: {message_id_origen} → {resultado}")
            break


# ==============================
# FILTROS POR PERÍODO
# ==============================

def filtrar_por_periodo(periodo: str) -> list:
    """
    Devuelve la lista de picks del período indicado.
    periodo: "dia" | "semana" | "mes"
    """
    ahora = ahora_madrid()

    if periodo == "dia":
        clave = hoy_str()
        return [x for x in STATE["estadisticas"] if x.get("fecha") == clave]

    if periodo == "semana":
        inicio = ahora - timedelta(days=ahora.weekday())
        inicio_str = inicio.strftime("%Y-%m-%d")
        return [x for x in STATE["estadisticas"] if x.get("fecha", "") >= inicio_str]

    if periodo == "mes":
        # Mes anterior completo
        primer_dia_mes_actual = ahora.replace(day=1)
        ultimo_mes = primer_dia_mes_actual - timedelta(days=1)
        inicio_str = ultimo_mes.strftime("%Y-%m-01")
        fin_str    = ultimo_mes.strftime("%Y-%m-%d")
        return [
            x for x in STATE["estadisticas"]
            if inicio_str <= x.get("fecha", "") <= fin_str
        ]

    return []


def filtrar_por_tipo(lista: list, tipo_pick) -> list:
    """
    Filtra una lista de picks por tipo.
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

    # Solo añadir desglose si hay picks de ambos tipos o es un resumen combinado
    if goles:
        lineas.append(f"\n⚽ Goles: {len(goles)} | ✅ {gh} | ❌ {gm} | ⚪ {gv}")
    if corners:
        lineas.append(f"🚩 Corners: {len(corners)} | ✅ {ch} | ❌ {cm} | ⚪ {cv}")

    return "\n".join(lineas)


# ==============================
# CLAVE DE CONTROL POR PERÍODO
# ==============================

def _clave_periodo(periodo: str) -> str:
    """Genera una clave única para el período actual, usada para evitar duplicados."""
    ahora = ahora_madrid()
    if periodo == "dia":
        return hoy_str()
    if periodo == "semana":
        return semana_str()
    if periodo == "mes":
        # Se publica el primer día del mes siguiente → usamos el mes anterior como clave
        primer_dia = ahora.replace(day=1)
        mes_anterior = primer_dia - timedelta(days=1)
        return mes_anterior.strftime("%Y-%m")
    return ""


def _ya_publicado(resumen_id: str, periodo: str) -> bool:
    control = STATE.get("resumen_control", {})
    key = f"{resumen_id}_{periodo}"
    return control.get(key) == _clave_periodo(periodo)


def _marcar_publicado(resumen_id: str, periodo: str) -> None:
    if "resumen_control" not in STATE:
        STATE["resumen_control"] = {}
    key = f"{resumen_id}_{periodo}"
    STATE["resumen_control"][key] = _clave_periodo(periodo)


# ==============================
# CONDICIONES DE PUBLICACIÓN
# ==============================

def _debe_publicar_ahora(periodo: str) -> bool:
    """Comprueba si el momento actual cumple las condiciones de publicación."""
    ahora = ahora_madrid()

    if periodo == "dia":
        return ahora.hour >= 22

    if periodo == "semana":
        # Domingos a las 22:00
        return ahora.weekday() == 6 and ahora.hour >= 22

    if periodo == "mes":
        # Primer día del mes siguiente a las 10:00
        return ahora.day == 1 and ahora.hour >= 10

    return False


# ==============================
# TÍTULOS DE RESUMEN
# ==============================

_TITULOS = {
    ("dia",    "gol"):     "RESUMEN DEL DÍA — GOLES",
    ("dia",    "corner"):  "RESUMEN DEL DÍA — CORNERS",
    ("dia",    None):      "RESUMEN DEL DÍA — GENERAL",
    ("dia",    "free"):    "RESUMEN DEL DÍA — CANAL FREE",
    ("semana", "gol"):     "RESUMEN SEMANAL — GOLES",
    ("semana", "corner"):  "RESUMEN SEMANAL — CORNERS",
    ("semana", None):      "RESUMEN SEMANAL — GENERAL",
    ("semana", "free"):    "RESUMEN SEMANAL — CANAL FREE",
    ("mes",    "gol"):     "RESUMEN MENSUAL — GOLES",
    ("mes",    "corner"):  "RESUMEN MENSUAL — CORNERS",
    ("mes",    None):      "RESUMEN MENSUAL — GENERAL",
    ("mes",    "free"):    "RESUMEN MENSUAL — CANAL FREE",
}


def _titulo_resumen(periodo: str, tipo_pick, label: str) -> str:
    base = _TITULOS.get((periodo, tipo_pick))
    if base:
        return base
    return f"RESUMEN — {label}"


# ==============================
# PUBLICACIÓN AUTOMÁTICA
# ==============================

async def publicar_resumenes_si_toca(context, periodo: str) -> None:
    """
    Publica todos los resúmenes configurados para el período dado,
    en sus canales correspondientes, si toca y no se han publicado ya.
    """
    if not _debe_publicar_ahora(periodo):
        return

    lista_base = filtrar_por_periodo(periodo)

    for cfg in RESUMENES_CONFIG:
        resumen_id = cfg["id"]
        canal_id   = cfg["canal_id"]
        tipo_pick  = cfg["tipo_pick"]
        label      = cfg["label"]

        if _ya_publicado(resumen_id, periodo):
            continue

        lista = filtrar_por_tipo(lista_base, tipo_pick)
        if not lista:
            logger.info(f"Resumen {resumen_id}/{periodo}: sin picks, se omite.")
            _marcar_publicado(resumen_id, periodo)
            continue

        titulo = _titulo_resumen(periodo, tipo_pick, label)
        texto  = construir_resumen(lista, titulo)

        try:
            await context.bot.send_message(chat_id=canal_id, text=texto)
            _marcar_publicado(resumen_id, periodo)
            save_state()
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
# RESÚMENES BAJO DEMANDA (comandos)
# ==============================

async def enviar_resumenes_comando(update, periodo: str) -> None:
    """
    Responde a un comando /resumen_hoy, /resumen_semana o /resumen_mes
    enviando un mensaje por cada canal configurado.
    """
    lista_base = filtrar_por_periodo(periodo)

    nombres_periodo = {"dia": "hoy", "semana": "esta semana", "mes": "el mes pasado"}
    nombre = nombres_periodo.get(periodo, periodo)

    if not lista_base:
        await update.message.reply_text(f"No hay picks registrados para {nombre}.")
        return

    for cfg in RESUMENES_CONFIG:
        lista   = filtrar_por_tipo(lista_base, cfg["tipo_pick"])
        titulo  = _titulo_resumen(periodo, cfg["tipo_pick"], cfg["label"])
        texto   = construir_resumen(lista, titulo) if lista else f"📊 {titulo}\n\nSin picks para {nombre}."
        await update.message.reply_text(texto)
