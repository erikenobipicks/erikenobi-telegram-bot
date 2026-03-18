import html
import logging

from utils import parse_marcador_total, normalizar_codigo
from extractor import (
    detectar_periodo_por_codigo,
    detectar_fase_por_codigo,
    detectar_modo_por_codigo,
    detectar_linea_por_codigo,
    detectar_historial_por_codigo,
)

logger = logging.getLogger(__name__)


# ==============================
# TABLAS DE TÍTULOS Y SUBTÍTULOS
# ==============================

TITULOS_BONITOS = {
    # GOLES
    "UGM":           "UN GOL MÁS",
    "LJ2":           "OVER 0.5 GOL EN LA 1ª MITAD",
    "LIVEJ1":        "ASIÁTICA 0.5/1 GOL EN LA 1ª MITAD",
    "A1.1":          "OVER 0.5 GOL EN LA 1ª MITAD",
    "C1.1":          "OVER 0.5 GOL EN LA 1ª MITAD",
    "A0.1":          "OVER 0.5 GOL EN LA 1ª MITAD",
    "A3.0":          "BUSCAMOS 1 GOL",
    "HT2":           "GOL ANTES DEL 75'",
    "L5.2":          "GOL ANTES DEL 75'",
    "QB":            "ASIÁTICA +1 GOL PARTIDO",
    "CM01":          "ASIÁTICA +1 GOL PARTIDO",
    "CM01V2":        "ASIÁTICA +1 GOL PARTIDO",
    "CM01V3":        "ASIÁTICA +1 GOL PARTIDO",
    "CM01V4":        "ASIÁTICA +1 GOL PARTIDO",
    "CM02V2":        "ASIÁTICA +1 GOL PARTIDO",
    "CM02V4":        "ASIÁTICA +1 GOL PARTIDO",
    "CM06V3":        "ASIÁTICA +1 GOL PARTIDO",
    "CM07V2":        "ASIÁTICA +1 GOL PARTIDO",
    "GFT1":          "ASIÁTICA 0.5/1 GOL PARTIDO",
    "GFT2":          "ASIÁTICA 0.5/1 GOL PARTIDO",
    "PREO25FT12H7":  "OVER 2.5 GOLES PREPARTIDO",
    # CORNERS
    "CF1":   "CÓRNER PARTIDO",
    "CF2":   "CÓRNER PARTIDO",
    "CF3":   "CÓRNER PARTIDO",
    "CF4":   "CÓRNER PARTIDO",
    "CF4V2": "CÓRNER PARTIDO",
    "CH3":   "CÓRNER EN LA 1ª MITAD",
    "CH4":   "CÓRNER EN LA 1ª MITAD",
    "CH5":   "CÓRNER EN LA 1ª MITAD",
}

SUBTITULOS_BONITOS = {
    "UGM":    "🎯 Buscar un gol más",
    "LJ2":    "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "LIVEJ1": "🎯 Entrada asiática 0.5/1 en la 1ª mitad",
    "A1.1":   "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "C1.1":   "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "A0.1":   "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "A3.0":   "🎯 Buscar 1 gol en el partido",
    "HT2":    "🎯 Buscar gol antes del 75' con línea asiática +1",
    "L5.2":   "🎯 Buscar 1 gol en la 2ª mitad antes del 75'",
    "QB":     "🎯 Entrada asiática +1 en el partido",
    "CM01":   "🎯 Entrada asiática +1 en el partido",
    "CM01V2": "🎯 Entrada asiática +1 en el partido",
    "CM01V3": "🎯 Entrada asiática +1 en el partido",
    "CM01V4": "🎯 Entrada asiática +1 en el partido",
    "CM02V2": "🎯 Entrada asiática +1 en el partido",
    "CM02V4": "🎯 Entrada asiática +1 en el partido",
    "CM06V3": "🎯 Entrada asiática +1 en el partido",
    "CM07V2": "🎯 Entrada asiática +1 en el partido",
    "GFT1":   "🎯 Entrada asiática 0.5/1 en el partido",
    "GFT2":   "🎯 Entrada asiática 0.5/1 en el partido",
    "PRE_O25FT": "🎯 Buscar over 2.5 goles prepartido",
    "CF1":   "🎯 Buscar 1 córner más en el partido",
    "CF2":   "🎯 Buscar 1 córner más en el partido",
    "CF3":   "🎯 Buscar 1 córner más en el partido",
    "CF4":   "🎯 Buscar 1 córner más en el partido",
    "CF4V2": "🎯 Buscar 1 córner más en el partido",
    "CH3":   "🎯 Buscar 1 córner más en la 1ª mitad",
    "CH4":   "🎯 Buscar 1 córner más en la 1ª mitad",
    "CH5":   "🎯 Buscar 1 córner más en la 1ª mitad",
}


# ==============================
# CÁLCULO DE ENTRADA SUGERIDA
# ==============================

def calcular_entrada_sugerida(datos: dict, tipo_pick: str) -> str | None:
    periodo  = detectar_periodo_por_codigo(datos)
    fase     = detectar_fase_por_codigo(datos)
    modo     = detectar_modo_por_codigo(datos)
    linea    = detectar_linea_por_codigo(datos)

    def sufijo_periodo() -> str:
        if periodo == "HT": return "HT"
        if periodo == "FT": return "FT"
        return ""

    if tipo_pick == "corner":
        total = parse_marcador_total(datos.get("corners"))
        if total is None:
            return None
        sf = sufijo_periodo()
        if modo in ("ASIAN+1", "SINGLE") and linea == "+1" or linea == "+1":
            return f"🎯 Entrada sugerida: línea {total + 1} córners {sf}".strip()

    if tipo_pick == "gol":
        total = parse_marcador_total(datos.get("goles"))
        if total is None:
            return None
        sf = sufijo_periodo() or ("2ª parte" if periodo == "2H" else "")

        if modo == "ASIAN0.5-1":
            return f"🎯 Entrada sugerida: asiática 0.5/1 goles {sf}".strip()

        if modo == "ASIAN+1" or (modo == "ASIAN" and linea == "+1") or (modo == "LIVE" and linea == "+1"):
            return (
                f"🎯 Entrada sugerida: línea {total + 1} goles {sf}\n"
                f"➕ Alternativa: over {total + 0.5} goles {sf}"
            ).strip()

        if modo == "NEXTGOAL":
            texto = f"🎯 Entrada sugerida: over {total + 0.5} goles {sf}"
            if linea and linea.startswith("ODDS"):
                texto += f"\n📊 Cuota objetivo: {linea.replace('ODDS', '')}"
            return texto.strip()

        if modo == "SINGLE":
            if linea == "OVER0.5":
                return f"🎯 Entrada sugerida: over 0.5 goles {sf}".strip()
            if linea == "OVER2.5":
                return f"🎯 Entrada sugerida: over 2.5 goles {sf}".strip()

    return None


# ==============================
# TÍTULOS / SUBTÍTULOS
# ==============================

def obtener_titulo_bonito(datos: dict, tipo_pick: str) -> str:
    codigo = normalizar_codigo(datos.get("codigo") or "")
    if codigo in TITULOS_BONITOS:
        return TITULOS_BONITOS[codigo]

    periodo = detectar_periodo_por_codigo(datos)
    if periodo == "2H":
        return "GOL 2ª MITAD"
    if periodo == "HT":
        return "GOL EN LA 1ª MITAD" if tipo_pick == "gol" else "CÓRNER EN LA 1ª MITAD"
    if periodo == "FT":
        return "GOL PARTIDO" if tipo_pick == "gol" else "CÓRNER PARTIDO"
    return "PICK"


def obtener_subtitulo_bonito(datos: dict, tipo_pick: str) -> str:
    codigo = normalizar_codigo(datos.get("codigo") or "")
    entrada = calcular_entrada_sugerida(datos, tipo_pick)

    if codigo in SUBTITULOS_BONITOS:
        base = SUBTITULOS_BONITOS[codigo]
        return f"{base}\n{entrada}" if entrada else base

    return entrada or ""


def obtener_resumen_linea(datos: dict, tipo_pick: str) -> str:
    periodo = detectar_periodo_por_codigo(datos)
    modo    = detectar_modo_por_codigo(datos)
    linea   = detectar_linea_por_codigo(datos)
    sf = "HT" if periodo == "HT" else "FT" if periodo == "FT" else "2H" if periodo == "2H" else ""

    if tipo_pick == "corner":
        total = parse_marcador_total(datos.get("corners"))
        if total is not None and (linea == "+1" or modo in ["SINGLE", "ASIAN+1", "ASIAN"]):
            return f"Línea {total + 1} {sf}".strip()

    if tipo_pick == "gol":
        total = parse_marcador_total(datos.get("goles"))
        if total is None:
            return ""
        if modo == "NEXTGOAL":
            return f"Over {total + 0.5} {sf}".strip()
        if modo == "ASIAN0.5-1":
            return f"Asian 0.5/1 {sf}".strip()
        if modo in ("ASIAN+1",) or (modo == "ASIAN" and linea == "+1") or (modo == "LIVE" and linea == "+1"):
            return f"Línea {total + 1} {sf}".strip()
        if modo == "SINGLE":
            if linea == "OVER0.5":
                return f"Over 0.5 {sf}".strip()
            if linea == "OVER2.5":
                return f"Over 2.5 {sf}".strip()

    return ""


def obtener_titulo_resumen(datos: dict, tipo_pick: str) -> str:
    partido      = datos.get("partido") or ""
    fase         = detectar_fase_por_codigo(datos)
    linea        = detectar_linea_por_codigo(datos)
    linea_resumen = obtener_resumen_linea(datos, tipo_pick)
    icono = "⚽" if tipo_pick == "gol" else "🚩"

    if fase == "PRE" and tipo_pick == "gol" and linea == "OVER2.5":
        return f"{icono} Over 2.5 FT | {partido}" if partido else f"{icono} Over 2.5 FT"

    if linea_resumen:
        return f"{icono} {linea_resumen} | {partido}" if partido else f"{icono} {linea_resumen}"

    periodo = detectar_periodo_por_codigo(datos)
    bloque = f"{icono} GOAL {periodo}" if tipo_pick == "gol" else f"{icono} CORNER {periodo}"
    return f"{bloque} | {partido}" if partido else bloque


# ==============================
# CONSTRUCCIÓN DE MENSAJES
# ==============================

def construir_mensaje_base(datos: dict, tipo_pick: str, para_free: bool = False) -> str:
    titulo_resumen = html.escape(obtener_titulo_resumen(datos, tipo_pick))
    titulo_bonito  = html.escape(obtener_titulo_bonito(datos, tipo_pick))
    subtitulo      = obtener_subtitulo_bonito(datos, tipo_pick)

    lineas = [f"<b>{titulo_resumen}</b>", "──────────────"]

    if titulo_bonito and titulo_bonito.lower() not in titulo_resumen.lower():
        lineas += [f"<b>{titulo_bonito}</b>", ""]

    if subtitulo:
        lineas += [html.escape(subtitulo), ""]

    if datos.get("picks") is not None:
        lineas.append(f"📦 Historial: <b>{datos['picks']} picks</b>")
    if datos.get("liga"):
        lineas.append(f"🏆 Liga: <b>{html.escape(datos['liga'])}</b>")
    if datos.get("partido"):
        lineas.append(f"⚽ Partido: <b>{html.escape(datos['partido'])}</b>")
    if datos.get("kickoff"):
        kickoff_txt = (
            datos["kickoff"]
            .replace("In ", "en ")
            .replace(" hours", " horas")
            .replace(" hour", " hora")
            .replace(" minutes", " minutos")
            .replace(" minute", " minuto")
        )
        lineas.append(f"⌛ Comienzo: <b>{html.escape(kickoff_txt)}</b>")

    lineas.append("")

    if datos.get("minuto") is not None:
        lineas.append(f"⏱ Minuto: <b>{datos['minuto']}</b>")
    elif datos.get("estado_partido"):
        _estados = {
            "DESCANSO":   "⏸ Estado: <b>Descanso</b>",
            "FINALIZADO": "🏁 Estado: <b>Finalizado</b>",
            "2ª MITAD":   "⏱ Estado: <b>2ª mitad</b>",
            "1ª MITAD":   "⏱ Estado: <b>1ª mitad</b>",
        }
        if datos["estado_partido"] in _estados:
            lineas.append(_estados[datos["estado_partido"]])

    if datos.get("goles"):
        tag = "<b>" if tipo_pick == "gol" else ""
        end = "</b>" if tipo_pick == "gol" else ""
        lineas.append(f"🥅 Goles: {tag}{html.escape(datos['goles'])}{end}")

    if datos.get("corners"):
        tag = "<b>" if tipo_pick == "corner" else ""
        end = "</b>" if tipo_pick == "corner" else ""
        lineas.append(f"🚩 Córners: {tag}{html.escape(datos['corners'])}{end}")

    if datos.get("momentum"):
        lineas.append(f"📈 Momentum: {html.escape(datos['momentum'])}")
    if datos.get("red_cards"):
        lineas.append(f"🟥 Rojas: {html.escape(datos['red_cards'])}")
    if datos.get("odds_1x2"):
        odds = " | ".join(datos["odds_1x2"].split())
        lineas.append(f"📊 Cuotas prepartido 1X2: {html.escape(odds)}")

    lineas.append("")

    if datos.get("strike_alerta"):
        lineas.append(f"📊 Acierto alerta: <b>{str(datos['strike_alerta']).replace('%', '')}%</b>")
    if datos.get("strike_liga"):
        sl = str(datos["strike_liga"])
        if sl.upper() == "N/A":
            lineas.append("📈 Acierto liga: <b>N/A</b>")
        else:
            lineas.append(f"📈 Acierto liga: <b>{sl.replace('%', '')}%</b>")

    if para_free:
        lineas += ["", "💎 Si quieres más picks en directo, contáctame por privado."]

    return "\n".join(lineas)


def construir_texto_resultado(datos: dict, tipo_pick: str) -> str:
    resultado = datos.get("resultado")
    if not resultado:
        return ""

    _mapa = {
        "HIT":  "✅ <b>Resultado: Hit</b>",
        "MISS": "❌ <b>Resultado: Miss</b>",
        "VOID": "⚪ <b>Resultado: Nulo</b>",
    }
    linea_resultado = _mapa.get(resultado, f"ℹ️ <b>Resultado: {html.escape(str(resultado))}</b>")
    extras = [linea_resultado]

    if datos.get("marcador_final"):
        extras.append(f"📌 Marcador final: <b>{html.escape(datos['marcador_final'])}</b>")
    elif datos.get("marcador_descanso"):
        extras.append(f"📌 Marcador descanso: <b>{html.escape(datos['marcador_descanso'])}</b>")

    return "\n".join(extras)


def construir_mensaje_editado(mensaje_original: str, datos: dict, tipo_pick: str) -> str:
    """
    Añade o actualiza el bloque de resultado en un mensaje ya publicado.
    Soporta múltiples ediciones: si el mensaje ya tiene un bloque de resultado,
    lo reemplaza en lugar de ignorarlo.
    """
    bloque_resultado = construir_texto_resultado(datos, tipo_pick)

    if not bloque_resultado:
        return mensaje_original

    # Si ya existe un bloque de resultado previo, lo reemplazamos
    if "Resultado:" in mensaje_original:
        # Separamos el cuerpo principal del bloque de resultado anterior
        partes = mensaje_original.split("\n\n")
        # Descartamos el último bloque si contiene "Resultado:"
        partes_limpias = [p for p in partes if "Resultado:" not in p]
        return "\n\n".join(partes_limpias) + f"\n\n{bloque_resultado}"

    return f"{mensaje_original}\n\n{bloque_resultado}"
