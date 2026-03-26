"""
formateador.py
──────────────
Construye los mensajes HTML que el bot publica en los canales de Telegram.

Dos funciones públicas:
  construir_mensaje_base(datos, tipo_pick, para_free=False)
      → mensaje inicial (sin resultado todavía)

  construir_mensaje_editado(mensaje_base, datos, tipo_pick)
      → mensaje con resultado añadido/actualizado
"""

import re
import logging

from bankroll import construir_linea_stake_pre
from extractor import (
    detectar_fase_por_codigo,
    detectar_periodo_por_codigo,
    detectar_modo_por_codigo,
    detectar_linea_por_codigo,
)
from utils import parse_marcador_total

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# SUBTÍTULOS
# ══════════════════════════════════════════════════════════════════════

def _subtitulo(datos: dict, tipo_pick: str) -> str:
    """
    Devuelve el subtítulo legible de la alerta según el mercado y período.
    Para picks live incluye la línea de entrada dinámica calculada
    a partir del marcador actual.
    """
    fase    = detectar_fase_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""

    # ── PREPARTIDO — sin línea dinámica ───────────────────────────────
    if fase == "PRE":
        if "1X" in linea.upper() or "1X" in modo.upper():
            return "GANADOR LOCAL"
        if "OVER2.5" in linea.upper() or "OVER2.5" in modo.upper():
            return "+2.5 GOLES EN TODO EL PARTIDO"
        if "OVER1.5" in linea.upper() or "OVER1.5" in modo.upper():
            return "+1.5 GOLES EN TODO EL PARTIDO"
        if "OVER0.5" in linea.upper() or "OVER0.5" in modo.upper():
            return "+0.5 GOLES EN TODO EL PARTIDO"
        return "PREPARTIDO"

    # ── LIVE: calcular línea de entrada dinámica ──────────────────────
    linea_din = _linea_entrada_dinamica(datos, tipo_pick)

    def _con_entrada(texto_mercado: str) -> str:
        """Adjunta la línea dinámica al mercado si está disponible."""
        if linea_din:
            return f"{texto_mercado}\n{linea_din}"
        return texto_mercado

    # ── LIVE — CORNERS ────────────────────────────────────────────────
    if tipo_pick == "corner":
        if "ASIAN" in modo:
            if "+1" in linea:
                return _con_entrada("ASIÁTICA +1 CÓRNER PARTIDO")
            if "0.5" in linea:
                return _con_entrada("ASIÁTICA 0.5/1 CÓRNER")
            return _con_entrada("ASIÁTICA CÓRNER")
        if modo == "+1" or "+1" in linea:
            return _con_entrada("CÓRNER MÁS")
        if "SINGLE" in modo:
            return _con_entrada("CÓRNER MÁS")
        if modo.startswith("OVER"):
            val = modo.replace("OVER", "").strip()
            return _con_entrada(f"OVER {val} CÓRNERS")
        if "OVER" in modo or "OVER" in linea:
            return _con_entrada(f"OVER {linea} CÓRNERS")
        return _con_entrada("CÓRNER")

    # ── LIVE — GOLES ──────────────────────────────────────────────────
    if modo == "NEXTGOAL":
        return _con_entrada("UN GOL MÁS")

    if "ASIAN" in modo:
        if "+1" in linea:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
            return _con_entrada(f"ASIÁTICA +1 GOL {sufijo}")
        if "0.5" in linea and "1" in linea:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
            return _con_entrada(f"ASIÁTICA 0.5/1 GOL {sufijo}")
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
        return _con_entrada(f"ASIÁTICA GOL {sufijo}")

    if modo.startswith("OVER"):
        val    = modo.replace("OVER", "").strip()
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
        return _con_entrada(f"OVER {val} GOL {sufijo}")

    if "OVER" in modo:
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
        return _con_entrada(f"OVER {linea} GOL {sufijo}")

    if modo == "+1":
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
        return _con_entrada(f"ASIÁTICA +1 GOL {sufijo}")

    if "SINGLE" in modo:
        if "OVER" in linea.upper():
            val    = linea.upper().replace("OVER", "").strip()
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
            return _con_entrada(f"OVER {val} GOL {sufijo}")
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
        return _con_entrada(f"GOL {sufijo}")

    return _con_entrada("GOL")


# ══════════════════════════════════════════════════════════════════════
# FORMATEO DE ODDS
# ══════════════════════════════════════════════════════════════════════

def _formatear_odds(odds_raw: str | None) -> str | None:
    """
    Convierte '3.10 3.30 2.30' → '3.10 | 3.30 | 2.30'
    Si ya tiene '|' lo devuelve limpio.
    """
    if not odds_raw:
        return None
    partes = re.split(r"[\s|]+", odds_raw.strip())
    partes = [p for p in partes if p]
    if len(partes) >= 3:
        return f"{partes[0]} | {partes[1]} | {partes[2]}"
    return odds_raw


def _cuota_local(odds_raw: str | None) -> str | None:
    """Extrae la cuota del equipo local (primera de las 3 cuotas 1X2)."""
    if not odds_raw:
        return None
    partes = re.split(r"[\s|]+", odds_raw.strip())
    partes = [p for p in partes if p]
    return partes[0] if partes else None


# ══════════════════════════════════════════════════════════════════════
# LÍNEA DE ENTRADA Y ENTRADA DOBLE — PICKS LIVE
# ══════════════════════════════════════════════════════════════════════

def _total_actual(datos: dict, tipo_pick: str) -> int | None:
    """
    Devuelve el total actual del marcador relevante para el tipo de pick:
      - corners → suma del marcador de córners ("4-3" → 7)
      - gol     → suma del marcador de goles   ("2-1" → 3)
    Devuelve None si no hay dato o no parsea.
    """
    if tipo_pick == "corner":
        return parse_marcador_total(datos.get("corners") or "")
    return parse_marcador_total(datos.get("goles") or "")


def _linea_entrada_dinamica(datos: dict, tipo_pick: str) -> str | None:
    """
    Calcula la línea de entrada real según el marcador actual y el tipo de alerta.

    Lógica:
      • Alerta "+1" / ASIÁTICA +1 / SINGLE+1  → entrada = total_actual + 1
        Ejemplo corners: llevan 5 → "Entrada línea 6"
        Ejemplo goles:   llevan 2 → "Entrada línea 3"

      • Alerta NEXTGOAL / OVER0.5 live         → entrada = total_actual + 0.5
        Ejemplo: llevan 1 gol → "Entrada línea 1.5"

      • Alerta OVER N (modo=OVERx.x)          → línea fija = x.x (ya viene en el código)
        No se recalcula; la línea está implícita en el mercado.

    Devuelve None si no hay suficientes datos para calcular.
    """
    modo  = detectar_modo_por_codigo(datos) or ""
    linea = detectar_linea_por_codigo(datos) or ""
    total = _total_actual(datos, tipo_pick)

    if total is None:
        return None

    # +1 / SINGLE+1 / ASIAN+1 → línea entera
    es_mas1 = (
        modo == "+1"
        or "+1" in linea
        or ("ASIAN" in modo and "+1" in linea)
        or ("SINGLE" in modo and "+1" in linea)
    )
    if es_mas1:
        entrada = total + 1
        unidad  = "córners" if tipo_pick == "corner" else "goles"
        return f"➡️ Entrada línea <b>{entrada} {unidad}</b>  <i>(ahora: {total})</i>"

    # NEXTGOAL / OVER0.5 live → línea con decimales
    es_over05 = (
        modo == "NEXTGOAL"
        or modo == "OVER0.5"
        or (modo.startswith("OVER") and "0.5" in modo)
        or ("SINGLE" in modo and "OVER0.5" in linea.upper())
    )
    if es_over05:
        entrada = total + 0.5
        unidad  = "córners" if tipo_pick == "corner" else "goles"
        return f"➡️ Entrada línea <b>{entrada} {unidad}</b>  <i>(ahora: {total})</i>"

    return None


def _entrada_doble_over05(datos: dict, tipo_pick: str) -> str | None:
    """
    Sugiere una entrada doble Over +0.5 cuando la alerta principal es +1 o ASIAN+1.
    La entrada doble es siempre total_actual + 0.5.

    Ejemplo: alerta +1 córners, llevan 5 → principal es línea 6,
             entrada doble: Over 5.5 córners.

    Solo se muestra cuando el pick principal ya es +1 (no para OVER fijos).
    Devuelve None si no aplica o no hay datos.
    """
    modo  = detectar_modo_por_codigo(datos) or ""
    linea = detectar_linea_por_codigo(datos) or ""
    total = _total_actual(datos, tipo_pick)

    if total is None:
        return None

    es_mas1 = (
        modo == "+1"
        or "+1" in linea
        or ("ASIAN" in modo and "+1" in linea)
        or ("SINGLE" in modo and "+1" in linea)
    )
    if not es_mas1:
        return None

    entrada_doble = total + 0.5
    unidad = "córners" if tipo_pick == "corner" else "goles"
    return (
        f"⚡ Entrada doble: Over <b>{entrada_doble} {unidad}</b>"
        f"  <i>(más segura, menor cuota)</i>"
    )


# ══════════════════════════════════════════════════════════════════════
# TÍTULO VISIBLE
# ══════════════════════════════════════════════════════════════════════

def _titulo_visible(datos: dict, tipo_pick: str) -> str:
    """
    Construye la primera línea del mensaje: emoji + mercado + partido.
    """
    emoji   = "⚽" if tipo_pick == "gol" else "🚩"
    partido = datos.get("partido") or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""
    fase    = detectar_fase_por_codigo(datos) or ""

    # ── PREPARTIDO ────────────────────────────────────────────────────
    if fase == "PRE":
        linea_up = linea.upper()
        if "1X" in linea_up:
            mercado = "Ganador Local"
        elif "OVER2.5" in linea_up:
            mercado = "Over 2.5 FT"
        elif "OVER1.5" in linea_up:
            mercado = "Over 1.5 FT"
        elif "OVER0.5" in linea_up:
            mercado = "Over 0.5 FT"
        else:
            mercado = "Prepartido"

    # ── LIVE — NEXTGOAL (UGM, NG1…) ──────────────────────────────────
    # linea puede ser "ODDS1.60" o un número → siempre "Over 0.5"
    elif modo == "NEXTGOAL":
        mercado = f"Over 0.5 {periodo}"

    # ── LIVE — ASIAN ──────────────────────────────────────────────────
    elif "ASIAN" in modo:
        if "+1" in linea:
            mercado = f"Línea 1 {periodo}"
        elif "0.5" in linea and "1" in linea:
            mercado = f"GOAL {periodo}"
        else:
            mercado = f"Asiática {periodo}"

    # ── LIVE — OVER0.5 / OVER (modo contiene el valor) ───────────────
    elif modo.startswith("OVER"):
        # modo = "OVER0.5" → extraer "0.5"
        val = modo.replace("OVER", "").strip() or linea
        mercado = f"Over {val} {periodo}"

    # ── LIVE — SINGLE ─────────────────────────────────────────────────
    elif "SINGLE" in modo:
        if tipo_pick == "corner":
            if "+1" in linea:
                mercado = f"Córner +1 {periodo}"
            elif linea:
                mercado = f"Córner {linea} {periodo}"
            else:
                mercado = f"Córner {periodo}"
        else:
            # SINGLE gol — linea puede ser "OVER2.5", "+1", etc.
            if "OVER" in linea.upper():
                val = linea.upper().replace("OVER", "").strip()
                mercado = f"Over {val} {periodo}"
            elif "+1" in linea:
                mercado = f"Línea 1 {periodo}"
            elif linea:
                mercado = f"Gol {linea} {periodo}"
            else:
                mercado = f"Gol {periodo}"

    # ── +1 directo (CH2…) ─────────────────────────────────────────────
    elif modo == "+1":
        mercado = f"Línea 1 {periodo}" if tipo_pick == "gol" else f"Córner +1 {periodo}"

    # ── Fallback ──────────────────────────────────────────────────────
    else:
        mercado = f"{'Gol' if tipo_pick == 'gol' else 'Córner'} {periodo}".strip()

    if partido:
        return f"{emoji} {mercado} | {partido}"
    return f"{emoji} {mercado}"


# ══════════════════════════════════════════════════════════════════════
# BLOQUE DE ESTADÍSTICAS IN-PLAY
# ══════════════════════════════════════════════════════════════════════

def _bloque_stats_live(datos: dict) -> list[str]:
    """Líneas con timer, goles, corners, momentum, rojas."""
    lineas = []

    minuto  = datos.get("minuto")
    estado  = datos.get("estado_partido")
    if minuto is not None:
        lineas.append(f"⏱ Minuto: <b>{minuto}'</b>")
    elif estado:
        lineas.append(f"⏱ Estado: <b>{estado}</b>")

    if datos.get("goles"):
        lineas.append(f"🥅 Goles: <b>{datos['goles']}</b>")
    if datos.get("corners"):
        lineas.append(f"🚩 Córners: {datos['corners']}")
    if datos.get("momentum"):
        lineas.append(f"📈 Momentum: {datos['momentum']}")
    if datos.get("red_cards"):
        lineas.append(f"🟥 Rojas: {datos['red_cards']}")

    return lineas


# ══════════════════════════════════════════════════════════════════════
# MENSAJE BASE — LIVE
# ══════════════════════════════════════════════════════════════════════

def _construir_live(datos: dict, tipo_pick: str, para_free: bool) -> str:
    subtitulo  = _subtitulo(datos, tipo_pick)
    titulo     = _titulo_visible(datos, tipo_pick)
    picks      = datos.get("picks")
    liga       = datos.get("liga")
    partido    = datos.get("partido")
    odds       = _formatear_odds(datos.get("odds_1x2"))
    s_alerta   = datos.get("strike_alerta")
    s_liga     = datos.get("strike_liga")
    modo       = detectar_modo_por_codigo(datos) or ""

    lineas = []
    lineas.append(f"<b>{titulo}</b>")
    lineas.append("──────────────")
    lineas.append(f"<b>{subtitulo}</b>")
    lineas.append("")

    if picks:
        lineas.append(f"📦 Historial: <b>{picks} picks</b>")
    if liga:
        lineas.append(f"🏆 Liga: <b>{liga}</b>")
    if partido:
        emoji_partido = "⚽" if tipo_pick == "gol" else "🚩"
        lineas.append(f"{emoji_partido} Partido: <b>{partido}</b>")

    # Stats en vivo
    stats = _bloque_stats_live(datos)
    if stats:
        lineas.append("")
        lineas.extend(stats)

    # Entrada doble Over +0.5 (solo cuando la alerta es +1 y hay marcador)
    entrada_doble = _entrada_doble_over05(datos, tipo_pick)
    if entrada_doble:
        lineas.append(entrada_doble)

    # Cuota del siguiente gol (Over 0.5) para picks NEXTGOAL FT
    if modo == "NEXTGOAL" and tipo_pick == "gol":
        odds_05_raw = datos.get("odds_over_0_5")
        if odds_05_raw:
            partes = odds_05_raw.split()
            if partes:
                lineas.append(f"💰 Cuota siguiente gol: <b>{partes[0]}</b>")

    # Cuotas prepartido 1X2
    if odds:
        lineas.append(f"📊 Cuotas prepartido 1X2: {odds}")

    # Aciertos
    lineas.append("")
    if s_alerta:
        lineas.append(f"📊 Acierto alerta: <b>{s_alerta}%</b>")
    if s_liga:
        s_liga_txt = s_liga if str(s_liga).upper() == "N/A" else f"{s_liga}%"
        lineas.append(f"📈 Acierto liga: <b>{s_liga_txt}</b>")

    return "\n".join(lineas)


# ══════════════════════════════════════════════════════════════════════
# MENSAJE BASE — PREPARTIDO
# ══════════════════════════════════════════════════════════════════════

def _construir_pre(datos: dict, tipo_pick: str) -> str:
    subtitulo = _subtitulo(datos, tipo_pick)
    titulo    = _titulo_visible(datos, tipo_pick)
    picks     = datos.get("picks")
    liga      = datos.get("liga")
    partido   = datos.get("partido")
    kickoff   = datos.get("kickoff")
    odds_raw  = datos.get("odds_1x2")
    odds      = _formatear_odds(odds_raw)
    s_alerta  = datos.get("strike_alerta")
    s_liga    = datos.get("strike_liga")
    modo      = detectar_modo_por_codigo(datos) or ""
    linea     = detectar_linea_por_codigo(datos) or ""

    lineas = []
    lineas.append(f"<b>{titulo}</b>")
    lineas.append("──────────────")
    lineas.append(f"<b>{subtitulo}</b>")
    lineas.append("")

    if picks:
        lineas.append(f"📦 Historial: <b>{picks} picks</b>")
    if liga:
        lineas.append(f"🏆 Liga: <b>{liga}</b>")
    if partido:
        emoji_partido = "⚽" if tipo_pick == "gol" else "🚩"
        lineas.append(f"{emoji_partido} Partido: <b>{partido}</b>")
    if kickoff:
        lineas.append(f"⌛ Kickoff: <b>{kickoff}</b>")

    lineas.append("")

    # ── Ganador Local: cuota 1X2 local + stake ────────────────────────
    if "1X" in linea.upper():
        cuota_local = _cuota_local(odds_raw)
        if cuota_local:
            lineas.append(f"💰 Cuota local: <b>{cuota_local}</b>")
            linea_stake = construir_linea_stake_pre(cuota_local)
            if linea_stake:
                lineas.append(linea_stake)
        if odds:
            lineas.append(f"📊 Cuotas 1X2: {odds}")

    # ── Over 2.5 FT: cuota over 2.5 prepartido ────────────────────────
    elif "OVER2.5" in linea.upper():
        odds_25_raw = datos.get("odds_over_2_5")
        if odds_25_raw:
            partes = odds_25_raw.split()
            if partes:
                lineas.append(f"💰 Cuota Over 2.5: <b>{partes[0]}</b>")
        if odds:
            lineas.append(f"📊 Cuotas 1X2: {odds}")

    # ── Resto de prepartidos: solo 1X2 ───────────────────────────────
    else:
        if odds:
            lineas.append(f"📊 Cuotas 1X2: {odds}")

    # Aciertos
    lineas.append("")
    if s_alerta:
        lineas.append(f"📊 Acierto alerta: <b>{s_alerta}%</b>")
    if s_liga:
        s_liga_txt = s_liga if str(s_liga).upper() in ("N/A", "0") else f"{s_liga}%"
        lineas.append(f"📈 Acierto liga: <b>{s_liga_txt}</b>")

    return "\n".join(lineas)


# ══════════════════════════════════════════════════════════════════════
# API PÚBLICA
# ══════════════════════════════════════════════════════════════════════

def construir_mensaje_base(
    datos: dict,
    tipo_pick: str,
    para_free: bool = False,
) -> str:
    """
    Construye el mensaje inicial (sin resultado).
    para_free=True oculta las cuotas detalladas.
    """
    fase = detectar_fase_por_codigo(datos) or ""

    if fase == "PRE":
        msg = _construir_pre(datos, tipo_pick)
    else:
        msg = _construir_live(datos, tipo_pick, para_free)

    if para_free:
        # En free no mostramos cuotas detalladas
        msg = re.sub(r"\n📊 Cuotas.*", "", msg)
        msg = re.sub(r"\n💰 Cuota local.*", "", msg)

    return msg.strip()


def construir_mensaje_editado(
    mensaje_base: str,
    datos: dict,
    tipo_pick: str,
) -> str:
    """
    Añade (o reemplaza) el bloque de resultado al mensaje base.
    Si ya hay un bloque de resultado previo, lo sustituye.
    """
    resultado       = datos.get("resultado", "").upper()
    marcador_final  = datos.get("marcador_final")
    marcador_desc   = datos.get("marcador_descanso")

    # Emoji y texto del resultado
    if resultado == "HIT":
        emoji_res = "✅"
        texto_res = "Resultado: Hit"
    elif resultado == "MISS":
        emoji_res = "❌"
        texto_res = "Resultado: Miss"
    elif resultado == "VOID":
        emoji_res = "⚪"
        texto_res = "Resultado: Void"
    else:
        emoji_res = "⏳"
        texto_res = "Resultado: Pendiente"

    bloque_resultado = f"\n\n{emoji_res} <b>{texto_res}</b>"

    if marcador_final:
        bloque_resultado += f"\n📌 Marcador final: <b>{marcador_final}</b>"
    elif marcador_desc:
        bloque_resultado += f"\n📌 Marcador descanso: <b>{marcador_desc}</b>"

    # Si ya existe un bloque de resultado, lo reemplazamos
    base_limpio = re.sub(
        r"\n\n[✅❌⚪⏳].*",
        "",
        mensaje_base,
        flags=re.DOTALL,
    )

    return (base_limpio + bloque_resultado).strip()
