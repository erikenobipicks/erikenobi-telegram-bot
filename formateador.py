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

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════
# SUBTÍTULOS
# ══════════════════════════════════════════════════════════════════════

def _subtitulo(datos: dict, tipo_pick: str) -> str:
    """
    Devuelve el subtítulo legible de la alerta según el mercado y período.
    """
    fase    = detectar_fase_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""

    # ── PREPARTIDO ────────────────────────────────────────────────────
    if fase == "PRE":
        if "1X" in linea.upper() or "1X" in modo.upper():
            return "GANADOR LOCAL"
        if "OVER2.5" in linea.upper() or "OVER2.5" in modo.upper():
            return "+2.5 GOLES EN TODO EL PARTIDO"
        if "OVER1.5" in linea.upper() or "OVER1.5" in modo.upper():
            return "+1.5 GOLES EN TODO EL PARTIDO"
        if "OVER0.5" in linea.upper() or "OVER0.5" in modo.upper():
            return "+0.5 GOLES EN TODO EL PARTIDO"
        # Fallback PRE
        return "PREPARTIDO"

    # ── LIVE — CORNERS ────────────────────────────────────────────────
    if tipo_pick == "corner":
        if "ASIAN" in modo:
            if "+1" in linea:
                sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
                return f"ASIÁTICA +1 CÓRNER {sufijo}"
            if "0.5" in linea:
                return "ASIÁTICA 0.5/1 CÓRNER"
            return "ASIÁTICA CÓRNER"
        if modo == "+1" or "+1" in linea:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
            return f"CÓRNER MÁS {sufijo}"
        if "SINGLE" in modo:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
            return f"CÓRNER MÁS {sufijo}"
        if modo.startswith("OVER"):
            val    = modo.replace("OVER", "").strip()
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
            return f"OVER {val} CÓRNERS {sufijo}"
        if "OVER" in modo or "OVER" in linea:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
            return f"OVER {linea} CÓRNERS {sufijo}"
        return "CÓRNER"

    # ── LIVE — GOLES ──────────────────────────────────────────────────
    if modo == "NEXTGOAL":
        return "UN GOL MÁS"

    if "ASIAN" in modo:
        if "+1" in linea:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
            return f"ASIÁTICA +1 GOL {sufijo}"
        if "0.5" in linea and "1" in linea:
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
            return f"ASIÁTICA 0.5/1 GOL {sufijo}"
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
        return f"ASIÁTICA GOL {sufijo}"

    # OVER0.5, OVER1.5… el valor está en el modo, no en linea
    if modo.startswith("OVER"):
        val    = modo.replace("OVER", "").strip()
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
        return f"OVER {val} GOL {sufijo}"

    if "OVER" in modo:
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
        return f"OVER {linea} GOL {sufijo}"

    if modo == "+1":
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "PARTIDO"
        return f"ASIÁTICA +1 GOL {sufijo}"

    if "SINGLE" in modo:
        if "OVER" in linea.upper():
            val    = linea.upper().replace("OVER", "").strip()
            sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
            return f"OVER {val} GOL {sufijo}"
        sufijo = "EN LA 1ª MITAD" if periodo == "HT" else "FT"
        return f"GOL {sufijo}"

    return "GOL"


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


def _linea_entrada_gol(datos: dict) -> str | None:
    """
    Construye la línea de entrada sugerida para picks de gol.
    Calcula el número concreto cuando hay stats en vivo.
    Ejemplos:
      UGM NEXTGOAL 1-1 → "Entrada: Over 2.5 goles FT"
      CM02v2 ASIAN +1 HT 0-0 → "Entrada: Over 0.5 goles 1ª mitad"
      CM07v2 ASIAN +1 FT 1-0 → "Entrada: Over 1.5 goles FT"
      GFT1 ASIAN 0.5-1 0-0 → "Entrada: Asian 0.5/1 goles FT"
      LJ3 OVER0.5 HT → "Entrada: Over 0.5 goles 1ª mitad"
    """
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    sufijo  = "1ª mitad" if periodo == "HT" else "FT"

    # Leer goles actuales del partido
    goles_raw = datos.get("goles") or ""
    total_goles = None
    if goles_raw:
        partes = [p.strip() for p in goles_raw.replace("-", " ").split() if p.strip().isdigit()]
        if len(partes) >= 2:
            try:
                total_goles = int(partes[0]) + int(partes[1])
            except ValueError:
                pass

    # NEXTGOAL — siguiente gol del partido
    if modo == "NEXTGOAL":
        if total_goles is not None:
            # Con 0 goles → over 0.5, con 1+ → número entero
            over_line = f"{total_goles + 0.5}" if total_goles == 0 else str(total_goles + 1)
            return f"🎯 Entrada: Over {over_line} goles {sufijo}"
        return f"🎯 Entrada: Over 0.5 goles {sufijo}"

    # ASIAN +1 — un gol más a partir del marcador actual
    if "ASIAN" in modo and "+1" in linea:
        if total_goles is not None:
            over_line = f"{total_goles + 0.5}" if total_goles == 0 else str(total_goles + 1)
            return f"🎯 Entrada: Asian +1 {sufijo} (over {over_line} goles)"
        return f"🎯 Entrada: Asian +1 goles {sufijo}"

    # ASIAN 0.5-1 o 0.5/1 — línea asiática mixta
    if "ASIAN" in modo and ("0.5" in linea and "1" in linea):
        return f"🎯 Entrada: Asian 0.5/1 goles {sufijo}"

    # OVER0.5, OVER1.5… valor en el modo
    if modo.startswith("OVER"):
        val = modo.replace("OVER", "").strip()
        return f"🎯 Entrada: Over {val} goles {sufijo}"

    return None


def _linea_entrada_corner(datos: dict) -> list[str]:
    """
    Devuelve 1-2 líneas de entrada para corners, al estilo:
      🎯 Buscar 1 córner más en el partido
      🎯 Entrada sugerida: línea 6 córners FT
    """
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    sufijo  = "1ª mitad" if periodo == "HT" else "FT"

    corners_raw = datos.get("corners") or ""
    total_corners = None
    if corners_raw:
        partes = [p.strip() for p in corners_raw.replace("-", " ").split() if p.strip().isdigit()]
        if len(partes) >= 2:
            try:
                total_corners = int(partes[0]) + int(partes[1])
            except ValueError:
                pass

    # ASIAN +1 o SINGLE +1 — buscar 1 córner más
    if ("ASIAN" in modo and "+1" in linea) or modo == "+1" or ("SINGLE" in modo and "+1" in linea):
        mercado = "Asian +1" if "ASIAN" in modo else "Córner +1"
        accion  = "1ª mitad" if periodo == "HT" else "partido"
        buscar  = f"🎯 Buscar 1 córner más en el {accion}"
        if total_corners is not None:
            linea_num = total_corners + 1
            entrada = f"🎯 Entrada sugerida: línea {linea_num} córners {sufijo}"
        else:
            entrada = f"🎯 Entrada sugerida: {mercado} córners {sufijo}"
        return [buscar, entrada]

    # OVER0.5, OVER1.5…
    if modo.startswith("OVER"):
        val    = modo.replace("OVER", "").strip()
        accion = "1ª mitad" if periodo == "HT" else "partido"
        return [f"🎯 Entrada sugerida: over {val} córners {sufijo}"]

    # ASIAN con valor distinto
    if "ASIAN" in modo:
        accion = "1ª mitad" if periodo == "HT" else "partido"
        return [f"🎯 Entrada sugerida: Asian {linea} córners {sufijo}"]

    return []


def _linea_entrada_gol(datos: dict) -> list[str]:
    """
    Devuelve 1-2 líneas de entrada para goles, al estilo:
      🎯 Buscar 1 gol más en el partido
      🎯 Entrada sugerida: over 2.5 goles FT
    """
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    sufijo  = "1ª mitad" if periodo == "HT" else "FT"

    goles_raw = datos.get("goles") or ""
    total_goles = None
    if goles_raw:
        partes = [p.strip() for p in goles_raw.replace("-", " ").split() if p.strip().isdigit()]
        if len(partes) >= 2:
            try:
                total_goles = int(partes[0]) + int(partes[1])
            except ValueError:
                pass

    # NEXTGOAL
    if modo == "NEXTGOAL":
        accion = "1ª mitad" if periodo == "HT" else "partido"
        buscar = f"🎯 Buscar 1 gol más en el {accion}"
        if total_goles is not None:
            over_line = f"{total_goles + 0.5}" if total_goles == 0 else str(total_goles + 1)
            entrada = f"🎯 Entrada sugerida: over {over_line} goles {sufijo}"
        else:
            entrada = f"🎯 Entrada sugerida: over 0.5 goles {sufijo}"
        return [buscar, entrada]

    # ASIAN +1
    if "ASIAN" in modo and "+1" in linea:
        accion = "1ª mitad" if periodo == "HT" else "partido"
        buscar = f"🎯 Buscar 1 gol más en el {accion}"
        if total_goles is not None:
            over_line = f"{total_goles + 0.5}" if total_goles == 0 else str(total_goles + 1)
            entrada = f"🎯 Entrada sugerida: Asian +1 {sufijo} (over {over_line} goles)"
        else:
            entrada = f"🎯 Entrada sugerida: Asian +1 goles {sufijo}"
        return [buscar, entrada]

    # ASIAN 0.5-1
    if "ASIAN" in modo and "0.5" in linea and "1" in linea:
        return [f"🎯 Entrada sugerida: Asian 0.5/1 goles {sufijo}"]

    # OVER0.5, OVER1.5…
    if modo.startswith("OVER"):
        val    = modo.replace("OVER", "").strip()
        accion = "1ª mitad" if periodo == "HT" else "partido"
        buscar = f"🎯 Buscar over {val} goles en el {accion}"
        return [buscar, f"🎯 Entrada sugerida: over {val} goles {sufijo}"]

    return []


def _construir_live_corner(datos: dict) -> str:
    """Mensaje simplificado para picks de corner."""
    titulo  = _titulo_visible(datos, "corner")
    picks   = datos.get("picks")
    liga    = datos.get("liga")
    partido = datos.get("partido")
    s_alerta = datos.get("strike_alerta")
    s_liga   = datos.get("strike_liga")

    lineas = []
    lineas.append(f"<b>{titulo}</b>")

    # Líneas de entrada justo debajo del título
    for l in _linea_entrada_corner(datos):
        lineas.append(l)

    lineas.append("")

    if picks:
        lineas.append(f"📦 Historial: <b>{picks} picks</b>")
    if liga:
        lineas.append(f"🏆 Liga: <b>{liga}</b>")
    if partido:
        lineas.append(f"🚩 Partido: <b>{partido}</b>")

    # Stats en vivo — sin tarjetas rojas ni cuotas
    minuto  = datos.get("minuto")
    estado  = datos.get("estado_partido")
    goles   = datos.get("goles")
    corners = datos.get("corners")
    momentum = datos.get("momentum")

    lineas.append("")
    if minuto is not None:
        lineas.append(f"⏱ Minuto: <b>{minuto}'</b>")
    elif estado:
        lineas.append(f"⏱ Estado: <b>{estado}</b>")
    if goles:
        lineas.append(f"🥅 Goles: <b>{goles}</b>")
    if corners:
        lineas.append(f"🚩 Córners: {corners}")
    if momentum:
        lineas.append(f"📈 Momentum: {momentum}")

    lineas.append("")
    if s_alerta:
        lineas.append(f"📊 Acierto alerta: <b>{s_alerta}%</b>")
    if s_liga:
        s_liga_txt = s_liga if str(s_liga).upper() == "N/A" else f"{s_liga}%"
        lineas.append(f"📈 Acierto liga: <b>{s_liga_txt}</b>")

    # Salto de línea final para separar visualmente las alertas
    lineas.append("")

    return "\n".join(lineas)

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

    # Líneas de entrada — justo después del subtítulo
    if tipo_pick == "corner":
        for l in _linea_entrada_corner(datos):
            lineas.append(l)
    elif tipo_pick == "gol":
        for l in _linea_entrada_gol(datos):
            lineas.append(l)

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
    elif tipo_pick == "corner":
        msg = _construir_live_corner(datos)
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
