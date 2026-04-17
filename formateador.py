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

import logging
import math
import re
from datetime import datetime
from html import escape

from bankroll import construir_linea_stake_pre
from extractor import (
    detectar_fase_por_codigo,
    detectar_periodo_por_codigo,
    detectar_modo_por_codigo,
    detectar_linea_por_codigo,
)

logger = logging.getLogger(__name__)


def _esc(valor) -> str:
    """Escapa texto dinÃ¡mico antes de insertarlo en mensajes HTML."""
    return escape(str(valor), quote=False)


def _limpiar_prefijo_visual(texto: str | None) -> str | None:
    if not texto:
        return texto
    return re.sub(r"^[^\w\d]+\s*", "", texto).strip()


def _compactar_xy(texto: str | None) -> str | None:
    if not texto:
        return texto
    return re.sub(r"(\d+(?:[.,]\d+)?)\s*-\s*(\d+(?:[.,]\d+)?)", r"\1-\2", texto)


def _generar_id_alerta(tipo_pick: str) -> str | None:
    prefijo = "GOL" if tipo_pick == "gol" else "CNR" if tipo_pick == "corner" else "GEN"
    try:
        return f"{prefijo}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    except Exception:
        return None


def _obtener_id_alerta(tipo_pick: str, datos: dict, mensaje_base: str | None = None) -> str | None:
    for clave in ("alert_id", "message_id_origen", "id"):
        valor = datos.get(clave)
        if valor:
            return str(valor)

    if mensaje_base:
        m = re.search(r"\n🆔 ID:\s*([^\n]+)", mensaje_base)
        if m:
            return m.group(1).strip()

    return _generar_id_alerta(tipo_pick)


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
        codigo = (datos.get("codigo") or "").upper()
        if codigo == "PRE_1XHT":
            return "GANADOR LOCAL 1ª MITAD"
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


def _sumar_marcador(raw: str | None) -> int | None:
    if not raw:
        return None
    partes = [p.strip() for p in raw.replace("-", " ").split() if p.strip().isdigit()]
    if len(partes) >= 2:
        try:
            return int(partes[0]) + int(partes[1])
        except ValueError:
            return None
    return None


def _formatear_linea_real(valor: float | int) -> str:
    if isinstance(valor, float) and valor.is_integer():
        return str(int(valor))
    if isinstance(valor, int):
        return str(valor)
    return str(valor).replace(".0", "")


def _extra_objetivo_desde_linea(valor: str | None) -> int | None:
    if not valor:
        return None

    numeros = re.findall(r"\d+(?:[.,]\d+)?", valor)
    if not numeros:
        return None

    try:
        maximo = max(float(num.replace(",", ".")) for num in numeros)
    except ValueError:
        return None

    return max(1, math.ceil(maximo))


def _linea_real_gol(datos: dict) -> str | None:
    modo = (detectar_modo_por_codigo(datos) or "").upper()
    linea = (detectar_linea_por_codigo(datos) or "").upper()
    total_goles = _sumar_marcador(datos.get("goles"))

    if total_goles is None:
        return None

    if modo == "NEXTGOAL":
        return _formatear_linea_real(total_goles + 1)

    if modo == "+1":
        return _formatear_linea_real(total_goles + 1)

    if "ASIAN" in modo:
        extra = 1 if "+1" in linea else _extra_objetivo_desde_linea(linea)
        if extra is not None:
            return _formatear_linea_real(total_goles + extra)

    if "SINGLE" in modo:
        if "+1" in linea:
            return _formatear_linea_real(total_goles + 1)
        if "OVER" in linea:
            extra = _extra_objetivo_desde_linea(linea.replace("OVER", "", 1))
            if extra is not None:
                return _formatear_linea_real(total_goles + extra)

    if modo.startswith("OVER"):
        extra = _extra_objetivo_desde_linea(modo.replace("OVER", "", 1))
        if extra is not None:
            return _formatear_linea_real(total_goles + extra)

    if "OVER" in modo:
        extra = _extra_objetivo_desde_linea(linea)
        if extra is not None:
            return _formatear_linea_real(total_goles + extra)

    return None

def _entrada_titulo(datos: dict, tipo_pick: str) -> str | None:
    """Texto corto de entrada para mostrar junto al título."""
    fase    = detectar_fase_por_codigo(datos) or ""
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    sufijo  = "HT" if periodo == "HT" else "FT"

    if fase == "PRE":
        return None

    if tipo_pick == "gol":
        linea_real = _linea_real_gol(datos)
        if linea_real is not None:
            return f"🎯 Línea {linea_real} {sufijo}"
        if modo.startswith("OVER"):
            val = modo.replace("OVER", "").strip()
            if val:
                return f"🎯 Over {val} {sufijo}"
        return None

    total_corners = _sumar_marcador(datos.get("corners"))
    if ("ASIAN" in modo and "+1" in linea) or modo == "+1" or ("SINGLE" in modo and "+1" in linea):
        if total_corners is not None:
            return f"🎯 Over {total_corners + 1} córners {sufijo}"
        return f"🎯 Over +1 córner {sufijo}"
    if modo.startswith("OVER"):
        val = modo.replace("OVER", "").strip()
        if val:
            return f"🎯 Over {val} córners {sufijo}"
    return None


# ══════════════════════════════════════════════════════════════════════
# TÍTULO VISIBLE
# ══════════════════════════════════════════════════════════════════════

def _titulo_visible(datos: dict, tipo_pick: str) -> str:
    """
    Construye la primera línea del mensaje: emoji + mercado + partido.
    """
    emoji   = "⚽" if tipo_pick == "gol" else "🚩"
    partido = _esc(datos.get("partido") or "")
    periodo = detectar_periodo_por_codigo(datos) or ""
    modo    = detectar_modo_por_codigo(datos) or ""
    linea   = detectar_linea_por_codigo(datos) or ""
    fase    = detectar_fase_por_codigo(datos) or ""

    # ── PREPARTIDO ────────────────────────────────────────────────────
    if fase == "PRE":
        codigo = (datos.get("codigo") or "").upper()
        linea_up = linea.upper()
        if codigo == "PRE_1XHT":
            mercado = "Ganador Local HT"
        elif "1X" in linea_up:
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
        mercado = f"Next Goal {periodo}"

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

    entrada = _entrada_titulo(datos, tipo_pick)
    # Corners y NEXTGOAL usan la línea de entrada directamente como título
    # para que sea visible de un vistazo en la notificación del móvil.
    _usar_entrada = entrada and (tipo_pick == "corner" or modo == "NEXTGOAL")
    pick_visible = _limpiar_prefijo_visual(entrada) if _usar_entrada else mercado
    base = f"{emoji} <b>{_esc(pick_visible)}</b>"
    if partido:
        base = f"{base} | {partido}"

    return base


# ══════════════════════════════════════════════════════════════════════
# BLOQUE DE ESTADÍSTICAS IN-PLAY
# ══════════════════════════════════════════════════════════════════════

def _es_momentum_cero(momentum_raw: str) -> bool:
    """Devuelve True si el momentum es 0-0 o equivalente."""
    if not momentum_raw:
        return True
    partes = [p.strip() for p in momentum_raw.replace("-", " ").split() if p.strip().isdigit()]
    return all(p == "0" for p in partes) if partes else True


def _formatear_momentum(momentum_raw: str) -> str:
    """
    Resalta en negrita el momentum cuando alguno de los dos lados es >= 60.
    Ejemplo: "62-38" -> "<b>62-38</b>"
    """
    if not momentum_raw:
        return momentum_raw

    partes = [p.strip() for p in momentum_raw.replace("-", " ").split() if p.strip().isdigit()]
    if len(partes) >= 2:
        try:
            if max(int(partes[0]), int(partes[1])) >= 60:
                return f"<b>{_esc(_compactar_xy(momentum_raw))}</b>"
        except ValueError:
            pass
    return _esc(_compactar_xy(momentum_raw))


def _bloque_stats_live(datos: dict, con_corners: bool = True, con_rojas: bool = True) -> list[str]:
    """Línea compacta con timer, goles, [corners], momentum, [rojas]."""
    contexto = []

    minuto  = datos.get("minuto")
    estado  = datos.get("estado_partido")
    if minuto is not None:
        contexto.append(f"⏱ {minuto}'")
    elif estado:
        contexto.append(f"⏱ {_esc(estado)}")

    if datos.get("goles"):
        contexto.append(f"🥅 {_esc(_compactar_xy(datos['goles']))}")
    if con_corners and datos.get("corners"):
        contexto.append(f"⚽ {_esc(_compactar_xy(datos['corners']))}")

    momentum = datos.get("momentum")
    if momentum and not _es_momentum_cero(momentum):
        contexto.append(f"📈 {_esc(_compactar_xy(momentum))}")

    if con_rojas and datos.get("red_cards"):
        rc = datos["red_cards"]
        partes = [p.strip() for p in rc.replace("-", " ").split() if p.strip().isdigit()]
        if not all(p == "0" for p in partes):
            contexto.append(f"🟥 {_esc(_compactar_xy(rc))}")

    return [" | ".join(contexto)] if contexto else []


def _linea_entrada_corner(datos: dict) -> list[str]:
    """
    Devuelve una línea corta de entrada para corners.
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
        if total_corners is not None:
            linea_num = total_corners + 1
            entrada = f"🎯 Entrada: línea {linea_num} córners {sufijo}"
        else:
            entrada = f"🎯 Entrada: córner +1 {sufijo}"
        return [entrada]

    # OVER0.5, OVER1.5…
    if modo.startswith("OVER"):
        val    = modo.replace("OVER", "").strip()
        return [f"🎯 Entrada: over {val} córners {sufijo}"]

    # ASIAN con valor distinto
    if "ASIAN" in modo:
        return [f"🎯 Entrada: Asian {linea} córners {sufijo}"]

    return []


def _linea_entrada_gol(datos: dict) -> list[str]:
    """
    Devuelve una línea corta de entrada para goles.
    """
    modo    = detectar_modo_por_codigo(datos) or ""
    periodo = detectar_periodo_por_codigo(datos) or ""
    sufijo  = "1ª mitad" if periodo == "HT" else "FT"

    linea_real = _linea_real_gol(datos)
    if linea_real is not None:
        return [f"🎯 Entrada: línea {linea_real} goles {sufijo}"]

    # OVER0.5, OVER1.5…
    if modo.startswith("OVER"):
        val    = modo.replace("OVER", "").strip()
        return [f"🎯 Entrada: over {val} goles {sufijo}"]

    return []


def _construir_live_corner(datos: dict) -> str:
    """Mensaje para picks de corner — mismo layout que goles."""
    titulo   = _titulo_visible(datos, "corner")
    liga     = _esc(datos.get("liga") or "")
    partido  = _esc(datos.get("partido") or "")
    s_alerta = datos.get("strike_alerta")
    s_liga   = datos.get("strike_liga")
    odds     = _formatear_odds(datos.get("odds_1x2"))

    lineas = []
    lineas.append(titulo)

    # Línea de entrada justo debajo del título
    for l in _linea_entrada_corner(datos):
        lineas.append(l)

    lineas.append("")

    # Stats en vivo — con banderín de corner, sin tarjetas rojas
    minuto   = datos.get("minuto")
    estado   = datos.get("estado_partido")
    goles    = datos.get("goles")
    corners  = datos.get("corners")
    momentum = datos.get("momentum")

    contexto = []
    if minuto is not None:
        contexto.append(f"⏱ {minuto}'")
    elif estado:
        contexto.append(f"⏱ {_esc(estado)}")
    if goles:
        contexto.append(f"🥅 {_esc(_compactar_xy(goles))}")
    if corners:
        contexto.append(f"🚩 {_esc(_compactar_xy(corners))}")
    if momentum and not _es_momentum_cero(momentum):
        contexto.append(f"📈 {_esc(_compactar_xy(momentum))}")

    if contexto:
        lineas.append(" | ".join(contexto))
        lineas.append("")

    # Contexto del partido
    if liga:
        lineas.append(f"🏆 Liga: <b>{liga}</b>")
    if partido:
        lineas.append(f"{partido}")
    if odds:
        lineas.append(f"📊 1X2: {odds}")

    # Rendimiento
    if s_alerta:
        lineas.append(f"📊 Strike alerta: <b>{s_alerta}%</b>")
    if s_liga:
        s_liga_txt = s_liga if str(s_liga).upper() == "N/A" else f"{s_liga}%"
        lineas.append(f"📈 Strike liga: <b>{s_liga_txt}</b>")

    # Modelo al final
    if datos.get("codigo"):
        lineas.append(f"📦 Modelo: <b>{_esc(_limpiar_prefijo_visual(datos['codigo']))}</b>")

    return "\n".join(lineas)

def _construir_live(datos: dict, tipo_pick: str, para_free: bool) -> str:
    titulo   = _titulo_visible(datos, tipo_pick)
    liga     = _esc(datos.get("liga") or "")
    partido  = _esc(datos.get("partido") or "")
    s_alerta = datos.get("strike_alerta")
    s_liga   = datos.get("strike_liga")
    odds     = _formatear_odds(datos.get("odds_1x2"))

    lineas = []
    lineas.append(titulo)

    # Líneas de entrada — justo después del título
    if tipo_pick == "corner":
        for l in _linea_entrada_corner(datos):
            lineas.append(l)
    elif tipo_pick == "gol":
        for l in _linea_entrada_gol(datos):
            lineas.append(l)

    lineas.append("")

    # Stats en vivo
    stats = _bloque_stats_live(datos, con_corners=False, con_rojas=True)
    if stats:
        lineas.extend(stats)
        lineas.append("")

    # Contexto del partido
    if liga:
        lineas.append(f"🏆 Liga: <b>{liga}</b>")
    if partido:
        lineas.append(f"{partido}")
    if odds:
        lineas.append(f"📊 1X2: {odds}")

    # Rendimiento
    if s_alerta:
        lineas.append(f"📊 Strike alerta: <b>{s_alerta}%</b>")
    if s_liga:
        s_liga_txt = s_liga if str(s_liga).upper() == "N/A" else f"{s_liga}%"
        lineas.append(f"📈 Strike liga: <b>{s_liga_txt}</b>")

    # Modelo al final
    if datos.get("codigo"):
        lineas.append(f"📦 Modelo: <b>{_esc(_limpiar_prefijo_visual(datos['codigo']))}</b>")

    return "\n".join(lineas)


# ══════════════════════════════════════════════════════════════════════
# MENSAJE BASE — PREPARTIDO
# ══════════════════════════════════════════════════════════════════════

def _construir_pre(datos: dict, tipo_pick: str) -> str:
    subtitulo = _subtitulo(datos, tipo_pick)
    titulo    = _titulo_visible(datos, tipo_pick)
    picks     = datos.get("picks")
    liga      = _esc(datos.get("liga") or "")
    kickoff   = _esc(datos.get("kickoff") or "")
    odds_raw  = datos.get("odds_1x2")
    odds      = _formatear_odds(odds_raw)
    s_alerta  = datos.get("strike_alerta")
    s_liga    = datos.get("strike_liga")
    codigo    = (datos.get("codigo") or "").upper()
    modo      = detectar_modo_por_codigo(datos) or ""
    linea     = detectar_linea_por_codigo(datos) or ""

    if "OVER2.5" in linea.upper():
        stake_raw = datos.get("stake")
        try:
            stake_u = float(str(stake_raw).replace(",", ".")) if stake_raw is not None else 1.0
        except (TypeError, ValueError):
            stake_u = 1.0
        stake_txt = "1.0" if stake_u == 1.0 else f"{stake_u:.2f}".rstrip("0").rstrip(".")
        partido = _esc(datos.get("partido") or "")
        titulo_pre = titulo if " | " in titulo or not partido else f"{titulo} | {partido}"
        lineas = [titulo_pre]
        cuota_25 = None
        odds_25_raw = datos.get("odds_over_2_5")
        if odds_25_raw:
            partes = odds_25_raw.split()
            if partes:
                cuota_25 = partes[0]

        if kickoff:
            lineas.append("")
            lineas.append(f"⌛ Kickoff: {kickoff}")

        if cuota_25:
            lineas.append("")
            lineas.append(f"💰 Cuota: <b>{cuota_25}</b>")

        lineas.append(f"📦 Stake: <b>{stake_txt}u</b>")

        lineas.append("")
        if s_alerta:
            lineas.append(f"📊 Strike alerta: <b>{s_alerta}%</b>")
        if s_liga:
            s_liga_txt = s_liga if str(s_liga).upper() in ("N/A", "0") else f"{s_liga}%"
            lineas.append(f"📈 Strike liga: <b>{s_liga_txt}</b>")
        if liga:
            lineas.append(f"🏆 Liga: <b>{liga}</b>")

        return "\n".join(lineas)

    lineas = []
    lineas.append(f"<b>{titulo}</b>")
    lineas.append(f"<b>{subtitulo}</b>")
    lineas.append("")

    if picks:
        lineas.append(f"📦 Hist.: <b>{picks}</b>")
    if liga:
        lineas.append(f"🏆 Liga: <b>{liga}</b>")
    if kickoff:
        lineas.append(f"⌛ Kickoff: <b>{kickoff}</b>")

    lineas.append("")

    # ── Ganador Local: cuota 1X2 local + stake ────────────────────────
    if codigo == "PRE_1X":
        cuota_local = _cuota_local(odds_raw)
        if cuota_local:
            lineas.append(f"💰 Cuota local: <b>{cuota_local}</b>")
            linea_stake = construir_linea_stake_pre(cuota_local)
            if linea_stake:
                lineas.append(linea_stake)
        if odds:
            lineas.append(f"📊 1X2: {odds}")
    elif codigo == "PRE_1XHT":
        if odds:
            lineas.append(f"📊 1X2 partido: {odds}")

    # ── Over 2.5 FT: cuota over 2.5 prepartido ────────────────────────
    elif "OVER2.5" in linea.upper():
        odds_25_raw = datos.get("odds_over_2_5")
        if odds_25_raw:
            partes = odds_25_raw.split()
            if partes:
                lineas.append(f"💰 Cuota Over 2.5: <b>{partes[0]}</b>")
        if odds:
            lineas.append(f"📊 1X2: {odds}")

    # ── Resto de prepartidos: solo 1X2 ───────────────────────────────
    else:
        if odds:
            lineas.append(f"📊 1X2: {odds}")

    # Aciertos
    lineas.append("")
    if s_alerta:
        lineas.append(f"📊 Strike alerta: <b>{s_alerta}%</b>")
    if s_liga:
        s_liga_txt = s_liga if str(s_liga).upper() in ("N/A", "0") else f"{s_liga}%"
        lineas.append(f"📈 Strike liga: <b>{s_liga_txt}</b>")

    return "\n".join(lineas)


# ══════════════════════════════════════════════════════════════════════
# API PÚBLICA
# ══════════════════════════════════════════════════════════════════════

def _barra_stake(stake: float, max_stake: float = 3.0, bloques: int = 10) -> str:
    """Barra visual proporcional al stake. Ej: stake=2 → ███████░░░"""
    llenos = round((stake / max_stake) * bloques)
    return "█" * llenos + "░" * (bloques - llenos)


def _bloque_clasificacion(clasificacion: dict) -> str:
    """
    Construye la cabecera de nivel para insertar al inicio del mensaje.

    Ejemplo (sin score):
        🔵 <b>ÉLITE</b> — Nivel 3/3
        ━━━━━━━━━━━━━━━━━━━━
        💰 <b>Stake: 3.0u</b> ██████████   WR: 96.0% <i>(n=25)</i>
        ━━━━━━━━━━━━━━━━━━━━

    Ejemplo (con score estadístico):
        🟢 <b>ALTO</b> — Nivel 2/3
        ━━━━━━━━━━━━━━━━━━━━
        💰 <b>Stake: 2.5u</b> ████████░░   WR: 86.3% <i>(n=51)</i>
        🔬 Score histórico: <b>81/100</b> <i>(alta)</i>
        ━━━━━━━━━━━━━━━━━━━━
    """
    sep   = "━━━━━━━━━━━━━━━━━━━━"
    emoji = clasificacion["emoji"]
    nom   = clasificacion["nombre"]
    nivel = clasificacion["nivel"]
    stake = clasificacion["stake"]
    wr    = clasificacion["wr"]
    n     = clasificacion["n"]
    adv   = clasificacion.get("advertencia")
    score_info = clasificacion.get("score_info")

    if stake > 0:
        barra       = _barra_stake(stake)
        linea_stake = f"💰 <b>Stake: {stake}u</b>  {barra}   WR: {wr}% <i>(n={n})</i>"
    else:
        linea_stake = f"💰 <b>Stake: 0u</b> — no apostar   WR: {wr}% <i>(n={n})</i>"

    if adv:
        linea_stake += f"\n⚠️ <i>{adv}</i>"

    lineas = [
        f"{emoji} <b>{nom}</b> — Nivel {nivel}/3",
        sep,
        linea_stake,
    ]

    # Score estadístico — solo visible cuando hay suficiente historial
    if score_info and score_info.get("confianza") in ("media", "alta"):
        sc        = score_info["score"]
        confianza = score_info["confianza"]
        sc_emoji  = "🔬" if confianza == "alta" else "📐"
        lineas.append(f"{sc_emoji} Score histórico: <b>{sc}/100</b> <i>({confianza})</i>")

    lineas.append(sep)
    return "\n".join(lineas)


def construir_mensaje_base(
    datos: dict,
    tipo_pick: str,
    para_free: bool = False,
    clasificacion: dict | None = None,
) -> str:
    """
    Construye el mensaje inicial (sin resultado).
    para_free=True oculta las cuotas detalladas.
    clasificacion=dict activa el bloque de nivel/stake al inicio.
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
        msg = re.sub(r"\n📊 (?:Cuotas.*|1X2:.*)", "", msg)
        msg = re.sub(r"\n💰 Cuota.*", "", msg)

    # Insertar bloque de clasificación después de la primera línea
    # para que el título (pick + partido) siga siendo lo primero visible
    # en la notificación del móvil.
    if clasificacion is not None:
        cabecera  = _bloque_clasificacion(clasificacion)
        lineas    = msg.strip().split("\n")
        primera   = lineas[0]
        resto     = "\n".join(lineas[1:]).strip() if len(lineas) > 1 else ""
        if resto:
            msg = primera + "\n\n" + cabecera + "\n" + resto
        else:
            msg = primera + "\n\n" + cabecera

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
    fase            = detectar_fase_por_codigo(datos) or ""
    linea           = detectar_linea_por_codigo(datos) or ""

    # Emoji y texto del resultado
    if resultado == "HIT":
        emoji_res = "✅"
        texto_res = "HIT"
    elif resultado == "MISS":
        emoji_res = "❌"
        texto_res = "MISS"
    elif resultado == "VOID":
        emoji_res = "⚪"
        texto_res = "VOID"
    else:
        emoji_res = "⏳"
        texto_res = "PENDIENTE"

    if fase == "PRE" and "OVER2.5" in linea.upper():
        stake_raw = datos.get("stake")
        try:
            stake_u = float(str(stake_raw).replace(",", ".")) if stake_raw is not None else 1.0
        except (TypeError, ValueError):
            stake_u = 1.0

        cuota_25 = None
        odds_25_raw = datos.get("odds_over_2_5")
        if odds_25_raw:
            partes = odds_25_raw.split()
            if partes:
                try:
                    cuota_25 = float(partes[0].replace(",", "."))
                except ValueError:
                    cuota_25 = None

        beneficio = None
        if resultado == "HIT" and cuota_25 is not None:
            beneficio = (cuota_25 - 1) * stake_u
        elif resultado == "MISS":
            beneficio = -stake_u

        base_limpio = re.sub(
            r"\n\n[✅❌⚪⏳].*",
            "",
            mensaje_base,
            flags=re.DOTALL,
        )

        bloque_resultado = f"\n\n{emoji_res} Resultado: <b>{texto_res}</b>"
        if marcador_final:
            bloque_resultado += f"\n📌 Marcador final: <b>{_esc(_compactar_xy(marcador_final))}</b>"
        elif marcador_desc:
            bloque_resultado += f"\n📌 Marcador descanso: <b>{_esc(_compactar_xy(marcador_desc))}</b>"
        if beneficio is not None:
            beneficio_txt = f"{beneficio:+.2f}u"
            bloque_resultado += f"\n💵 Beneficio: <b>{beneficio_txt}</b>"

        return (base_limpio + bloque_resultado).strip()

    bloque_resultado = f"\n\n{emoji_res} Resultado: <b>{texto_res}</b>"

    if marcador_final:
        bloque_resultado += f"\n📌 Marcador final: <b>{_esc(_compactar_xy(marcador_final))}</b>"
    elif marcador_desc:
        bloque_resultado += f"\n📌 Marcador descanso: <b>{_esc(_compactar_xy(marcador_desc))}</b>"

    # Si ya existe un bloque de resultado, lo reemplazamos
    base_limpio = re.sub(
        r"\n\n[✅❌⚪⏳].*",
        "",
        mensaje_base,
        flags=re.DOTALL,
    )

    return (base_limpio + bloque_resultado).strip()
