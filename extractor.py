import re
import logging

from utils import parse_percent, parse_marcador_total, normalizar_codigo
from config import FILTRO_STRIKE_LIGA

logger = logging.getLogger(__name__)


# ==============================
# EXTRACCIÓN DE DATOS
# ==============================

def extraer_numero_picks_desde_titulo(titulo_bruto: str) -> int | None:
    partes = [p.strip() for p in titulo_bruto.split("|")]
    for parte in reversed(partes):
        limpio = parte.replace(".", "").strip()
        limpio = limpio.replace("picks", "").replace("PICKS", "").strip()
        if limpio.isdigit():
            try:
                return int(limpio)
            except Exception:
                pass
    return None


def extraer_datos(texto: str) -> dict:
    datos = {
        "codigo": None,
        "meta_alerta": None,
        "titulo": None,
        "picks": None,
        "liga": None,
        "partido": None,
        "minuto": None,
        "estado_partido": None,
        "goles": None,
        "corners": None,
        "momentum": None,
        "red_cards": None,
        "odds_1x2": None,
        "odds_over_0_5": None,
        "odds_over_1_5": None,
        "odds_over_2_5": None,
        "strike_alerta": None,
        "strike_liga": None,
        "resultado": None,
        "marcador_descanso": None,
        "marcador_final": None,
        "kickoff": None,
    }

    lineas = [line.strip() for line in texto.splitlines() if line.strip()]

    # --- Título / código ---
    if lineas:
        titulo_bruto = lineas[0].strip()
        titulo_limpio = re.sub(r"^🔔\s*", "", titulo_bruto).strip()
        datos["meta_alerta"] = titulo_limpio

        partes = [p.strip() for p in titulo_limpio.split("|") if p.strip()]
        if partes:
            datos["codigo"] = partes[0]

        datos["picks"] = extraer_numero_picks_desde_titulo(titulo_limpio)

        titulo_visible = None
        if len(partes) >= 2:
            resto = partes[1:]
            candidatos = []
            for c in resto:
                if "%" in c.upper():
                    continue
                limpio = c.replace("picks", "").replace("PICKS", "").strip()
                if limpio.replace(".", "").isdigit():
                    continue
                candidatos.append(c)
            if candidatos:
                titulo_visible = " | ".join(candidatos)

        datos["titulo"] = titulo_visible or titulo_limpio

    # --- Localizar bloque TIMER ---
    _PREFIJOS_EXCLUIDOS = {
        "🔔", "TIMER:", "GOALS:", "CORNERS:", "MOMENTUM:",
        "RED CARDS:", "1X2 PRE-MATCH ODDS", "STRIKE RATE",
        "LIVE STATS", "POWERED BY", "MATCH SUMMARY",
    }

    def _es_linea_excluida(linea: str) -> bool:
        linea_up = linea.upper()
        return any(linea_up.startswith(p.upper()) or p.upper() in linea_up
                   for p in _PREFIJOS_EXCLUIDOS)

    timer_idx = next(
        (i for i, l in enumerate(lineas) if l.upper().startswith("TIMER:")),
        None,
    )

    # Kickoff idx — ancla para mensajes prepartido (no tienen TIMER)
    kickoff_idx = next(
        (i for i, l in enumerate(lineas) if "KICKOFF:" in l.upper()),
        None,
    )

    # Partido = línea inmediatamente anterior a TIMER (live)
    # o línea con " vs " antes de Kickoff (prepartido)
    partido_idx = None
    if timer_idx is not None and timer_idx >= 1:
        posible = lineas[timer_idx - 1].strip()
        if not _es_linea_excluida(posible):
            datos["partido"] = re.sub(r"^[^\w\d]+", "", posible).strip()
            partido_idx = timer_idx - 1

    elif kickoff_idx is not None:
        # Prepartido: buscar la línea con " vs " justo antes del Kickoff
        for idx in range(kickoff_idx - 1, 0, -1):
            linea = lineas[idx].strip()
            if " vs " in linea.lower() and not _es_linea_excluida(linea):
                datos["partido"] = re.sub(r"^[^\w\d]+", "", linea).strip()
                partido_idx = idx
                break

    # Liga = línea anterior al partido
    if partido_idx is not None and partido_idx >= 1:
        posible = lineas[partido_idx - 1].strip()
        if not _es_linea_excluida(posible):
            datos["liga"] = re.sub(r"^[^\w\d]+", "", posible).strip()

    # Fallback liga
    if not datos["liga"] and partido_idx is not None:
        for idx in range(partido_idx - 1, -1, -1):
            linea = lineas[idx].strip()
            if not linea or _es_linea_excluida(linea) or " vs " in linea.lower():
                continue
            datos["liga"] = re.sub(r"^[^\w\d]+", "", linea).strip()
            break

    # --- Timer / estado ---
    m = re.search(r"Timer:\s*(\d+)'", texto, re.IGNORECASE)
    if m:
        datos["minuto"] = int(m.group(1))
    else:
        m2 = re.search(r"Timer:\s*([^\n]+)", texto, re.IGNORECASE)
        if m2:
            timer_txt = m2.group(1).strip().upper()
            if "HALF TIME" in timer_txt or "HALFTIME" in timer_txt:
                datos["estado_partido"] = "DESCANSO"
            elif "FULL TIME" in timer_txt or "FULLTIME" in timer_txt:
                datos["estado_partido"] = "FINALIZADO"
            elif "2ND HALF" in timer_txt or "SECOND HALF" in timer_txt:
                datos["estado_partido"] = "2ª MITAD"
            elif "1ST HALF" in timer_txt or "FIRST HALF" in timer_txt:
                datos["estado_partido"] = "1ª MITAD"

    # --- Campos numéricos / de texto ---
    _campos_regex = [
        ("goles",           r"Goals:\s*([^\n]+)"),
        ("corners",         r"Corners:\s*([^\n]+)"),
        ("momentum",        r"Momentum:\s*([^\n]+)"),
        ("red_cards",       r"Red Cards:\s*([^\n]+)"),
        ("strike_alerta",   r"Strike Rate %:\s*([^\n]+)"),
        ("strike_liga",     r"Strike Rate % \(League\):\s*([^\n]+)"),
        ("marcador_descanso", r"Half-Time Score:\s*([^\n]+)"),
        ("marcador_final",  r"(?:Full-Time Score|Final Score):\s*([^\n]+)"),
        ("kickoff",         r"Kickoff:\s*([^\n]+)"),
    ]
    for campo, patron in _campos_regex:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            datos[campo] = m.group(1).strip()

    # Odds 1X2 (el valor está en la línea siguiente al label)
    m = re.search(r"1X2 Pre-Match Odds:\s*\n([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["odds_1x2"] = m.group(1).strip()

    # Over/Under 0.50 Odds (siguiente gol)
    m = re.search(r"Over/Under 0\.50 Odds:\s*\n([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["odds_over_0_5"] = m.group(1).strip()

    # Over/Under 2.50 Odds
    m = re.search(r"Over/Under 2\.50 Odds:\s*\n([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["odds_over_2_5"] = m.group(1).strip()

    # Over/Under 1.50 Odds
    m = re.search(r"Over/Under 1\.50 Odds:\s*\n([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["odds_over_1_5"] = m.group(1).strip()

    # --- Resultado: solo se acepta como palabra completa para evitar falsos positivos ---
    texto_up = texto.upper()
    if re.search(r"\b(HIT|WIN)\b", texto_up):
        datos["resultado"] = "HIT"
    elif re.search(r"\b(MISS|LOSS)\b", texto_up):
        datos["resultado"] = "MISS"
    elif re.search(r"\b(VOID|NULL)\b", texto_up):
        datos["resultado"] = "VOID"

    logger.debug(f"Extracción → liga: {datos['liga']} | partido: {datos['partido']}")
    return datos


# ==============================
# DETECCIÓN DE TIPO Y CÓDIGO
# ==============================

def obtener_bloques_codigo(datos: dict) -> list[str]:
    meta = datos.get("meta_alerta") or ""
    return [p.strip() for p in meta.split("|") if p.strip()]


def _es_formato_pre_corto(datos: dict) -> bool:
    partes = obtener_bloques_codigo(datos)
    return len(partes) >= 4 and partes[0].upper().startswith("PRE_") and partes[1].upper() == "PRE"


def _es_formato_rem(datos: dict) -> bool:
    """Detecta alertas REM_* (recordatorio de kickoff vinculado a un PRE)."""
    partes = obtener_bloques_codigo(datos)
    return bool(partes) and partes[0].upper().startswith("REM_")


def detectar_tipo_pick_por_codigo(datos: dict) -> str | None:
    meta = datos.get("meta_alerta") or ""
    codigo = (datos.get("codigo") or "").upper()

    # PRE_ y REM_ comparten la misma lógica de tipo (GOAL → gol, CORNER → corner)
    if codigo.startswith("PRE_") or codigo.startswith("REM_"):
        if "CORNER" in meta.upper() or "CORNER" in codigo:
            return "corner"
        return "gol"

    # ── 1. Emojis en toda la línea de título (más fiable que palabras clave) ──
    # ⛳️ o ⛳ → corner
    if "⛳" in meta:
        return "corner"
    # ⚽️ o ⚽ → gol
    if "⚽" in meta:
        return "gol"

    # ── 2. Fallback: palabras clave en partes[1] ──────────────────────────────
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 2:
        mercado = partes[1].upper()
        if "CORNER" in mercado:
            return "corner"
        if "GOAL" in mercado or "GOL" in mercado or "OVER" in mercado:
            return "gol"

    # ── 3. Fallback amplio: buscar en toda la meta_alerta ─────────────────────
    meta_up = meta.upper()
    if "CORNER" in meta_up:
        return "corner"
    if "GOAL" in meta_up or "GOL" in meta_up or "OVER 2.5" in meta_up or "OVER 0.5" in meta_up:
        return "gol"

    return None


def detectar_periodo_por_codigo(datos: dict) -> str | None:
    partes = obtener_bloques_codigo(datos)
    if _es_formato_pre_corto(datos):
        return partes[2].upper() if len(partes) >= 3 else None
    return partes[2].upper() if len(partes) >= 3 else None


def detectar_fase_por_codigo(datos: dict) -> str | None:
    partes = obtener_bloques_codigo(datos)
    # REM debe comprobarse ANTES que PRE porque su partes[3] == "PRE"
    # y sin esta guarda se clasificaría incorrectamente como prepartido.
    if _es_formato_rem(datos):
        return "REM"
    if _es_formato_pre_corto(datos):
        return "PRE"
    return partes[3].upper() if len(partes) >= 4 else None


def detectar_modo_por_codigo(datos: dict) -> str | None:
    partes = obtener_bloques_codigo(datos)
    if _es_formato_pre_corto(datos):
        return partes[3].upper() if len(partes) >= 4 else None
    return partes[4].upper() if len(partes) >= 5 else None


def detectar_linea_por_codigo(datos: dict) -> str | None:
    partes = obtener_bloques_codigo(datos)
    if _es_formato_pre_corto(datos):
        return (datos.get("codigo") or "").upper() or None
    return partes[5].upper() if len(partes) >= 6 else None


# ==============================
# FILTRO STRIKE LIGA
# ==============================

def pasa_filtro_strike_liga(datos: dict) -> bool:
    codigo = datos.get("codigo")

    if codigo not in FILTRO_STRIKE_LIGA:
        return True

    strike_liga = datos.get("strike_liga")

    if not strike_liga or str(strike_liga).upper() == "N/A":
        logger.info(f"⛔ {codigo} filtrado — sin Strike League")
        return False

    try:
        strike_liga = int(str(strike_liga).replace("%", "").strip())
    except Exception:
        logger.warning(f"⛔ {codigo} filtrado — Strike League inválido: {strike_liga}")
        return False

    minimo = FILTRO_STRIKE_LIGA[codigo]
    if strike_liga >= minimo:
        return True

    logger.info(f"⛔ {codigo} filtrado — Strike Liga {strike_liga}% < {minimo}%")
    return False
