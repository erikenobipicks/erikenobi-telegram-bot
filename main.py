import re
import json
import html
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    MessageHandler,
    ContextTypes,
    CommandHandler,
    filters,
)

# ==============================
# CONFIGURACIÓN
# ==============================

import os

TOKEN = os.getenv("BOT_TOKEN")

# ORIGEN ACTUAL = PREMIUM
CANAL_ORIGEN_ID = -1002037791209

# DESTINOS
CANAL_PRUEBAS_ID = -1002037791209
CANAL_CORNERS_ID = -1003895151594
CANAL_GOLES_ID = -1003818905455
CANAL_GENERAL_ID = -1003876204382
CANAL_FREE_ID = -1002973101273

ENVIAR_A_GENERAL = True

# Cupos FREE por día
MAX_FREE_GOLES = 2
MAX_FREE_CORNERS = 2
MAX_FREE_TOTAL = 4

# Horario permitido para FREE (hora española)
FREE_TIMEZONE = "Europe/Madrid"
FREE_HORA_INICIO = 10
FREE_HORA_FIN = 22

# Canal/grupo donde quieres publicar los resúmenes automáticos
CANALES_RESUMEN = [
    CANAL_FREE_ID,
    CANAL_GOLES_ID,
    CANAL_CORNERS_ID,
]

# Archivo para persistencia simple
STATE_FILE = "bot_state.json"

# ==============================
# FILTRO POR STRIKE LIGA
# ==============================

FILTRO_STRIKE_LIGA = {
    "UGM": 65,
    "LJ2": 65,
}

# ==============================
# MEMORIA / ESTADO
# ==============================

STATE = {
    "mensajes_publicados": {},
    "free_state": {
        "fecha": None,
        "goles_enviados": 0,
        "corners_enviados": 0,
        "ultimo_score_gol": -1,
        "ultimo_score_corner": -1,
        "ultima_hora_envio": None,
    },
    "estadisticas": [],
    "resumen_control": {
        "ultimo_resumen_dia": None,
        "ultimo_resumen_semana": None,
    },
}

# ==============================
# PERSISTENCIA
# ==============================


def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"❌ Error guardando estado: {e}")


def load_state():
    global STATE
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            cargado = json.load(f)

        STATE["mensajes_publicados"] = cargado.get("mensajes_publicados", {})
        STATE["estadisticas"] = cargado.get("estadisticas", [])
        STATE["resumen_control"] = cargado.get(
            "resumen_control",
            {
                "ultimo_resumen_dia": None,
                "ultimo_resumen_semana": None,
            },
        )

        fs = cargado.get("free_state", {})
        STATE["free_state"] = {
            "fecha": fs.get("fecha"),
            "goles_enviados": fs.get("goles_enviados", 0),
            "corners_enviados": fs.get("corners_enviados", 0),
            "ultimo_score_gol": fs.get("ultimo_score_gol", -1),
            "ultimo_score_corner": fs.get("ultimo_score_corner", -1),
            "ultima_hora_envio": fs.get("ultima_hora_envio", None),
        }

        print("✅ Estado cargado desde disco")
    except FileNotFoundError:
        print("ℹ️ No existe estado previo, se crea uno nuevo")
    except Exception as e:
        print(f"❌ Error cargando estado: {e}")


# ==============================
# UTILIDADES GENERALES
# ==============================


def ahora_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ahora_madrid():
    return datetime.now(ZoneInfo(FREE_TIMEZONE))


def esta_en_horario_free():
    ahora = ahora_madrid()
    hora = ahora.hour
    return FREE_HORA_INICIO <= hora < FREE_HORA_FIN


def clave_hora_actual_free():
    ahora = ahora_madrid()
    return ahora.strftime("%Y-%m-%d %H")

def hoy_str():
    return datetime.now().strftime("%Y-%m-%d")


def semana_str():
    year, week, _ = datetime.now().isocalendar()
    return f"{year}-W{week}"


def parse_percent(valor):
    if valor is None:
        return None
    valor = str(valor).strip().replace("%", "")
    if valor.upper() == "N/A":
        return None
    try:
        return int(valor)
    except Exception:
        return None


def parse_marcador_total(valor: str):
    if not valor:
        return None
    m = re.match(r"\s*(\d+)\s*-\s*(\d+)\s*$", valor)
    if not m:
        return None
    return int(m.group(1)) + int(m.group(2))


def esta_en_horario_free():
    ahora_madrid = datetime.now(ZoneInfo(FREE_TIMEZONE))
    hora = ahora_madrid.hour
    return FREE_HORA_INICIO <= hora < FREE_HORA_FIN


def normalizar_codigo(codigo: str) -> str:
    if not codigo:
        return ""
    return codigo.upper().replace("_", "").replace(" ", "")


def pasa_filtro_strike_liga(datos):
    codigo = datos.get("codigo")

    if codigo not in FILTRO_STRIKE_LIGA:
        return True

    strike_liga = datos.get("strike_liga")

    if not strike_liga or str(strike_liga).upper() == "N/A":
        print(f"⛔ {codigo} filtrado | sin Strike League")
        return False

    try:
        strike_liga = int(str(strike_liga).replace("%", "").strip())
    except Exception:
        print(f"⛔ {codigo} filtrado | Strike League inválido: {strike_liga}")
        return False

    minimo = FILTRO_STRIKE_LIGA[codigo]

    if strike_liga >= minimo:
        return True

    print(f"⛔ {codigo} filtrado | Strike Liga {strike_liga}% < {minimo}%")
    return False


# ==============================
# FREE
# ==============================

def ahora_madrid():
    return datetime.now(ZoneInfo(FREE_TIMEZONE))


def esta_en_horario_free():
    ahora = ahora_madrid()
    hora = ahora.hour
    return FREE_HORA_INICIO <= hora < FREE_HORA_FIN


def clave_hora_actual_free():
    ahora = ahora_madrid()
    return ahora.strftime("%Y-%m-%d %H")


def reset_free_state_si_toca():
    fs = STATE["free_state"]
    hoy = hoy_str()
    if fs["fecha"] != hoy:
        fs["fecha"] = hoy
        fs["goles_enviados"] = 0
        fs["corners_enviados"] = 0
        fs["ultimo_score_gol"] = -1
        fs["ultimo_score_corner"] = -1
        fs["ultima_hora_envio"] = None


def total_free_enviados():
    fs = STATE["free_state"]
    return fs["goles_enviados"] + fs["corners_enviados"]


def score_para_free(datos):
    strike_alerta = parse_percent(datos.get("strike_alerta"))
    strike_liga = parse_percent(datos.get("strike_liga"))

    if strike_alerta is None and strike_liga is None:
        return -1

    if strike_alerta is not None and strike_liga is not None:
        return strike_alerta * 1000 + strike_liga

    if strike_alerta is not None:
        return strike_alerta * 1000

    return strike_liga if strike_liga is not None else -1


def debe_enviar_a_free(tipo_pick, datos):
    reset_free_state_si_toca()
    fs = STATE["free_state"]

    if not esta_en_horario_free():
        return False, f"Fuera de horario FREE ({FREE_HORA_INICIO}:00-{FREE_HORA_FIN}:00 {FREE_TIMEZONE})"

    hora_actual = clave_hora_actual_free()
    if fs.get("ultima_hora_envio") == hora_actual:
        return False, "Ya se ha enviado un pick FREE en esta hora"

    if total_free_enviados() >= MAX_FREE_TOTAL:
        return False, f"FREE diario completo ({MAX_FREE_TOTAL})"

    score = score_para_free(datos)

    if tipo_pick == "gol":
        if fs["goles_enviados"] >= MAX_FREE_GOLES:
            return False, f"FREE goles completo ({MAX_FREE_GOLES})"
        if score <= fs["ultimo_score_gol"]:
            return False, f"Score gol insuficiente ({score})"
        return True, "OK"

    if tipo_pick == "corner":
        if fs["corners_enviados"] >= MAX_FREE_CORNERS:
            return False, f"FREE corners completo ({MAX_FREE_CORNERS})"
        if score <= fs["ultimo_score_corner"]:
            return False, f"Score corner insuficiente ({score})"
        return True, "OK"

    return False, "Tipo no válido"


def registrar_envio_free(tipo_pick, datos):
    fs = STATE["free_state"]
    score = score_para_free(datos)
    fs["ultima_hora_envio"] = clave_hora_actual_free()

    if tipo_pick == "gol":
        fs["goles_enviados"] += 1
        fs["ultimo_score_gol"] = score

    elif tipo_pick == "corner":
        fs["corners_enviados"] += 1
        fs["ultimo_score_corner"] = score


# ==============================
# EXTRACCIÓN
# ==============================


def extraer_numero_picks_desde_titulo(titulo_bruto: str):
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
        "strike_alerta": None,
        "strike_liga": None,
        "resultado": None,
        "marcador_descanso": None,
        "marcador_final": None,
        "kickoff": None,
    }

    lineas = [line.strip() for line in texto.splitlines() if line.strip()]

    # Línea estructurada superior
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
                c_up = c.upper()

                if "%" in c_up:
                    continue

                limpio = c.replace("picks", "").replace("PICKS", "").strip()
                if limpio.replace(".", "").isdigit():
                    continue

                candidatos.append(c)

            if candidatos:
                titulo_visible = " | ".join(candidatos)

        if not titulo_visible:
            titulo_visible = titulo_limpio

        datos["titulo"] = titulo_visible

        # Buscar índice de Timer
    timer_idx = None
    for idx, linea in enumerate(lineas):
        if linea.upper().startswith("TIMER:"):
            timer_idx = idx
            break

    # Partido = línea inmediatamente anterior a Timer
    partido_idx = None
    if timer_idx is not None and timer_idx >= 1:
        posible_partido = lineas[timer_idx - 1].strip()

        # Evitamos coger cosas raras
        if (
            not posible_partido.upper().startswith("🔔")
            and not posible_partido.upper().startswith("TIMER:")
            and not posible_partido.upper().startswith("GOALS:")
            and not posible_partido.upper().startswith("CORNERS:")
            and not posible_partido.upper().startswith("MOMENTUM:")
            and not posible_partido.upper().startswith("RED CARDS:")
        ):
            datos["partido"] = re.sub(r"^[^\w\d]+", "", posible_partido).strip()
            partido_idx = timer_idx - 1

    # Liga = línea inmediatamente anterior al partido
    if partido_idx is not None and partido_idx >= 1:
        posible_liga = lineas[partido_idx - 1].strip()

        if (
            not posible_liga.upper().startswith("🔔")
            and not posible_liga.upper().startswith("TIMER:")
            and not posible_liga.upper().startswith("GOALS:")
            and not posible_liga.upper().startswith("CORNERS:")
            and not posible_liga.upper().startswith("MOMENTUM:")
            and not posible_liga.upper().startswith("RED CARDS:")
        ):
            datos["liga"] = re.sub(r"^[^\w\d]+", "", posible_liga).strip()

    # Fallback: si aún no hay liga, buscamos línea de competición antes del partido
    if not datos["liga"] and partido_idx is not None:
        for idx in range(partido_idx - 1, -1, -1):
            linea = lineas[idx].strip()

            if not linea:
                continue
            if linea.upper().startswith("🔔"):
                continue
            if linea.upper().startswith("TIMER:"):
                continue
            if " vs " in linea.lower():
                continue
            if linea.upper().startswith("GOALS:"):
                continue
            if linea.upper().startswith("CORNERS:"):
                continue
            if linea.upper().startswith("MOMENTUM:"):
                continue
            if linea.upper().startswith("RED CARDS:"):
                continue
            if linea.upper().startswith("1X2 PRE-MATCH ODDS"):
                continue
            if linea.upper().startswith("STRIKE RATE"):
                continue
            if "LIVE STATS" in linea.upper():
                continue
            if "POWERED BY" in linea.upper():
                continue
            if "MATCH SUMMARY" in linea.upper():
                continue

            datos["liga"] = re.sub(r"^[^\w\d]+", "", linea).strip()
            break

    # Timer / estado
    m = re.search(r"Timer:\s*(\d+)'", texto, re.IGNORECASE)
    if m:
        datos["minuto"] = int(m.group(1))
    else:
        m_timer = re.search(r"Timer:\s*([^\n]+)", texto, re.IGNORECASE)
        if m_timer:
            timer_txt = m_timer.group(1).strip().upper()

            if "HALF TIME" in timer_txt or "HALFTIME" in timer_txt:
                datos["estado_partido"] = "DESCANSO"
            elif "FULL TIME" in timer_txt or "FULLTIME" in timer_txt:
                datos["estado_partido"] = "FINALIZADO"
            elif "2ND HALF" in timer_txt or "SECOND HALF" in timer_txt:
                datos["estado_partido"] = "2ª MITAD"
            elif "1ST HALF" in timer_txt or "FIRST HALF" in timer_txt:
                datos["estado_partido"] = "1ª MITAD"

    # Goles
    m = re.search(r"Goals:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["goles"] = m.group(1).strip()

    # Corners
    m = re.search(r"Corners:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["corners"] = m.group(1).strip()

    # Momentum
    m = re.search(r"Momentum:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["momentum"] = m.group(1).strip()

    # Red cards
    m = re.search(r"Red Cards:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["red_cards"] = m.group(1).strip()

    # Odds
    m = re.search(r"1X2 Pre-Match Odds:\s*\n([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["odds_1x2"] = m.group(1).strip()

    # Strike alerta
    m = re.search(r"Strike Rate %:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["strike_alerta"] = m.group(1).strip()

    # Strike liga
    m = re.search(r"Strike Rate % \(League\):\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["strike_liga"] = m.group(1).strip()

    # Resultado
    texto_up = texto.upper()
    if "HIT" in texto_up or "WIN" in texto_up:
        datos["resultado"] = "HIT"
    elif "MISS" in texto_up or "LOSS" in texto_up:
        datos["resultado"] = "MISS"
    elif "VOID" in texto_up or "NULL" in texto_up:
        datos["resultado"] = "VOID"

    # Marcador descanso
    m = re.search(r"Half-Time Score:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["marcador_descanso"] = m.group(1).strip()

    # Marcador final
    m = re.search(r"(?:Full-Time Score|Final Score):\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["marcador_final"] = m.group(1).strip()

    # Kickoff
    m = re.search(r"Kickoff:\s*([^\n]+)", texto, re.IGNORECASE)
    if m:
        datos["kickoff"] = m.group(1).strip()

    print(f"DEBUG -> liga: {datos['liga']} | partido: {datos['partido']}")

    return datos


# ==============================
# DETECCIÓN
# ==============================


def obtener_bloques_codigo(datos):
    meta = datos.get("meta_alerta") or ""
    return [p.strip() for p in meta.split("|") if p.strip()]


def detectar_tipo_pick_por_codigo(datos):
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 2:
        mercado = partes[1].upper()
        if "CORNER" in mercado:
            return "corner"
        if "GOAL" in mercado or "GOL" in mercado:
            return "gol"
    return None


def detectar_periodo_por_codigo(datos):
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 3:
        return partes[2].upper()
    return None


def detectar_fase_por_codigo(datos):
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 4:
        return partes[3].upper()
    return None


def detectar_modo_por_codigo(datos):
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 5:
        return partes[4].upper()
    return None


def detectar_linea_por_codigo(datos):
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 6:
        return partes[5].upper()
    return None


def detectar_condicion_por_codigo(datos):
    partes = obtener_bloques_codigo(datos)
    if len(partes) >= 7:
        return partes[6].upper()
    return None


# ==============================
# CÁLCULO DE ENTRADA SUGERIDA
# ==============================


def calcular_entrada_sugerida(datos: dict, tipo_pick: str):
    periodo = detectar_periodo_por_codigo(datos)
    fase = detectar_fase_por_codigo(datos)
    modo = detectar_modo_por_codigo(datos)
    linea = detectar_linea_por_codigo(datos)
    condicion = detectar_condicion_por_codigo(datos)

    if tipo_pick == "corner":
        total_corners = parse_marcador_total(datos.get("corners"))
        if total_corners is None:
            return None

        sufijo = "HT" if periodo == "HT" else "FT" if periodo == "FT" else ""

        if modo == "ASIAN+1":
            return f"🎯 Entrada sugerida: línea {total_corners + 1} córners {sufijo}"

        if modo == "SINGLE" and linea == "+1":
            return f"🎯 Entrada sugerida: línea {total_corners + 1} córners {sufijo}"

        if linea == "+1":
            return f"🎯 Entrada sugerida: línea {total_corners + 1} córners {sufijo}"

    if tipo_pick == "gol":
        total_goles = parse_marcador_total(datos.get("goles"))
        if total_goles is None:
            return None

        sufijo = "HT" if periodo == "HT" else "FT" if periodo == "FT" else "2ª parte" if periodo == "2H" else ""

        if modo == "ASIAN0.5-1":
            linea_asian = f"{total_goles + 0.5}/{total_goles + 1}"
            texto = f"🎯 Entrada sugerida: asiática {linea_asian} goles {sufijo}"
            if condicion:
                texto += f"\n📌 Condición: {condicion}"
            return texto

         if modo == "ASIAN+1":
             linea_asian = f"{total_goles + 0.5}/{total_goles + 1}"
             texto = f"🎯 Entrada sugerida: asiática {linea_asian} goles {sufijo}"
             if condicion:
                 texto += f"\n📌 Condición: {condicion}"
             return texto

        if modo == "NEXTGOAL":
            texto = f"🎯 Entrada sugerida: +{total_goles + 0.5} goles {sufijo}"
            if linea and linea.startswith("ODDS"):
                texto += f"\n📊 Cuota objetivo: {linea.replace('ODDS', '')}"
            return texto

        if modo == "SINGLE" and linea == "OVER0.5":
            texto = f"🎯 Entrada sugerida: +0.5 goles {sufijo}"
            if condicion:
                texto += f"\n📌 Condición: {condicion}"
            return texto

        if modo == "SINGLE" and linea == "OVER2.5":
            return f"🎯 Entrada sugerida: over 2.5 goles {sufijo}"

        if modo == "LIVE" and linea == "+1":
            texto = f"🎯 Entrada sugerida: +1 gol {sufijo}"
            if condicion:
                texto += f"\n📌 Condición: {condicion}"
            return texto

        if fase == "LIVE" and modo == "ASIAN" and linea == "+1":
            linea_asian = f"{total_goles + 0.5}/{total_goles + 1}"
            texto = f"🎯 Entrada sugerida: asiática {linea_asian} goles {sufijo}"
            if condicion:
                texto += f"\n📌 Condición: {condicion}"
            return texto

    return None


# ==============================
# FORMATO
# ==============================

TITULOS_BONITOS = {
    # GOLES
    "UGM": "UN GOL MÁS",
    "LJ2": "OVER 0.5 GOL EN LA 1ª MITAD",
    "LIVEJ1": "ASIÁTICA 0.5/1 GOL EN LA 1ª MITAD",
    "A1.1": "OVER 0.5 GOL EN LA 1ª MITAD",
    "C1.1": "OVER 0.5 GOL EN LA 1ª MITAD",
    "A0.1": "OVER 0.5 GOL EN LA 1ª MITAD",
    "A3.0": "BUSCAMOS 1 GOL",
    "HT2": "GOL ANTES DEL 75'",
    "L5.2": "GOL ANTES DEL 75'",
    "QB": "ASIÁTICA +1 GOL PARTIDO",
    "CM01": "ASIÁTICA +1 GOL PARTIDO",
    "CM01V2": "ASIÁTICA +1 GOL PARTIDO",
    "CM01V3": "ASIÁTICA +1 GOL PARTIDO",
    "CM01V4": "ASIÁTICA +1 GOL PARTIDO",
    "CM02V2": "ASIÁTICA +1 GOL PARTIDO",
    "CM02V4": "ASIÁTICA +1 GOL PARTIDO",
    "CM06V3": "ASIÁTICA +1 GOL PARTIDO",
    "CM07V2": "ASIÁTICA +1 GOL PARTIDO",
    "GFT1": "ASIÁTICA 0.5/1 GOL PARTIDO",
    "GFT2": "ASIÁTICA 0.5/1 GOL PARTIDO",
    "PREO25FT12H7": "OVER 2.5 GOLES PREPARTIDO",

    # CORNERS
    "CF1": "CÓRNER PARTIDO",
    "CF2": "CÓRNER PARTIDO",
    "CF3": "CÓRNER PARTIDO",
    "CF4": "CÓRNER PARTIDO",
    "CF4V2": "CÓRNER PARTIDO",
    "CH3": "CÓRNER EN LA 1ª MITAD",
    "CH4": "CÓRNER EN LA 1ª MITAD",
    "CH5": "CÓRNER EN LA 1ª MITAD",
}

SUBTITULOS_BONITOS = {
    "UGM": "🎯 Buscar un gol más",
    "LJ2": "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "LIVEJ1": "🎯 Entrada asiática 0.5/1 en la 1ª mitad",
    "A1.1": "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "C1.1": "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "A0.1": "🎯 Buscar over 0.5 gol en la 1ª mitad",
    "A3.0": "🎯 Buscar 1 gol en el partido",
    "HT2": "🎯 Buscar gol antes del 75' con línea asiática +1",
    "L5.2": "🎯 Buscar 1 gol en la 2ª mitad antes del 75'",
    "QB": "🎯 Entrada asiática +1 en el partido",
    "CM01": "🎯 Entrada asiática +1 en el partido",
    "CM01V2": "🎯 Entrada asiática +1 en el partido",
    "CM01V3": "🎯 Entrada asiática +1 en el partido",
    "CM01V4": "🎯 Entrada asiática +1 en el partido",
    "CM02V2": "🎯 Entrada asiática +1 en el partido",
    "CM02V4": "🎯 Entrada asiática +1 en el partido",
    "CM06V3": "🎯 Entrada asiática +1 en el partido",
    "CM07V2": "🎯 Entrada asiática +1 en el partido",
    "GFT1": "🎯 Entrada asiática 0.5/1 en el partido",
    "GFT2": "🎯 Entrada asiática 0.5/1 en el partido",
    "PRE_O25FT": "🎯 Buscar over 2.5 goles prepartido",
    "CF1": "🎯 Buscar 1 córner más en el partido",
    "CF2": "🎯 Buscar 1 córner más en el partido",
    "CF3": "🎯 Buscar 1 córner más en el partido",
    "CF4": "🎯 Buscar 1 córner más en el partido",
    "CF4V2": "🎯 Buscar 1 córner más en el partido",
    "CH3": "🎯 Buscar 1 córner más en la 1ª mitad",
    "CH4": "🎯 Buscar 1 córner más en la 1ª mitad",
    "CH5": "🎯 Buscar 1 córner más en la 1ª mitad",
}


def obtener_titulo_bonito(datos: dict, tipo_pick: str) -> str:
    codigo = normalizar_codigo(datos.get("codigo") or "")

    if codigo in TITULOS_BONITOS:
        return TITULOS_BONITOS[codigo]

    periodo = detectar_periodo_por_codigo(datos)

    if periodo == "2H":
        return "GOL 2ª MITAD"

    if periodo == "HT" and tipo_pick == "gol":
        return "GOL EN LA 1ª MITAD"

    if periodo == "FT" and tipo_pick == "gol":
        return "GOL PARTIDO"

    if periodo == "HT" and tipo_pick == "corner":
        return "CÓRNER EN LA 1ª MITAD"

    if periodo == "FT" and tipo_pick == "corner":
        return "CÓRNER PARTIDO"

    return "PICK"


def obtener_subtitulo_bonito(datos: dict, tipo_pick: str) -> str:
    codigo = normalizar_codigo(datos.get("codigo") or "")

    if codigo in SUBTITULOS_BONITOS:
        base = SUBTITULOS_BONITOS[codigo]
        entrada = calcular_entrada_sugerida(datos, tipo_pick)
        if entrada:
            return f"{base}\n{entrada}"
        return base

    entrada = calcular_entrada_sugerida(datos, tipo_pick)
    if entrada:
        return entrada

    return ""


def obtener_equipo_principal(datos: dict) -> str:
    partido = datos.get("partido") or ""
    if " vs " in partido.lower():
        partes = re.split(r"\s+vs\s+", partido, flags=re.IGNORECASE)
        if partes:
            return partes[0].strip()
    return partido.strip()


def obtener_resumen_linea(datos: dict, tipo_pick: str) -> str:
    periodo = detectar_periodo_por_codigo(datos)
    modo = detectar_modo_por_codigo(datos)
    linea = detectar_linea_por_codigo(datos)
    condicion = detectar_condicion_por_codigo(datos)

    if tipo_pick == "corner":
        total_corners = parse_marcador_total(datos.get("corners"))
        if total_corners is not None and (linea == "+1" or modo in ["SINGLE", "ASIAN+1", "ASIAN"]):
            return f"Línea {total_corners + 1}"

    if tipo_pick == "gol":
        total_goles = parse_marcador_total(datos.get("goles"))
        if total_goles is None:
            return ""

        if modo == "NEXTGOAL":
            return f"+{total_goles + 0.5}"

        if modo == "ASIAN0.5-1":
            linea_asian = f"{total_goles + 0.5}/{total_goles + 1}"
            if condicion:
                return f"Asian {linea_asian} · {condicion}"
            return f"Asian {linea_asian}"

        if modo == "ASIAN+1" or (modo == "ASIAN" and linea == "+1"):
            linea_asian = f"{total_goles + 0.5}/{total_goles + 1}"
            if condicion:
                return f"Asian {linea_asian} · {condicion}"
            return f"Asian {linea_asian}"

        if modo == "SINGLE" and linea == "OVER0.5":
            return "+0.5"

        if modo == "SINGLE" and linea == "OVER2.5":
            return "Over 2.5"

        if modo == "LIVE" and linea == "+1":
            if condicion:
                return f"+1 · {condicion}"
            return "+1"

    return ""


def obtener_titulo_resumen(datos, tipo_pick):
    partido = datos.get("partido") or ""
    periodo = detectar_periodo_por_codigo(datos)
    fase = detectar_fase_por_codigo(datos)
    linea = detectar_linea_por_codigo(datos)
    linea_resumen = obtener_resumen_linea(datos, tipo_pick)

    icono = "⚽" if tipo_pick == "gol" else "🚩"

    # Caso especial PREPARTIDO OVER 2.5
    if fase == "PRE" and tipo_pick == "gol" and linea == "OVER2.5":
        if partido:
            return f"{icono} PREPARTIDO OVER 2.5 FT | {partido}"
        return f"{icono} PREPARTIDO OVER 2.5 FT"

    # Caso general
    bloque_tipo = f"{icono} GOAL {periodo}" if tipo_pick == "gol" else f"{icono} CORNER {periodo}"

    partes = [bloque_tipo]

    if partido:
        partes.append(partido)

    if linea_resumen:
        partes.append(linea_resumen)

    return " | ".join(partes)


def construir_mensaje_base(datos: dict, tipo_pick: str, para_free=False) -> str:
    titulo_resumen = html.escape(obtener_titulo_resumen(datos, tipo_pick))
    titulo_bonito = html.escape(obtener_titulo_bonito(datos, tipo_pick))
    subtitulo = obtener_subtitulo_bonito(datos, tipo_pick)

    lineas = [f"<b>{titulo_resumen}</b>", "──────────────"]

    if titulo_bonito and titulo_bonito.lower() not in titulo_resumen.lower():
        lineas.append(f"<b>{titulo_bonito}</b>")
        lineas.append("")

    if subtitulo:
        subtitulo = html.escape(subtitulo)
        lineas.append(subtitulo)
        lineas.append("")

    if datos.get("picks") is not None:
        lineas.append(f"📦 Historial: <b>{datos['picks']} picks</b>")

    if datos.get("liga"):
        lineas.append(f"🏆 Liga: <b>{html.escape(datos['liga'])}</b>")

    if datos.get("partido"):
        lineas.append(f"⚽ Partido: <b>{html.escape(datos['partido'])}</b>")

    if datos.get("kickoff"):
        kickoff_txt = datos["kickoff"]
        kickoff_txt = kickoff_txt.replace("In ", "en ")
        kickoff_txt = kickoff_txt.replace(" hours", " horas")
        kickoff_txt = kickoff_txt.replace(" hour", " hora")
        kickoff_txt = kickoff_txt.replace(" minutes", " minutos")
        kickoff_txt = kickoff_txt.replace(" minute", " minuto")
        lineas.append(f"⌛ Comienzo: <b>{html.escape(kickoff_txt)}</b>")

    lineas.append("")

    if datos.get("minuto") is not None:
        lineas.append(f"⏱ Minuto: <b>{datos['minuto']}</b>")
    elif datos.get("estado_partido"):
        estado = datos["estado_partido"]
        if estado == "DESCANSO":
            lineas.append("⏸ Estado: <b>Descanso</b>")
        elif estado == "FINALIZADO":
            lineas.append("🏁 Estado: <b>Finalizado</b>")
        elif estado == "2ª MITAD":
            lineas.append("⏱ Estado: <b>2ª mitad</b>")
        elif estado == "1ª MITAD":
            lineas.append("⏱ Estado: <b>1ª mitad</b>")

    if datos.get("goles"):
        if tipo_pick == "gol":
            lineas.append(f"🥅 Goles: <b>{html.escape(datos['goles'])}</b>")
        else:
            lineas.append(f"🥅 Goles: {html.escape(datos['goles'])}")

    if datos.get("corners"):
        if tipo_pick == "corner":
            lineas.append(f"🚩 Córners: <b>{html.escape(datos['corners'])}</b>")
        else:
            lineas.append(f"🚩 Córners: {html.escape(datos['corners'])}")

    if datos.get("momentum"):
        lineas.append(f"📈 Momentum: {html.escape(datos['momentum'])}")

    if datos.get("odds_1x2"):
        odds_formateadas = " | ".join(datos["odds_1x2"].split())
        lineas.append(f"📊 Cuotas prepartido 1X2: {html.escape(odds_formateadas)}")

    lineas.append("")

    if datos.get("strike_alerta"):
        strike_alerta = str(datos["strike_alerta"]).replace("%", "")
        lineas.append(f"📊 Acierto alerta: <b>{strike_alerta}%</b>")

    if datos.get("strike_liga"):
        strike_liga = str(datos["strike_liga"])
        if strike_liga.upper() == "N/A":
            lineas.append("📈 Acierto liga: <b>N/A</b>")
        else:
            strike_liga = strike_liga.replace("%", "")
            lineas.append(f"📈 Acierto liga: <b>{strike_liga}%</b>")

    if para_free:
        lineas.append("")
        lineas.append("💎 Si quieres más picks en directo, contáctame por privado.")

    return "\n".join(lineas)


def construir_texto_resultado(datos: dict, tipo_pick: str) -> str:
    resultado = datos.get("resultado")

    if not resultado:
        return ""

    if resultado == "HIT":
        linea_resultado = "✅ <b>Resultado: Hit</b>"
    elif resultado == "MISS":
        linea_resultado = "❌ <b>Resultado: Miss</b>"
    elif resultado == "VOID":
        linea_resultado = "⚪ <b>Resultado: Nulo</b>"
    else:
        linea_resultado = f"ℹ️ <b>Resultado: {html.escape(str(resultado))}</b>"

    extras = [linea_resultado]

    if datos.get("marcador_final"):
        extras.append(f"📌 Marcador final: <b>{html.escape(datos['marcador_final'])}</b>")
    elif datos.get("marcador_descanso"):
        extras.append(f"📌 Marcador descanso: <b>{html.escape(datos['marcador_descanso'])}</b>")

    return "\n".join(extras)


def construir_mensaje_editado(mensaje_original_publicado: str, datos: dict, tipo_pick: str) -> str:
    bloque_resultado = construir_texto_resultado(datos, tipo_pick)

    if not bloque_resultado:
        return mensaje_original_publicado

    if "Resultado:" in mensaje_original_publicado:
        return mensaje_original_publicado

    return f"{mensaje_original_publicado}\n\n{bloque_resultado}"


# ==============================
# ESTADÍSTICAS / RESÚMENES
# ==============================


def registrar_pick_estadistica(message_id_origen, datos, tipo_pick):
    registro = {
        "message_id_origen": str(message_id_origen),
        "codigo": datos.get("codigo"),
        "tipo_pick": tipo_pick,
        "liga": datos.get("liga"),
        "partido": datos.get("partido"),
        "strike_alerta": datos.get("strike_alerta"),
        "strike_liga": datos.get("strike_liga"),
        "resultado": None,
        "fecha": hoy_str(),
        "fecha_hora": ahora_str(),
    }
    STATE["estadisticas"].append(registro)


def actualizar_resultado_estadistica(message_id_origen, resultado):
    for item in STATE["estadisticas"]:
        if str(item.get("message_id_origen")) == str(message_id_origen):
            item["resultado"] = resultado
            break


def filtrar_estadisticas_hoy():
    hoy = hoy_str()
    return [x for x in STATE["estadisticas"] if x.get("fecha") == hoy]


def filtrar_estadisticas_semana():
    ahora = datetime.now()
    inicio_semana = ahora - timedelta(days=ahora.weekday())
    inicio_semana_str = inicio_semana.strftime("%Y-%m-%d")
    return [x for x in STATE["estadisticas"] if x.get("fecha") >= inicio_semana_str]


def construir_resumen(lista, titulo):
    total = len(lista)
    hits = sum(1 for x in lista if x.get("resultado") == "HIT")
    miss = sum(1 for x in lista if x.get("resultado") == "MISS")
    voids = sum(1 for x in lista if x.get("resultado") == "VOID")
    pendientes = total - hits - miss - voids

    goles = [x for x in lista if x.get("tipo_pick") == "gol"]
    corners = [x for x in lista if x.get("tipo_pick") == "corner"]

    goles_hits = sum(1 for x in goles if x.get("resultado") == "HIT")
    goles_miss = sum(1 for x in goles if x.get("resultado") == "MISS")
    goles_void = sum(1 for x in goles if x.get("resultado") == "VOID")

    corners_hits = sum(1 for x in corners if x.get("resultado") == "HIT")
    corners_miss = sum(1 for x in corners if x.get("resultado") == "MISS")
    corners_void = sum(1 for x in corners if x.get("resultado") == "VOID")

    resueltos = hits + miss
    strike = round((hits / resueltos) * 100, 1) if resueltos > 0 else 0

    lineas = [
        f"📊 {titulo}",
        "",
        f"Total picks: {total}",
        f"✅ Hits: {hits}",
        f"❌ Miss: {miss}",
        f"⚪ Nulos: {voids}",
        f"⏳ Pendientes: {pendientes}",
        f"📈 Strike: {strike}%",
        "",
        f"⚽ Goles: {len(goles)} | ✅ {goles_hits} | ❌ {goles_miss} | ⚪ {goles_void}",
        f"🚩 Corners: {len(corners)} | ✅ {corners_hits} | ❌ {corners_miss} | ⚪ {corners_void}",
    ]

    return "\n".join(lineas)


async def publicar_resumen_diario_si_toca(context: ContextTypes.DEFAULT_TYPE):
    control = STATE["resumen_control"]
    hoy = hoy_str()
    hora_actual = datetime.now().hour

    # hora mínima para publicar resumen
    if hora_actual < 22:
        return

    # evitar publicar dos veces el mismo día
    if control["ultimo_resumen_dia"] == hoy:
        return

    lista = filtrar_estadisticas_hoy()
    if not lista:
        return

    texto = construir_resumen(lista, "📊 RESUMEN DEL DÍA")

    for canal_id in CANALES_RESUMEN:
        try:
            await context.bot.send_message(
                chat_id=canal_id,
                text=texto
            )
            print(f"✅ Resumen diario enviado a {canal_id}")
        except Exception as e:
            print(f"❌ Error enviando resumen a {canal_id}: {e}")

    control["ultimo_resumen_dia"] = hoy
    save_state()


async def publicar_resumen_semanal_si_toca(context: ContextTypes.DEFAULT_TYPE):
    control = STATE["resumen_control"]
    semana = semana_str()
    ahora = datetime.now()

    if ahora.weekday() != 6 or ahora.hour < 23:
        return

    if control["ultimo_resumen_semana"] == semana:
        return

    lista = filtrar_estadisticas_semana()
    if not lista:
        return

    texto = construir_resumen(lista, "RESUMEN SEMANAL")

    enviados_ok = 0
    for canal_id in CANALES_RESUMEN:
        try:
            await context.bot.send_message(chat_id=canal_id, text=texto)
            enviados_ok += 1
            print(f"✅ Resumen semanal publicado en {canal_id}")
        except Exception as e:
            print(f"❌ Error publicando resumen semanal en {canal_id}: {e}")

    if enviados_ok > 0:
        control["ultimo_resumen_semana"] = semana
        save_state()


# ==============================
# ENVÍO / EDICIÓN
# ==============================


async def enviar_mensaje(context: ContextTypes.DEFAULT_TYPE, canal_id: int, texto: str):
    return await context.bot.send_message(
        chat_id=canal_id,
        text=texto,
        parse_mode="HTML"
    )


async def editar_mensaje(context: ContextTypes.DEFAULT_TYPE, canal_id: int, message_id: int, texto_nuevo: str):
    try:
        await context.bot.edit_message_text(
            chat_id=canal_id,
            message_id=message_id,
            text=texto_nuevo,
            parse_mode="HTML"
        )
        print(f"✏️ Mensaje editado en canal/grupo {canal_id}")
    except Exception as e:
        print(f"❌ Error editando mensaje en {canal_id}: {e}")


# ==============================
# PROCESAR NUEVO
# ==============================


async def procesar_nuevo_mensaje(mensaje, context: ContextTypes.DEFAULT_TYPE):
    texto = mensaje.text or mensaje.caption or ""
    chat_id = mensaje.chat_id
    message_id_origen = mensaje.message_id

    if chat_id != CANAL_ORIGEN_ID:
    	print(f"⛔ Ignorado por origen incorrecto | recibido: {chat_id} | esperado: {CANAL_ORIGEN_ID}")
    	return

    datos = extraer_datos(texto)
	print("DEBUG DATOS EXTRAÍDOS:")
	print(datos)

    if not pasa_filtro_strike_liga(datos):
    	print("⛔ Ignorado por filtro strike liga")
    	return

    tipo_pick = detectar_tipo_pick_por_codigo(datos)

    if not tipo_pick:
    	print("⛔ Ignorado: no se detecta GOAL/CORNER por código")
    	print(f"meta_alerta: {datos.get('meta_alerta')}")
    	print(f"codigo: {datos.get('codigo')}")
    	print("-" * 60)
    	return

    canales_destino = []

    if tipo_pick == "corner":
        canales_destino.append(CANAL_CORNERS_ID)

    elif tipo_pick == "gol":
          canales_destino.append(CANAL_GOLES_ID)

    if ENVIAR_A_GENERAL:
          canales_destino.append(CANAL_GENERAL_ID)

    mensaje_limpio = construir_mensaje_base(datos, tipo_pick)

    print("\n" + "=" * 60)
    print("NUEVO MENSAJE")
    print(f"Tipo: {tipo_pick}")
    print(f"Origen msg_id: {message_id_origen}")
    print(mensaje_limpio)
    print(f"Destinos: {canales_destino}")

    destinos_publicados = {}

    for canal_id in canales_destino:
        try:
            enviado = await enviar_mensaje(context, canal_id, mensaje_limpio)
            destinos_publicados[str(canal_id)] = enviado.message_id
            print(f"✅ Enviado a {canal_id} (msg {enviado.message_id})")
        except Exception as e:
            print(f"❌ Error enviando a {canal_id}: {e}")

    ok_free, motivo_free = debe_enviar_a_free(tipo_pick, datos)
    if ok_free:
        try:
            mensaje_free = construir_mensaje_base(datos, tipo_pick, para_free=True)
            enviado_free = await enviar_mensaje(context, CANAL_FREE_ID, mensaje_free)
            destinos_publicados[str(CANAL_FREE_ID)] = enviado_free.message_id
            registrar_envio_free(tipo_pick, datos)
            print(f"✅ Enviado a FREE {CANAL_FREE_ID} (msg {enviado_free.message_id})")
        except Exception as e:
            print(f"❌ Error enviando a FREE: {e}")
    else:
        print(f"ℹ️ No enviado a FREE: {motivo_free}")

    STATE["mensajes_publicados"][str(message_id_origen)] = {
        "tipo_pick": tipo_pick,
        "mensaje_base": mensaje_limpio,
        "mensaje_base_free": construir_mensaje_base(datos, tipo_pick, para_free=True),
        "destinos": destinos_publicados,
    }

    registrar_pick_estadistica(message_id_origen, datos, tipo_pick)
    save_state()

    await publicar_resumen_diario_si_toca(context)
    await publicar_resumen_semanal_si_toca(context)

    print("=" * 60 + "\n")


# ==============================
# PROCESAR EDITADO
# ==============================


async def procesar_mensaje_editado(mensaje, context: ContextTypes.DEFAULT_TYPE):
    texto = mensaje.text or mensaje.caption or ""
    chat_id = mensaje.chat_id
    message_id_origen = mensaje.message_id

    if chat_id != CANAL_ORIGEN_ID:
        return

    key = str(message_id_origen)
    if key not in STATE["mensajes_publicados"]:
        print("⛔ Editado ignorado: no tenemos referencia del original")
        print("-" * 60)
        return

    registro = STATE["mensajes_publicados"][key]
    tipo_pick = registro["tipo_pick"]
    destinos = registro["destinos"]

    datos = extraer_datos(texto)

    if not datos.get("resultado"):
        print("ℹ️ Editado detectado, pero sin Hit/Miss todavía")
        print("-" * 60)
        return

    print("\n" + "=" * 60)
    print("MENSAJE EDITADO")
    print(f"Tipo: {tipo_pick}")
    print(f"Resultado: {datos.get('resultado')}")

    for canal_id, message_id_publicado in destinos.items():
        if str(canal_id) == str(CANAL_FREE_ID):
            base = registro["mensaje_base_free"]
        else:
            base = registro["mensaje_base"]

        texto_editado = construir_mensaje_editado(base, datos, tipo_pick)
        await editar_mensaje(context, int(canal_id), message_id_publicado, texto_editado)

    actualizar_resultado_estadistica(message_id_origen, datos.get("resultado"))
    save_state()

    await publicar_resumen_diario_si_toca(context)
    await publicar_resumen_semanal_si_toca(context)

    print("=" * 60 + "\n")


# ==============================
# COMANDOS DE PRUEBA
# ==============================


async def resumen_hoy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lista = filtrar_estadisticas_hoy()
    texto = construir_resumen(lista, "RESUMEN DEL DÍA")
    await update.message.reply_text(texto)


async def resumen_semana(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lista = filtrar_estadisticas_semana()
    texto = construir_resumen(lista, "RESUMEN SEMANAL")
    await update.message.reply_text(texto)


# ==============================
# HANDLERS
# ==============================


async def handler_nuevo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.channel_post:
        await procesar_nuevo_mensaje(update.channel_post, context)


async def handler_editado(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.edited_channel_post:
        await procesar_mensaje_editado(update.edited_channel_post, context)


# ==============================
# ARRANQUE
# ==============================

if __name__ == "__main__":
    load_state()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("resumen_hoy", resumen_hoy))
    app.add_handler(CommandHandler("resumen_semana", resumen_semana))

    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, handler_nuevo))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_CHANNEL_POST, handler_editado))

    print("Bot escuchando mensajes nuevos y editados...")
    app.run_polling(drop_pending_updates=True)