import os

# ==============================
# TOKEN
# ==============================

TOKEN = os.getenv("BOT_TOKEN")

# ==============================
# ADMIN
# ==============================
# IDs de Telegram con acceso al comando /resultado
ADMIN_IDS = [9330181]

# ==============================
# CANALES
# ==============================

CANAL_ORIGEN_ID  = -1003876204382   # Origen PREMIUM

CANAL_PRUEBAS_ID = -1002037791209
CANAL_CORNERS_ID = -1003895151594
CANAL_GOLES_ID   = -1003818905455
CANAL_GENERAL_ID = -1002037791209
CANAL_FREE_ID    = -1002973101273
CANAL_PRE_ID     = -1003774898516  # Canal prepartido

ENVIAR_A_GENERAL = True

# ==============================
# CANAL FREE — CUPOS Y HORARIO
# ==============================

MAX_FREE_GOLES   = 2
MAX_FREE_CORNERS = 2
MAX_FREE_TOTAL   = 4

FREE_TIMEZONE    = "Europe/Madrid"
FREE_HORA_INICIO = 10
FREE_HORA_FIN    = 22

# ==============================
# FILTROS POR CÓDIGO
# ==============================

FILTRO_STRIKE_LIGA = {
    "UGM": 65,
    "LJ2": 65,
    "CF3": 75,
    "CH3": 75,
}

# ==============================
# ANTI-DUPLICADO POR PARTIDO
# ==============================
#
# Bloquea un pick si en los últimos DUPLICADO_VENTANA_MINUTOS minutos
# ya se envió uno del mismo partido + mismo periodo + mismo tipo.

DUPLICADO_VENTANA_MINUTOS = 15

# ==============================
# FILTRO FIN DE SEMANA — CORNERS
# ==============================
#
# Solo sábado (weekday=5), franja 17-22h.
# Si el strike_alerta de un corner es inferior al mínimo, se descarta.

FINDE_CORNER_HORA_INICIO  = 17
FINDE_CORNER_HORA_FIN     = 22
FINDE_CORNER_STRIKE_MIN   = 85   # strike_alerta mínimo (%)

# ==============================
# RACHA — NOTIFICACIONES
# ==============================
#
# Se notifica cuando la racha de HITs consecutivos (ignorando VOIDs)
# alcanza un múltiplo de RACHA_MINIMA: 5, 10, 15...
# La notificación se envía al canal CANAL_RACHA_ID.

RACHA_MINIMA   = 5
CANAL_RACHA_ID = CANAL_GENERAL_ID   # cambia si quieres un canal específico

# ==============================
# RESÚMENES — DESTINOS
# ==============================
#
# tipo_pick:
#   "gol"    → solo picks de goles
#   "corner" → solo picks de corners
#   None     → todos (goles + corners)
#   "free"   → solo picks enviados al canal FREE

RESUMENES_CONFIG = [
    {
        "id":        "goles",
        "canal_id":  CANAL_GOLES_ID,
        "tipo_pick": "gol",
        "label":     "⚽ GOLES",
    },
    {
        "id":        "corners",
        "canal_id":  CANAL_CORNERS_ID,
        "tipo_pick": "corner",
        "label":     "🚩 CORNERS",
    },
    {
        "id":        "general",
        "canal_id":  CANAL_GENERAL_ID,
        "tipo_pick": None,
        "label":     "📊 GENERAL",
    },
    {
        "id":        "free_picks",
        "canal_id":  CANAL_FREE_ID,
        "tipo_pick": "free",
        "label":     "🆓 FREE — Picks del canal",
    },
    {
        "id":        "free_pago",
        "canal_id":  CANAL_FREE_ID,
        "tipo_pick": None,
        "label":     "💎 FREE — Picks premium (referencia)",
    },
]

# ==============================
# PERSISTENCIA
# ==============================

STATE_FILE = "bot_state.json"

# ==============================
# ESTADO INICIAL (plantilla)
# ==============================

DEFAULT_STATE = {
    "mensajes_publicados": {},
    "free_state": {
        "fecha": None,
        "goles_enviados": 0,
        "corners_enviados": 0,
        "ultimo_score_gol": -1,
        "ultimo_score_corner": -1,
        "ultima_hora_envio": None,
    },
    "alertas_recientes": {},
    "estadisticas": [],
    # resumen_control: dict plano con claves "{resumen_id}_{frecuencia}"
    # Ejemplo: {"goles_dia": "2025-03-18", "general_semana": "2025-W11", "corners_mes": "2025-03"}
    "resumen_control": {},
}
