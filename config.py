import os

# ==============================
# TOKEN
# ==============================

TOKEN = os.getenv("BOT_TOKEN")

# ==============================
# CANALES
# ==============================

CANAL_ORIGEN_ID = -1003876204382   # Origen PREMIUM

CANAL_PRUEBAS_ID  = -1002037791209
CANAL_CORNERS_ID  = -1003895151594
CANAL_GOLES_ID    = -1003818905455
CANAL_GENERAL_ID  = -1002037791209
CANAL_FREE_ID     = -1002973101273
CANAL_RESUMEN_ID  = CANAL_PRUEBAS_ID

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

# Solo se publican picks de estas ligas si su Strike Liga >= el mínimo indicado
FILTRO_STRIKE_LIGA = {
    "UGM": 65,
    "LJ2": 65,
}

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
    "estadisticas": [],
    "resumen_control": {
        "ultimo_resumen_dia": None,
        "ultimo_resumen_semana": None,
    },
}
