import logging
import sys

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from config import TOKEN
from state import load_state, STATE
from db import init_db, migrar_desde_json
from handlers import (
    handler_nuevo,
    handler_editado,
    cmd_resumen_hoy,
    cmd_resumen_semana,
    cmd_resumen_mes,
)


# ==============================
# LOGGING
# ==============================

def configurar_logging() -> None:
    fmt_consola = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    fmt_archivo = "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"

    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ]
    handlers[0].setLevel(logging.INFO)
    handlers[0].setFormatter(logging.Formatter(fmt_consola, datefmt=date_fmt))
    handlers[1].setLevel(logging.DEBUG)
    handlers[1].setFormatter(logging.Formatter(fmt_archivo, datefmt=date_fmt))

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in handlers:
        root.addHandler(h)

    for lib in ("httpx", "httpcore", "telegram.ext", "apscheduler"):
        logging.getLogger(lib).setLevel(logging.WARNING)


# ==============================
# ARRANQUE
# ==============================

def main() -> None:
    configurar_logging()
    logger = logging.getLogger(__name__)

    if not TOKEN:
        logger.critical("BOT_TOKEN no definido. Exporta la variable de entorno y vuelve a intentarlo.")
        sys.exit(1)

    # Inicializar DB (crea tablas si no existen)
    try:
        init_db()
    except Exception as e:
        logger.critical(f"No se pudo conectar a la base de datos: {e}")
        sys.exit(1)

    # Cargar estado volátil desde disco
    load_state()

    # Migración automática: si el JSON tiene estadísticas del sistema anterior,
    # las importamos a la DB y las vaciamos del JSON para no duplicar.
    estadisticas_json = STATE.get("estadisticas", [])
    if estadisticas_json:
        logger.info(f"Migrando {len(estadisticas_json)} picks del JSON a PostgreSQL...")
        migrar_desde_json(estadisticas_json)
        STATE["estadisticas"] = []   # limpiar para no volver a migrar
        from state import save_state
        save_state()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("resumen_hoy",    cmd_resumen_hoy))
    app.add_handler(CommandHandler("resumen_semana", cmd_resumen_semana))
    app.add_handler(CommandHandler("resumen_mes",    cmd_resumen_mes))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST,        handler_nuevo))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_CHANNEL_POST, handler_editado))

    logger.info("Bot iniciado — escuchando mensajes nuevos y editados.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
