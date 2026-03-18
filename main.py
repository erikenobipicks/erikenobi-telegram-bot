import logging
import sys

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from config import TOKEN
from state import load_state
from handlers import (
    handler_nuevo,
    handler_editado,
    cmd_resumen_hoy,
    cmd_resumen_semana,
)


# ==============================
# LOGGING
# ==============================

def configurar_logging() -> None:
    """
    Configura el sistema de logging:
    - Nivel INFO en consola (con formato legible).
    - Nivel DEBUG en archivo bot.log (con timestamp completo).
    - Silencia los logs verbosos de httpx y telegram internos.
    """
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

    # Silenciar librerías externas demasiado verbosas
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

    load_state()

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("resumen_hoy",    cmd_resumen_hoy))
    app.add_handler(CommandHandler("resumen_semana", cmd_resumen_semana))
    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST,        handler_nuevo))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_CHANNEL_POST, handler_editado))

    logger.info("Bot iniciado — escuchando mensajes nuevos y editados.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
