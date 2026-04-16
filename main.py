import datetime
import logging
import sys

from zoneinfo import ZoneInfo

from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
)

from config import TOKEN
from state import load_state, STATE
from db import init_db, init_pool, migrar_desde_json
from handlers import (
    handler_nuevo,
    handler_editado,
    limpiar_mensajes_publicados,
    purgar_alertas_recientes,
    cmd_resumen_hoy,
    cmd_resumen_semana,
    cmd_resumen_mes,
    cmd_resumen_anual,
    cmd_resumen_liga,
    cmd_resumen_pre,
    cmd_resumen_codigo,
    cmd_analisis_filtros,
    cmd_resultado,
    cmd_resultado_corner,
    cmd_bankroll,
    handler_excel_bankroll,
)
from estadisticas import (
    publicar_resumen_diario_si_toca,
    publicar_resumen_semanal_si_toca,
    publicar_resumen_mensual_si_toca,
    notificar_picks_pendientes_si_toca,
    notificar_picks_pendientes_retrasados,
)

_TZ_MADRID = ZoneInfo("Europe/Madrid")


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

    # Inicializar pool de conexiones y luego las tablas
    try:
        init_pool()
        init_db()
    except Exception as e:
        logger.critical(f"No se pudo conectar a la base de datos: {e}")
        sys.exit(1)

    # Cargar estado volátil desde disco
    load_state()

    # Limpieza preventiva: recortar STATE si creció demasiado entre reinicios
    limpiar_mensajes_publicados()
    from state import save_state as _save
    _save()

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

    # Resúmenes estándar
    app.add_handler(CommandHandler("resumen_hoy",    cmd_resumen_hoy))
    app.add_handler(CommandHandler("resumen_semana", cmd_resumen_semana))
    app.add_handler(CommandHandler("resumen_mes",    cmd_resumen_mes))

    # Nuevos resúmenes
    app.add_handler(CommandHandler("resumen_anual",  cmd_resumen_anual))
    app.add_handler(CommandHandler("resumen_liga",   cmd_resumen_liga))
    app.add_handler(CommandHandler("resumen_pre",    cmd_resumen_pre))
    app.add_handler(CommandHandler("resumen_codigo", cmd_resumen_codigo))
    app.add_handler(CommandHandler("analisis_filtros", cmd_analisis_filtros))

    # Corrección manual de resultados (solo admins)
    app.add_handler(CommandHandler("resultado",  cmd_resultado))
    app.add_handler(CommandHandler("resultado_corner", cmd_resultado_corner))

    # Bankroll (solo admins)
    app.add_handler(CommandHandler("bankroll",   cmd_bankroll))
    app.add_handler(MessageHandler(
        filters.ChatType.PRIVATE & filters.Document.MimeType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        handler_excel_bankroll,
    ))

    app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST,        handler_nuevo))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_CHANNEL_POST, handler_editado))

    # ── Jobs programados — resúmenes automáticos ─────────────────────
    # Se disparan a hora fija en zona horaria Madrid, independientemente
    # de si llegan alertas. La lógica _debe_publicar_ahora() ya garantiza
    # que no se dupliquen si la alerta los disparó antes.

    async def job_resumen_diario(ctx):
        await publicar_resumen_diario_si_toca(ctx)

    async def job_resumen_semanal(ctx):
        await publicar_resumen_semanal_si_toca(ctx)

    async def job_resumen_mensual(ctx):
        await publicar_resumen_mensual_si_toca(ctx)

    async def job_revision_pendientes(ctx):
        await notificar_picks_pendientes_si_toca(ctx)

    async def job_revision_pendientes_retrasados(ctx):
        await notificar_picks_pendientes_retrasados(ctx)

    async def job_limpiar_state(ctx):
        limpiar_mensajes_publicados()
        from state import save_state as _sv
        _sv()

    async def job_purgar_alertas(ctx):
        """Purga alertas_recientes antiguas aunque no lleguen picks nuevos."""
        purgar_alertas_recientes()
        from state import save_state as _sv
        _sv()

    # Diario: 23:45 todos los días
    app.job_queue.run_daily(
        callback = job_resumen_diario,
        time     = datetime.time(23, 45, tzinfo=_TZ_MADRID),
        name     = "resumen_diario",
    )
    # Semanal: 23:45 los lunes (publica el resumen de la semana anterior ya cerrada)
    app.job_queue.run_daily(
        callback = job_resumen_semanal,
        time     = datetime.time(23, 45, tzinfo=_TZ_MADRID),
        days     = (0,),   # 0 = lunes
        name     = "resumen_semanal",
    )
    # Mensual: 09:00 el día 1 de cada mes
    # (_debe_publicar_ahora comprueba que sea día 1)
    app.job_queue.run_daily(
        callback = job_resumen_mensual,
        time     = datetime.time(9, 0, tzinfo=_TZ_MADRID),
        name     = "resumen_mensual",
    )
    app.job_queue.run_daily(
        callback = job_revision_pendientes,
        time     = datetime.time(8, 0, tzinfo=_TZ_MADRID),
        name     = "revision_pendientes",
    )
    app.job_queue.run_repeating(
        callback = job_revision_pendientes_retrasados,
        interval = 6 * 60 * 60,
        first    = 60,
        name     = "revision_pendientes_retrasados",
    )
    # Limpieza preventiva de STATE cada 12 horas
    app.job_queue.run_repeating(
        callback = job_limpiar_state,
        interval = 12 * 60 * 60,
        first    = 300,
        name     = "limpiar_state",
    )
    # Purga de alertas_recientes antiguas cada 4 horas (evita crecimiento
    # del dict en noches/periodos sin actividad)
    app.job_queue.run_repeating(
        callback = job_purgar_alertas,
        interval = 4 * 60 * 60,
        first    = 120,
        name     = "purgar_alertas",
    )

    logger.info("Bot iniciado — escuchando mensajes nuevos y editados.")
    app.run_polling()


if __name__ == "__main__":
    main()
