import os
import logging

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


# ==============================
# CONEXIÓN
# ==============================

def get_conn():
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


# ==============================
# INICIALIZACIÓN
# ==============================

def init_db() -> None:
    """Crea las tablas si no existen. Seguro de ejecutar en cada arranque."""
    with get_conn() as conn:
        with conn.cursor() as cur:

            # Tabla principal de picks
            cur.execute("""
                CREATE TABLE IF NOT EXISTS picks (
                    id                BIGSERIAL PRIMARY KEY,
                    message_id_origen TEXT      NOT NULL UNIQUE,
                    codigo            TEXT,
                    tipo_pick         TEXT      NOT NULL,
                    liga              TEXT,
                    partido           TEXT,
                    strike_alerta     TEXT,
                    strike_liga       TEXT,
                    resultado         TEXT,
                    enviado_a_free    BOOLEAN   NOT NULL DEFAULT FALSE,
                    fecha             DATE      NOT NULL,
                    fecha_hora        TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """)

            # Índices útiles para filtrar por fecha y tipo
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_fecha
                ON picks (fecha);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_tipo
                ON picks (tipo_pick);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_free
                ON picks (enviado_a_free);
            """)

            # Control de resúmenes ya publicados (evita duplicados tras reinicios)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS resumen_control (
                    clave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                );
            """)

    logger.info("Base de datos inicializada correctamente.")


# ==============================
# PICKS — ESCRITURA
# ==============================

def db_registrar_pick(
    message_id_origen: str,
    codigo: str | None,
    tipo_pick: str,
    liga: str | None,
    partido: str | None,
    strike_alerta: str | None,
    strike_liga: str | None,
    enviado_a_free: bool,
    fecha: str,
    fecha_hora: str,
) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO picks (
                        message_id_origen, codigo, tipo_pick, liga, partido,
                        strike_alerta, strike_liga, resultado, enviado_a_free,
                        fecha, fecha_hora
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, %s, %s, %s)
                    ON CONFLICT (message_id_origen) DO NOTHING;
                """, (
                    message_id_origen, codigo, tipo_pick, liga, partido,
                    strike_alerta, strike_liga, enviado_a_free,
                    fecha, fecha_hora,
                ))
        logger.debug(f"Pick guardado en DB: {message_id_origen}")
    except Exception as e:
        logger.error(f"Error guardando pick en DB: {e}")


def db_actualizar_resultado(message_id_origen: str, resultado: str) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE picks
                    SET resultado = %s
                    WHERE message_id_origen = %s;
                """, (resultado, message_id_origen))
        logger.debug(f"Resultado actualizado en DB: {message_id_origen} → {resultado}")
    except Exception as e:
        logger.error(f"Error actualizando resultado en DB: {e}")


# ==============================
# PICKS — LECTURA / FILTROS
# ==============================

def db_picks_por_periodo(periodo: str) -> list[dict]:
    """
    Devuelve picks del período indicado.
    periodo: "dia" | "semana" | "mes_anterior"
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                if periodo == "dia":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE fecha = CURRENT_DATE AT TIME ZONE 'Europe/Madrid'
                        ORDER BY fecha_hora;
                    """)

                elif periodo == "semana":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE fecha >= date_trunc('week',
                            CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid'
                        )::date
                        ORDER BY fecha_hora;
                    """)

                elif periodo == "mes_anterior":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE date_trunc('month', fecha) =
                              date_trunc('month', CURRENT_DATE - INTERVAL '1 month')
                        ORDER BY fecha_hora;
                    """)

                else:
                    return []

                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error leyendo picks de DB ({periodo}): {e}")
        return []


# ==============================
# RESUMEN CONTROL — LECTURA/ESCRITURA
# ==============================

def db_ya_publicado(clave: str, valor: str) -> bool:
    """Devuelve True si ya se publicó el resumen con esa clave y ese valor."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT valor FROM resumen_control WHERE clave = %s;",
                    (clave,),
                )
                row = cur.fetchone()
                return row is not None and row["valor"] == valor
    except Exception as e:
        logger.error(f"Error leyendo resumen_control: {e}")
        return False


def db_marcar_publicado(clave: str, valor: str) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO resumen_control (clave, valor)
                    VALUES (%s, %s)
                    ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor;
                """, (clave, valor))
        logger.debug(f"Resumen marcado como publicado: {clave} = {valor}")
    except Exception as e:
        logger.error(f"Error marcando resumen_control: {e}")


# ==============================
# MIGRACIÓN DESDE JSON
# ==============================

def migrar_desde_json(estadisticas: list) -> int:
    """
    Importa picks del array estadisticas del bot_state.json a la base de datos.
    Devuelve el número de picks importados.
    Seguro de llamar múltiples veces (usa ON CONFLICT DO NOTHING).
    """
    importados = 0
    for item in estadisticas:
        try:
            db_registrar_pick(
                message_id_origen = str(item.get("message_id_origen", "")),
                codigo            = item.get("codigo"),
                tipo_pick         = item.get("tipo_pick", ""),
                liga              = item.get("liga"),
                partido           = item.get("partido"),
                strike_alerta     = item.get("strike_alerta"),
                strike_liga       = item.get("strike_liga"),
                enviado_a_free    = bool(item.get("enviado_a_free", False)),
                fecha             = item.get("fecha", ""),
                fecha_hora        = item.get("fecha_hora", item.get("fecha", "")),
            )
            # Actualizar resultado si ya lo tiene
            resultado = item.get("resultado")
            if resultado:
                db_actualizar_resultado(
                    str(item.get("message_id_origen", "")),
                    resultado,
                )
            importados += 1
        except Exception as e:
            logger.error(f"Error migrando pick {item.get('message_id_origen')}: {e}")

    logger.info(f"Migración completada: {importados} picks importados.")
    return importados
