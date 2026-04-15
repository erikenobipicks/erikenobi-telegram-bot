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
                    message_id_origen TEXT         NOT NULL UNIQUE,
                    codigo            TEXT,
                    tipo_pick         TEXT         NOT NULL,
                    periodo_codigo    TEXT,
                    modo_codigo       TEXT,
                    linea_codigo      TEXT,
                    liga              TEXT,
                    partido           TEXT,
                    strike_alerta     TEXT,
                    strike_liga       TEXT,
                    strike_alerta_pct INTEGER,
                    strike_liga_pct   INTEGER,
                    resultado         TEXT,
                    enviado_a_free    BOOLEAN      NOT NULL DEFAULT FALSE,
                    minuto_alerta     INTEGER,
                    goles_entrada_total INTEGER,
                    corners_entrada_total INTEGER,
                    red_cards_entrada_total INTEGER,
                    momentum_local    INTEGER,
                    momentum_visitante INTEGER,
                    mensaje_base      TEXT,
                    mensaje_base_free TEXT,
                    destinos_json     TEXT,
                    odds              NUMERIC(5,2),
                    fecha             DATE         NOT NULL,
                    fecha_hora        TIMESTAMP    NOT NULL DEFAULT NOW()
                );
            """)

            # Columna odds puede no existir en instalaciones anteriores
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS odds NUMERIC(5,2);
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS periodo_codigo TEXT;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS modo_codigo TEXT;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS linea_codigo TEXT;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS corners_entrada_total INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS strike_alerta_pct INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS strike_liga_pct INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS minuto_alerta INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS goles_entrada_total INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS red_cards_entrada_total INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS momentum_local INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS momentum_visitante INTEGER;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS mensaje_base TEXT;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS mensaje_base_free TEXT;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS destinos_json TEXT;
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
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_liga
                ON picks (liga);
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_codigo
                ON picks (codigo);
            """)

            # Control de resúmenes ya publicados (evita duplicados tras reinicios)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS resumen_control (
                    clave TEXT PRIMARY KEY,
                    valor TEXT NOT NULL
                );
            """)

            # Estado del canal FREE — persiste entre reinicios
            cur.execute("""
                CREATE TABLE IF NOT EXISTS free_state (
                    clave  TEXT PRIMARY KEY,
                    valor  TEXT NOT NULL
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
    periodo_codigo: str | None,
    modo_codigo: str | None,
    linea_codigo: str | None,
    liga: str | None,
    partido: str | None,
    strike_alerta: str | None,
    strike_liga: str | None,
    enviado_a_free: bool,
    strike_alerta_pct: int | None,
    strike_liga_pct: int | None,
    minuto_alerta: int | None,
    goles_entrada_total: int | None,
    corners_entrada_total: int | None,
    red_cards_entrada_total: int | None,
    momentum_local: int | None,
    momentum_visitante: int | None,
    fecha: str,
    fecha_hora: str,
    odds: float | None = None,
) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO picks (
                        message_id_origen, codigo, tipo_pick, periodo_codigo, modo_codigo,
                        linea_codigo, liga, partido,
                        strike_alerta, strike_liga, strike_alerta_pct, strike_liga_pct,
                        resultado, enviado_a_free, minuto_alerta, goles_entrada_total,
                        corners_entrada_total, red_cards_entrada_total,
                        momentum_local, momentum_visitante, odds, fecha, fecha_hora
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (message_id_origen) DO NOTHING;
                """, (
                    message_id_origen, codigo, tipo_pick, periodo_codigo, modo_codigo,
                    linea_codigo, liga, partido,
                    strike_alerta, strike_liga, strike_alerta_pct, strike_liga_pct,
                    enviado_a_free, minuto_alerta, goles_entrada_total,
                    corners_entrada_total, red_cards_entrada_total,
                    momentum_local, momentum_visitante, odds, fecha, fecha_hora,
                ))
        logger.debug(f"Pick guardado en DB: {message_id_origen}")
    except Exception as e:
        logger.error(f"Error guardando pick en DB: {e}")


def db_actualizar_resultado(message_id_origen: str, resultado: str) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE picks
                    SET resultado = %s
                    WHERE message_id_origen = %s;
                """, (resultado, message_id_origen))
                actualizado = (cur.rowcount or 0) > 0
        logger.debug(f"Resultado actualizado en DB: {message_id_origen} → {resultado}")
        return actualizado
    except Exception as e:
        logger.error(f"Error actualizando resultado en DB: {e}")
        return False


# ==============================
# PICKS — LECTURA / FILTROS
# ==============================

def db_actualizar_resultado_confirmado(message_id_origen: str, resultado: str) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE picks
                    SET resultado = %s
                    WHERE message_id_origen = %s;
                    """,
                    (resultado, message_id_origen),
                )
                actualizado = (cur.rowcount or 0) > 0
        if actualizado:
            logger.debug(f"Resultado actualizado en DB: {message_id_origen} -> {resultado}")
        else:
            logger.warning(f"No se encontro el pick en DB para actualizar: {message_id_origen}")
        return actualizado
    except Exception as e:
        logger.error(f"Error actualizando resultado en DB: {e}")
        return False


def db_guardar_publicacion(
    message_id_origen: str,
    mensaje_base: str | None,
    mensaje_base_free: str | None,
    destinos_json: str | None,
) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE picks
                    SET mensaje_base = %s,
                        mensaje_base_free = %s,
                        destinos_json = %s
                    WHERE message_id_origen = %s;
                    """,
                    (mensaje_base, mensaje_base_free, destinos_json, message_id_origen),
                )
                actualizado = (cur.rowcount or 0) > 0
        if not actualizado:
            logger.warning(
                f"No se pudo guardar la publicacion en DB para msg_id {message_id_origen}"
            )
        return actualizado
    except Exception as e:
        logger.error(f"Error guardando publicacion en DB: {e}")
        return False


def db_picks_por_periodo(periodo: str) -> list[dict]:
    """
    Devuelve picks del período indicado.
    periodo: "dia" | "ayer" | "semana" | "semana_anterior" | "mes_anterior" | "anio"
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                if periodo == "dia":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE fecha = (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date
                        ORDER BY fecha_hora;
                    """)

                elif periodo == "ayer":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE fecha = (
                            (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date - INTERVAL '1 day'
                        )::date
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

                elif periodo == "semana_anterior":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE fecha >= (
                            date_trunc('week', CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date
                            - INTERVAL '7 day'
                        )::date
                          AND fecha < date_trunc(
                            'week',
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

                elif periodo == "anio":
                    cur.execute("""
                        SELECT * FROM picks
                        WHERE date_trunc('year', fecha) =
                              date_trunc('year', CURRENT_DATE AT TIME ZONE 'Europe/Madrid')
                        ORDER BY fecha_hora;
                    """)

                else:
                    return []

                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error leyendo picks de DB ({periodo}): {e}")
        return []


def db_picks_filtrados(
    liga: str | None = None,
    codigo: str | None = None,
) -> list[dict]:
    """
    Devuelve todos los picks filtrando opcionalmente por liga y/o código.
    La búsqueda es case-insensitive y acepta coincidencia parcial (ILIKE '%valor%').
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                condiciones = []
                params = []

                if liga:
                    condiciones.append("liga ILIKE %s")
                    params.append(f"%{liga}%")

                if codigo:
                    condiciones.append("codigo ILIKE %s")
                    params.append(f"%{codigo}%")

                where = ("WHERE " + " AND ".join(condiciones)) if condiciones else ""

                cur.execute(f"""
                    SELECT * FROM picks
                    {where}
                    ORDER BY fecha_hora;
                """, params)

                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error en db_picks_filtrados (liga={liga}, codigo={codigo}): {e}")
        return []


def db_pick_por_message_id(message_id_origen: str) -> dict | None:
    """Devuelve un pick concreto por su message_id_origen."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM picks
                    WHERE message_id_origen = %s
                    LIMIT 1;
                    """,
                    (message_id_origen,),
                )
                return cur.fetchone()
    except Exception as e:
        logger.error(f"Error leyendo pick por message_id {message_id_origen}: {e}")
        return None


def db_picks_pendientes_revision(max_dias: int = 7, min_horas: int = 6) -> list[dict]:
    """
    Devuelve picks sin resultado pendientes de revision manual.
    Incluye solo picks recientes (max_dias) pero con suficiente antiguedad (min_horas)
    para evitar avisos demasiado pronto.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *,
                           ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - fecha_hora)) / 3600.0, 1)
                               AS horas_pendiente
                    FROM picks
                    WHERE resultado IS NULL
                      AND fecha_hora >= CURRENT_TIMESTAMP - (%s || ' days')::INTERVAL
                      AND fecha_hora <= CURRENT_TIMESTAMP - (%s || ' hours')::INTERVAL
                    ORDER BY fecha_hora ASC;
                    """,
                    (max_dias, min_horas),
                )
                return cur.fetchall()
    except Exception as e:
        logger.error(
            f"Error leyendo picks pendientes de revision (dias={max_dias}, horas={min_horas}): {e}"
        )
        return []


def db_picks_para_analisis(
    codigo: str | None = None,
    tipo_pick: str | None = None,
    dias: int = 180,
) -> list[dict]:
    """
    Devuelve picks resueltos (HIT/MISS) para análisis de filtros.
    Permite filtrar por código o tipo y limitar a los últimos N días.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                condiciones = [
                    "resultado IN ('HIT', 'MISS')",
                    "fecha >= ((CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date - (%s * INTERVAL '1 day'))::date",
                ]
                params: list = [dias]

                if codigo:
                    condiciones.append("codigo ILIKE %s")
                    params.append(codigo)

                if tipo_pick:
                    condiciones.append("tipo_pick = %s")
                    params.append(tipo_pick)

                where = " AND ".join(condiciones)
                cur.execute(
                    f"""
                    SELECT *
                    FROM picks
                    WHERE {where}
                    ORDER BY fecha_hora DESC;
                    """,
                    params,
                )
                return cur.fetchall()
    except Exception as e:
        logger.error(
            f"Error leyendo picks para analisis (codigo={codigo}, tipo={tipo_pick}, dias={dias}): {e}"
        )
        return []


# ==============================
# RACHA
# ==============================

def db_calcular_racha_actual(tipo_pick: str | None = None) -> int:
    """
    Cuenta los HITs consecutivos al final de la lista de picks resueltos.
    Los VOIDs se ignoran (no cortan la racha).
    Si tipo_pick se especifica, filtra solo ese tipo.
    Devuelve el número de HITs consecutivos actuales.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if tipo_pick:
                    cur.execute("""
                        SELECT resultado FROM picks
                        WHERE resultado IS NOT NULL
                          AND resultado != 'VOID'
                          AND tipo_pick = %s
                        ORDER BY fecha_hora DESC
                        LIMIT 30;
                    """, (tipo_pick,))
                else:
                    cur.execute("""
                        SELECT resultado FROM picks
                        WHERE resultado IS NOT NULL
                          AND resultado != 'VOID'
                        ORDER BY fecha_hora DESC
                        LIMIT 30;
                    """)
                rows = cur.fetchall()

        racha = 0
        for row in rows:
            if row["resultado"] == "HIT":
                racha += 1
            else:
                break
        return racha
    except Exception as e:
        logger.error(f"Error calculando racha: {e}")
        return 0


# ==============================
# STATS MENSUALES (para bot premium)
# ==============================

def db_stats_por_mes(meses: int = 6) -> list[dict]:
    """
    Devuelve estadísticas agrupadas por mes y tipo_pick para los últimos N meses.
    Cada fila incluye: mes (YYYY-MM), tipo_pick, total, hits, misses, voids.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        TO_CHAR(fecha, 'YYYY-MM') AS mes,
                        tipo_pick,
                        COUNT(*)                                              AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END) AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses,
                        SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END) AS voids
                    FROM picks
                    WHERE resultado IS NOT NULL
                      AND fecha >= DATE_TRUNC(
                            'month',
                            CURRENT_DATE - ((%s - 1) || ' months')::INTERVAL
                          )
                    GROUP BY mes, tipo_pick
                    ORDER BY mes DESC, tipo_pick;
                """, (meses,))
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error en db_stats_por_mes: {e}")
        return []


# ==============================
# STATS PREPARTIDO (mes a mes por estrategia)
# ==============================

def db_stats_prepartido_por_mes() -> list[dict]:
    """
    Estadísticas de picks prepartido agrupadas por mes y código.
    Incluye profit y ROI calculados en unidades (stake base = 1u).
    Solo picks con resultado resuelto.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        TO_CHAR(fecha, 'YYYY-MM')                              AS mes,
                        codigo,
                        COUNT(*)                                                AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END)   AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END)   AS misses,
                        SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END)   AS voids,
                        -- Profit en unidades con el mismo rango operativo del stake
                        SUM(CASE
                            WHEN resultado = 'HIT' AND odds IS NOT NULL THEN
                                (odds - 1) * CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                                    ELSE 0.0
                                END
                            WHEN resultado = 'MISS' AND odds IS NOT NULL THEN
                                -1.0 * CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                                    ELSE 0.0
                                END
                            ELSE 0
                        END)                                                    AS profit_units,
                        SUM(CASE
                            WHEN resultado IN ('HIT','MISS') AND odds IS NOT NULL THEN
                                CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 1
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1
                                    ELSE 0
                                END
                            ELSE 0
                        END)                                                    AS picks_con_odds
                    FROM picks
                    WHERE codigo ILIKE 'PRE_%'
                      AND resultado IS NOT NULL
                    GROUP BY mes, codigo
                    ORDER BY mes DESC, codigo;
                """)
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error en db_stats_prepartido_por_mes: {e}")
        return []


def db_stats_prepartido_global() -> list[dict]:
    """
    Estadísticas globales (todos los meses) de picks prepartido por estrategia.
    Incluye profit y ROI en unidades con multiplicador real por rango de cuota.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        codigo,
                        COUNT(*)                                                AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END)   AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END)   AS misses,
                        SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END)   AS voids,
                        -- Profit usando multiplicador por rango de cuota
                        SUM(CASE
                            WHEN resultado = 'HIT' AND odds IS NOT NULL THEN
                                (odds - 1) * CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                                    ELSE 0.0
                                END
                            WHEN resultado = 'MISS' AND odds IS NOT NULL THEN
                                -1.0 * CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                                    ELSE 0.0
                                END
                            ELSE 0
                        END)                                                    AS profit_units,
                        SUM(CASE
                            WHEN resultado IN ('HIT','MISS') AND odds IS NOT NULL THEN
                                CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                                    ELSE 0.0
                                END
                            ELSE 0
                        END)                                                    AS staked_units,
                        SUM(CASE
                            WHEN resultado IN ('HIT','MISS') AND odds IS NOT NULL THEN
                                CASE
                                    WHEN odds >= 1.70 AND odds < 1.80 THEN 1
                                    WHEN odds >= 1.80 AND odds < 1.90 THEN 1
                                    WHEN odds >= 1.90 AND odds <= 2.60 THEN 1
                                    ELSE 0
                                END
                            ELSE 0
                        END)                                                    AS picks_con_odds
                    FROM picks
                    WHERE codigo ILIKE 'PRE_%'
                      AND resultado IS NOT NULL
                    GROUP BY codigo
                    ORDER BY codigo;
                """)
                return cur.fetchall()
    except Exception as e:
        logger.error(f"Error en db_stats_prepartido_global: {e}")
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
# BANKROLL — LECTURA/ESCRITURA EN DB
# ==============================

def db_get_bankroll(default: float = 1000.0) -> float:
    """Lee el bankroll desde la tabla free_state (clave 'bankroll')."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT valor FROM free_state WHERE clave = 'bankroll' LIMIT 1;"
                )
                row = cur.fetchone()
        if row:
            import json as _json
            return float(_json.loads(row["valor"]))
        return default
    except Exception as e:
        logger.error(f"Error leyendo bankroll de DB: {e}")
        return default


def db_set_bankroll(valor: float) -> None:
    """Persiste el bankroll en la tabla free_state (clave 'bankroll')."""
    import json as _json
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO free_state (clave, valor)
                    VALUES ('bankroll', %s)
                    ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor;
                """, (_json.dumps(round(valor, 2)),))
        logger.info(f"Bankroll guardado en DB: {valor}€")
    except Exception as e:
        logger.error(f"Error guardando bankroll en DB: {e}")


# ==============================
# FREE STATE — LECTURA/ESCRITURA
# ==============================

_FREE_STATE_DEFAULT = {
    "fecha":              None,
    "goles_enviados":     0,
    "corners_enviados":   0,
    "ultimo_score_gol":   -1,
    "ultimo_score_corner": -1,
    "ultima_hora_envio":  None,
}


def db_leer_free_state() -> dict:
    """Lee el free_state desde DB. Si no existe devuelve el estado por defecto."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT clave, valor FROM free_state;")
                rows = cur.fetchall()

        import json as _json
        estado = dict(_FREE_STATE_DEFAULT)
        for row in rows:
            try:
                estado[row["clave"]] = _json.loads(row["valor"])
            except Exception:
                estado[row["clave"]] = row["valor"]
        return estado
    except Exception as e:
        logger.error(f"Error leyendo free_state de DB: {e}")
        return dict(_FREE_STATE_DEFAULT)


def db_guardar_free_state(estado: dict) -> None:
    """Persiste todo el free_state en DB."""
    import json as _json
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                for clave, valor in estado.items():
                    cur.execute("""
                        INSERT INTO free_state (clave, valor)
                        VALUES (%s, %s)
                        ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor;
                    """, (clave, _json.dumps(valor)))
        logger.debug("free_state guardado en DB.")
    except Exception as e:
        logger.error(f"Error guardando free_state en DB: {e}")


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
                periodo_codigo    = item.get("periodo_codigo"),
                modo_codigo       = item.get("modo_codigo"),
                linea_codigo      = item.get("linea_codigo"),
                liga              = item.get("liga"),
                partido           = item.get("partido"),
                strike_alerta     = item.get("strike_alerta"),
                strike_liga       = item.get("strike_liga"),
                enviado_a_free    = bool(item.get("enviado_a_free", False)),
                strike_alerta_pct = item.get("strike_alerta_pct"),
                strike_liga_pct   = item.get("strike_liga_pct"),
                minuto_alerta     = item.get("minuto_alerta"),
                goles_entrada_total = item.get("goles_entrada_total"),
                corners_entrada_total = item.get("corners_entrada_total"),
                red_cards_entrada_total = item.get("red_cards_entrada_total"),
                momentum_local    = item.get("momentum_local"),
                momentum_visitante = item.get("momentum_visitante"),
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
