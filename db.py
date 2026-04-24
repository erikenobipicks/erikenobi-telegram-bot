import os
import logging
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

# ==============================
# POOL DE CONEXIONES
# ==============================

_pool: ConnectionPool | None = None


def init_pool() -> None:
    """
    Inicializa el pool de conexiones PostgreSQL.
    Llamar UNA vez al arrancar el bot, antes de init_db().
    El pool reutiliza conexiones en lugar de abrir una nueva por query,
    evitando agotar el límite de conexiones de Railway.
    """
    global _pool
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")
    _pool = ConnectionPool(
        conninfo=DATABASE_URL,
        min_size=1,
        max_size=5,
        kwargs={"row_factory": dict_row},
    )
    _pool.wait()  # espera hasta que min_size conexiones estén listas
    logger.info("Pool de conexiones DB inicializado (min=1, max=5).")


# ==============================
# CONEXIÓN
# ==============================

def get_conn():
    """
    Devuelve una conexión del pool (si está inicializado) o una conexión
    directa como fallback. Compatible con el patrón 'with get_conn() as conn:'.
    """
    if _pool is not None:
        return _pool.connection()
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
                    odds              NUMERIC(6,2),
                    fecha             DATE         NOT NULL,
                    fecha_hora        TIMESTAMP    NOT NULL DEFAULT NOW()
                );
            """)

            # Columna odds: añadir si no existe y ampliar precisión en instalaciones antiguas
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS odds NUMERIC(6,2);
            """)
            # Migración silenciosa: ampliar NUMERIC(5,2) → NUMERIC(6,2) si ya existe
            # (ALTER TYPE solo se ejecuta si la precisión actual es menor)
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'picks'
                          AND column_name = 'odds'
                          AND numeric_precision = 5
                    ) THEN
                        ALTER TABLE picks ALTER COLUMN odds TYPE NUMERIC(6,2);
                    END IF;
                END $$;
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
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS nivel TEXT;
            """)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS sistema TEXT;
            """)
            # stake: unidades apostadas reales por pick (5.0, 3.0, 1.0…)
            cur.execute("""
                ALTER TABLE picks ADD COLUMN IF NOT EXISTS stake NUMERIC(4,1);
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

            # Índice para consultas de resultado (resúmenes, rachas)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_resultado
                ON picks (resultado);
            """)
            # Índice para análisis por nivel (ÉLITE/ALTO/FAVORABLE/BAJO)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_picks_nivel
                ON picks (nivel);
            """)

            # Integridad: CHECK en tipo_pick para evitar valores inválidos.
            # Se añade de forma segura (sin crash si la restricción ya existe
            # o si hay datos que no la cumplen en instalaciones antiguas).
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint
                        WHERE conname = 'picks_tipo_pick_check'
                    ) THEN
                        BEGIN
                            ALTER TABLE picks
                                ADD CONSTRAINT picks_tipo_pick_check
                                CHECK (tipo_pick IN ('gol', 'corner'));
                        EXCEPTION WHEN check_violation THEN
                            -- datos históricos con otro valor: se ignora la restricción
                            NULL;
                        END;
                    END IF;
                END $$;
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
    nivel: str | None = None,
    sistema: str | None = None,
    stake: float | None = None,
) -> None:
    params = (
        message_id_origen, codigo, tipo_pick, periodo_codigo, modo_codigo,
        linea_codigo, liga, partido,
        strike_alerta, strike_liga, strike_alerta_pct, strike_liga_pct,
        enviado_a_free, minuto_alerta, goles_entrada_total,
        corners_entrada_total, red_cards_entrada_total,
        momentum_local, momentum_visitante, odds, nivel, sistema, stake, fecha, fecha_hora,
    )
    # Un reintento automático ante fallos transitorios de red/DB
    for intento in range(2):
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
                            momentum_local, momentum_visitante, odds, nivel, sistema, stake,
                            fecha, fecha_hora
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, NULL, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (message_id_origen) DO NOTHING;
                    """, params)
                    _rows = cur.rowcount
            if _rows == 0:
                logger.warning(
                    "PICK IGNORADO — ON CONFLICT en picks para message_id_origen=%s. "
                    "La fila ya existe en DB. Ninguna fila nueva creada.",
                    message_id_origen,
                )
            else:
                logger.info("Pick registrado en DB: %s", message_id_origen)
            return
        except Exception as e:
            if intento == 0:
                logger.warning(
                    "Reintentando guardado del pick %s tras error: %s",
                    message_id_origen, e,
                )
            else:
                logger.error(
                    "Error definitivo guardando pick %s en DB: %s",
                    message_id_origen, e,
                )


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
                              date_trunc('month',
                                  (CURRENT_TIMESTAMP AT TIME ZONE 'Europe/Madrid')::date
                                  - INTERVAL '1 month'
                              )
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


def db_picks_pre_por_periodo(periodo: str) -> list[dict]:
    """
    Devuelve solo picks PRE_* del período indicado, para los resúmenes
    de los canales prepartido (Carlos Mollar y General).
    Reutiliza db_picks_por_periodo y filtra en Python.
    """
    todos = db_picks_por_periodo(periodo)
    return [p for p in todos if (p.get("codigo") or "").upper().startswith("PRE_")]


def db_picks_cm_por_periodo(periodo: str) -> list[dict]:
    """
    Devuelve picks REM_* de Carlos Mollar del período para el análisis
    del canal Carlos Mollar.

    Se usan picks REM (no PRE) porque:
    - El resultado se actualiza directamente cuando inplayguru edita el
      mensaje REM (HIT / MISS), sin necesidad de fuzzy match.
    - Los PRE pueden quedarse pendientes si la propagación falla.
    """
    todos = db_picks_por_periodo(periodo)
    return [
        p for p in todos
        if (p.get("codigo") or "").upper().startswith("REM_")
        and "CARLOS" in (p.get("sistema") or "").upper()
    ]


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


def db_buscar_pre_para_rem(
    codigo_pre: str,
    tipo_pick: str,
    dias: int = 14,
) -> list[dict]:
    """
    Devuelve picks PRE sin resultado candidatos a vincularse con un recordatorio REM.
    Filtra por código PRE exacto y tipo_pick dentro de los últimos N días.
    El llamador aplica el fuzzy match de partido sobre la lista devuelta.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM picks
                    WHERE codigo ILIKE %s
                      AND tipo_pick = %s
                      AND resultado IS NULL
                      AND fecha >= CURRENT_DATE - (%s * INTERVAL '1 day')::INTERVAL
                    ORDER BY fecha_hora DESC
                    LIMIT 20;
                """, (codigo_pre, tipo_pick, dias))
                return cur.fetchall()
    except Exception as e:
        logger.error("Error en db_buscar_pre_para_rem (codigo=%s): %s", codigo_pre, e)
        return []


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


def db_racha_miss_ng1() -> int:
    """
    Cuenta los MISS consecutivos más recientes específicamente para picks NG1.
    Se filtra por codigo='NG1' (no por tipo_pick) para aislar la racha de esta estrategia.
    Los VOIDs se ignoran.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT resultado FROM picks
                    WHERE resultado IS NOT NULL
                      AND resultado != 'VOID'
                      AND codigo = 'NG1'
                    ORDER BY fecha_hora DESC
                    LIMIT 20;
                """)
                rows = cur.fetchall()
        racha = 0
        for row in rows:
            if row["resultado"] == "MISS":
                racha += 1
            else:
                break
        return racha
    except Exception as e:
        logger.error(f"Error calculando racha MISS para NG1: {e}")
        return 0


def db_racha_miss_actual(tipo_pick: str) -> int:
    """
    Cuenta los MISS consecutivos más recientes para tipo_pick.
    Los VOIDs se ignoran (no cortan ni suman a la racha).
    Devuelve 0 si el pick más reciente no es MISS.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT resultado FROM picks
                    WHERE resultado IS NOT NULL
                      AND resultado != 'VOID'
                      AND tipo_pick = %s
                    ORDER BY fecha_hora DESC
                    LIMIT 20;
                """, (tipo_pick,))
                rows = cur.fetchall()
        racha = 0
        for row in rows:
            if row["resultado"] == "MISS":
                racha += 1
            else:
                break
        return racha
    except Exception as e:
        logger.error(f"Error calculando racha MISS para {tipo_pick}: {e}")
        return 0


# ═══════════════════════════════════════════════════════════════════════════
# SISTEMA DE LIGAS — CLASIFICACIÓN DINÁMICA (estrategia_liga_stats)
# ═══════════════════════════════════════════════════════════════════════════

# Mapa auxiliar: tier → columna de multiplicador en estrategia_config
_TIER_MULT_COL = {
    "ELITE":    "stake_mult_elite",
    "SANA":     "stake_mult_sana",
    "NEUTRAL":  "stake_mult_neutral",
    "DUDOSA":   "stake_mult_dudosa",
    "DESCARTE": "stake_mult_descarte",
}

# Multiplicadores hardcoded por si la config falla (fallback seguro)
_TIER_MULT_DEFAULT = {
    "ELITE": 1.20, "SANA": 1.00, "NEUTRAL": 1.00,
    "DUDOSA": 0.70, "DESCARTE": 0.00,
}


def db_get_liga_tier(estrategia_id: str, liga: str) -> tuple[str, float]:
    """
    Devuelve (tier, stake_multiplier) para la liga en la estrategia dada.

    - Búsqueda case-insensitive con TRIM.
    - Si la liga no existe, inserta fila NEUTRAL y devuelve ('NEUTRAL', mult_neutral).
    - Si la tabla no existe o hay error de DB, devuelve ('NEUTRAL', 1.00) — falla segura.
    - El multiplicador se lee de estrategia_config; si la config no existe usa _TIER_MULT_DEFAULT.
    """
    if not liga:
        return ("NEUTRAL", 1.00)

    liga_norm = liga.strip()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # ── 1. Buscar liga ─────────────────────────────────────────
                cur.execute("""
                    SELECT ls.tier,
                           COALESCE(
                               CASE ls.tier
                                   WHEN 'ELITE'    THEN ec.stake_mult_elite
                                   WHEN 'SANA'     THEN ec.stake_mult_sana
                                   WHEN 'NEUTRAL'  THEN ec.stake_mult_neutral
                                   WHEN 'DUDOSA'   THEN ec.stake_mult_dudosa
                                   WHEN 'DESCARTE' THEN ec.stake_mult_descarte
                                   ELSE ec.stake_mult_neutral
                               END,
                               1.00
                           )::float AS stake_mult
                    FROM estrategia_liga_stats ls
                    LEFT JOIN estrategia_config ec
                           ON ec.estrategia_id = ls.estrategia_id
                    WHERE ls.estrategia_id = %s
                      AND LOWER(TRIM(ls.liga)) = LOWER(TRIM(%s))
                """, (estrategia_id, liga_norm))

                row = cur.fetchone()
                if row:
                    return (row["tier"], float(row["stake_mult"]))

                # ── 2. Liga nueva: obtener mult NEUTRAL de config ──────────
                cur.execute("""
                    SELECT stake_mult_neutral
                    FROM estrategia_config
                    WHERE estrategia_id = %s
                """, (estrategia_id,))
                cfg = cur.fetchone()
                mult_neutral = float(cfg["stake_mult_neutral"]) if cfg else 1.00

                # ── 3. Insertar fila NEUTRAL (ON CONFLICT por si hubo race) ─
                cur.execute("""
                    INSERT INTO estrategia_liga_stats
                        (estrategia_id, liga, tier, ultima_actualizacion)
                    VALUES (%s, %s, 'NEUTRAL', NOW())
                    ON CONFLICT (estrategia_id, liga) DO NOTHING
                """, (estrategia_id, liga_norm))

                logger.info(
                    "Liga nueva registrada como NEUTRAL | estrategia=%s | liga=%s",
                    estrategia_id, liga_norm,
                )
                return ("NEUTRAL", mult_neutral)

    except Exception as e:
        logger.error(
            "Error en db_get_liga_tier(%s, %s): %s — devuelve NEUTRAL fallback",
            estrategia_id, liga_norm, e,
        )
        return ("NEUTRAL", 1.00)


def db_recalcular_liga(estrategia_id: str, liga: str) -> dict | None:
    """
    Recalcula tasa_eb y tier de una liga usando picks reales de la ventana.

    Reglas NO negociables (spec del briefing):
    - Si override_manual=TRUE y override_expira > NOW(): solo actualiza stats, NO cambia tier.
    - Rate limit: el tier no puede cambiar más de 1 vez cada 14 días.
    - Nunca modifica estrategia_config.
    - Si falla, loggea y devuelve None (no propaga la excepción).

    Devuelve dict con resultado del recálculo o None si falla.
    """
    if not liga:
        return None

    liga_norm = liga.strip()

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:

                # ── 1. Leer config de estrategia ──────────────────────────
                cur.execute(
                    "SELECT * FROM estrategia_config WHERE estrategia_id = %s",
                    (estrategia_id,),
                )
                cfg = cur.fetchone()
                if not cfg:
                    logger.warning(
                        "db_recalcular_liga: no hay config para estrategia '%s'",
                        estrategia_id,
                    )
                    return None

                alpha        = float(cfg["prior_alpha"])
                beta         = float(cfg["prior_beta"])
                ventana_dias = int(cfg["ventana_dias"])
                min_picks    = int(cfg["min_picks_tier"])
                min_descarte = int(cfg["min_picks_descarte"])
                min_elite    = int(cfg["min_picks_elite"])
                t_elite      = float(cfg["tier_elite_min"])
                t_sana       = float(cfg["tier_sana_min"])
                t_neutral    = float(cfg["tier_neutral_min"])
                t_dudosa     = float(cfg["tier_dudosa_min"])

                # ── 2. Contar picks y wins en la ventana ──────────────────
                fecha_limite = datetime.now(timezone.utc) - timedelta(days=ventana_dias)
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE resultado IN ('HIT','MISS')) AS picks,
                        COUNT(*) FILTER (WHERE resultado = 'HIT')           AS wins
                    FROM picks
                    WHERE codigo ILIKE 'NG1%%'
                      AND LOWER(TRIM(liga)) = LOWER(TRIM(%s))
                      AND fecha_hora >= %s
                      AND resultado IS NOT NULL
                      AND resultado != 'VOID'
                """, (liga_norm, fecha_limite))
                stats = cur.fetchone()
                picks = int(stats["picks"] or 0)
                wins  = int(stats["wins"]  or 0)

                # ── 3. Tasa Empirical Bayes ────────────────────────────────
                tasa_eb = (wins + alpha) / (picks + alpha + beta)

                # ── 4. Determinar tier nuevo ──────────────────────────────
                if picks < min_picks:
                    nuevo_tier = "NEUTRAL"
                elif tasa_eb >= t_elite and picks >= min_elite:
                    nuevo_tier = "ELITE"
                elif tasa_eb >= t_sana:
                    nuevo_tier = "SANA"
                elif tasa_eb >= t_neutral:
                    nuevo_tier = "NEUTRAL"
                elif tasa_eb >= t_dudosa and picks >= min_descarte:
                    nuevo_tier = "DUDOSA"
                elif picks >= min_descarte:
                    nuevo_tier = "DESCARTE"
                else:
                    nuevo_tier = "NEUTRAL"

                # ── 5. Leer estado actual de la liga ──────────────────────
                cur.execute("""
                    SELECT tier, tier_anterior, fecha_cambio_tier,
                           override_manual, override_expira
                    FROM estrategia_liga_stats
                    WHERE estrategia_id = %s
                      AND LOWER(TRIM(liga)) = LOWER(TRIM(%s))
                """, (estrategia_id, liga_norm))
                row = cur.fetchone()

                tier_actual       = (row["tier"]             if row else "NEUTRAL")
                fecha_cambio_tier = (row["fecha_cambio_tier"] if row else None)
                override_manual   = (row["override_manual"]   if row else False)
                override_expira   = (row["override_expira"]   if row else None)

                # ── 6. Override manual vigente: no cambiar tier ───────────
                ahora_utc = datetime.now(timezone.utc)
                override_activo = (
                    override_manual
                    and override_expira is not None
                    and (
                        # soporte TIMESTAMP y TIMESTAMPTZ
                        (override_expira.tzinfo is not None and override_expira > ahora_utc)
                        or
                        (override_expira.tzinfo is None
                         and override_expira > ahora_utc.replace(tzinfo=None))
                    )
                )

                tier_cambio = False
                if override_activo:
                    nuevo_tier = tier_actual   # no tocar
                elif nuevo_tier != tier_actual:
                    # ── Rate limit 14 días ────────────────────────────────
                    if fecha_cambio_tier is not None:
                        fct_naive = (
                            fecha_cambio_tier.replace(tzinfo=None)
                            if fecha_cambio_tier.tzinfo is None
                            else fecha_cambio_tier.astimezone(timezone.utc).replace(tzinfo=None)
                        )
                        dias_desde_cambio = (ahora_utc.replace(tzinfo=None) - fct_naive).days
                        if dias_desde_cambio < 14:
                            nuevo_tier = tier_actual   # rate limit: no cambiar aún
                        else:
                            tier_cambio = True
                    else:
                        tier_cambio = True

                # ── 7. UPSERT ─────────────────────────────────────────────
                if row:
                    if tier_cambio:
                        cur.execute("""
                            UPDATE estrategia_liga_stats
                            SET picks_90d            = %s,
                                wins_90d             = %s,
                                tasa_eb              = %s,
                                tier_anterior        = tier,
                                tier                 = %s,
                                fecha_cambio_tier    = NOW(),
                                ultima_actualizacion = NOW()
                            WHERE estrategia_id = %s
                              AND LOWER(TRIM(liga)) = LOWER(TRIM(%s))
                        """, (picks, wins, round(tasa_eb, 4),
                              nuevo_tier, estrategia_id, liga_norm))
                    else:
                        cur.execute("""
                            UPDATE estrategia_liga_stats
                            SET picks_90d            = %s,
                                wins_90d             = %s,
                                tasa_eb              = %s,
                                ultima_actualizacion = NOW()
                            WHERE estrategia_id = %s
                              AND LOWER(TRIM(liga)) = LOWER(TRIM(%s))
                        """, (picks, wins, round(tasa_eb, 4),
                              estrategia_id, liga_norm))
                else:
                    # Liga no existe aún → insertar
                    cur.execute("""
                        INSERT INTO estrategia_liga_stats
                            (estrategia_id, liga, picks_90d, wins_90d,
                             tasa_eb, tier, ultima_actualizacion)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (estrategia_id, liga) DO UPDATE SET
                            picks_90d            = EXCLUDED.picks_90d,
                            wins_90d             = EXCLUDED.wins_90d,
                            tasa_eb              = EXCLUDED.tasa_eb,
                            ultima_actualizacion = NOW()
                    """, (estrategia_id, liga_norm, picks, wins,
                          round(tasa_eb, 4), nuevo_tier))

                if tier_cambio:
                    logger.info(
                        "Tier cambiado | estrategia=%s | liga=%s | %s → %s | "
                        "picks=%d wins=%d tasa_eb=%.4f",
                        estrategia_id, liga_norm,
                        tier_actual, nuevo_tier,
                        picks, wins, tasa_eb,
                    )

                return {
                    "estrategia_id": estrategia_id,
                    "liga":          liga_norm,
                    "picks":         picks,
                    "wins":          wins,
                    "tasa_eb":       round(tasa_eb, 4),
                    "tier_anterior": tier_actual,
                    "tier_nuevo":    nuevo_tier,
                    "tier_cambio":   tier_cambio,
                    "override_activo": override_activo,
                }

    except Exception as e:
        logger.error(
            "Error en db_recalcular_liga(%s, %s): %s",
            estrategia_id, liga_norm, e,
        )
        return None


def db_ligas_activas_ng1(ventana_dias: int = 90) -> list[str]:
    """
    Devuelve lista de ligas con al menos 1 pick NG1 en los últimos ventana_dias días.
    Usada por el proceso nocturno para saber qué ligas recalcular.
    """
    try:
        fecha_limite = datetime.now(timezone.utc) - timedelta(days=ventana_dias)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT TRIM(liga) AS liga
                    FROM picks
                    WHERE codigo ILIKE 'NG1%%'
                      AND fecha_hora >= %s
                      AND liga IS NOT NULL
                      AND TRIM(liga) != ''
                    ORDER BY 1
                """, (fecha_limite,))
                rows = cur.fetchall()
        return [r["liga"] for r in rows]
    except Exception as e:
        logger.error("Error en db_ligas_activas_ng1: %s", e)
        return []


def db_stats_ng1_ultimos_dias(dias: int = 30) -> dict:
    """
    Estadísticas resumen de picks NG1 en los últimos N días.
    Devuelve dict con: picks, wins, tasa.
    """
    try:
        fecha_limite = datetime.now(timezone.utc) - timedelta(days=dias)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*) FILTER (WHERE resultado IN ('HIT','MISS')) AS picks,
                        COUNT(*) FILTER (WHERE resultado = 'HIT')           AS wins
                    FROM picks
                    WHERE codigo ILIKE 'NG1%%'
                      AND fecha_hora >= %s
                      AND resultado IS NOT NULL
                      AND resultado != 'VOID'
                """, (fecha_limite,))
                row = cur.fetchone()
        picks = int(row["picks"] or 0)
        wins  = int(row["wins"]  or 0)
        tasa  = round(wins / picks * 100, 2) if picks > 0 else 0.0
        return {"picks": picks, "wins": wins, "tasa": tasa}
    except Exception as e:
        logger.error("Error en db_stats_ng1_ultimos_dias: %s", e)
        return {"picks": 0, "wins": 0, "tasa": 0.0}


def db_conteo_tiers_ng1() -> dict[str, int]:
    """
    Devuelve conteo de ligas por tier para la estrategia NG1.
    Solo ligas con picks_90d >= min_picks_tier (para excluir sin muestra).
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT ls.tier, COUNT(*) AS n
                    FROM estrategia_liga_stats ls
                    JOIN estrategia_config ec ON ec.estrategia_id = ls.estrategia_id
                    WHERE ls.estrategia_id = 'NG1'
                      AND ls.picks_90d >= ec.min_picks_tier
                    GROUP BY ls.tier
                """)
                rows = cur.fetchall()
        return {r["tier"]: int(r["n"]) for r in rows}
    except Exception as e:
        logger.error("Error en db_conteo_tiers_ng1: %s", e)
        return {}


def db_ligas_con_tier_y_picks(estrategia_id: str) -> list[dict]:
    """
    Devuelve todas las ligas de la estrategia con sus stats actuales.
    Usado por el proceso nocturno para detectar ligas inactivas.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT liga, tier, picks_90d, wins_90d, tasa_eb,
                           override_manual, override_expira, ultima_actualizacion
                    FROM estrategia_liga_stats
                    WHERE estrategia_id = %s
                    ORDER BY picks_90d DESC
                """, (estrategia_id,))
                return cur.fetchall()
    except Exception as e:
        logger.error("Error en db_ligas_con_tier_y_picks: %s", e)
        return []


def db_expirar_overrides_y_ligar(estrategia_id: str) -> list[str]:
    """
    Expira overrides caducados (override_expira < NOW()) y devuelve las ligas afectadas
    para que el llamador pueda recalcular su tier.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE estrategia_liga_stats
                    SET override_manual = FALSE
                    WHERE estrategia_id = %s
                      AND override_manual = TRUE
                      AND override_expira < NOW()
                    RETURNING liga
                """, (estrategia_id,))
                rows = cur.fetchall()
        ligas = [r["liga"] for r in rows]
        if ligas:
            logger.info(
                "Overrides expirados | estrategia=%s | ligas: %s",
                estrategia_id, ", ".join(ligas),
            )
        return ligas
    except Exception as e:
        logger.error("Error en db_expirar_overrides_y_ligar: %s", e)
        return []


def db_reset_ligas_inactivas_ng1(ventana_dias: int = 90) -> int:
    """
    Resetea a NEUTRAL las ligas sin picks en los últimos ventana_dias días.
    Devuelve el número de ligas reseteadas.
    """
    try:
        fecha_limite = datetime.now(timezone.utc) - timedelta(days=ventana_dias)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE estrategia_liga_stats
                    SET tier    = 'NEUTRAL',
                        picks_90d = 0,
                        wins_90d  = 0,
                        ultima_actualizacion = NOW()
                    WHERE estrategia_id = 'NG1'
                      AND override_manual = FALSE
                      AND liga NOT IN (
                          SELECT DISTINCT TRIM(liga)
                          FROM picks
                          WHERE codigo ILIKE 'NG1%%'
                            AND fecha_hora >= %s
                            AND liga IS NOT NULL
                      )
                      AND tier != 'NEUTRAL'
                """, (fecha_limite,))
                n = cur.rowcount
        if n > 0:
            logger.info("Ligas inactivas reseteadas a NEUTRAL | n=%d", n)
        return n
    except Exception as e:
        logger.error("Error en db_reset_ligas_inactivas_ng1: %s", e)
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
# ALERTAS RECIENTES — LECTURA/ESCRITURA EN DB
# ==============================

def db_leer_alertas_recientes() -> dict:
    """
    Lee el dict alertas_recientes desde free_state (clave 'alertas_recientes').
    Devuelve {} si no existe o falla.
    """
    import json as _json
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT valor FROM free_state WHERE clave = 'alertas_recientes' LIMIT 1;"
                )
                row = cur.fetchone()
        if row:
            return _json.loads(row["valor"])
        return {}
    except Exception as e:
        logger.error(f"Error leyendo alertas_recientes de DB: {e}")
        return {}


def db_guardar_alertas_recientes(alertas: dict) -> None:
    """Persiste alertas_recientes en free_state."""
    import json as _json
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO free_state (clave, valor)
                    VALUES ('alertas_recientes', %s)
                    ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor;
                """, (_json.dumps(alertas),))
    except Exception as e:
        logger.error(f"Error guardando alertas_recientes en DB: {e}")


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
# SCORING ESTADÍSTICO
# ==============================

def db_score_por_dimension(
    tipo_pick: str,
    codigo: str | None = None,
    liga: str | None = None,
    hora: int | None = None,
    minuto_min: int | None = None,
    minuto_max: int | None = None,
    dias: int = 90,
) -> tuple[int, int]:
    """
    Devuelve (hits, total) de picks resueltos para la dimensión indicada.
    Solo se aplica un filtro de dimensión por llamada; el resto son None.
    Usado por scorer.py para calcular el score estadístico de cada alerta.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                condiciones = [
                    "resultado IN ('HIT', 'MISS')",
                    "tipo_pick = %s",
                    "fecha >= (CURRENT_DATE - (%s * INTERVAL '1 day')::INTERVAL)::date",
                ]
                params: list = [tipo_pick, dias]

                if codigo is not None:
                    condiciones.append("codigo = %s")
                    params.append(codigo)

                if liga is not None:
                    condiciones.append("liga ILIKE %s")
                    params.append(liga)

                if hora is not None:
                    condiciones.append(
                        "EXTRACT(HOUR FROM (fecha_hora AT TIME ZONE 'Europe/Madrid')) = %s"
                    )
                    params.append(hora)

                if minuto_min is not None and minuto_max is not None:
                    condiciones.append("minuto_alerta BETWEEN %s AND %s")
                    params.extend([minuto_min, minuto_max])

                where = " AND ".join(condiciones)
                cur.execute(
                    f"""
                    SELECT
                        SUM(CASE WHEN resultado = 'HIT' THEN 1 ELSE 0 END) AS hits,
                        COUNT(*) AS total
                    FROM picks
                    WHERE {where};
                    """,
                    params,
                )
                row = cur.fetchone()
                if row and row["total"]:
                    return int(row["hits"] or 0), int(row["total"])
                return 0, 0
    except Exception as e:
        logger.error(f"Error en db_score_por_dimension: {e}")
        return 0, 0


def db_stats_pre_rapido(
    codigo: str,
    tipo_pick: str,
    dias: int = 365,
) -> dict:
    """
    Devuelve en una sola consulta todos los contadores necesarios para el
    historial del recordatorio REM: hits, misses, voids, pendientes y total.

    A diferencia de db_picks_para_analisis (que solo devuelve HIT/MISS),
    aquí se incluyen todos los picks del código — resueltos y pendientes —
    para que el historial muestre cuántos hay en espera de resultado.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        COUNT(*)                                               AS total,
                        SUM(CASE WHEN resultado = 'HIT'  THEN 1 ELSE 0 END)  AS hits,
                        SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END)  AS misses,
                        SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END)  AS voids,
                        SUM(CASE WHEN resultado IS NULL  THEN 1 ELSE 0 END)  AS pendientes
                    FROM picks
                    WHERE codigo ILIKE %s
                      AND tipo_pick = %s
                      AND fecha >= (CURRENT_DATE - (%s * INTERVAL '1 day')::INTERVAL)::date;
                """, (codigo, tipo_pick, dias))
                row = cur.fetchone()
                if row:
                    return {
                        "total":      int(row["total"]      or 0),
                        "hits":       int(row["hits"]       or 0),
                        "misses":     int(row["misses"]     or 0),
                        "voids":      int(row["voids"]      or 0),
                        "pendientes": int(row["pendientes"] or 0),
                    }
                return {"total": 0, "hits": 0, "misses": 0, "voids": 0, "pendientes": 0}
    except Exception as e:
        logger.error("Error en db_stats_pre_rapido (codigo=%s): %s", codigo, e)
        return {"total": 0, "hits": 0, "misses": 0, "voids": 0, "pendientes": 0}


def db_score_pre_por_dimension(
    codigo: str | None = None,
    liga: str | None = None,
    odds_min: float | None = None,
    odds_max: float | None = None,
    dias: int = 365,
) -> tuple[int, int]:
    """
    Devuelve (hits, total) de picks PRE resueltos para la dimensión indicada.
    Análogo a db_score_por_dimension pero solo para PRE_* con ventana anual.
    Usado por scorer_pre.py para el cálculo bayesiano del stake.
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                condiciones = [
                    "codigo ILIKE 'PRE_%'",
                    "resultado IN ('HIT', 'MISS')",
                    "fecha >= (CURRENT_DATE - (%s * INTERVAL '1 day')::INTERVAL)::date",
                ]
                params: list = [dias]

                if codigo is not None:
                    condiciones.append("codigo ILIKE %s")
                    params.append(codigo)

                if liga is not None:
                    condiciones.append("liga ILIKE %s")
                    params.append(liga)

                if odds_min is not None:
                    condiciones.append("odds >= %s")
                    params.append(odds_min)

                if odds_max is not None:
                    condiciones.append("odds < %s")
                    params.append(odds_max)

                where = " AND ".join(condiciones)
                cur.execute(
                    f"""
                    SELECT
                        SUM(CASE WHEN resultado = 'HIT' THEN 1 ELSE 0 END) AS hits,
                        COUNT(*) AS total
                    FROM picks
                    WHERE {where};
                    """,
                    params,
                )
                row = cur.fetchone()
                if row and row["total"]:
                    return int(row["hits"] or 0), int(row["total"])
                return 0, 0
    except Exception as e:
        logger.error("Error en db_score_pre_por_dimension: %s", e)
        return 0, 0


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
                db_actualizar_resultado_confirmado(
                    str(item.get("message_id_origen", "")),
                    resultado,
                )
            importados += 1
        except Exception as e:
            logger.error(f"Error migrando pick {item.get('message_id_origen')}: {e}")

    logger.info(f"Migración completada: {importados} picks importados.")
    return importados
