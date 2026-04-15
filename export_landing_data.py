import json
import os
from datetime import datetime
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_OUTPUT = Path("landing-ventas") / "data" / "landing-data.json"


def get_conn():
    if not DATABASE_URL:
        raise ValueError("Falta DATABASE_URL en variables de entorno.")
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def calc_strike(hits, misses):
    resolved = (hits or 0) + (misses or 0)
    if resolved <= 0:
        return 0.0
    return round((hits / resolved) * 100, 1)


def fetch_type_stats(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                tipo_pick,
                COUNT(*) AS total,
                SUM(CASE WHEN resultado = 'HIT' THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses,
                SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END) AS voids
            FROM picks
            WHERE resultado IS NOT NULL
              AND codigo NOT ILIKE 'PRE_%'
            GROUP BY tipo_pick
            ORDER BY tipo_pick;
            """
        )
        rows = cur.fetchall()

    stats = {}
    for row in rows:
        tipo = row["tipo_pick"]
        stats[tipo] = {
            "total": int(row["total"] or 0),
            "hits": int(row["hits"] or 0),
            "misses": int(row["misses"] or 0),
            "voids": int(row["voids"] or 0),
            "strike": calc_strike(int(row["hits"] or 0), int(row["misses"] or 0)),
        }
    return stats


def fetch_pre_stats(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                codigo,
                COUNT(*) AS total,
                SUM(CASE WHEN resultado = 'HIT' THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses,
                SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END) AS voids,
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
                END) AS profit_units,
                SUM(CASE
                    WHEN resultado IN ('HIT','MISS') AND odds IS NOT NULL THEN
                        CASE
                            WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                            WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                            WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                            ELSE 0.0
                        END
                    ELSE 0
                END) AS staked_units
            FROM picks
            WHERE codigo = 'PRE_O25FT'
              AND resultado IS NOT NULL
            GROUP BY codigo
            LIMIT 1;
            """
        )
        global_row = cur.fetchone()

        cur.execute(
            """
            SELECT
                TO_CHAR(fecha, 'YYYY-MM') AS mes,
                COUNT(*) AS total,
                SUM(CASE WHEN resultado = 'HIT' THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN resultado = 'MISS' THEN 1 ELSE 0 END) AS misses,
                SUM(CASE WHEN resultado = 'VOID' THEN 1 ELSE 0 END) AS voids,
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
                END) AS profit_units,
                SUM(CASE
                    WHEN resultado IN ('HIT','MISS') AND odds IS NOT NULL THEN
                        CASE
                            WHEN odds >= 1.70 AND odds < 1.80 THEN 1.0
                            WHEN odds >= 1.80 AND odds < 1.90 THEN 0.5
                            WHEN odds >= 1.90 AND odds <= 2.60 THEN 1.0
                            ELSE 0.0
                        END
                    ELSE 0
                END) AS staked_units
            FROM picks
            WHERE codigo = 'PRE_O25FT'
              AND resultado IS NOT NULL
            GROUP BY mes
            ORDER BY mes DESC
            LIMIT 6;
            """
        )
        month_rows = cur.fetchall()

    if not global_row:
        return None

    profit_units = float(global_row["profit_units"] or 0)
    staked_units = float(global_row["staked_units"] or 0)
    roi = round((profit_units / staked_units) * 100, 1) if staked_units > 0 else 0.0
    hits = int(global_row["hits"] or 0)
    misses = int(global_row["misses"] or 0)

    months = []
    for row in month_rows:
        month_profit = float(row["profit_units"] or 0)
        month_staked = float(row["staked_units"] or 0)
        month_hits = int(row["hits"] or 0)
        month_misses = int(row["misses"] or 0)
        months.append(
            {
                "month": row["mes"],
                "total": int(row["total"] or 0),
                "hits": month_hits,
                "misses": month_misses,
                "voids": int(row["voids"] or 0),
                "strike": calc_strike(month_hits, month_misses),
                "profit_units": round(month_profit, 2),
                "roi": round((month_profit / month_staked) * 100, 1) if month_staked > 0 else 0.0,
            }
        )

    return {
        "code": "PRE_O25FT",
        "name": "Over 2.5 FT",
        "total": int(global_row["total"] or 0),
        "hits": hits,
        "misses": misses,
        "voids": int(global_row["voids"] or 0),
        "strike": calc_strike(hits, misses),
        "profit_units": round(profit_units, 2),
        "roi": roi,
        "months": months,
    }


def _market_label(row):
    codigo = (row.get("codigo") or "").upper()
    if codigo == "PRE_O25FT":
        return "Over 2.5 FT"
    if codigo.startswith("PRE_"):
        return "Prepartido"

    tipo = (row.get("tipo_pick") or "").lower()
    periodo = (row.get("periodo_codigo") or "").upper()
    modo = (row.get("modo_codigo") or "").upper()
    linea = (row.get("linea_codigo") or "").upper()

    parts = []
    if tipo == "gol":
        parts.append("Goles")
    elif tipo == "corner":
        parts.append("Corners")

    if modo:
        parts.append(modo)
    if linea:
        parts.append(linea)
    if periodo:
        parts.append(periodo)

    return " ".join(parts) if parts else codigo or "Pick"


def fetch_latest_picks(conn):
    queries = {
        "goals": (
            """
            SELECT *
            FROM picks
            WHERE tipo_pick = 'gol'
              AND codigo NOT ILIKE 'PRE_%'
            ORDER BY fecha_hora DESC
            LIMIT 6;
            """
        ),
        "corners": (
            """
            SELECT *
            FROM picks
            WHERE tipo_pick = 'corner'
            ORDER BY fecha_hora DESC
            LIMIT 6;
            """
        ),
        "pre_match": (
            """
            SELECT *
            FROM picks
            WHERE codigo = 'PRE_O25FT'
            ORDER BY fecha_hora DESC
            LIMIT 6;
            """
        ),
    }

    data = {}
    with conn.cursor() as cur:
        for key, query in queries.items():
            cur.execute(query)
            rows = cur.fetchall()
            data[key] = [
                {
                    "message_id": row["message_id_origen"],
                    "market": _market_label(row),
                    "code": row["codigo"],
                    "league": row["liga"],
                    "match": row["partido"],
                    "result": row["resultado"],
                    "timestamp": row["fecha_hora"].isoformat() if row.get("fecha_hora") else None,
                    "is_free": bool(row["enviado_a_free"]),
                }
                for row in rows
            ]
    return data


def build_payload():
    with get_conn() as conn:
        return {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "stats": {
                "types": fetch_type_stats(conn),
                "pre_match_manual": fetch_pre_stats(conn),
            },
            "latest_picks": fetch_latest_picks(conn),
        }


def write_payload(payload, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def main():
    output = Path(os.getenv("LANDING_DATA_OUTPUT", DEFAULT_OUTPUT))
    payload = build_payload()
    write_payload(payload, output)
    print(f"JSON generado en: {output}")


if __name__ == "__main__":
    main()
