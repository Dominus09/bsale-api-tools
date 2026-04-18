"""
Análisis completo de clientes (documentos Distribuidora + bsale.clients): montos, frecuencia y nivel A–E.
"""

from __future__ import annotations

import logging
from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd

from backend.db import get_connection
from backend.services.distribuidora.orders_service import _row_to_dict, _serialize_row

logger = logging.getLogger(__name__)

MAX_ANALISIS_CLIENTES = 10_000

_SQL_ANALISIS = """
WITH base AS (
    SELECT
        d.client_id AS client_id,
        MAX(
            NULLIF(
                TRIM(
                    CONCAT_WS(
                        ' ',
                        NULLIF(TRIM(c.first_name), ''),
                        NULLIF(TRIM(c.last_name), '')
                    )
                ),
                ''
            )
        ) AS nombre,
        MAX(NULLIF(TRIM(c.nombre_fantasia), '')) AS fantasy_name,
        MAX(c.rut_clean) AS rut_clean,
        MAX(
            COALESCE(
                NULLIF(TRIM(d.municipality), ''),
                NULLIF(TRIM(c.municipality), '')
            )
        ) AS municipality,
        MAX(
            COALESCE(
                NULLIF(TRIM(d.city), ''),
                NULLIF(TRIM(c.city), '')
            )
        ) AS city,
        MAX(d.emission_date) AS ultima_compra,
        SUM(
            CASE
                WHEN d.emission_date >= (CURRENT_TIMESTAMP - INTERVAL '30 days') THEN
                    CASE
                        WHEN d.document_type_id = 9 THEN -COALESCE(d.total_amount, 0::numeric)
                        ELSE COALESCE(d.total_amount, 0::numeric)
                    END
                ELSE 0::numeric
            END
        ) AS compra_30_dias,
        SUM(
            CASE
                WHEN d.emission_date >= (CURRENT_TIMESTAMP - INTERVAL '60 days') THEN
                    CASE
                        WHEN d.document_type_id = 9 THEN -COALESCE(d.total_amount, 0::numeric)
                        ELSE COALESCE(d.total_amount, 0::numeric)
                    END
                ELSE 0::numeric
            END
        ) AS compra_60_dias,
        COUNT(*) FILTER (
            WHERE d.document_type_id IN (1, 6)
              AND date_part('month', d.emission_date) = 1
              AND date_part('year', d.emission_date) = date_part('year', CURRENT_TIMESTAMP)
        )::bigint AS freq_enero,
        COUNT(*) FILTER (
            WHERE d.document_type_id IN (1, 6)
              AND date_part('month', d.emission_date) = 2
              AND date_part('year', d.emission_date) = date_part('year', CURRENT_TIMESTAMP)
        )::bigint AS freq_febrero,
        COUNT(*) FILTER (
            WHERE d.document_type_id IN (1, 6)
              AND date_part('month', d.emission_date) = 3
              AND date_part('year', d.emission_date) = date_part('year', CURRENT_TIMESTAMP)
        )::bigint AS freq_marzo,
        COUNT(*) FILTER (
            WHERE d.document_type_id IN (1, 6)
              AND date_part('month', d.emission_date) = 4
              AND date_part('year', d.emission_date) = date_part('year', CURRENT_TIMESTAMP)
        )::bigint AS freq_abril
    FROM distribuidora.v_documents_latest d
    LEFT JOIN bsale.clients c
        ON c.company_id = d.company_id
       AND c.bsale_id = d.client_id
    WHERE
        d.office_id = 1
        AND d.company_id = 3
        AND COALESCE(d.state, 0) = 0
        AND d.document_type_id IN (1, 6, 9)
        AND d.client_id IS NOT NULL
    GROUP BY
        d.client_id
)
SELECT
    client_id,
    nombre,
    fantasy_name,
    rut_clean,
    municipality,
    city,
    ultima_compra,
    compra_30_dias,
    compra_60_dias,
    freq_enero,
    freq_febrero,
    freq_marzo,
    freq_abril,
    CASE
        WHEN compra_30_dias > 500000 AND freq_abril >= 4 THEN 'A'
        WHEN compra_30_dias > 300000 AND freq_abril >= 3 THEN 'B'
        WHEN compra_30_dias > 150000 THEN 'C'
        WHEN compra_30_dias > 50000 THEN 'D'
        ELSE 'E'
    END AS nivel_cliente,
    CASE
        WHEN ultima_compra IS NULL THEN NULL
        ELSE (CURRENT_DATE - ultima_compra::date)
    END AS dias_sin_comprar
FROM base
ORDER BY compra_30_dias DESC NULLS LAST
LIMIT %s
"""


def list_clientes_analisis(*, limit: int = MAX_ANALISIS_CLIENTES) -> list[dict[str, Any]]:
    cap = min(max(1, limit), MAX_ANALISIS_CLIENTES)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(_SQL_ANALISIS, (cap,))
        rows = cur.fetchall()
        out = [_serialize_row(_row_to_dict(cur, r)) for r in rows]
        cur.close()
        logger.info("clientes analisis: %s filas (limit=%s)", len(out), cap)
        return out
    finally:
        conn.close()


def build_clientes_analisis_excel_bytes(*, limit: int = MAX_ANALISIS_CLIENTES) -> tuple[bytes, str]:
    """Genera .xlsx en memoria; nombre sugerido con fecha."""
    rows = list_clientes_analisis(limit=limit)
    if not rows:
        df = pd.DataFrame(
            columns=[
                "client_id",
                "nombre",
                "fantasy_name",
                "rut_clean",
                "municipality",
                "city",
                "ultima_compra",
                "dias_sin_comprar",
                "compra_30_dias",
                "compra_60_dias",
                "freq_enero",
                "freq_febrero",
                "freq_marzo",
                "freq_abril",
                "nivel_cliente",
            ]
        )
    else:
        df = pd.DataFrame(rows)
    buf = BytesIO()
    df.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    raw = buf.getvalue()
    fname = f"analisis_clientes_{datetime.utcnow().strftime('%Y%m%d_%H%M')}.xlsx"
    return raw, fname
