"""Listado de documentos para pre‑planificación de despacho (filtro por observaciones / día)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection

_ALLOWED_DAYS = frozenset(
    {"lunes", "martes", "miercoles", "jueves", "viernes", "sabado"},
)


def _normalize_date_range(d0: date, d1: date) -> tuple[date, date]:
    if d0 > d1:
        return d1, d0
    return d0, d1


def _row_to_dict(cur, row: tuple) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _serialize(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for k, v in list(out.items()):
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
    return out


def list_dispatch_planning_orders(
    *,
    emission_date_from: date,
    emission_date_to: date,
    delivery_day: str = "all",
    company_id: int = 3,
    office_id: int = 1,
) -> list[dict[str, Any]]:
    """
    Órdenes de compra Bsale (``document_type_id = 33``) pendientes (``state = 0``) con texto de
    observación derivado de atributo ``OBSERVACIONES`` y/o ``raw_data->>'comments'``.
    """
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    day = (delivery_day or "all").strip().lower()
    if day == "all" or day == "todos":
        day_filter_active = False
        day_pat: str | None = None
    else:
        day_n = "".join(ch for ch in day if ch.isalpha())
        day_n = day_n.lower()
        if day_n not in _ALLOWED_DAYS:
            day_filter_active = False
            day_pat = None
        else:
            day_filter_active = True
            day_pat = f"%{day_n}%"

    conn = get_connection()
    try:
        cur = conn.cursor()
        if day_filter_active and day_pat:
            cur.execute(
                """
                SELECT
                    d.document_id,
                    d.client_id,
                    d.number AS oc,
                    COALESCE(
                        NULLIF(BTRIM(c.nombre_fantasia), ''),
                        NULLIF(BTRIM(c.company), ''),
                        CONCAT_WS(
                            ' ',
                            NULLIF(BTRIM(c.first_name), ''),
                            NULLIF(BTRIM(c.last_name), '')
                        )
                    ) AS nombre_fantasia,
                    COALESCE(
                        NULLIF(BTRIM(d.municipality), ''),
                        NULLIF(BTRIM(c.municipality), '')
                    ) AS municipality,
                    COALESCE(
                        NULLIF(BTRIM(d.address), ''),
                        NULLIF(BTRIM(c.address), '')
                    ) AS direccion,
                    NULLIF(BTRIM(c.municipality), '') AS comuna,
                    NULLIF(BTRIM(d.seller_name), '') AS seller_name,
                    d.total_amount,
                    (c.lat IS NOT NULL AND c.lon IS NOT NULL) AS has_georef,
                    c.lat::double precision AS lat,
                    c.lon::double precision AS lng,
                    COALESCE(
                        NULLIF(
                            BTRIM(
                                (
                                    SELECT da.attribute_value
                                    FROM distribuidora.document_attributes da
                                    WHERE da.document_id = d.document_id
                                      AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
                                    ORDER BY da.id DESC NULLS LAST
                                    LIMIT 1
                                )
                            ),
                            ''
                        ),
                        NULLIF(BTRIM(d.raw_data->>'comments'), '')
                    ) AS observations
                FROM distribuidora.v_documents_latest d
                LEFT JOIN bsale.clients c
                    ON c.company_id = d.company_id
                   AND c.bsale_id = d.client_id
                WHERE d.company_id = %s
                  AND d.office_id = %s
                  AND d.document_type_id = 33
                  AND d.state = 0
                  AND d.emission_date >= %s::date
                  AND d.emission_date < (%s::date + interval '1 day')
                  AND translate(
                        lower(
                            COALESCE(
                                (
                                    SELECT da.attribute_value
                                    FROM distribuidora.document_attributes da
                                    WHERE da.document_id = d.document_id
                                      AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
                                    ORDER BY da.id DESC NULLS LAST
                                    LIMIT 1
                                ),
                                ''
                            )
                            || ' '
                            || COALESCE(d.raw_data->>'comments', '')
                        ),
                        'áéíóúü',
                        'aeiouu'
                    ) LIKE %s
                ORDER BY d.number DESC NULLS LAST, d.document_id DESC
                LIMIT 5000
                """,
                (company_id, office_id, d0, d1, day_pat),
            )
        else:
            cur.execute(
                """
                SELECT
                    d.document_id,
                    d.client_id,
                    d.number AS oc,
                    COALESCE(
                        NULLIF(BTRIM(c.nombre_fantasia), ''),
                        NULLIF(BTRIM(c.company), ''),
                        CONCAT_WS(
                            ' ',
                            NULLIF(BTRIM(c.first_name), ''),
                            NULLIF(BTRIM(c.last_name), '')
                        )
                    ) AS nombre_fantasia,
                    COALESCE(
                        NULLIF(BTRIM(d.municipality), ''),
                        NULLIF(BTRIM(c.municipality), '')
                    ) AS municipality,
                    COALESCE(
                        NULLIF(BTRIM(d.address), ''),
                        NULLIF(BTRIM(c.address), '')
                    ) AS direccion,
                    NULLIF(BTRIM(c.municipality), '') AS comuna,
                    NULLIF(BTRIM(d.seller_name), '') AS seller_name,
                    d.total_amount,
                    (c.lat IS NOT NULL AND c.lon IS NOT NULL) AS has_georef,
                    c.lat::double precision AS lat,
                    c.lon::double precision AS lng,
                    COALESCE(
                        NULLIF(
                            BTRIM(
                                (
                                    SELECT da.attribute_value
                                    FROM distribuidora.document_attributes da
                                    WHERE da.document_id = d.document_id
                                      AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
                                    ORDER BY da.id DESC NULLS LAST
                                    LIMIT 1
                                )
                            ),
                            ''
                        ),
                        NULLIF(BTRIM(d.raw_data->>'comments'), '')
                    ) AS observations
                FROM distribuidora.v_documents_latest d
                LEFT JOIN bsale.clients c
                    ON c.company_id = d.company_id
                   AND c.bsale_id = d.client_id
                WHERE d.company_id = %s
                  AND d.office_id = %s
                  AND d.document_type_id = 33
                  AND d.state = 0
                  AND d.emission_date >= %s::date
                  AND d.emission_date < (%s::date + interval '1 day')
                ORDER BY d.number DESC NULLS LAST, d.document_id DESC
                LIMIT 5000
                """,
                (company_id, office_id, d0, d1),
            )
        rows = [_serialize(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()
