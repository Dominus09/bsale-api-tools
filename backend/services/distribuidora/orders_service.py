"""Consultas de negocio: órdenes de compra (vista enriquecida + detalle)."""

from __future__ import annotations

import unicodedata
from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection

_DAY_FILTER_ALLOW = frozenset({"lunes", "martes", "miercoles", "jueves", "viernes", "sabado"})

# OC (tipo 33): facturada si hay ``document_related`` desde un detalle de la OC hacia boleta/factura (1 o 6).
OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL = """
NOT EXISTS (
    SELECT 1
    FROM distribuidora.document_related dr
    INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
    INNER JOIN distribuidora.v_documents_latest inv
        ON inv.document_id = dr.related_document_id
       AND inv.document_type_id IN (1, 6)
       AND inv.company_id = d.company_id
       AND inv.office_id = d.office_id
    WHERE dd.document_id = d.document_id
)
""".strip()

OC_PURCHASE_ESTADO_REAL_SQL = """
CASE
    WHEN EXISTS (
        SELECT 1
        FROM distribuidora.document_related dr
        INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
        INNER JOIN distribuidora.v_documents_latest inv
            ON inv.document_id = dr.related_document_id
           AND inv.document_type_id IN (1, 6)
           AND inv.company_id = d.company_id
           AND inv.office_id = d.office_id
        WHERE dd.document_id = d.document_id
    ) THEN 'Facturada'
    ELSE 'Pendiente'
END
""".strip()

# Texto “observaciones” en documento OC: atributo + comentarios JSON (no hay columna ``observations``).
_OBS_NORMALIZED_D = """translate(lower(
    COALESCE(
        NULLIF(BTRIM((
            SELECT da.attribute_value
            FROM distribuidora.document_attributes da
            WHERE da.document_id = d.document_id
              AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
            ORDER BY da.id DESC NULLS LAST
            LIMIT 1
        )), ''),
        NULLIF(BTRIM(d.raw_data->>'comments'), '')
    )
), 'áéíóúü', 'aeiouu')"""

_OBS_NORMALIZED_P = """translate(lower(
    COALESCE(
        NULLIF(BTRIM(p.observaciones), ''),
        NULLIF(BTRIM(d.raw_data->>'comments'), '')
    )
), 'áéíóúü', 'aeiouu')"""


def _sanitize_day_filter(raw: str | None) -> str | None:
    if raw is None or not str(raw).strip():
        return None
    s = "".join(
        c
        for c in unicodedata.normalize("NFD", str(raw).strip().lower())
        if unicodedata.category(c) != "Mn"
    )
    s = "".join(c for c in s if c.isalpha())
    if s not in _DAY_FILTER_ALLOW:
        return None
    return s


def _day_filter_sql_params(day_filter: str | None) -> tuple[bool, str]:
    """Para ``CASE WHEN %s THEN TRUE ELSE <obs> LIKE %s END``."""
    tok = _sanitize_day_filter(day_filter)
    if not tok:
        return True, ""
    return False, f"%{tok}%"


def _row_to_dict(cur, row: tuple) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _serialize_row(d: dict[str, Any]) -> dict[str, Any]:
    out = dict(d)
    for k, v in list(out.items()):
        if hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif isinstance(v, (int, float)) and (k.endswith("amount") or k == "total_amount"):
            out[k] = float(v) if v is not None else None
    return out


def _split_csv_terms(raw: str | None) -> list[str]:
    if not raw or not raw.strip():
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def list_purchase_orders(
    *,
    only_not_invoiced: bool = False,
    emission_date_from: date | None = None,
    emission_date_to: date | None = None,
    delivery_search: str | None = None,
    municipality: str | None = None,
    client_id: int | None = None,
    user_id: int | None = None,
    limit: int = 500,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    where = ["1=1"]
    params: list[Any] = []
    if only_not_invoiced:
        where.append("COALESCE(e.is_invoiced, FALSE) = FALSE")
    if emission_date_from is not None:
        params.append(emission_date_from)
        where.append("e.emission_date >= %s::date")
    if emission_date_to is not None:
        params.append(emission_date_to)
        where.append("e.emission_date < (%s::date + interval '1 day')")
    delivery_terms = _split_csv_terms(delivery_search)
    if delivery_terms:
        ors: list[str] = []
        for term in delivery_terms:
            ors.append("e.observaciones ILIKE %s")
            params.append(f"%{term}%")
        where.append("(" + " OR ".join(ors) + ")")
    muni_keys = _split_csv_terms(municipality)
    if muni_keys:
        muni_expr = (
            "COALESCE(NULLIF(BTRIM(e.municipality), ''), NULLIF(BTRIM(e.city), ''), '')"
        )
        muni_parts: list[str] = []
        for k in muni_keys:
            if k == "__NONE__":
                muni_parts.append(f"({muni_expr} = '')")
            else:
                params.append(k)
                muni_parts.append(f"{muni_expr} = %s")
        where.append("(" + " OR ".join(muni_parts) + ")")
    if client_id is not None:
        params.append(client_id)
        where.append("e.client_id = %s")
    if user_id is not None:
        params.append(user_id)
        where.append("e.user_id = %s")

    where_sql = " AND ".join(where)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT COUNT(*) FROM distribuidora.v_orders_purchase_enriched e
            WHERE {where_sql}
            """,
            tuple(params),
        )
        total = int(cur.fetchone()[0])
        params2 = list(params)
        params2.extend([limit, offset])
        cur.execute(
            f"""
            SELECT
                e.*,
                COALESCE(
                    NULLIF(BTRIM(e.seller_name), ''),
                    NULLIF(
                        BTRIM(
                            COALESCE(u.first_name, '')
                            || ' '
                            || COALESCE(u.last_name, '')
                        ),
                        ''
                    ),
                    NULLIF(BTRIM(u.email), ''),
                    CASE
                        WHEN e.user_id IS NULL THEN NULL
                        ELSE 'Usuario ' || e.user_id::text
                    END
                ) AS seller
            FROM distribuidora.v_orders_purchase_enriched e
            LEFT JOIN bsale.bsale_users u
                ON u.company_id = 3
               AND u.bsale_id = e.user_id
            WHERE {where_sql}
            ORDER BY e.emission_date DESC NULLS LAST, e.document_id DESC
            LIMIT %s OFFSET %s
            """,
            tuple(params2),
        )
        rows = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return rows, total
    finally:
        conn.close()


def get_purchase_order_detail(document_id: int) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT *
            FROM distribuidora.v_orders_purchase_enriched
            WHERE document_id = %s
            """,
            (document_id,),
        )
        r = cur.fetchone()
        if not r:
            return None
        header = _serialize_row(_row_to_dict(cur, r))

        cur.execute(
            """
            SELECT *
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY line_number NULLS LAST, detail_id
            """,
            (document_id,),
        )
        details = [_serialize_row(_row_to_dict(cur, x)) for x in cur.fetchall()]

        cur.execute(
            """
            SELECT id, document_id, attribute_id, attribute_name, attribute_value, created_at
            FROM distribuidora.document_attributes
            WHERE document_id = %s
            ORDER BY attribute_name
            """,
            (document_id,),
        )
        attributes = [_serialize_row(_row_to_dict(cur, x)) for x in cur.fetchall()]

        cur.close()
        return {
            "header": header,
            "details": details,
            "attributes": attributes,
        }
    finally:
        conn.close()


def get_sync_status_payload() -> dict[str, Any]:
    """Estado sync + lock (sin bloquear al caller si otro proceso corre)."""
    from backend.repositories.distribuidora.sync_repo import ensure_distribuidora_schema
    from backend.services.distribuidora.sync_service import ADVISORY_LOCK_KEY

    conn = get_connection()
    try:
        cur = conn.cursor()
        ensure_distribuidora_schema(cur)
        conn.commit()
        cur.execute(
            """
            SELECT process_name, last_sync, last_status, last_message, updated_at
            FROM distribuidora.sync_state
            ORDER BY process_name
            """
        )
        states = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]

        cur.execute(
            """
            SELECT id, process_name, started_at, finished_at, status,
                   documents_processed, documents_inserted, documents_updated,
                   details_inserted, attributes_inserted, references_inserted, message
            FROM distribuidora.sync_logs
            ORDER BY id DESC
            LIMIT 1
            """
        )
        lr = cur.fetchone()
        last_log = _serialize_row(_row_to_dict(cur, lr)) if lr else None

        cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
        got = bool(cur.fetchone()[0])
        if got:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
        active = not got

        sync_by_domain: dict[str, Any] | None = None
        try:
            cur.execute("SELECT * FROM distribuidora.v_sync_status")
            row = cur.fetchone()
            if row:
                sync_by_domain = _serialize_row(_row_to_dict(cur, row))
        except Exception:
            sync_by_domain = None

        cur.close()
        return {
            "sync_state": states,
            "last_log": last_log,
            "sync_lock_active": active,
            "sync_last_by_domain": sync_by_domain,
        }
    finally:
        conn.close()


def _normalize_date_range(
    emission_date_from: date,
    emission_date_to: date,
) -> tuple[date, date]:
    if emission_date_from > emission_date_to:
        return emission_date_to, emission_date_from
    return emission_date_from, emission_date_to


def list_dispatch_prep_by_municipality(
    *,
    emission_date_from: date,
    emission_date_to: date,
    only_not_invoiced: bool = True,
    day_filter: str | None = None,
) -> list[dict[str, Any]]:
    """
    Resumen por comuna para pre‑planificación de despacho (órdenes de compra Bsale tipo 33).

    Filtro "solo no facturadas": sin fila en ``document_related`` hacia boleta/factura (tipos 1/6),
    vía detalles de la misma OC (no se usa ``state``).
    """
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    skip_day, day_like = _day_filter_sql_params(day_filter)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                COALESCE(
                    NULLIF(BTRIM(d.municipality), ''),
                    NULLIF(BTRIM(d.city), ''),
                    '(Sin comuna)'
                ) AS municipality,
                COUNT(DISTINCT d.client_id) AS clientes_unicos,
                COUNT(*) AS pedidos,
                SUM(COALESCE(d.total_amount, 0::numeric)) AS total_ventas
            FROM distribuidora.v_documents_latest d
            WHERE d.company_id = 3
              AND d.office_id = 1
              AND d.document_type_id = 33
              AND d.emission_date >= %s::date
              AND d.emission_date < (%s::date + interval '1 day')
              AND (%s = FALSE OR {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})
              AND CASE WHEN %s THEN TRUE ELSE {_OBS_NORMALIZED_D} LIKE %s END
            GROUP BY 1
            ORDER BY total_ventas DESC NULLS LAST, municipality ASC
            """,
            (d0, d1, only_not_invoiced, skip_day, day_like),
        )
        rows = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def list_dispatch_prep_observation_texts(
    *,
    emission_date_from: date,
    emission_date_to: date,
    only_not_invoiced: bool = True,
    limit: int = 2000,
    day_filter: str | None = None,
) -> list[str]:
    """Textos de observaciones (atributo OBSERVACIONES en OC) para análisis en frontend."""
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    lim = max(1, min(int(limit), 5000))
    skip_day, day_like = _day_filter_sql_params(day_filter)
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT p.observaciones
            FROM distribuidora.v_orders_purchase p
            INNER JOIN distribuidora.v_documents_latest d ON d.document_id = p.document_id
            WHERE p.observaciones IS NOT NULL
              AND BTRIM(p.observaciones) <> ''
              AND d.emission_date >= %s::date
              AND d.emission_date < (%s::date + interval '1 day')
              AND (%s = FALSE OR {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})
              AND CASE WHEN %s THEN TRUE ELSE {_OBS_NORMALIZED_P} LIKE %s END
            LIMIT %s
            """,
            (d0, d1, only_not_invoiced, skip_day, day_like, lim),
        )
        out: list[str] = []
        for (text,) in cur.fetchall():
            if text is None:
                continue
            s = str(text).strip()
            if s:
                out.append(s)
        cur.close()
        return out
    finally:
        conn.close()


def list_dispatch_prep_planning_rows(
    *,
    emission_date_from: date,
    emission_date_to: date,
    only_not_invoiced: bool = True,
    day_filter: str | None = None,
    limit: int = 5000,
) -> list[dict[str, Any]]:
    """
    Filas OC (33) para tabla de pre‑planificación: join ``bsale.clients`` (no existe ``clientes``).

    Observaciones: mismo criterio que el resumen por comuna (atributo + ``comments`` en JSON).
    """
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    skip_day, day_like = _day_filter_sql_params(day_filter)
    lim = max(1, min(int(limit), 5000))
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT
                d.document_id,
                d.number AS oc,
                d.client_id,
                NULLIF(BTRIM(c.nombre_fantasia), '') AS nombre_fantasia,
                COALESCE(
                    NULLIF(BTRIM(d.municipality), ''),
                    NULLIF(BTRIM(c.municipality), '')
                ) AS municipality,
                COALESCE(
                    NULLIF(BTRIM(d.address), ''),
                    NULLIF(BTRIM(c.address), '')
                ) AS direccion,
                NULLIF(BTRIM(d.seller_name), '') AS seller_name,
                d.total_amount,
                (c.lat IS NOT NULL AND c.lon IS NOT NULL) AS has_georef,
                c.lat::double precision AS lat,
                c.lon::double precision AS lng,
                ({OC_PURCHASE_ESTADO_REAL_SQL}) AS estado_real
            FROM distribuidora.v_documents_latest d
            LEFT JOIN bsale.clients c
                ON c.company_id = d.company_id
               AND c.bsale_id = d.client_id
            WHERE d.company_id = 3
              AND d.office_id = 1
              AND d.document_type_id = 33
              AND d.emission_date >= %s::date
              AND d.emission_date < (%s::date + interval '1 day')
              AND (%s = FALSE OR {OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL})
              AND CASE WHEN %s THEN TRUE ELSE {_OBS_NORMALIZED_D} LIKE %s END
            ORDER BY d.number DESC NULLS LAST, d.document_id DESC
            LIMIT %s
            """,
            (d0, d1, only_not_invoiced, skip_day, day_like, lim),
        )
        rows = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()
