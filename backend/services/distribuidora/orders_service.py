"""Consultas de negocio: órdenes de compra (vista enriquecida + detalle)."""

from __future__ import annotations

import time
import unicodedata
from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection
from backend.utils.dispatch_prep_common import (
    DEFAULT_DISPATCH_PREP_LIMIT,
    effective_page_limit,
    log_dispatch_prep,
    payload_size_bytes,
    wide_range_meta,
)
from backend.utils.planning_rows_debug import PlanningRowsTimer, log_planning_rows

_DISPATCH_PREP_DOC_FILTER = """
    d.company_id = 3
    AND d.office_id = 1
    AND d.document_type_id = 33
    AND d.emission_date >= %s::date
    AND d.emission_date < (%s::date + interval '1 day')
""".strip()

_DISPATCH_PREP_NOT_INVOICED_FILTER = (
    "(%s = FALSE OR NOT COALESCE(conf_f.is_invoiced, FALSE))"
)

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

# Pre-planificación: observaciones vía v_oc_attributes_flat (evita subquery por fila).
_PLANNING_ROWS_OBS_TEXT = """translate(lower(
    COALESCE(
        NULLIF(BTRIM(obs_a.observaciones), ''),
        NULLIF(BTRIM(d.raw_data->>'comments'), '')
    )
), 'áéíóúü', 'aeiouu')"""

# Estado OC sin materializar v_purchase_document_status_full (LATERAL prob + conf).
_PLANNING_ROWS_STATUS_JOINS = """
LEFT JOIN distribuidora.v_oc_attributes_flat obs_a
    ON obs_a.document_id = d.document_id
LEFT JOIN distribuidora.v_orders_purchase_status conf
    ON conf.document_id = d.document_id
LEFT JOIN LATERAL (
    SELECT
        pm.score,
        pm.candidate_document_id,
        dinv.number AS candidate_number,
        dinv.document_type_id AS candidate_document_type,
        CASE dinv.document_type_id
            WHEN 1 THEN 'Boleta'
            WHEN 6 THEN 'Factura'
            ELSE 'Tipo ' || dinv.document_type_id::text
        END AS candidate_document_type_label
    FROM distribuidora.document_probable_matches pm
    INNER JOIN distribuidora.documents dinv
        ON dinv.document_id = pm.candidate_document_id
       AND dinv.document_type_id IN (1, 6)
       AND dinv.company_id = 3
       AND dinv.office_id = 1
    WHERE pm.oc_document_id = d.document_id
      AND pm.score >= 60
      AND NOT COALESCE(conf.is_invoiced, FALSE)
    ORDER BY pm.score DESC, pm.candidate_document_id DESC
    LIMIT 1
) prob ON TRUE
"""

_PLANNING_ROWS_STATUS_SELECT = """
                CASE
                    WHEN COALESCE(conf.is_invoiced, FALSE) THEN 'FACTURADA_CONFIRMADA'
                    WHEN prob.score >= 90 THEN 'PROBABLE_FACTURADA_HIGH'
                    WHEN prob.score >= 75 THEN 'PROBABLE_FACTURADA_MEDIUM'
                    WHEN prob.score >= 60 THEN 'PROBABLE_FACTURADA_LOW'
                    ELSE 'PENDIENTE'
                END AS purchase_status,
                CASE
                    WHEN COALESCE(conf.is_invoiced, FALSE) THEN 'Facturada'
                    WHEN prob.score >= 60 THEN 'Probable facturada'
                    ELSE 'Pendiente'
                END AS estado_real,
                prob.score AS probable_score,
                CASE
                    WHEN prob.score >= 90 THEN 'PROBABLE_FACTURADA_HIGH'
                    WHEN prob.score >= 75 THEN 'PROBABLE_FACTURADA_MEDIUM'
                    WHEN prob.score >= 60 THEN 'PROBABLE_FACTURADA_LOW'
                    ELSE NULL
                END AS probable_tier,
                CASE
                    WHEN COALESCE(conf.is_invoiced, FALSE) THEN
                        CASE conf.invoicing_document_type_id
                            WHEN 1 THEN 'Boleta'
                            WHEN 6 THEN 'Factura'
                            ELSE 'Documento'
                        END
                        || ' '
                        || COALESCE(conf.invoicing_number::text, conf.invoicing_document_id::text)
                    WHEN prob.score >= 60 THEN
                        prob.candidate_document_type_label
                        || ' '
                        || COALESCE(prob.candidate_number::text, prob.candidate_document_id::text)
                    ELSE NULL
                END AS associated_document_label,
                CASE
                    WHEN COALESCE(conf.is_invoiced, FALSE) THEN 100::numeric
                    WHEN prob.score >= 60 THEN prob.score
                    ELSE NULL
                END AS display_score
"""


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


_PURCHASE_STATUS_FILTER_SQL = {
    "confirmed": "e.purchase_status = 'FACTURADA_CONFIRMADA'",
    "probable": (
        "e.purchase_status IN ("
        "'PROBABLE_FACTURADA_HIGH', "
        "'PROBABLE_FACTURADA_MEDIUM', "
        "'PROBABLE_FACTURADA_LOW'"
        ")"
    ),
    "pending": "e.purchase_status = 'PENDIENTE'",
}


def list_purchase_orders(
    *,
    only_not_invoiced: bool = False,
    invoice_status: str | None = None,
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
    status_key = (invoice_status or "").strip().lower()
    if status_key in _PURCHASE_STATUS_FILTER_SQL:
        where.append(_PURCHASE_STATUS_FILTER_SQL[status_key])
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
    from backend.services.distribuidora.sync_service import ADVISORY_LOCK_KEY

    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT process_name, last_sync, last_status, last_message, updated_at
            FROM distribuidora.sync_process_cursor
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
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


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
    limit: int = 250,
) -> list[dict[str, Any]]:
    """
    Resumen por comuna para pre‑planificación de despacho (órdenes de compra Bsale tipo 33).

    Filtro "solo no facturadas": sin fila en ``document_related`` hacia boleta/factura (tipos 1/6),
    vía detalles de la misma OC (no se usa ``state``).
    """
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    skip_day, day_like = _day_filter_sql_params(day_filter)
    lim = max(1, min(int(limit), 300))
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
            LIMIT %s
            """,
            (d0, d1, only_not_invoiced, skip_day, day_like, lim),
        )
        rows = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def _planning_rows_ids_sql(*, with_day_obs: bool) -> str:
    """Fase 1: IDs paginados sobre ``documents`` (filtro de fecha primero)."""
    obs_join = (
        ""
        if not with_day_obs
        else """
            INNER JOIN distribuidora.v_oc_attributes_flat obs_a
                ON obs_a.document_id = d.document_id
        """
    )
    day_clause = "TRUE" if not with_day_obs else f"{_PLANNING_ROWS_OBS_TEXT} LIKE %s"
    return f"""
            SELECT d.document_id
            FROM distribuidora.documents d
            {obs_join}
            LEFT JOIN distribuidora.v_orders_purchase_status conf_f
                ON conf_f.document_id = d.document_id
            WHERE {_DISPATCH_PREP_DOC_FILTER}
              AND {_DISPATCH_PREP_NOT_INVOICED_FILTER}
              AND CASE WHEN %s THEN TRUE ELSE {day_clause} END
            ORDER BY d.number DESC NULLS LAST, d.document_id DESC
            LIMIT %s OFFSET %s
            """


def _planning_rows_enrich_sql() -> str:
    """Fase 2: enriquecer solo los IDs de la página."""
    return f"""
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
                {_PLANNING_ROWS_STATUS_SELECT}
            FROM distribuidora.documents d
            {_PLANNING_ROWS_STATUS_JOINS}
            LEFT JOIN bsale.clients c
                ON c.company_id = d.company_id
               AND c.bsale_id = d.client_id
            WHERE d.document_id = ANY(%s::bigint[])
            ORDER BY d.number DESC NULLS LAST, d.document_id DESC
            """


def list_dispatch_prep_observation_texts(
    *,
    emission_date_from: date,
    emission_date_to: date,
    only_not_invoiced: bool = True,
    limit: int = DEFAULT_DISPATCH_PREP_LIMIT,
    offset: int = 0,
    day_filter: str | None = None,
) -> dict[str, Any]:
    """Textos de observaciones (OC) paginados; filtra por ``documents`` antes de agregar."""
    t0 = time.perf_counter()
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    lim = effective_page_limit(limit, d0, d1)
    off = max(0, int(offset))
    fetch = lim + 1
    skip_day, day_like = _day_filter_sql_params(day_filter)
    day_clause = "TRUE" if skip_day else f"{_PLANNING_ROWS_OBS_TEXT} LIKE %s"
    conn = get_connection()
    try:
        cur = conn.cursor()
        t_sql = time.perf_counter()
        params: tuple[Any, ...] = (d0, d1, only_not_invoiced, skip_day)
        if not skip_day:
            params = (*params, day_like)
        params = (*params, fetch, off)
        cur.execute(
            f"""
            SELECT obs_a.observaciones
            FROM distribuidora.documents d
            INNER JOIN distribuidora.v_oc_attributes_flat obs_a
                ON obs_a.document_id = d.document_id
            LEFT JOIN distribuidora.v_orders_purchase_status conf_f
                ON conf_f.document_id = d.document_id
            WHERE {_DISPATCH_PREP_DOC_FILTER}
              AND obs_a.observaciones IS NOT NULL
              AND BTRIM(obs_a.observaciones) <> ''
              AND {_DISPATCH_PREP_NOT_INVOICED_FILTER}
              AND CASE WHEN %s THEN TRUE ELSE {day_clause} END
            ORDER BY obs_a.observaciones
            LIMIT %s OFFSET %s
            """,
            params,
        )
        raw = cur.fetchall() or []
        sql_ms = round((time.perf_counter() - t_sql) * 1000.0, 2)
        has_more = len(raw) > lim
        out: list[str] = []
        for (text,) in raw[:lim]:
            if text is None:
                continue
            s = str(text).strip()
            if s:
                out.append(s)
        cur.close()
        meta = wide_range_meta(d0, d1)
        payload: dict[str, Any] = {
            "items": out,
            "has_more": has_more,
            "limit": lim,
            "offset": off,
            **meta,
        }
        total_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        log_dispatch_prep(
            "observaciones",
            date_from=d0,
            date_to=d1,
            sql_ms=sql_ms,
            total_ms=total_ms,
            rows_count=len(out),
            payload_bytes=payload_size_bytes(payload),
            limit=lim,
            offset=off,
        )
        return payload
    finally:
        conn.close()


def list_dispatch_prep_planning_rows(
    *,
    emission_date_from: date,
    emission_date_to: date,
    only_not_invoiced: bool = True,
    day_filter: str | None = None,
    limit: int = DEFAULT_DISPATCH_PREP_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Filas OC (33) para tabla de pre‑planificación.

    Dos fases: paginar IDs en ``documents`` (filtro fecha + índice 028), luego joins de estado.
    """
    timer = PlanningRowsTimer()
    t0 = time.perf_counter()
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    skip_day, day_like = _day_filter_sql_params(day_filter)
    lim = effective_page_limit(limit, d0, d1)
    off = max(0, int(offset))
    fetch = lim + 1
    ids_sql = _planning_rows_ids_sql(with_day_obs=not skip_day)
    ids_params: tuple[Any, ...] = (d0, d1, only_not_invoiced, skip_day)
    if not skip_day:
        ids_params = (*ids_params, day_like)
    ids_params = (*ids_params, fetch, off)

    conn = get_connection()
    sql_ms = 0.0
    try:
        cur = conn.cursor()
        t_sql = time.perf_counter()
        cur.execute(ids_sql, ids_params)
        id_rows = cur.fetchall() or []
        sql_ms = round((time.perf_counter() - t_sql) * 1000.0, 2)
        timer.mark("sql_ids")

        has_more = len(id_rows) > lim
        doc_ids = [int(r[0]) for r in id_rows[:lim]]
        if not doc_ids:
            cur.close()
            meta = wide_range_meta(d0, d1)
            payload: dict[str, Any] = {
                "items": [],
                "has_more": False,
                "limit": lim,
                "offset": off,
                **meta,
            }
            total_ms = round((time.perf_counter() - t0) * 1000.0, 2)
            log_dispatch_prep(
                "planning-rows",
                date_from=d0,
                date_to=d1,
                sql_ms=sql_ms,
                total_ms=total_ms,
                rows_count=0,
                payload_bytes=payload_size_bytes(payload),
                limit=lim,
                offset=off,
            )
            return payload

        t_enrich = time.perf_counter()
        cur.execute(_planning_rows_enrich_sql(), (doc_ids,))
        raw = cur.fetchall() or []
        sql_ms += round((time.perf_counter() - t_enrich) * 1000.0, 2)
        timer.mark("sql_enrich")

        rows = [_serialize_row(_row_to_dict(cur, r)) for r in raw]
        timer.mark("serialize")
        cur.close()

        meta = wide_range_meta(d0, d1)
        payload = {
            "items": rows,
            "has_more": has_more,
            "limit": lim,
            "offset": off,
            **meta,
        }
        total_ms = round((time.perf_counter() - t0) * 1000.0, 2)
        log_dispatch_prep(
            "planning-rows",
            date_from=d0,
            date_to=d1,
            sql_ms=sql_ms,
            total_ms=total_ms,
            rows_count=len(rows),
            payload_bytes=payload_size_bytes(payload),
            limit=lim,
            offset=off,
        )
        log_planning_rows(
            "done",
            total_ms=total_ms,
            sql_ms=sql_ms,
            row_count=len(rows),
            payload_bytes=payload_size_bytes(payload),
            phases=timer.phases,
        )
        return payload
    finally:
        conn.close()
