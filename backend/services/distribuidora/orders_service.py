"""Consultas de negocio: órdenes de compra (vista enriquecida + detalle)."""

from __future__ import annotations

import logging
import time
import unicodedata
from datetime import date
from decimal import Decimal
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)
from backend.utils.dispatch_prep_common import (
    DEFAULT_DISPATCH_PREP_LIMIT,
    effective_page_limit,
    log_dispatch_prep,
    payload_size_bytes,
    wide_range_meta,
)
from backend.utils.planning_rows_debug import (
    PlanningRowsTimer,
    attach_perf_debug,
    explain_analyze_enabled,
    log_planning_rows,
    run_explain_analyze,
)
from backend.utils.delivery_day_detect import (
    delivery_day_label,
    resolve_delivery_day,
    sql_resolve_delivery_day,
)
from backend.utils.planning_rows_stage import (
    PlanningRowsStageCollector,
    planning_rows_stage_enabled,
)
from backend.utils.request_audit import (
    RequestAudit,
    log_pg_connection_stats,
    slow_serialize_threshold_ms,
    timed_query,
)
from backend.utils.planning_sql_fragments import (
    PLANNING_LATEST_OBS_LATERAL,
    PLANNING_LAST_BS_UPDATE_EXPR,
    PLANNING_OBSERVACIONES_EXPR,
    PLANNING_WEIGHT_SELECT,
)

_DISPATCH_PREP_DOC_FILTER = """
    d.company_id = 3
    AND d.office_id = 1
    AND d.document_type_id = 33
    AND d.emission_date >= %s::date
    AND d.emission_date < (%s::date + interval '1 day')
""".strip()

# Facturación OC sin v_orders_purchase_status / v_documents_latest (misma semántica que 026).
_OC_IS_INVOICED_SQL = """
EXISTS (
    SELECT 1
    FROM distribuidora.document_details dd
    INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
    INNER JOIN distribuidora.documents inv
        ON inv.document_id = dr.related_document_id
       AND inv.document_type_id IN (1, 6)
       AND inv.company_id = d.company_id
       AND inv.office_id = d.office_id
    WHERE dd.document_id = d.document_id
)
""".strip()

_DISPATCH_PREP_NOT_INVOICED_FILTER = f"(%s = FALSE OR NOT {_OC_IS_INVOICED_SQL})"

_DAY_FILTER_ALLOW = frozenset({"lunes", "martes", "miercoles", "jueves", "viernes", "sabado"})

# OC (tipo 33): facturada si hay ``document_related`` desde un detalle de la OC hacia boleta/factura (1 o 6).
from backend.utils.distribuidora_oc_sql import OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL  # noqa: F401

OC_PURCHASE_ESTADO_REAL_SQL = """
CASE
    WHEN EXISTS (
        SELECT 1
        FROM distribuidora.document_related dr
        INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
        INNER JOIN distribuidora.documents inv
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

# Fase 2: facturación + probables en batch (sin LATERAL por fila ni vistas pesadas).
_PLANNING_ROWS_ENRICH_STATUS_JOINS = """
LEFT JOIN (
    SELECT DISTINCT ON (dd.document_id)
        dd.document_id AS oc_document_id,
        TRUE AS is_invoiced,
        inv.document_id AS invoicing_document_id,
        inv.document_type_id AS invoicing_document_type_id,
        inv.number AS invoicing_number
    FROM distribuidora.document_details dd
    INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
    INNER JOIN distribuidora.documents inv
        ON inv.document_id = dr.related_document_id
       AND inv.document_type_id IN (1, 6)
       AND inv.company_id = 3
       AND inv.office_id = 1
    INNER JOIN page_ids p ON p.document_id = dd.document_id
    ORDER BY dd.document_id, inv.emission_date DESC NULLS LAST, inv.document_id DESC
) conf ON conf.oc_document_id = d.document_id
LEFT JOIN (
    SELECT DISTINCT ON (pm.oc_document_id)
        pm.oc_document_id,
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
    INNER JOIN page_ids p ON p.document_id = pm.oc_document_id
    WHERE pm.score >= 60
    ORDER BY pm.oc_document_id, pm.score DESC, pm.candidate_document_id DESC
) prob ON prob.oc_document_id = d.document_id
   AND NOT COALESCE(conf.is_invoiced, FALSE)
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


def _parse_day_filters(raw: str | None) -> list[str]:
    """Uno o varios días (coma): jueves,viernes → OR en observaciones."""
    if raw is None or not str(raw).strip():
        return []
    seen: list[str] = []
    for part in str(raw).split(","):
        tok = _sanitize_day_filter(part)
        if tok and tok not in seen:
            seen.append(tok)
    return seen


def _day_resolved_filter_clause(
    observaciones_expr: str,
    comments_expr: str,
    dia_atencion_expr: str,
    day_filter: str | None,
) -> tuple[str, list[str]]:
    """Filtro exacto por día resuelto (observación > comentarios > ruta)."""
    tokens = _parse_day_filters(day_filter)
    if not tokens:
        return "TRUE", []
    resolved = sql_resolve_delivery_day(
        observaciones_expr,
        comments_expr,
        dia_atencion_expr,
    )
    placeholders = ", ".join(["%s"] * len(tokens))
    return f"({resolved} IN ({placeholders}))", list(tokens)


def _enrich_row_delivery_day(row: dict[str, Any]) -> None:
    obs = row.get("observaciones")
    comments = row.get("comments")
    dia_atencion = row.get("dia_atencion")
    day, source = resolve_delivery_day(obs, comments, dia_atencion)
    row["dia_entrega_detectado"] = day
    row["dia_entrega_fuente"] = source
    row["dia_entrega_label"] = delivery_day_label(day)


_PLANNING_ROWS_UNRENDERED_MARKERS = (
    "{_PLANNING_ROWS_STATUS_SELECT}",
    "{_PLANNING_ROWS_ENRICH_STATUS_JOINS}",
    "{_DISPATCH_PREP_NOT_INVOICED_FILTER}",
    "{day_clause}",
    "{PLANNING_WEIGHT_SELECT}",
    "{PLANNING_WEIGHT_PLACEHOLDER}",
    "{PLANNING_WEIGHT_LATERAL}",
)


def _assert_sql_template_rendered(sql: str, *, context: str = "planning_rows") -> None:
    for marker in _PLANNING_ROWS_UNRENDERED_MARKERS:
        if marker in sql:
            raise RuntimeError(f"SQL template not rendered ({context}): {marker}")


def _overlay_order_weights_to_rows(rows: list[dict[str, Any]]) -> None:
    """Peso desde OrderWeightSummary — misma fuente que el popup."""
    if not rows:
        return
    from backend.services.order_weight_service import (
        apply_order_weight_summary_to_row,
        get_order_weight_summaries_batch,
        metrics_to_order_weight_summary,
    )

    ids = [int(r["document_id"]) for r in rows if r.get("document_id") is not None]
    by_w = get_order_weight_summaries_batch(ids, persist_cache=True, log_planning=True)
    for row in rows:
        doc_id = int(row["document_id"])
        w = by_w.get(doc_id)
        if not w:
            logger.warning(
                "[PLANNING_WEIGHT] order_id=%s weight_source=overlay_missing total_weight=null",
                doc_id,
            )
            continue
        summary = metrics_to_order_weight_summary(w)
        apply_order_weight_summary_to_row(
            row,
            summary,
            weight_source="order_weight_summary",
            extras={
                "cantidad_unidades": w.get("cantidad_unidades"),
                "cantidad_cajas": w.get("cantidad_cajas"),
                "productos_totales": w.get("productos_totales"),
            },
        )


def _apply_live_sync_flags(row: dict[str, Any]) -> None:
    from backend.utils.order_live_metrics import bsale_ahead_of_erp_sync

    row["bsale_updated_pending"] = bsale_ahead_of_erp_sync(
        row.get("last_bs_update"),
        row.get("last_erp_update"),
    )


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
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], bool]:
    """Lista OC enriquecidas paginadas.

    Devuelve ``(rows, has_more)``. Se pide ``limit + 1`` filas en vez de un
    ``COUNT(*)`` sobre la vista completa (costo prohibitivo por los LATERAL).
    """
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
        params2 = list(params)
        params2.extend([limit + 1, offset])
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
        has_more = len(rows) > limit
        return rows[:limit], has_more
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
    day_clause, day_params = _day_resolved_filter_clause(
        """NULLIF(BTRIM((
            SELECT da.attribute_value
            FROM distribuidora.document_attributes da
            WHERE da.document_id = d.document_id
              AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
            ORDER BY da.id DESC NULLS LAST
            LIMIT 1
        )), '')""",
        "NULLIF(BTRIM(d.raw_data->>'comments'), '')",
        """NULLIF(BTRIM((
            SELECT c_inner.dia_atencion
            FROM bsale.clients c_inner
            WHERE c_inner.company_id = d.company_id
              AND c_inner.bsale_id = d.client_id
            LIMIT 1
        )), '')""",
        day_filter,
    )
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
              AND {day_clause}
            GROUP BY 1
            ORDER BY total_ventas DESC NULLS LAST, municipality ASC
            LIMIT %s
            """,
            (d0, d1, only_not_invoiced, *day_params, lim),
        )
        rows = [_serialize_row(_row_to_dict(cur, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def _planning_rows_ids_sql(*, day_tokens: list[str]) -> str:
    """Fase 1: IDs paginados sobre ``documents`` (filtro de fecha primero)."""
    obs_join = ""
    if day_tokens:
        obs_join = """
            LEFT JOIN distribuidora.v_oc_attributes_flat obs_a
                ON obs_a.document_id = d.document_id
            LEFT JOIN bsale.clients c_day
                ON c_day.company_id = d.company_id
               AND c_day.bsale_id = d.client_id
        """
        day_clause, _ = _day_resolved_filter_clause(
            "obs_a.observaciones",
            "d.raw_data->>'comments'",
            "c_day.dia_atencion",
            ",".join(day_tokens),
        )
    else:
        day_clause = "TRUE"
    return f"""
            SELECT d.document_id
            FROM distribuidora.documents d
            {obs_join}
            WHERE {_DISPATCH_PREP_DOC_FILTER}
              AND {_DISPATCH_PREP_NOT_INVOICED_FILTER}
              AND {day_clause}
            ORDER BY d.number DESC NULLS LAST, d.document_id DESC
            LIMIT %s OFFSET %s
            """


def _planning_rows_enrich_sql() -> str:
    """Fase 2: enriquecer solo los IDs de la página (CTE page_ids + joins batch)."""
    return f"""
            WITH page_ids AS (
                SELECT unnest(%s::bigint[]) AS document_id
            )
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
                {PLANNING_OBSERVACIONES_EXPR} AS observaciones,
                NULLIF(BTRIM(d.raw_data->>'comments'), '') AS comments,
                NULLIF(BTRIM(c.dia_atencion), '') AS dia_atencion,
                {PLANNING_LAST_BS_UPDATE_EXPR} AS last_bs_update,
                d.updated_at AS last_erp_update,
                {PLANNING_WEIGHT_SELECT},
                {_PLANNING_ROWS_STATUS_SELECT}
            FROM distribuidora.documents d
            INNER JOIN page_ids pi ON pi.document_id = d.document_id
            {_PLANNING_ROWS_ENRICH_STATUS_JOINS}
            {PLANNING_LATEST_OBS_LATERAL}
            LEFT JOIN bsale.clients c
                ON c.company_id = d.company_id
               AND c.bsale_id = d.client_id
            ORDER BY d.number DESC NULLS LAST, d.document_id DESC
            """


def _planning_rows_use_monolith_enrich() -> bool:
    import os

    return os.environ.get("PLANNING_ROWS_MONOLITH_ENRICH", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _planning_rows_base_orders_sql() -> str:
    return f"""
            WITH page_ids AS (
                SELECT unnest(%s::bigint[]) AS document_id
            )
            SELECT
                d.document_id,
                d.number AS oc,
                d.client_id,
                d.company_id,
                NULLIF(BTRIM(d.municipality), '') AS municipality,
                NULLIF(BTRIM(d.address), '') AS direccion,
                NULLIF(BTRIM(d.seller_name), '') AS seller_name,
                d.total_amount,
                {PLANNING_OBSERVACIONES_EXPR} AS observaciones,
                NULLIF(BTRIM(d.raw_data->>'comments'), '') AS comments,
                {PLANNING_LAST_BS_UPDATE_EXPR} AS last_bs_update,
                d.updated_at AS last_erp_update,
                {PLANNING_WEIGHT_SELECT}
            FROM distribuidora.documents d
            INNER JOIN page_ids pi ON pi.document_id = d.document_id
            {PLANNING_LATEST_OBS_LATERAL}
            ORDER BY d.number DESC NULLS LAST, d.document_id DESC
            """


def _planning_rows_purchase_status_sql() -> str:
    return """
            WITH page_ids AS (
                SELECT unnest(%s::bigint[]) AS document_id
            )
            SELECT DISTINCT ON (dd.document_id)
                dd.document_id AS oc_document_id,
                TRUE AS is_invoiced,
                inv.document_id AS invoicing_document_id,
                inv.document_type_id AS invoicing_document_type_id,
                inv.number AS invoicing_number
            FROM distribuidora.document_details dd
            INNER JOIN distribuidora.document_related dr ON dr.detail_id = dd.detail_id
            INNER JOIN distribuidora.documents inv
                ON inv.document_id = dr.related_document_id
               AND inv.document_type_id IN (1, 6)
               AND inv.company_id = 3
               AND inv.office_id = 1
            INNER JOIN page_ids p ON p.document_id = dd.document_id
            ORDER BY dd.document_id, inv.emission_date DESC NULLS LAST, inv.document_id DESC
            """


def _planning_rows_probable_matches_sql() -> str:
    return """
            WITH page_ids AS (
                SELECT unnest(%s::bigint[]) AS document_id
            )
            SELECT DISTINCT ON (pm.oc_document_id)
                pm.oc_document_id,
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
            INNER JOIN page_ids p ON p.document_id = pm.oc_document_id
            WHERE pm.score >= 60
            ORDER BY pm.oc_document_id, pm.score DESC, pm.candidate_document_id DESC
            """


def _planning_rows_observaciones_sql() -> str:
    return """
            SELECT
                obs_a.document_id,
                obs_a.observaciones
            FROM distribuidora.v_oc_attributes_flat obs_a
            WHERE obs_a.document_id = ANY(%s::bigint[])
              AND obs_a.observaciones IS NOT NULL
              AND BTRIM(obs_a.observaciones) <> ''
            """


def _planning_rows_georef_sql() -> str:
    return """
            SELECT
                c.company_id,
                c.bsale_id AS client_id,
                NULLIF(BTRIM(c.nombre_fantasia), '') AS nombre_fantasia,
                NULLIF(BTRIM(c.municipality), '') AS municipality,
                NULLIF(BTRIM(c.address), '') AS address,
                NULLIF(BTRIM(c.dia_atencion), '') AS dia_atencion,
                c.lat,
                c.lon
            FROM bsale.clients c
            WHERE c.company_id = %s
              AND c.bsale_id = ANY(%s::bigint[])
            """


def _apply_status_fields_to_row(
    row: dict[str, Any],
    conf: dict[str, Any] | None,
    prob: dict[str, Any] | None,
) -> None:
    is_inv = bool(conf and conf.get("is_invoiced"))
    score = prob.get("score") if prob and not is_inv else None
    score_f = float(score) if score is not None else None

    if is_inv:
        purchase_status = "FACTURADA_CONFIRMADA"
        estado_real = "Facturada"
        probable_tier = None
    elif score_f is not None and score_f >= 90:
        purchase_status = "PROBABLE_FACTURADA_HIGH"
        estado_real = "Probable facturada"
        probable_tier = "PROBABLE_FACTURADA_HIGH"
    elif score_f is not None and score_f >= 75:
        purchase_status = "PROBABLE_FACTURADA_MEDIUM"
        estado_real = "Probable facturada"
        probable_tier = "PROBABLE_FACTURADA_MEDIUM"
    elif score_f is not None and score_f >= 60:
        purchase_status = "PROBABLE_FACTURADA_LOW"
        estado_real = "Probable facturada"
        probable_tier = "PROBABLE_FACTURADA_LOW"
    else:
        purchase_status = "PENDIENTE"
        estado_real = "Pendiente"
        probable_tier = None

    row["purchase_status"] = purchase_status
    row["estado_real"] = estado_real
    row["probable_score"] = score if score_f is not None and score_f >= 60 else None
    row["probable_tier"] = probable_tier

    if is_inv and conf:
        tipo = conf.get("invoicing_document_type_id")
        tipo_lbl = (
            "Boleta"
            if tipo == 1
            else "Factura"
            if tipo == 6
            else "Documento"
        )
        num = conf.get("invoicing_number") or conf.get("invoicing_document_id")
        row["associated_document_label"] = f"{tipo_lbl} {num}"
        row["display_score"] = 100
    elif score_f is not None and score_f >= 60 and prob:
        lbl = prob.get("candidate_document_type_label") or ""
        num = prob.get("candidate_number") or prob.get("candidate_document_id")
        row["associated_document_label"] = f"{lbl} {num}".strip()
        row["display_score"] = score
    else:
        row["associated_document_label"] = None
        row["display_score"] = None


def _merge_planning_rows_staged(
    base_rows: list[dict[str, Any]],
    conf_by_doc: dict[int, dict[str, Any]],
    prob_by_doc: dict[int, dict[str, Any]],
    geo_by_client: dict[tuple[int, int], dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for base in base_rows:
        doc_id = int(base["document_id"])
        company_id = int(base.get("company_id") or 3)
        client_id = base.get("client_id")
        conf = conf_by_doc.get(doc_id)
        prob = prob_by_doc.get(doc_id) if not (conf and conf.get("is_invoiced")) else None
        geo = (
            geo_by_client.get((company_id, int(client_id)))
            if client_id is not None
            else None
        )
        row: dict[str, Any] = {
            "document_id": doc_id,
            "oc": base.get("oc"),
            "client_id": client_id,
            "nombre_fantasia": (geo or {}).get("nombre_fantasia"),
            "municipality": base.get("municipality")
            or (geo or {}).get("municipality"),
            "direccion": base.get("direccion") or (geo or {}).get("address"),
            "seller_name": base.get("seller_name"),
            "total_amount": base.get("total_amount"),
            "observaciones": base.get("observaciones"),
            "comments": base.get("comments"),
            "weight_kg": base.get("weight_kg"),
            "peso_total_kg": base.get("peso_total_kg"),
            "productos_sin_peso": base.get("productos_sin_peso"),
            "porcentaje_cobertura_peso": base.get("porcentaje_cobertura_peso"),
            "last_bs_update": base.get("last_bs_update"),
            "last_erp_update": base.get("last_erp_update"),
            "dia_atencion": (geo or {}).get("dia_atencion"),
            "has_georef": bool(
                geo and geo.get("lat") is not None and geo.get("lon") is not None
            ),
            "lat": float(geo["lat"]) if geo and geo.get("lat") is not None else None,
            "lng": float(geo["lon"]) if geo and geo.get("lon") is not None else None,
        }
        _apply_status_fields_to_row(row, conf, prob)
        _enrich_row_delivery_day(row)
        _apply_live_sync_flags(row)
        out.append(row)
    return out


def _fetch_planning_rows_staged(
    cur,
    doc_ids: list[int],
    stages: PlanningRowsStageCollector,
    audit: RequestAudit | None = None,
) -> list[dict[str, Any]]:
    """Carga por etapas instrumentadas (misma semántica que enrich monolítico)."""
    params = (doc_ids,)

    base_sql = _planning_rows_base_orders_sql()
    _assert_sql_template_rendered(base_sql, context="_planning_rows_base_orders_sql")
    ms = timed_query(cur, "planning_rows_base_orders", base_sql, params, audit=audit)
    base_raw = cur.fetchall() or []
    base_rows = [_row_to_dict(cur, r) for r in base_raw]
    stages.record(
        "load_base_orders",
        elapsed_ms=ms,
        rows_count=len(base_rows),
        step="document_fields",
    )

    ms = timed_query(
        cur,
        "planning_rows_purchase_status",
        _planning_rows_purchase_status_sql(),
        params,
        audit=audit,
    )
    conf_rows = [_row_to_dict(cur, r) for r in cur.fetchall() or []]
    conf_by_doc = {int(r["oc_document_id"]): r for r in conf_rows}
    stages.record(
        "load_purchase_status",
        elapsed_ms=ms,
        rows_count=len(conf_rows),
    )

    ms = timed_query(
        cur,
        "planning_rows_probable_matches",
        _planning_rows_probable_matches_sql(),
        params,
        audit=audit,
    )
    prob_rows = [_row_to_dict(cur, r) for r in cur.fetchall() or []]
    prob_by_doc = {int(r["oc_document_id"]): r for r in prob_rows}
    stages.record(
        "load_probable_matches",
        elapsed_ms=ms,
        rows_count=len(prob_rows),
    )

    ms = timed_query(
        cur,
        "planning_rows_observaciones",
        _planning_rows_observaciones_sql(),
        params,
        audit=audit,
    )
    obs_rows = cur.fetchall() or []
    stages.record(
        "load_observaciones",
        elapsed_ms=ms,
        rows_count=len(obs_rows),
    )

    client_ids = sorted(
        {int(r["client_id"]) for r in base_rows if r.get("client_id") is not None}
    )
    geo_by_client: dict[tuple[int, int], dict[str, Any]] = {}
    ms = 0.0
    if client_ids:
        ms = timed_query(
            cur,
            "planning_rows_georef",
            _planning_rows_georef_sql(),
            (3, client_ids),
            audit=audit,
        )
        for r in cur.fetchall() or []:
            d = _row_to_dict(cur, r)
            geo_by_client[(int(d["company_id"]), int(d["client_id"]))] = d
    stages.record(
        "load_georef",
        elapsed_ms=ms,
        rows_count=len(geo_by_client),
    )

    return _merge_planning_rows_staged(base_rows, conf_by_doc, prob_by_doc, geo_by_client)


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
    day_clause, day_params = _day_resolved_filter_clause(
        "obs_a.observaciones",
        "d.raw_data->>'comments'",
        "c_day.dia_atencion",
        day_filter,
    )
    conn = get_connection()
    try:
        cur = conn.cursor()
        t_sql = time.perf_counter()
        params: tuple[Any, ...] = (
            d0,
            d1,
            only_not_invoiced,
            *day_params,
            fetch,
            off,
        )
        cur.execute(
            f"""
            SELECT obs_a.observaciones
            FROM distribuidora.documents d
            INNER JOIN distribuidora.v_oc_attributes_flat obs_a
                ON obs_a.document_id = d.document_id
            LEFT JOIN bsale.clients c_day
                ON c_day.company_id = d.company_id
               AND c_day.bsale_id = d.client_id
            WHERE {_DISPATCH_PREP_DOC_FILTER}
              AND obs_a.observaciones IS NOT NULL
              AND BTRIM(obs_a.observaciones) <> ''
              AND {_DISPATCH_PREP_NOT_INVOICED_FILTER}
              AND {day_clause}
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

    Envoltura de auditoría (diagnóstico ECONNRESET): captura CUALQUIER excepción
    fatal con stack completo, última query ejecutada y último paso alcanzado,
    y luego re-lanza sin alterar el comportamiento.
    """
    audit = RequestAudit("planning-rows")
    try:
        return _list_dispatch_prep_planning_rows_impl(
            audit,
            emission_date_from=emission_date_from,
            emission_date_to=emission_date_to,
            only_not_invoiced=only_not_invoiced,
            day_filter=day_filter,
            limit=limit,
            offset=offset,
        )
    except BaseException as exc:  # nunca perder un traceback (incluye CancelledError)
        audit.log_fatal(exc)
        raise


def _list_dispatch_prep_planning_rows_impl(
    audit: RequestAudit,
    *,
    emission_date_from: date,
    emission_date_to: date,
    only_not_invoiced: bool = True,
    day_filter: str | None = None,
    limit: int = DEFAULT_DISPATCH_PREP_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    """
    Cuerpo original de planning-rows (lógica comercial intacta).

    Instrumentación ``[PLANNING_ROWS_STAGE]`` por etapa (ver ``planning_rows_stage.py``)
    + ``[REQUEST_AUDIT]`` (request_id, memoria, connection_id, SLOW QUERY).
    Por defecto carga desglosada por etapa; ``PLANNING_ROWS_MONOLITH_ENRICH=1`` usa un
    solo SQL enrich (comparable con latencia histórica ~35s).
    """
    timer = PlanningRowsTimer()
    stages = PlanningRowsStageCollector()
    d0, d1 = _normalize_date_range(emission_date_from, emission_date_to)
    day_tokens = _parse_day_filters(day_filter)
    lim = effective_page_limit(limit, d0, d1)
    off = max(0, int(offset))
    fetch = lim + 1
    ids_sql = _planning_rows_ids_sql(day_tokens=day_tokens)
    enrich_sql = _planning_rows_enrich_sql()
    _assert_sql_template_rendered(enrich_sql, context="_planning_rows_enrich_sql")
    ids_params: tuple[Any, ...] = (
        d0,
        d1,
        only_not_invoiced,
        *day_tokens,
        fetch,
        off,
    )
    use_monolith = _planning_rows_use_monolith_enrich()
    audit.step("parse_params", limit=lim, offset=off, date_from=d0, date_to=d1)

    conn = get_connection()
    sql_ids_ms = 0.0
    sql_enrich_ms = 0.0
    explains: list[dict[str, Any]] = []
    try:
        # Lecturas: autocommit evita idle in transaction / locks retenidos entre queries
        # (si el proxy Next corta a ~30s, no dejamos AccessShareLock colgado).
        conn.autocommit = True
        cur = conn.cursor()
        audit.step("db_connect", pg_pid=cur.connection.get_backend_pid(), autocommit=True)
        log_pg_connection_stats(cur, label="before")
        if explain_analyze_enabled():
            try:
                explains.append(
                    run_explain_analyze(
                        cur, ids_sql, ids_params, label="sql_ids"
                    ),
                )
            except Exception as exc:
                log_planning_rows("explain_ids_failed", error=repr(exc))
                try:
                    if not conn.autocommit:
                        conn.rollback()
                except Exception:
                    pass

        _assert_sql_template_rendered(ids_sql, context="_planning_rows_ids_sql")
        sql_ids_ms = timed_query(cur, "sql_ids", ids_sql, ids_params, audit=audit)
        id_rows = cur.fetchall() or []
        timer.mark("sql_ids")
        audit.step("sql_ids", rows=len(id_rows), execution_ms=sql_ids_ms)

        has_more = len(id_rows) > lim
        doc_ids = [int(r[0]) for r in id_rows[:lim]]
        meta = wide_range_meta(d0, d1)

        if not doc_ids:
            log_pg_connection_stats(cur, label="after")
            cur.close()
            audit.step("empty_result")
            t_sum = time.perf_counter()
            payload: dict[str, Any] = {
                "items": [],
                "has_more": False,
                "limit": lim,
                "offset": off,
                **meta,
            }
            stages.record(
                "build_summary",
                elapsed_ms=(time.perf_counter() - t_sum) * 1000.0,
                rows_count=0,
            )
            t_json = time.perf_counter()
            pbytes = payload_size_bytes(payload)
            stages.record(
                "serialize_response",
                elapsed_ms=(time.perf_counter() - t_json) * 1000.0,
                rows_count=0,
                payload_size=pbytes,
            )
            stage_report = stages.finish(rows_count=0)
            if explain_analyze_enabled():
                attach_perf_debug(
                    payload,
                    timer=timer,
                    sql_ids_ms=sql_ids_ms,
                    sql_enrich_ms=0.0,
                    serialize_ms=0.0,
                    json_ms=0.0,
                    explains=explains if explain_analyze_enabled() else None,
                )
            if planning_rows_stage_enabled():
                payload["_stage_profile"] = stage_report
            log_dispatch_prep(
                "planning-rows",
                date_from=d0,
                date_to=d1,
                sql_ms=sql_ids_ms,
                total_ms=stage_report["total_ms"],
                rows_count=0,
                payload_bytes=pbytes,
                limit=lim,
                offset=off,
            )
            audit.memory_report(payload_bytes=pbytes)
            audit.step("request_end", rows=0, payload_bytes=pbytes)
            return payload

        if use_monolith:
            enrich_params: tuple[Any, ...] = (doc_ids,)
            if explain_analyze_enabled():
                try:
                    explains.append(
                        run_explain_analyze(
                            cur, enrich_sql, enrich_params, label="sql_enrich"
                        ),
                    )
                except Exception as exc:
                    log_planning_rows("explain_enrich_failed", error=repr(exc))
                    try:
                        if not conn.autocommit:
                            conn.rollback()
                    except Exception:
                        pass
            _assert_sql_template_rendered(enrich_sql, context="_planning_rows_enrich_sql")
            sql_enrich_ms = timed_query(
                cur, "sql_enrich_monolith", enrich_sql, enrich_params, audit=audit
            )
            raw = cur.fetchall() or []
            timer.mark("sql_enrich")
            audit.step("sql_enrich", rows=len(raw), execution_ms=sql_enrich_ms)
            enrich_ms = sql_enrich_ms
            stages.record(
                "load_base_orders",
                elapsed_ms=sql_ids_ms,
                rows_count=len(doc_ids),
                step="pagination",
            )
            stages.record(
                "load_purchase_status",
                elapsed_ms=enrich_ms,
                rows_count=len(raw),
                mode="monolith_enrich_combined",
            )
            stages.record(
                "load_probable_matches",
                elapsed_ms=0.0,
                rows_count=0,
                mode="included_in_monolith",
            )
            stages.record(
                "load_observaciones",
                elapsed_ms=0.0,
                rows_count=0,
                mode="not_in_endpoint",
            )
            stages.record(
                "load_georef",
                elapsed_ms=0.0,
                rows_count=len(raw),
                mode="included_in_monolith",
            )
            t_build = time.perf_counter()
            merged = [_serialize_row(_row_to_dict(cur, r)) for r in raw]
            for row in merged:
                _enrich_row_delivery_day(row)
                _apply_live_sync_flags(row)
            _overlay_order_weights_to_rows(merged)
            stages.record(
                "build_rows",
                elapsed_ms=(time.perf_counter() - t_build) * 1000.0,
                rows_count=len(merged),
                mode="monolith",
            )
        else:
            if planning_rows_stage_enabled():
                stages.record(
                    "load_base_orders",
                    elapsed_ms=sql_ids_ms,
                    rows_count=len(doc_ids),
                    step="pagination",
                )
            merged_raw = _fetch_planning_rows_staged(cur, doc_ids, stages, audit=audit)
            t_build = time.perf_counter()
            merged = [_serialize_row(r) for r in merged_raw]
            _overlay_order_weights_to_rows(merged)
            stages.record(
                "build_rows",
                elapsed_ms=(time.perf_counter() - t_build) * 1000.0,
                rows_count=len(merged),
            )
            sql_enrich_ms = stages.sum_elapsed(
                "load_purchase_status",
                "load_probable_matches",
                "load_observaciones",
                "load_georef",
            )

        audit.step("build_rows", rows=len(merged))
        log_pg_connection_stats(cur, label="after")
        cur.close()

        t_sum = time.perf_counter()
        payload = {
            "items": merged,
            "has_more": has_more,
            "limit": lim,
            "offset": off,
            **meta,
        }
        stages.record(
            "build_summary",
            elapsed_ms=(time.perf_counter() - t_sum) * 1000.0,
            rows_count=len(merged),
        )

        t_json = time.perf_counter()
        pbytes = payload_size_bytes(payload)
        json_ms = round((time.perf_counter() - t_json) * 1000.0, 2)
        stages.record(
            "serialize_response",
            elapsed_ms=json_ms,
            rows_count=len(merged),
            payload_size=pbytes,
        )
        audit.step(
            "serialize_json",
            json_dumps_ms=json_ms,
            payload_kb=round(pbytes / 1024.0, 1),
            rows=len(merged),
        )
        if json_ms > slow_serialize_threshold_ms():
            logger.warning(
                "[REQUEST_AUDIT] SLOW_SERIALIZE request_id=%s json_dumps_ms=%s "
                "payload_kb=%s rows=%s threshold_ms=%s",
                audit.request_id,
                json_ms,
                round(pbytes / 1024.0, 1),
                len(merged),
                slow_serialize_threshold_ms(),
            )
        stage_report = stages.finish(rows_count=len(merged))

        if explain_analyze_enabled():
            attach_perf_debug(
                payload,
                timer=timer,
                sql_ids_ms=sql_ids_ms,
                sql_enrich_ms=sql_enrich_ms,
                serialize_ms=json_ms,
                json_ms=json_ms,
                explains=explains,
            )
        if planning_rows_stage_enabled():
            payload["_stage_profile"] = stage_report

        total_ms = stage_report["total_ms"]
        top_stage = (
            stage_report["stages"][0]["stage"] if stage_report.get("stages") else None
        )
        log_dispatch_prep(
            "planning-rows",
            date_from=d0,
            date_to=d1,
            sql_ms=round(sql_ids_ms + sql_enrich_ms, 2),
            total_ms=total_ms,
            rows_count=len(merged),
            payload_bytes=pbytes,
            limit=lim,
            offset=off,
            sql_ids_ms=sql_ids_ms,
            sql_enrich_ms=sql_enrich_ms,
            slowest_stage=top_stage,
            enrich_mode="monolith" if use_monolith else "staged",
        )
        log_planning_rows(
            "phase_ranking",
            total_ms=total_ms,
            sql_ids_ms=sql_ids_ms,
            sql_enrich_ms=sql_enrich_ms,
            row_count=len(merged),
            stage_profile=stage_report,
        )
        if explains:
            for ex in explains:
                log_planning_rows(
                    "explain",
                    label=ex.get("label"),
                    issues=ex.get("issues"),
                    top_node=(ex.get("top_nodes") or [None])[0],
                )
        audit.memory_report(payload_bytes=pbytes)
        audit.step(
            "request_end",
            rows=len(merged),
            payload_bytes=pbytes,
            total_ms=total_ms,
        )
        return payload
    finally:
        conn.close()
        audit.step("db_connection_closed")


def fetch_planning_live_by_document_ids(document_ids: list[int]) -> list[dict[str, Any]]:
    """Métricas live por OC (peso, monto, día, timestamps) para refrescar planificación."""
    from backend.utils.order_live_metrics import fetch_live_metrics_by_document_ids

    ids = list(dict.fromkeys(int(x) for x in document_ids if x))
    if not ids:
        return []
    conn = get_connection()
    try:
        cur = conn.cursor()
        by_id = fetch_live_metrics_by_document_ids(cur, ids)
        cur.close()
    finally:
        conn.close()
    return [_serialize_row(by_id[i]) for i in ids if i in by_id]
