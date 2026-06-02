"""Planificación por camión: confirmación, snapshot, facturación y picking."""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from datetime import date, datetime, timezone
from decimal import Decimal
from io import BytesIO
from typing import Any

import pandas as pd

from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_picking_repo as picking_repo
from backend.repositories.distribuidora import dispatch_plan_repo as repo
from backend.services.distribuidora.dispatch_commercial_margin_service import (
    audit_bsale_margin_fields,
    compute_plan_commercial_margin,
)
from backend.utils.json_safe import serialize_row, serialize_rows, serialize_value
from backend.utils.plan_debug import (
    PLAN_DEBUG_RERAISE,
    log_plan_debug_context,
    plan_debug_on_error,
)
from backend.utils.ors_stability import (
    empty_invoicing_payload,
    empty_invoiced_documents_response,
    empty_picking_by_client_response,
    empty_picking_by_product_response,
    empty_plan_dashboard,
    log_debug,
    log_error,
)
from backend.utils.dashboard_stage import (
    PICKING_CLIENT_COUNT_SQL,
    PICKING_PRODUCT_COUNT_SQL,
    DashboardStageRun,
    dashboard_connection,
    log_repo_end,
    log_repo_start,
)
from backend.utils.deadlock_debug import timed_execute
from backend.utils.invoicing_auto_confirm import (
    apply_operational_invoicing_rows,
    invoicing_summary_counts,
)
from backend.utils.dispatch_plan_picking import (
    build_picking_client_excel,
    build_picking_product_excel,
    fetch_picking_header,
    inv_status_sql_filter,
    normalize_client_stop,
    normalize_product_row,
    picking_warnings_from_stops,
)
from backend.utils.picking_readiness import (
    PICKING_WAIT_MESSAGE,
    evaluate_picking_readiness,
    picking_block_from_invoicing,
    picking_not_ready_payload,
)
from backend.utils.picking_sql import (
    CAJAS_AGG_EXPR,
    CAJAS_LINE_EXPR,
    PM_JOIN,
    PM_TIPO_PRODUCTO_EXPR,
    VARIANTS_JOIN,
)
from backend.utils.plan_detail_debug import log_plan_detail_debug, plan_detail_step

logger = logging.getLogger(__name__)


class InvoicingDocumentsUnavailable(Exception):
    """Vista full y fallback lite fallaron; el dashboard debe degradar, no 500."""

    def __init__(
        self,
        full_view_error: str,
        lite_exc: Exception | None = None,
    ) -> None:
        self.full_view_error = full_view_error
        self.lite_error = (
            f"{type(lite_exc).__name__}: {lite_exc}" if lite_exc is not None else None
        )
        super().__init__(full_view_error)


def _invoicing_connection_rollback(conn: Any, *, plan_id: int, label: str) -> None:
    try:
        conn.rollback()
        logger.info("[INVOICING_ROLLBACK] plan_id=%s label=%s", plan_id, label)
    except Exception as rb_exc:
        logger.warning(
            "[INVOICING_ROLLBACK] plan_id=%s label=%s failed: %s",
            plan_id,
            label,
            rb_exc,
        )


def _format_invoicing_exc(exc: Exception) -> str:
    return f"{type(exc).__name__}: {exc}"


VALID_STATUSES = frozenset(
    {
        "draft",
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
        "delivered",
    }
)

MARGIN_VIEW_ROLES = frozenset(
    {
        "admin",
        "superadmin",
        "super_admin",
        "administrator",
        "finanzas",
        "finance",
        "gerencia",
    }
)


def _pick(*values: Any) -> Any:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _enrich_orders_snapshot(cur, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Congela datos operativos desde OC + atributos + vendedores (no solo vista parcial)."""
    ids = [int(o["oc_document_id"]) for o in orders if o.get("oc_document_id")]
    if not ids:
        return orders
    cur.execute(
        """
        SELECT
            d.document_id,
            d.number,
            d.client_id,
            COALESCE(
                NULLIF(BTRIM(p.nombre_fantasia), ''),
                NULLIF(BTRIM(fa.nombre_fantasia), ''),
                NULLIF(BTRIM(c.nombre_fantasia), ''),
                NULLIF(BTRIM(c.company), ''),
                CONCAT_WS(
                    ' ',
                    NULLIF(BTRIM(c.first_name), ''),
                    NULLIF(BTRIM(c.last_name), '')
                )
            ) AS client_name,
            COALESCE(
                NULLIF(BTRIM(p.nombre_fantasia), ''),
                NULLIF(BTRIM(fa.nombre_fantasia), ''),
                NULLIF(BTRIM(c.nombre_fantasia), '')
            ) AS fantasy_name,
            COALESCE(
                NULLIF(BTRIM(p.address), ''),
                NULLIF(BTRIM(d.address), ''),
                NULLIF(BTRIM(c.address), '')
            ) AS address,
            COALESCE(
                NULLIF(BTRIM(p.city), ''),
                NULLIF(BTRIM(p.municipality), ''),
                NULLIF(BTRIM(d.municipality), ''),
                NULLIF(BTRIM(d.city), ''),
                NULLIF(BTRIM(c.municipality), ''),
                NULLIF(BTRIM(c.city), '')
            ) AS city,
            COALESCE(
                NULLIF(BTRIM(p.forma_pago), ''),
                NULLIF(BTRIM(attr_pay.attribute_value), '')
            ) AS payment_method,
            COALESCE(
                NULLIF(BTRIM(p.tipo_documento_a_generar), ''),
                NULLIF(BTRIM(attr_tipo.attribute_value), '')
            ) AS document_type_to_generate,
            COALESCE(
                NULLIF(BTRIM(p.seller_name), ''),
                NULLIF(BTRIM(ds.seller_name), ''),
                NULLIF(BTRIM(d.seller_name), '')
            ) AS seller_name,
            COALESCE(p.total_amount, d.total_amount) AS total_amount
        FROM distribuidora.v_documents_latest d
        LEFT JOIN distribuidora.v_orders_purchase p ON p.document_id = d.document_id
        LEFT JOIN distribuidora.v_oc_attributes_flat fa ON fa.document_id = d.document_id
        LEFT JOIN bsale.clients c
            ON c.company_id = 3 AND c.bsale_id = d.client_id
        LEFT JOIN LATERAL (
            SELECT s.seller_name
            FROM distribuidora.document_sellers s
            WHERE s.document_id = d.document_id
            ORDER BY s.id ASC
            LIMIT 1
        ) ds ON TRUE
        LEFT JOIN LATERAL (
            SELECT da.attribute_value
            FROM distribuidora.document_attributes da
            WHERE da.document_id = d.document_id
              AND UPPER(BTRIM(da.attribute_name)) = 'FORMA DE PAGO'
            ORDER BY da.id DESC NULLS LAST
            LIMIT 1
        ) attr_pay ON TRUE
        LEFT JOIN LATERAL (
            SELECT da.attribute_value
            FROM distribuidora.document_attributes da
            WHERE da.document_id = d.document_id
              AND UPPER(BTRIM(da.attribute_name)) IN (
                  'TIPO DE DOCUMENTO A GENERAR',
                  'TIPO DOCUMENTO A GENERAR'
              )
            ORDER BY da.id DESC NULLS LAST
            LIMIT 1
        ) attr_tipo ON TRUE
        WHERE d.document_id = ANY(%s)
          AND d.company_id = 3
        """,
        (ids,),
    )
    cols = [c[0] for c in cur.description]
    by_id = {int(r[0]): dict(zip(cols, r)) for r in cur.fetchall()}
    out: list[dict[str, Any]] = []
    for o in orders:
        row = dict(o)
        src = by_id.get(int(row["oc_document_id"])) or {}
        row["oc_number"] = _pick(row.get("oc_number"), src.get("number"))
        row["client_id"] = _pick(row.get("client_id"), src.get("client_id"))
        row["client_name"] = _pick(row.get("client_name"), src.get("client_name"))
        row["fantasy_name"] = _pick(row.get("fantasy_name"), src.get("fantasy_name"))
        row["address"] = _pick(row.get("address"), src.get("address"))
        row["city"] = _pick(row.get("city"), src.get("city"))
        row["payment_method"] = _pick(row.get("payment_method"), src.get("payment_method"))
        row["document_type_to_generate"] = _pick(
            row.get("document_type_to_generate"),
            src.get("document_type_to_generate"),
        )
        row["seller_name"] = _pick(row.get("seller_name"), src.get("seller_name"))
        row["oc_total_amount"] = _pick(row.get("oc_total_amount"), src.get("total_amount"))
        out.append(row)
    return out


def _serialize(d: dict[str, Any]) -> dict[str, Any]:
    return serialize_row(d)


def _slug_filename_part(text: str) -> str:
    s = unicodedata.normalize("NFKD", text or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()
    return s[:48] or "ruta"


def list_recent_plans(*, limit: int = 50) -> list[dict[str, Any]]:
    log_debug("GET /dispatch-plans", extra={"limit": limit})
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            rows = repo.list_recent_plans(cur, limit=limit)
        except Exception as exc:
            log_error("GET /dispatch-plans", exc, extra={"phase": "primary"})
            rows = repo.list_recent_plans_minimal(cur, limit=limit)
        cur.close()
        out = serialize_rows(rows)
        log_debug("GET /dispatch-plans", rows=len(out))
        return out
    except Exception as exc:
        log_error("GET /dispatch-plans", exc, extra={"phase": "connection"})
        return []
    finally:
        conn.close()


def list_session_plans(plan_session_id: str) -> list[dict[str, Any]]:
    log_debug("GET /dispatch-plans/by-session", extra={"session": plan_session_id[:12]})
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            rows = repo.list_plans_by_session(cur, plan_session_id)
        except Exception as exc:
            log_error(
                "GET /dispatch-plans/by-session",
                exc,
                extra={"session": plan_session_id[:12]},
            )
            rows = []
        cur.close()
        out = serialize_rows(rows)
        log_debug("GET /dispatch-plans/by-session", rows=len(out))
        return out
    except Exception as exc:
        log_error("GET /dispatch-plans/by-session", exc)
        return []
    finally:
        conn.close()


def get_plan_header(
    plan_id: int,
    *,
    stage_run: DashboardStageRun | None = None,
) -> dict[str, Any] | None:
    log_debug("GET /dispatch-plans/{id}/header", planning_id=plan_id)

    def _fetch(cur: Any) -> dict[str, Any] | None:
        t0 = log_repo_start(plan_id, "repo.get_plan_header")
        try:
            plan = repo.get_plan_header(cur, plan_id)
            log_repo_end(plan_id, "repo.get_plan_header", t0, rows=1 if plan else 0)
            return plan
        except Exception as exc:
            log_repo_end(plan_id, "repo.get_plan_header", t0, error=repr(exc))
            raise

    try:
        if stage_run:
            with dashboard_connection(plan_id, stage_run) as (cur, _conn):
                plan = _fetch(cur)
        else:
            conn = get_connection()
            try:
                cur = conn.cursor()
                plan = _fetch(cur)
                cur.close()
            finally:
                conn.close()
    except Exception as exc:
        log_error("GET /dispatch-plans/{id}/header", exc, planning_id=plan_id)
        return None
    if not plan:
        return None
    return _serialize(plan)


def get_dispatch_plan(plan_id: int) -> dict[str, Any] | None:
    log_debug("GET /dispatch-plans/{id}", planning_id=plan_id)
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            plan = repo.get_plan_by_id(cur, plan_id)
            if not plan:
                cur.close()
                return None
            orders = repo.list_plan_orders(cur, plan_id)
        except Exception as exc:
            log_error("GET /dispatch-plans/{id}", exc, planning_id=plan_id)
            return None
        cur.close()
        out = {"plan": _serialize(plan), "orders": serialize_rows(orders)}
        log_debug("GET /dispatch-plans/{id}", planning_id=plan_id, rows=len(orders))
        return out
    finally:
        conn.close()


def confirm_dispatch_plan(
    *,
    plan_session_id: str,
    truck_id: int,
    route_name: str,
    planning_name: str | None = None,
    driver_count: int,
    assistant_count: int,
    driver_cost_clp: int,
    assistant_cost_clp: int,
    diesel_price_per_liter: float,
    km_total: float,
    duration_min: float,
    liters_estimated: float,
    fuel_cost_clp: int,
    ferry_cost_clp: int,
    toll_cost_clp: int,
    extras_cost_clp: int,
    crew_cost_clp: int,
    total_route_cost_clp: int,
    route_geometry: dict[str, Any] | None,
    orders: list[dict[str, Any]],
    planning_date: date | None = None,
) -> dict[str, Any]:
    if not orders:
        raise ValueError("Se requiere al menos una OC para confirmar el plan.")
    pname = (planning_name or route_name or "").strip() or f"Camión {truck_id}"
    now = datetime.now(timezone.utc)
    pdate = planning_date or date.today()

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT name FROM distribuidora.trucks WHERE id = %s",
            (truck_id,),
        )
        truck_row = cur.fetchone()
        frozen_truck_name = (
            str(truck_row[0]).strip() if truck_row and truck_row[0] else route_name
        )
        existing = repo.get_latest_plan_for_truck_session(
            cur, plan_session_id=plan_session_id, truck_id=truck_id
        )
        if existing and existing.get("status") in (
            "planned",
            "invoicing",
            "ready_for_picking",
            "picking_generated",
            "dispatched",
            "delivered",
        ):
            raise ValueError(
                f"Ya existe un plan confirmado para este camión (id={existing['id']}, "
                f"estado={existing['status']})."
            )

        fields = {
            "plan_session_id": plan_session_id.strip(),
            "planning_date": pdate,
            "truck_id": truck_id,
            "route_name": (route_name or "").strip() or pname,
            "planning_name": pname,
            "truck_name": frozen_truck_name,
            "status": "planned",
            "driver_count": max(0, int(driver_count)),
            "assistant_count": max(0, int(assistant_count)),
            "driver_cost_clp": int(driver_cost_clp),
            "assistant_cost_clp": int(assistant_cost_clp),
            "diesel_price_per_liter": round(float(diesel_price_per_liter), 2),
            "km_total": round(float(km_total), 3),
            "duration_min": round(float(duration_min), 2),
            "liters_estimated": round(float(liters_estimated), 3),
            "fuel_cost_clp": int(fuel_cost_clp),
            "ferry_cost_clp": int(ferry_cost_clp),
            "toll_cost_clp": int(toll_cost_clp),
            "extras_cost_clp": int(extras_cost_clp),
            "crew_cost_clp": int(crew_cost_clp),
            "total_route_cost_clp": int(total_route_cost_clp),
            "route_geometry": json.dumps(route_geometry) if route_geometry else None,
            "confirmed_at": now,
        }
        enriched = _enrich_orders_snapshot(cur, orders)
        plan_id, planning_code = repo.insert_dispatch_plan(cur, fields)
        logger.info(
            "[PLANNING_CODE_DEBUG] confirm_dispatch_plan plan_id=%s planning_code=%s",
            plan_id,
            planning_code,
        )
        repo.insert_plan_orders(cur, plan_id, enriched)
        conn.commit()
        cur.close()
    finally:
        conn.close()

    return get_dispatch_plan(plan_id) or {"plan": {"id": plan_id}}


def update_dispatch_plan_status(plan_id: int, status: str) -> dict[str, Any]:
    st = status.strip().lower()
    if st not in VALID_STATUSES:
        raise ValueError(f"Estado inválido: {status}")
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not repo.get_plan_by_id(cur, plan_id):
            raise ValueError("Plan no encontrado")
        repo.update_plan_status(cur, plan_id, st)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    data = get_dispatch_plan(plan_id)
    if not data:
        raise ValueError("Plan no encontrado")
    return data


def compute_plan_margin(
    plan_id: int,
    *,
    stage_run: DashboardStageRun | None = None,
) -> dict[str, Any]:
    def _compute(cur: Any, conn: Any) -> dict[str, Any]:
        t0 = log_repo_start(plan_id, "repo.get_plan_by_id")
        plan = repo.get_plan_by_id(cur, plan_id)
        log_repo_end(plan_id, "repo.get_plan_by_id", t0, rows=1 if plan else 0)
        if not plan:
            raise ValueError("Plan no encontrado")
        t1 = log_repo_start(plan_id, "compute_plan_commercial_margin")
        cm = compute_plan_commercial_margin(cur, plan_id)
        log_repo_end(plan_id, "compute_plan_commercial_margin", t1)
        route_cost = int(plan.get("total_route_cost_clp") or 0)
        net_op = (
            int(cm.commercial_margin_clp) - route_cost
            if cm.commercial_margin_clp is not None
            else None
        )
        t2 = log_repo_start(plan_id, "repo.update_plan_margin")
        repo.update_plan_margin(
            cur,
            plan_id,
            invoiced_sales_clp=cm.invoiced_revenue_clp,
            commercial_margin_clp=cm.commercial_margin_clp,
            final_margin_clp=net_op if net_op is not None else None,
            net_operational_clp=net_op,
            margin_computation_source=cm.source,
            margin_lines_with_cost=cm.lines_with_cost,
            margin_lines_total=cm.lines_total,
        )
        log_repo_end(plan_id, "repo.update_plan_margin", t2)
        conn.commit()
        return cm, route_cost, net_op

    if stage_run:
        with dashboard_connection(plan_id, stage_run) as (cur, conn):
            cm, route_cost, net_op = _compute(cur, conn)
    else:
        conn = get_connection()
        try:
            cur = conn.cursor()
            cm, route_cost, net_op = _compute(cur, conn)
            cur.close()
        finally:
            conn.close()
    out = cm.as_dict()
    out["route_cost_clp"] = route_cost
    out["net_operational_clp"] = net_op
    out["dispatch_plan_id"] = plan_id
    return out


def get_margin_audit() -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        data = audit_bsale_margin_fields(cur)
        cur.close()
        return data
    finally:
        conn.close()


def _load_plan_orders_safe(
    plan_id: int,
    *,
    stage_run: DashboardStageRun | None = None,
) -> list[dict[str, Any]]:
    endpoint = "GET /dispatch-plans/{id}/dashboard"
    try:
        with plan_detail_step(
            endpoint,
            planning_id=plan_id,
            query="repo.list_plan_orders(dispatch_plan_orders)",
        ):
            if stage_run:
                with dashboard_connection(plan_id, stage_run) as (cur, _conn):
                    t0 = log_repo_start(plan_id, "repo.list_plan_orders")
                    orders = repo.list_plan_orders(cur, plan_id)
                    log_repo_end(
                        plan_id,
                        "repo.list_plan_orders",
                        t0,
                        rows=len(orders),
                    )
            else:
                conn = get_connection()
                try:
                    cur = conn.cursor()
                    orders = repo.list_plan_orders(cur, plan_id)
                    cur.close()
                finally:
                    conn.close()
        rows = serialize_rows(orders)
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            query="repo.list_plan_orders",
            rows=len(rows),
        )
        return rows
    except Exception as exc:
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            query="repo.list_plan_orders",
            rows=0,
            error=repr(exc),
        )
        log_error(endpoint, exc, planning_id=plan_id, extra={"phase": "orders"})
        return []


def count_picking_client_rows(
    plan_id: int,
    *,
    stage_run: DashboardStageRun | None = None,
) -> int:
    """Conteo liviano; no usar en GET /dashboard (lento: join a vista facturación)."""
    sql = PICKING_CLIENT_COUNT_SQL

    def _run(cur: Any) -> int:
        cur.execute(sql, (plan_id,))
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    try:
        if stage_run:
            with dashboard_connection(plan_id, stage_run) as (cur, _conn):
                return _run(cur)
        conn = get_connection()
        try:
            cur = conn.cursor()
            n = _run(cur)
            cur.close()
            return n
        finally:
            conn.close()
    except Exception:
        if stage_run:
            raise
        return 0


def count_picking_product_rows(
    plan_id: int,
    *,
    stage_run: DashboardStageRun | None = None,
) -> int:
    """Filas consolidadas por producto; no usar en GET /dashboard."""
    sql = PICKING_PRODUCT_COUNT_SQL

    def _run(cur: Any) -> int:
        cur.execute(sql, (plan_id,))
        row = cur.fetchone()
        return int(row[0] or 0) if row else 0

    try:
        if stage_run:
            with dashboard_connection(plan_id, stage_run) as (cur, _conn):
                return _run(cur)
        conn = get_connection()
        try:
            cur = conn.cursor()
            n = _run(cur)
            cur.close()
            return n
        finally:
            conn.close()
    except Exception:
        if stage_run:
            raise
        return 0


def _build_plan_dashboard(
    plan_id: int,
    plan: dict[str, Any],
    *,
    user_role: str | None = None,
    include_items: bool = False,
    include_margin: bool = False,
    stage_run: DashboardStageRun | None = None,
) -> dict[str, Any]:
    endpoint = "GET /dispatch-plans/{id}/dashboard"
    if stage_run:
        stage_run.log_stage(3, "load_invoiced_documents")
    orders = _load_plan_orders_safe(plan_id, stage_run=stage_run)

    try:
        with plan_detail_step(
            endpoint,
            planning_id=plan_id,
            query="get_invoiced_documents (v_dispatch_plan_invoiced_documents)",
        ):
            inv = get_invoiced_documents(plan_id, stage_run=stage_run)
    except ValueError:
        raise
    except Exception as exc:
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            query="get_invoiced_documents",
            error=repr(exc),
        )
        inv = empty_invoicing_payload(
            plan_id,
            orders,
            full_view_error=_format_invoicing_exc(exc),
        )

    inv_items = inv.get("items") if isinstance(inv.get("items"), list) else []
    inv_summary = inv.get("summary") if isinstance(inv.get("summary"), dict) else {}
    inv_summary = {
        "confirmed": int(inv_summary.get("confirmed") or 0),
        "auto_confirmed": int(inv_summary.get("auto_confirmed") or 0),
        "probable": int(inv_summary.get("probable") or 0),
        "missing": int(inv_summary.get("missing") or 0),
        "total": int(inv_summary.get("total") or len(inv_items)),
    }

    total_oc_amount = sum(float(o.get("oc_total_amount") or 0) for o in orders)
    confirmed_amount = 0.0
    probable_amount = 0.0
    pending_amount = 0.0
    by_oc: dict[int, dict[str, Any]] = {}
    for o in orders:
        try:
            oc_id = int(o.get("oc_document_id") or 0)
        except (TypeError, ValueError):
            continue
        if oc_id:
            by_oc[oc_id] = o
    for item in inv_items:
        try:
            oc_id = int(item.get("oc_document_id") or 0)
        except (TypeError, ValueError):
            continue
        oc = by_oc.get(oc_id) or {}
        amt = float(oc.get("oc_total_amount") or 0)
        st = item.get("status")
        if st == "confirmed":
            confirmed_amount += amt
        elif st == "probable":
            probable_amount += amt
        else:
            pending_amount += amt

    inv_auto_confirmed = int(inv_summary.get("auto_confirmed") or 0)

    can_calc_margin = (
        include_margin
        and inv_summary["total"] > 0
        and inv_summary["missing"] == 0
        and inv_summary["confirmed"] > 0
    )
    margin_block: dict[str, Any] | None = None
    role = (user_role or "").strip().lower()
    show_margin = role in MARGIN_VIEW_ROLES
    if can_calc_margin:
        if stage_run:
            stage_run.log_stage("margin", "load_margin")
        try:
            with plan_detail_step(
                endpoint,
                planning_id=plan_id,
                query="compute_plan_margin",
            ):
                margin_data = compute_plan_margin(plan_id, stage_run=stage_run)
        except Exception as exc:
            log_plan_detail_debug(
                endpoint,
                planning_id=plan_id,
                query="compute_plan_margin",
                error=repr(exc),
            )
            margin_data = {
                "commercial_margin_clp": None,
                "partial": True,
                "message": "No se pudo calcular margen (costos o migración pendiente).",
            }
        if show_margin:
            margin_block = {
                "visible": True,
                "commercial_margin_clp": margin_data.get("commercial_margin_clp"),
                "invoiced_revenue_clp": margin_data.get("invoiced_revenue_clp"),
                "invoiced_cost_clp": margin_data.get("invoiced_cost_clp"),
                "route_cost_clp": margin_data.get("route_cost_clp"),
                "net_operational_clp": margin_data.get("net_operational_clp"),
                "source": margin_data.get("source"),
                "partial": margin_data.get("partial"),
                "unavailable": margin_data.get("commercial_margin_clp") is None,
                "message": margin_data.get("message"),
            }
        else:
            margin_block = {"visible": False, "restricted": True}
    elif plan.get("commercial_margin_clp") is not None and show_margin:
        margin_block = {
            "visible": True,
            "commercial_margin_clp": plan.get("commercial_margin_clp"),
            "invoiced_revenue_clp": plan.get("invoiced_sales_clp"),
            "route_cost_clp": plan.get("total_route_cost_clp"),
            "net_operational_clp": plan.get("net_operational_clp") or plan.get("final_margin_clp"),
            "source": plan.get("margin_computation_source"),
            "partial": False,
            "message": "Margen almacenado.",
        }

    warnings = inv.get("warnings") if isinstance(inv.get("warnings"), list) else []
    probable_notes = (
        inv.get("probable_notes") if isinstance(inv.get("probable_notes"), list) else []
    )

    if stage_run:
        stage_run.log_stage(4, "build_response")

    payload = {
        "plan": plan,
        "invoicing": {
            "total_orders": len(orders),
            "total_oc_amount_clp": int(round(total_oc_amount)),
            "confirmed": {
                "count": inv_summary["confirmed"],
                "amount_clp": int(round(confirmed_amount)),
                "auto_confirmed_count": inv_auto_confirmed,
            },
            "probable": {
                "count": inv_summary["probable"],
                "amount_clp": int(round(probable_amount)),
            },
            "pending": {
                "count": inv_summary["missing"],
                "amount_clp": int(round(pending_amount)),
            },
        },
        "invoiced_items": inv_items if include_items else [],
        "warnings": warnings,
        "probable_notes": probable_notes,
        "margin": margin_block,
        "picking": picking_block_from_invoicing(plan_id, inv),
        "degraded": bool(inv.get("invoicing_degraded")),
        "invoicing_source": inv.get("invoicing_source"),
    }
    return serialize_value(payload)


def get_plan_dashboard(
    plan_id: int,
    *,
    user_role: str | None = None,
    include_items: bool = False,
    include_margin: bool = False,
    stage_run: DashboardStageRun | None = None,
) -> dict[str, Any]:
    """Dashboard del plan."""
    endpoint = "GET /dispatch-plans/{id}/dashboard"
    log_plan_detail_debug(endpoint, planning_id=plan_id, query="start")
    if stage_run:
        stage_run.log_stage(2, "load_header")
    try:
        with plan_detail_step(
            endpoint,
            planning_id=plan_id,
            query="get_plan_header",
        ):
            plan = get_plan_header(plan_id, stage_run=stage_run)
    except Exception as exc:
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            query="get_plan_header",
            error=repr(exc),
        )
        raise ValueError("Plan no encontrado") from exc

    if not plan:
        raise ValueError("Plan no encontrado")

    try:
        return _build_plan_dashboard(
            plan_id,
            plan,
            user_role=user_role,
            include_items=include_items,
            include_margin=include_margin,
            stage_run=stage_run,
        )
    except ValueError:
        raise
    except Exception as exc:
        plan_debug_on_error(
            endpoint,
            plan_id,
            exc,
            log_plan_debug_context(plan_id, endpoint),
        )
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            query="_build_plan_dashboard",
            error=repr(exc),
        )
        log_error(endpoint, exc, planning_id=plan_id)
        if PLAN_DEBUG_RERAISE:
            raise
        return serialize_value(
            empty_plan_dashboard(
                plan_id,
                plan,
                degraded_message=(
                    "No se pudo cargar facturación o métricas completas; "
                    "se muestran valores en cero."
                ),
            )
        )


def _invoicing_payload_from_rows(
    plan_id: int,
    rows: list[dict[str, Any]],
    *,
    invoicing_source: str = "full",
    invoicing_degraded: bool = False,
    extra_warnings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    items = serialize_rows(apply_operational_invoicing_rows(rows))
    summary = invoicing_summary_counts(items)
    warnings = list(extra_warnings or [])
    warnings.extend(
        {
            "oc_document_id": x["oc_document_id"],
            "oc_number": x.get("oc_number"),
            "message": "OC aún sin documento facturado asociado",
        }
        for x in items
        if x.get("status") == "missing"
    )
    probable_notes = [
        {
            "oc_document_id": x["oc_document_id"],
            "oc_number": x.get("oc_number"),
            "message": "Coincidencia probable — no usar para picking hasta confirmar en Bsale",
            "probable_document_number": x.get("probable_document_number"),
            "probable_score": x.get("probable_score"),
        }
        for x in items
        if x.get("status") == "probable"
    ]
    ready = summary["missing"] == 0 and summary["confirmed"] > 0
    return {
        "dispatch_plan_id": plan_id,
        "items": items,
        "summary": summary,
        "warnings": warnings,
        "probable_notes": probable_notes,
        "ready_for_picking": ready and summary["confirmed"] == summary["total"],
        "invoicing_source": invoicing_source,
        "invoicing_degraded": invoicing_degraded,
    }


def _load_invoiced_rows_with_fallback(
    conn: Any,
    cur: Any,
    plan_id: int,
) -> tuple[list[dict[str, Any]], str, bool, str | None]:
    """
    Vista completa; si falla, rollback y consulta lite por document_related.

    Returns:
        rows, source ('full' | 'lite'), degraded, full_view_error (si full falló y lite OK).
    """
    full_view_error: str | None = None
    exc_full: Exception | None = None

    t0 = log_repo_start(plan_id, "repo.list_invoiced_documents")
    try:
        rows = repo.list_invoiced_documents(cur, plan_id)
        log_repo_end(plan_id, "repo.list_invoiced_documents", t0, rows=len(rows))
        return rows, "full", False, None
    except Exception as e:
        exc_full = e
        full_view_error = _format_invoicing_exc(e)
        log_repo_end(
            plan_id,
            "repo.list_invoiced_documents",
            t0,
            error=full_view_error,
        )
        logger.warning(
            "[INVOICING_FALLBACK] plan_id=%s full_view_failed error=%s",
            plan_id,
            full_view_error,
            exc_info=e,
        )
        _invoicing_connection_rollback(conn, plan_id=plan_id, label="after_full_view_failed")

    t1 = log_repo_start(plan_id, "repo.list_invoiced_documents_lite")
    try:
        rows = repo.list_invoiced_documents_lite(cur, plan_id)
        log_repo_end(plan_id, "repo.list_invoiced_documents_lite", t1, rows=len(rows))
        return rows, "lite", True, full_view_error
    except Exception as exc_lite:
        log_repo_end(
            plan_id,
            "repo.list_invoiced_documents_lite",
            t1,
            error=_format_invoicing_exc(exc_lite),
        )
        _invoicing_connection_rollback(conn, plan_id=plan_id, label="after_lite_failed")
        raise InvoicingDocumentsUnavailable(
            full_view_error or _format_invoicing_exc(exc_lite),
            exc_lite,
        ) from exc_full


def get_invoiced_documents(
    plan_id: int,
    *,
    stage_run: DashboardStageRun | None = None,
) -> dict[str, Any]:
    """Facturación vinculada (alias operacional: /facturacion)."""
    endpoint = "GET /dispatch-plans/{id}/invoiced-documents"
    log_plan_detail_debug(
        endpoint,
        planning_id=plan_id,
        query="v_dispatch_plan_invoiced_documents",
    )

    def _fetch(cur: Any, conn: Any) -> dict[str, Any]:
        rows, source, degraded, full_err = _load_invoiced_rows_with_fallback(
            conn, cur, plan_id
        )
        extra: list[dict[str, Any]] = []
        if source == "lite":
            if full_err:
                extra.append(
                    {
                        "oc_document_id": 0,
                        "oc_number": None,
                        "message": (
                            "Error en v_dispatch_plan_invoiced_documents (vista full): "
                            f"{full_err}"
                        ),
                    }
                )
            extra.append(
                {
                    "oc_document_id": 0,
                    "oc_number": None,
                    "message": (
                        "Facturación en modo rápido (document_related). "
                        "Coincidencias probables no evaluadas."
                    ),
                }
            )
        return _invoicing_payload_from_rows(
            plan_id,
            rows,
            invoicing_source=source,
            invoicing_degraded=degraded,
            extra_warnings=extra,
        )

    def _empty_from_invoicing_failure(
        orders: list[dict[str, Any]],
        *,
        full_view_error: str | None = None,
        lite_error: str | None = None,
        summary_error: str | None = None,
    ) -> dict[str, Any]:
        return empty_invoicing_payload(
            plan_id,
            orders,
            invoicing_error=summary_error or full_view_error,
            full_view_error=full_view_error,
            lite_error=lite_error,
        )

    if stage_run:
        try:
            with dashboard_connection(plan_id, stage_run) as (cur, conn):
                t0 = log_repo_start(plan_id, "repo.get_plan_by_id")
                plan = repo.get_plan_by_id(cur, plan_id)
                log_repo_end(plan_id, "repo.get_plan_by_id", t0, rows=1 if plan else 0)
                if not plan:
                    raise ValueError("Plan no encontrado")
                with plan_detail_step(
                    endpoint,
                    planning_id=plan_id,
                    query="invoiced_documents",
                ):
                    return _fetch(cur, conn)
        except ValueError:
            raise
        except InvoicingDocumentsUnavailable as inv_exc:
            log_plan_detail_debug(
                endpoint,
                planning_id=plan_id,
                query="invoiced_documents",
                rows=0,
                error=inv_exc.full_view_error,
            )
            logger.warning(
                "[INVOICING_UNAVAILABLE] plan_id=%s full=%s lite=%s",
                plan_id,
                inv_exc.full_view_error,
                inv_exc.lite_error,
            )
            try:
                with dashboard_connection(plan_id, stage_run) as (cur, _conn):
                    orders = repo.list_plan_orders(cur, plan_id)
            except Exception:
                orders = []
            return _empty_from_invoicing_failure(
                serialize_rows(orders),
                full_view_error=inv_exc.full_view_error,
                lite_error=inv_exc.lite_error,
            )
        except Exception as exc:
            log_plan_detail_debug(
                endpoint,
                planning_id=plan_id,
                query="invoiced_documents",
                rows=0,
                error=repr(exc),
            )
            log_error(endpoint, exc, planning_id=plan_id)
            try:
                with dashboard_connection(plan_id, stage_run) as (cur, _conn):
                    orders = repo.list_plan_orders(cur, plan_id)
            except Exception:
                orders = []
            return _empty_from_invoicing_failure(
                serialize_rows(orders),
                full_view_error=_format_invoicing_exc(exc),
            )

    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise ValueError("Plan no encontrado")
        try:
            with plan_detail_step(
                endpoint,
                planning_id=plan_id,
                query="invoiced_documents",
            ):
                payload = _fetch(cur, conn)
        except InvoicingDocumentsUnavailable as inv_exc:
            log_plan_detail_debug(
                endpoint,
                planning_id=plan_id,
                query="invoiced_documents",
                rows=0,
                error=inv_exc.full_view_error,
            )
            logger.warning(
                "[INVOICING_UNAVAILABLE] plan_id=%s full=%s lite=%s",
                plan_id,
                inv_exc.full_view_error,
                inv_exc.lite_error,
            )
            _invoicing_connection_rollback(conn, plan_id=plan_id, label="invoicing_unavailable")
            try:
                orders = repo.list_plan_orders(cur, plan_id)
            except Exception:
                orders = []
            cur.close()
            return _empty_from_invoicing_failure(
                serialize_rows(orders),
                full_view_error=inv_exc.full_view_error,
                lite_error=inv_exc.lite_error,
            )
        except Exception as exc:
            log_plan_detail_debug(
                endpoint,
                planning_id=plan_id,
                query="invoiced_documents",
                rows=0,
                error=repr(exc),
            )
            log_error(endpoint, exc, planning_id=plan_id)
            _invoicing_connection_rollback(conn, plan_id=plan_id, label="invoiced_documents_failed")
            try:
                orders = repo.list_plan_orders(cur, plan_id)
            except Exception:
                orders = []
            cur.close()
            return _empty_from_invoicing_failure(
                serialize_rows(orders),
                full_view_error=_format_invoicing_exc(exc),
            )
        cur.close()
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            query="list_invoiced_documents",
            rows=len(payload.get("items") or []),
        )
        return payload
    except ValueError:
        raise
    except Exception as exc:
        log_plan_detail_debug(
            endpoint,
            planning_id=plan_id,
            error=repr(exc),
        )
        log_error(endpoint, exc, planning_id=plan_id)
        return empty_invoiced_documents_response(plan_id)
    finally:
        conn.close()


def build_billing_excel_bytes(plan_id: int) -> tuple[bytes, str]:
    data = get_dispatch_plan(plan_id)
    if not data:
        raise ValueError("Plan no encontrado")
    plan = data["plan"]
    orders = data["orders"]
    if plan.get("status") == "draft":
        raise ValueError("El plan debe estar confirmado (planned) para exportar facturación.")

    crew_label = (
        f"{plan.get('driver_count', 1)} chofer / "
        f"{plan.get('assistant_count', 0)} peoneta(s)"
    )
    rows = []
    for o in orders:
        rows.append(
            {
                "numero_oc": o.get("oc_number") or o.get("oc_document_id"),
                "forma_pago": o.get("payment_method") or "",
                "tipo_documento_generar": o.get("document_type_to_generate") or "",
                "cliente": o.get("client_name") or "",
                "nombre_fantasia": o.get("fantasy_name") or o.get("client_name") or "",
                "direccion": o.get("address") or "",
                "ciudad": o.get("city") or "",
                "vendedor": o.get("seller_name") or "",
                "total": o.get("oc_total_amount"),
                "orden_ruta": o.get("route_order"),
                "camion": plan.get("truck_name") or plan.get("route_name"),
                "tripulacion": crew_label,
            }
        )
    df = pd.DataFrame(rows)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Facturacion", index=False)
        meta = pd.DataFrame(
            [
                {"campo": "plan_id", "valor": plan_id},
                {"campo": "planning_code", "valor": plan.get("planning_code")},
                {"campo": "planning_name", "valor": plan.get("planning_name")},
                {"campo": "ruta", "valor": plan.get("route_name")},
                {"campo": "km_total", "valor": plan.get("km_total")},
                {"campo": "costo_total_ruta", "valor": plan.get("total_route_cost_clp")},
            ]
        )
        meta.to_excel(writer, sheet_name="Resumen", index=False)
    buf.seek(0)
    slug = _slug_filename_part(str(plan.get("truck_name") or plan.get("route_name")))
    fname = f"facturacion_{slug}_{date.today().strftime('%Y%m%d')}.xlsx"
    return buf.getvalue(), fname


def _picking_doc_id_expr() -> str:
    return """
        COALESCE(
            inv.related_document_id,
            CASE WHEN inv.status = 'probable' THEN inv.probable_document_id END
        )
    """


def _picking_client_sql_view(*, include_probable: bool) -> str:
    status_filter = inv_status_sql_filter(include_probable=include_probable)
    doc_id = _picking_doc_id_expr()
    return f"""
            SELECT
                dpo.route_order,
                dpo.oc_document_id,
                dpo.lat,
                dpo.lng,
                dpo.client_id,
                dpo.client_name,
                dpo.fantasy_name,
                dpo.address,
                dpo.city,
                NULLIF(BTRIM(cl.phone), '') AS phone,
                {doc_id} AS related_document_id,
                COALESCE(
                    inv.related_document_number,
                    inv.probable_document_number
                ) AS related_document_number,
                COALESCE(
                    inv.related_document_type_label,
                    inv.probable_document_type_label
                ) AS related_document_type_label,
                inv.relation_source,
                inv.status AS invoicing_status,
                inv.probable_score,
                d_pay.forma_pago,
                d_pay.tipo_documento_a_generar,
                COALESCE(
                    NULLIF(BTRIM(d_pay.observaciones), ''),
                    NULLIF(BTRIM(d.tracking_number), '')
                ) AS observaciones,
                COALESCE(NULLIF(BTRIM(d.seller_name), ''), dpo.seller_name) AS seller_name,
                d.total_amount AS document_total,
                d.document_type_id
            FROM distribuidora.dispatch_plan_orders dpo
            INNER JOIN distribuidora.v_dispatch_plan_invoiced_documents inv
                ON inv.dispatch_plan_id = dpo.dispatch_plan_id
               AND inv.oc_document_id = dpo.oc_document_id
               {status_filter}
               AND {doc_id} IS NOT NULL
            INNER JOIN distribuidora.v_documents_latest d
                ON d.document_id = {doc_id}
            LEFT JOIN distribuidora.v_orders_purchase d_pay
                ON d_pay.document_id = dpo.oc_document_id
            LEFT JOIN bsale.clients cl
                ON cl.company_id = 3 AND cl.bsale_id = dpo.client_id
            WHERE dpo.dispatch_plan_id = %s
            ORDER BY dpo.route_order ASC, dpo.oc_document_id ASC
            """

_PICKING_CLIENT_SQL_LITE = """
            SELECT
                dpo.route_order,
                dpo.oc_document_id,
                dpo.lat,
                dpo.lng,
                dpo.client_id,
                dpo.client_name,
                dpo.fantasy_name,
                dpo.address,
                dpo.city,
                NULLIF(BTRIM(cl.phone), '') AS phone,
                st.invoicing_document_id AS related_document_id,
                st.invoicing_number AS related_document_number,
                CASE st.invoicing_document_type_id
                    WHEN 1 THEN 'Boleta'
                    WHEN 6 THEN 'Factura'
                    ELSE NULL
                END AS related_document_type_label,
                'relateddetailid' AS relation_source,
                'confirmed' AS invoicing_status,
                NULL::numeric AS probable_score,
                d_pay.forma_pago,
                d_pay.tipo_documento_a_generar,
                COALESCE(NULLIF(BTRIM(d_pay.observaciones), ''), NULLIF(BTRIM(d.tracking_number), '')) AS observaciones,
                COALESCE(NULLIF(BTRIM(d.seller_name), ''), dpo.seller_name) AS seller_name,
                d.total_amount AS document_total,
                d.document_type_id
            FROM distribuidora.dispatch_plan_orders dpo
            INNER JOIN distribuidora.v_orders_purchase_status st
                ON st.document_id = dpo.oc_document_id
               AND COALESCE(st.is_invoiced, FALSE) = TRUE
            INNER JOIN distribuidora.v_documents_latest d
                ON d.document_id = st.invoicing_document_id
            LEFT JOIN distribuidora.v_orders_purchase d_pay
                ON d_pay.document_id = dpo.oc_document_id
            LEFT JOIN bsale.clients cl
                ON cl.company_id = 3 AND cl.bsale_id = dpo.client_id
            WHERE dpo.dispatch_plan_id = %s
            ORDER BY dpo.route_order ASC, dpo.oc_document_id ASC
            """

def _picking_product_sql_view(*, include_probable: bool) -> str:
    status_filter = inv_status_sql_filter(include_probable=include_probable)
    doc_id = _picking_doc_id_expr()
    return f"""
                SELECT
                    COALESCE(NULLIF(BTRIM(t.name), ''), 'Centro de despacho') AS sucursal_bodega,
                    {PM_TIPO_PRODUCTO_EXPR} AS tipo_producto,
                    dd.variant_description AS producto,
                    dd.variant_description AS variante,
                    NULLIF(BTRIM(dd.variant_code), '') AS codigo_barras,
                    SUM(dd.quantity) AS unidades,
                    {CAJAS_AGG_EXPR} AS cajas,
                    MAX(v.units_per_box) AS units_per_box,
                    (
                        MAX(v.units_per_box) IS NULL OR MAX(v.units_per_box) <= 0
                    ) AS sin_unidad_caja,
                    SUM(dd.total_amount) AS total_monto
                FROM distribuidora.dispatch_plan_orders dpo
                INNER JOIN distribuidora.dispatch_plan dp
                    ON dp.id = dpo.dispatch_plan_id
                LEFT JOIN distribuidora.trucks t ON t.id = dp.truck_id
                INNER JOIN distribuidora.v_dispatch_plan_invoiced_documents inv
                    ON inv.dispatch_plan_id = dpo.dispatch_plan_id
                   AND inv.oc_document_id = dpo.oc_document_id
                   {status_filter}
                   AND {doc_id} IS NOT NULL
                INNER JOIN distribuidora.document_details dd
                    ON dd.document_id = {doc_id}
                {PM_JOIN}
                {VARIANTS_JOIN}
                WHERE dpo.dispatch_plan_id = %s
                GROUP BY
                    COALESCE(NULLIF(BTRIM(t.name), ''), 'Centro de despacho'),
                    {PM_TIPO_PRODUCTO_EXPR},
                    dd.variant_description,
                    NULLIF(BTRIM(dd.variant_code), '')
                ORDER BY tipo_producto, producto, codigo_barras NULLS LAST
                """

_PICKING_PRODUCT_SQL_LITE = f"""
                SELECT
                    {PM_TIPO_PRODUCTO_EXPR} AS tipo_producto,
                    dd.variant_description AS producto,
                    dd.variant_description AS variante,
                    NULLIF(BTRIM(dd.variant_code), '') AS codigo_barras,
                    SUM(dd.quantity) AS unidades,
                    {CAJAS_AGG_EXPR} AS cajas,
                    SUM(dd.total_amount) AS total_monto
                FROM distribuidora.dispatch_plan_orders dpo
                INNER JOIN distribuidora.v_orders_purchase_status st
                    ON st.document_id = dpo.oc_document_id
                   AND COALESCE(st.is_invoiced, FALSE) = TRUE
                INNER JOIN distribuidora.document_details dd
                    ON dd.document_id = st.invoicing_document_id
                {PM_JOIN}
                {VARIANTS_JOIN}
                WHERE dpo.dispatch_plan_id = %s
                GROUP BY
                    {PM_TIPO_PRODUCTO_EXPR},
                    dd.variant_description,
                    NULLIF(BTRIM(dd.variant_code), '')
                ORDER BY tipo_producto, producto, codigo_barras NULLS LAST
                """


def _resolve_picking_row(
    plan_id: int,
    *,
    version: int | None = None,
    picking_id: int | None = None,
) -> dict[str, Any] | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = picking_repo.get_picking_row(
            cur,
            plan_id,
            version=version,
            picking_id=picking_id,
            current_only=version is None and picking_id is None,
        )
        cur.close()
        return row
    finally:
        conn.close()


def _load_persisted_picking_bundle(
    plan_id: int,
    picking_row: dict[str, Any],
) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        pid = int(picking_row["id"])
        client_rows = picking_repo.list_picking_clients(cur, pid)
        product_rows = picking_repo.list_picking_products(cur, pid)
        cur.close()
    finally:
        conn.close()

    meta = picking_repo.picking_meta_to_api(picking_row)
    clients = [picking_repo.client_row_to_api(r) for r in client_rows]
    items = [picking_repo.product_row_to_api(r) for r in product_rows]
    header = meta.get("header") or {}
    warnings = meta.get("warnings") or []
    return {
        "dispatch_plan_id": plan_id,
        "picking_id": meta["picking_id"],
        "version": meta["version"],
        "is_current": meta.get("is_current"),
        "generated_at": meta.get("generated_at"),
        "ready": True,
        "reason": None,
        "header": header,
        "clients": clients,
        "items": items,
        "warnings": warnings,
        "include_probable": meta.get("include_probable"),
        "source": "persisted",
        "totals": {
            "stops": len(clients),
            "document_total_clp": meta.get("document_total_clp"),
            "lines": len(items),
            "unidades": sum(float(i.get("unidades") or 0) for i in items),
            "cajas": sum(
                float(i.get("cajas") or 0)
                for i in items
                if not i.get("sin_unidad_caja")
            ),
            "total_monto_clp": meta.get("product_total_monto_clp"),
        },
    }


def _compute_picking_from_invoicing(
    plan_id: int,
    *,
    include_probable: bool,
    inv_check: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    use_lite = inv_check.get("invoicing_source") == "lite" and not include_probable
    stops_sql = (
        _PICKING_CLIENT_SQL_LITE
        if use_lite
        else _picking_client_sql_view(include_probable=include_probable)
    )
    product_sql = (
        _PICKING_PRODUCT_SQL_LITE
        if use_lite
        else _picking_product_sql_view(include_probable=include_probable)
    )
    conn = get_connection()
    try:
        cur = conn.cursor()
        timed_execute(cur, stops_sql, (plan_id,), plan_id=plan_id, endpoint="compute-picking")
        cols = [c[0] for c in cur.description]
        stops = [dict(zip(cols, r)) for r in cur.fetchall()]
        timed_execute(cur, product_sql, (plan_id,), plan_id=plan_id, endpoint="compute-picking")
        pcols = [c[0] for c in cur.description]
        products = [dict(zip(pcols, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    clients = [normalize_client_stop(_serialize(s)) for s in stops]
    items = [normalize_product_row(_serialize(p)) for p in products]
    return clients, items


def generate_plan_picking(
    plan_id: int,
    *,
    validate: bool = True,
    include_probable: bool = False,
) -> dict[str, Any]:
    """Calcula picking desde facturación y persiste nueva versión (cliente + producto)."""
    endpoint = "POST /dispatch-plans/{id}/picking/generate"
    try:
        inv_check = get_invoiced_documents(plan_id)
    except Exception as exc:
        inv_check = {
            "summary": {"confirmed": 0, "probable": 0, "missing": 0, "total": 0},
            "invoicing_unavailable": True,
            "invoicing_error": str(exc),
        }

    readiness = evaluate_picking_readiness(
        inv_check, include_probable=include_probable
    )
    if validate and not readiness["ready"]:
        return serialize_value(
            {
                "dispatch_plan_id": plan_id,
                "ready": False,
                "reason": readiness["reason"] or PICKING_WAIT_MESSAGE,
                "include_probable": include_probable,
            }
        )

    clients, items = _compute_picking_from_invoicing(
        plan_id, include_probable=include_probable, inv_check=inv_check
    )
    header = fetch_picking_header(plan_id)
    warnings = picking_warnings_from_stops(clients)
    if include_probable and int((inv_check.get("summary") or {}).get("probable") or 0) > 0:
        warnings.append(
            "El consolidado incluye documentos con coincidencia probable (60–74)."
        )
    if readiness.get("reason"):
        warnings.insert(0, readiness["reason"])

    doc_total = sum(float(c.get("document_total") or 0) for c in clients)
    prod_total = sum(float(i.get("total_monto") or 0) for i in items)

    conn = get_connection()
    try:
        cur = conn.cursor()
        version = picking_repo.get_next_picking_version(cur, plan_id)
        picking_repo.supersede_current_pickings(cur, plan_id)
        new_picking_id = picking_repo.insert_picking(
            cur,
            plan_id=plan_id,
            version=version,
            include_probable=include_probable,
            header=header,
            warnings=warnings,
            stops_count=len(clients),
            product_lines_count=len(items),
            document_total_clp=doc_total,
            product_total_monto_clp=prod_total,
        )
        picking_repo.insert_picking_clients(
            cur, picking_id=new_picking_id, plan_id=plan_id, clients=clients
        )
        picking_repo.insert_picking_products(
            cur, picking_id=new_picking_id, plan_id=plan_id, items=items
        )
        conn.commit()
        cur.close()
    except Exception as exc:
        conn.rollback()
        log_error(endpoint, exc, planning_id=plan_id)
        if PLAN_DEBUG_RERAISE:
            raise
        return serialize_value(
            {
                "dispatch_plan_id": plan_id,
                "ready": False,
                "reason": f"No se pudo persistir picking: {exc}",
            }
        )
    finally:
        conn.close()

    mark_picking_generated(plan_id)
    row = _resolve_picking_row(plan_id, picking_id=new_picking_id)
    if not row:
        raise ValueError("Picking persistido no encontrado tras generar")
    bundle = _load_persisted_picking_bundle(plan_id, row)
    bundle["validation"] = inv_check if validate else None
    return serialize_value(bundle)


def list_plan_pickings(plan_id: int, *, limit: int = 30) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        versions = picking_repo.list_picking_versions(cur, plan_id, limit=limit)
        cur.close()
    finally:
        conn.close()
    return serialize_value(
        {
            "dispatch_plan_id": plan_id,
            "items": [
                {
                    "picking_id": v["id"],
                    "version": v["version"],
                    "is_current": v.get("is_current"),
                    "include_probable": v.get("include_probable"),
                    "generated_at": v.get("generated_at"),
                    "superseded_at": v.get("superseded_at"),
                    "stops_count": v.get("stops_count"),
                    "product_lines_count": v.get("product_lines_count"),
                }
                for v in versions
            ],
        }
    )


def get_picking_by_client(
    plan_id: int,
    *,
    validate: bool = True,
    include_probable: bool = False,
    version: int | None = None,
    picking_id: int | None = None,
) -> dict[str, Any]:
    endpoint = "GET /dispatch-plans/{id}/picking-by-client"
    row = _resolve_picking_row(plan_id, version=version, picking_id=picking_id)
    if not row:
        return serialize_value(
            {
                "dispatch_plan_id": plan_id,
                "ready": False,
                "reason": (
                    "No hay picking persistido. Use POST /picking/generate "
                    "para crear una versión."
                ),
                "clients": [],
                "source": "persisted",
            }
        )
    bundle = _load_persisted_picking_bundle(plan_id, row)
    out = {
        k: v
        for k, v in bundle.items()
        if k in (
            "dispatch_plan_id",
            "picking_id",
            "version",
            "is_current",
            "generated_at",
            "ready",
            "reason",
            "header",
            "warnings",
            "include_probable",
            "source",
            "totals",
        )
    }
    out["clients"] = bundle["clients"]
    log_plan_detail_debug(
        endpoint, planning_id=plan_id, rows=len(out["clients"])
    )
    return serialize_value(out)


def get_picking_by_product(
    plan_id: int,
    *,
    validate: bool = True,
    include_probable: bool = False,
    version: int | None = None,
    picking_id: int | None = None,
) -> dict[str, Any]:
    endpoint = "GET /dispatch-plans/{id}/picking-by-product"
    row = _resolve_picking_row(plan_id, version=version, picking_id=picking_id)
    if not row:
        return serialize_value(
            {
                "dispatch_plan_id": plan_id,
                "ready": False,
                "reason": (
                    "No hay picking persistido. Use POST /picking/generate "
                    "para crear una versión."
                ),
                "items": [],
                "source": "persisted",
            }
        )
    bundle = _load_persisted_picking_bundle(plan_id, row)
    out = {
        k: v
        for k, v in bundle.items()
        if k in (
            "dispatch_plan_id",
            "picking_id",
            "version",
            "is_current",
            "generated_at",
            "ready",
            "reason",
            "header",
            "warnings",
            "include_probable",
            "source",
            "totals",
        )
    }
    out["items"] = bundle["items"]
    log_plan_detail_debug(endpoint, planning_id=plan_id, rows=len(out["items"]))
    return serialize_value(out)


def mark_picking_generated(plan_id: int) -> dict[str, Any]:
    return update_dispatch_plan_status(plan_id, "picking_generated")


def build_picking_client_excel_bytes(
    plan_id: int,
    *,
    version: int | None = None,
    picking_id: int | None = None,
) -> tuple[bytes, str]:
    data = get_picking_by_client(
        plan_id, validate=False, version=version, picking_id=picking_id
    )
    if not data.get("ready"):
        raise ValueError(data.get("reason") or "Picking cliente no disponible")
    header = data.get("header") or fetch_picking_header(plan_id)
    clients = data.get("clients") or []
    ver = data.get("version")
    base, _ = build_picking_client_excel(plan_id, header, clients)
    if ver is not None:
        slug = _slug_filename_part(str(header.get("truck_name")))
        fname = f"picking_cliente_{slug}_v{ver}.xlsx"
        return base, fname
    return build_picking_client_excel(plan_id, header, clients)


def build_picking_product_excel_bytes(
    plan_id: int,
    *,
    version: int | None = None,
    picking_id: int | None = None,
) -> tuple[bytes, str]:
    data = get_picking_by_product(
        plan_id, validate=False, version=version, picking_id=picking_id
    )
    if not data.get("ready"):
        raise ValueError(data.get("reason") or "Picking producto no disponible")
    header = data.get("header") or fetch_picking_header(plan_id)
    items = data.get("items") or []
    ver = data.get("version")
    base, _ = build_picking_product_excel(plan_id, header, items)
    if ver is not None:
        slug = _slug_filename_part(str(header.get("truck_name")))
        fname = f"picking_producto_{slug}_v{ver}.xlsx"
        return base, fname
    return build_picking_product_excel(plan_id, header, items)


def repair_order_snapshots(plan_id: int) -> dict[str, Any]:
    """
    Rellena solo campos vacíos en órdenes ya guardadas (fix planes históricos con Excel incompleto).
    No sobrescribe valores ya congelados.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        orders = repo.list_plan_orders(cur, plan_id)
        if not orders:
            raise ValueError("Plan sin órdenes")
        enriched = _enrich_orders_snapshot(cur, orders)
        for o in enriched:
            cur.execute(
                """
                UPDATE distribuidora.dispatch_plan_orders
                SET                     client_name = COALESCE(NULLIF(BTRIM(client_name), ''), %s),
                    fantasy_name = COALESCE(NULLIF(BTRIM(fantasy_name), ''), %s),
                    address = COALESCE(NULLIF(BTRIM(address), ''), %s),
                    city = COALESCE(NULLIF(BTRIM(city), ''), %s),
                    seller_name = COALESCE(NULLIF(BTRIM(seller_name), ''), %s),
                    payment_method = COALESCE(NULLIF(BTRIM(payment_method), ''), %s),
                    document_type_to_generate = COALESCE(
                        NULLIF(BTRIM(document_type_to_generate), ''), %s
                    )
                WHERE dispatch_plan_id = %s AND oc_document_id = %s
                """,
                (
                    o.get("client_name"),
                    o.get("fantasy_name"),
                    o.get("address"),
                    o.get("city"),
                    o.get("seller_name"),
                    o.get("payment_method"),
                    o.get("document_type_to_generate"),
                    plan_id,
                    int(o["oc_document_id"]),
                ),
            )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return get_dispatch_plan(plan_id) or {}
