"""Exclusión administrativa de OC desde un dispatch_plan (soft-exclude auditable).

No muta Bsale, stock, picking físico ni visitas. Requiere migración 062.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.auth.permissions import has_management_access
from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as batch_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo
from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_operational_statuses_batch,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_INVOICED,
    BILLING_INVOICED_FULL_CN,
    BILLING_PROBABLE,
)
from backend.utils.json_safe import serialize_value

logger = logging.getLogger(__name__)

ACTION_EXCLUDE_ADMIN = "exclude_order_admin"

# Planes con despacho/cierre contable: exclusión solo si no hay ops de la OC.
HARD_BLOCK_PLAN_STATUSES = frozenset({"dispatched", "delivered", "squared"})


class OrderExcludeError(ValueError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "exclude_rejected",
        http_status: int = 400,
        details: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.http_status = http_status
        self.details = details or {}


def _staff_label(user: dict[str, Any] | None) -> str | None:
    if not user:
        return None
    return (
        (user.get("email") or user.get("name") or user.get("username") or "")
        .strip()
        or None
    )


def _require_management(user: dict[str, Any]) -> None:
    if not has_management_access(user):
        raise OrderExcludeError(
            "Solo OWNER/gerencia puede excluir OC de una planificación",
            code="forbidden",
            http_status=403,
        )


def _is_already_excluded(row: dict[str, Any]) -> bool:
    return row.get("excluded_at") is not None


def _collect_blockers(
    cur,
    *,
    plan: dict[str, Any],
    plan_id: int,
    oc_document_id: int,
    order_row: dict[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    status = str(plan.get("status") or "")

    # Factura/boleta vigente en cadena operacional
    try:
        statuses = resolve_operational_statuses_batch(
            cur, [oc_document_id], in_plan=True
        )
        st = statuses.get(oc_document_id)
    except Exception as exc:
        logger.warning("[EXCLUDE_OC] operational status failed: %s", exc)
        st = None
    if st is not None:
        if st.billing_status in (BILLING_INVOICED, BILLING_INVOICED_FULL_CN) or (
            st.confirmed_invoice and st.billing_status != "cancelled"
        ):
            inv = st.confirmed_invoice or {}
            blockers.append(
                {
                    "code": "has_confirmed_invoice",
                    "message": (
                        "La OC tiene factura/boleta vigente asociada. "
                        "No se puede excluir directamente; anular o desvincular "
                        "el documento fiscal primero (procedimiento contable)."
                    ),
                    "invoice_document_id": inv.get("document_id"),
                    "invoice_number": inv.get("number"),
                    "billing_status": st.billing_status,
                }
            )
        elif st.billing_status == BILLING_PROBABLE:
            blockers.append(
                {
                    "code": "has_probable_invoice",
                    "message": (
                        "Hay coincidencia probable de facturación. "
                        "Confirme o descarte el vínculo antes de excluir."
                    ),
                    "billing_status": st.billing_status,
                }
            )

    # Visitas de entrega no anuladas
    try:
        cur.execute(
            """
            SELECT v.*
            FROM distribuidora.delivery_visits v
            WHERE v.dispatch_plan_id = %s
              AND EXISTS (
                    SELECT 1
                    FROM distribuidora.delivery_document_results dr
                    WHERE dr.visit_id = v.id
                      AND dr.oc_document_id = %s
              )
            LIMIT 5
            """,
            (plan_id, oc_document_id),
        )
        visit_cols = [d[0] for d in cur.description]
        live_visits = [dict(zip(visit_cols, vr)) for vr in (cur.fetchall() or [])]
        if live_visits:
            try:
                from backend.repositories.distribuidora import (
                    delivery_visit_repo as visit_repo,
                )

                filtered = [
                    v for v in live_visits if not visit_repo.is_admin_voided_row(v)
                ]
            except Exception:
                filtered = live_visits
            if filtered:
                blockers.append(
                    {
                        "code": "has_delivery_visits",
                        "message": (
                            "Existen gestiones/visitas de entrega vinculadas a esta OC. "
                            "No se puede excluir directamente; use anulación administrativa "
                            "de visitas (void) y luego reintente si aplica."
                        ),
                        "visit_ids": [int(v["id"]) for v in filtered if v.get("id")],
                    }
                )
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass

    # Picking actual con esta OC
    try:
        cur.execute(
            """
            SELECT pc.id, pc.picking_id, p.version, p.is_current
            FROM distribuidora.dispatch_plan_picking_clients pc
            INNER JOIN distribuidora.dispatch_plan_pickings p
                ON p.id = pc.picking_id
            WHERE pc.dispatch_plan_id = %s
              AND pc.oc_document_id = %s
              AND COALESCE(p.is_current, TRUE) = TRUE
            LIMIT 5
            """,
            (plan_id, oc_document_id),
        )
        picking_hits = cur.fetchall() or []
        if picking_hits:
            blockers.append(
                {
                    "code": "in_current_picking",
                    "message": (
                        "La OC está en el picking operativo actual. "
                        "Regenere/superseda el picking sin esta OC o espere "
                        "procedimiento de bodega; no se excluye en caliente."
                    ),
                    "picking_rows": len(picking_hits),
                }
            )
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass

    # Asignaciones de carga (OC o documentos)
    try:
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM distribuidora.dispatch_plan_order_load_assignments
            WHERE dispatch_plan_id = %s AND oc_document_id = %s
            """,
            (plan_id, oc_document_id),
        )
        n_oc_load = int((cur.fetchone() or [0])[0] or 0)
        if n_oc_load > 0:
            blockers.append(
                {
                    "code": "in_load_assignment",
                    "message": (
                        "La OC tiene asignación de carga/lote. "
                        "Desasígnela del lote o regenere la organización de carga "
                        "antes de excluir."
                    ),
                    "assignment_count": n_oc_load,
                }
            )
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass

    # Cuadratura cerrada
    try:
        cur.execute(
            """
            SELECT closed_at, status
            FROM distribuidora.dispatch_plan_cuadratura
            WHERE dispatch_plan_id = %s
            LIMIT 1
            """,
            (plan_id,),
        )
        cq = cur.fetchone()
        if cq and (cq[0] is not None or str(cq[1] or "") == "closed"):
            blockers.append(
                {
                    "code": "cuadratura_closed",
                    "message": (
                        "La cuadratura del plan está cerrada. "
                        "Reabra/ajuste cuadratura con procedimiento contable "
                        "antes de excluir OC."
                    ),
                }
            )
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass

    if status in HARD_BLOCK_PLAN_STATUSES:
        # Aún permitir si no hay otros blockers operativos? User said block if
        # despacho iniciado. Hard block these statuses always for safety.
        blockers.append(
            {
                "code": "plan_status_blocks",
                "message": (
                    f"Estado del plan '{status}' no admite exclusión directa. "
                    "Use procedimiento de despacho/cuadratura según corresponda."
                ),
                "plan_status": status,
            }
        )

    _ = order_row  # reservado para montos en preview
    return blockers


def preview_exclude_order(
    *,
    user: dict[str, Any],
    plan_id: int,
    oc_document_id: int,
) -> dict[str, Any]:
    _require_management(user)
    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.order_exclude_columns_ready(cur):
            raise OrderExcludeError(
                "Migración 062 pendiente: columnas de exclusión administrativa",
                code="migration_pending",
                http_status=503,
            )
        plan = plan_repo.get_plan_by_id(cur, plan_id)
        if not plan:
            raise OrderExcludeError("Plan no encontrado", code="not_found", http_status=404)
        order = plan_repo.get_plan_order(cur, plan_id, oc_document_id)
        if not order:
            raise OrderExcludeError(
                "La OC no pertenece a este plan",
                code="oc_not_in_plan",
                http_status=404,
            )
        already = _is_already_excluded(order)
        blockers = [] if already else _collect_blockers(
            cur,
            plan=plan,
            plan_id=plan_id,
            oc_document_id=oc_document_id,
            order_row=order,
        )
        cur.close()
        return serialize_value(
            {
                "plan_id": plan_id,
                "planning_code": plan.get("planning_code"),
                "plan_status": plan.get("status"),
                "oc_document_id": oc_document_id,
                "oc_number": order.get("oc_number"),
                "oc_total_amount": order.get("oc_total_amount"),
                "client_name": order.get("client_name") or order.get("fantasy_name"),
                "already_excluded": already,
                "can_exclude": already or len(blockers) == 0,
                "blockers": blockers,
                "requires_reason": True,
                "note_es": (
                    "Exclusión administrativa auditable. No elimina la OC en Bsale. "
                    "Una sincronización no la reincorpora automáticamente a este plan."
                ),
            }
        )
    finally:
        conn.close()


def exclude_order(
    *,
    user: dict[str, Any],
    plan_id: int,
    oc_document_id: int,
    reason: str,
) -> dict[str, Any]:
    _require_management(user)
    reason_clean = (reason or "").strip()
    if len(reason_clean) < 5:
        raise OrderExcludeError(
            "Motivo obligatorio (mínimo 5 caracteres)",
            code="reason_required",
            http_status=400,
        )
    user_id = int(user.get("id") or 0)
    if user_id <= 0:
        raise OrderExcludeError(
            "Usuario sin id válido", code="invalid_user", http_status=403
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        if not plan_repo.order_exclude_columns_ready(cur):
            raise OrderExcludeError(
                "Migración 062 pendiente: columnas de exclusión administrativa",
                code="migration_pending",
                http_status=503,
            )
        # Lock plan + order row
        cur.execute(
            """
            SELECT id, status, needs_recalculation
            FROM distribuidora.dispatch_plan
            WHERE id = %s
            FOR UPDATE
            """,
            (plan_id,),
        )
        prow = cur.fetchone()
        if not prow:
            raise OrderExcludeError("Plan no encontrado", code="not_found", http_status=404)
        plan = {"id": prow[0], "status": prow[1], "needs_recalculation": prow[2]}

        cur.execute(
            """
            SELECT *
            FROM distribuidora.dispatch_plan_orders
            WHERE dispatch_plan_id = %s AND oc_document_id = %s
            FOR UPDATE
            """,
            (plan_id, oc_document_id),
        )
        orow = cur.fetchone()
        if not orow:
            raise OrderExcludeError(
                "La OC no pertenece a este plan",
                code="oc_not_in_plan",
                http_status=404,
            )
        order = dict(zip([d[0] for d in cur.description], orow))

        # Idempotente
        if _is_already_excluded(order):
            conn.commit()
            cur.close()
            return serialize_value(
                {
                    "ok": True,
                    "idempotent": True,
                    "plan_id": plan_id,
                    "oc_document_id": oc_document_id,
                    "oc_number": order.get("oc_number"),
                    "excluded_at": order.get("excluded_at"),
                    "exclude_reason": order.get("exclude_reason"),
                    "message": "La OC ya estaba excluida de este plan",
                }
            )

        blockers = _collect_blockers(
            cur,
            plan=plan,
            plan_id=plan_id,
            oc_document_id=oc_document_id,
            order_row=order,
        )
        if blockers:
            raise OrderExcludeError(
                blockers[0]["message"],
                code=str(blockers[0].get("code") or "exclude_blocked"),
                http_status=409,
                details={"blockers": blockers},
            )

        updated = plan_repo.mark_plan_order_excluded(
            cur,
            plan_id=plan_id,
            oc_document_id=oc_document_id,
            user_id=user_id,
            user_name=_staff_label(user),
            reason=reason_clean,
            exclusion_kind="admin",
        )

        # Marcar plan para revisión de ruta/carga/peso (sin regenerar automático).
        cur.execute("SAVEPOINT sp_exclude_recalc")
        try:
            cur.execute(
                """
                UPDATE distribuidora.dispatch_plan
                SET needs_recalculation = TRUE,
                    invalidated_at = COALESCE(invalidated_at, NOW()),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (plan_id,),
            )
            cur.execute("RELEASE SAVEPOINT sp_exclude_recalc")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_exclude_recalc")
            logger.warning(
                "[EXCLUDE_OC] no se pudo marcar needs_recalculation plan_id=%s",
                plan_id,
            )

        event_id = batch_repo.insert_order_event(
            cur,
            plan_id=plan_id,
            action=ACTION_EXCLUDE_ADMIN,
            user_name=_staff_label(user),
            reason=reason_clean,
            oc_document_id=oc_document_id,
            oc_number=order.get("oc_number"),
            payload={
                "exclusion_kind": "admin",
                "actor_user_id": user_id,
                "oc_total_amount": float(order.get("oc_total_amount") or 0),
                "client_name": order.get("client_name"),
                "plan_status": plan.get("status"),
                "before": {
                    "route_order": order.get("route_order"),
                    "oc_total_amount": order.get("oc_total_amount"),
                },
                "flags": {
                    "needs_recalculation": True,
                    "route_review_required": True,
                    "load_review_required": True,
                    "weight_review_required": True,
                },
            },
        )

        # Totales activos post-exclusión
        active = plan_repo.list_plan_orders(cur, plan_id, include_excluded=False)
        active_count = len(active)
        active_amount = sum(float(o.get("oc_total_amount") or 0) for o in active)

        conn.commit()
        cur.close()
        return serialize_value(
            {
                "ok": True,
                "idempotent": False,
                "plan_id": plan_id,
                "oc_document_id": oc_document_id,
                "oc_number": updated.get("oc_number"),
                "oc_total_amount": updated.get("oc_total_amount"),
                "excluded_at": updated.get("excluded_at"),
                "excluded_by_user_id": updated.get("excluded_by_user_id"),
                "excluded_by_user_name": updated.get("excluded_by_user_name"),
                "exclude_reason": updated.get("exclude_reason"),
                "event_id": event_id,
                "active_order_count": active_count,
                "active_oc_amount": active_amount,
                "needs_recalculation": True,
                "message": (
                    "OC excluida del plan. Bsale intacto. "
                    "Revise ruta/carga/peso si el plan tenía snapshot operativo."
                ),
            }
        )
    except OrderExcludeError:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("[EXCLUDE_OC] plan_id=%s oc=%s", plan_id, oc_document_id)
        raise OrderExcludeError(
            f"No se pudo excluir: {exc}",
            code="exclude_failed",
            http_status=500,
        ) from exc
    finally:
        conn.close()


def list_admin_exclusions(*, plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        excluded = plan_repo.list_excluded_plan_orders(cur, plan_id)
        events = []
        try:
            events = [
                e
                for e in batch_repo.list_order_events(cur, plan_id, limit=100)
                if e.get("action") == ACTION_EXCLUDE_ADMIN
            ]
        except Exception:
            events = []
        cur.close()
        return serialize_value(
            {
                "plan_id": plan_id,
                "excluded_orders": excluded,
                "events": events,
                "count": len(excluded),
            }
        )
    finally:
        conn.close()
