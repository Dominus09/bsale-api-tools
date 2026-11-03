"""Protección transaccional de OC en confirmación de dispatch_plan (ORS y simple).

No introduce UNIQUE global: el historial closed/squared es legítimo.
Bloquea reasignación mientras exista un plan en estados activos o entregados.
"""

from __future__ import annotations

from typing import Any

# Namespace int4 para pg_advisory_xact_lock(key1, key2).
ADVISORY_LOCK_NS_DISPATCH_OC = 192_718_050

# Bloquean reasignación a otro despacho (incluye delivered).
# closed / squared permiten reprogramación autorizada.
CONFIRM_BLOCKING_PLAN_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
        "delivered",
    }
)

REPROGRAMMABLE_PLAN_STATUSES = frozenset({"closed", "squared", "draft"})


class OcConfirmConflictError(ValueError):
    """OC no disponible para confirmar en otro plan."""

    def __init__(self, message: str, *, conflicts: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.conflicts = conflicts or []


def acquire_oc_confirm_locks(cur, document_ids: list[int]) -> None:
    """Advisory xact locks + FOR UPDATE de filas origen (si existen).

    El advisory lock cubre carreras cuando la fila aún no está en un plan
    (FOR UPDATE solo sobre dispatch_plan_orders no bastaría).
    """
    ids = sorted({int(d) for d in document_ids if d is not None})
    for doc_id in ids:
        key2 = int(doc_id) & 0x7FFFFFFF
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (ADVISORY_LOCK_NS_DISPATCH_OC, key2),
        )
    if not ids:
        return
    cur.execute(
        """
        SELECT document_id
        FROM distribuidora.documents
        WHERE document_id = ANY(%s)
        ORDER BY document_id
        FOR UPDATE
        """,
        (ids,),
    )


def find_blocking_oc_assignments(
    cur,
    document_ids: list[int],
    *,
    exclude_plan_id: int | None = None,
) -> list[dict[str, Any]]:
    """OC con plan en estado que impide otra confirmación."""
    ids = sorted({int(d) for d in document_ids if d is not None})
    if not ids:
        return []
    cur.execute(
        """
        SELECT
            dpo.oc_document_id,
            p.id AS dispatch_plan_id,
            p.planning_code,
            p.status,
            p.planning_date,
            p.truck_id,
            p.plan_session_id
        FROM distribuidora.dispatch_plan_orders dpo
        INNER JOIN distribuidora.dispatch_plan p ON p.id = dpo.dispatch_plan_id
        WHERE dpo.oc_document_id = ANY(%s)
          AND p.status = ANY(%s)
          AND (%s::bigint IS NULL OR p.id <> %s::bigint)
        ORDER BY dpo.oc_document_id, p.id DESC
        """,
        (
            ids,
            list(CONFIRM_BLOCKING_PLAN_STATUSES),
            exclude_plan_id,
            exclude_plan_id,
        ),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def assert_ocs_available_for_confirm(
    cur,
    document_ids: list[int],
    *,
    exclude_plan_id: int | None = None,
) -> None:
    conflicts = find_blocking_oc_assignments(
        cur, document_ids, exclude_plan_id=exclude_plan_id
    )
    if conflicts:
        sample = conflicts[0]
        raise OcConfirmConflictError(
            (
                f"OC {sample.get('oc_document_id')} ya está en plan "
                f"{sample.get('planning_code') or sample.get('dispatch_plan_id')} "
                f"(estado={sample.get('status')})."
            ),
            conflicts=conflicts,
        )
