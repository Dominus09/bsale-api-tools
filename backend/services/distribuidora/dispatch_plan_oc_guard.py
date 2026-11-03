"""Protección transaccional de OC en confirmación de dispatch_plan (ORS y simple).

No introduce UNIQUE global: el historial permanece. Sin evidencia de cantidad
reprogramable, se bloquea cualquier plan no-draft y cualquier señal de entrega.
"""

from __future__ import annotations

from typing import Any

# Namespace int4 para pg_advisory_xact_lock(key1, key2).
ADVISORY_LOCK_NS_DISPATCH_OC = 192_718_050
# Segundo namespace para source_document_id Bsale (reemisiones).
ADVISORY_LOCK_NS_DISPATCH_OC_SOURCE = 192_718_051

# Cualquier plan confirmado/cerrado bloquea hasta existir saldo de reprogramación.
# Solo ``draft`` no bloquea (borrador no operativo).
CONFIRM_BLOCKING_PLAN_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
        "delivered",
        "closed",
        "squared",
    }
)

# Unicamente borradores; closed/squared NO son reprogramables sin modelo de saldos.
REPROGRAMMABLE_PLAN_STATUSES = frozenset({"draft"})

DELIVERY_EVENT_ACTION = "delivery_stop_status"


class OcConfirmConflictError(ValueError):
    """OC no disponible para confirmar en otro plan."""

    def __init__(
        self,
        message: str,
        *,
        conflicts: list[dict[str, Any]] | None = None,
        code: str = "oc_plan_conflict",
    ):
        super().__init__(message)
        self.conflicts = conflicts or []
        self.code = code


class OcDocumentStateError(ValueError):
    """OC cancelada/reemplazada o inexistente."""

    def __init__(self, message: str, *, code: str = "oc_document_invalid"):
        super().__init__(message)
        self.code = code


def _unique_ids(document_ids: list[int]) -> list[int]:
    return sorted({int(d) for d in document_ids if d is not None})


def expand_document_ids_for_reissue(cur, document_ids: list[int]) -> list[int]:
    """Incluye hermanos por ``source_document_id`` (reemisión Bsale).

    Si el modelo no tiene source_document_id, retorna los ids originales.
    """
    ids = _unique_ids(document_ids)
    if not ids:
        return []
    try:
        cur.execute(
            """
            SELECT DISTINCT d2.document_id
            FROM distribuidora.documents d
            INNER JOIN distribuidora.documents d2
                ON d2.source_document_id IS NOT NULL
               AND d.source_document_id IS NOT NULL
               AND d2.source_document_id = d.source_document_id
            WHERE d.document_id = ANY(%s)
            """,
            (ids,),
        )
        extra = [int(r[0]) for r in cur.fetchall() if r and r[0] is not None]
    except Exception:
        # Tabla/columna ausente en entornos parciales de test.
        return ids
    return _unique_ids([*ids, *extra])


def acquire_oc_confirm_locks(cur, document_ids: list[int]) -> list[int]:
    """Advisory xact locks (document_id + source) + FOR UPDATE de filas origen.

    Usado por ORS y simple_list. Retorna el set expandido de document_ids.
    """
    ids = expand_document_ids_for_reissue(cur, document_ids)
    for doc_id in ids:
        key2 = int(doc_id) & 0x7FFFFFFF
        cur.execute(
            "SELECT pg_advisory_xact_lock(%s, %s)",
            (ADVISORY_LOCK_NS_DISPATCH_OC, key2),
        )
    # Locks adicionales por source_document_id Bsale.
    try:
        cur.execute(
            """
            SELECT DISTINCT source_document_id
            FROM distribuidora.documents
            WHERE document_id = ANY(%s)
              AND source_document_id IS NOT NULL
            ORDER BY source_document_id
            """,
            (ids,),
        )
        for (source_id,) in cur.fetchall():
            if source_id is None:
                continue
            key2 = int(source_id) & 0x7FFFFFFF
            cur.execute(
                "SELECT pg_advisory_xact_lock(%s, %s)",
                (ADVISORY_LOCK_NS_DISPATCH_OC_SOURCE, key2),
            )
    except Exception:
        pass

    if ids:
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
    return ids


def find_blocking_oc_assignments(
    cur,
    document_ids: list[int],
    *,
    exclude_plan_id: int | None = None,
) -> list[dict[str, Any]]:
    """OC con plan en estado que impide otra confirmación."""
    ids = _unique_ids(document_ids)
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
            p.plan_session_id,
            'plan_status'::text AS block_reason
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


def find_delivery_evidence(
    cur,
    document_ids: list[int],
    *,
    exclude_plan_id: int | None = None,
) -> list[dict[str, Any]]:
    """Evidencia de entrega (evento delivery_stop_status=delivered).

    No calcula saldos: cualquier entrega registrada bloquea reprogramación.
    """
    ids = _unique_ids(document_ids)
    if not ids:
        return []
    try:
        cur.execute(
            """
            SELECT
                e.oc_document_id,
                e.dispatch_plan_id,
                p.planning_code,
                p.status,
                e.action,
                e.payload->>'status' AS delivery_status,
                'delivery_event'::text AS block_reason
            FROM distribuidora.dispatch_plan_order_events e
            INNER JOIN distribuidora.dispatch_plan p ON p.id = e.dispatch_plan_id
            WHERE e.action = %s
              AND lower(COALESCE(e.payload->>'status', '')) = 'delivered'
              AND (
                    e.oc_document_id = ANY(%s)
                    OR e.dispatch_plan_id IN (
                        SELECT dpo.dispatch_plan_id
                        FROM distribuidora.dispatch_plan_orders dpo
                        WHERE dpo.oc_document_id = ANY(%s)
                    )
              )
              AND (%s::bigint IS NULL OR e.dispatch_plan_id <> %s::bigint)
            ORDER BY e.id DESC
            """,
            (
                DELIVERY_EVENT_ACTION,
                ids,
                ids,
                exclude_plan_id,
                exclude_plan_id,
            ),
        )
    except Exception:
        return []
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def assert_documents_active_for_confirm(cur, document_ids: list[int]) -> None:
    """Rechaza OC inexistentes o state != 0 (cancelada/reemplazada)."""
    ids = _unique_ids(document_ids)
    if not ids:
        raise OcDocumentStateError("Sin document_ids para confirmar")
    cur.execute(
        """
        SELECT document_id, COALESCE(state, 0) AS state
        FROM distribuidora.documents
        WHERE document_id = ANY(%s)
        """,
        (ids,),
    )
    found = {int(r[0]): int(r[1] or 0) for r in cur.fetchall()}
    missing = [i for i in ids if i not in found]
    if missing:
        raise OcDocumentStateError(
            f"OC document_id inexistente(s): {missing[:8]}",
            code="oc_not_found",
        )
    bad = [i for i, st in found.items() if st != 0]
    if bad:
        raise OcDocumentStateError(
            f"OC cancelada/reemplazada (state!=0): {bad[:8]}",
            code="oc_cancelled_or_replaced",
        )


def assert_ocs_available_for_confirm(
    cur,
    document_ids: list[int],
    *,
    exclude_plan_id: int | None = None,
) -> None:
    """Validación segura compartida por ORS y simple_list.

    Política (sin inventar saldos):
    - Bloquea planes en CONFIRM_BLOCKING (incluye closed/squared/delivered).
    - Bloquea si hay evento de entrega delivered ligado a la OC/plan.
    - No permite reprogramar solo porque el plan esté cerrado/cuadrado.
    """
    ids = _unique_ids(document_ids)
    conflicts = find_blocking_oc_assignments(
        cur, ids, exclude_plan_id=exclude_plan_id
    )
    if conflicts:
        sample = conflicts[0]
        raise OcConfirmConflictError(
            (
                f"OC {sample.get('oc_document_id')} ya está en plan "
                f"{sample.get('planning_code') or sample.get('dispatch_plan_id')} "
                f"(estado={sample.get('status')}). "
                "Reprogramación bloqueada: el modelo no expone saldos "
                "entregados/pendientes confiables."
            ),
            conflicts=conflicts,
            code="oc_plan_conflict",
        )

    deliveries = find_delivery_evidence(
        cur, ids, exclude_plan_id=exclude_plan_id
    )
    if deliveries:
        sample = deliveries[0]
        raise OcConfirmConflictError(
            (
                f"OC {sample.get('oc_document_id') or ids[0]} tiene evidencia de "
                f"entrega en plan {sample.get('dispatch_plan_id')}. "
                "No se reprograma mercadería posiblemente entregada."
            ),
            conflicts=deliveries,
            code="oc_delivery_evidence",
        )


# Trabajo futuro documentado (no inventar saldos):
REPLAN_MODEL_GAP = (
    "Para reprogramar con seguridad hace falta: (1) cantidad entregada vs pendiente "
    "por OC/línea, (2) vínculo explícito devolución→replan, (3) override auditado. "
    "Hasta entonces closed/squared/delivered bloquean."
)
