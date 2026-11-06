"""Incorporación suplementaria de OC ya facturadas.

La regla «no corresponde a una nueva planificación» se conserva para el alta
de un despacho nuevo y no se aplica a OC adicionales.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import dispatch_plan_supplemental_order_service as svc
from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    RESERVED_SUPPLEMENTAL_GROUP_NAME,
    encode_supplemental_batch_description,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_CANCELLED,
    BILLING_INVOICED,
    BILLING_INVOICED_FULL_CN,
    BILLING_PENDING,
    LinkedDocument,
    OcDocumentChain,
    OcOperationalStatus,
    blocks_planning_admission,
    blocks_supplemental_admission,
)


def _status(
    oc_id: int,
    oc_number: int,
    billing: str,
    *,
    invoice_id: int | None = None,
    invoice_number: int | None = None,
    extra_invoice_id: int | None = None,
) -> OcOperationalStatus:
    invoices = []
    if invoice_id is not None:
        invoices.append(
            LinkedDocument(
                document_id=invoice_id,
                number=invoice_number,
                document_type_id=1,
            )
        )
    if extra_invoice_id is not None:
        invoices.append(
            LinkedDocument(
                document_id=extra_invoice_id,
                number=(invoice_number or 0) + 1,
                document_type_id=1,
            )
        )
    invoiced = billing not in {BILLING_PENDING, BILLING_CANCELLED}
    chain = OcDocumentChain(
        oc_document_id=oc_id,
        oc_number=oc_number,
        confirmed_invoices=invoices,
    )
    return OcOperationalStatus(
        oc_document_id=oc_id,
        billing_status=billing,
        dispatch_status="closed" if invoiced or billing == BILLING_CANCELLED else "pending",
        planning_eligible=not invoiced and billing != BILLING_CANCELLED,
        dispatch_closed=invoiced or billing == BILLING_CANCELLED,
        billing_label_es=billing,
        confirmed_invoice=(
            {"document_id": invoice_id, "number": invoice_number} if invoice_id else None
        ),
        chain=chain,
    )


def test_new_planning_still_blocks_invoiced_oc():
    st = _status(3959592, 70566, BILLING_INVOICED, invoice_id=3966323, invoice_number=2792373)
    assert st.planning_eligible is False
    assert blocks_planning_admission(st) is True
    assert blocks_supplemental_admission(st) is False


def test_supplemental_blocks_cancelled_and_full_credit_note_only():
    pending = _status(1, 100, BILLING_PENDING)
    invoiced = _status(3959595, 70567, BILLING_INVOICED, invoice_id=3962878, invoice_number=2789213)
    cancelled = _status(3, 300, BILLING_CANCELLED)
    full_cn = _status(4, 400, BILLING_INVOICED_FULL_CN, invoice_id=9, invoice_number=9)
    assert blocks_supplemental_admission(pending) is False
    assert blocks_supplemental_admission(invoiced) is False
    assert blocks_supplemental_admission(cancelled) is True
    assert blocks_supplemental_admission(full_cn) is True
    assert blocks_planning_admission(invoiced) is True


def test_ambiguous_invoice_rejected_without_touching_new_planning_rule():
    st = _status(
        10,
        1000,
        BILLING_INVOICED,
        invoice_id=20,
        invoice_number=1,
        extra_invoice_id=21,
    )
    found = svc.collect_supplemental_admission_rejections(
        MagicMock(), 79, [10], {10: st}
    )
    assert found[0]["conflict"] == "invoice_ambiguous"
    assert found[0]["oc_number"] == 1000
    assert found[0]["saved"] is False


def test_shared_invoice_in_same_batch_rejected():
    a = _status(1, 70566, BILLING_INVOICED, invoice_id=50, invoice_number=111)
    b = _status(2, 70567, BILLING_INVOICED, invoice_id=50, invoice_number=111)
    found = svc.collect_supplemental_admission_rejections(
        MagicMock(), 79, [1, 2], {1: a, 2: b}
    )
    assert {row["conflict"] for row in found} == {"invoice_shared_in_batch"}
    assert {row["oc_number"] for row in found} == {70566, 70567}
    text = svc.format_supplemental_rejections(found)
    assert "rollback completo" in text
    assert "OC 70566" in text
    assert "OC 70567" in text


class _LiveCursor:
    """Cursor mínimo. El módulo se parchea para que el servicio lo trate como Postgres."""

    __module__ = "psycopg2.extras"

    def __init__(self, docs, plans=None, invoices=None):
        self.docs = docs
        self.plans = plans or []
        self.invoices = invoices or []
        self.description = [("document_id",)]
        self._rows = []

    def execute(self, sql, _params=None):
        text = " ".join(sql.split())
        if "dispatch_plan_orders" in text:
            self.description = [
                ("oc_document_id",),
                ("dispatch_plan_id",),
                ("planning_code",),
                ("status",),
                ("planning_date",),
                ("truck_id",),
                ("plan_session_id",),
                ("block_reason",),
            ]
            self._rows = self.plans
        elif "document_load_assignments" in text:
            self.description = [
                ("related_document_id",),
                ("dispatch_plan_id",),
                ("load_batch_id",),
                ("planning_code",),
                ("name",),
            ]
            self._rows = self.invoices
        else:
            self.description = [
                ("document_id",),
                ("number",),
                ("company_id",),
                ("office_id",),
                ("state",),
            ]
            self._rows = self.docs

    def fetchall(self):
        return self._rows


def test_wrong_office_and_other_plan_and_cross_group():
    invoiced = _status(3959592, 70566, BILLING_INVOICED, invoice_id=3966323, invoice_number=2792373)
    other = _status(3959595, 70567, BILLING_INVOICED, invoice_id=3962878, invoice_number=2789213)
    cur = _LiveCursor(
        docs=[
            (3959592, 70566, 3, 9, 0),
            (3959595, 70567, 3, 1, 0),
        ],
        plans=[
            (
                3959595,
                80,
                "PLAN-00080",
                "picking_generated",
                None,
                1,
                None,
                "plan_status",
            )
        ],
        invoices=[(3966323, 79, 8, "PLAN-00079", "consolidado")],
    )
    found = svc._scope_rejections_live(
        cur,
        79,
        [3959592, 3959595],
        {3959592: invoiced, 3959595: other},
        {3966323: [3959592], 3962878: [3959595]},
    )
    by_code = {row["conflict"]: row for row in found}
    assert by_code["oc_wrong_company_or_office"]["oc_number"] == 70566
    assert by_code["oc_on_other_plan"]["oc_number"] == 70567
    assert "PLAN-00080" in by_code["oc_on_other_plan"]["message"]
    assert by_code["invoice_crosses_groups"]["oc_number"] == 70566
    assert "consolidado" in by_code["invoice_crosses_groups"]["message"]


def _wire_add(monkeypatch, statuses, *, plan_status="picking_generated", cuadratura=None, orders=None):
    plan_id = 79
    locked = {
        "id": plan_id,
        "status": plan_status,
        "load_organization_status": "organized",
        "load_organization_version": 3,
    }
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(svc.org_repo, "lock_plan_for_organization", lambda *_a, **_k: locked)
    monkeypatch.setattr(svc, "_cuadratura_blocks_add", lambda *_a, **_k: cuadratura)
    monkeypatch.setattr(
        svc.plan_repo,
        "get_plan_by_id",
        lambda *_a, **_k: {"id": plan_id, "truck_max_weight_kg": 7600},
    )
    monkeypatch.setattr(
        svc,
        "_capacity_for_plan_plus_ocs",
        lambda *_a, **_k: {
            "capacity_known": True,
            "overloaded": False,
            "blocking": False,
            "unevaluable_order_count": 0,
            "incomplete_order_count": 1,
            "additional_known_weight_kg": 119.9,
        },
    )
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: statuses)
    monkeypatch.setattr(
        svc.plan_repo,
        "list_plan_orders",
        lambda *_a, **_k: orders or [],
    )
    monkeypatch.setattr(svc.org_repo, "assignments_table_ready", lambda *_c: True)
    monkeypatch.setattr(
        svc.batch_repo,
        "list_load_batches",
        lambda *_a, **_k: [
            {"id": 8, "name": "consolidado", "description": "role=consolidated"},
            {"id": 9, "name": "grupo 1", "description": "role=priority"},
        ],
    )
    monkeypatch.setattr(
        svc.batch_repo,
        "insert_load_batch",
        lambda _cur, **kw: {"id": 50, "name": kw["name"], "description": kw["description"], "sort_order": kw["sort_order"]},
    )
    monkeypatch.setattr(svc.batch_repo, "max_route_order", lambda *_a, **_k: 25)
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda _c, orders_payload: [
            {
                "oc_document_id": row["oc_document_id"],
                "oc_number": 70566 if row["oc_document_id"] == 3959592 else 70567,
                "client_id": 1,
                "client_name": "Cliente",
            }
            for row in orders_payload
        ],
    )
    monkeypatch.setattr(
        "backend.services.order_weight_service.fetch_weights_by_document_ids",
        lambda *_a, **_k: {},
    )
    inserted = {"called": False}

    def _insert(*_a, **_k):
        inserted["called"] = True

    monkeypatch.setattr(svc.plan_repo, "insert_plan_orders", _insert)
    monkeypatch.setattr(svc.org_repo, "list_order_assignments", lambda *_a, **_k: [])
    monkeypatch.setattr(svc.org_repo, "upsert_order_assignment", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc.org_repo,
        "update_organization_meta",
        lambda *_a, **_k: {"load_organization_version": 4, "load_organization_status": "organized"},
    )
    monkeypatch.setattr(svc.batch_repo, "insert_order_event", lambda *_a, **_k: 1)
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    monkeypatch.setattr(svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": plan_id})
    monkeypatch.setattr(
        svc,
        "preview_supplemental_orders",
        lambda *_a, **_k: {"can_add": True, "already_in_plan": []},
    )
    monkeypatch.setattr(
        svc,
        "assess_oc_invoicing_for_picking",
        lambda *_a, **_k: {
            "items": [],
            "ready_oc_document_ids": [3959592, 3959595],
            "pending_invoice_oc_document_ids": [],
            "any_invoiced": True,
            "all_invoiced": True,
            "picking_pending_invoice": False,
        },
    )
    monkeypatch.setattr(
        svc.plan_svc,
        "generate_plan_picking",
        lambda *_a, **_k: {"version": 4, "picking_version": 4},
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_load_batch_service.reconcile_document_load_assignments",
        lambda *_a, **_k: {"document_assignment_complete": True},
    )
    return conn, inserted


def test_two_invoiced_ocs_are_added_together(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_INVOICED, invoice_id=3966323, invoice_number=2792373),
        3959595: _status(3959595, 70567, BILLING_INVOICED, invoice_id=3962878, invoice_number=2789213),
    }
    _conn, inserted = _wire_add(monkeypatch, statuses)
    out = svc.add_supplemental_orders_to_plan(
        79,
        oc_document_ids=[3959592, 3959595],
        reason="OC facturadas de prueba",
        user={"email": "op@x.cl"},
        acknowledge_incomplete_weight=True,
    )
    assert out["added"] is True
    assert inserted["called"] is True
    assert out["supplemental_group"]["name"] == RESERVED_SUPPLEMENTAL_GROUP_NAME
    assert "nueva planificación" not in out["message_es"]


def test_one_invalid_oc_rolls_back_the_whole_batch(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_INVOICED, invoice_id=3966323, invoice_number=2792373),
        3959595: _status(3959595, 70567, BILLING_CANCELLED),
    }
    conn, inserted = _wire_add(monkeypatch, statuses)
    with pytest.raises(svc.SupplementalOrderError) as exc:
        svc.add_supplemental_orders_to_plan(
            79,
            oc_document_ids=[3959592, 3959595],
            reason="lote mixto",
            user={"email": "op@x.cl"},
            acknowledge_incomplete_weight=True,
        )
    err = exc.value
    assert err.http_status == 409
    assert "OC 70567" in str(err)
    assert "oc_cancelled" in str(err)
    assert "rollback completo" in str(err)
    assert "OC 70566" not in str(err)
    assert err.details["saved"] is False
    assert inserted["called"] is False
    conn.rollback.assert_called()


def test_pending_invoice_still_allowed(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_PENDING),
    }
    _conn, inserted = _wire_add(monkeypatch, statuses)
    out = svc.add_supplemental_orders_to_plan(
        79,
        oc_document_ids=[3959592],
        reason="pendiente de factura",
        user={"email": "op@x.cl"},
        acknowledge_incomplete_weight=True,
    )
    assert out["added"] is True
    assert inserted["called"] is True


def test_duplicate_on_same_plan_does_not_insert(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_INVOICED, invoice_id=1, invoice_number=1),
    }
    conn, inserted = _wire_add(
        monkeypatch,
        statuses,
        orders=[{"oc_document_id": 3959592}],
    )
    with pytest.raises(svc.SupplementalOrderError) as exc:
        svc.add_supplemental_orders_to_plan(
            79,
            oc_document_ids=[3959592],
            reason="duplicada",
            user={"email": "op@x.cl"},
            acknowledge_incomplete_weight=True,
        )
    assert exc.value.code == "oc_duplicate"
    assert "OC 70566" in str(exc.value)
    assert inserted["called"] is False
    conn.rollback.assert_called()


def test_idempotent_retry_of_invoiced_oc_keeps_group(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_INVOICED, invoice_id=1, invoice_number=1),
    }
    desc = encode_supplemental_batch_description(1, "reservado")
    _conn, inserted = _wire_add(
        monkeypatch,
        statuses,
        orders=[{"oc_document_id": 3959592}],
    )
    monkeypatch.setattr(
        svc.batch_repo,
        "list_load_batches",
        lambda *_a, **_k: [
            {"id": 50, "name": RESERVED_SUPPLEMENTAL_GROUP_NAME, "description": desc, "sort_order": 4}
        ],
    )
    monkeypatch.setattr(
        svc.org_repo,
        "list_order_assignments",
        lambda *_a, **_k: [{"oc_document_id": 3959592, "load_batch_id": 50}],
    )
    out = svc.add_supplemental_orders_to_plan(
        79,
        oc_document_ids=[3959592],
        reason="reintento",
        user={"email": "op@x.cl"},
        acknowledge_incomplete_weight=True,
    )
    assert out["added"] is False
    assert out["status"] == "unchanged"
    assert inserted["called"] is False


def test_closed_cuadratura_and_dispatched_plan_still_block(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_INVOICED, invoice_id=1, invoice_number=1),
    }
    _conn, inserted = _wire_add(
        monkeypatch,
        statuses,
        cuadratura={"code": "cuadratura_closed", "message": "cuadratura cerrada"},
    )
    with pytest.raises(svc.SupplementalOrderError) as exc:
        svc.add_supplemental_orders_to_plan(
            79,
            oc_document_ids=[3959592],
            reason="cuadratura",
            user={"email": "op@x.cl"},
            acknowledge_incomplete_weight=True,
        )
    assert exc.value.code == "cuadratura_closed"
    assert inserted["called"] is False

    _wire_add(monkeypatch, statuses, plan_status="delivered")
    with pytest.raises(svc.SupplementalOrderError) as exc2:
        svc.add_supplemental_orders_to_plan(
            79,
            oc_document_ids=[3959592],
            reason="entregado",
            user={"email": "op@x.cl"},
            acknowledge_incomplete_weight=True,
        )
    assert exc2.value.code == "plan_not_addable"


def test_incomplete_weight_still_requires_ack(monkeypatch):
    statuses = {
        3959592: _status(3959592, 70566, BILLING_INVOICED, invoice_id=1, invoice_number=1),
    }
    _conn, inserted = _wire_add(monkeypatch, statuses)
    monkeypatch.setattr(
        svc,
        "_capacity_for_plan_plus_ocs",
        lambda *_a, **_k: {
            "capacity_known": True,
            "overloaded": False,
            "blocking": False,
            "unevaluable_order_count": 0,
            "incomplete_order_count": 1,
        },
    )
    with pytest.raises(svc.SupplementalOrderError) as exc:
        svc.add_supplemental_orders_to_plan(
            79,
            oc_document_ids=[3959592],
            reason="sin ack de peso",
            user={"email": "op@x.cl"},
            acknowledge_incomplete_weight=False,
        )
    assert exc.value.code == "weight_incomplete_ack_required"
    assert inserted["called"] is False
