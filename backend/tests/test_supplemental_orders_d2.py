"""F3-D2: facturación → regeneración versionada + conciliación (mocks)."""

from __future__ import annotations

from unittest.mock import MagicMock

from backend.services.distribuidora import dispatch_plan_supplemental_order_service as svc
from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    encode_supplemental_batch_description,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_INVOICED,
    BILLING_PENDING,
)


class _St:
    def __init__(self, billing, inv=None, label=""):
        self.billing_status = billing
        self.billing_label_es = label or billing
        self.confirmed_invoice = inv
        self.credit_notes = []
        self.dispatch_closed = False
        self.planning_eligible = True


def test_assess_invoicing_pending_vs_ready(monkeypatch):
    cur = MagicMock()
    monkeypatch.setattr(
        svc,
        "resolve_operational_statuses_batch",
        lambda *_a, **_k: {
            1: _St(BILLING_PENDING),
            2: _St(
                BILLING_INVOICED,
                inv={"document_id": 90, "number": 100},
                label="Facturada",
            ),
        },
    )
    out = svc.assess_oc_invoicing_for_picking(cur, [1, 2])
    assert out["any_invoiced"] is True
    assert out["all_invoiced"] is False
    assert out["pending_invoice_oc_document_ids"] == [1]
    assert out["ready_oc_document_ids"] == [2]


def test_add_skips_regen_when_pending_invoice(monkeypatch):
    """Grupo suplementario sin facturas: no inventa picking definitivo."""
    preview = {
        "can_add": True,
        "already_in_plan": [],
        "uses_supplemental_group": True,
        "has_picking": True,
    }
    monkeypatch.setattr(svc, "preview_supplemental_orders", lambda *_a, **_k: preview)

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)

    monkeypatch.setattr(
        svc.org_repo,
        "lock_plan_for_organization",
        lambda *_a, **_k: {
            "id": 1,
            "status": "picking_generated",
            "load_organization_status": "organized",
            "load_organization_version": 2,
        },
    )
    monkeypatch.setattr(svc, "_cuadratura_blocks_add", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc.plan_repo,
        "get_plan_by_id",
        lambda *_a, **_k: {"id": 1, "truck_max_weight_kg": 5000, "truck_id": 1},
    )
    monkeypatch.setattr(
        svc,
        "_capacity_for_plan_plus_ocs",
        lambda *_a, **_k: {
            "capacity_known": True,
            "overloaded": False,
            "blocking": False,
            "unevaluable_order_count": 0,
            "incomplete_order_count": 0,
        },
    )
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(svc.plan_repo, "list_plan_orders", lambda *_a, **_k: [])
    monkeypatch.setattr(svc.org_repo, "assignments_table_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(svc.batch_repo, "list_load_batches", lambda *_a, **_k: [])
    created = {
        "id": 55,
        "name": "OC adicionales 01",
        "description": encode_supplemental_batch_description(1),
    }
    monkeypatch.setattr(
        svc.batch_repo, "insert_load_batch", lambda *_a, **_k: created
    )
    monkeypatch.setattr(svc.batch_repo, "max_route_order", lambda *_a, **_k: 10)
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda *_a, **_k: [
            {
                "oc_document_id": 501,
                "client_id": 9,
                "client_name": "Nuevo",
                "oc_number": 501,
            }
        ],
    )
    monkeypatch.setattr(
        "backend.services.order_weight_service.fetch_weights_by_document_ids",
        lambda *_a, **_k: {},
    )
    monkeypatch.setattr(svc.plan_repo, "insert_plan_orders", lambda *_a, **_k: None)
    monkeypatch.setattr(svc.org_repo, "list_order_assignments", lambda *_a, **_k: [])
    monkeypatch.setattr(
        svc.org_repo, "upsert_order_assignment", lambda *_a, **_k: {}
    )
    monkeypatch.setattr(
        svc.org_repo,
        "update_organization_meta",
        lambda *_a, **_k: {
            "load_organization_version": 3,
            "load_organization_status": "organized",
        },
    )
    monkeypatch.setattr(svc.batch_repo, "insert_order_event", lambda *_a, **_k: 1)

    regen_calls = []
    monkeypatch.setattr(
        svc.plan_svc,
        "generate_plan_picking",
        lambda *a, **k: regen_calls.append(k) or {"version": 2},
    )
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    monkeypatch.setattr(
        svc,
        "assess_oc_invoicing_for_picking",
        lambda *_a, **_k: {
            "items": [{"oc_document_id": 501, "ready_for_product_picking": False}],
            "ready_oc_document_ids": [],
            "pending_invoice_oc_document_ids": [501],
            "any_invoiced": False,
            "all_invoiced": False,
            "picking_pending_invoice": True,
        },
    )
    monkeypatch.setattr(
        svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": 1}
    )

    out = svc.add_supplemental_orders_to_plan(
        1,
        oc_document_ids=[501],
        reason="última hora",
        regenerate_picking=True,  # no inventa si falta factura
    )
    assert out["added"] is True
    assert out["picking_updated"] is False
    assert out["picking_pending_invoice"] is True
    assert out["picking_ready"] is False
    assert regen_calls == []
    assert "pendiente de facturación" in out["message_es"].lower()


def test_add_regens_when_invoiced(monkeypatch):
    preview = {
        "can_add": True,
        "already_in_plan": [],
        "uses_supplemental_group": True,
        "has_picking": True,
    }
    monkeypatch.setattr(svc, "preview_supplemental_orders", lambda *_a, **_k: preview)

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)

    monkeypatch.setattr(
        svc.org_repo,
        "lock_plan_for_organization",
        lambda *_a, **_k: {
            "id": 1,
            "status": "picking_generated",
            "load_organization_status": "organized",
            "load_organization_version": 2,
        },
    )
    monkeypatch.setattr(svc, "_cuadratura_blocks_add", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc.plan_repo,
        "get_plan_by_id",
        lambda *_a, **_k: {"id": 1, "truck_max_weight_kg": 5000, "truck_id": 1},
    )
    monkeypatch.setattr(
        svc,
        "_capacity_for_plan_plus_ocs",
        lambda *_a, **_k: {
            "capacity_known": True,
            "overloaded": False,
            "blocking": False,
            "unevaluable_order_count": 0,
            "incomplete_order_count": 0,
        },
    )
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(svc.plan_repo, "list_plan_orders", lambda *_a, **_k: [])
    monkeypatch.setattr(svc.org_repo, "assignments_table_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(svc.batch_repo, "list_load_batches", lambda *_a, **_k: [])
    created = {
        "id": 56,
        "name": "OC adicionales 01",
        "description": encode_supplemental_batch_description(1),
    }
    monkeypatch.setattr(
        svc.batch_repo, "insert_load_batch", lambda *_a, **_k: created
    )
    monkeypatch.setattr(svc.batch_repo, "max_route_order", lambda *_a, **_k: 10)
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda *_a, **_k: [
            {
                "oc_document_id": 502,
                "client_id": 9,
                "client_name": "X",
                "oc_number": 502,
            }
        ],
    )
    monkeypatch.setattr(
        "backend.services.order_weight_service.fetch_weights_by_document_ids",
        lambda *_a, **_k: {},
    )
    monkeypatch.setattr(svc.plan_repo, "insert_plan_orders", lambda *_a, **_k: None)
    monkeypatch.setattr(svc.org_repo, "list_order_assignments", lambda *_a, **_k: [])
    monkeypatch.setattr(
        svc.org_repo, "upsert_order_assignment", lambda *_a, **_k: {}
    )
    monkeypatch.setattr(
        svc.org_repo,
        "update_organization_meta",
        lambda *_a, **_k: {
            "load_organization_version": 3,
            "load_organization_status": "organized",
        },
    )
    monkeypatch.setattr(svc.batch_repo, "insert_order_event", lambda *_a, **_k: 1)
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    monkeypatch.setattr(
        svc,
        "assess_oc_invoicing_for_picking",
        lambda *_a, **_k: {
            "items": [{"oc_document_id": 502, "ready_for_product_picking": True}],
            "ready_oc_document_ids": [502],
            "pending_invoice_oc_document_ids": [],
            "any_invoiced": True,
            "all_invoiced": True,
            "picking_pending_invoice": False,
        },
    )
    monkeypatch.setattr(
        svc.plan_svc,
        "generate_plan_picking",
        lambda *a, **k: {
            "version": 3,
            "action": k.get("regeneration_action"),
        },
    )

    recon = {"document_assignment_complete": True, "conflicts": []}
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_load_batch_service.reconcile_document_load_assignments",
        lambda *a, **k: recon,
    )
    monkeypatch.setattr(
        svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": 1}
    )

    out = svc.add_supplemental_orders_to_plan(
        1, oc_document_ids=[502], reason="facturada"
    )
    assert out["picking_updated"] is True
    assert out["picking"]["version"] == 3
    assert out["picking"]["action"] == svc.ACTION_ADD_SUPPLEMENTAL
    assert out["supplemental_picking_ready"] is True
    assert out["picking_pending_invoice"] is False
