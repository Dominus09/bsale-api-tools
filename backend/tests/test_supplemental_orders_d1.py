"""F3-D1: grupo suplementario, capacidad y estados (mocks / sin analytics)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import dispatch_plan_supplemental_order_service as svc
from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    encode_supplemental_batch_description,
    parse_batch_presentation_role,
    parse_supplemental_seq,
)


def test_supplemental_description_roundtrip():
    desc = encode_supplemental_batch_description(2, "nota")
    assert parse_batch_presentation_role(desc) == "supplemental"
    assert parse_supplemental_seq(desc) == 2


def test_next_seq_from_batches():
    batches = [
        {"name": "grupo 1", "description": "role=priority"},
        {
            "name": "OC adicionales 01",
            "description": encode_supplemental_batch_description(1),
        },
    ]
    assert svc._next_supplemental_seq(batches) == 2


def test_assert_capacity_blocks_overload():
    with pytest.raises(svc.SupplementalOrderError) as ei:
        svc.assert_capacity_allows_add(
            {
                "capacity_known": True,
                "overloaded": True,
                "blocking": True,
                "unevaluable_order_count": 0,
                "incomplete_order_count": 0,
            }
        )
    assert ei.value.code == "truck_overloaded"


def test_assert_capacity_requires_unevaluable_ack():
    with pytest.raises(svc.SupplementalOrderError) as ei:
        svc.assert_capacity_allows_add(
            {
                "capacity_known": True,
                "overloaded": False,
                "blocking": False,
                "unevaluable_order_count": 2,
                "incomplete_order_count": 0,
                "requires_unevaluable_weight_ack": True,
            },
            acknowledge_unevaluable_weight=False,
        )
    assert ei.value.code == "weight_unevaluable_ack_required"
    ack = svc.assert_capacity_allows_add(
        {
            "capacity_known": True,
            "overloaded": False,
            "blocking": False,
            "unevaluable_order_count": 2,
            "incomplete_order_count": 0,
        },
        acknowledge_unevaluable_weight=True,
    )
    assert ack["unevaluable_weight"] is True


def test_add_creates_supplemental_group(monkeypatch):
    plan_id = 50
    locked = {
        "id": plan_id,
        "status": "picking_generated",
        "load_organization_status": "organized",
        "load_organization_version": 3,
    }
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(svc.org_repo, "lock_plan_for_organization", lambda *_a, **_k: locked)
    monkeypatch.setattr(svc, "_cuadratura_blocks_add", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc.plan_repo,
        "get_plan_by_id",
        lambda *_a, **_k: {"id": plan_id, "truck_id": 1, "truck_max_weight_kg": 5000},
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
            "known_weight_kg": 100,
        },
    )
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(
        svc.plan_repo,
        "list_plan_orders",
        lambda *_a, **_k: [
            {"oc_document_id": 1, "client_id": 10, "peso_total_kg": 50, "cobertura_logistica": 100}
        ],
    )
    monkeypatch.setattr(svc.org_repo, "assignments_table_ready", lambda *_c: True)
    monkeypatch.setattr(
        svc.batch_repo,
        "list_load_batches",
        lambda *_a, **_k: [
            {"id": 1, "sort_order": 1, "name": "g1", "description": "role=priority"}
        ],
    )
    created = {}

    def _ins_batch(**kwargs):
        created.update(kwargs)
        return {
            "id": 99,
            "name": kwargs["name"],
            "description": kwargs["description"],
            "sort_order": kwargs["sort_order"],
        }

    monkeypatch.setattr(
        svc.batch_repo, "insert_load_batch", lambda _cur, **kw: _ins_batch(**kw)
    )
    monkeypatch.setattr(svc.batch_repo, "max_route_order", lambda *_a, **_k: 5)
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda *_a, **_k: [
            {
                "oc_document_id": 77,
                "oc_number": 900,
                "route_order": 6,
                "client_id": 10,
                "client_name": "Cliente existente",
            }
        ],
    )
    monkeypatch.setattr(
        "backend.services.order_weight_service.fetch_weights_by_document_ids",
        lambda *_a, **_k: {},
    )
    monkeypatch.setattr(svc.plan_repo, "insert_plan_orders", lambda *_a, **_k: None)
    monkeypatch.setattr(
        svc.org_repo,
        "list_order_assignments",
        lambda *_a, **_k: [
            {"oc_document_id": 1, "load_batch_id": 1, "client_id": 10}
        ],
    )
    upserts = []
    monkeypatch.setattr(
        svc.org_repo,
        "upsert_order_assignment",
        lambda *_a, **kw: upserts.append(kw) or kw,
    )
    monkeypatch.setattr(
        svc.org_repo,
        "update_organization_meta",
        lambda *_a, **_k: {
            "load_organization_version": 4,
            "load_organization_status": "organized",
        },
    )
    events = []
    monkeypatch.setattr(
        svc.batch_repo,
        "insert_order_event",
        lambda *_a, **kw: events.append(kw) or 1,
    )
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    monkeypatch.setattr(svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": plan_id})
    monkeypatch.setattr(
        svc,
        "preview_supplemental_orders",
        lambda *_a, **_k: {
            "can_add": True,
            "uses_supplemental_group": True,
            "has_picking": True,
            "already_in_plan": [],
        },
    )

    out = svc.add_supplemental_orders_to_plan(
        plan_id,
        oc_document_ids=[77],
        reason="Última hora",
        user={"email": "op@x.cl"},
        regenerate_picking=False,
    )
    assert out["added"] is True
    assert out["supplemental_group"]["name"] == "OC adicionales 01"
    assert created["name"] == "OC adicionales 01"
    assert parse_batch_presentation_role(created["description"]) == "supplemental"
    assert upserts and upserts[0]["load_batch_id"] == 99
    assert out["client_same_route_exceptions"]
    assert events[0]["action"] == svc.ACTION_ADD_SUPPLEMENTAL
