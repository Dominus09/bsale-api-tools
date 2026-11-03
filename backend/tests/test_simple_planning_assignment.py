"""Asignación preliminar OC→camión (solo lectura)."""

from __future__ import annotations

import pytest

from backend.services.distribuidora import simple_planning_assignment_service as assign
from backend.services.distribuidora.simple_planning_preview_service import (
    STATUS_FOUND_AVAILABLE,
    STATUS_NOT_FOUND,
    oc_identity_fingerprint,
)


def _item(
    folio: int,
    *,
    peso: float | None = 10.0,
    quality: str = "known",
    status: str = STATUS_FOUND_AVAILABLE,
    blocking: bool = False,
    plans: list | None = None,
    fingerprint: str | None = None,
):
    fp = fingerprint or oc_identity_fingerprint(
        document_id=folio,
        source_document_id=9000 + folio,
        state=0,
        client_id=folio,
        total_amount=100,
        emission_date=None,
        products=[{"variant_id": 1, "quantity": 2}],
    )
    return {
        "folio": folio,
        "document_id": folio,
        "source_document_id": 9000 + folio,
        "state": 0,
        "status": status,
        "blocking": blocking,
        "warnings": [],
        "client": {"client_name": f"C{folio}", "municipality": "Castro"},
        "products": [
            {"variant_id": 1, "description": "Prod", "code": "P", "quantity": 2}
        ],
        "weight": {
            "peso_total_kg": peso,
            "coverage_percent": 100 if quality == "known" else 80,
            "missing_products": 0 if quality != "unavailable" else 2,
            "quality": quality,
        },
        "billing_status": "pending",
        "invoices": [],
        "plans": plans or [],
        "conflicting_active_plans": [
            p for p in (plans or []) if p.get("status") == "planned"
        ],
        "identity_fingerprint": fp,
        "assigned_to_plan": bool(plans),
    }


def _preview_stub(items):
    def _fn(**_k):
        return {
            "wrote": False,
            "unique_folios": len({i["folio"] for i in items}),
            "counts": {},
            "blocking_count": sum(1 for i in items if i.get("blocking")),
            "items": items,
        }

    return _fn


def test_validate_truck_capacity_overload_and_unknown():
    ok = assign.validate_truck_capacity(
        assigned_weight_kg=500,
        known_weight_kg=500,
        estimated_weight_kg=0,
        unavailable_orders=0,
        truck_max_weight_kg=1000,
    )
    assert ok["overloaded"] is False
    assert ok["utilization_pct"] == 50.0

    over = assign.validate_truck_capacity(
        assigned_weight_kg=1200,
        known_weight_kg=1200,
        estimated_weight_kg=0,
        unavailable_orders=0,
        truck_max_weight_kg=1000,
    )
    assert over["overloaded"] is True
    assert over["blocking"] is True

    unknown = assign.validate_truck_capacity(
        assigned_weight_kg=100,
        known_weight_kg=100,
        estimated_weight_kg=0,
        unavailable_orders=0,
        truck_max_weight_kg=None,
    )
    assert unknown["capacity_known"] is False
    assert "truck_capacity_unknown" in unknown["warnings"]
    assert unknown["overloaded"] is False


def test_single_oc_one_truck():
    items = [_item(100)]
    report = assign.preview_truck_assignments(
        company_id=3,
        office_id=1,
        planning_date="2026-11-10",
        dispatch_name="Sur",
        assignments=[{"folio": 100, "truck_id": 2, "driver_name": "Juan"}],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            2: {"truck_id": 2, "name": "Hino 2", "max_weight_kg": 2000, "active": True}
        },
    )
    assert report["wrote"] is False
    assert report["planning_origin"] == "simple_list"
    assert report["truck_count"] == 1
    truck = report["trucks"][0]
    assert truck["order_count"] == 1
    assert truck["capacity"]["assigned_weight_kg"] == 10.0
    assert truck["picking_preview"]["not_final_invoiced_picking"] is True
    assert truck["picking_preview"]["products_consolidated"][0]["quantity"] == 2


def test_multi_truck_and_duplicates():
    items = [_item(1, peso=100), _item(2, peso=50)]
    report = assign.preview_truck_assignments(
        assignments=[
            {"folio": 1, "truck_id": 1},
            {"folio": 2, "truck_id": 2},
            {"folio": 1, "truck_id": 2},  # dup
        ],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            i: {"truck_id": i, "name": f"T{i}", "max_weight_kg": 500, "active": True}
            for i in ids
        },
    )
    assert 1 in report["request_duplicate_folios"]
    assert any(a["status"] == "duplicate_in_list" for a in report["assignments"])


def test_already_planned_conflict_informative():
    items = [
        _item(
            7,
            plans=[
                {
                    "dispatch_plan_id": 9,
                    "planning_code": "PLAN-9",
                    "status": "planned",
                    "active": True,
                }
            ],
        )
    ]
    report = assign.preview_truck_assignments(
        assignments=[{"folio": 7, "truck_id": 1}],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            1: {"truck_id": 1, "name": "T", "max_weight_kg": 1000, "active": True}
        },
    )
    assert report["active_plan_conflicts"]
    assert report["active_plan_conflicts"][0]["blocking_for_future_confirm"] is True


def test_overload_blocks_truck():
    items = [_item(1, peso=800), _item(2, peso=800)]
    report = assign.preview_truck_assignments(
        assignments=[
            {"folio": 1, "truck_id": 1},
            {"folio": 2, "truck_id": 1},
        ],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            1: {"truck_id": 1, "name": "T", "max_weight_kg": 1000, "active": True}
        },
    )
    assert report["trucks"][0]["capacity"]["overloaded"] is True
    assert report["trucks"][0]["blocking"] is True


def test_truck_without_capacity():
    items = [_item(1)]
    report = assign.preview_truck_assignments(
        assignments=[{"folio": 1, "truck_id": 5}],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            5: {"truck_id": 5, "name": "X", "max_weight_kg": None, "active": True}
        },
    )
    cap = report["trucks"][0]["capacity"]
    assert cap["capacity_known"] is False
    assert "truck_capacity_unknown" in cap["warnings"]


def test_unreliable_weight():
    items = [_item(3, peso=None, quality="unavailable")]
    report = assign.preview_truck_assignments(
        assignments=[{"folio": 3, "truck_id": 1}],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            1: {"truck_id": 1, "name": "T", "max_weight_kg": 500, "active": True}
        },
    )
    assert report["trucks"][0]["capacity"]["unavailable_order_count"] == 1


def test_fingerprint_drift_blocks():
    items = [_item(4, fingerprint="aaa")]
    report = assign.preview_truck_assignments(
        assignments=[
            {"folio": 4, "truck_id": 1, "expected_fingerprint": "bbb"},
        ],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            1: {"truck_id": 1, "name": "T", "max_weight_kg": 500, "active": True}
        },
    )
    assert report["fingerprint_drifts"]
    assert any("modified_after_preview" in (a.get("warnings") or []) for a in report["assignments"])


def test_future_date_accepted():
    items = [_item(8)]
    report = assign.preview_truck_assignments(
        planning_date="2099-01-15",
        assignments=[{"folio": 8, "truck_id": 1}],
        preview_fn=_preview_stub(items),
        trucks_loader=lambda ids: {
            1: {"truck_id": 1, "name": "T", "max_weight_kg": 500, "active": True}
        },
    )
    assert report["planning_date"] == "2099-01-15"


def test_not_found_oc():
    report = assign.preview_truck_assignments(
        assignments=[{"folio": 999, "truck_id": 1}],
        preview_fn=_preview_stub(
            [
                {
                    "folio": 999,
                    "status": STATUS_NOT_FOUND,
                    "blocking": True,
                    "warnings": [],
                    "weight": {"quality": "unavailable"},
                    "products": [],
                    "plans": [],
                    "conflicting_active_plans": [],
                }
            ]
        ),
        trucks_loader=lambda ids: {
            1: {"truck_id": 1, "name": "T", "max_weight_kg": 500, "active": True}
        },
    )
    assert any(a["blocking"] for a in report["assignments"])
