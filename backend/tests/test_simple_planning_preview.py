"""Preview de listado OC para planificación simplificada (sin BD real)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
import pytest

from backend.services.distribuidora import simple_planning_preview_service as preview
from backend.services.distribuidora.oc_operational_status import (
    BILLING_INVOICED,
    BILLING_INVOICED_PARTIAL_CN,
    BILLING_PENDING,
    OcDocumentChain,
    OcOperationalStatus,
    LinkedDocument,
)


def _oc_row(folio: int, *, doc_id: int | None = None, state: int = 0, **extra):
    return {
        "document_id": doc_id or folio,
        "folio": folio,
        "state": state,
        "client_id": 10,
        "client_name": "Cliente Demo",
        "municipality": "Castro",
        "address": "Calle 1",
        "total_amount": 15000,
        "emission_date": date(2026, 11, 1),
        "source_document_id": 9000 + folio,
        "company_id": 3,
        "office_id": 1,
        "document_type_id": 33,
        **extra,
    }


def _run(folios, *, by_folio=None, products=None, plans=None, chains=None, weights=None):
    by_folio = by_folio or {}
    products = products or {}
    plans = plans or {}
    chains = chains or ({}, {})
    weights = weights or {}

    def oc_loader(**_k):
        return {f: by_folio[f] for f in _k["folios"] if f in by_folio}

    return preview.preview_ocs_from_folios(
        company_id=3,
        office_id=1,
        folios=folios,
        oc_loader=oc_loader,
        products_loader=lambda ids: {
            i: products.get(i, []) for i in ids
        },
        plans_loader=lambda ids: {i: plans.get(i, []) for i in ids},
        chains_loader=lambda ids, **kw: (
            {i: chains[0].get(i) for i in ids if i in chains[0]},
            {i: chains[1].get(i) for i in ids if i in chains[1]},
        ),
        weight_loader=lambda ids, **kw: {i: weights[i] for i in ids if i in weights},
    )


def test_normalize_excel_paste_and_duplicates():
    norm = preview.normalize_folio_inputs(
        ["70625", " 70626 ", "", "70625", "70627.0", "abc"]
    )
    assert norm["unique_folios"] == [70625, 70626, 70627]
    assert 70625 in norm["duplicate_folios"]
    assert len(norm["invalid"]) == 1
    assert norm["invalid"][0]["status"] == preview.STATUS_INVALID_FOLIO


def test_found_available_with_products_and_weight():
    folio = 70625
    row = _oc_row(folio)
    chain = OcDocumentChain(
        oc_document_id=folio,
        oc_number=folio,
        oc_state=0,
        confirmed_invoices=[
            LinkedDocument(
                document_id=5001,
                number=1001,
                document_type_id=1,
                total_amount=Decimal("15000"),
            )
        ],
    )
    op = OcOperationalStatus(
        oc_document_id=folio,
        billing_status=BILLING_INVOICED,
        dispatch_status="closed",
        planning_eligible=False,
        dispatch_closed=True,
    )
    report = _run(
        [str(folio)],
        by_folio={folio: row},
        products={
            folio: [
                {
                    "detail_id": 1,
                    "variant_id": 9,
                    "description": "Producto A",
                    "code": "A",
                    "quantity": 2,
                    "total_amount": 15000,
                }
            ]
        },
        chains=({folio: chain}, {folio: op}),
        weights={
            folio: {
                "peso_total_kg": 12.5,
                "coverage_percent": 100,
                "missing_products": 0,
                "productos_totales": 1,
                "cantidad_cajas": 1,
                "cantidad_unidades": 2,
            }
        },
    )
    assert report["wrote"] is False
    item = report["items"][0]
    assert item["status"] == preview.STATUS_FOUND_AVAILABLE
    assert item["blocking"] is False
    assert item["document_id"] == folio
    assert item["source_document_id"] == 9000 + folio
    assert item["products"][0]["quantity"] == 2
    assert item["invoice_count"] == 1
    assert item["weight"]["peso_total_kg"] == 12.5


def test_not_found_suggests_audit_oc():
    report = _run(["99999"], by_folio={})
    item = report["items"][0]
    assert item["status"] == preview.STATUS_NOT_FOUND
    assert item["blocking"] is True
    assert item["audit_oc"]["suggested"] is True
    assert "auditoria-oc" in item["audit_oc"]["path"]


def test_duplicate_in_list_is_blocking():
    folio = 100
    report = _run(
        [folio, folio],
        by_folio={folio: _oc_row(folio)},
        chains=(
            {folio: OcDocumentChain(oc_document_id=folio, oc_number=folio, oc_state=0)},
            {
                folio: OcOperationalStatus(
                    oc_document_id=folio,
                    billing_status=BILLING_PENDING,
                    dispatch_status="pending",
                    planning_eligible=True,
                    dispatch_closed=False,
                )
            },
        ),
        weights={folio: {"peso_total_kg": 1, "coverage_percent": 100, "missing_products": 0}},
    )
    statuses = [i["status"] for i in report["items"]]
    assert preview.STATUS_DUPLICATE_IN_LIST in statuses
    assert any(i["blocking"] for i in report["items"] if i["status"] == preview.STATUS_DUPLICATE_IN_LIST)


def test_cancelled_oc():
    folio = 50
    report = _run(
        [folio],
        by_folio={folio: _oc_row(folio, state=8888)},
        chains=(
            {folio: OcDocumentChain(oc_document_id=folio, oc_number=folio, oc_state=8888)},
            {
                folio: OcOperationalStatus(
                    oc_document_id=folio,
                    billing_status="cancelled",
                    dispatch_status="excluded",
                    planning_eligible=False,
                    dispatch_closed=True,
                )
            },
        ),
    )
    assert report["items"][0]["status"] == preview.STATUS_CANCELLED_OR_REPLACED
    assert report["items"][0]["blocking"] is True


def test_already_planned_informative():
    folio = 70
    report = _run(
        [folio],
        by_folio={folio: _oc_row(folio)},
        plans={
            folio: [
                {
                    "dispatch_plan_id": 9,
                    "planning_code": "PLAN-00009",
                    "status": "planned",
                    "planning_date": "2026-11-03",
                    "truck_name": "Hino 2",
                    "route_name": "Sur",
                    "planning_name": "Sur",
                    "active": True,
                }
            ]
        },
        chains=(
            {folio: OcDocumentChain(oc_document_id=folio, oc_number=folio, oc_state=0)},
            {
                folio: OcOperationalStatus(
                    oc_document_id=folio,
                    billing_status=BILLING_INVOICED,
                    dispatch_status="closed",
                    planning_eligible=False,
                    dispatch_closed=True,
                )
            },
        ),
        weights={folio: {"peso_total_kg": 5, "coverage_percent": 100, "missing_products": 0}},
    )
    item = report["items"][0]
    assert item["status"] == preview.STATUS_ALREADY_PLANNED
    assert item["blocking"] is False
    assert item["assigned_to_plan"] is True


def test_pending_and_partial_invoice():
    pending = _run(
        [1],
        by_folio={1: _oc_row(1)},
        chains=(
            {1: OcDocumentChain(oc_document_id=1, oc_number=1, oc_state=0)},
            {
                1: OcOperationalStatus(
                    oc_document_id=1,
                    billing_status=BILLING_PENDING,
                    dispatch_status="pending",
                    planning_eligible=True,
                    dispatch_closed=False,
                )
            },
        ),
        weights={1: {"peso_total_kg": 2, "coverage_percent": 100, "missing_products": 0}},
    )
    assert pending["items"][0]["status"] == preview.STATUS_PENDING_INVOICE

    partial = _run(
        [2],
        by_folio={2: _oc_row(2)},
        chains=(
            {2: OcDocumentChain(oc_document_id=2, oc_number=2, oc_state=0)},
            {
                2: OcOperationalStatus(
                    oc_document_id=2,
                    billing_status=BILLING_INVOICED_PARTIAL_CN,
                    dispatch_status="closed",
                    planning_eligible=False,
                    dispatch_closed=True,
                )
            },
        ),
        weights={2: {"peso_total_kg": 2, "coverage_percent": 100, "missing_products": 0}},
    )
    assert partial["items"][0]["status"] == preview.STATUS_PARTIAL_INVOICE


def test_multiple_invoices_do_not_duplicate_products():
    folio = 80
    chain = OcDocumentChain(
        oc_document_id=folio,
        oc_number=folio,
        oc_state=0,
        confirmed_invoices=[
            LinkedDocument(document_id=1, number=10, document_type_id=1),
            LinkedDocument(document_id=2, number=11, document_type_id=6),
        ],
    )
    report = _run(
        [folio],
        by_folio={folio: _oc_row(folio)},
        products={
            folio: [
                {"detail_id": 1, "quantity": 3, "description": "Solo desde OC"},
            ]
        },
        chains=(
            {folio: chain},
            {
                folio: OcOperationalStatus(
                    oc_document_id=folio,
                    billing_status=BILLING_INVOICED,
                    dispatch_status="closed",
                    planning_eligible=False,
                    dispatch_closed=True,
                )
            },
        ),
        weights={folio: {"peso_total_kg": 3, "coverage_percent": 100, "missing_products": 0}},
    )
    item = report["items"][0]
    assert item["invoice_count"] == 2
    assert len(item["products"]) == 1
    assert item["products"][0]["quantity"] == 3
    assert item["products_source"] == "oc_document_details"


def test_insufficient_weight():
    folio = 90
    report = _run(
        [folio],
        by_folio={folio: _oc_row(folio)},
        chains=(
            {folio: OcDocumentChain(oc_document_id=folio, oc_number=folio, oc_state=0)},
            {
                folio: OcOperationalStatus(
                    oc_document_id=folio,
                    billing_status=BILLING_INVOICED,
                    dispatch_status="closed",
                    planning_eligible=False,
                    dispatch_closed=True,
                )
            },
        ),
        weights={folio: {"peso_total_kg": None, "coverage_percent": 0, "missing_products": 4}},
    )
    assert report["items"][0]["status"] == preview.STATUS_INSUFFICIENT_WEIGHT
    assert report["items"][0]["blocking"] is False


def test_rejects_oversized_unique_list():
    folios = list(range(1, preview.MAX_FOLIOS + 2))
    with pytest.raises(preview.SimplePlanningPreviewError, match="Máximo"):
        preview.normalize_folio_inputs(folios)


def test_rejects_non_oc_type():
    with pytest.raises(preview.SimplePlanningPreviewError):
        preview.preview_ocs_from_folios(
            folios=[1],
            document_type_id=1,
            oc_loader=lambda **_k: {},
            products_loader=lambda *_a, **_k: {},
            plans_loader=lambda *_a, **_k: {},
            chains_loader=lambda *_a, **_k: ({}, {}),
            weight_loader=lambda *_a, **_k: {},
        )
