"""Comparativo por oficina — lógica pura + servicio (FakeRepo)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from backend.services.analytics_v2.access import CompanyScope
from backend.services.analytics_v2.cost_control_service import (
    CostControlNotFound,
    CostControlValidationError,
    CostControlV2Service,
)
from backend.services.analytics_v2.cost_office_compare import (
    OfficeCellBuild,
    OfficeInfoTag,
    build_office_cell,
    info_tag_for_cell,
    office_supplier_spread,
)
from backend.services.analytics_v2.contracts import MovementClass
from backend.tests.test_analytics_v2_cost_control import FakeRepo, fact, purchase

D = Decimal
TODAY = date(2026, 10, 27)
SCOPE = CompanyScope(company_id=3, company_name="La Quillotana SpA")


def _svc(repo: FakeRepo) -> CostControlV2Service:
    return CostControlV2Service(repo, SCOPE, today=TODAY, use_cache=False)


def test_info_tags_factura_guia_sin_info():
    assert (
        info_tag_for_cell(
            last_documented_date=None,
            last_factura_date=None,
            last_movement=None,
            today=TODAY,
        )
        == OfficeInfoTag.SIN_INFORMACION
    )
    assert (
        info_tag_for_cell(
            last_documented_date=date(2026, 10, 20),
            last_factura_date=None,
            last_movement=MovementClass.GUIA_DESPACHO,
            today=TODAY,
        )
        == OfficeInfoTag.SOLO_GUIA
    )
    assert (
        info_tag_for_cell(
            last_documented_date=date(2026, 10, 20),
            last_factura_date=date(2026, 10, 20),
            last_movement=MovementClass.FACTURA,
            today=TODAY,
        )
        == OfficeInfoTag.ACTUALIZADO
    )
    assert (
        info_tag_for_cell(
            last_documented_date=date(2025, 1, 1),
            last_factura_date=date(2025, 1, 1),
            last_movement=MovementClass.FACTURA,
            today=TODAY,
        )
        == OfficeInfoTag.ANTIGUO
    )
    # Guía reciente no “refresca” una factura antigua (caso Cristal of.3).
    assert (
        info_tag_for_cell(
            last_documented_date=date(2026, 10, 8),
            last_factura_date=date(2025, 9, 1),
            last_movement=MovementClass.GUIA_DESPACHO,
            today=TODAY,
        )
        == OfficeInfoTag.ANTIGUO
    )


def test_build_cell_does_not_label_office_average_and_ignores_guia_as_supplier():
    offices = {1: "BODEGA CENTRAL"}
    lines = [
        {
            "reception_id": 2,
            "detail_id": 20,
            "admission_date": date(2026, 10, 15),
            "office_id": 1,
            "document": "GUÍA DE DESPACHO ELECTRÓNICA",
            "document_number": 100,
            "movement_class": "guia_despacho",
            "cost": D("450"),
            "quantity": D("24"),
            "details_complete": True,
        },
        {
            "reception_id": 1,
            "detail_id": 10,
            "admission_date": date(2026, 9, 1),
            "office_id": 1,
            "document": "Factura",
            "document_number": 99,
            "movement_class": "factura",
            "cost": D("437"),
            "quantity": D("100"),
            "details_complete": True,
        },
    ]
    cell = build_office_cell(
        OfficeCellBuild(
            office_id=1,
            office_name="BODEGA CENTRAL",
            stock_qty=D("50"),
            lines_newest_first=lines,
        ),
        today=TODAY,
        offices=offices,
    )
    assert cell["last_reception_unit_cost"]["cost"] == "450"
    assert cell["last_reception_unit_cost"]["movement_label"] == "Guía"
    assert cell["last_supplier_purchase"]["cost"] == "437"
    assert cell["last_supplier_purchase"]["comparable_supplier"] is True
    # Frescura de compra (factura 1/9) — no de la guía del 15/10.
    assert cell["info_tag"] == OfficeInfoTag.ANTIGUO.value
    assert "promedio" not in str(cell).lower()


def test_build_cell_ignores_internal_transfer_as_purchase_or_documented():
    offices = {4: "QUILLOTANA I"}
    lines = [
        {
            "reception_id": 9,
            "detail_id": 90,
            "admission_date": date(2026, 10, 30),
            "office_id": 4,
            "document": "GUÍA DE DESPACHO ELECTRÓNICA",
            "document_number": 500,
            "movement_class": "transferencia_interna",
            "cost": D("423"),
            "quantity": D("48"),
            "details_complete": True,
        }
    ]
    cell = build_office_cell(
        OfficeCellBuild(
            office_id=4,
            office_name="QUILLOTANA I",
            stock_qty=D("140"),
            lines_newest_first=lines,
        ),
        today=TODAY,
        offices=offices,
    )
    assert cell["last_reception_unit_cost"] is None
    assert cell["last_supplier_purchase"] is None
    assert cell["last_guide"] is None
    assert cell["info_tag"] == OfficeInfoTag.SIN_INFORMACION.value


def test_spread_only_uses_facturas():
    cells = [
        {
            "office_id": 1,
            "last_supplier_purchase": {"cost": "400"},
        },
        {
            "office_id": 3,
            "last_supplier_purchase": {"cost": "440"},
        },
        {
            "office_id": 4,
            "last_supplier_purchase": None,
        },
    ]
    spread = office_supplier_spread(cells)
    assert spread is not None
    assert spread["min_cost"] == "400"
    assert spread["max_cost"] == "440"
    assert spread["spread_pct"] == "10.00"
    assert spread["alert"] is True


def test_list_office_compare_excludes_auxiliary_offices():
    rows = [
        fact(
            11053,
            code="7802100505323",
            bar_code="7802100505323",
            product_name="CRISTAL LATA 470 CC",
            variant_name="LATA 470 CC (SEC 24)",
            average_cost=D("423.07"),
            **purchase("lc_", 151416, "437", date(2026, 7, 29), office=1),
            **purchase("last_", 151416, "437", date(2026, 7, 29), office=1),
            lines_total=10,
            lines_comparable=4,
        )
    ]
    repo = FakeRepo(rows)

    def stocks_by_office(**kw):
        return [
            {"variant_id": 11053, "office_id": oid, "quantity": D("10")}
            for oid in (1, 3, 4, 5)
        ]

    def office_compare_lines_batch(**kw):
        return [
            {
                "variant_id": 11053,
                "reception_id": 200 + oid,
                "detail_id": 2000 + oid,
                "admission_date": date(2026, 10, 10),
                "office_id": oid,
                "document": "Factura" if oid == 1 else "GUÍA DE DESPACHO ELECTRÓNICA",
                "document_number": 1,
                "internal_dispatch_id": None,
                "movement_class": "factura" if oid == 1 else "guia_despacho",
                "cost": D("437"),
                "quantity": D("24"),
                "details_complete": True,
                "comparable": True,
                "cost_modified": False,
                "quantity_modified": False,
                "variant_modified": False,
                "rn_office": 1,
            }
            for oid in (1, 3, 4, 5)
        ]

    repo.stocks_by_office = stocks_by_office  # type: ignore[method-assign]
    repo.office_compare_lines_batch = office_compare_lines_batch  # type: ignore[method-assign]

    out = _svc(repo).list_office_compare(stock_filter="all", sort="product", page_size=20)
    assert out["total"] == 1
    office_ids = [c["office_id"] for c in out["items"][0]["offices"]]
    assert office_ids == [1, 3, 4, 5]
    assert 2 not in office_ids and 6 not in office_ids
    assert out["items"][0]["offices"][0]["last_supplier_purchase"] is not None
    assert out["items"][0]["offices"][1]["info_tag"] == "solo_guia"
    assert out["items"][0]["review_hint"]["office_scope_supported"] is False


def test_office_compare_rejects_auxiliary_office_filter():
    repo = FakeRepo([fact(1, average_cost=D("10"))])
    with pytest.raises(CostControlValidationError):
        _svc(repo).list_office_compare(office_id=2)


def test_get_office_compare_product_not_found():
    repo = FakeRepo([])
    with pytest.raises(CostControlNotFound):
        _svc(repo).get_office_compare_product(999)


def test_cristal_aug_oct_fixture_timeline_labels():
    """Fixture representativa: factura bodega + guías retail ago–oct (SEC 24)."""
    rows = [
        fact(
            11053,
            code="7802100505323",
            bar_code="7802100505323",
            product_name="CRISTAL",
            variant_name="LATA 470 CC (SEC 24)",
            average_cost=D("423.07"),
            **purchase("lc_", 160001, "437", date(2026, 10, 5), office=1),
            **purchase("last_", 160001, "437", date(2026, 10, 5), office=1),
        )
    ]
    lines = [
        {
            "reception_id": 160001,
            "detail_id": 1,
            "admission_date": date(2026, 8, 12),
            "office_id": 1,
            "document": "Factura",
            "document_number": 1,
            "internal_dispatch_id": None,
            "movement_class": "factura",
            "cost": D("437"),
            "quantity": D("2400"),
            "details_complete": True,
            "comparable": True,
            "cost_modified": False,
            "quantity_modified": False,
            "variant_modified": False,
        },
        {
            "reception_id": 160050,
            "detail_id": 2,
            "admission_date": date(2026, 9, 18),
            "office_id": 3,
            "document": "GUÍA DE DESPACHO ELECTRÓNICA",
            "document_number": 27001,
            "internal_dispatch_id": None,
            "movement_class": "guia_despacho",
            "cost": D("437"),
            "quantity": D("480"),
            "details_complete": True,
            "comparable": True,
            "cost_modified": False,
            "quantity_modified": False,
            "variant_modified": False,
        },
        {
            "reception_id": 160090,
            "detail_id": 3,
            "admission_date": date(2026, 10, 5),
            "office_id": 1,
            "document": "Factura",
            "document_number": 2,
            "internal_dispatch_id": None,
            "movement_class": "factura",
            "cost": D("438"),
            "quantity": D("1200"),
            "details_complete": True,
            "comparable": True,
            "cost_modified": False,
            "quantity_modified": False,
            "variant_modified": False,
        },
    ]
    repo = FakeRepo(rows, lines=lines)
    hist = _svc(repo).office_compare_timeline(
        11053, date_from=date(2026, 8, 1), date_to=date(2026, 10, 31), page_size=50
    )
    labels = {i["movement_label"] for i in hist["items"]}
    assert "Factura" in labels and "Guía" in labels
    assert all(i["office_id"] in (1, 3, 4, 5) or i["office_id"] == 1 for i in hist["items"])
