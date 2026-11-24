"""Ampliación de cobertura Catálogo V2: flag, contrato, UPB, rollback."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.services.catalog_v2 import projection as proj
from backend.services.catalog_v2 import source_flag as sf


def test_flag_default_legacy(monkeypatch):
    monkeypatch.delenv("CATALOG_SOURCE", raising=False)
    monkeypatch.delenv("CATALOG_SOURCE_PUBLIC", raising=False)
    assert sf.resolve_catalog_source(consumer="public_web") == "legacy"


def test_flag_public_isolated_from_global(monkeypatch):
    monkeypatch.setenv("CATALOG_SOURCE", "v2")
    monkeypatch.delenv("CATALOG_SOURCE_PUBLIC", raising=False)
    # Global v2 NO activa público sin CATALOG_SOURCE_PUBLIC
    assert sf.resolve_catalog_source(consumer="public_web") == "legacy"
    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "v2")
    assert sf.resolve_catalog_source(consumer="public_web") == "v2"
    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "legacy")
    assert sf.resolve_catalog_source(consumer="public_web") == "legacy"


def test_flag_hard_deny_picking_and_order_pricing(monkeypatch):
    monkeypatch.setenv("CATALOG_SOURCE", "v2")
    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "v2")
    assert sf.resolve_catalog_source(consumer="picking") == "legacy"
    assert sf.resolve_catalog_source(consumer="order_pricing") == "legacy"
    assert sf.resolve_catalog_source(consumer="dispatch") == "legacy"
    assert sf.resolve_catalog_source(consumer="purchase_orders") == "legacy"


def test_flag_erp_preview_defaults_v2(monkeypatch):
    monkeypatch.delenv("CATALOG_SOURCE_ERP_PREVIEW", raising=False)
    assert sf.resolve_catalog_source(consumer="erp_preview") == "v2"


def test_public_stock_uses_quantity_available_not_quantity():
    row = {
        "variant_id": 28216,
        "product_name": "HARINA MARIPOSA",
        "variant_name": "25 KG",
        "product_type": "Harinas",
        "barcode": "7802626001163",
        "price_gross": 20990,
        "price_net": 16022.9,
        "stock_office_quantity": 503,
        "stock_office_available": 446,
        "stock_office_reserved": 57,
        "stock_all_offices": 500,
        "image_raw": None,
        "units_per_box_source": 1,
        "variant_description": "25 KG (SEC 1)",
        "pm_units_per_box": 1,
        "pm_sale_type": "ENTERA",
        "pm_quantity_step": 1,
        "pm_id": 1,
        "peso_caja_kg": 25,
        "variant_state": 0,
        "sku": "H",
        "price_fetched_at": None,
        "stock_office_fetched_at": datetime(2026, 11, 24, tzinfo=timezone.utc),
        "variant_fetched_at": None,
        "logistics_completed": True,
        "stock_office_payload": {
            "quantity": 503.0,
            "quantityAvailable": 446.0,
            "quantityReserved": 57.0,
        },
    }
    item = proj._row_to_public_item(row, price_list_key="factura")
    assert item["stock"] == 446
    assert item["v2"]["stock_quantity"] == 503.0
    assert item["v2"]["stock_reserved"] == 57.0
    assert item["v2"]["upb_status"] == "verified"
    assert "cost" not in item


def test_upb_inherited_sec_not_from_x18_name():
    row = {
        "variant_id": 31199,
        "product_name": "SOL",
        "variant_name": "PACK X 18 UND",
        "product_type": "Cervezas",
        "barcode": "7802100003720",
        "price_gross": 12800,
        "price_net": 9000,
        "stock_office_quantity": 243,
        "stock_office_available": 243,
        "stock_office_reserved": 0,
        "stock_all_offices": 243,
        "image_raw": None,
        "units_per_box_source": None,
        "variant_description": "PACK DESECHABLE 300 CC X 18 UND (SEC 1)",
        "pm_units_per_box": None,
        "pm_sale_type": None,
        "pm_quantity_step": None,
        "pm_id": None,
        "peso_caja_kg": None,
        "variant_state": 0,
        "sku": None,
        "price_fetched_at": None,
        "stock_office_fetched_at": None,
        "variant_fetched_at": None,
        "logistics_completed": None,
        "stock_office_payload": {},
    }
    item = proj._row_to_public_item(row, price_list_key="factura")
    # SEC 1 → UPB 1 vía reglas vigentes; NO 18 desde el nombre.
    assert item["units_per_box"] == 1
    assert item["v2"]["upb_status"] == "inherited_sec"
    assert item["v2"]["upb_source"] == "variant_description_SEC"


def test_upb_missing_when_no_pm_no_sec():
    row = {
        "variant_id": 9,
        "product_name": "SIN DATOS",
        "variant_name": "",
        "product_type": "X",
        "barcode": "",
        "price_gross": None,
        "price_net": None,
        "stock_office_quantity": 0,
        "stock_office_available": 0,
        "stock_office_reserved": 0,
        "stock_all_offices": 0,
        "image_raw": None,
        "units_per_box_source": None,
        "variant_description": "SIN SEC",
        "pm_units_per_box": None,
        "pm_sale_type": None,
        "pm_quantity_step": None,
        "pm_id": None,
        "peso_caja_kg": None,
        "variant_state": 0,
        "sku": None,
        "price_fetched_at": None,
        "stock_office_fetched_at": None,
        "variant_fetched_at": None,
        "logistics_completed": None,
        "stock_office_payload": {},
    }
    item = proj._row_to_public_item(row, price_list_key="factura")
    assert item["price"] == 0.0
    assert item["stock"] == 0
    assert item["image"] is None
    assert item["v2"]["missing_upb"] is True
    assert item["v2"]["upb_status"] == "missing"


def test_get_catalog_stays_legacy_by_default(monkeypatch):
    from backend.routers import catalog as cat

    monkeypatch.delenv("CATALOG_SOURCE", raising=False)
    monkeypatch.delenv("CATALOG_SOURCE_PUBLIC", raising=False)

    called = {"legacy": False}

    class _Cur:
        description = []

        def execute(self, *_a, **_k):
            called["legacy"] = True
            return None

        def fetchall(self):
            return []

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(cat, "get_connection", lambda: _Conn())
    out = cat.get_catalog(price_list="factura", in_stock=None)
    assert out == []
    assert called["legacy"] is True


def test_get_catalog_v2_flag_and_rollback_on_error(monkeypatch):
    from backend.routers import catalog as cat

    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "v2")

    def _boom(*_a, **_k):
        raise RuntimeError("RAW down")

    monkeypatch.setattr(
        "backend.services.catalog_v2.projection.public_items_from_v2",
        _boom,
    )

    class _Cur:
        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return []

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(cat, "get_connection", lambda: _Conn())
    # Fallback a legacy ante falla V2
    assert cat.get_catalog(price_list="factura", in_stock=None) == []


def test_get_catalog_v2_returns_public_contract(monkeypatch):
    from backend.routers import catalog as cat

    monkeypatch.setenv("CATALOG_SOURCE_PUBLIC", "v2")
    monkeypatch.setattr(
        "backend.services.catalog_v2.projection.public_items_from_v2",
        lambda *_a, **_k: [
            {
                "id": 1,
                "name": "A",
                "type": "T",
                "barcode": "1",
                "price": 10.0,
                "stock": 0,
                "image": None,
                "units_per_box": None,
                "sale_type": "UNITARIO",
                "quantity_step": 1,
            }
        ],
    )
    out = cat.get_catalog(price_list="factura", in_stock=None)
    assert out[0]["id"] == 1
    assert "v2" not in out[0]
    assert "cost" not in out[0]


def test_preview_endpoints_require_management():
    from backend.routers import catalog_v2_preview as r

    app = FastAPI()
    app.include_router(r.router, prefix="/api")

    def _deny():
        raise HTTPException(status_code=403, detail="forbidden")

    app.dependency_overrides[r.require_management_access] = _deny
    client = TestClient(app)
    for path in (
        "/api/catalog-v2/preview?price_list=factura&limit=1",
        "/api/catalog-v2/compare",
        "/api/catalog-v2/integrity/raw-sync",
        "/api/catalog-v2/coverage/upb",
        "/api/catalog-v2/coverage/upb-missing",
        "/api/catalog-v2/source-flag",
    ):
        assert client.get(path).status_code == 403


def test_melinka_and_factura_aliases():
    assert proj._resolve_price_list_id("melinka") == 16
    assert proj._resolve_price_list_id("factura") == 13
