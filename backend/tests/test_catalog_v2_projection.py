"""Catálogo V2 preview — unit tests (sin escritura / sin Coolify)."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from backend.services.catalog_v2 import projection as proj


def test_resolve_price_list_aliases():
    assert proj._resolve_price_list_id("factura") == 13
    assert proj._resolve_price_list_id("melinka") == 16
    assert proj._resolve_price_list_id("13") == 13
    with pytest.raises(proj.CatalogV2ProjectionError):
        proj._resolve_price_list_id("comoditi")
    with pytest.raises(proj.CatalogV2ProjectionError):
        proj._resolve_price_list_id("")


def test_row_to_public_item_contract_no_cost():
    row = {
        "variant_id": 101,
        "product_name": "Harina",
        "variant_name": "25 kg",
        "product_type": "Harinas",
        "barcode": "780123",
        "price_gross": 15990,
        "price_net": 13437,
        "stock_office_quantity": 12,
        "stock_office_available": 10,
        "stock_office_reserved": 2,
        "stock_all_offices": 40,
        "stock_office_payload": {"quantity": 12, "quantityAvailable": 10, "quantityReserved": 2},
        "image_raw": None,
        "units_per_box_source": 1,
        "variant_description": "Harina 25 kg SEC 1",
        "pm_units_per_box": 1,
        "pm_sale_type": "ENTERA",
        "pm_quantity_step": 1,
        "pm_id": 9,
        "peso_caja_kg": 25,
        "variant_state": 0,
        "sku": "H25",
        "price_fetched_at": datetime(2026, 11, 24, tzinfo=timezone.utc),
        "stock_office_fetched_at": datetime(2026, 11, 24, tzinfo=timezone.utc),
        "variant_fetched_at": datetime(2026, 11, 24, tzinfo=timezone.utc),
        "logistics_completed": True,
    }
    item = proj._row_to_public_item(row, price_list_key="factura")
    assert item["id"] == 101
    assert "Harina" in item["name"]
    assert item["barcode"] == "780123"
    assert item["price"] == 15990.0
    assert item["stock"] == 10  # quantity_available (paridad catalog_view)
    assert item["units_per_box"] == 1
    assert "cost" not in item
    assert "margin" not in item
    assert item["v2"]["missing_upb"] is False
    assert item["v2"]["upb_status"] == "verified"
    assert item["v2"]["price_list_id"] == 13
    assert item["v2"]["stock_quantity"] == 12.0



def test_compare_flags_price_diff(monkeypatch):
    v2_payload = {
        "items": [
            {
                "id": 1,
                "name": "A",
                "barcode": "111",
                "price": 100.0,
                "stock": 5,
                "units_per_box": 6,
                "v2": {"missing_upb": False},
            }
        ]
    }
    monkeypatch.setattr(proj, "list_catalog_v2", lambda **_k: v2_payload)

    class _Cur:
        description = [
            ("id",),
            ("name",),
            ("type",),
            ("barcode",),
            ("stock",),
            ("price",),
            ("units_per_box",),
        ]

        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return [(1, "A", "T", "111", 5, 120.0, 6)]

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(proj, "get_connection", lambda: _Conn())
    out = proj.compare_catalog_sample(price_list="factura", variant_ids=[1])
    assert out["compared"] == 1
    assert out["diff_count"] == 1
    assert "precio_difiere" in out["rows"][0]["causes"]


def test_raw_integrity_flags_residual_error(monkeypatch):
    now = datetime(2026, 11, 24, 12, 0, tzinfo=timezone.utc)
    earlier = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)

    class _Cur:
        description = [
            ("resource",),
            ("scope",),
            ("status",),
            ("last_success_at",),
            ("last_attempt_at",),
            ("last_error_at",),
            ("rows_received",),
            ("last_error",),
        ]
        _n = 0

        def execute(self, *_a, **_k):
            return None

        def fetchall(self):
            return [
                (
                    "stocks",
                    "office:1",
                    "SUCCESS",
                    now,
                    now,
                    earlier,
                    100,
                    "ConnectTimeout",
                ),
                (
                    "variant_costs",
                    "scanner",
                    "RUNNING",
                    now,
                    now,
                    now,
                    1000,
                    "HTTP 401",
                ),
            ]

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

    monkeypatch.setattr(proj, "get_connection", lambda: _Conn())
    rep = proj.raw_sync_integrity_report()
    assert rep["flagged_partial_or_noisy"] == 2
    assert rep["residual_stale_count"] == 1
    assert "residual_error_stale_after_success" in rep["flagged"][0]["issues"]
    assert "status_running" in rep["flagged"][1]["issues"]


def test_preview_router_requires_management():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.routers import catalog_v2_preview as r

    app = FastAPI()
    app.include_router(r.router, prefix="/api")

    def _deny():
        from fastapi import HTTPException

        raise HTTPException(status_code=403, detail="forbidden")

    app.dependency_overrides[r.require_management_access] = _deny
    client = TestClient(app)
    res = client.get("/api/catalog-v2/preview?price_list=factura&limit=1")
    assert res.status_code == 403
