"""HTTP contracts with fake GitHub and DB operations. No real network or production DB."""
import base64
import io
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from PIL import Image

from backend.routers import product_photos as r
from backend.routers import product_pallets as pallets
from backend.services import product_photo_service as p


@pytest.fixture
def app_client(monkeypatch):
    app = FastAPI()
    app.include_router(r.router)
    app.include_router(pallets.router)
    app.dependency_overrides[r.require_management_access] = lambda: {"id": 42}
    # Make any accidental real connection fail immediately.
    monkeypatch.setattr(r, "get_connection", Mock(side_effect=AssertionError("real DB forbidden")))
    monkeypatch.setattr(r.admin, "get_connection", Mock(side_effect=AssertionError("real DB forbidden")))
    monkeypatch.setattr(p, "_request", Mock(side_effect=AssertionError("real GitHub forbidden")))
    product = {"row_id": 8, "barcode": "00123", "catalog_variant_ids": [500],
               "filename": "00123.webp", "catalog_asset_url": "https://cat.quillotana.cl/products/00123.webp",
               "blocked_reason": None}
    monkeypatch.setattr(r, "_product", lambda row_id: product.copy())
    monkeypatch.setattr(r, "_writes_enabled", lambda: False)
    monkeypatch.setattr(p, "photo_sha", Mock(return_value=None))
    return app, TestClient(app), product


def png():
    out = io.BytesIO()
    Image.new("RGB", (300, 100), "red").save(out, "PNG")
    return out.getvalue()


def prepare(client):
    return client.post("/products-master/id/8/photo/prepare", data={"mode": "white"},
                       files={"image": ("example.png", png(), "image/png")})


def test_preview_without_publish_does_not_contact_github(app_client):
    _, client, _ = app_client
    response = prepare(client)
    assert response.status_code == 200
    data = response.json()
    assert data["preview_token"] is None
    assert data["writes_enabled"] is False
    p.validate_prepared(base64.b64decode(data["data_base64"]))
    p.photo_sha.assert_not_called()


def test_github_read_error_is_not_missing_photo(app_client):
    _, client, _ = app_client
    p.photo_sha.side_effect = HTTPException(503, "GitHub unavailable")
    response = client.get("/products-master/id/8/photo")
    assert response.status_code == 200
    assert response.json()["has_photo"] is None
    assert response.json()["github_checked"] is False
    assert response.json()["writes_enabled"] is False


def test_blocked_variant_can_preview_but_not_commit(app_client, monkeypatch):
    _, client, product = app_client
    product["blocked_reason"] = "shared barcode"
    monkeypatch.setattr(r, "_writes_enabled", lambda: True)
    response = prepare(client)
    assert response.status_code == 200
    data = response.json()
    assert data["publication_reason"] == "shared barcode"
    assert data["preview_token"] is None
    response = client.post("/products-master/id/8/photo/commit",
                           data={"preview_token": "invalid"},
                           files={"image": ("00123.webp", base64.b64decode(data["data_base64"]), "image/webp")})
    assert response.status_code == 409


def test_commit_disabled(app_client):
    _, client, _ = app_client
    response = client.post("/products-master/id/8/photo/commit",
                           data={"preview_token": "invalid"},
                           files={"image": ("00123.webp", b"test", "image/webp")})
    assert response.status_code == 503


def test_variant_changed_between_preview_and_commit(app_client, monkeypatch):
    _, client, product = app_client
    monkeypatch.setattr(r, "_writes_enabled", lambda: True)
    monkeypatch.setenv("CATALOG_PHOTO_PREVIEW_SECRET", "test-secret-" * 4)
    response = prepare(client)
    assert response.status_code == 200
    data = response.json()
    assert data["preview_token"]
    product["catalog_variant_ids"] = [501]
    response = client.post("/products-master/id/8/photo/commit",
                           data={"preview_token": data["preview_token"]},
                           files={"image": ("00123.webp", base64.b64decode(data["data_base64"]), "image/webp")})
    assert response.status_code == 409


@pytest.mark.parametrize("status", [401, 403])
def test_denied_management_dependency_blocks_routes(app_client, status):
    app, client, _ = app_client
    def denied():
        raise HTTPException(status)
    app.dependency_overrides[r.require_management_access] = denied
    assert client.get("/products-master/id/8/photo").status_code == status
    assert prepare(client).status_code == status
    assert client.get("/products-master/pallets?ids=8").status_code == status


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "12", 2147483648])
def test_pallet_invalid_values_are_rejected(app_client, value):
    _, client, _ = app_client
    assert client.patch("/products-master/id/8/pallet", json={"boxes_per_pallet": value}).status_code == 422


def test_pallet_without_flag_cannot_write(app_client, monkeypatch):
    _, client, _ = app_client
    monkeypatch.delenv("PRODUCT_MASTER_PALLET_WRITES_ENABLED", raising=False)
    assert client.patch("/products-master/id/8/pallet", json={"boxes_per_pallet": 50}).status_code == 503


def test_pallet_calculations_use_manual_boxes_only():
    assert r.admin._pallet_row((8, 50, 24, 12.5)) == {
        "id": 8, "boxes_per_pallet": 50, "units_per_pallet": 1200, "load_weight_kg": 625.0}
    assert r.admin._pallet_row((8, None, 24, 12.5))["units_per_pallet"] is None
