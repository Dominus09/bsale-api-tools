import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from unittest.mock import Mock

from backend.routers import products_master as pm
from backend.services import catalog_photo_inventory as inventory


def payload():
    return {"sha": "a"*40, "truncated": False, "tree": [
        {"path": "public/products", "type": "tree"},
        {"path": "public/products/00123.webp", "type": "blob", "mode": "100644", "size": 12},
        {"path": "public/products/00456.webp", "type": "blob", "mode": "100644", "size": 12},
        {"path": "public/products/other.png", "type": "blob", "mode": "100644", "size": 10},
        {"path": "public/products/linked.webp", "type": "blob", "mode": "120000", "size": 10},
        {"path": "public/products/sub/987.webp", "type": "blob", "mode": "100644", "size": 10},
    ]}


def test_exact_filenames_preserve_zeroes_and_exclude_other_paths():
    snapshot = inventory.parse_tree(payload())
    assert snapshot.barcodes == {"00123", "00456"}
    assert snapshot.status()["file_count"] == 2


@pytest.mark.parametrize("change", [{"truncated": True}, {"tree": []}, {"sha": "invalid"}, {"tree": [None]}])
def test_incomplete_inventory_not_treated_as_empty(change):
    with pytest.raises(HTTPException) as exc:
        inventory.parse_tree(payload() | change)
    assert exc.value.status_code == 503


def test_cached_reads_and_invalidation(monkeypatch):
    inventory.invalidate_inventory()
    fetch = Mock(return_value=inventory.parse_tree(payload()))
    monkeypatch.setattr(inventory, "_fetch", fetch)
    try:
        assert inventory.get_inventory() is inventory.get_inventory()
        assert fetch.call_count == 1
        inventory.invalidate_inventory()
        inventory.get_inventory()
        assert fetch.call_count == 2
    finally:
        inventory.invalidate_inventory()


def test_failed_refresh_never_returns_old_snapshot(monkeypatch):
    inventory.invalidate_inventory()
    fetch = Mock(return_value=inventory.parse_tree(payload()))
    monkeypatch.setattr(inventory, "_fetch", fetch)
    clock = [100.]
    monkeypatch.setattr(inventory.time, "monotonic", lambda: clock[0])
    try:
        inventory.get_inventory()
        clock[0] += inventory.CACHE_SECONDS + 1
        fetch.side_effect = HTTPException(503, "Unavailable")
        with pytest.raises(HTTPException):
            inventory.get_inventory()
        with pytest.raises(HTTPException):
            inventory.get_inventory()
        assert fetch.call_count == 2
    finally:
        inventory.invalidate_inventory()


def test_photo_filter_applies_before_count_and_limit(monkeypatch):
    cur = Mock()
    cur.fetchone.return_value = (2,)
    cur.fetchall.return_value = [("00123",), ("00456",)]
    cur.description = [("barcode",)]
    conn = Mock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(pm, "get_connection", lambda: conn)
    snap = inventory.parse_tree(payload())
    result = pm._list_master_page(None, False, " CERVEZAS ", "Corona", False, 25, 50,
                                  inventory=snap, photo_status="with")
    count, data = cur.execute.call_args_list
    assert "BTRIM(product_type) = %s" in count.args[0]
    assert " = ANY(%s::text[])" in count.args[0]
    assert count.args[1][-1] == ["00123", "00456"]
    assert data.args[1][-2:] == (25, 50)
    assert all(item["has_catalog_photo"] for item in result["items"])
    assert result["total"] == 2
    cur.reset_mock()
    cur.fetchone.return_value = (1,)
    cur.fetchall.return_value = [("123",)]
    result = pm._list_master_page(None, False, None, None, False, 25, 0,
                                  inventory=snap, photo_status="without")
    assert "NOT (BTRIM(COALESCE(barcode, '')) = ANY(%s::text[]))" in cur.execute.call_args_list[0].args[0]
    assert result["items"][0]["has_catalog_photo"] is False


@pytest.fixture
def client(monkeypatch):
    app = FastAPI()
    app.include_router(pm.router)
    app.dependency_overrides[pm.require_management_access] = lambda: {"id": 42}
    app.dependency_overrides[pm.require_erp_app] = lambda: {"id": 42}
    monkeypatch.setattr(pm, "_list_master_page", Mock(return_value={"items": [], "total": 0, "limit": 50, "offset": 0}))
    monkeypatch.setattr(inventory, "get_inventory", Mock(side_effect=HTTPException(503)))
    return app, TestClient(app)


def test_without_inventory_all_rows_available_but_filtered_requests_fail(client):
    _, http = client
    base = "/products-master/management-page"
    r = http.get(base)
    assert r.status_code == 200
    assert r.json()["photo_inventory"]["available"] is False
    for state in ["with", "without"]:
        assert http.get(base, params={"photo_status": state}).status_code == 503
    assert http.get(base, params={"photo_status": "invalid"}).status_code == 422


def test_legacy_route_does_not_fetch_github(client):
    _, http = client
    assert http.get("/products-master").status_code == 200
    inventory.get_inventory.assert_not_called()


def test_management_required_for_photo_inventory(client):
    app, http = client
    def denied():
        raise HTTPException(403)
    app.dependency_overrides[pm.require_management_access] = denied
    assert http.get("/products-master/management-page").status_code == 403
    inventory.get_inventory.assert_not_called()


def test_stock_filter_only_management_and_valid_values(client):
    _, http = client
    base = "/products-master/management-page"
    assert http.get(base, params={"stock_status": "invalid"}).status_code == 422
    assert http.get(base, params={"include_stock": "true", "stock_status": "with"}).status_code == 200
    kwargs = pm._list_master_page.call_args.kwargs
    assert kwargs["include_stock"] is True
    assert kwargs["stock_status"] == "with"
    assert kwargs["stock_company_ids"] is None


def test_stock_unavailable_all_falls_back_but_with_fails(client):
    _, http = client
    pm._list_master_page.side_effect = [HTTPException(403, "Acceso restringido"), {"items": [], "total": 0}]
    result = http.get("/products-master/management-page", params={"include_stock": "true"})
    assert result.status_code == 200
    assert result.json()["stock_inventory"]["available"] is False
    assert "include_stock" not in pm._list_master_page.call_args.kwargs
    pm._list_master_page.side_effect = HTTPException(403, "Acceso restringido")
    assert http.get("/products-master/management-page", params={"stock_status": "with"}).status_code == 403
