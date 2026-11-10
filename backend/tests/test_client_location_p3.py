"""P3: propuestas GPS / fotos — unit tests sin analytics."""

from __future__ import annotations

import pytest

from backend.services.distribuidora import client_location_service as svc


def test_haversine_significant_distance():
    # ~111 m por 0.001° lat
    d = svc._haversine_m(-41.47, -72.94, -41.471, -72.94)
    assert 100 < d < 130


def test_parse_client_id_from_key_and_payload():
    assert svc._parse_client_id("id:99", {"client_id": None}) == 99
    assert svc._parse_client_id("id:1", {"client_id": 42}) == 42
    with pytest.raises(Exception) as ei:
        svc._parse_client_id("name:foo", {"client_id": None})
    assert ei.value.status_code == 422


def test_object_key_safe_and_organized():
    from backend.services import object_storage_service as storage

    key = storage.build_facade_object_key(company_id=3, client_id=12345, ext="jpg")
    assert key.startswith("operaciones/clientes/3/12345/fachadas/")
    assert key.endswith(".jpg")
    assert ".." not in key


def test_r2_status_disabled_without_env(monkeypatch):
    from backend.services import object_storage_service as storage

    for k in (
        "R2_ENDPOINT",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_BUCKET",
    ):
        monkeypatch.delenv(k, raising=False)
    assert storage.is_r2_configured() is False
    st = storage.status_payload()
    assert st["configured"] is False


def test_propose_requires_confirm_on_low_accuracy(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda u, p: 1,
    )
    monkeypatch.setattr(
        svc,
        "_resolve_client",
        lambda plan_id, key: {
            "client_id": 10,
            "customer_key": "id:10",
            "latitude": -41.5,
            "longitude": -72.9,
            "coordinates_source": "bsale.clients.lat/lon",
            "has_coordinates": True,
        },
    )
    out = svc.propose_location(
        user={"id": 7, "email": "chofer@x.cl"},
        plan_id=1,
        customer_key="id:10",
        latitude=-41.5005,
        longitude=-72.9,
        accuracy_m=120,
        confirm_low_accuracy=False,
        confirm_significant_move=True,
    )
    assert out["requires_confirmation"] is True
    assert out["reason"] == "low_accuracy"


def test_propose_blocks_name_key(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda u, p: 1,
    )
    monkeypatch.setattr(
        svc,
        "_resolve_client",
        lambda plan_id, key: {
            "client_id": None,
            "customer_key": "name:sin id",
            "has_coordinates": False,
        },
    )
    with pytest.raises(Exception) as ei:
        svc.propose_location(
            user={"id": 7},
            plan_id=1,
            customer_key="name:sin id",
            latitude=-41.5,
            longitude=-72.9,
            accuracy_m=10,
            confirm_low_accuracy=True,
            confirm_significant_move=True,
        )
    assert ei.value.status_code == 422


def test_normalize_rejects_non_image():
    with pytest.raises(Exception) as ei:
        svc._normalize_facade_image(b"%PDF-1.4 fake", "image/jpeg")
    assert ei.value.status_code == 400


def test_facade_prefix_uses_company_id():
    assert svc._facade_object_prefix(company_id=7, client_id=1001) == (
        "operaciones/clientes/7/1001/fachadas/"
    )
    assert "clientes/3/" not in svc._facade_object_prefix(company_id=12, client_id=1)


def test_authorized_company_id_rejects_disallowed(monkeypatch):
    class _Scope:
        load_failed = False
        companies_unverified = False
        company_ids = (3,)

    class _Cur:
        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def close(self):
            return None

        def rollback(self):
            return None

    monkeypatch.setattr(svc, "get_scope", lambda u: _Scope())
    monkeypatch.setattr(svc, "company_allowed", lambda scope, cid: False)
    monkeypatch.setattr(
        svc.driver_svc,
        "resolve_plan_company_office",
        lambda cur, plan_id: (9, 1),
    )
    monkeypatch.setattr(svc, "get_connection", lambda: _Conn())
    with pytest.raises(Exception) as ei:
        svc._authorized_company_id_for_plan({"id": 1}, 1)
    assert ei.value.status_code == 403


def test_object_key_uses_dynamic_company():
    from backend.services import object_storage_service as storage

    key = storage.build_facade_object_key(company_id=7, client_id=55, ext="jpg")
    assert key.startswith("operaciones/clientes/7/55/fachadas/")
    assert "/clientes/3/" not in key
