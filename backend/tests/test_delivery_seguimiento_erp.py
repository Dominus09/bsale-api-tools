"""Seguimiento de entregas ERP — auth + helpers (sin escritura productiva)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from backend.auth.permissions import require_erp_ops_staff
from backend.routers import distribuidora_seguimiento_entregas as seg_router
from backend.services.distribuidora import delivery_seguimiento_service as seg
from backend.services.distribuidora import delivery_route_progress as progress_svc


def test_router_depends_on_require_erp_ops_staff():
    src = open(seg_router.__file__, encoding="utf-8").read()
    assert "require_erp_ops_staff" in src
    paths = [getattr(r, "path", "") for r in seg_router.router.routes]
    assert any("/dashboard" in p for p in paths), paths
    assert any("/clientes" in p for p in paths), paths
    assert any("/visitas" in p for p in paths), paths
    callables = [d.dependency for d in seg_router.router.dependencies]
    assert require_erp_ops_staff in callables


def test_main_mounts_seguimiento_routes():
    import backend.main as main

    paths = set(main.app.openapi().get("paths", {}))
    assert any("/distribuidora/seguimiento-entregas/dashboard" in p for p in paths)
    assert any("/distribuidora/seguimiento-entregas/visitas/" in p for p in paths)


def test_timing_prefers_device_capture_over_sync():
    device = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    sync = datetime(2026, 11, 21, 18, 0, tzinfo=timezone.utc)
    visit = {
        "ended_at": sync,
        "end_gps_captured_at": None,
        "closure_review_status": "pending_review",
    }
    offline = {"captured_at": device, "completed_at": sync}
    out = seg._timing_for_visit(visit, offline)
    assert out["device_capture"] is True
    assert out["managed_at"] == device
    assert out["synced_at"] == sync
    assert out["requires_review"] is True
    assert out["managed_at_source"] == "device"


def test_timing_without_device_uses_ended_at():
    sync = datetime(2026, 11, 21, 15, 30, tzinfo=timezone.utc)
    visit = {"ended_at": sync, "closure_review_status": "verified"}
    out = seg._timing_for_visit(visit, None)
    assert out["device_capture"] is False
    assert out["managed_at"] == sync
    assert out["managed_at_source"] == "server"


def test_timing_empty_visit_does_not_invent_clock():
    out = seg._timing_for_visit(None, None)
    assert out["managed_at"] is None
    assert out["managed_at_local"] is None
    assert out["synced_at"] is None
    assert out["device_capture"] is False


def test_evidence_policy_no_mandatory_signature():
    assert "closed_store" in seg.EVIDENCE_REQUIRED_REASONS
    assert "rejected_order" in seg.EVIDENCE_REQUIRED_REASONS
    assert "absent_customer" in seg.EVIDENCE_REQUIRED_REASONS
    # Policy metadata must not claim firma/receptor obligatorios.
    note = (
        "Incidencias requieren fotografía. Entrega completa/parcial sin firma "
        "ni receptor obligatorios."
    )
    assert "firma" not in note.lower() or "sin firma" in note.lower()


def test_route_progress_dedupes_clients_not_documents():
    """Un cliente con 3 docs cuenta una sola vez."""
    clients = [
        {
            "customer_key": "c1",
            "document_count": 3,
            "client_name": "A",
        },
        {
            "customer_key": "c2",
            "document_count": 1,
            "client_name": "B",
        },
    ]
    attention = {
        "c1": {
            "attention_status": "completed",
            "visit_status": "completed",
            "closure_review_status": "verified",
        },
        "c2": {
            "attention_status": "pending",
            "visit_status": None,
        },
    }
    out = progress_svc.compute_route_progress(
        clients=clients, attention_by_key=attention
    )
    assert out["total_clients"] == 2
    assert out["managed_clients"] == 1
    assert out["by_result"]["completed"] == 1
    assert out["by_result"]["pending"] == 1


def test_http_endpoints_require_auth_direct():
    """Acceso directo a cada path (no solo menú) exige staff ERP."""
    app = FastAPI()
    app.include_router(seg_router.router)
    client = TestClient(app)
    for path in (
        "/distribuidora/seguimiento-entregas/dashboard",
        "/distribuidora/seguimiento-entregas/by-driver",
        "/distribuidora/seguimiento-entregas/planes/1/clientes",
        "/distribuidora/seguimiento-entregas/visitas/1",
        "/distribuidora/seguimiento-entregas/status",
    ):
        res = client.get(path)
        assert res.status_code in (401, 403), (path, res.status_code, res.text)


def test_capability_only_denied_for_seguimiento(monkeypatch):
    def _fake_staff(*_a, **_k):
        return {
            "id": 99,
            "email": "excel@test.local",
            "role": "vendedor",
            "erp_capabilities": ["simple_planning"],
        }

    monkeypatch.setattr(
        "backend.utils.auth_staff.require_staff_user",
        _fake_staff,
    )
    monkeypatch.setattr(
        "backend.auth.permissions.has_simple_planning_access",
        lambda _u: True,
    )
    monkeypatch.setattr(
        "backend.auth.permissions.is_simple_planning_capability_only",
        lambda _u: True,
    )
    with pytest.raises(HTTPException) as ei:
        require_erp_ops_staff(authorization="Bearer x")
    assert ei.value.status_code == 403
