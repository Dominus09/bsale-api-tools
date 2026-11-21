"""Seguimiento de entregas ERP — auth + helpers de tiempo (sin escritura)."""

from __future__ import annotations

from datetime import datetime, timezone

from backend.auth.permissions import require_erp_ops_staff
from backend.routers import distribuidora_seguimiento_entregas as seg_router
from backend.services.distribuidora import delivery_seguimiento_service as seg


def test_router_depends_on_require_erp_ops_staff():
    src = open(seg_router.__file__, encoding="utf-8").read()
    assert "require_erp_ops_staff" in src
    paths = [getattr(r, "path", "") for r in seg_router.router.routes]
    assert any("/dashboard" in p for p in paths), paths
    assert any("/clientes" in p for p in paths), paths
    assert any("/visitas" in p for p in paths), paths
    callables = []
    for dep in seg_router.router.dependencies:
        callables.append(dep.dependency)
    assert require_erp_ops_staff in callables


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


def test_evidence_required_reasons_documented():
    assert "closed_store" in seg.EVIDENCE_REQUIRED_REASONS
    assert "rejected_order" in seg.EVIDENCE_REQUIRED_REASONS
