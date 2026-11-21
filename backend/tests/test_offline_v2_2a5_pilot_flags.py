"""Offline V2 — kill switch + autorización chofer (sin allowlists)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_offline_sync_service as offline_svc


def test_writes_default_off(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_WRITES_ENABLED", raising=False)
    assert offline_svc.offline_sync_writes_enabled() is False


def test_authorization_requires_writes_and_authenticated_user(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "true")
    auth = offline_svc.get_offline_v2_authorization({"id": 42}, plan_id=920)
    assert auth["writes_enabled"] is True
    assert auth["user_allowed"] is True
    assert auth["offline_v2_enabled"] is True
    assert auth["can_use_offline_v2"] is True
    assert auth["authorized_plan_ids"] == []
    assert auth["scope"] == "all_assigned_driver_plans"

    no_user = offline_svc.get_offline_v2_authorization({}, plan_id=920)
    assert no_user["offline_v2_enabled"] is False
    assert no_user["can_use_offline_v2"] is False


def test_kill_switch_disables_for_all_drivers(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "false")
    auth = offline_svc.get_offline_v2_authorization({"id": 42}, plan_id=920)
    assert auth["writes_enabled"] is False
    assert auth["offline_v2_enabled"] is False
    assert auth["can_use_offline_v2"] is False


def test_plan_access_flag_respected(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "true")
    denied = offline_svc.get_offline_v2_authorization(
        {"id": 42}, plan_id=920, plan_access_ok=False
    )
    assert denied["plan_allowed"] is False
    assert denied["can_use_offline_v2"] is False
    ok = offline_svc.get_offline_v2_authorization(
        {"id": 42}, plan_id=920, plan_access_ok=True
    )
    assert ok["plan_allowed"] is True


def test_sync_http_503_when_writes_off(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_WRITES_ENABLED", raising=False)
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user={"id": 42},
            plan_id=920,
            body={"idempotency_key": "x", "op_type": "complete_delivery"},
            require_writes_flag=True,
        )
    assert ei.value.status_code == 503
    assert ei.value.detail["code"] == offline_svc.ERR_SYNC_WRITES_DISABLED


def test_no_pilot_allowlist_helpers():
    assert not hasattr(offline_svc, "offline_pilot_user_allowed")
    assert not hasattr(offline_svc, "offline_pilot_plan_allowed")


def test_stable_r2_key_idempotent():
    a = offline_svc.stable_evidence_object_key(
        company_id=3,
        plan_id=920,
        idempotency_key="abc-1",
        local_evidence_id="closed_store",
    )
    b = offline_svc.stable_evidence_object_key(
        company_id=3,
        plan_id=920,
        idempotency_key="abc-1",
        local_evidence_id="closed_store",
    )
    assert a == b
    assert "offline/abc-1/" in a
