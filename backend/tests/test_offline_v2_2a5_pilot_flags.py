"""Piloto Offline V2 — allowlist usuario + despacho + kill switch (sin PG)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_offline_sync_service as offline_svc


def test_writes_default_off(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_WRITES_ENABLED", raising=False)
    assert offline_svc.offline_sync_writes_enabled() is False


def test_pilot_allowlist_empty_denies(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_PILOT_ALLOWLIST", raising=False)
    assert offline_svc.offline_pilot_user_allowed(42) is False
    assert offline_svc.offline_pilot_user_allowed(None) is False


def test_pilot_allowlist_restricts(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_ALLOWLIST", "42,77")
    assert offline_svc.offline_pilot_user_allowed(42) is True
    assert offline_svc.offline_pilot_user_allowed(77) is True
    assert offline_svc.offline_pilot_user_allowed(99) is False
    assert offline_svc.offline_pilot_user_allowed(None) is False


def test_pilot_plan_allowlist_empty_denies(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST", raising=False)
    assert offline_svc.offline_pilot_plan_allowed(920) is False
    assert offline_svc.offline_pilot_plan_allowed(None) is False


def test_pilot_plan_allowlist_restricts(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST", "920,921")
    assert offline_svc.offline_pilot_plan_allowed(920) is True
    assert offline_svc.offline_pilot_plan_allowed(999) is False


def test_pilot_authorization_requires_writes_user_and_plans(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "true")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_ALLOWLIST", "42")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST", "920")
    auth = offline_svc.get_offline_v2_pilot_authorization({"id": 42}, plan_id=920)
    assert auth["offline_v2_enabled"] is True
    assert auth["can_use_offline_v2"] is True
    assert auth["authorized_plan_ids"] == [920]

    denied_user = offline_svc.get_offline_v2_pilot_authorization({"id": 99}, plan_id=920)
    assert denied_user["offline_v2_enabled"] is False
    assert denied_user["can_use_offline_v2"] is False

    denied_plan = offline_svc.get_offline_v2_pilot_authorization({"id": 42}, plan_id=111)
    assert denied_plan["plan_allowed"] is False
    assert denied_plan["can_use_offline_v2"] is False


def test_kill_switch_disables_even_with_allowlists(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "false")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_ALLOWLIST", "42")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST", "920")
    auth = offline_svc.get_offline_v2_pilot_authorization({"id": 42}, plan_id=920)
    assert auth["writes_enabled"] is False
    assert auth["offline_v2_enabled"] is False
    assert auth["authorized_plan_ids"] == []


def test_sync_http_403_outside_user_allowlist(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "true")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_ALLOWLIST", "42")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST", "920")
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user={"id": 99},
            plan_id=920,
            body={"idempotency_key": "x", "op_type": "complete_delivery"},
            require_writes_flag=True,
        )
    assert ei.value.status_code == 403
    assert ei.value.detail["code"] == "PILOT_USER_NOT_ALLOWED"


def test_sync_http_403_outside_plan_allowlist(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_WRITES_ENABLED", "true")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_ALLOWLIST", "42")
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_PLAN_ALLOWLIST", "920")
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user={"id": 42},
            plan_id=111,
            body={"idempotency_key": "x", "op_type": "complete_delivery"},
            require_writes_flag=True,
        )
    assert ei.value.status_code == 403
    assert ei.value.detail["code"] == "PILOT_PLAN_NOT_ALLOWED"


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
