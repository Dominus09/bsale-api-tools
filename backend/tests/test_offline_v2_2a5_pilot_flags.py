"""2A.5 — allowlist piloto + flags (sin PG)."""

from __future__ import annotations

from backend.services.distribuidora import delivery_offline_sync_service as offline_svc


def test_writes_default_off(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_WRITES_ENABLED", raising=False)
    assert offline_svc.offline_sync_writes_enabled() is False


def test_pilot_allowlist_empty_allows(monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_PILOT_ALLOWLIST", raising=False)
    assert offline_svc.offline_pilot_user_allowed(42) is True
    assert offline_svc.offline_pilot_user_allowed(None) is True


def test_pilot_allowlist_restricts(monkeypatch):
    monkeypatch.setenv("OFFLINE_SYNC_PILOT_ALLOWLIST", "42,77")
    assert offline_svc.offline_pilot_user_allowed(42) is True
    assert offline_svc.offline_pilot_user_allowed(77) is True
    assert offline_svc.offline_pilot_user_allowed(99) is False
    assert offline_svc.offline_pilot_user_allowed(None) is False


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
