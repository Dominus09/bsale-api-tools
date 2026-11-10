"""Detalle ERP de visitas: aislamiento por empresa (sin DB)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc


class _Scope:
    def __init__(self, company_ids):
        self.company_ids = company_ids
        self.load_failed = False
        self.companies_unverified = False


def test_erp_detail_hides_other_company(monkeypatch):
    monkeypatch.setattr(
        "backend.auth.multiapp.get_scope",
        lambda _user: _Scope([3]),
    )
    monkeypatch.setattr(svc, "get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        svc.visit_repo,
        "tables_ready",
        lambda *_a, **_k: True,
    )
    monkeypatch.setattr(
        svc.visit_repo,
        "get_visit",
        lambda *_a, **_k: {
            "id": 99,
            "company_id": 9,
            "dispatch_plan_id": 1,
            "customer_key": "id:1",
            "driver_user_id": 1,
            "status": "completed",
            "client_id": 1,
            "version": 1,
        },
    )
    with pytest.raises(HTTPException) as ei:
        svc.get_visit_detail_for_erp(user={"id": 1}, visit_id=99)
    assert ei.value.status_code == 404


class _FakeConn:
    def cursor(self):
        return _FakeCur()

    def close(self):
        return None


class _FakeCur:
    def close(self):
        return None
