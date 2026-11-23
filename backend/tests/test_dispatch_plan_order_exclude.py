"""Exclusión administrativa de OC — unit tests (sin OC reales / sin SQL prod)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import distribuidora_dispatch_plan as dp_router
from backend.services.distribuidora import dispatch_plan_order_exclude_service as excl
from backend.services.distribuidora.dispatch_plan_service import (
    _invoicing_payload_from_rows,
)


def test_062_sql_awaiting_approval_content():
    from pathlib import Path

    path = (
        Path(__file__).resolve().parents[1]
        / "sql"
        / "distribuidora"
        / "062_dispatch_plan_order_admin_exclude_AWAITING_APPROVAL.sql"
    )
    text = path.read_text(encoding="utf-8")
    assert "excluded_at" in text
    assert "AWAITING" in path.name.upper() or "AWAITING_APPROVAL" in path.name
    assert "NO APLICAR EN PRODUCCIÓN" in text
    assert "ck_dispatch_plan_orders_admin_exclude_integrity" in text


def test_serialize_document_total_from_confirmed_invoice():
    """Regresión: 7 facturas vigentes no deben sumar $0 si hay total_amount."""

    class _St:
        billing_status = "invoiced"
        dispatch_status = "pending"
        planning_eligible = True
        dispatch_closed = False
        fulfillment_status = "pending"
        excluded_reason = None
        completion_source = "invoice"
        billing_label_es = "Facturada"
        fulfillment_label_es = "Pendiente"
        credit_notes = []
        evidence_source = "direct_related"
        confirmed_invoice = {
            "document_id": 55,
            "number": 9001,
            "total_amount": 150000,
        }

    rows = [
        {
            "oc_document_id": 1,
            "oc_number": 71000,
            "status": "missing",
            "related_document_id": None,
        }
    ]
    out = _invoicing_payload_from_rows(
        10, rows, operational_by_oc={1: _St()}
    )
    item = out["items"][0]
    assert item["status"] == "confirmed"
    assert float(item["document_total"]) == 150000.0


def test_exclude_requires_management(monkeypatch):
    with pytest.raises(excl.OrderExcludeError) as ei:
        excl.preview_exclude_order(
            user={"id": 2, "role": "operativo", "email": "op@x"},
            plan_id=1,
            oc_document_id=71041,
        )
    assert ei.value.http_status == 403


def test_exclude_idempotent_and_reason(monkeypatch):
    class _Cur:
        description = [
            ("id",),
            ("dispatch_plan_id",),
            ("oc_document_id",),
            ("oc_number",),
            ("oc_total_amount",),
            ("excluded_at",),
            ("exclude_reason",),
            ("excluded_by_user_id",),
            ("excluded_by_user_name",),
            ("exclusion_kind",),
            ("client_name",),
            ("route_order",),
        ]

        def __init__(self):
            self._phase = 0

        def execute(self, sql, params=None):
            self._sql = sql
            self._params = params
            if "FOR UPDATE" in sql and "dispatch_plan" in sql and "orders" not in sql:
                self._phase = 1
            elif "FOR UPDATE" in sql and "dispatch_plan_orders" in sql:
                self._phase = 2
            elif "SAVEPOINT" in sql or "RELEASE" in sql or "ROLLBACK TO" in sql:
                self._phase = 9
            elif "UPDATE distribuidora.dispatch_plan" in sql:
                self._phase = 3
            elif "INSERT INTO distribuidora.dispatch_plan_order_events" in sql:
                self._phase = 4
            elif "FROM distribuidora.dispatch_plan_orders o" in sql or (
                "SELECT o.*" in (sql or "")
            ):
                self._phase = 5

        def fetchone(self):
            if self._phase == 1:
                return (1, "closed", False)
            if self._phase == 2:
                return (
                    99,
                    1,
                    71041,
                    71041,
                    223128,
                    datetime(2026, 11, 20, tzinfo=timezone.utc),
                    "pedido cancelado",
                    1,
                    "boss@x",
                    "admin",
                    "Cliente",
                    8,
                )
            if self._phase == 4:
                return (501,)
            return None

        def fetchall(self):
            if self._phase == 5:
                return []
            return []

        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(excl, "get_connection", lambda: _Conn())
    monkeypatch.setattr(
        excl.plan_repo, "order_exclude_columns_ready", lambda _c: True
    )
    monkeypatch.setattr(
        excl.plan_repo,
        "list_plan_orders",
        lambda *_a, **_k: [],
    )
    out = excl.exclude_order(
        user={"id": 1, "role": "gerencia", "email": "boss@x", "is_owner": True},
        plan_id=1,
        oc_document_id=71041,
        reason="pedido cancelado por vendedor",
    )
    assert out["ok"] is True
    assert out["idempotent"] is True


def test_http_exclude_endpoints_require_auth():
    app = FastAPI()
    app.include_router(dp_router.router)
    client = TestClient(app)
    for path in (
        "/distribuidora/dispatch-plans/1/orders/71041/exclude-preview",
        "/distribuidora/dispatch-plans/1/orders/exclusions",
    ):
        res = client.get(path)
        assert res.status_code in (401, 403), (path, res.status_code)
    res = client.post(
        "/distribuidora/dispatch-plans/1/orders/71041/exclude",
        json={"reason": "pedido cancelado prueba"},
    )
    assert res.status_code in (401, 403)
