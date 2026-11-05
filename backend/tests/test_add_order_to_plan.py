"""Compat de Agregar OC → F3-D (mocks; sin analytics)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import dispatch_plan_load_batch_service as svc


def test_preview_blocks_dispatched(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 1, "status": "dispatched"}
    )
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    out = svc.preview_add_order(1)
    assert out["can_add"] is False
    assert "despachado" in (out.get("blocked_reason") or "").lower()


def test_preview_warns_when_picking_exists(monkeypatch):
    monkeypatch.setattr(svc, "get_connection", lambda: MagicMock(cursor=lambda: MagicMock()))
    monkeypatch.setattr(
        svc.plan_repo,
        "get_plan_by_id",
        lambda *_a, **_k: {"id": 1, "status": "picking_generated"},
    )
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    out = svc.preview_add_order(1)
    assert out["can_add"] is True
    assert out["has_picking"] is True
    assert out["warning"]


def test_add_requires_regenerate_flag_when_picking_legacy(monkeypatch):
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_supplemental_order_service.preview_supplemental_orders",
        lambda *_a, **_k: {
            "can_add": True,
            "has_picking": True,
            "uses_supplemental_group": False,
            "warning": "x",
            "blocked_reason": None,
        },
    )
    out = svc.add_order_to_plan(10, oc_document_id=999, regenerate_picking=False)
    assert out["added"] is False
    assert out["requires_regenerate"] is True


def test_add_delegates_to_supplemental(monkeypatch):
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_supplemental_order_service.preview_supplemental_orders",
        lambda *_a, **_k: {
            "can_add": True,
            "has_picking": False,
            "uses_supplemental_group": True,
            "already_in_plan": [],
        },
    )
    called = {}

    def _add(*_a, **kwargs):
        called.update(kwargs)
        return {
            "added": True,
            "oc_document_ids": [77],
            "supplemental_group": {"name": "OC adicionales 01"},
        }

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_supplemental_order_service.add_supplemental_orders_to_plan",
        _add,
    )
    out = svc.add_order_to_plan(
        10,
        oc_document_id=77,
        regenerate_picking=False,
        reason="test",
        acknowledge_incomplete_weight=True,
    )
    assert out["added"] is True
    assert out["oc_document_id"] == 77
    assert called["oc_document_ids"] == [77]
    assert called["acknowledge_incomplete_weight"] is True


def test_add_propagates_supplemental_error(monkeypatch):
    from backend.services.distribuidora.dispatch_plan_supplemental_order_service import (
        SupplementalOrderError,
    )

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_supplemental_order_service.preview_supplemental_orders",
        lambda *_a, **_k: {
            "can_add": True,
            "has_picking": False,
            "uses_supplemental_group": True,
        },
    )

    def _boom(*_a, **_k):
        raise SupplementalOrderError(
            "Sobrecarga conocida",
            code="truck_overloaded",
            http_status=409,
        )

    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_supplemental_order_service.add_supplemental_orders_to_plan",
        _boom,
    )
    with pytest.raises(SupplementalOrderError) as ei:
        svc.add_order_to_plan(1, oc_document_id=1, reason="x")
    assert ei.value.code == "truck_overloaded"
    assert ei.value.http_status == 409
