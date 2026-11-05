"""Comportamiento actual de Agregar OC al plan (mocks; sin analytics)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import dispatch_plan_load_batch_service as svc


def _conn_cur(fetchone_seq):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = list(fetchone_seq)
    return conn, cur


def test_preview_blocks_dispatched(monkeypatch):
    conn, cur = _conn_cur([{"id": 1, "status": "dispatched"}])
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 1, "status": "dispatched"})
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    out = svc.preview_add_order(1)
    assert out["can_add"] is False
    assert "despachado" in (out.get("blocked_reason") or "").lower()


def test_preview_warns_when_picking_exists(monkeypatch):
    monkeypatch.setattr(svc, "get_connection", lambda: MagicMock(cursor=lambda: MagicMock()))
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 1, "status": "picking_generated"}
    )
    monkeypatch.setattr(svc.batch_repo, "plan_has_current_picking", lambda *_a, **_k: True)
    out = svc.preview_add_order(1)
    assert out["can_add"] is True
    assert out["has_picking"] is True
    assert out["warning"]


def test_add_requires_regenerate_flag_when_picking(monkeypatch):
    monkeypatch.setattr(
        svc,
        "preview_add_order",
        lambda _pid: {
            "can_add": True,
            "has_picking": True,
            "warning": "x",
            "blocked_reason": None,
        },
    )
    out = svc.add_order_to_plan(10, oc_document_id=999, regenerate_picking=False)
    assert out["added"] is False
    assert out["requires_regenerate"] is True


def test_add_rejects_duplicate_oc(monkeypatch):
    monkeypatch.setattr(
        svc,
        "preview_add_order",
        lambda _pid: {"can_add": True, "has_picking": False, "blocked_reason": None},
    )
    conn, cur = _conn_cur([{"id": 10, "status": "planned"}, (1,)])
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 10, "status": "planned"}
    )
    with pytest.raises(ValueError, match="ya pertenece"):
        svc.add_order_to_plan(10, oc_document_id=55, regenerate_picking=False)


def test_add_inserts_order_without_new_batch_or_org(monkeypatch):
    """Flujo actual: inserta OC + evento; no crea grupo suplementario."""
    monkeypatch.setattr(
        svc,
        "preview_add_order",
        lambda _pid: {"can_add": True, "has_picking": False, "blocked_reason": None},
    )
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [None]  # no duplicate
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 10, "status": "planned"}
    )
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(svc.batch_repo, "max_route_order", lambda *_a, **_k: 2)
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda *_a, **_k: [
            {
                "oc_document_id": 77,
                "oc_number": 1001,
                "route_order": 3,
                "client_name": "X",
            }
        ],
    )
    inserts = []
    monkeypatch.setattr(
        svc.plan_repo,
        "insert_plan_orders",
        lambda *_a, **_k: inserts.append("orders"),
    )
    events = []
    monkeypatch.setattr(
        svc.batch_repo,
        "insert_order_event",
        lambda *_a, **kwargs: events.append(kwargs) or 1,
    )
    gen_calls = []
    monkeypatch.setattr(
        svc.plan_svc,
        "generate_plan_picking",
        lambda *_a, **_k: gen_calls.append(1) or {"version": 2},
    )
    monkeypatch.setattr(svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": 10})

    out = svc.add_order_to_plan(10, oc_document_id=77, regenerate_picking=False, reason="test")
    assert out["added"] is True
    assert inserts == ["orders"]
    assert events and events[0].get("action") == "add_order"
    assert gen_calls == []  # sin picking previo no regenera


def test_add_with_regenerate_calls_full_picking(monkeypatch):
    monkeypatch.setattr(
        svc,
        "preview_add_order",
        lambda _pid: {"can_add": True, "has_picking": True, "blocked_reason": None},
    )
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.fetchone.side_effect = [None]
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 10, "status": "picking_generated"}
    )
    monkeypatch.setattr(svc, "resolve_operational_statuses_batch", lambda *_a, **_k: {})
    monkeypatch.setattr(svc.batch_repo, "max_route_order", lambda *_a, **_k: 1)
    monkeypatch.setattr(
        svc.plan_svc,
        "_enrich_orders_snapshot",
        lambda *_a, **_k: [{"oc_document_id": 88, "oc_number": 2, "route_order": 2}],
    )
    monkeypatch.setattr(svc.plan_repo, "insert_plan_orders", lambda *_a, **_k: None)
    monkeypatch.setattr(svc.batch_repo, "insert_order_event", lambda *_a, **_k: 1)
    gen = []
    monkeypatch.setattr(
        svc.plan_svc,
        "generate_plan_picking",
        lambda plan_id, **kwargs: gen.append({"plan_id": plan_id, **kwargs})
        or {"version": 3, "picking_id": 99},
    )
    monkeypatch.setattr(svc.plan_svc, "get_dispatch_plan", lambda *_a, **_k: {"id": 10})

    out = svc.add_order_to_plan(
        10, oc_document_id=88, regenerate_picking=True, reason="OC extra", user={"email": "a@x.cl"}
    )
    assert out["added"] is True
    assert len(gen) == 1
    assert gen[0]["plan_id"] == 10
    assert gen[0]["regeneration_action"] == "add_order"
    # No hay creación de batch suplementario en este servicio.
    assert "load_batch" not in out
