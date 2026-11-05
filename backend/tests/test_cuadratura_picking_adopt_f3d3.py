"""F3-D3: comparación documental y adopción controlada (sin analytics)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from backend.utils.dispatch_plan_cuadratura_picking_adopt import (
    compare_document_scopes,
    doc_has_financial_work,
    document_key,
    merge_adopted_documents,
)
from backend.services.distribuidora import dispatch_plan_cuadratura_service as svc


def test_document_key_prefers_related_id():
    assert document_key({"related_document_id": 10, "document_number": 99}) == "id:10"
    assert document_key({"document_number": 99}) == "num:99"


def test_compare_added_removed_modified():
    adopted = [
        {
            "related_document_id": 1,
            "document_number": 100,
            "monto_clp": 1000,
            "medio_pago": "efectivo",
            "observacion": "ok",
        },
        {
            "related_document_id": 2,
            "document_number": 200,
            "monto_clp": 2000,
            "medio_pago": "pendiente",
        },
    ]
    proposed = [
        {
            "related_document_id": 1,
            "document_number": 100,
            "monto_clp": 1500,
            "medio_pago": "efectivo",
        },
        {
            "related_document_id": 3,
            "document_number": 300,
            "monto_clp": 3000,
            "medio_pago": "pendiente",
        },
    ]
    diff = compare_document_scopes(adopted, proposed)
    assert diff["added_count"] == 1
    assert diff["removed_count"] == 1
    assert diff["modified_count"] == 1
    assert diff["venta_delta_clp"] == 1500  # 4500 - 3000
    # Doc 2 retirado estaba en pendiente sin obs → no bloquea drop automático.
    assert diff["blocks_auto_drop"] is False
    assert doc_has_financial_work(adopted[0]) is True


def test_merge_blocks_removed_with_payments():
    adopted = [
        {
            "related_document_id": 1,
            "monto_clp": 1000,
            "medio_pago": "transferencia",
            "observacion": "",
        }
    ]
    proposed = []
    with pytest.raises(ValueError) as ei:
        merge_adopted_documents(adopted_docs=adopted, proposed_docs=proposed)
    assert "pagos u observaciones" in str(ei.value).lower()


def test_get_does_not_auto_adopt(monkeypatch):
    """GET con drift: totales oficiales = adoptado; propuesta en picking_update."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo,
        "get_plan_by_id",
        lambda *_a, **_k: {"id": 7, "office_id": 1},
    )
    cur.fetchone.return_value = [5000]
    monkeypatch.setattr(
        svc.cuad_repo,
        "get_cuadratura_row",
        lambda *_a, **_k: {
            "schema_version": 2,
            "status": "draft",
            "documents": [
                {
                    "related_document_id": 1,
                    "document_number": 10,
                    "monto_clp": 1000,
                    "medio_pago": "efectivo",
                    "observacion": "contado",
                    "client_name": "A",
                }
            ],
            "credit_notes_v2": [{"documento": "10", "nota_credito": "NC1", "monto": 0, "observacion": "x"}],
            "not_loaded_v2": [],
            "cash_count": [],
            "picking_id": 100,
            "picking_version": 1,
            "closed_at": None,
            "observacion": "obs",
            "updated_at": "2026-01-01",
        },
    )
    monkeypatch.setattr(svc.cuad_repo, "list_cuadratura_history", lambda *_a, **_k: [])

    def _bundle(plan_id, *, version=None, picking_id=None):
        if version == 1:
            return {
                "ready": True,
                "clients": [
                    {
                        "related_document_id": 1,
                        "document_number": 10,
                        "document_total": 1000,
                        "fantasy_name": "A",
                        "payment_method": "efectivo",
                        "route_order": 1,
                    }
                ],
                "items": [],
                "venta_picking_clp": 1000,
                "picking_id": 100,
                "picking_version": 1,
                "header": {},
                "is_current": False,
            }
        # current v2 with extra doc
        return {
            "ready": True,
            "clients": [
                {
                    "related_document_id": 1,
                    "document_number": 10,
                    "document_total": 1000,
                    "fantasy_name": "A",
                    "payment_method": "efectivo",
                    "route_order": 1,
                },
                {
                    "related_document_id": 2,
                    "document_number": 20,
                    "document_total": 500,
                    "fantasy_name": "B",
                    "payment_method": None,
                    "route_order": 2,
                },
            ],
            "items": [],
            "venta_picking_clp": 1500,
            "picking_id": 200,
            "picking_version": 2,
            "header": {},
            "is_current": True,
        }

    monkeypatch.setattr(svc, "_load_picking_bundle", _bundle)
    monkeypatch.setattr(svc, "_venta_facturada_clp", lambda *_a, **_k: 1500)

    out = svc.get_dispatch_plan_cuadratura(7)
    assert out["ventas"]["venta_picking_clp"] == 1000
    assert out["adopted_picking_version"] == 1
    assert out["available_picking_version"] == 2
    assert out["picking_update"]["pending"] is True
    assert out["picking_update"]["documents_added_count"] == 1
    assert out["write_blocked_by_picking_update"] is True
    # medio preservado en alcance adoptado
    assert out["documents"][0]["medio_pago"] == "efectivo"
    assert out["documents"][0]["observacion"] == "contado"
    assert out["credit_notes_v2"][0]["nota_credito"] == "NC1"


def test_save_blocked_when_drift(monkeypatch):
    monkeypatch.setattr(
        svc,
        "get_dispatch_plan_cuadratura",
        lambda *_a, **_k: {
            "picking_ready": True,
            "closed_at": None,
            "cuadratura_started": True,
            "write_blocked_by_picking_update": True,
            "picking_update": {
                "write_blocked": True,
                "message_es": "Actualización pendiente",
            },
            "adopted_picking_version": 1,
            "ventas": {"venta_picking_clp": 1000},
            "documents": [],
            "cash_count": [],
        },
    )
    with pytest.raises(svc.CuadraturaConflictError) as ei:
        svc.save_dispatch_plan_cuadratura_v2(7, {"documents": [], "schema_version": 2})
    assert ei.value.code == "picking_update_pending"


def test_adopt_idempotent(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 7}
    )
    monkeypatch.setattr(
        svc.cuad_repo,
        "lock_cuadratura_row",
        lambda *_a, **_k: {
            "documents": [{"related_document_id": 1, "monto_clp": 10, "medio_pago": "pendiente"}],
            "credit_notes_v2": [],
            "not_loaded_v2": [],
            "cash_count": [],
            "picking_id": 200,
            "picking_version": 2,
            "closed_at": None,
            "status": "draft",
            "observacion": None,
        },
    )
    monkeypatch.setattr(
        svc,
        "_load_picking_bundle",
        lambda *_a, **_k: {
            "ready": True,
            "clients": [{"related_document_id": 1, "document_total": 10}],
            "items": [],
            "venta_picking_clp": 10,
            "picking_id": 200,
            "picking_version": 2,
            "header": {},
        },
    )
    events = []
    monkeypatch.setattr(
        svc.batch_repo,
        "insert_order_event",
        lambda *_a, **kw: events.append(kw) or 1,
    )
    monkeypatch.setattr(
        svc,
        "get_dispatch_plan_cuadratura",
        lambda *_a, **_k: {"adopted_picking_version": 2},
    )
    out = svc.adopt_cuadratura_picking(7, user={"email": "a@b.c"})
    assert out["adopt_status"] == "unchanged"
    assert events and events[0]["reason"] == "idempotent_already_adopted"


def test_closed_blocks_adopt(monkeypatch):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc.plan_repo, "get_plan_by_id", lambda *_a, **_k: {"id": 7}
    )
    monkeypatch.setattr(
        svc.cuad_repo,
        "lock_cuadratura_row",
        lambda *_a, **_k: {
            "closed_at": "2026-01-01T00:00:00Z",
            "status": "squared",
            "picking_version": 1,
            "documents": [],
        },
    )
    with pytest.raises(svc.CuadraturaConflictError) as ei:
        svc.adopt_cuadratura_picking(7)
    assert ei.value.code == "cuadratura_closed"
