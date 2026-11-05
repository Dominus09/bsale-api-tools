"""Conciliación documental OC→grupo→factura (sin PG analytics)."""

from __future__ import annotations

from backend.services.distribuidora import dispatch_plan_load_batch_service as svc


class _Cur:
    pass


def test_reconcile_fills_null_batch_from_oc_group(monkeypatch):
    calls = []

    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.assignments_table_ready",
        lambda _c: True,
    )
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.list_order_assignments",
        lambda _c, _pid: [
            {"oc_document_id": 10, "load_batch_id": 100},
            {"oc_document_id": 11, "load_batch_id": 200},
        ],
    )
    monkeypatch.setattr(
        svc.batch_repo,
        "list_document_assignments",
        lambda _c, _pid: [
            {
                "related_document_id": 1000,
                "oc_document_id": 10,
                "load_batch_id": None,
                "document_number": 1,
                "client_name": "A",
                "document_total": 100,
            },
            {
                "related_document_id": 1001,
                "oc_document_id": 11,
                "load_batch_id": None,
                "document_number": 2,
                "client_name": "B",
                "document_total": 200,
            },
        ],
    )

    def _upsert(_cur, **kwargs):
        calls.append(kwargs)

    monkeypatch.setattr(svc.batch_repo, "upsert_document_assignment", _upsert)

    out = svc._reconcile_document_batches_from_oc(_Cur(), 79, overwrite_manual=False)
    assert out["reconciled"] == 2
    assert out["pending"] == 0
    assert out["conflicts"] == []
    assert {c["load_batch_id"] for c in calls} == {100, 200}


def test_reconcile_detects_invoice_spanning_groups(monkeypatch):
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.assignments_table_ready",
        lambda _c: True,
    )
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.list_order_assignments",
        lambda _c, _pid: [
            {"oc_document_id": 10, "load_batch_id": 100},
            {"oc_document_id": 11, "load_batch_id": 200},
        ],
    )
    # Mismo related_document con dos filas no ocurre por UNIQUE; simulamos
    # dos OCs apuntando al mismo documento vía payloads distintos en la detección
    # agrupando por related_document_id con OCs de grupos distintos.
    monkeypatch.setattr(
        svc.batch_repo,
        "list_document_assignments",
        lambda _c, _pid: [
            {
                "related_document_id": 999,
                "oc_document_id": 10,
                "load_batch_id": None,
                "document_number": 9,
                "client_name": "X",
                "document_total": 1,
            },
            {
                # Segunda fila con mismo related_id no es realista en DB, pero
                # la lógica de conflicto se basa en doc_batches map; forzar
                # dos OCs bajo el mismo related_id en el mapa.
                "related_document_id": 999,
                "oc_document_id": 11,
                "load_batch_id": None,
                "document_number": 9,
                "client_name": "X",
                "document_total": 1,
            },
        ],
    )
    upserts = []
    monkeypatch.setattr(
        svc.batch_repo,
        "upsert_document_assignment",
        lambda **kw: upserts.append(kw),
    )
    out = svc._reconcile_document_batches_from_oc(_Cur(), 79)
    assert out["reconciled"] == 0
    assert any(c.get("code") == "invoice_spans_groups" for c in out["conflicts"])
    assert upserts == []


def test_reconcile_respects_manual_assignment(monkeypatch):
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.assignments_table_ready",
        lambda _c: True,
    )
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.list_order_assignments",
        lambda _c, _pid: [{"oc_document_id": 10, "load_batch_id": 100}],
    )
    monkeypatch.setattr(
        svc.batch_repo,
        "list_document_assignments",
        lambda _c, _pid: [
            {
                "related_document_id": 1000,
                "oc_document_id": 10,
                "load_batch_id": 200,  # manual distinto
                "document_number": 1,
                "client_name": "A",
                "document_total": 100,
            }
        ],
    )
    upserts = []
    monkeypatch.setattr(
        svc.batch_repo,
        "upsert_document_assignment",
        lambda **kw: upserts.append(kw),
    )
    out = svc._reconcile_document_batches_from_oc(_Cur(), 79, overwrite_manual=False)
    assert out["reconciled"] == 0
    assert upserts == []
    assert any(c.get("code") == "manual_differs_from_oc_group" for c in out["conflicts"])
