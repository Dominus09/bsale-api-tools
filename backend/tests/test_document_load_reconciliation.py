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


def test_plan82_style_move_oc_to_consolidado_keeps_docs_until_overwrite(monkeypatch):
    """Caso PLAN-00082: OC en consolidado (13), facturas aún en Grupo 1 (11).

    Tras mover OC, ``save_load_organization`` concilia con overwrite_manual=False,
    por lo que los documentos permanecen en el lote previo y reportan conflicto.
    Con overwrite_manual=True se alinean al grupo de la OC.
    """
    grupo1, consolidado = 11, 13
    ocs = [
        {"oc_document_id": 3975069, "load_batch_id": consolidado},  # 70745
        {"oc_document_id": 3975063, "load_batch_id": consolidado},  # 70744
        {"oc_document_id": 3975259, "load_batch_id": consolidado},  # 70756
    ]
    docs = [
        {
            "related_document_id": 3975261,
            "oc_document_id": 3975069,
            "load_batch_id": grupo1,
            "document_number": 51818,
            "client_name": "puñadito de dulzura",
            "document_total": 344043,
        },
        {
            "related_document_id": 3975263,
            "oc_document_id": 3975063,
            "load_batch_id": grupo1,
            "document_number": 51819,
            "client_name": "Romeo Alfa",
            "document_total": 155033,
        },
        {
            "related_document_id": 3975294,
            "oc_document_id": 3975259,
            "load_batch_id": grupo1,
            "document_number": 51820,
            "client_name": "Nayi's 2",
            "document_total": 75580,
        },
    ]
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.assignments_table_ready",
        lambda _c: True,
    )
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.list_order_assignments",
        lambda _c, _pid: ocs,
    )
    monkeypatch.setattr(svc.batch_repo, "list_document_assignments", lambda _c, _pid: docs)

    upserts: list[dict] = []

    def _upsert(_cur, **kw):
        upserts.append(kw)

    monkeypatch.setattr(svc.batch_repo, "upsert_document_assignment", _upsert)

    blocked = svc._reconcile_document_batches_from_oc(_Cur(), 82, overwrite_manual=False)
    assert blocked["reconciled"] == 0
    assert upserts == []
    assert len(blocked["conflicts"]) == 3
    assert all(c.get("code") == "manual_differs_from_oc_group" for c in blocked["conflicts"])
    assert {c["current_load_batch_id"] for c in blocked["conflicts"]} == {grupo1}
    assert {c["suggested_load_batch_id"] for c in blocked["conflicts"]} == {consolidado}
    for c in blocked["conflicts"]:
        assert f"lote manual {grupo1}" in c["message_es"]
        assert f"sugiere grupo {consolidado}" in c["message_es"]

    fixed = svc._reconcile_document_batches_from_oc(_Cur(), 82, overwrite_manual=True)
    assert fixed["reconciled"] == 3
    assert fixed["conflicts"] == []
    assert {u["load_batch_id"] for u in upserts} == {consolidado}
    assert {u["related_document_id"] for u in upserts} == {3975261, 3975263, 3975294}


def test_scoped_overwrite_only_touches_selected_ocs(monkeypatch):
    """Overwrite selectivo: no reescribe asignaciones ajenas al alcance."""
    grupo1, consolidado, otro = 11, 13, 99
    ocs = [
        {"oc_document_id": 1, "load_batch_id": consolidado},
        {"oc_document_id": 2, "load_batch_id": consolidado},
        {"oc_document_id": 3, "load_batch_id": consolidado},
    ]
    docs = [
        {
            "related_document_id": 10,
            "oc_document_id": 1,
            "load_batch_id": grupo1,
            "document_number": 1,
            "client_name": "A",
            "document_total": 1,
        },
        {
            "related_document_id": 20,
            "oc_document_id": 2,
            "load_batch_id": grupo1,
            "document_number": 2,
            "client_name": "B",
            "document_total": 2,
        },
        {
            "related_document_id": 30,
            "oc_document_id": 3,
            "load_batch_id": otro,
            "document_number": 3,
            "client_name": "C",
            "document_total": 3,
        },
    ]
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.assignments_table_ready",
        lambda _c: True,
    )
    monkeypatch.setattr(
        "backend.repositories.distribuidora.dispatch_plan_order_load_repo.list_order_assignments",
        lambda _c, _pid: ocs,
    )
    monkeypatch.setattr(svc.batch_repo, "list_document_assignments", lambda _c, _pid: docs)
    upserts: list[dict] = []
    monkeypatch.setattr(
        svc.batch_repo,
        "upsert_document_assignment",
        lambda _c, **kw: upserts.append(kw),
    )

    out = svc._reconcile_document_batches_from_oc(
        _Cur(),
        82,
        overwrite_manual=True,
        scope_oc_document_ids={1, 2},
    )
    assert out["reconciled"] == 2
    assert {u["related_document_id"] for u in upserts} == {10, 20}
    assert all(u["load_batch_id"] == consolidado for u in upserts)
    # Doc 30 (OC 3) fuera de alcance: intacto.
    assert 30 not in {u["related_document_id"] for u in upserts}

    dry = svc._reconcile_document_batches_from_oc(
        _Cur(),
        82,
        overwrite_manual=True,
        scope_related_document_ids={10},
        dry_run=True,
    )
    assert dry["reconciled"] == 1
    assert dry["moves"][0]["related_document_id"] == 10
    assert dry["dry_run"] is True


def test_assess_detects_omissions_not_just_duplicates(monkeypatch):
    """UNIQUE evita doble grupo; la completitud exige cero omisiones."""
    monkeypatch.setattr(
        svc.batch_repo,
        "list_document_assignments",
        lambda _c, _pid: [
            {"related_document_id": 1, "load_batch_id": 100},
            {"related_document_id": 2, "load_batch_id": None},
        ],
    )
    status = svc.assess_document_assignment_completeness(_Cur(), 79)
    assert status["documents_total"] == 2
    assert status["documents_unassigned"] == 1
    assert status["document_assignment_complete"] is False
    assert status["unassigned_related_document_ids"] == [2]


class _Conn:
    def __init__(self, cur):
        self._cur = cur

    def cursor(self):
        return self._cur

    def close(self):
        return None


def test_assert_group_export_complete_blocks_partial(monkeypatch):
    monkeypatch.setattr(
        svc.batch_repo,
        "list_document_assignments",
        lambda _c, _pid: [
            {"related_document_id": 1, "load_batch_id": 100},
            {"related_document_id": 2, "load_batch_id": None},
        ],
    )
    cur = _Cur()
    cur.close = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setattr(svc, "get_connection", lambda: _Conn(cur))
    try:
        svc.assert_group_export_complete(79)
        raise AssertionError("debía fallar por omisiones")
    except ValueError as exc:
        assert "incompleta" in str(exc).lower()


def test_assert_group_export_complete_ok_when_all_assigned(monkeypatch):
    monkeypatch.setattr(
        svc.batch_repo,
        "list_document_assignments",
        lambda _c, _pid: [
            {"related_document_id": 1, "load_batch_id": 100},
            {"related_document_id": 2, "load_batch_id": 200},
        ],
    )
    cur = _Cur()
    cur.close = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setattr(svc, "get_connection", lambda: _Conn(cur))
    status = svc.assert_group_export_complete(79)
    assert status["document_assignment_complete"] is True
