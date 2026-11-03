"""Confirmación segura planificación simplificada (mocks, sin PG analytics)."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from backend.services.distribuidora import simple_planning_confirm_service as svc
from backend.services.distribuidora.simple_planning_preview_service import (
    oc_identity_fingerprint,
)


def _fp(folio: int) -> str:
    return oc_identity_fingerprint(
        document_id=folio,
        source_document_id=9000 + folio,
        state=0,
        client_id=1,
        total_amount=100,
        emission_date=date(2026, 11, 1),
        products=[],
    )


def _order(folio: int, *, peso=10.0, quality="known", fingerprint=None):
    return {
        "folio": folio,
        "document_id": folio,
        "identity_fingerprint": fingerprint or _fp(folio),
        "blocking": False,
        "warnings": [],
        "client": {"client_id": 1, "client_name": f"C{folio}", "municipality": "X"},
        "total_amount": 100,
        "weight": {
            "peso_total_kg": peso,
            "quality": quality,
            "coverage_percent": 100,
            "missing_products": 0,
        },
        "products": [],
    }


def _assign_report(orders, *, truck_id=2, max_kg=2000, capacity_known=True):
    from backend.services.distribuidora.simple_planning_assignment_service import (
        validate_truck_capacity,
    )

    known = sum(
        float(o["weight"]["peso_total_kg"])
        for o in orders
        if o["weight"]["quality"] == "known"
    )
    unavailable = sum(1 for o in orders if o["weight"]["quality"] == "unavailable")
    capacity = validate_truck_capacity(
        assigned_weight_kg=known,
        known_weight_kg=known,
        estimated_weight_kg=0,
        unavailable_orders=unavailable,
        truck_max_weight_kg=max_kg if capacity_known else None,
    )
    return {
        "wrote": False,
        "blocking_count": 0,
        "request_duplicate_folios": [],
        "fingerprint_drifts": [],
        "active_plan_conflicts": [],
        "trucks": [
            {
                "truck_id": truck_id,
                "truck": {
                    "truck_id": truck_id,
                    "name": "Hino",
                    "max_weight_kg": max_kg if capacity_known else None,
                    "active": True,
                },
                "truck_missing": False,
                "truck_inactive": False,
                "orders": orders,
                "capacity": capacity,
                "blocking": capacity.get("blocking"),
            }
        ],
        "assignments": [
            {"folio": o["folio"], "truck_id": truck_id, "order": o, "blocking": False}
            for o in orders
        ],
    }


class _FakeConn:
    def __init__(self, sessions=None):
        self.sessions = sessions if sessions is not None else {}
        self.committed = False
        self.rolled_back = False
        self._cur = _FakeCur(self)

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


class _FakeCur:
    def __init__(self, conn: _FakeConn):
        self.conn = conn
        self._last = None
        self.description = [("id",)]

    def execute(self, sql, params=None):
        self._last = (sql, params)
        sql_l = " ".join(sql.lower().split())
        if "from distribuidora.simple_planning_sessions" in sql_l and "for update" in sql_l:
            sid = str(params[0])
            self._row = self.conn.sessions.get(sid)
        elif "insert into distribuidora.simple_planning_sessions" in sql_l:
            # handled by monkeypatched repo in tests
            self._row = None
        else:
            self._row = None

    def fetchone(self):
        return self._row


def test_prepare_blocks_unknown_capacity(monkeypatch):
    orders = [_order(1)]
    monkeypatch.setattr(
        svc,
        "preview_truck_assignments",
        lambda **_k: _assign_report(orders, capacity_known=False),
    )
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.prepare_confirm_session(
            user_id=1,
            company_id=3,
            office_id=1,
            planning_date="2026-11-10",
            truck_id=2,
            dispatch_name="Sur",
            driver_name=None,
            assignments=[{"folio": 1, "truck_id": 2}],
            conn_factory=lambda: (_ for _ in ()).throw(RuntimeError("no db")),
        )
    assert ei.value.code == "truck_capacity_unknown"


def test_prepare_blocks_overload(monkeypatch):
    orders = [_order(1, peso=900), _order(2, peso=900)]
    monkeypatch.setattr(
        svc,
        "preview_truck_assignments",
        lambda **_k: _assign_report(orders, max_kg=1000),
    )
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.prepare_confirm_session(
            user_id=1,
            company_id=3,
            office_id=1,
            planning_date=date(2026, 11, 10),
            truck_id=2,
            dispatch_name="Sur",
            driver_name="Juan",
            assignments=[
                {"folio": 1, "truck_id": 2},
                {"folio": 2, "truck_id": 2},
            ],
            conn_factory=lambda: (_ for _ in ()).throw(RuntimeError("no db")),
        )
    assert ei.value.code == "truck_overloaded"


def test_prepare_blocks_unavailable_weight(monkeypatch):
    orders = [_order(3, peso=None, quality="unavailable")]
    monkeypatch.setattr(
        svc,
        "preview_truck_assignments",
        lambda **_k: _assign_report(orders),
    )
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.prepare_confirm_session(
            user_id=1,
            company_id=3,
            office_id=1,
            planning_date=date.today(),
            truck_id=2,
            dispatch_name=None,
            driver_name=None,
            assignments=[{"folio": 3, "truck_id": 2}],
            conn_factory=lambda: (_ for _ in ()).throw(RuntimeError("no db")),
        )
    assert ei.value.code == "weight_unavailable"


def test_prepare_and_confirm_happy_path(monkeypatch):
    orders = [_order(10), _order(11)]
    report = _assign_report(orders)
    monkeypatch.setattr(svc, "preview_truck_assignments", lambda **_k: report)

    inserted = {}

    def _insert(cur, fields):
        row = {
            **fields,
            "id": str(fields["id"]),
            "confirmed_plan_id": None,
            "payload": fields["payload"]
            if isinstance(fields["payload"], dict)
            else fields["payload"],
        }
        inserted["row"] = row
        return row

    monkeypatch.setattr(svc.session_repo, "insert_session", _insert)
    monkeypatch.setattr(
        svc.session_repo,
        "get_session_by_idempotency",
        lambda *_a, **_k: None,
    )

    conn = _FakeConn()
    out = svc.prepare_confirm_session(
        user_id=7,
        company_id=3,
        office_id=1,
        planning_date="2026-12-01",
        truck_id=2,
        dispatch_name="Norte",
        driver_name="Ana",
        assignments=[
            {"folio": 10, "truck_id": 2, "expected_fingerprint": _fp(10)},
            {"folio": 11, "truck_id": 2, "expected_fingerprint": _fp(11)},
        ],
        conn_factory=lambda: conn,
    )
    assert out["status"] == "created"
    session_id = out["session"]["session_id"]
    assert out["session"]["ors_metrics_calculated"] is False

    # Confirm path
    session_row = dict(inserted["row"])
    session_row["expires_at"] = datetime.now(timezone.utc) + timedelta(minutes=10)
    session_row["status"] = "open"
    session_row["user_id"] = 7
    session_row["company_id"] = 3
    session_row["office_id"] = 1
    session_row["truck_id"] = 2
    session_row["planning_date"] = date(2026, 12, 1)
    session_row["dispatch_name"] = "Norte"
    session_row["driver_name"] = "Ana"
    session_row["plan_session_id"] = inserted["row"]["plan_session_id"]
    session_row["payload"] = inserted["row"]["payload"]

    confirm_conn = _FakeConn(sessions={session_id: session_row})

    def _get_for_update(cur, sid):
        return confirm_conn.sessions.get(str(sid))

    marks = []

    def _mark(cur, sid, **kw):
        marks.append(kw)
        row = confirm_conn.sessions[str(sid)]
        row["status"] = kw["status"]
        if kw.get("confirmed_plan_id"):
            row["confirmed_plan_id"] = kw["confirmed_plan_id"]

    monkeypatch.setattr(svc.session_repo, "get_session_by_id_for_update", _get_for_update)
    monkeypatch.setattr(svc.session_repo, "mark_session_status", _mark)

    confirmed_calls = []

    def _confirm(**kwargs):
        confirmed_calls.append(kwargs)
        assert kwargs["planning_origin"] == "simple_list"
        assert kwargs["ors_metrics_calculated"] is False
        assert kwargs["km_total"] is None
        assert kwargs["diesel_price_per_liter"] is None
        assert kwargs["conn"] is confirm_conn
        return {"plan": {"id": 555}}

    monkeypatch.setattr(svc, "get_dispatch_plan", lambda _id: {"plan": {"id": 555, "status": "planned"}})
    monkeypatch.setattr(
        "backend.services.distribuidora.dispatch_plan_service._try_auto_generate_picking_snapshot",
        lambda *_a, **_k: None,
    )

    result = svc.confirm_simple_planning_session(
        session_id=session_id,
        user_id=7,
        confirm_fn=_confirm,
        conn_factory=lambda: confirm_conn,
    )
    assert result["status"] == "confirmed"
    assert result["wrote"] is True
    assert result["ors_metrics_calculated"] is False
    assert confirmed_calls and len(confirmed_calls[0]["orders"]) == 2
    assert any(m.get("status") == "confirmed" for m in marks)


def test_confirm_rejects_foreign_session(monkeypatch):
    sid = str(uuid4())
    row = {
        "id": sid,
        "user_id": 99,
        "status": "open",
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        "payload": {"orders": []},
        "truck_id": 1,
        "company_id": 3,
        "office_id": 1,
        "plan_session_id": "simple-x",
    }
    conn = _FakeConn(sessions={sid: row})
    monkeypatch.setattr(
        svc.session_repo,
        "get_session_by_id_for_update",
        lambda cur, s: conn.sessions.get(str(s)),
    )
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.confirm_simple_planning_session(
            session_id=sid,
            user_id=1,
            conn_factory=lambda: conn,
        )
    assert ei.value.code == "session_forbidden"


def test_confirm_fingerprint_drift(monkeypatch):
    sid = str(uuid4())
    fp = _fp(50)
    row = {
        "id": sid,
        "user_id": 1,
        "status": "open",
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        "payload": {
            "orders": [
                {
                    "folio": 50,
                    "document_id": 50,
                    "identity_fingerprint": fp,
                    "fingerprint_parts": {
                        "identity": "a",
                        "header": "b",
                        "lines": "c",
                    },
                }
            ]
        },
        "truck_id": 2,
        "company_id": 3,
        "office_id": 1,
        "plan_session_id": "simple-y",
        "dispatch_name": "Z",
        "driver_name": None,
        "planning_date": date.today(),
    }
    conn = _FakeConn(sessions={sid: row})
    monkeypatch.setattr(
        svc.session_repo,
        "get_session_by_id_for_update",
        lambda cur, s: conn.sessions.get(str(s)),
    )

    marks = []

    def _mark(cur, sid_, **kw):
        marks.append(kw)
        row_s = conn.sessions[str(sid_)]
        row_s["status"] = kw["status"]

    monkeypatch.setattr(svc.session_repo, "mark_session_status", _mark)

    drifted_order = _order(50, fingerprint="other")
    drifted_order["fingerprint_parts"] = {
        "identity": "a",
        "header": "CHANGED",
        "lines": "c",
    }
    drifted = _assign_report([drifted_order])
    drifted["fingerprint_drifts"] = []
    monkeypatch.setattr(svc, "preview_truck_assignments", lambda **_k: drifted)

    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.confirm_simple_planning_session(
            session_id=sid,
            user_id=1,
            conn_factory=lambda: conn,
        )
    assert ei.value.code == "fingerprint_drift"
    assert "OC 50:" in str(ei.value)
    assert ei.value.details.get("session_obsolete") is True
    assert any(m.get("status") == "failed" for m in marks)


def test_prepare_fingerprint_drift_message_includes_folio(monkeypatch):
    orders = [_order(70625, fingerprint="fresh")]
    report = _assign_report(orders)
    report["fingerprint_drifts"] = [
        {
            "folio": 70625,
            "changed_parts": ["cabecera_comercial"],
            "detail": "OC 70625: sus datos cambiaron después de la previsualización",
        }
    ]
    monkeypatch.setattr(svc, "preview_truck_assignments", lambda **_k: report)
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.prepare_confirm_session(
            user_id=1,
            company_id=3,
            office_id=1,
            planning_date=date.today(),
            truck_id=2,
            dispatch_name=None,
            driver_name=None,
            assignments=[
                {
                    "folio": 70625,
                    "truck_id": 2,
                    "expected_fingerprint": "stale",
                }
            ],
            conn_factory=lambda: (_ for _ in ()).throw(RuntimeError("no db")),
        )
    assert ei.value.code == "fingerprint_drift"
    assert "OC 70625:" in str(ei.value)
    assert ei.value.details.get("stage") == "prepare-confirm"


def test_confirm_rejects_expired_session(monkeypatch):
    sid = str(uuid4())
    row = {
        "id": sid,
        "user_id": 1,
        "status": "open",
        "expires_at": datetime.now(timezone.utc) - timedelta(minutes=5),
        "payload": {"orders": [{"folio": 1, "document_id": 1, "identity_fingerprint": "x"}]},
        "truck_id": 2,
        "company_id": 3,
        "office_id": 1,
        "plan_session_id": "simple-exp",
    }
    conn = _FakeConn(sessions={sid: row})
    monkeypatch.setattr(
        svc.session_repo,
        "get_session_by_id_for_update",
        lambda cur, s: conn.sessions.get(str(s)),
    )
    monkeypatch.setattr(svc.session_repo, "mark_session_status", lambda *_a, **_k: None)
    with pytest.raises(svc.SimplePlanningConfirmError) as ei:
        svc.confirm_simple_planning_session(
            session_id=sid,
            user_id=1,
            conn_factory=lambda: conn,
        )
    assert ei.value.code in ("session_expired", "session_obsolete", "confirm_rejected") or "expir" in str(
        ei.value
    ).lower()


def test_confirm_idempotent_replay(monkeypatch):
    sid = str(uuid4())
    row = {
        "id": sid,
        "user_id": 1,
        "status": "confirmed",
        "confirmed_plan_id": 77,
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        "payload": {},
        "truck_id": 2,
        "company_id": 3,
        "office_id": 1,
        "plan_session_id": "simple-z",
    }
    conn = _FakeConn(sessions={sid: row})
    monkeypatch.setattr(
        svc.session_repo,
        "get_session_by_id_for_update",
        lambda cur, s: conn.sessions.get(str(s)),
    )
    monkeypatch.setattr(
        svc, "get_dispatch_plan", lambda _id: {"plan": {"id": 77, "status": "planned"}}
    )
    out = svc.confirm_simple_planning_session(
        session_id=sid,
        user_id=1,
        conn_factory=lambda: conn,
        confirm_fn=lambda **_k: (_ for _ in ()).throw(RuntimeError("should not confirm")),
    )
    assert out["status"] == "already_confirmed"
    assert out["idempotent"] is True
    assert out["wrote"] is False


def test_confirm_rolls_back_on_mid_transaction_error(monkeypatch):
    sid = str(uuid4())
    orders = [_order(70)]
    row = {
        "id": sid,
        "user_id": 1,
        "status": "open",
        "expires_at": datetime.now(timezone.utc) + timedelta(hours=1),
        "payload": {
            "orders": [
                {
                    "folio": 70,
                    "document_id": 70,
                    "identity_fingerprint": _fp(70),
                }
            ]
        },
        "truck_id": 2,
        "company_id": 3,
        "office_id": 1,
        "plan_session_id": "simple-mid",
        "dispatch_name": "M",
        "driver_name": None,
        "planning_date": date.today(),
    }
    conn = _FakeConn(sessions={sid: row})
    monkeypatch.setattr(
        svc.session_repo,
        "get_session_by_id_for_update",
        lambda cur, s: conn.sessions.get(str(s)),
    )
    monkeypatch.setattr(svc.session_repo, "mark_session_status", lambda *_a, **_k: None)
    monkeypatch.setattr(svc, "preview_truck_assignments", lambda **_k: _assign_report(orders))
    monkeypatch.setattr(
        svc,
        "_mark_session_failed_new_conn",
        lambda **_k: None,
    )

    def _boom(**_k):
        raise ValueError("boom midway")

    with pytest.raises(svc.SimplePlanningConfirmError):
        svc.confirm_simple_planning_session(
            session_id=sid,
            user_id=1,
            confirm_fn=_boom,
            conn_factory=lambda: conn,
        )
    assert conn.rolled_back is True
