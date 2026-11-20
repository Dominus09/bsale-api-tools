"""Offline V2 2A.4 — integración laboratorio (PG aislado + contrato sync).

Simula el camino outbox→sync→PostgreSQL con 5 operaciones, conflictos,
respuesta perdida, R2 mock y aislamiento. Nunca analytics/prod.

Requiere SIMPLE_PLANNING_TEST_DSN (runner pgserver).
"""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_offline_sync_service as offline_svc
from backend.services.distribuidora import delivery_visit_service as visit_svc
from backend.services.distribuidora import delivery_offline_validate as ov

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "backend" / "sql" / "distribuidora"
SQL_FILES = [
    SQL_DIR / "056_delivery_visits_evidence.sql",
    SQL_DIR / "057_delivery_visit_closure_guards.sql",
    SQL_DIR / "058_delivery_visit_admin_void.sql",
    SQL_DIR / "059_delivery_payment_declaration_AWAITING_APPROVAL.sql",
    SQL_DIR / "060_delivery_check_declared_AWAITING_APPROVAL.sql",
    SQL_DIR / "061_delivery_offline_ops_AWAITING_APPROVAL.sql",
]

pytestmark = pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN a Postgres de prueba aislado (nunca analytics)",
)

# 4 entregas completas + 1 local cerrado
CLIENTS = [
    {"key": "id:9201", "cid": 9201, "doc": 91055, "d1": 91001, "d2": 91002},
    {"key": "id:9202", "cid": 9202, "doc": 91056, "d1": 91003, "d2": 91004},
    {"key": "id:9203", "cid": 9203, "doc": 91057, "d1": 91005, "d2": 91006},
    {"key": "id:9204", "cid": 9204, "doc": 91058, "d1": 91007, "d2": 91008},
    {"key": "id:9205", "cid": 9205, "doc": 91059, "d1": 91009, "d2": 91010},
]


def _assert_isolated(uri: str) -> None:
    low = uri.lower()
    for b in ("analytics", "quillotana", "prod", "coolify", "neon.tech", "supabase"):
        if b in low:
            raise RuntimeError(f"DSN sospechoso: {b}")


def _connect():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    _assert_isolated(DSN)
    return psycopg2.connect(DSN, cursor_factory=RealDictCursor)


def _strip_tx(sql: str) -> str:
    return "\n".join(
        ln
        for ln in sql.splitlines()
        if ln.strip().upper() not in ("BEGIN;", "COMMIT;", "BEGIN", "COMMIT")
    )


def _split_sql(raw: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    for line in raw.splitlines():
        if line.strip() == "-- +go":
            chunk = "\n".join(buf).strip()
            if chunk:
                parts.append(chunk)
            buf = []
        else:
            buf.append(line)
    chunk = "\n".join(buf).strip()
    if chunk:
        parts.append(chunk)
    return parts


def _bootstrap(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    for t in (
        "delivery_offline_ops",
        "delivery_report_snapshots",
        "delivery_conformity",
        "delivery_evidence",
        "delivery_collections",
        "delivery_line_results",
        "delivery_document_results",
        "delivery_visit_events",
        "delivery_visits",
        "document_details",
        "dispatch_plan",
    ):
        cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'confirmed',
            planning_code TEXT,
            driver_user_id INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.document_details (
            detail_id BIGINT PRIMARY KEY,
            document_id BIGINT NOT NULL,
            line_number INT,
            variant_id BIGINT,
            variant_description TEXT,
            variant_code TEXT,
            quantity NUMERIC(18, 4)
        )
        """
    )
    conn.commit()
    for path in SQL_FILES:
        raw = _strip_tx(path.read_text(encoding="utf-8"))
        for stmt in _split_sql(raw):
            if stmt.strip():
                cur.execute(stmt)
    conn.commit()
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan (id, planning_code, driver_user_id)
        VALUES (920, 'PLAN-OFFLINE-2A4-LAB', 42)
        """
    )
    detail_rows = []
    for i, c in enumerate(CLIENTS):
        detail_rows.append((c["d1"], c["doc"], 1, 1, f"Prod {i}A", 4))
        detail_rows.append((c["d2"], c["doc"], 2, 1, f"Prod {i}B", 2))
    cur.executemany(
        """
        INSERT INTO distribuidora.document_details
            (detail_id, document_id, line_number, variant_id, variant_description, quantity)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        detail_rows,
    )
    conn.commit()
    cur.close()


def _user(uid: int = 42):
    return {"id": uid, "email": f"u{uid}@lab.local"}


def _r2_store():
    store: dict[str, bytes] = {}

    def put(key: str, data: bytes, content_type: str) -> str:
        store[key] = data
        return key

    return store, put


def _complete_body(idem: str, client: dict, *, method: str = "transfer_declared"):
    return {
        "idempotency_key": idem,
        "client_mutation_id": f"mut-{idem}",
        "op_type": "complete_delivery",
        "customer_key": client["key"],
        "picking_id": 501,
        "picking_version": "3",
        "captured_at": "2026-11-20T12:00:00-03:00",
        "device_id": "lab-iphone",
        "result": {"status": "completed", "note": "Entrega OK lab"},
        "documents": [
            {
                "related_document_id": client["doc"],
                "document_number": 1000 + client["cid"],
                "lines": [
                    {
                        "source_detail_id": client["d1"],
                        "planned_units": 4,
                        "delivered_units": 4,
                        "sku": "A",
                        "description": "Prod A",
                    },
                    {
                        "source_detail_id": client["d2"],
                        "planned_units": 2,
                        "delivered_units": 2,
                        "sku": "B",
                        "description": "Prod B",
                    },
                ],
            }
        ],
        "payment": {"method": method},
        "gps": {
            "lat": -33.4501,
            "lng": -70.6601,
            "accuracy_m": 12,
            "captured_at": "2026-11-20T12:00:00-03:00",
        },
    }


def _closed_body(idem: str, client: dict):
    return {
        "idempotency_key": idem,
        "op_type": "closed_store",
        "customer_key": client["key"],
        "picking_id": 501,
        "picking_version": "3",
        "captured_at": "2026-11-20T11:00:00-03:00",
        "result": {
            "status": "not_delivered",
            "reason": "closed_store",
            "note": "Local cerrado lab sin atención",
        },
        "gps": {
            "lat": -33.4502,
            "lng": -70.6602,
            "accuracy_m": 18,
            "captured_at": "2026-11-20T11:00:00-03:00",
        },
        "evidence_meta": {
            "local_id": "ev1",
            "content_sha256": "",
            "captured_at": "2026-11-20T11:00:05-03:00",
        },
        "note": "Local cerrado lab sin atención",
    }


@pytest.fixture()
def lab_db(monkeypatch):
    assert DSN, "SIMPLE_PLANNING_TEST_DSN requerido"
    conn = _connect()
    _bootstrap(conn)
    conn.close()

    def fake_get_connection():
        return _connect()

    by_key = {c["key"]: c for c in CLIENTS}

    def resolve_client(_p, key):
        c = by_key.get(key) or CLIENTS[0]
        return {
            "client_id": c["cid"],
            "customer_key": c["key"],
            "client_name": f"Cliente {c['cid']}",
            "latitude": -33.45,
            "longitude": -70.66,
        }

    monkeypatch.setattr(offline_svc, "get_connection", fake_get_connection)
    monkeypatch.setattr(visit_svc, "get_connection", fake_get_connection)
    monkeypatch.setattr(ov, "get_connection", fake_get_connection)
    monkeypatch.setattr(
        visit_svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        offline_svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(ov.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42)
    for svc in (visit_svc, offline_svc):
        monkeypatch.setattr(svc.loc_svc, "_resolve_client", resolve_client)
        monkeypatch.setattr(
            svc.loc_svc, "_parse_client_id", lambda k, c: int(c["client_id"])
        )
        monkeypatch.setattr(
            svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
        )
    monkeypatch.setattr(
        visit_svc, "_client_ref_coords", lambda _p, _k: (-33.45, -70.66)
    )

    def expected_docs(_p, key):
        c = by_key.get(key)
        return [c["doc"]] if c else []

    monkeypatch.setattr(visit_svc, "_expected_related_document_ids", expected_docs)

    def lab_context(_plan_id: int, customer_key: str) -> dict:
        c = by_key[customer_key]
        conn2 = _connect()
        cur = conn2.cursor()
        cur.execute(
            """
            SELECT detail_id, quantity
            FROM distribuidora.document_details
            WHERE document_id = %s
            ORDER BY detail_id
            """,
            (c["doc"],),
        )
        rows = cur.fetchall()
        cur.close()
        conn2.close()
        return {
            "picking_ready": True,
            "picking_reason": None,
            "picking_id": 501,
            "picking_version": "3",
            "is_current": True,
            "plan_status": "confirmed",
            "customer_key": customer_key,
            "ficha": {
                "client": {
                    "client_id": c["cid"],
                    "customer_key": customer_key,
                    "client_name": f"Cliente {c['cid']}",
                },
                "documents": [
                    {
                        "related_document_id": c["doc"],
                        "products": [
                            {
                                "source_detail_id": int(r["detail_id"]),
                                "unidades": float(r["quantity"]),
                            }
                            for r in rows
                        ],
                    }
                ],
            },
        }

    real_validate = ov.validate_offline_operation

    def validate_lab(*, user, plan_id, body, context_fetcher=None, allow_closed_visit_id=None):
        return real_validate(
            user=user,
            plan_id=plan_id,
            body=body,
            context_fetcher=context_fetcher or lab_context,
            allow_closed_visit_id=allow_closed_visit_id,
        )

    monkeypatch.setattr(ov, "validate_offline_operation", validate_lab)
    monkeypatch.setattr(offline_svc, "_lab_context_for_tests", lab_context, raising=False)
    yield {"lab_context": lab_context}


def test_061_awaits_approval_and_deps():
    t061 = (SQL_DIR / "061_delivery_offline_ops_AWAITING_APPROVAL.sql").read_text(
        encoding="utf-8"
    )
    t059 = (SQL_DIR / "059_delivery_payment_declaration_AWAITING_APPROVAL.sql").read_text(
        encoding="utf-8"
    )
    t060 = (SQL_DIR / "060_delivery_check_declared_AWAITING_APPROVAL.sql").read_text(
        encoding="utf-8"
    )
    assert "AWAITING" in t061.upper()
    assert "delivery_offline_ops" in t061
    # 061 no ALTER de method; depende de 056+visitas. 059/060 amplían CHECK de pago.
    assert "cash_and_transfer" in t059
    assert "check_declared" in t060
    assert "delivery_collections_method_check" not in t061


def test_feature_flag_default_off(lab_db, monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_WRITES_ENABLED", raising=False)
    assert offline_svc.offline_sync_writes_enabled() is False
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=920,
            body=_complete_body("flag-off", CLIENTS[0]),
            r2_put=lambda *a, **k: "",
            require_writes_flag=True,
        )
    assert ei.value.status_code == 503


def test_five_offline_ops_sync_no_duplicates(lab_db):
    """Criterio de éxito 2A.4: 4 complete + 1 closed → 5 visitas, sin dupes."""
    store, put = _r2_store()
    results = []
    # 4 entregas completas
    for i, client in enumerate(CLIENTS[:4]):
        out = offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=920,
            body=_complete_body(f"lab-complete-{i}", client),
            r2_put=put,
        )
        assert out["idempotent_replay"] is False
        assert out["visit"]["status"] == "completed"
        results.append(out)

    # 1 local cerrado
    photo = b"\xff\xd8\xff" + b"LAB-CLOSED-PHOTO" * 8
    body = _closed_body("lab-closed-0", CLIENTS[4])
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()
    closed = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=920,
        body=body,
        evidence_bytes=photo,
        r2_put=put,
    )
    assert closed["visit"]["status"] == "not_delivered"
    assert closed["gps_deferred"]["verified"] is False
    assert closed["gps_deferred"]["closure_review_status"] == "pending_review"
    assert closed["evidence_object_key"] in store
    results.append(closed)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=920"
    )
    assert int(cur.fetchone()["n"]) == 5
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_offline_ops
        WHERE dispatch_plan_id=920 AND status='completed'
        """
    )
    assert int(cur.fetchone()["n"]) == 5
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_collections c
        JOIN distribuidora.delivery_visits v ON v.id = c.visit_id
        WHERE v.dispatch_plan_id=920
        """
    )
    assert int(cur.fetchone()["n"]) == 4  # solo completes
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_evidence e
        JOIN distribuidora.delivery_visits v ON v.id = e.visit_id
        WHERE v.dispatch_plan_id=920 AND e.upload_confirmed
        """
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_visit_events e
        JOIN distribuidora.delivery_visits v ON v.id = e.visit_id
        WHERE v.dispatch_plan_id=920 AND e.action='visit_finalized'
        """
    )
    assert int(cur.fetchone()["n"]) == 5
    # Sin cobros en local cerrado
    vid_closed = closed["visit"]["id"]
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_collections WHERE visit_id=%s",
        (vid_closed,),
    )
    assert int(cur.fetchone()["n"]) == 0
    cur.close()
    conn.close()


def test_lost_http_replay_same_key(lab_db):
    client = CLIENTS[0]
    body = _complete_body("lab-lost-http", client)
    first = offline_svc.sync_offline_operation(
        user=_user(), plan_id=920, body=body, r2_put=lambda *a, **k: ""
    )
    again = offline_svc.sync_offline_operation(
        user=_user(), plan_id=920, body=body, r2_put=lambda *a, **k: ""
    )
    assert again["idempotent_replay"] is True
    assert again["visit"]["id"] == first["visit"]["id"]
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=920"
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.execute(
        """
        SELECT COUNT(*) AS n FROM distribuidora.delivery_visit_events
        WHERE visit_id=%s AND action='visit_finalized'
        """,
        (first["visit"]["id"],),
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()


def test_concurrent_sync_same_key_one_visit(lab_db):
    body = _complete_body("lab-concurrent", CLIENTS[1])

    def run():
        return offline_svc.sync_offline_operation(
            user=_user(), plan_id=920, body=body, r2_put=lambda *a, **k: ""
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(run), pool.submit(run)]
        outs = []
        for f in futs:
            try:
                outs.append(f.result())
            except Exception:
                pass
    assert outs
    ids = {o["visit"]["id"] for o in outs}
    assert len(ids) == 1
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=920"
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()


def test_conflicts_preserve_local_semantics(lab_db):
    # Primera OK
    offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=920,
        body=_complete_body("lab-conf-a", CLIENTS[2]),
        r2_put=lambda *a, **k: "",
    )
    # Dos claves mismo cliente
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=920,
            body=_complete_body("lab-conf-b", CLIENTS[2]),
            r2_put=lambda *a, **k: "",
        )
    assert ei.value.status_code == 409

    # Misma clave payload distinto
    body = _complete_body("lab-mismatch", CLIENTS[3])
    offline_svc.sync_offline_operation(
        user=_user(), plan_id=920, body=body, r2_put=lambda *a, **k: ""
    )
    bad = dict(body)
    bad["result"] = {"status": "completed", "note": "OTRO"}
    with pytest.raises(HTTPException) as ei2:
        offline_svc.sync_offline_operation(
            user=_user(), plan_id=920, body=bad, r2_put=lambda *a, **k: ""
        )
    assert ei2.value.status_code == 409
    assert ei2.value.detail["code"] == offline_svc.ERR_IDEMPOTENCY_PAYLOAD_MISMATCH

    # Picking cambiado
    def stale(plan_id, key):
        ctx = offline_svc._lab_context_for_tests(plan_id, key)
        ctx = dict(ctx)
        ctx["picking_version"] = "999"
        return ctx

    with pytest.raises(HTTPException) as ei3:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=920,
            body=_complete_body("lab-pick-chg", CLIENTS[4]),
            r2_put=lambda *a, **k: "",
            context_fetcher=stale,
        )
    assert ei3.value.detail["code"] == offline_svc.ERR_PICKING_CHANGED


def test_r2_fail_keeps_retryable_and_photo_ack(lab_db):
    store, put = _r2_store()
    calls = {"n": 0}
    photo = b"\xff\xd8\xff" + b"RETRY" * 20
    body = _closed_body("lab-r2-fail", CLIENTS[0])
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()

    def flaky(key, data, ctype):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("R2 down lab")
        store[key] = data
        return key

    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=920,
            body=body,
            evidence_bytes=photo,
            r2_put=flaky,
        )
    assert ei.value.status_code == 503
    out = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=920,
        body=body,
        evidence_bytes=photo,
        r2_put=flaky,
    )
    assert out["visit"]["status"] == "not_delivered"
    assert out["evidence_object_key"] in store


def test_gps_deferred_pending_review_not_verified(lab_db):
    photo = b"\xff\xd8\xff" + b"GPS" * 30
    body = _closed_body("lab-gps", CLIENTS[1])
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()
    out = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=920,
        body=body,
        evidence_bytes=photo,
        r2_put=lambda *a, **k: a[0],
    )
    assert out["gps_deferred"]["verified"] is False
    assert out["gps_deferred"]["closure_review_status"] == "pending_review"
    assert out["visit"].get("closure_review_status") in (
        "pending_review",
        "verified",
        None,
    ) or True
    # Preferimos pending_review en cierre con excepción diferida
    assert out["visit"].get("closure_review_status") == "pending_review"


def test_unauthorized_and_flag_docs(lab_db, monkeypatch):
    monkeypatch.setattr(
        offline_svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda *_a, **_k: (_ for _ in ()).throw(HTTPException(status_code=403, detail="no")),
    )
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(99),
            plan_id=920,
            body=_complete_body("lab-unauth", CLIENTS[0]),
            r2_put=lambda *a, **k: "",
        )
    assert ei.value.status_code == 403
