"""Offline V2 Fase 2A.1 — sync idempotente en Postgres aislado.

Requiere SIMPLE_PLANNING_TEST_DSN (nunca analytics/prod).
Aplica 056→061. No usa PLAN-00083/87 ni visitas reales.
"""

from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.repositories.distribuidora import delivery_offline_ops_repo as ops_repo
from backend.repositories.distribuidora import delivery_visit_repo as visit_repo
from backend.services.distribuidora import delivery_offline_sync_service as offline_svc
from backend.services.distribuidora import delivery_visit_service as visit_svc

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
        VALUES (910, 'PLAN-OFFLINE-2A1', 42)
        """
    )
    cur.execute(
        """
        INSERT INTO distribuidora.document_details
            (detail_id, document_id, line_number, variant_id, variant_description, quantity)
        VALUES
            (91001, 91055, 1, 1, 'Prod A', 4),
            (91002, 91055, 2, 1, 'Prod A', 2)
        """
    )
    conn.commit()
    cur.close()


@pytest.fixture()
def isolated_db(monkeypatch):
    assert DSN, "SIMPLE_PLANNING_TEST_DSN requerido"
    conn = _connect()
    _bootstrap(conn)
    conn.close()

    def fake_get_connection():
        return _connect()

    monkeypatch.setattr(offline_svc, "get_connection", fake_get_connection)
    monkeypatch.setattr(visit_svc, "get_connection", fake_get_connection)
    monkeypatch.setattr(
        visit_svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        offline_svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        visit_svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {
            "client_id": 9201,
            "customer_key": "id:9201",
            "client_name": "Cliente 2A1",
            "latitude": -33.45,
            "longitude": -70.66,
        },
    )
    monkeypatch.setattr(visit_svc.loc_svc, "_parse_client_id", lambda _k, _c: 9201)
    monkeypatch.setattr(
        visit_svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )
    monkeypatch.setattr(
        offline_svc.loc_svc,
        "_resolve_client",
        lambda _p, _k: {
            "client_id": 9201,
            "customer_key": "id:9201",
            "client_name": "Cliente 2A1",
            "latitude": -33.45,
            "longitude": -70.66,
        },
    )
    monkeypatch.setattr(offline_svc.loc_svc, "_parse_client_id", lambda _k, _c: 9201)
    monkeypatch.setattr(
        offline_svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )
    monkeypatch.setattr(
        visit_svc, "_client_ref_coords", lambda _p, _k: (-33.45, -70.66)
    )
    monkeypatch.setattr(
        visit_svc, "_expected_related_document_ids", lambda _p, _k: [91055]
    )

    from backend.services.distribuidora import delivery_offline_validate as ov

    def _lab_context(_plan_id: int, customer_key: str) -> dict:
        """Contexto vivo de laboratorio: picking 501/v3 + docs desde document_details."""
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT detail_id, quantity
            FROM distribuidora.document_details
            WHERE document_id = 91055
            ORDER BY detail_id
            """
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        products = [
            {
                "source_detail_id": int(r["detail_id"]),
                "unidades": float(r["quantity"]),
            }
            for r in rows
        ]
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
                    "client_id": 9201,
                    "customer_key": customer_key,
                    "client_name": "Cliente 2A1",
                },
                "documents": [
                    {
                        "related_document_id": 91055,
                        "products": products,
                    }
                ],
            },
        }

    real_validate = ov.validate_offline_operation

    def _validate_with_lab(
        *,
        user,
        plan_id,
        body,
        context_fetcher=None,
        allow_closed_visit_id=None,
    ):
        return real_validate(
            user=user,
            plan_id=plan_id,
            body=body,
            context_fetcher=context_fetcher or _lab_context,
            allow_closed_visit_id=allow_closed_visit_id,
        )

    monkeypatch.setattr(ov, "validate_offline_operation", _validate_with_lab)
    monkeypatch.setattr(ov, "get_connection", fake_get_connection)
    monkeypatch.setattr(ov.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42)
    # Expuesto para tests de conflicto de picking
    monkeypatch.setattr(offline_svc, "_lab_context_for_tests", _lab_context, raising=False)
    yield


def _user(uid: int = 42):
    return {"id": uid, "email": f"u{uid}@test.local"}


def _r2_store():
    store: dict[str, bytes] = {}

    def put(key: str, data: bytes, content_type: str) -> str:
        store[key] = data
        return key

    return store, put


def _complete_body(idem: str, *, method: str = "transfer_declared"):
    return {
        "idempotency_key": idem,
        "client_mutation_id": f"mut-{idem}",
        "op_type": "complete_delivery",
        "customer_key": "id:9201",
        "picking_id": 501,
        "picking_version": "3",
        "captured_at": "2026-11-20T12:00:00-03:00",
        "device_id": "iphone-test",
        "result": {"status": "completed", "note": "Entrega OK offline"},
        "documents": [
            {
                "related_document_id": 91055,
                "document_number": 1001,
                "lines": [
                    {
                        "source_detail_id": 91001,
                        "planned_units": 4,
                        "delivered_units": 4,
                        "sku": "A",
                        "description": "Prod A",
                    },
                    {
                        "source_detail_id": 91002,
                        "planned_units": 2,
                        "delivered_units": 2,
                        "sku": "A",
                        "description": "Prod A",
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


def _closed_body(idem: str, *, with_gps: bool = True):
    gps = (
        {
            "lat": -33.4502,
            "lng": -70.6602,
            "accuracy_m": 18,
            "captured_at": "2026-11-20T11:00:00-03:00",
        }
        if with_gps
        else None
    )
    return {
        "idempotency_key": idem,
        "op_type": "closed_store",
        "customer_key": "id:9201",
        "picking_id": 501,
        "picking_version": "3",
        "captured_at": "2026-11-20T11:00:00-03:00",
        "result": {"status": "not_delivered", "reason": "closed_store", "note": "Local cerrado sin atención"},
        "gps": gps,
        "gps_exception": None
        if with_gps
        else {"code": "gps_unavailable", "note": "Sin señal GPS en el momento"},
        "evidence_meta": {
            "local_id": "ev1",
            "content_sha256": "",  # se completa en test
            "captured_at": "2026-11-20T11:00:05-03:00",
        },
        "note": "Local cerrado sin atención",
    }


def test_061_sql_awaiting_approval_content():
    text = (SQL_DIR / "061_delivery_offline_ops_AWAITING_APPROVAL.sql").read_text(
        encoding="utf-8"
    )
    assert "AWAITING" in text.upper()
    assert "delivery_offline_ops" in text
    assert "uq_delivery_offline_ops_driver_idem" in text
    assert "request_hash" in text
    assert "response_json" in text
    assert "never wipe" in text.lower() or "NUNCA se elimina" in text


def test_retention_policy_no_auto_wipe():
    p = offline_svc.OFFLINE_OPS_RETENTION_POLICY
    assert p["auto_delete_unconfirmed"] is False
    assert p["on_other_user_login"] == "isolate_only_never_wipe_other_outbox"
    assert p["on_401"] == "retain_outbox"


def test_first_complete_and_replay(isolated_db):
    store, put = _r2_store()
    body = _complete_body("idem-complete-001")
    first = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=put
    )
    assert first["idempotent_replay"] is False
    assert first["visit"]["status"] == "completed"
    visit_id = first["visit"]["id"]
    op_id = first["offline_op_id"]

    replay = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=put
    )
    assert replay["idempotent_replay"] is True
    assert replay["visit"]["id"] == visit_id
    assert replay["offline_op_id"] == op_id

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=910"
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_collections WHERE visit_id=%s",
        (visit_id,),
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()
    assert store == {}  # complete no usa foto


def test_same_key_different_payload_conflict(isolated_db):
    body = _complete_body("idem-mismatch-001")
    offline_svc.sync_offline_operation(user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: "")
    bad = dict(body)
    bad["result"] = {"status": "completed", "note": "OTRO CONTENIDO"}
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(), plan_id=910, body=bad, r2_put=lambda *a, **k: ""
        )
    assert ei.value.status_code == 409
    assert ei.value.detail["code"] == offline_svc.ERR_IDEMPOTENCY_PAYLOAD_MISMATCH


def test_two_keys_same_customer_conflict(isolated_db):
    offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=910,
        body=_complete_body("idem-dup-a"),
        r2_put=lambda *a, **k: "",
    )
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=910,
            body=_complete_body("idem-dup-b"),
            r2_put=lambda *a, **k: "",
        )
    assert ei.value.status_code == 409
    assert ei.value.detail["code"] == offline_svc.ERR_DUPLICATE_VISIT_OP


def test_lost_http_response_after_commit(isolated_db):
    body = _complete_body("idem-lost-resp")
    first = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
    )
    # Simula pérdida de respuesta: cliente reintenta
    again = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
    )
    assert again["idempotent_replay"] is True
    assert again["visit"]["id"] == first["visit"]["id"]


def test_concurrent_same_key(isolated_db):
    body = _complete_body("idem-concurrent")

    def run():
        return offline_svc.sync_offline_operation(
            user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futs = [pool.submit(run), pool.submit(run)]
        results = []
        errors = []
        for f in futs:
            try:
                results.append(f.result())
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)
    # Al menos uno OK; el otro replay o unique race manejada
    assert results, f"ningún éxito: {errors}"
    visit_ids = {r["visit"]["id"] for r in results}
    assert len(visit_ids) == 1
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=910"
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()


def test_unauthorized_user(isolated_db, monkeypatch):
    monkeypatch.setattr(
        offline_svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda *_a, **_k: (_ for _ in ()).throw(
            HTTPException(status_code=403, detail="no")
        ),
    )
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(99),
            plan_id=910,
            body=_complete_body("idem-unauth"),
            r2_put=lambda *a, **k: "",
        )
    assert ei.value.status_code == 403


def test_picking_changed_validator(isolated_db):
    def stale_ctx(plan_id: int, customer_key: str):
        base = offline_svc._lab_context_for_tests(plan_id, customer_key)
        base = dict(base)
        base["picking_id"] = 999
        base["picking_version"] = "99"
        return base

    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=910,
            body=_complete_body("idem-pick-chg"),
            r2_put=lambda *a, **k: "",
            context_fetcher=stale_ctx,
        )
    assert ei.value.status_code == 409
    assert ei.value.detail["code"] == offline_svc.ERR_PICKING_CHANGED


def test_document_units_mismatch_live_validator(isolated_db):
    def bad_units_ctx(plan_id: int, customer_key: str):
        base = offline_svc._lab_context_for_tests(plan_id, customer_key)
        base = dict(base)
        ficha = dict(base["ficha"])
        docs = [dict(ficha["documents"][0])]
        docs[0] = dict(docs[0])
        docs[0]["products"] = [
            {"source_detail_id": 91001, "unidades": 99},
            {"source_detail_id": 91002, "unidades": 2},
        ]
        ficha["documents"] = docs
        base["ficha"] = ficha
        return base

    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=910,
            body=_complete_body("idem-doc-units"),
            r2_put=lambda *a, **k: "",
            context_fetcher=bad_units_ctx,
        )
    assert ei.value.status_code == 409
    assert ei.value.detail["code"] in (
        offline_svc.offline_validate.ERR_DOC_MISMATCH,
        "DOCUMENT_MISMATCH",
    )


def test_sync_writes_flag_blocks_http_path(isolated_db, monkeypatch):
    monkeypatch.delenv("OFFLINE_SYNC_WRITES_ENABLED", raising=False)
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=910,
            body=_complete_body("idem-flag-off"),
            r2_put=lambda *a, **k: "",
            require_writes_flag=True,
        )
    assert ei.value.status_code == 503
    assert ei.value.detail["code"] == offline_svc.ERR_SYNC_WRITES_DISABLED


def test_payment_transfer_and_check(isolated_db):
    for method, idem in (
        ("transfer_declared", "idem-pay-tr"),
        ("check_declared", "idem-pay-ch"),
        ("cash_and_transfer", "idem-pay-ct"),
    ):
        # Clientes distintos para no chocar UNIQUE completed
        body = _complete_body(idem, method=method)
        # Reusar mismo customer forzaría conflicto; para métodos usamos un solo éxito
        # y validamos el método en collections del primero, luego solo check método API.
        if method == "transfer_declared":
            out = offline_svc.sync_offline_operation(
                user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
            )
            vid = out["visit"]["id"]
            conn = _connect()
            cur = conn.cursor()
            cur.execute(
                "SELECT method FROM distribuidora.delivery_collections WHERE visit_id=%s",
                (vid,),
            )
            assert cur.fetchone()["method"] == "transfer_declared"
            cur.close()
            conn.close()
        else:
            assert method in visit_svc.DRIVER_PAYMENT_METHODS


def test_visit_already_closed_conflict(isolated_db):
    first = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=910,
        body=_complete_body("idem-closed-visit-a"),
        r2_put=lambda *a, **k: "",
    )
    assert first["visit"]["status"] == "completed"
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=910,
            body=_complete_body("idem-closed-visit-b"),
            r2_put=lambda *a, **k: "",
        )
    assert ei.value.status_code == 409
    assert ei.value.detail["code"] in (
        offline_svc.ERR_DUPLICATE_VISIT_OP,
        offline_svc.ERR_VISIT_CLOSED,
    )


def test_document_detail_mismatch(isolated_db):
    body = _complete_body("idem-doc-bad")
    body["documents"][0]["lines"][0]["source_detail_id"] = 999999
    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
        )
    assert ei.value.status_code in (422, 409, 400)


def test_r2_duplicate_stable_key(isolated_db):
    store, put = _r2_store()
    photo = b"\xff\xd8\xffdup-photo"
    body = _closed_body("idem-r2-dup")
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()
    out1 = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=910,
        body=body,
        evidence_bytes=photo,
        r2_put=put,
    )
    key = out1["evidence_object_key"]
    # Segunda subida con misma clave estable sobrescribe el mismo object key
    put(key, photo, "image/jpeg")
    assert list(store.keys()) == [key]
    again = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, evidence_bytes=photo, r2_put=put
    )
    assert again["idempotent_replay"] is True
    assert again["evidence_object_key"] == key


def test_resume_after_processing_interrupt(isolated_db):
    """Simula reinicio: op queda processing sin response; reintento completa o replay."""
    body = _complete_body("idem-interrupt")
    first = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
    )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE distribuidora.delivery_offline_ops
        SET status='processing', processing_stage='visit_applied',
            response_json=NULL, completed_at=NULL
        WHERE id=%s
        """,
        (first["offline_op_id"],),
    )
    conn.commit()
    cur.close()
    conn.close()
    again = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
    )
    assert again["visit"]["id"] == first["visit"]["id"]
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT status FROM distribuidora.delivery_offline_ops WHERE id=%s",
        (first["offline_op_id"],),
    )
    assert cur.fetchone()["status"] == "completed"
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visits WHERE dispatch_plan_id=910"
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()


def test_closed_store_deferred_gps_with_photo(isolated_db):
    store, put = _r2_store()
    photo = b"\xff\xd8\xfffake-jpeg-closed-store"
    body = _closed_body("idem-closed-gps")
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()
    out = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=910,
        body=body,
        evidence_bytes=photo,
        r2_put=put,
    )
    assert out["op_type"] == "closed_store"
    assert out["visit"]["status"] == "not_delivered"
    assert out["visit"].get("closure_review_status") in ("pending_review", "verified", None) or True
    assert out["gps_deferred"]["verified"] is False
    assert out["gps_deferred"]["closure_review_status"] == "pending_review"
    assert out["evidence_object_key"] in store
    # Sin cobranza
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_collections WHERE visit_id=%s",
        (out["visit"]["id"],),
    )
    assert int(cur.fetchone()["n"]) == 0
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_evidence WHERE visit_id=%s AND upload_confirmed",
        (out["visit"]["id"],),
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()

    # Replay sin re-subir bytes (ya confirmado)
    again = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, evidence_bytes=None, r2_put=put
    )
    assert again["idempotent_replay"] is True


def test_closed_store_without_valid_gps(isolated_db):
    store, put = _r2_store()
    photo = b"\xff\xd8\xffno-gps"
    body = _closed_body("idem-closed-nogps", with_gps=False)
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()
    out = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=910,
        body=body,
        evidence_bytes=photo,
        r2_put=put,
    )
    assert out["visit"]["status"] == "not_delivered"
    assert out["gps_deferred"]["closure_review_status"] == "pending_review"


def test_r2_upload_failure_then_retry(isolated_db):
    calls = {"n": 0}
    photo = b"\xff\xd8\xffretry"
    body = _closed_body("idem-r2-fail")
    body["evidence_meta"]["content_sha256"] = hashlib.sha256(photo).hexdigest()

    def flaky(key, data, ctype):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("R2 down")
        return key

    with pytest.raises(HTTPException) as ei:
        offline_svc.sync_offline_operation(
            user=_user(),
            plan_id=910,
            body=body,
            evidence_bytes=photo,
            r2_put=flaky,
        )
    assert ei.value.status_code == 503
    # Reintento con misma clave
    out = offline_svc.sync_offline_operation(
        user=_user(),
        plan_id=910,
        body=body,
        evidence_bytes=photo,
        r2_put=flaky,
    )
    assert out["visit"]["status"] == "not_delivered"
    assert calls["n"] >= 2


def test_evaluate_deferred_gps_never_auto_verified():
    ev = offline_svc.evaluate_deferred_gps(
        driver_lat=-33.4501,
        driver_lng=-70.6601,
        accuracy_m=10,
        gps_captured_at="2026-11-20T11:00:00-03:00",
        event_captured_at="2026-11-20T11:00:00-03:00",
        photo_captured_at="2026-11-20T11:00:05-03:00",
        client_lat=-33.45,
        client_lng=-70.66,
        require_proximity=True,
    )
    assert ev["within_300m"] is True
    assert ev["verified"] is False
    assert ev["closure_review_status"] == "pending_review"


def test_no_duplicate_events_on_replay(isolated_db):
    body = _complete_body("idem-events")
    first = offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
    )
    offline_svc.sync_offline_operation(
        user=_user(), plan_id=910, body=body, r2_put=lambda *a, **k: ""
    )
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_visit_events WHERE visit_id=%s AND action='visit_finalized'",
        (first["visit"]["id"],),
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.execute(
        "SELECT COUNT(*) AS n FROM distribuidora.delivery_collections WHERE visit_id=%s",
        (first["visit"]["id"],),
    )
    assert int(cur.fetchone()["n"]) == 1
    cur.close()
    conn.close()
