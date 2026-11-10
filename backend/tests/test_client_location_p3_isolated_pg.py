"""P3: flujo GPS/foto/revisión en Postgres aislado (nunca analytics).

Requiere SIMPLE_PLANNING_TEST_DSN.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from fastapi import HTTPException

from backend.repositories.distribuidora import client_location_repo as loc_repo
from backend.services.distribuidora import client_location_service as svc

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
ROOT = Path(__file__).resolve().parents[2]
SQL_055 = ROOT / "backend" / "sql" / "distribuidora" / "055_client_location_photos.sql"

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

    _assert_isolated(DSN)
    return psycopg2.connect(DSN)


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
        "client_location_audit",
        "client_verified_locations",
        "client_location_proposals",
        "client_facade_photos",
        "dispatch_plan_order_events",
        "dispatch_plan",
    ):
        cur.execute(f"DROP TABLE IF EXISTS distribuidora.{t} CASCADE")
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'confirmed',
            planning_code TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_events (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL REFERENCES distribuidora.dispatch_plan(id),
            action TEXT NOT NULL,
            user_name TEXT,
            reason TEXT,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    conn.commit()
    raw = SQL_055.read_text(encoding="utf-8")
    for stmt in _split_sql(raw):
        cur.execute(stmt)
    conn.commit()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan (id, planning_code) VALUES (1, 'PLAN-TEST')"
    )
    conn.commit()
    cur.close()


@pytest.fixture()
def db(monkeypatch):
    conn = _connect()
    _bootstrap(conn)
    monkeypatch.setattr(svc, "get_connection", _connect)
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda u, p: 1,
    )
    # company_id dinámico del contexto (no literal 3)
    monkeypatch.setattr(
        svc,
        "_authorized_company_id_for_plan",
        lambda u, p: 7,
    )
    monkeypatch.setattr(
        svc,
        "_resolve_client",
        lambda plan_id, key: {
            "client_id": 1001,
            "customer_key": key,
            "latitude": -41.4700,
            "longitude": -72.9400,
            "coordinates_source": "bsale.clients.lat/lon",
            "has_coordinates": True,
        },
    )
    # insert_order_event path uses events_repo — ensure it works against our stub table
    from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as events_repo

    def _insert_order_event(cur, *, plan_id, action, user_name=None, reason=None, payload=None):
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_order_events
                (dispatch_plan_id, action, user_name, reason, payload)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            """,
            (
                int(plan_id),
                action,
                user_name,
                reason,
                __import__("json").dumps(payload or {}),
            ),
        )
        return 1

    monkeypatch.setattr(events_repo, "insert_order_event", _insert_order_event)
    yield conn
    conn.close()


def test_propose_pending_and_self_review_does_not_mutate(db):
    out = svc.propose_location(
        user={"id": 7, "email": "chofer@x.cl"},
        plan_id=1,
        customer_key="id:1001",
        latitude=-41.4705,
        longitude=-72.9401,
        accuracy_m=12,
        confirm_low_accuracy=True,
        confirm_significant_move=True,
        idempotency_key="gps-1",
    )
    assert out["idempotent"] is False
    pid = int(out["proposal"]["id"])
    assert out["proposal"]["status"] == "pending"

    with pytest.raises(HTTPException) as ei:
        svc.review_proposal_erp(
            user={"id": 7, "email": "chofer@x.cl"},
            proposal_id=pid,
            approve=True,
        )
    assert ei.value.status_code == 403

    cur = db.cursor()
    prop = loc_repo.get_proposal(cur, pid)
    assert prop["status"] == "pending"
    assert prop["reviewed_by_user_id"] is None
    cur.execute(
        """
        SELECT action FROM distribuidora.client_location_audit
        WHERE entity_id = %s AND action = 'self_review_denied'
        """,
        (pid,),
    )
    assert cur.fetchone() is not None
    cur.execute("SELECT COUNT(*) FROM distribuidora.client_verified_locations")
    assert cur.fetchone()[0] == 0
    cur.close()


def test_authorized_approve_then_reuse_verified(db):
    out = svc.propose_location(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        latitude=-41.4710,
        longitude=-72.9410,
        accuracy_m=8,
        confirm_low_accuracy=True,
        confirm_significant_move=True,
    )
    pid = int(out["proposal"]["id"])
    approved = svc.review_proposal_erp(
        user={"id": 99, "email": "ops@x.cl"},
        proposal_id=pid,
        approve=True,
        note="ok",
    )
    assert approved["proposal"]["status"] == "approved"

    cur = db.cursor()
    ver = loc_repo.get_verified_location(cur, 1001)
    assert ver is not None
    assert abs(float(ver["latitude"]) - (-41.4710)) < 1e-6
    assert int(ver["company_id"]) == 7
    cur.close()

    enriched = svc.enrich_client_location_fields(
        {
            "client_id": 1001,
            "latitude": -41.47,
            "longitude": -72.94,
            "has_coordinates": True,
            "coordinates_source": "bsale.clients.lat/lon",
        }
    )
    assert enriched["coordinates_source"] == "client_verified_locations"
    assert enriched["location_registration"]["verified"] is True
    assert abs(float(enriched["latitude"]) - (-41.4710)) < 1e-6


def test_authorized_reject_and_concurrent_conflict(db):
    a = svc.propose_location(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        latitude=-41.472,
        longitude=-72.942,
        accuracy_m=10,
        confirm_low_accuracy=True,
        confirm_significant_move=True,
    )
    pid = int(a["proposal"]["id"])
    svc.review_proposal_erp(user={"id": 50}, proposal_id=pid, approve=False, note="malo")
    with pytest.raises(HTTPException) as ei:
        svc.review_proposal_erp(user={"id": 51}, proposal_id=pid, approve=True)
    assert ei.value.status_code == 409


def test_r2_disabled_blocks_photo(db, monkeypatch):
    from backend.services import object_storage_service as storage

    for k in ("R2_ENDPOINT", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY", "R2_BUCKET"):
        monkeypatch.delenv(k, raising=False)
    assert storage.is_r2_configured() is False
    with pytest.raises(HTTPException) as ei:
        svc.upload_facade_bytes(
            user={"id": 7},
            plan_id=1,
            customer_key="id:1001",
            content_type="image/jpeg",
            data=b"\xff\xd8\xff" + b"\x00" * 64,
        )
    assert ei.value.status_code == 503
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM distribuidora.client_facade_photos")
    assert cur.fetchone()[0] == 0
    cur.close()


def _tiny_jpeg() -> bytes:
    from io import BytesIO

    from PIL import Image

    img = Image.new("RGB", (32, 24), color=(20, 40, 60))
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def test_confirm_missing_object_no_confirm(db, monkeypatch):
    from backend.services import object_storage_service as storage

    monkeypatch.setattr(storage, "is_r2_configured", lambda: True)

    def _boom(*_a, **_k):
        raise storage.ObjectStorageError("NoSuchKey")

    monkeypatch.setattr(storage, "get_object_bytes", _boom)
    cur = db.cursor()
    photo = loc_repo.insert_facade_photo(
        cur,
        client_id=1001,
        company_id=7,
        dispatch_plan_id=1,
        customer_key="id:1001",
        object_key="operaciones/clientes/7/1001/fachadas/deadbeef.jpg",
        content_type="image/jpeg",
        size_bytes=None,
        sha256=None,
        width=None,
        height=None,
        captured_by_user_id=7,
        validation_status="upload_pending",
        upload_confirmed=False,
        idempotency_key="p-miss",
    )
    db.commit()
    photo_id = int(photo["id"])
    with pytest.raises(HTTPException) as ei:
        svc.confirm_facade_upload(
            user={"id": 7},
            plan_id=1,
            customer_key="id:1001",
            photo_id=photo_id,
        )
    assert ei.value.status_code == 400
    again = loc_repo.get_facade_photo(cur, photo_id)
    assert again["upload_confirmed"] is False
    assert again["validation_status"] == "upload_pending"
    cur.close()


def test_confirm_other_client_forbidden(db, monkeypatch):
    from backend.services import object_storage_service as storage

    monkeypatch.setattr(storage, "is_r2_configured", lambda: True)
    cur = db.cursor()
    photo = loc_repo.insert_facade_photo(
        cur,
        client_id=9999,
        company_id=7,
        dispatch_plan_id=1,
        customer_key="id:9999",
        object_key="operaciones/clientes/7/9999/fachadas/x.jpg",
        content_type="image/jpeg",
        size_bytes=10,
        sha256=None,
        width=None,
        height=None,
        captured_by_user_id=7,
        validation_status="upload_pending",
        upload_confirmed=False,
        idempotency_key="p-other",
    )
    db.commit()
    with pytest.raises(HTTPException) as ei:
        svc.confirm_facade_upload(
            user={"id": 7},
            plan_id=1,
            customer_key="id:1001",
            photo_id=int(photo["id"]),
        )
    assert ei.value.status_code == 404
    cur.close()


def test_confirm_rejects_fake_content_without_confirming(db, monkeypatch):
    """Bypass prepare+PUT basura: confirm debe fallar y dejar upload_confirmed=false."""
    from backend.services import object_storage_service as storage

    monkeypatch.setattr(storage, "is_r2_configured", lambda: True)
    monkeypatch.setattr(
        storage,
        "get_object_bytes",
        lambda *_a, **_k: (b"not-a-real-image-payload", "image/jpeg", 24),
    )
    put_calls: list[str] = []
    monkeypatch.setattr(
        storage,
        "put_bytes",
        lambda **kw: put_calls.append(kw["object_key"]) or kw,
    )
    cur = db.cursor()
    photo = loc_repo.insert_facade_photo(
        cur,
        client_id=1001,
        company_id=7,
        dispatch_plan_id=1,
        customer_key="id:1001",
        object_key="operaciones/clientes/7/1001/fachadas/fake.jpg",
        content_type="image/jpeg",
        size_bytes=24,
        sha256=None,
        width=None,
        height=None,
        captured_by_user_id=7,
        validation_status="upload_pending",
        upload_confirmed=False,
        idempotency_key="p-fake",
    )
    db.commit()
    photo_id = int(photo["id"])
    with pytest.raises(HTTPException) as ei:
        svc.confirm_facade_upload(
            user={"id": 7},
            plan_id=1,
            customer_key="id:1001",
            photo_id=photo_id,
        )
    assert ei.value.status_code == 400
    assert put_calls == []
    again = loc_repo.get_facade_photo(cur, photo_id)
    assert again["upload_confirmed"] is False
    cur.execute(
        """
        SELECT action FROM distribuidora.client_location_audit
        WHERE entity_id = %s AND action = 'confirm_rejected_invalid_content'
        """,
        (photo_id,),
    )
    assert cur.fetchone() is not None
    cur.close()


def test_confirm_validates_rewrites_and_is_idempotent(db, monkeypatch):
    from backend.services import object_storage_service as storage

    jpeg = _tiny_jpeg()
    store = {"bytes": jpeg}
    monkeypatch.setattr(storage, "is_r2_configured", lambda: True)
    monkeypatch.setattr(
        storage,
        "get_object_bytes",
        lambda *_a, **_k: (store["bytes"], "image/jpeg", len(store["bytes"])),
    )

    def _put(**kw):
        store["bytes"] = kw["body"]
        return {"object_key": kw["object_key"], "size": len(kw["body"])}

    monkeypatch.setattr(storage, "put_bytes", _put)
    monkeypatch.setattr(storage, "object_exists", lambda *_a, **_k: True)

    cur = db.cursor()
    photo = loc_repo.insert_facade_photo(
        cur,
        client_id=1001,
        company_id=7,
        dispatch_plan_id=1,
        customer_key="id:1001",
        object_key="operaciones/clientes/7/1001/fachadas/ok.jpg",
        content_type="image/jpeg",
        size_bytes=len(jpeg),
        sha256=None,
        width=None,
        height=None,
        captured_by_user_id=7,
        validation_status="upload_pending",
        upload_confirmed=False,
        idempotency_key="p-ok",
    )
    db.commit()
    photo_id = int(photo["id"])
    out = svc.confirm_facade_upload(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        photo_id=photo_id,
    )
    assert out["idempotent"] is False
    assert out["photo"]["upload_confirmed"] is True
    assert out["photo"]["sha256"]
    assert out["photo"]["content_type"] == "image/jpeg"

    again = svc.confirm_facade_upload(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        photo_id=photo_id,
    )
    assert again["idempotent"] is True
    assert again["photo"]["upload_confirmed"] is True
    cur.close()


def test_confirm_rejects_object_key_for_other_client_prefix(db, monkeypatch):
    from backend.services import object_storage_service as storage

    monkeypatch.setattr(storage, "is_r2_configured", lambda: True)
    cur = db.cursor()
    photo = loc_repo.insert_facade_photo(
        cur,
        client_id=1001,
        company_id=7,
        dispatch_plan_id=1,
        customer_key="id:1001",
        object_key="operaciones/clientes/7/9999/fachadas/hijack.jpg",
        content_type="image/jpeg",
        size_bytes=10,
        sha256=None,
        width=None,
        height=None,
        captured_by_user_id=7,
        validation_status="upload_pending",
        upload_confirmed=False,
        idempotency_key="p-hijack",
    )
    db.commit()
    with pytest.raises(HTTPException) as ei:
        svc.confirm_facade_upload(
            user={"id": 7},
            plan_id=1,
            customer_key="id:1001",
            photo_id=int(photo["id"]),
        )
    assert ei.value.status_code == 400
    again = loc_repo.get_facade_photo(cur, int(photo["id"]))
    assert again["upload_confirmed"] is False
    cur.close()


def test_confirm_rejects_other_company_id(db, monkeypatch):
    from backend.services import object_storage_service as storage

    monkeypatch.setattr(storage, "is_r2_configured", lambda: True)
    cur = db.cursor()
    photo = loc_repo.insert_facade_photo(
        cur,
        client_id=1001,
        company_id=3,  # distinta a company autorizada (=7)
        dispatch_plan_id=1,
        customer_key="id:1001",
        object_key="operaciones/clientes/3/1001/fachadas/cross.jpg",
        content_type="image/jpeg",
        size_bytes=10,
        sha256=None,
        width=None,
        height=None,
        captured_by_user_id=7,
        validation_status="upload_pending",
        upload_confirmed=False,
        idempotency_key="p-cross-co",
    )
    db.commit()
    with pytest.raises(HTTPException) as ei:
        svc.confirm_facade_upload(
            user={"id": 7},
            plan_id=1,
            customer_key="id:1001",
            photo_id=int(photo["id"]),
        )
    assert ei.value.status_code == 403
    again = loc_repo.get_facade_photo(cur, int(photo["id"]))
    assert again["upload_confirmed"] is False
    cur.close()


def test_propose_persists_authorized_company_id(db):
    out = svc.propose_location(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        latitude=-41.473,
        longitude=-72.943,
        accuracy_m=9,
        confirm_low_accuracy=True,
        confirm_significant_move=True,
    )
    assert int(out["proposal"]["company_id"]) == 7
    cur = db.cursor()
    ver_before = loc_repo.get_verified_location(cur, 1001)
    assert ver_before is None
    cur.close()
    approved = svc.review_proposal_erp(
        user={"id": 99},
        proposal_id=int(out["proposal"]["id"]),
        approve=True,
    )
    assert approved["proposal"]["status"] == "approved"
    cur = db.cursor()
    ver = loc_repo.get_verified_location(cur, 1001)
    assert int(ver["company_id"]) == 7
    cur.close()


def test_name_key_blocked(monkeypatch, db):
    monkeypatch.setattr(
        svc,
        "_resolve_client",
        lambda plan_id, key: {
            "client_id": None,
            "customer_key": key,
            "has_coordinates": False,
        },
    )
    with pytest.raises(HTTPException) as ei:
        svc.propose_location(
            user={"id": 7},
            plan_id=1,
            customer_key="name:sin-id",
            latitude=-41.47,
            longitude=-72.94,
            accuracy_m=10,
            confirm_low_accuracy=True,
            confirm_significant_move=True,
        )
    assert ei.value.status_code == 422


def test_image_magic_rejected(monkeypatch):
    with pytest.raises(HTTPException) as ei:
        svc._normalize_facade_image(b"not-an-image", "image/jpeg")
    assert ei.value.status_code == 400


def test_low_accuracy_requires_confirm_then_flags(db):
    out = svc.propose_location(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        latitude=-41.4702,
        longitude=-72.9402,
        accuracy_m=120,
        confirm_low_accuracy=False,
        confirm_significant_move=True,
    )
    assert out["requires_confirmation"] is True
    assert out["reason"] == "low_accuracy"

    saved = svc.propose_location(
        user={"id": 7},
        plan_id=1,
        customer_key="id:1001",
        latitude=-41.4702,
        longitude=-72.9402,
        accuracy_m=120,
        confirm_low_accuracy=True,
        confirm_significant_move=True,
    )
    assert saved["proposal"]["status"] == "pending"
    enriched = svc.enrich_client_location_fields(
        {
            "client_id": 1001,
            "has_coordinates": True,
            "latitude": -41.47,
            "longitude": -72.94,
        }
    )
    assert enriched["location_registration"]["low_accuracy"] is True
