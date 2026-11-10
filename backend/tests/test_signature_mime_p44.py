"""P4.4: coherencia MIME declarado vs bytes de firma (canvas JPEG → R2)."""

from __future__ import annotations

import io

import pytest
from fastapi import HTTPException
from PIL import Image

from backend.services.distribuidora import client_location_service as loc_svc


def _jpeg_bytes(*, color=(12, 34, 56)) -> bytes:
    img = Image.new("RGB", (64, 32), color)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=90)
    data = buf.getvalue()
    assert data[:3] == b"\xff\xd8\xff"
    return data


def _png_bytes() -> bytes:
    img = Image.new("RGB", (48, 24), (200, 10, 10))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    data = buf.getvalue()
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    return data


def test_normalize_jpeg_with_matching_claim():
    raw = _jpeg_bytes()
    out, mime, meta = loc_svc._normalize_facade_image(raw, "image/jpeg")
    assert mime == "image/jpeg"
    assert out[:3] == b"\xff\xd8\xff"
    assert meta.get("normalized_mime") == "image/jpeg"
    assert meta.get("detected_mime") == "image/jpeg"


def test_normalize_png_reencodes_to_jpeg_with_matching_claim():
    raw = _png_bytes()
    out, mime, meta = loc_svc._normalize_facade_image(raw, "image/png")
    assert mime == "image/jpeg"
    assert out[:3] == b"\xff\xd8\xff"
    assert meta.get("detected_mime") == "image/png"
    assert meta.get("normalized_mime") == "image/jpeg"


def test_normalize_rejects_png_claim_on_jpeg_bytes():
    """Causa raíz P4.4: claimed image/png + bytes JPEG del canvas."""
    raw = _jpeg_bytes()
    with pytest.raises(HTTPException) as ei:
        loc_svc._normalize_facade_image(raw, "image/png")
    assert ei.value.status_code == 400
    detail = str(ei.value.detail)
    assert "image/png" in detail
    assert "image/jpeg" in detail


def test_normalize_rejects_jpeg_claim_on_png_bytes():
    raw = _png_bytes()
    with pytest.raises(HTTPException) as ei:
        loc_svc._normalize_facade_image(raw, "image/jpeg")
    assert ei.value.status_code == 400


def test_submit_conformity_passes_claimed_mime_to_normalizer(monkeypatch):
    """Router debe reenviar content_type del part; no hardcodear PNG."""
    from backend.services.distribuidora import delivery_visit_service as svc

    seen: dict = {}

    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 42
    )
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda _u, _p: 3
    )

    class _Cur:
        def close(self):
            return None

    class _Conn:
        def cursor(self):
            return _Cur()

        def commit(self):
            return None

        def close(self):
            return None

        def rollback(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: _Conn())
    monkeypatch.setattr(
        svc,
        "_require_own_open_visit",
        lambda *_a, **_k: {"id": 1, "version": 2, "status": "in_progress"},
    )
    monkeypatch.setattr(
        svc.visit_repo,
        "list_document_results",
        lambda *_a, **_k: [{"id": 9, "document_total": 100}],
    )
    monkeypatch.setattr(
        svc.visit_repo,
        "list_line_results",
        lambda *_a, **_k: [{"id": 1, "source_detail_id": 1}],
    )
    monkeypatch.setattr(
        svc.visit_repo, "list_active_collections", lambda *_a, **_k: []
    )
    monkeypatch.setattr(
        "backend.services.distribuidora.delivery_visit_closure.build_conformity_summary",
        lambda **_k: {"docs": 1, "lines": 1},
    )

    def _norm(data, claimed):
        seen["claimed"] = claimed
        seen["raw_len"] = len(data)
        return b"\xff\xd8\xffNORMALIZED", "image/jpeg", {"normalized_mime": "image/jpeg"}

    monkeypatch.setattr(svc.loc_svc, "_normalize_facade_image", _norm)

    put_args: dict = {}

    class _FakeStorage:
        ObjectStorageNotConfigured = type("ObjectStorageNotConfigured", (Exception,), {})

        @staticmethod
        def build_delivery_signature_object_key(**kw):
            seen["ext"] = kw.get("ext")
            return f"firmas/test.{kw.get('ext')}"

        @staticmethod
        def put_bytes(**kw):
            put_args.update(kw)
            return None

        @staticmethod
        def object_exists(_k):
            return True

    monkeypatch.setattr(svc, "storage", _FakeStorage)

    def _upsert(cur, **kw):
        seen["persisted_ctype"] = kw.get("signature_content_type")
        seen["object_key"] = kw.get("signature_object_key")
        return {"id": 1, "summary_payload": {}, "accepted_summary_sha256": "x"}

    monkeypatch.setattr(svc.visit_repo, "upsert_conformity", _upsert)
    monkeypatch.setattr(svc.visit_repo, "insert_event", lambda *_a, **_k: None)

    jpeg = _jpeg_bytes()
    out = svc.submit_conformity(
        user={"id": 42},
        plan_id=10,
        visit_id=99,
        receptor_name="Receptor",
        signature_png=jpeg,
        signature_content_type="image/jpeg",
        refused=False,
        refuse_reason=None,
        latitude=-42.0,
        longitude=-73.0,
        accuracy_m=5.0,
    )
    assert out["persisted"] is True
    assert seen["claimed"] == "image/jpeg"
    assert seen["ext"] == "jpg"
    assert seen["persisted_ctype"] == "image/jpeg"
    assert put_args.get("content_type") == "image/jpeg"
    assert put_args.get("body")[:3] == b"\xff\xd8\xff"
    assert str(seen["object_key"]).endswith(".jpg")
