"""P4.3: integración HTTP real multipart (boundary) + JWT.

Usa TestClient de Starlette: el cuerpo se serializa como multipart/form-data
con boundary real (no es un mock de Form fields ya parseados).
"""

from __future__ import annotations

import io

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.auth.jwt import create_staff_token
from backend.auth.multiapp import APP_PWA, MultiAppScope
from backend.routers import distribuidora_pwa_driver


def _token(user_id: int = 42) -> dict:
    tok = create_staff_token(
        email=f"u{user_id}@test.local",
        role="operativo",
        user_id=user_id,
        token_version=0,
    )
    return {"Authorization": f"Bearer {tok}"}


@pytest.fixture()
def multipart_client(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "development")

    def _enrich(p):
        uid = int(p.get("uid") or p.get("id") or 0)
        return {**p, "id": uid, "uid": uid, "email": p.get("email"), "role": "operativo"}

    def _load(uid: int) -> MultiAppScope:
        return MultiAppScope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
        )

    monkeypatch.setattr("backend.auth.multiapp.enrich_staff_payload", _enrich)
    monkeypatch.setattr("backend.auth.multiapp.load_multiapp_scope", _load)

    captured: dict = {}

    def _conformity(**kwargs):
        captured["conformity"] = {
            "receptor_name": kwargs.get("receptor_name"),
            "signature_len": len(kwargs.get("signature_png") or b""),
            "signature_content_type": kwargs.get("signature_content_type"),
            "refused": kwargs.get("refused"),
        }
        return {
            "conformity": {"receptor_name": kwargs.get("receptor_name"), "id": 1},
            "persisted": True,
            "valid": True,
            "summary_sha256": "abc",
        }

    def _evidence(**kwargs):
        captured["evidence"] = {
            "kind": kwargs.get("evidence_kind"),
            "data_len": len(kwargs.get("data") or b""),
            "content_type": kwargs.get("content_type"),
        }
        return {"persisted": True, "evidence": {"id": 9}}

    def _facade(**kwargs):
        captured["facade"] = {
            "data_len": len(kwargs.get("data") or b""),
            "customer_key": kwargs.get("customer_key"),
        }
        return {"persisted": True, "photo": {"id": 3}}

    monkeypatch.setattr(
        "backend.routers.distribuidora_pwa_driver.visit_svc.submit_conformity",
        _conformity,
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_pwa_driver.visit_svc.upload_incident_evidence",
        _evidence,
    )
    monkeypatch.setattr(
        "backend.routers.distribuidora_pwa_driver.loc_svc.upload_facade_bytes",
        _facade,
    )

    # JSON endpoint smoke (cobranza) — Content-Type application/json.
    monkeypatch.setattr(
        "backend.routers.distribuidora_pwa_driver.visit_svc.declare_collection",
        lambda **k: {"persisted": True, "collection": {"method": k.get("method")}},
    )

    app = FastAPI()
    app.include_router(distribuidora_pwa_driver.router)
    client = TestClient(app)
    return client, captured


def test_conformity_multipart_receives_name_and_signature(multipart_client):
    client, captured = multipart_client
    # Simula iPhone/Safari: canvas.toBlob("image/jpeg") → firma.jpg + image/jpeg.
    jpeg = b"\xff\xd8\xff" + b"0" * 128
    res = client.post(
        "/distribuidora/pwa/driver/despachos/10/visitas/99/conformidad",
        headers=_token(42),
        data={
            "receptor_name": "Yoyo",
            "refused": "false",
            "latitude": "-42.1",
            "longitude": "-73.2",
        },
        files={"signature": ("firma.jpg", io.BytesIO(jpeg), "image/jpeg")},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body.get("persisted") is True
    assert captured["conformity"]["receptor_name"] == "Yoyo"
    assert captured["conformity"]["signature_len"] == len(jpeg)
    assert captured["conformity"]["signature_content_type"] == "image/jpeg"
    # Request fue multipart (TestClient lo fuerza al usar files=)
    assert "multipart/form-data" in (res.request.headers.get("content-type") or "")
    assert "boundary=" in (res.request.headers.get("content-type") or "")


def test_conformity_multipart_forwards_png_claim_when_part_is_png(multipart_client):
    client, captured = multipart_client
    png = b"\x89PNG\r\n\x1a\n" + b"0" * 64
    res = client.post(
        "/distribuidora/pwa/driver/despachos/10/visitas/99/conformidad",
        headers=_token(42),
        data={"receptor_name": "Ana", "refused": "false"},
        files={"signature": ("firma.png", io.BytesIO(png), "image/png")},
    )
    assert res.status_code == 200, res.text
    assert captured["conformity"]["signature_content_type"] == "image/png"


def test_conformity_rejects_without_jwt(multipart_client):
    client, _captured = multipart_client
    res = client.post(
        "/distribuidora/pwa/driver/despachos/10/visitas/99/conformidad",
        data={"receptor_name": "Yoyo", "refused": "false"},
        files={"signature": ("firma.jpg", io.BytesIO(b"x"), "image/jpeg")},
    )
    assert res.status_code in (401, 403)


def test_evidence_multipart_receives_file(multipart_client):
    client, captured = multipart_client
    blob = b"\xff\xd8\xff" + b"EVID" * 40
    res = client.post(
        "/distribuidora/pwa/driver/despachos/10/visitas/99/evidencia",
        headers=_token(42),
        data={"evidence_kind": "closed_store", "latitude": "-42.0"},
        files={"file": ("evidencia.jpg", io.BytesIO(blob), "image/jpeg")},
    )
    assert res.status_code == 200, res.text
    assert captured["evidence"]["data_len"] == len(blob)
    assert "boundary=" in (res.request.headers.get("content-type") or "")


def test_facade_multipart_receives_file(multipart_client):
    client, captured = multipart_client
    blob = b"\xff\xd8\xff" + b"FACH" * 40
    res = client.post(
        "/distribuidora/pwa/driver/despachos/10/clientes/id%3A9010/foto-fachada",
        headers=_token(42),
        data={},
        files={"file": ("fachada.jpg", io.BytesIO(blob), "image/jpeg")},
    )
    assert res.status_code == 200, res.text
    assert captured["facade"]["data_len"] == len(blob)
    assert "boundary=" in (res.request.headers.get("content-type") or "")


def test_json_collection_keeps_application_json(multipart_client):
    client, _captured = multipart_client
    res = client.post(
        "/distribuidora/pwa/driver/despachos/10/visitas/99/cobranza",
        headers={**_token(42), "Content-Type": "application/json"},
        json={
            "document_result_id": 1,
            "method": "cash",
            "document_amount": 1000,
            "declared_amount": 1000,
            "note": None,
        },
    )
    assert res.status_code == 200, res.text
    ct = res.request.headers.get("content-type") or ""
    assert "application/json" in ct
    assert "boundary=" not in ct
