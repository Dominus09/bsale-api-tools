"""P4: reglas de visita sin DB (validaciones / status)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc


def test_schema_status_shape(monkeypatch):
    class FakeCur:
        def execute(self, *_a, **_k):
            return None

        def fetchone(self):
            return {"tables_ready": False}

        def close(self):
            return None

    class FakeConn:
        def cursor(self):
            return FakeCur()

        def close(self):
            return None

    monkeypatch.setattr(svc, "get_connection", lambda: FakeConn())
    monkeypatch.setattr(
        "backend.repositories.distribuidora.delivery_visit_repo.tables_ready",
        lambda _cur: False,
    )
    st = svc.schema_status()
    assert st["migration"] == "056_delivery_visits_evidence.sql"
    assert st["mode"] == "online_only"
    assert st["tables_ready"] is False
    assert "offline" in st["note_es"].lower() or "online" in st["note_es"].lower()


def test_finalize_rejects_bad_status(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda _u, _p: 7,
    )
    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            expected_version=1,
            status="weird",
        )
    assert ei.value.status_code == 400


def test_finalize_not_delivered_requires_reason(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda _u, _p: 7,
    )
    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            expected_version=1,
            status="not_delivered",
            result_reason=None,
        )
    assert ei.value.status_code == 400


def test_finalize_other_requires_note(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc,
        "enforce_driver_plan_read_access",
        lambda _u, _p: 7,
    )
    with pytest.raises(HTTPException) as ei:
        svc.finalize_visit(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            expected_version=1,
            status="not_delivered",
            result_reason="other",
            result_note="  ",
        )
    assert ei.value.status_code == 400


def test_local_santiago_formats():
    from datetime import datetime, timezone

    dt = datetime(2026, 3, 15, 15, 30, tzinfo=timezone.utc)
    local = svc._local_santiago(dt)
    assert local is not None
    assert "-03:00" in local or "-04:00" in local


def test_save_lines_rejects_over_delivery(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    with pytest.raises(HTTPException) as ei:
        svc.save_document_lines(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            related_document_id=99,
            oc_document_id=None,
            document_number=1,
            document_type="boleta",
            document_total=1000,
            result_status="partial",
            notes=None,
            lines=[
                {
                    "source_detail_id": 101,
                    "planned_units": 2,
                    "delivered_units": 3,
                    "product_name": "X",
                }
            ],
        )
    assert ei.value.status_code == 400


def test_save_lines_requires_source_detail_id(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    with pytest.raises(HTTPException) as ei:
        svc.save_document_lines(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            related_document_id=99,
            oc_document_id=None,
            document_number=1,
            document_type="boleta",
            document_total=1000,
            result_status="full",
            notes=None,
            lines=[
                {
                    "planned_units": 1,
                    "delivered_units": 1,
                    "product_name": "X",
                    "variant_id": 5,
                }
            ],
        )
    assert ei.value.status_code == 400
    assert "source_detail_id" in str(ei.value.detail)


def test_save_lines_rejects_duplicate_source_detail(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    with pytest.raises(HTTPException) as ei:
        svc.save_document_lines(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            related_document_id=99,
            oc_document_id=None,
            document_number=1,
            document_type="boleta",
            document_total=1000,
            result_status="partial",
            notes=None,
            lines=[
                {
                    "source_detail_id": 7,
                    "planned_units": 1,
                    "delivered_units": 1,
                    "product_name": "A",
                },
                {
                    "source_detail_id": 7,
                    "planned_units": 2,
                    "delivered_units": 1,
                    "product_name": "A",
                    "undelivered_reason": "physical_shortage",
                },
            ],
        )
    assert ei.value.status_code == 400
    assert "duplicada" in str(ei.value.detail).lower()


def test_save_lines_rejects_when_document_has_no_details(monkeypatch):
    """Sin detail_id en BD: no fabricar ni registrar cantidades."""

    class Cur:
        def execute(self, sql, params=None):
            self._sql = sql

        def fetchone(self):
            sql = (self._sql or "").lower()
            if "to_regclass" in sql:
                return {"ok": True}
            if "count(*)" in sql:
                return {"n": 0}
            return None

        def fetchall(self):
            return []

        def close(self):
            return None

    class Conn:
        def cursor(self):
            return Cur()

        def commit(self):
            return None

        def rollback(self):
            return None

        def close(self):
            return None

    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    monkeypatch.setattr(svc, "get_connection", lambda: Conn())
    monkeypatch.setattr(svc.visit_repo, "tables_ready", lambda *_a, **_k: True)
    monkeypatch.setattr(
        svc.visit_repo,
        "get_visit_for_update",
        lambda *_a, **_k: {"id": 1, "driver_user_id": 7, "version": 1, "status": "in_progress"},
    )
    monkeypatch.setattr(
        svc,
        "_require_own_open_visit",
        lambda *_a, **_k: {"id": 1, "driver_user_id": 7, "version": 1},
    )
    with pytest.raises(HTTPException) as ei:
        svc.save_document_lines(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            related_document_id=99,
            oc_document_id=None,
            document_number=1,
            document_type="boleta",
            document_total=1000,
            result_status="full",
            notes=None,
            lines=[
                {
                    "source_detail_id": 999001,
                    "planned_units": 1,
                    "delivered_units": 1,
                    "product_name": "Inventado",
                }
            ],
        )
    assert ei.value.status_code == 422
    assert "detail_id" in str(ei.value.detail).lower() or "confiable" in str(
        ei.value.detail
    ).lower()


def test_declare_collection_blocks_transfer_verified(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    with pytest.raises(HTTPException) as ei:
        svc.declare_collection(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            document_result_id=None,
            method="transfer_verified",
            document_amount=1000,
            declared_amount=1000,
            note=None,
        )
    assert ei.value.status_code == 403


def test_declare_collection_pending_rejects_positive_amount(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    with pytest.raises(HTTPException) as ei:
        svc.declare_collection(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            document_result_id=None,
            method="pending",
            document_amount=155033,
            declared_amount=100000,
            note=None,
        )
    assert ei.value.status_code == 400
    assert "pendiente" in str(ei.value.detail).lower()


def test_submit_conformity_requires_receptor_name(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda *_a, **_k: 1
    )
    with pytest.raises(HTTPException) as ei:
        svc.submit_conformity(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            receptor_name=" ",
            signature_png=b"fake",
            refused=False,
            refuse_reason=None,
            latitude=None,
            longitude=None,
            accuracy_m=None,
        )
    assert ei.value.status_code == 400
    assert "receptor" in str(ei.value.detail).lower()


def test_submit_conformity_refuse_requires_reason(monkeypatch):
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )
    monkeypatch.setattr(
        svc.loc_svc, "_authorized_company_id_for_plan", lambda *_a, **_k: 1
    )
    with pytest.raises(HTTPException) as ei:
        svc.submit_conformity(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            receptor_name="",
            signature_png=None,
            refused=True,
            refuse_reason="  ",
            latitude=None,
            longitude=None,
            accuracy_m=None,
        )
    assert ei.value.status_code == 400
    assert "negativa" in str(ei.value.detail).lower()
