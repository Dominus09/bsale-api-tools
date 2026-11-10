"""Validaciones de cierre P4 (sin DB)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_closure as closure


class FakeCur:
    pass


def test_completed_requires_lines_and_signature(monkeypatch):
    monkeypatch.setattr(
        closure.visit_repo,
        "list_document_results",
        lambda *_a, **_k: [{"related_document_id": 1, "result_status": "full"}],
    )
    monkeypatch.setattr(closure.visit_repo, "list_line_results", lambda *_a, **_k: [])
    monkeypatch.setattr(closure.visit_repo, "list_evidence", lambda *_a, **_k: [])
    monkeypatch.setattr(closure.visit_repo, "get_conformity", lambda *_a, **_k: None)

    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit={"id": 1, "gps_exception_code": None},
            status="completed",
            result_reason=None,
            result_note=None,
            has_end_gps=True,
            expected_related_ids=[1],
        )
    assert ei.value.status_code == 422


def test_completed_blocks_undelivered(monkeypatch):
    monkeypatch.setattr(
        closure.visit_repo,
        "list_document_results",
        lambda *_a, **_k: [{"related_document_id": 1}],
    )
    monkeypatch.setattr(
        closure.visit_repo,
        "list_line_results",
        lambda *_a, **_k: [
            {
                "planned_units": 2,
                "delivered_units": 1,
                "undelivered_units": 1,
                "undelivered_reason": "physical_shortage",
            }
        ],
    )
    monkeypatch.setattr(closure.visit_repo, "list_evidence", lambda *_a, **_k: [])
    monkeypatch.setattr(
        closure.visit_repo,
        "get_conformity",
        lambda *_a, **_k: {
            "signature_object_key": "k",
            "receptor_name": "Ana",
            "refused": False,
            "invalidated_at": None,
        },
    )
    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit={"id": 1},
            status="completed",
            result_reason=None,
            result_note=None,
            has_end_gps=True,
            expected_related_ids=[1],
        )
    assert "faltantes" in str(ei.value.detail).lower()


def test_incident_requires_evidence(monkeypatch):
    monkeypatch.setattr(closure.visit_repo, "list_document_results", lambda *_a, **_k: [])
    monkeypatch.setattr(closure.visit_repo, "list_line_results", lambda *_a, **_k: [])
    monkeypatch.setattr(closure.visit_repo, "list_evidence", lambda *_a, **_k: [])
    monkeypatch.setattr(closure.visit_repo, "get_conformity", lambda *_a, **_k: None)
    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit={"id": 1},
            status="not_delivered",
            result_reason="closed_store",
            result_note="x",
            has_end_gps=True,
            expected_related_ids=[],
        )
    assert ei.value.status_code == 422


def test_incident_exception_pending_review(monkeypatch):
    monkeypatch.setattr(closure.visit_repo, "list_document_results", lambda *_a, **_k: [])
    monkeypatch.setattr(closure.visit_repo, "list_line_results", lambda *_a, **_k: [])
    monkeypatch.setattr(
        closure.visit_repo,
        "list_evidence",
        lambda *_a, **_k: [
            {
                "evidence_kind": "closed_store",
                "upload_confirmed": False,
                "validation_status": "exception",
            }
        ],
    )
    monkeypatch.setattr(closure.visit_repo, "get_conformity", lambda *_a, **_k: None)
    gate = closure.assert_finalize_ready(
        FakeCur(),
        visit={"id": 1},
        status="not_delivered",
        result_reason="closed_store",
        result_note="obs",
        has_end_gps=True,
        expected_related_ids=[],
    )
    assert gate["closure_review_status"] == "pending_review"
