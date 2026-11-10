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
                "source_detail_id": 11,
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


def test_signature_sha_mismatch_blocks_finalize(monkeypatch):
    monkeypatch.setattr(
        closure.visit_repo,
        "list_document_results",
        lambda *_a, **_k: [{"related_document_id": 1, "document_number": 1}],
    )
    monkeypatch.setattr(
        closure.visit_repo,
        "list_line_results",
        lambda *_a, **_k: [
            {
                "source_detail_id": 22,
                "planned_units": 2,
                "delivered_units": 2,
                "undelivered_units": 0,
                "product_name": "A",
            }
        ],
    )
    monkeypatch.setattr(closure.visit_repo, "list_evidence", lambda *_a, **_k: [])
    monkeypatch.setattr(
        closure.visit_repo, "list_active_collections", lambda *_a, **_k: []
    )
    monkeypatch.setattr(
        closure.visit_repo,
        "get_conformity",
        lambda *_a, **_k: {
            "signature_object_key": "k",
            "receptor_name": "Ana",
            "refused": False,
            "invalidated_at": None,
            "accepted_summary_sha256": "deadbeef",
            "visit_version_at_sign": 1,
        },
    )
    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit={"id": 1, "version": 1, "customer_key": "id:1"},
            status="completed",
            result_reason=None,
            result_note=None,
            has_end_gps=True,
            expected_related_ids=[1],
        )
    assert "conformidad" in str(ei.value.detail).lower() or "firmar" in str(ei.value.detail).lower()


def test_refused_is_pending_review_not_verified(monkeypatch):
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
                "source_detail_id": 33,
                "planned_units": 1,
                "delivered_units": 1,
                "undelivered_units": 0,
            }
        ],
    )
    monkeypatch.setattr(closure.visit_repo, "list_evidence", lambda *_a, **_k: [])
    monkeypatch.setattr(
        closure.visit_repo,
        "get_conformity",
        lambda *_a, **_k: {
            "refused": True,
            "refuse_reason": "No quiso firmar",
            "invalidated_at": None,
            "signature_object_key": None,
        },
    )
    gate = closure.assert_finalize_ready(
        FakeCur(),
        visit={"id": 1, "version": 1},
        status="completed",
        result_reason=None,
        result_note=None,
        has_end_gps=True,
        expected_related_ids=[1],
    )
    assert gate["closure_review_status"] == "pending_review"


def test_finalize_blocks_lines_without_source_detail(monkeypatch):
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
                "planned_units": 1,
                "delivered_units": 1,
                "undelivered_units": 0,
                "variant_id": 9,
                # sin source_detail_id → ambiguo
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
            "accepted_summary_sha256": "x",
        },
    )
    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit={"id": 1, "version": 1},
            status="completed",
            result_reason=None,
            result_note=None,
            has_end_gps=True,
            expected_related_ids=[1],
        )
    assert "source_detail_id" in str(ei.value.detail).lower()


def test_matching_signature_sha_allows_verified(monkeypatch):
    import hashlib
    import json

    visit = {"id": 1, "version": 1, "customer_key": "id:1"}
    docs = [{"related_document_id": 1, "document_number": 10, "result_status": "full"}]
    lines = [
        {
            "source_detail_id": 501,
            "line_number": 1,
            "planned_units": 2,
            "delivered_units": 2,
            "undelivered_units": 0,
            "product_name": "A",
            "variant_id": 9,
        }
    ]
    summary = closure.build_conformity_summary(
        visit=visit, docs=docs, lines=lines, collections=[]
    )
    sha = hashlib.sha256(
        json.dumps(summary, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    monkeypatch.setattr(
        closure.visit_repo, "list_document_results", lambda *_a, **_k: docs
    )
    monkeypatch.setattr(
        closure.visit_repo, "list_line_results", lambda *_a, **_k: lines
    )
    monkeypatch.setattr(
        closure.visit_repo, "list_active_collections", lambda *_a, **_k: []
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
            "accepted_summary_sha256": sha,
            "visit_version_at_sign": 1,
        },
    )
    gate = closure.assert_finalize_ready(
        FakeCur(),
        visit=visit,
        status="completed",
        result_reason=None,
        result_note=None,
        has_end_gps=True,
        expected_related_ids=[1],
    )
    assert gate["closure_review_status"] == "verified"


def test_not_delivered_blocks_received_qty(monkeypatch):
    monkeypatch.setattr(closure.visit_repo, "list_document_results", lambda *_a, **_k: [])
    monkeypatch.setattr(
        closure.visit_repo,
        "list_line_results",
        lambda *_a, **_k: [
            {"planned_units": 2, "delivered_units": 1, "undelivered_units": 1}
        ],
    )
    monkeypatch.setattr(
        closure.visit_repo,
        "list_evidence",
        lambda *_a, **_k: [
            {
                "evidence_kind": "closed_store",
                "upload_confirmed": True,
                "validation_status": "pending",
            }
        ],
    )
    monkeypatch.setattr(closure.visit_repo, "get_conformity", lambda *_a, **_k: None)
    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit={"id": 1},
            status="not_delivered",
            result_reason="closed_store",
            result_note=None,
            has_end_gps=True,
            expected_related_ids=[],
        )
    assert "recibidas" in str(ei.value.detail).lower()


def test_conformity_summary_includes_collections_in_sha():
    visit = {"id": 1, "version": 2, "customer_key": "id:1"}
    docs = [
        {
            "related_document_id": 1,
            "document_number": 10,
            "document_type": "boleta",
            "document_total": 155033,
            "result_status": "full",
        }
    ]
    lines = [
        {
            "source_detail_id": 501,
            "line_number": 1,
            "planned_units": 2,
            "delivered_units": 2,
            "undelivered_units": 0,
            "product_name": "A",
            "variant_id": 9,
        }
    ]
    base = closure.build_conformity_summary(
        visit=visit, docs=docs, lines=lines, collections=[]
    )
    with_pay = closure.build_conformity_summary(
        visit=visit,
        docs=docs,
        lines=lines,
        collections=[
            {
                "id": 1,
                "document_result_id": 9,
                "method": "cash",
                "document_amount": 155033,
                "declared_amount": 50000,
                "pending_balance": None,
                "note": None,
            },
            {
                "id": 2,
                "document_result_id": 9,
                "method": "transfer_declared",
                "document_amount": 155033,
                "declared_amount": 70000,
                "pending_balance": None,
                "note": None,
            },
            {
                "id": 3,
                "document_result_id": 9,
                "method": "pending",
                "document_amount": 155033,
                "declared_amount": 35033,
                "pending_balance": 35033,
                "note": None,
            },
        ],
    )
    assert "collections" in with_pay
    assert len(with_pay["collections"]) == 3
    assert base != with_pay


def test_collection_change_breaks_signed_sha(monkeypatch):
    import hashlib
    import json

    visit = {"id": 1, "version": 1, "customer_key": "id:1"}
    docs = [{"related_document_id": 1, "document_number": 10, "result_status": "full"}]
    lines = [
        {
            "source_detail_id": 501,
            "line_number": 1,
            "planned_units": 2,
            "delivered_units": 2,
            "undelivered_units": 0,
            "product_name": "A",
            "variant_id": 9,
        }
    ]
    cols_signed = [
        {
            "id": 1,
            "document_result_id": 9,
            "method": "cash",
            "document_amount": 1000,
            "declared_amount": 1000,
            "pending_balance": 0,
            "note": None,
        }
    ]
    summary = closure.build_conformity_summary(
        visit=visit, docs=docs, lines=lines, collections=cols_signed
    )
    sha = hashlib.sha256(
        json.dumps(summary, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    cols_changed = [
        {
            "id": 2,
            "document_result_id": 9,
            "method": "cash",
            "document_amount": 1000,
            "declared_amount": 400,
            "pending_balance": None,
            "note": None,
        },
        {
            "id": 3,
            "document_result_id": 9,
            "method": "pending",
            "document_amount": 1000,
            "declared_amount": 600,
            "pending_balance": 600,
            "note": None,
        },
    ]
    monkeypatch.setattr(
        closure.visit_repo, "list_document_results", lambda *_a, **_k: docs
    )
    monkeypatch.setattr(
        closure.visit_repo, "list_line_results", lambda *_a, **_k: lines
    )
    monkeypatch.setattr(
        closure.visit_repo, "list_active_collections", lambda *_a, **_k: cols_changed
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
            "accepted_summary_sha256": sha,
            "visit_version_at_sign": 1,
        },
    )
    with pytest.raises(HTTPException) as ei:
        closure.assert_finalize_ready(
            FakeCur(),
            visit=visit,
            status="completed",
            result_reason=None,
            result_note=None,
            has_end_gps=True,
            expected_related_ids=[1],
        )
    assert "vuelva a firmar" in str(ei.value.detail).lower()


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
