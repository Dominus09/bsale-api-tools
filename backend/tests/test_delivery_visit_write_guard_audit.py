"""Auditoría estática: escrituras pasan por _require_own_open_visit / start_visit."""

from __future__ import annotations

import inspect

from backend.services.distribuidora import delivery_visit_service as svc
from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as read_svc


WRITE_FUNCS = (
    "save_document_lines",
    "declare_collection",
    "declare_collections_mixed",
    "upload_incident_evidence",
    "register_evidence_exception",
    "submit_conformity",
)


def test_write_entrypoints_call_require_own_open_visit():
    for name in WRITE_FUNCS:
        fn = getattr(svc, name)
        src = inspect.getsource(fn)
        assert "_require_own_open_visit" in src, name


def test_finalize_checks_valid_closed_before_mutate():
    src = inspect.getsource(svc.finalize_visit)
    assert "get_valid_closed_visit" in src
    assert "ERR_VISIT_SUPERSEDED_BY_CLOSED" in src or "VISIT_SUPERSEDED_BY_CLOSED" in src


def test_get_visit_detail_is_read_only_no_commit():
    src = inspect.getsource(svc.get_visit_detail)
    assert "commit(" not in src
    assert "insert_" not in src
    assert "finalize_visit" not in src
    assert "_require_own_open_visit" not in src  # lectura de cerradas/históricas OK


def test_start_visit_locks_and_blocks_superseded():
    src = inspect.getsource(svc.start_visit)
    assert "lock_visits_for_client" in src
    assert "get_valid_closed_visit" in src
    assert "ERR_VISIT_SUPERSEDED_BY_CLOSED" in src or "VISIT_SUPERSEDED_BY_CLOSED" in src
    assert "ERR_VISIT_ALREADY_CLOSED" in src or "VISIT_ALREADY_CLOSED" in src


def test_ficha_exposes_admin_review_pending_capability():
    src = inspect.getsource(read_svc.get_client_ficha_for_driver)
    assert "admin_review_pending" in src
    assert "resume_active_visit" in src
