"""Simulación y confirmación de recuperación manual OC (sin BD/Bsale reales)."""

from __future__ import annotations

import time
from unittest.mock import MagicMock

import pytest

from backend.services.distribuidora import oc_audit_recovery_service as recover
from backend.services.distribuidora.oc_correlative_audit_service import (
    STATUS_CANCELLED,
    STATUS_MISSING_IN_PG,
    STATUS_NOT_FOUND_IN_BSALE,
)


def _active(source_id: int, folio: int) -> dict:
    return {
        "id": source_id,
        "number": folio,
        "state": 0,
        "commercialState": 0,
        "emissionDate": 1_700_000_000,
        "generationDate": 1_700_000_100,
        "modificationDate": 1_700_000_200,
        "totalAmount": 5500,
        "office": {"id": 1},
        "document_type": {"id": 33},
        "company": {"id": 3},
        "client": {"id": 44, "companyName": "Cliente Demo"},
    }


def _dry_report(folio: int, source_id: int, *, source_hash: str = "abc") -> dict:
    doc = _active(source_id, folio)
    return {
        "status": "dry_run_needs_sync",
        "dry_run": True,
        "wrote": False,
        "folio": folio,
        "local_document_id": None,
        "current_bsale_source_document_id": source_id,
        "source_hash": source_hash,
        "bsale_details_count": 2,
        "needs_detail_sync": True,
        "source_changed": False,
        "diff": {"matches": False},
        "bsale_document": {
            "id": source_id,
            "number": folio,
            "state": 0,
            "totalAmount": 5500,
            "generationDate": doc["generationDate"],
            "modificationDate": doc["modificationDate"],
        },
        "source_discovery": {
            "active_document": {
                "id": source_id,
                "number": folio,
                "state": 0,
                "totalAmount": 5500,
                "generationDate": doc["generationDate"],
                "modificationDate": doc["modificationDate"],
            }
        },
    }


def test_simulate_no_writes_and_issues_token():
    folio = 70550
    source_id = 99001
    calls = {"reconcile": 0}

    def search(_c, *, folio, **_k):
        return [_active(source_id, folio)]

    def reconcile(_c, **kwargs):
        calls["reconcile"] += 1
        assert kwargs.get("dry_run") is True
        return _dry_report(folio, source_id)

    out = recover.simulate_oc_recovery(
        MagicMock(),
        folio=folio,
        company_id=3,
        office_id=1,
        user_id=7,
        user_email="gerencia@test.local",
        search_fn=search,
        local_loader=lambda **_k: {"by_folio": {}, "zero_number_rows": [], "row_count": 0},
        reconcile_fn=reconcile,
    )
    assert out["wrote"] is False
    assert out["dry_run"] is True
    assert out["simulation_token"]
    assert out["bsale_source_document_id"] == source_id
    assert out["details_count"] == 2
    assert calls["reconcile"] == 1


def test_simulate_rejects_cancelled():
    def search(_c, *, folio, **_k):
        return [
            {
                "id": 1,
                "number": 0,
                "state": 8888,
                "commercialState": 0,
                "emissionDate": 1,
                "generationDate": 2,
                "totalAmount": 0,
                "office": {"id": 1},
                "document_type": {"id": 33},
                "company": {"id": 3},
            }
        ]

    with pytest.raises(recover.RecoveryValidationError):
        recover.simulate_oc_recovery(
            MagicMock(),
            folio=100,
            search_fn=search,
            local_loader=lambda **_k: {"by_folio": {}},
            reconcile_fn=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()),
        )


def test_simulate_rejects_already_in_pg():
    with pytest.raises(recover.RecoveryValidationError, match="ya existe"):
        recover.simulate_oc_recovery(
            MagicMock(),
            folio=50,
            search_fn=lambda *_a, **_k: [],
            local_loader=lambda **_k: {
                "by_folio": {
                    50: {
                        "folio": 50,
                        "document_id": 500,
                        "state": 0,
                        "source_document_id": 900,
                    }
                }
            },
            reconcile_fn=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()),
        )


def test_confirm_requires_matching_fingerprint():
    folio = 80001
    source_id = 88001

    def search(_c, *, folio, **_k):
        return [_active(source_id, folio)]

    sim = recover.simulate_oc_recovery(
        MagicMock(),
        folio=folio,
        user_id=1,
        user_email="a@test.local",
        search_fn=search,
        local_loader=lambda **_k: {"by_folio": {}},
        reconcile_fn=lambda *_a, **_k: _dry_report(folio, source_id, source_hash="h1"),
    )

    def reconcile_changed(_c, **kwargs):
        if kwargs.get("dry_run"):
            # Mismo source id pero hash distinto → fingerprint mismatch
            return _dry_report(folio, source_id, source_hash="h2-changed")
        raise AssertionError("no debe escribir si fingerprint cambió")

    with pytest.raises(recover.RecoveryConflictError, match="cambió"):
        recover.confirm_oc_recovery(
            MagicMock(),
            folio=folio,
            simulation_token=sim["simulation_token"],
            user_id=1,
            user_email="a@test.local",
            search_fn=search,
            local_loader=lambda **_k: {"by_folio": {}},
            reconcile_fn=reconcile_changed,
        )


def test_confirm_writes_once_and_idempotent_second_call():
    folio = 80002
    source_id = 88002
    writes = {"n": 0}

    def search(_c, *, folio, **_k):
        return [_active(source_id, folio)]

    def reconcile(_c, **kwargs):
        if kwargs.get("dry_run"):
            return _dry_report(folio, source_id, source_hash="stable")
        writes["n"] += 1
        return {
            "status": "synced",
            "wrote": True,
            "dry_run": False,
            "folio": folio,
            "local_document_id": 555,
            "current_bsale_source_document_id": source_id,
            "source_hash": "stable",
            "bsale_details_count": 2,
            "postgresql_details_count": 2,
        }

    local_state = {"by_folio": {}}

    def loader(**_k):
        return {
            "by_folio": dict(local_state["by_folio"]),
            "zero_number_rows": [],
            "row_count": len(local_state["by_folio"]),
        }

    sim = recover.simulate_oc_recovery(
        MagicMock(),
        folio=folio,
        user_id=2,
        user_email="adm@test.local",
        search_fn=search,
        local_loader=loader,
        reconcile_fn=reconcile,
    )
    first = recover.confirm_oc_recovery(
        MagicMock(),
        folio=folio,
        simulation_token=sim["simulation_token"],
        user_id=2,
        user_email="adm@test.local",
        search_fn=search,
        local_loader=loader,
        reconcile_fn=reconcile,
    )
    assert first["wrote"] is True
    assert first["status"] == "recovered"
    assert writes["n"] == 1

    local_state["by_folio"][folio] = {
        "folio": folio,
        "document_id": 555,
        "state": 0,
        "source_document_id": source_id,
    }
    second = recover.confirm_oc_recovery(
        MagicMock(),
        folio=folio,
        simulation_token=sim["simulation_token"],
        user_id=2,
        user_email="adm@test.local",
        search_fn=search,
        local_loader=loader,
        reconcile_fn=reconcile,
    )
    assert second["status"] == "already_present"
    assert second["wrote"] is False
    assert writes["n"] == 1


def test_confirm_expired_token():
    folio = 11
    source_id = 22
    sim = recover.simulate_oc_recovery(
        MagicMock(),
        folio=folio,
        user_id=1,
        search_fn=lambda *_a, **_k: [_active(source_id, folio)],
        local_loader=lambda **_k: {"by_folio": {}},
        reconcile_fn=lambda *_a, **_k: _dry_report(folio, source_id),
    )
    # Manipular claims con exp pasado.
    claims = recover._verify_simulation(sim["simulation_token"])
    claims["exp"] = int(time.time()) - 10
    bad = recover._sign_simulation(claims)
    with pytest.raises(recover.RecoveryValidationError, match="expiró"):
        recover.confirm_oc_recovery(
            MagicMock(),
            folio=folio,
            simulation_token=bad,
            user_id=1,
            search_fn=lambda *_a, **_k: [_active(source_id, folio)],
            local_loader=lambda **_k: {"by_folio": {}},
            reconcile_fn=lambda *_a, **_k: _dry_report(folio, source_id),
        )


def test_simulate_maps_429_to_transient():
    def search(*_a, **_k):
        raise RuntimeError("Bsale HTTP 429: rate limit agotado")

    with pytest.raises(recover.RecoveryTransientError):
        recover.simulate_oc_recovery(
            MagicMock(),
            folio=1,
            search_fn=search,
            local_loader=lambda **_k: {"by_folio": {}},
            reconcile_fn=lambda *_a, **_k: {},
        )


def test_assert_recoverable_statuses():
    recover._assert_recoverable_investigation(
        {"status": STATUS_MISSING_IN_PG, "recoverable": True},
        folio=1,
    )
    with pytest.raises(recover.RecoveryValidationError):
        recover._assert_recoverable_investigation(
            {"status": STATUS_CANCELLED, "recoverable": False},
            folio=1,
        )
    with pytest.raises(recover.RecoveryValidationError):
        recover._assert_recoverable_investigation(
            {"status": STATUS_NOT_FOUND_IN_BSALE, "recoverable": False},
            folio=1,
        )


def test_confirm_rejects_cross_company_token_reuse():
    folio = 90001
    source_id = 91001
    sim = recover.simulate_oc_recovery(
        MagicMock(),
        folio=folio,
        company_id=3,
        office_id=1,
        user_id=5,
        search_fn=lambda *_a, **_k: [_active(source_id, folio)],
        local_loader=lambda **_k: {"by_folio": {}},
        reconcile_fn=lambda *_a, **_k: _dry_report(folio, source_id),
    )
    with pytest.raises(recover.RecoveryValidationError, match="otra empresa"):
        recover.confirm_oc_recovery(
            MagicMock(),
            folio=folio,
            simulation_token=sim["simulation_token"],
            company_id=1,  # manipulado
            office_id=1,
            user_id=5,
            search_fn=lambda *_a, **_k: [_active(source_id, folio)],
            local_loader=lambda **_k: {"by_folio": {}},
            reconcile_fn=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()),
        )


def test_confirm_rejects_non_oc_document_type_in_token():
    folio = 90002
    source_id = 91002
    sim = recover.simulate_oc_recovery(
        MagicMock(),
        folio=folio,
        user_id=5,
        search_fn=lambda *_a, **_k: [_active(source_id, folio)],
        local_loader=lambda **_k: {"by_folio": {}},
        reconcile_fn=lambda *_a, **_k: _dry_report(folio, source_id),
    )
    claims = recover._verify_simulation(sim["simulation_token"])
    claims["document_type_id"] = 1
    bad = recover._sign_simulation(claims)
    with pytest.raises(recover.RecoveryValidationError, match="tipo 33"):
        recover.confirm_oc_recovery(
            MagicMock(),
            folio=folio,
            simulation_token=bad,
            user_id=5,
            search_fn=lambda *_a, **_k: [_active(source_id, folio)],
            local_loader=lambda **_k: {"by_folio": {}},
            reconcile_fn=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError()),
        )
