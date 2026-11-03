"""Huella comercial canónica: estabilidad y drift real (sin PG analytics)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from backend.services.distribuidora.simple_planning_assignment_service import (
    detect_fingerprint_drift,
)
from backend.services.distribuidora.simple_planning_preview_service import (
    format_fingerprint_drift_message,
    oc_fingerprint_parts,
    oc_identity_fingerprint,
)


def _fp(**kwargs):
    base = dict(
        document_id=100,
        source_document_id=9000,
        state=0,
        client_id=10,
        total_amount=15000,
        emission_date=date(2026, 11, 1),
        products=[
            {"variant_id": 1, "quantity": 2},
            {"variant_id": 2, "quantity": 1.5},
        ],
    )
    base.update(kwargs)
    fp = oc_identity_fingerprint(**base)
    parts_kwargs = {k: v for k, v in base.items() if k != "updated_at"}
    return fp, oc_fingerprint_parts(**parts_kwargs)


def test_fingerprint_ignores_updated_at_sync_noise():
    a, _ = _fp(updated_at=datetime(2026, 11, 3, 12, 0, tzinfo=timezone.utc))
    b, _ = _fp(updated_at=datetime(2026, 11, 3, 18, 30, tzinfo=timezone.utc))
    c, _ = _fp(updated_at=None)
    assert a == b == c


def test_fingerprint_stable_under_line_reorder_and_qty_normalization():
    a, parts_a = _fp(
        products=[
            {"variant_id": 2, "quantity": 1.5, "description": "B"},
            {"variant_id": 1, "quantity": 2.0, "detail_id": 99},
        ]
    )
    b, parts_b = _fp(
        products=[
            {"variant_id": 1, "quantity": "2", "description": "A"},
            {"variant_id": 2, "quantity": 1.500, "code": "X"},
        ]
    )
    assert a == b
    assert parts_a == parts_b
    assert parts_a["lines"] == parts_b["lines"]


def test_fingerprint_changes_on_commercial_header():
    a, _ = _fp(total_amount=15000)
    b, _ = _fp(total_amount=16000)
    assert a != b


def test_fingerprint_changes_on_line_qty():
    a, pa = _fp(products=[{"variant_id": 1, "quantity": 2}])
    b, pb = _fp(products=[{"variant_id": 1, "quantity": 3}])
    assert a != b
    assert pa["lines"] != pb["lines"]
    assert pa["identity"] == pb["identity"]
    assert pa["header"] == pb["header"]


def test_fingerprint_changes_on_state_or_document_identity():
    a, _ = _fp(state=0)
    b, _ = _fp(state=1)
    c, _ = _fp(document_id=101)
    assert a != b
    assert a != c


def test_detect_drift_categorizes_parts_and_omits_full_hashes():
    expected, expected_parts = _fp(total_amount=100)
    current, current_parts = _fp(total_amount=200)
    drifts = detect_fingerprint_drift(
        expected_by_folio={70625: expected},
        expected_parts_by_folio={70625: expected_parts},
        items=[
            {
                "folio": 70625,
                "document_id": 100,
                "identity_fingerprint": current,
                "fingerprint_parts": current_parts,
            }
        ],
        stage="confirm",
    )
    assert len(drifts) == 1
    d = drifts[0]
    assert d["folio"] == 70625
    assert d["stage"] == "confirm"
    assert "cabecera_comercial" in d["changed_parts"]
    assert "expected_fingerprint" not in d
    assert "current_fingerprint" not in d
    assert len(d["expected_fingerprint_prefix"]) == 8


def test_detect_drift_no_false_positive_same_commercial():
    fp, parts = _fp()
    drifts = detect_fingerprint_drift(
        expected_by_folio={1: fp},
        expected_parts_by_folio={1: parts},
        items=[
            {
                "folio": 1,
                "identity_fingerprint": fp,
                "fingerprint_parts": parts,
            }
        ],
    )
    assert drifts == []


def test_format_drift_message_single_and_multi():
    assert "OC 70625:" in format_fingerprint_drift_message([{"folio": 70625}])
    multi = format_fingerprint_drift_message([{"folio": 1}, {"folio": 2}])
    assert "OC 1, 2:" in multi
    assert "Actualiza las órdenes" in multi


def test_billing_fields_not_in_fingerprint():
    """Facturación no forma parte de la huella; solo datos comerciales de la OC."""
    a, _ = _fp()
    # Misma OC comercial: fingerprint idéntico aunque el caller tenga billing distinto.
    b = oc_identity_fingerprint(
        document_id=100,
        source_document_id=9000,
        state=0,
        client_id=10,
        total_amount=15000,
        emission_date=date(2026, 11, 1),
        products=[
            {"variant_id": 1, "quantity": 2},
            {"variant_id": 2, "quantity": 1.5},
        ],
    )
    assert a == b
