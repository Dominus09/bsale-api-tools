"""Tests unitarios del score probable facturada (sin BD)."""

from datetime import datetime, timezone

from backend.services.distribuidora.probable_invoice_service import (
    DocumentLine,
    DocumentSnapshot,
    compute_probable_match_score,
    score_tier,
)


def _oc_66697_snapshot() -> DocumentSnapshot:
    """Líneas y totales alineados con exports/raw_dump_oc_66697.json."""
    return DocumentSnapshot(
        document_id=3755778,
        document_type_id=33,
        number=66697,
        client_id=1473,
        user_id=85,
        seller_id=None,
        emission_date=datetime(2026, 5, 17, tzinfo=timezone.utc),
        total_amount=1507970.0,
        tracking_number=None,
        municipality="Puqueldon ",
        address="San Agustín rural ",
        lines=(
            DocumentLine(23178, 30.0),
            DocumentLine(24705, 80.0),
            DocumentLine(23179, 40.0),
            DocumentLine(23584, 30.0),
        ),
    )


def _boleta_2616098_snapshot() -> DocumentSnapshot:
    """Primeras 4 líneas = OC; quinta línea extra (2880 u) en boleta."""
    return DocumentSnapshot(
        document_id=3756913,
        document_type_id=1,
        number=2616098,
        client_id=1473,
        user_id=49,
        seller_id=None,
        emission_date=datetime(2026, 5, 18, tzinfo=timezone.utc),
        total_amount=3268548.0,
        tracking_number="6a0cb8d6f7b50d9709f8ded3",
        municipality="Puqueldon ",
        address="San Agustín rural ",
        lines=(
            DocumentLine(23178, 30.0),
            DocumentLine(24705, 80.0),
            DocumentLine(23179, 40.0),
            DocumentLine(23584, 30.0),
            DocumentLine(11053, 2880.0),
        ),
    )


def test_oc_66697_boleta_2616098_high_tier():
    result = compute_probable_match_score(
        _oc_66697_snapshot(),
        _boleta_2616098_snapshot(),
    )
    assert result.match_products_pct == 100.0
    assert result.same_client is True
    assert result.score >= 90.0
    assert score_tier(result.score) == "PROBABLE_FACTURADA_HIGH"


def test_different_client_scores_low():
    from dataclasses import replace

    oc = _oc_66697_snapshot()
    boleta = replace(_boleta_2616098_snapshot(), client_id=9999)
    result = compute_probable_match_score(oc, boleta)
    assert result.score < 60.0
