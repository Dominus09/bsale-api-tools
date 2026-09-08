"""Regresión OC 69330: boleta de ciclo anterior + NC no debe ser probable del pedido nuevo."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from backend.services.distribuidora.probable_invoice_service import (
    REJECT_ALREADY_RELATED_OTHER_OC,
    REJECT_INVOICE_BEFORE_OC,
    DocumentLine,
    DocumentSnapshot,
    compute_probable_match_score,
    evaluate_probable_candidate_eligibility,
    score_tier,
)


def _oc_69330() -> DocumentSnapshot:
    """OC nueva ALEJANDRA 03-sept ~$54.800."""
    return DocumentSnapshot(
        document_id=3886028,
        document_type_id=33,
        number=69330,
        client_id=1131,
        user_id=59,
        seller_id=59,
        emission_date=datetime(2026, 9, 3, 17, 9, 38, tzinfo=timezone.utc),
        total_amount=54800.0,
        tracking_number=None,
        municipality=None,
        address="misma direccion",
        lines=(
            DocumentLine(9084, 24.0),
            DocumentLine(22806, 10.0),
        ),
    )


def _boleta_2725758() -> DocumentSnapshot:
    """Boleta del pedido anterior (31-ago), mismos productos/cliente."""
    return DocumentSnapshot(
        document_id=3883175,
        document_type_id=1,
        number=2725758,
        client_id=1131,
        user_id=59,
        seller_id=59,
        emission_date=datetime(2026, 8, 31, 19, 56, 24, tzinfo=timezone.utc),
        total_amount=54252.0,
        tracking_number=None,
        municipality=None,
        address="misma direccion",
        lines=(
            DocumentLine(9084, 24.0),
            DocumentLine(22806, 10.0),
        ),
    )


def _prior_oc_69070() -> int:
    """OC del ciclo anterior que ya tiene related real a la boleta 2725758."""
    return 3880001  # document_id placeholder del pedido anterior


def test_oc_69330_rejects_older_boleta_by_date_rule_a():
    oc = _oc_69330()
    boleta = _boleta_2725758()
    assert boleta.emission_date is not None and oc.emission_date is not None
    assert boleta.emission_date.date() < oc.emission_date.date()

    reason = evaluate_probable_candidate_eligibility(oc, boleta)
    assert reason == REJECT_INVOICE_BEFORE_OC

    result = compute_probable_match_score(oc, boleta)
    assert result.score == 0.0
    assert result.tier is None


def test_oc_69330_rejects_boleta_already_related_to_prior_oc_rule_b():
    """Aunque forzamos misma fecha, related a otra OC bloquea (ciclo anterior)."""
    oc = _oc_69330()
    boleta = replace(
        _boleta_2725758(),
        emission_date=datetime(2026, 9, 3, 12, 0, 0, tzinfo=timezone.utc),
    )
    prior_oc_id = _prior_oc_69070()

    reason = evaluate_probable_candidate_eligibility(
        oc,
        boleta,
        related_from_oc_document_ids={prior_oc_id},
    )
    assert reason == REJECT_ALREADY_RELATED_OTHER_OC

    result = compute_probable_match_score(
        oc,
        boleta,
        related_from_oc_document_ids={prior_oc_id},
    )
    assert result.score == 0.0
    assert result.tier is None


def test_nc_does_not_free_old_boleta_rule_c():
    """
    NC sobre el pedido anterior no cambia document_related 1/6 de la boleta.
    Mientras el related exista, la boleta sigue bloqueada para OC nueva.
    """
    oc = _oc_69330()
    boleta = replace(
        _boleta_2725758(),
        emission_date=datetime(2026, 9, 3, tzinfo=timezone.utc),
    )
    # Simula: hubo NC, pero related OC_antigua → boleta permanece.
    reason = evaluate_probable_candidate_eligibility(
        oc,
        boleta,
        related_from_oc_document_ids={_prior_oc_69070()},
    )
    assert reason == REJECT_ALREADY_RELATED_OTHER_OC


def test_same_client_amount_products_not_enough_across_cycles_rule_d():
    """Sin gates, el score sería alto; con A el match queda anulado."""
    oc = _oc_69330()
    boleta = _boleta_2725758()
    # Similitud operativa (lo que disparó el falso positivo histórico):
    # mismos productos/cliente/monto cercano — pero fecha anterior → score 0.
    result = compute_probable_match_score(oc, boleta)
    assert result.score < 60.0
    assert score_tier(result.score) is None


def test_valid_same_day_unrelated_boleta_still_matches():
    """Control positivo: boleta mismo día, no relacionada a otra OC, sigue probable."""
    oc = _oc_69330()
    boleta = replace(
        _boleta_2725758(),
        document_id=9999999,
        number=9999999,
        emission_date=datetime(2026, 9, 3, 18, 0, 0, tzinfo=timezone.utc),
        total_amount=54800.0,
    )
    assert evaluate_probable_candidate_eligibility(oc, boleta) is None
    result = compute_probable_match_score(oc, boleta)
    assert result.score >= 60.0
    assert result.same_client is True
    assert result.match_products_pct == 100.0


def test_related_only_to_same_oc_does_not_block():
    oc = _oc_69330()
    boleta = replace(
        _boleta_2725758(),
        emission_date=datetime(2026, 9, 3, tzinfo=timezone.utc),
        total_amount=54800.0,
    )
    assert (
        evaluate_probable_candidate_eligibility(
            oc,
            boleta,
            related_from_oc_document_ids={oc.document_id},
        )
        is None
    )
