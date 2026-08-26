"""Cola probable: CONFIRMED > PROBABLE (sin PostgreSQL)."""

from __future__ import annotations

from backend.services.distribuidora.document_relation_sync_service import (
    PROBABLE_QUEUE_MIN_SCORE,
    build_probable_investigation_queue_from_fixtures,
    discover_oc_missing_related_with_probable_sql,
    filter_probable_investigation_queue,
    oc_has_confirmed_invoice_relation,
)

# Canarios documentales (IDs ficticios para fixtures)
OC_69074 = 3875555
OC_69073 = 3875553
OC_68933 = 3867897


def _prob(oc_id: int, oc_number: int, score: float, cand: int) -> dict:
    return {
        "oc_document_id": oc_id,
        "oc_number": oc_number,
        "probable_score": score,
        "candidate_document_id": cand,
        "candidate_number": cand,
    }


def test_sql_uses_not_exists_related_type_not_documents_join():
    sql = discover_oc_missing_related_with_probable_sql()
    assert "NOT EXISTS" in sql
    assert "related_document_type IN (1, 6)" in sql
    assert "LEFT JOIN direct" not in sql
    assert "DISTINCT ON (o.document_id)" in sql


def test_case_a_confirmed_factura_plus_probable_100_excluded():
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=[_prob(OC_69073, 69073, 100, 50610)],
        related_types_by_oc={OC_69073: [6]},
    )
    assert queue == []


def test_case_b_confirmed_boleta_plus_probable_100_excluded():
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=[_prob(OC_69074, 69074, 100, 2719368)],
        related_types_by_oc={OC_69074: [1]},
    )
    assert queue == []


def test_case_c_no_confirmed_probable_100_included():
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=[_prob(OC_68933, 68933, 100, 2715060)],
        related_types_by_oc={OC_68933: []},
    )
    assert len(queue) == 1
    assert queue[0]["oc_number"] == 68933


def test_case_d_only_nc_type_9_not_invoice_confirmed():
    assert oc_has_confirmed_invoice_relation([9]) is False
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=[_prob(99, 99999, 100, 1)],
        related_types_by_oc={99: [9]},
    )
    assert len(queue) == 1


def test_case_e_confirmed_invoice_and_nc_dominates_probable():
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=[_prob(OC_69073, 69073, 100, 50610)],
        related_types_by_oc={OC_69073: [6, 9]},
    )
    assert queue == []


def test_canary_68933_present_when_no_confirmed():
    related = {
        OC_69074: [1],
        OC_69073: [6],
        OC_68933: [],
    }
    probables = [
        _prob(OC_69074, 69074, 100, 2719368),
        _prob(OC_69073, 69073, 100, 50610),
        _prob(OC_68933, 68933, 100, 2715060),
    ]
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=probables,
        related_types_by_oc=related,
    )
    numbers = {int(r["oc_number"]) for r in queue}
    assert 68933 in numbers
    assert 69074 not in numbers
    assert 69073 not in numbers


def test_post_filter_excludes_confirmed_ids_even_if_sql_leaked():
    leaked = [
        _prob(OC_69074, 69074, 100, 2719368),
        _prob(OC_68933, 68933, 100, 2715060),
    ]
    filtered = filter_probable_investigation_queue(
        leaked,
        confirmed_oc_document_ids={OC_69074},
    )
    assert {int(r["oc_number"]) for r in filtered} == {68933}


def test_dedupe_multiple_probables_same_oc():
    rows = [
        _prob(OC_68933, 68933, 80, 1),
        _prob(OC_68933, 68933, 100, 2),
    ]
    out = filter_probable_investigation_queue(rows)
    assert len(out) == 1
    assert float(out[0]["probable_score"]) == 100


def test_score_below_threshold_excluded():
    queue = build_probable_investigation_queue_from_fixtures(
        oc_probables=[_prob(OC_68933, 68933, PROBABLE_QUEUE_MIN_SCORE - 1, 1)],
        related_types_by_oc={OC_68933: []},
    )
    assert queue == []
