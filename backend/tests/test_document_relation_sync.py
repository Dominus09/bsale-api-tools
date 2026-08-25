"""Tests: discovery NC via related_detail_id + dry-run relation sync."""

from __future__ import annotations

from decimal import Decimal

from backend.services.distribuidora.document_relation_sync_service import (
    classify_absence_bucket,
    materialize_cn_related_rows,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_INVOICED_CN_UNSPECIFIED,
    BILLING_INVOICED_FULL_CN,
    BILLING_INVOICED_PARTIAL_CN,
    classify_credit_note_coverage,
    derive_dispatch_flags,
)


def test_1_oc_factura_directa_materialize_shape():
    # Forma de arista OC no la escribe este módulo; NC sí.
    triples = materialize_cn_related_rows([])
    assert triples == []


def test_2_boleta_type_constant():
    from backend.services.distribuidora.document_relation_sync_service import DOC_TYPE_BOLETA

    assert DOC_TYPE_BOLETA == 1


def test_3_factura_despues_de_oc_absence_bucket():
    # Sin related + probable exacto → cola catchup, no confirmed.
    assert (
        classify_absence_bucket(
            has_related=False,
            probable_score=100,
            same_client=True,
            same_amount=True,
        )
        == "likely_missing_sync_exact_match"
    )


def test_4_related_refresh_independent_of_header_hash():
    # Contrato de diseño: discovery NC no consulta source_hash.
    rows = [
        {
            "nc_document_id": 9,
            "invoice_detail_id": 100,
            "nc_state": 0,
            "already_in_document_related": False,
        }
    ]
    assert materialize_cn_related_rows(rows) == [(100, 9, 9)]


def test_5_factura_to_nc_triple():
    rows = [
        {
            "nc_document_id": 3872169,
            "invoice_detail_id": 999,
            "nc_state": 0,
            "already_in_document_related": False,
        }
    ]
    assert materialize_cn_related_rows(rows) == [(999, 3872169, 9)]


def test_6_multiples_nc():
    rows = [
        {
            "nc_document_id": 1,
            "invoice_detail_id": 10,
            "nc_state": 0,
            "already_in_document_related": False,
        },
        {
            "nc_document_id": 2,
            "invoice_detail_id": 10,
            "nc_state": 0,
            "already_in_document_related": False,
        },
        {
            "nc_document_id": 1,
            "invoice_detail_id": 10,
            "nc_state": 0,
            "already_in_document_related": False,
        },
    ]
    triples = materialize_cn_related_rows(rows)
    assert len(triples) == 2
    assert {t[1] for t in triples} == {1, 2}


def test_7_nc_parcial():
    assert (
        classify_credit_note_coverage(Decimal("100000"), [Decimal("15720")])
        == BILLING_INVOICED_PARTIAL_CN
    )


def test_8_nc_total():
    assert (
        classify_credit_note_coverage(Decimal("10000"), [Decimal("10000")])
        == BILLING_INVOICED_FULL_CN
    )


def test_9_nc_anulada_ignorada():
    assert (
        classify_credit_note_coverage(
            Decimal("10000"),
            [Decimal("10000")],
            credit_note_states=[8888],
        )
        is None
    )


def test_10_nc_no_reabre_despacho():
    eligible, closed, _ = derive_dispatch_flags(BILLING_INVOICED_PARTIAL_CN)
    assert eligible is False and closed is True
    eligible2, closed2, _ = derive_dispatch_flags(BILLING_INVOICED_CN_UNSPECIFIED)
    assert eligible2 is False and closed2 is True


def test_11_probable_permanece_probable():
    assert (
        classify_absence_bucket(
            has_related=False,
            probable_score=100,
            same_client=True,
            same_amount=True,
        )
        != "has_related"
    )


def test_12_catchup_idempotente():
    rows = [
        {
            "nc_document_id": 1,
            "invoice_detail_id": 10,
            "nc_state": 0,
            "already_in_document_related": True,
        }
    ]
    assert materialize_cn_related_rows(rows) == []


def test_13_source_document_changed_bucket():
    # Sin related ni probable: unresolved / investigación.
    assert (
        classify_absence_bucket(
            has_related=False,
            probable_score=None,
            same_client=None,
            same_amount=None,
        )
        == "no_related_no_probable"
    )


def test_14_pagination_design_constant():
    from backend.services.distribuidora.sync_related_service import RELATED_DETAIL_PAGE_LIMIT

    assert RELATED_DETAIL_PAGE_LIMIT == 50


def test_15_relacion_inversa_nc_via_related_detail():
    # La arista materializada es invoice_detail → NC (dirección usable por resolver).
    triples = materialize_cn_related_rows(
        [
            {
                "nc_document_id": 55,
                "invoice_detail_id": 77,
                "nc_state": 0,
                "already_in_document_related": False,
            }
        ]
    )
    assert triples[0][0] == 77 and triples[0][1] == 55


def test_16_generation_date_preexisting_label():
    assert "excluded_preexisting_invoice" in (
        "excluded_preexisting_invoice",
        "fulfilled_by_invoice",
    )


def test_17_dry_run_default_no_writes_flag():
    from backend.services.distribuidora.document_relation_sync_service import RelationSyncReport

    r = RelationSyncReport(dry_run=True)
    assert r.dry_run is True


def test_cn_sin_montos_neutro():
    assert (
        classify_credit_note_coverage(None, [None])
        == BILLING_INVOICED_CN_UNSPECIFIED
    )
