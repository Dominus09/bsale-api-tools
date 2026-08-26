"""Regresión canario OC 68677: factura + NC total no reabre Pre-despacho."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from backend.services.distribuidora.oc_document_chain_resolver import (
    resolve_oc_operational_status_from_parts,
)
from backend.services.distribuidora.oc_operational_status import (
    BILLING_CANCELLED,
    BILLING_INVOICED,
    BILLING_INVOICED_FULL_CN,
    BILLING_INVOICED_PARTIAL_CN,
    BILLING_PENDING,
    BILLING_PROBABLE,
    is_predespacho_pending_row,
)
from backend.services.distribuidora.orders_service import (
    _dispatch_prep_invoice_filter_sql,
)
from backend.utils.distribuidora_oc_sql import (
    OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL,
    OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL,
)

TS = datetime(2026, 8, 6, 15, 0, tzinfo=timezone.utc)


def _oc_68677() -> dict:
    return {"document_id": 3852324, "number": 68677, "state": 0}


def _edge_invoice(*, total: str = "310350") -> dict:
    return {
        "from_document_id": 3852324,
        "to_document_id": 3853417,
        "to_number": 50367,
        "to_document_type_id": 6,
        "to_total_amount": Decimal(total),
        "to_raw_data": {"generationDate": int(TS.timestamp())},
        "to_emission_date": TS,
    }


def test_68677_full_credit_note_closes_dispatch_not_pending():
    st = resolve_oc_operational_status_from_parts(
        _oc_68677(),
        [_edge_invoice()],
        credit_notes_by_invoice={
            3853417: [
                {
                    "document_id": 99918408,
                    "number": 18408,
                    "total_amount": Decimal("310350"),
                    "raw_data": {},
                }
            ]
        },
    )
    assert st.billing_status == BILLING_INVOICED_FULL_CN
    assert st.dispatch_closed is True
    assert st.planning_eligible is False
    assert is_predespacho_pending_row(
        billing_status=st.billing_status,
        planning_eligible=st.planning_eligible,
        dispatch_closed=st.dispatch_closed,
    ) is False


def test_factura_plus_partial_cn_still_closed():
    st = resolve_oc_operational_status_from_parts(
        _oc_68677(),
        [_edge_invoice()],
        credit_notes_by_invoice={
            3853417: [
                {
                    "document_id": 1,
                    "number": 1,
                    "total_amount": Decimal("100000"),
                    "raw_data": {},
                }
            ]
        },
    )
    assert st.billing_status == BILLING_INVOICED_PARTIAL_CN
    assert st.dispatch_closed is True
    assert st.planning_eligible is False


def test_factura_sin_nc_closed():
    st = resolve_oc_operational_status_from_parts(
        _oc_68677(),
        [_edge_invoice()],
    )
    assert st.billing_status == BILLING_INVOICED
    assert st.dispatch_closed is True
    assert st.planning_eligible is False


def test_oc_sin_factura_pending():
    st = resolve_oc_operational_status_from_parts(_oc_68677(), [])
    assert st.billing_status == BILLING_PENDING
    assert st.planning_eligible is True
    assert st.dispatch_closed is False
    assert is_predespacho_pending_row(
        billing_status=st.billing_status,
        planning_eligible=st.planning_eligible,
        dispatch_closed=st.dispatch_closed,
    ) is True


def test_anulada_not_pending_predespacho():
    st = resolve_oc_operational_status_from_parts(
        {"document_id": 1, "number": 1, "state": 8888},
        [],
    )
    assert st.billing_status == BILLING_CANCELLED
    assert is_predespacho_pending_row(
        billing_status=st.billing_status,
        planning_eligible=st.planning_eligible,
        dispatch_closed=st.dispatch_closed,
    ) is False


def test_probable_without_confirmed_not_auto_promoted():
    st = resolve_oc_operational_status_from_parts(
        _oc_68677(),
        [],
        probable={
            "document_id": 300,
            "number": 50367,
            "document_type_id": 6,
            "score": 100,
            "total_amount": 310350,
            "raw_data": {},
        },
    )
    assert st.billing_status == BILLING_PROBABLE
    assert st.dispatch_closed is False
    assert st.confirmed_invoice is None


def test_not_invoiced_sql_uses_related_document_type_not_documents_join():
    sql = OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL
    assert "related_document_type IN (1, 6)" in sql
    assert "inv.document_type_id IN (1, 6)" not in sql
    assert "INNER JOIN distribuidora.documents inv" not in sql
    assert "related_document_type IN (1, 6)" in OC_PURCHASE_IS_INVOICED_BY_RELATED_SQL


def test_dispatch_prep_filter_uses_related_type():
    sql = _dispatch_prep_invoice_filter_sql(True)
    assert "related_document_type IN (1, 6)" in sql
    assert "INNER JOIN distribuidora.documents inv" not in sql


def test_orphan_related_edge_still_confirms_invoice():
    """Canario 68677: related apunta a doc ausente en documents → sigue closed."""
    st = resolve_oc_operational_status_from_parts(
        _oc_68677(),
        [
            {
                "from_document_id": 3852324,
                "to_document_id": 3853417,
                "to_number": None,  # documents row missing
                "to_document_type_id": 6,  # from related_document_type
                "to_total_amount": None,
                "to_raw_data": {},
                "to_emission_date": None,
            }
        ],
    )
    assert st.billing_status == BILLING_INVOICED
    assert st.dispatch_closed is True
    assert st.planning_eligible is False
    assert is_predespacho_pending_row(
        billing_status=st.billing_status,
        planning_eligible=st.planning_eligible,
        dispatch_closed=st.dispatch_closed,
    ) is False
