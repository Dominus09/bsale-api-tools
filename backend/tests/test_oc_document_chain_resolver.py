"""Tests del resolver canónico OC → factura/NC y cumplimiento de planificación."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from backend.services.distribuidora.oc_document_chain_resolver import (
    assemble_chains_from_edges,
    resolve_oc_operational_status_from_parts,
)
from backend.services.distribuidora.oc_operational_status import (
    ADMISSION_BLOCK_MESSAGE,
    BILLING_CANCELLED,
    BILLING_INVOICED,
    BILLING_INVOICED_FULL_CN,
    BILLING_INVOICED_PARTIAL_CN,
    BILLING_PENDING,
    BILLING_PROBABLE,
    DISPATCH_COMPLETED,
    DISPATCH_EXCLUDED,
    EXCLUDED_REASON_ALREADY_INVOICED,
    EXCLUDED_REASON_CANCELLED_ORDER,
    FULFILL_BY_INVOICE,
    FULFILL_EXCLUDED_CANCELLED,
    FULFILL_EXCLUDED_PREEXISTING,
    FULFILL_PENDING,
    FULFILL_UNRESOLVED,
    blocks_planning_admission,
    build_operational_status,
    plan_progress_counts,
)


def _oc(oid: int = 100, number: int = 69000, state: int = 0) -> dict:
    return {"document_id": oid, "number": number, "state": state}


def _edge(
    frm: int,
    to: int,
    dtype: int,
    *,
    number: int = 1,
    total: str | float | None = "10000",
    issued: datetime | None = None,
) -> dict:
    raw = {}
    if issued is not None:
        raw["generationDate"] = int(issued.timestamp())
    return {
        "from_document_id": frm,
        "to_document_id": to,
        "to_number": number,
        "to_document_type_id": dtype,
        "to_total_amount": Decimal(str(total)) if total is not None else None,
        "to_raw_data": raw,
        "to_emission_date": issued,
    }


TS0 = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
TS_PLAN = datetime(2026, 8, 12, 15, 0, tzinfo=timezone.utc)
TS_AFTER = datetime(2026, 8, 12, 18, 0, tzinfo=timezone.utc)
TS_BEFORE = datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc)


def test_1_oc_sin_factura_pendiente():
    st = resolve_oc_operational_status_from_parts(_oc(), [])
    assert st.billing_status == BILLING_PENDING
    assert st.planning_eligible is True
    assert st.dispatch_closed is False


def test_2_oc_factura_directa_facturada():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=50500, issued=TS0)],
    )
    assert st.billing_status == BILLING_INVOICED
    assert st.dispatch_closed is True
    assert st.planning_eligible is False
    assert st.evidence_source == "direct_related"
    assert st.confirmed_invoice["number"] == 50500


def test_3_oc_picking_factura():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [
            _edge(100, 150, 5, number=1),  # picking-like
            _edge(150, 200, 6, number=50501, issued=TS0),
        ],
    )
    assert st.billing_status == BILLING_INVOICED
    assert st.dispatch_closed is True
    assert "picking" in st.relation_path or st.chain.pickings
    assert st.evidence_source == "indirect_related"


def test_4_oc_guia_factura():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [
            _edge(100, 160, 14, number=900),  # guía / intermedio
            _edge(160, 201, 1, number=80001, issued=TS0),
        ],
    )
    assert st.billing_status == BILLING_INVOICED
    assert st.evidence_source == "indirect_related"
    assert st.relation_path[:2] == ["oc", "intermediate"]


def test_5_factura_nc_parcial():
    inv_total = "100000"
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=1, total=inv_total, issued=TS0)],
        credit_notes_by_invoice={
            200: [
                {
                    "document_id": 900,
                    "number": 18481,
                    "total_amount": Decimal("15720"),
                    "raw_data": {},
                }
            ]
        },
    )
    assert st.billing_status == BILLING_INVOICED_PARTIAL_CN
    assert st.dispatch_closed is True
    assert st.planning_eligible is False


def test_6_factura_nc_total():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=1, total="10000", issued=TS0)],
        credit_notes_by_invoice={
            200: [
                {
                    "document_id": 901,
                    "number": 18400,
                    "total_amount": Decimal("10000"),
                    "raw_data": {},
                }
            ]
        },
    )
    assert st.billing_status == BILLING_INVOICED_FULL_CN
    assert st.dispatch_closed is True


def test_7_nc_no_vuelve_a_pendiente():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=1, total="50000", issued=TS0)],
        credit_notes_by_invoice={
            200: [
                {
                    "document_id": 902,
                    "number": 1,
                    "total_amount": Decimal("50000"),
                    "raw_data": {},
                }
            ]
        },
    )
    assert st.billing_status != BILLING_PENDING
    assert st.billing_status == BILLING_INVOICED_FULL_CN
    assert blocks_planning_admission(st) is True
    assert ADMISSION_BLOCK_MESSAGE


def test_8_factura_previa_bloquea_admision():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=1, issued=TS_BEFORE)],
        planned_at=TS_PLAN,
        in_plan=False,
    )
    # Fuera de plan: ya closed → no eligible
    assert st.planning_eligible is False
    assert blocks_planning_admission(st) is True


def test_9_factura_previa_detectada_despues_excluye():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=1, issued=TS_BEFORE)],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    assert st.fulfillment_status == FULFILL_EXCLUDED_PREEXISTING
    assert st.excluded_reason == EXCLUDED_REASON_ALREADY_INVOICED
    assert st.dispatch_status == DISPATCH_EXCLUDED


def test_10_11_12_excluida_fuera_peso_denominador_margen():
    excluded = resolve_oc_operational_status_from_parts(
        _oc(100, 1),
        [_edge(100, 200, 6, issued=TS_BEFORE)],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    completed = resolve_oc_operational_status_from_parts(
        _oc(101, 2),
        [_edge(101, 201, 6, issued=TS_AFTER)],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    pending = resolve_oc_operational_status_from_parts(
        _oc(102, 3),
        [],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    # 10: excluida no aporta a eligible (proxy peso/capacidad)
    assert excluded.fulfillment_status == FULFILL_EXCLUDED_PREEXISTING

    prog = plan_progress_counts([excluded, completed, pending])
    # 11: excluded fuera del denominador
    assert prog["eligible_planned_orders"] == 2
    assert prog["excluded_orders"] == 1
    assert prog["completed_orders"] == 1
    assert prog["pending_orders"] == 1
    assert prog["completion_percent"] == 50.0
    # 12: mientras haya pending, margen bloqueado; excluded no bloquea sola
    assert prog["margin_blocked"] is True

    prog2 = plan_progress_counts([excluded, completed])
    assert prog2["eligible_planned_orders"] == 1
    assert prog2["completion_percent"] == 100.0
    assert prog2["margin_blocked"] is False
    assert prog2["fully_complete"] is True


def test_13_factura_posterior_completada():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, number=1, issued=TS_AFTER)],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    assert st.fulfillment_status == FULFILL_BY_INVOICE
    assert st.completion_source == "invoice"
    assert st.dispatch_status == DISPATCH_COMPLETED


def test_14_factura_posterior_sin_picking_adicional():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [
            _edge(100, 150, 5),
            _edge(150, 200, 6, issued=TS_AFTER),
        ],
        planned_at=TS_PLAN,
        in_plan=True,
        has_picking=False,
    )
    assert st.fulfillment_status == FULFILL_BY_INVOICE
    assert st.completion_source == "invoice"


def test_15_probable_no_es_confirmed():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [],
        probable={
            "document_id": 300,
            "number": 2715059,
            "document_type_id": 1,
            "score": 100,
            "total_amount": 1000,
            "raw_data": {"generationDate": int(TS_AFTER.timestamp())},
        },
        planned_at=TS_PLAN,
        in_plan=True,
    )
    assert st.billing_status == BILLING_PROBABLE
    assert st.dispatch_closed is False
    assert st.fulfillment_status == FULFILL_UNRESOLVED
    assert st.confirmed_invoice is None


def test_16_missing_direct_related_indirect_ok():
    chains = assemble_chains_from_edges(
        [_oc()],
        [
            _edge(100, 170, 16, number=1),
            _edge(170, 210, 6, number=999, issued=TS0),
        ],
    )
    ch = chains[100]
    assert ch.confirmed_invoices
    assert ch.evidence_source == "indirect_related"
    assert not any(p == ("oc", "invoice") for p in ch.relation_paths)


def test_17_nc_margen_via_fuente_existente_no_duplica_logica():
    """NC no reabre logística; el descuento financiero queda en módulo margen/returns."""
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [_edge(100, 200, 6, total="100000", issued=TS0)],
        credit_notes_by_invoice={
            200: [{"document_id": 9, "number": 1, "total_amount": Decimal("10000"), "raw_data": {}}]
        },
    )
    assert st.billing_status == BILLING_INVOICED_PARTIAL_CN
    assert st.dispatch_closed is True
    # Contrato: credit_notes expuestas para que margen reutilice fuente oficial
    assert len(st.credit_notes) == 1
    assert st.credit_notes[0]["number"] == 1


def test_18_resolver_batch_sin_n_plus_1_assembly():
    ocs = [_oc(i, i) for i in range(100, 110)]
    edges = [_edge(i, 1000 + i, 6, number=i, issued=TS0) for i in range(100, 110)]
    chains = assemble_chains_from_edges(ocs, edges)
    assert len(chains) == 10
    assert all(c.confirmed_invoices for c in chains.values())


def test_19_idempotencia():
    edges = [_edge(100, 200, 6, issued=TS0)]
    a = resolve_oc_operational_status_from_parts(_oc(), edges)
    b = resolve_oc_operational_status_from_parts(_oc(), edges)
    assert a.as_dict()["billing_status"] == b.as_dict()["billing_status"]
    assert a.confirmed_invoice == b.confirmed_invoice


def test_20_planificacion_llega_100():
    rows = [
        resolve_oc_operational_status_from_parts(
            _oc(100 + i, 100 + i),
            [_edge(100 + i, 200 + i, 6, issued=TS_AFTER)],
            planned_at=TS_PLAN,
            in_plan=True,
        )
        for i in range(5)
    ]
    # una excluida no impide 100 %
    rows.append(
        resolve_oc_operational_status_from_parts(
            _oc(200, 200),
            [_edge(200, 300, 6, issued=TS_BEFORE)],
            planned_at=TS_PLAN,
            in_plan=True,
        )
    )
    prog = plan_progress_counts(rows)
    assert prog["excluded_orders"] == 1
    assert prog["eligible_planned_orders"] == 5
    assert prog["completed_orders"] == 5
    assert prog["completion_percent"] == 100.0
    assert prog["fully_complete"] is True
    assert prog["margin_blocked"] is False


def test_pending_in_plan():
    st = resolve_oc_operational_status_from_parts(
        _oc(),
        [],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    assert st.fulfillment_status == FULFILL_PENDING


def test_cancelled_without_related_is_cancelled_not_pending():
    st = resolve_oc_operational_status_from_parts(
        _oc(state=8888),
        [],
    )
    assert st.billing_status == BILLING_CANCELLED
    assert st.billing_status != BILLING_PENDING
    assert st.planning_eligible is False
    assert st.dispatch_closed is True
    assert st.dispatch_status == DISPATCH_EXCLUDED
    assert blocks_planning_admission(st) is True


def test_cancelled_dominates_probable():
    st = resolve_oc_operational_status_from_parts(
        _oc(state=8888),
        [],
        probable={
            "document_id": 999,
            "number": 1,
            "document_type_id": 6,
            "score": 100,
            "total_amount": 10000,
            "raw_data": {},
        },
    )
    assert st.billing_status == BILLING_CANCELLED
    assert st.billing_status != BILLING_PROBABLE


def test_cancelled_in_plan_excluded_cancelled_order():
    st = resolve_oc_operational_status_from_parts(
        _oc(state=8888),
        [],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    assert st.fulfillment_status == FULFILL_EXCLUDED_CANCELLED
    assert st.excluded_reason == EXCLUDED_REASON_CANCELLED_ORDER
    assert st.dispatch_status == DISPATCH_EXCLUDED


def test_cancelled_does_not_block_100_percent():
    cancelled = resolve_oc_operational_status_from_parts(
        _oc(100, 1, state=8888),
        [],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    completed = resolve_oc_operational_status_from_parts(
        _oc(101, 2),
        [_edge(101, 201, 6, issued=TS_AFTER)],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    prog = plan_progress_counts([cancelled, completed])
    assert cancelled.fulfillment_status == FULFILL_EXCLUDED_CANCELLED
    assert prog["eligible_planned_orders"] == 1
    assert prog["excluded_orders"] == 1
    assert prog["completion_percent"] == 100.0
    assert prog["fully_complete"] is True
    assert prog["margin_blocked"] is False


def test_active_without_related_remains_pending():
    st = resolve_oc_operational_status_from_parts(
        _oc(state=0),
        [],
    )
    assert st.billing_status == BILLING_PENDING
    assert st.planning_eligible is True
    assert st.dispatch_closed is False


def test_confirmed_then_cancelled_state_wins():
    """
    Semántica real Bsale: documents.state != 0 ⇒ anulada.
    Si la OC queda anulada tras haber tenido related 1/6, cancelled domina
    (no inventar híbrido facturada+anulada).
    """
    st = resolve_oc_operational_status_from_parts(
        _oc(state=8888),
        [_edge(100, 200, 6, number=1, issued=TS_AFTER)],
        planned_at=TS_PLAN,
        in_plan=True,
    )
    assert st.billing_status == BILLING_CANCELLED
    assert st.fulfillment_status == FULFILL_EXCLUDED_CANCELLED
    assert st.excluded_reason == EXCLUDED_REASON_CANCELLED_ORDER
    assert st.confirmed_invoice is None
