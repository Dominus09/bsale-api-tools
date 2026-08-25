"""La identidad visible de la OC no puede ser reemplazada por logística."""

from __future__ import annotations

from backend.services import order_weight_service as service
from backend.utils.order_weight_calc import (
    WEIGHT_MATCH_PENDING_WARNING,
    aggregate_order_summary,
    compute_line_from_row,
    logistics_identity_consistent,
)


REAL_OC_CASES = [
    {
        "folio": 68714,
        "producto": "CREMA NESTLE",
        "variante": "DE LECHE 157 GR (SEC 48)",
        "barcode": "7613032414580",
        "stale_product": "PREPIZZA IDEAL",
        "stale_barcode": "7803403232114",
    },
    {
        "folio": 68754,
        "producto": "CLORINDA",
        "variante": "CLORO TRADICIONAL 1.0 LT (SEC 15)",
        "barcode": "7805080100021",
        "stale_product": "JALEA NESTLE",
        "stale_barcode": "7613035281462",
    },
    {
        "folio": 68750,
        "producto": "YERBA MATE PIPORE",
        "variante": "250 GR (SEC 24)",
        "barcode": "7793750000057",
        "stale_product": "ARROZ MIRAFLORES",
        "stale_barcode": "7802615006568",
    },
    {
        "folio": 68750,
        "producto": "VELAS LUMINOSA",
        "variante": "GIGANTE 9 HRS x 4 UNID (SEC 10)",
        "barcode": "7805025692079",
        "stale_product": "LEMON STONES",
        "stale_barcode": "7802100006431",
    },
    {
        "folio": 68749,
        "producto": "YERBA MATE AGUANTADORA",
        "variante": "250 GR (SEC 24)",
        "barcode": "7790326000312",
        "stale_product": "BON O BON",
        "stale_barcode": "78033948",
    },
    {
        "folio": 68749,
        "producto": "YERBA MATE TARAGUI",
        "variante": "250 GR (SEC 10)",
        "barcode": "7790387100310",
        "stale_product": "CAFÉ ECCO",
        "stale_barcode": "7802950008715",
    },
    {
        "folio": 68747,
        "producto": "VELAS LUMINOSA",
        "variante": "GIGANTE 9 HRS x 4 UNID (SEC 10)",
        "barcode": "7805025692079",
        "stale_product": "LEMON STONES",
        "stale_barcode": "7802100006431",
    },
    {
        "folio": 68747,
        "producto": "SAL LOBOS",
        "variante": "GRUESA 1 KG (SEC 10)",
        "barcode": "7803600041236",
        "stale_product": "MARGARINA MASAPLUS",
        "stale_barcode": "7790813002843",
    },
]


def _source_row(
    *,
    producto: str,
    variante: str,
    barcode: str,
    **overrides,
):
    row = {
        "detail_id": 9022201,
        "line_number": 0,
        "variant_id": 8879,
        "codigo": barcode,
        "producto": variante,
        "cantidad_unitaria": 12,
        "units_per_box": 1,
        "peso_unitario_kg": 0.75,
        "peso_caja_kg": 0.75,
        "products_master_id": 5037,
        "product_name": "PREPIZZA IDEAL",
        "variante": "1 METRO 3 UNID 750 GR (SEC 1)",
        "bsale_product_name": producto,
        "logistics_completed": False,
        "pm_updated_at": None,
        "last_bsale_sync_at": None,
        "height_cm": None,
        "width_cm": None,
        "length_cm": None,
        "product_id": 4028,
        "barcode": barcode,
        "codigo_interno": barcode,
        "matched_barcode": "7803403232114",
        "join_variant_ok": True,
        "join_barcode_ok": False,
        "exists_in_pm": True,
        "logistics_match_status": "matched_variant",
        "conflicting_products_master_ids": [],
    }
    row.update(overrides)
    return row


def _cream_oc_row(**overrides):
    return _source_row(
        producto="CREMA NESTLE",
        variante="DE LECHE 157 GR (SEC 48)",
        barcode="7613032414580",
        **overrides,
    )


def test_source_identity_preserved_when_logistics_points_to_other_product():
    line = compute_line_from_row(_cream_oc_row())

    assert line["producto"] == "CREMA NESTLE"
    assert "PREPIZZA" not in (line["producto"] or "")
    assert "PREPIZZA" not in (line["variante"] or "")
    assert line["variante"] == "DE LECHE 157 GR (SEC 48)"
    assert line["codigo"] == "7613032414580"
    assert line["cantidad_unitaria"] == 12


def test_reused_variant_without_reliable_barcode_is_conflict():
    line = compute_line_from_row(_cream_oc_row())

    assert line["match_status"] == "match_conflict"
    assert line["peso_unitario_kg"] is None
    assert line["peso_linea_kg"] == 0
    assert line["estado_linea"] == "sin_peso"
    assert line["weight_match"] == {
        "strategy": "match_conflict",
        "status": "match_conflict",
        "source": None,
        "warning": WEIGHT_MATCH_PENDING_WARNING,
    }
    assert "match_conflict" in (line.get("warnings") or [])


def test_reused_variant_with_unique_source_barcode_uses_barcode_weight():
    line = compute_line_from_row(
        _cream_oc_row(
            product_name="PREPIZZA IDEAL",
            matched_barcode="7613032414580",
            products_master_id=1543,
            peso_unitario_kg=0.28625,
            peso_caja_kg=13.74,
            units_per_box=48,
            join_variant_ok=False,
            join_barcode_ok=True,
            logistics_match_status="matched_barcode_after_variant_conflict",
        )
    )

    assert line["producto"] == "CREMA NESTLE"
    assert "PREPIZZA" not in (line["producto"] or "")
    assert line["peso_unitario_kg"] == 0.28625
    assert line["peso_linea_kg"] == 3.435
    assert line["weight_match"]["strategy"] == "matched_barcode_after_variant_conflict"
    assert line["weight_match"]["status"] == "ok"
    assert line["weight_match"]["source"] == "barcode"
    assert line["weight_match"]["warning"] is None


def test_barcode_conflict_does_not_assign_weight():
    line = compute_line_from_row(
        _cream_oc_row(
            product_name="CREMA NESTLE",
            matched_barcode="7803403232114",
            logistics_match_status="matched_barcode",
            join_variant_ok=False,
            join_barcode_ok=True,
        )
    )

    assert line["producto"] == "CREMA NESTLE"
    assert line["match_status"] == "match_conflict"
    assert line["peso_unitario_kg"] is None
    assert line["weight_match"]["status"] == "match_conflict"


def test_correct_product_keeps_correct_weight():
    line = compute_line_from_row(
        _cream_oc_row(
            product_name="CREMA NESTLE",
            matched_barcode="7613032414580",
            products_master_id=1543,
            peso_unitario_kg=0.28625,
            peso_caja_kg=13.74,
            units_per_box=48,
            join_variant_ok=True,
            logistics_match_status="matched_variant",
        )
    )

    assert line["producto"] == "CREMA NESTLE"
    assert line["variante"] == "DE LECHE 157 GR (SEC 48)"
    assert line["peso_unitario_kg"] == 0.28625
    assert line["peso_linea_kg"] == 3.435
    assert line["match_status"] == "matched_variant"
    assert line["weight_match"]["source"] == "variant"
    assert line["weight_match"]["warning"] is None


def test_correct_product_with_unavailable_weight():
    line = compute_line_from_row(
        _cream_oc_row(
            product_name=None,
            matched_barcode=None,
            products_master_id=None,
            peso_unitario_kg=None,
            peso_caja_kg=None,
            join_variant_ok=False,
            join_barcode_ok=False,
            exists_in_pm=False,
            logistics_match_status="pending",
        )
    )

    assert line["producto"] == "CREMA NESTLE"
    assert line["peso_unitario_kg"] is None
    assert line["estado_linea"] == "sin_peso"
    assert line["weight_match"]["status"] == "pending"


def test_logistics_name_never_replaces_product_name():
    line = compute_line_from_row(
        _cream_oc_row(
            product_name="PREPIZZA IDEAL",
            bsale_product_name="CREMA NESTLE",
            matched_barcode="7613032414580",
            peso_unitario_kg=0.28625,
            join_variant_ok=False,
            join_barcode_ok=True,
            logistics_match_status="matched_barcode_after_variant_conflict",
        )
    )
    assert line["producto"] == "CREMA NESTLE"
    assert "PREPIZZA" not in (line["producto"] or "")
    assert line["peso_unitario_kg"] == 0.28625


def test_name_mismatch_is_not_used_for_weight_matching():
    assert logistics_identity_consistent(
        source_barcode="7613032414580",
        matched_barcode="7613032414580",
        source_product_name="CREMA NESTLE",
        matched_product_name="PREPIZZA IDEAL",
    )


def test_conflicted_weight_does_not_enter_truck_capacity():
    conflicted = compute_line_from_row(_cream_oc_row(peso_unitario_kg=12.0))
    ok = compute_line_from_row(
        _cream_oc_row(
            detail_id=1,
            product_name="CREMA NESTLE",
            matched_barcode="7613032414580",
            peso_unitario_kg=0.28625,
            logistics_match_status="matched_barcode",
            join_variant_ok=False,
            join_barcode_ok=True,
        )
    )
    summary = aggregate_order_summary([conflicted, ok])
    payload = service.build_weight_payload(summary, lines=[conflicted, ok])

    assert conflicted["peso_linea_kg"] == 0
    assert summary["peso_total_kg"] == ok["peso_linea_kg"]
    assert payload["value_kg"] == ok["peso_linea_kg"]
    assert payload["status"] == "partial"
    assert payload["lines_missing_weight"] == 1


def test_all_identity_conflicts_keep_weight_out_of_capacity():
    line = compute_line_from_row(_cream_oc_row())
    summary = aggregate_order_summary([line])
    payload = service.build_weight_payload(summary, lines=[line])

    assert payload["value_kg"] is None
    assert payload["status"] == "unavailable"
    assert payload["reason"] == "logistics_match_conflict"


def test_order_lines_preserve_source_identity_through_service():
    from backend.tests.test_order_weight_logistics_matching import RowsCursor

    cur = RowsCursor([_cream_oc_row()])
    lines = service.compute_order_lines(cur, document_id=3853469, company_id=3)

    assert len(lines) == 1
    assert lines[0]["producto"] == "CREMA NESTLE"
    assert "PREPIZZA" not in (lines[0]["producto"] or "")
    assert lines[0]["peso_unitario_kg"] is None
    assert lines[0]["match_status"] == "match_conflict"
    assert lines[0]["weight_match"]["status"] == "match_conflict"


def test_identity_consistent_helper_uses_barcode_only():
    assert logistics_identity_consistent(
        source_barcode="7613032414580",
        matched_barcode="7613032414580",
        source_product_name="CREMA NESTLE",
        matched_product_name="CREMA NESTLE",
    )
    assert not logistics_identity_consistent(
        source_barcode="7613032414580",
        matched_barcode="7803403232114",
        source_product_name="CREMA NESTLE",
        matched_product_name="PREPIZZA IDEAL",
    )


def test_real_oc_cases_keep_source_identity_and_prefer_unique_barcode():
    for case in REAL_OC_CASES:
        conflict = compute_line_from_row(
            _source_row(
                producto=case["producto"],
                variante=case["variante"],
                barcode=case["barcode"],
                product_name=case["stale_product"],
                matched_barcode=case["stale_barcode"],
            )
        )
        rescued = compute_line_from_row(
            _source_row(
                producto=case["producto"],
                variante=case["variante"],
                barcode=case["barcode"],
                product_name=case["stale_product"],
                matched_barcode=case["barcode"],
                join_variant_ok=False,
                join_barcode_ok=True,
                logistics_match_status="matched_barcode_after_variant_conflict",
                peso_unitario_kg=0.2,
            )
        )
        assert conflict["producto"] == case["producto"], case
        assert case["stale_product"] not in (conflict["producto"] or "")
        assert conflict["weight_match"]["status"] == "match_conflict"
        assert rescued["producto"] == case["producto"], case
        assert case["stale_product"] not in (rescued["producto"] or "")
        assert rescued["peso_unitario_kg"] == 0.2
        assert rescued["weight_match"]["strategy"] == (
            "matched_barcode_after_variant_conflict"
        )
        assert rescued["weight_match"]["source"] == "barcode"


def test_order_lines_sql_has_no_name_fuzzy_weight_match():
    sql = service._ORDER_LINES_SQL
    assert "UPPER(BTRIM(pl_v.product_name))" not in sql
    assert "UPPER(BTRIM(pl_b.product_name))" not in sql
    assert "matched_barcode_after_variant_conflict" in sql
    assert "matched_barcode_after_identity_conflict" not in sql
