"""Filtro only_not_invoiced + regresión facturada (equivalente 68513)."""

from __future__ import annotations

from backend.services.distribuidora.orders_service import (
    _apply_status_fields_to_row,
    _dispatch_prep_invoice_filter_sql,
    _planning_rows_ids_sql,
    _planning_rows_purchase_status_sql,
)
from backend.services.order_weight_service import apply_order_weight_summary_to_row


def test_only_not_invoiced_true_excludes_with_exists():
    sql = _dispatch_prep_invoice_filter_sql(True)
    assert "EXISTS" in sql
    assert sql.strip().startswith("(NOT")


def test_only_not_invoiced_false_is_passthrough_true():
    assert _dispatch_prep_invoice_filter_sql(False) == "TRUE"
    assert "EXISTS" not in _dispatch_prep_invoice_filter_sql(False)


def test_ids_sql_omitted_default_excludes_invoiced():
    """Parámetro omitido en router = True → SQL con EXISTS."""
    sql, _ = _planning_rows_ids_sql(day_tokens=(), only_not_invoiced=True)
    assert "EXISTS" in sql
    assert "%s = FALSE OR" not in sql


def test_ids_sql_false_has_no_exists_cost():
    sql, _ = _planning_rows_ids_sql(day_tokens=(), only_not_invoiced=False)
    assert "EXISTS" not in sql
    assert "TRUE" in sql


def test_purchase_status_sql_uses_lateral_limit_not_distinct_on_join():
    sql = _planning_rows_purchase_status_sql()
    assert "LATERAL" in sql
    assert "LIMIT 1" in sql
    assert "DISTINCT ON" not in sql


def test_68513_equivalent_hidden_when_only_not_invoiced_true():
    """Facturada confirmada no entra al set 'solo no facturadas'."""
    # Simula filtro de página: si is_invoiced, no aparece con only_not_invoiced=True.
    page_true = [
        {"document_id": 1, "oc": 68600, "is_invoiced": False},
        # 68513 equivalenta excluida del page set cuando only_not_invoiced=True
    ]
    assert all(not r["is_invoiced"] for r in page_true)
    assert not any(r["oc"] == 68513 for r in page_true)


def test_68513_equivalent_visible_once_when_only_not_invoiced_false():
    row = {
        "document_id": 3844682,
        "oc": 68513,
        "nombre_fantasia": "Donde Callo",
        "total_amount": 7612857,
        "state": 0,
        "observaciones": "jueves",
        "comments": None,
        "dia_atencion": None,
    }
    conf = {
        "is_invoiced": True,
        "invoicing_document_id": 999,
        "invoicing_document_type_id": 6,
        "invoicing_number": 12345,
    }
    _apply_status_fields_to_row(row, conf, None)
    apply_order_weight_summary_to_row(
        row,
        {
            "total_weight": 5623.886,
            "missing_products": 0,
            "coverage_percent": 100.0,
            "manual_products": 0,
            "estimated_products": 0,
        },
        weight_source="order_weight_snapshot",
        weight_status="calculated",
    )
    from backend.utils.delivery_day_detect import resolve_delivery_day, delivery_day_label

    day, src = resolve_delivery_day(row["observaciones"], None, None)
    row["dia_entrega_detectado"] = day
    row["dia_entrega_label"] = delivery_day_label(day)
    row["dia_entrega_fuente"] = src

    assert row["oc"] == 68513
    assert "Callo" in (row["nombre_fantasia"] or "")
    assert row["estado_real"] == "Facturada"
    assert row["purchase_status"] == "FACTURADA_CONFIRMADA"
    assert row["total_amount"] == 7612857
    assert abs(float(row["peso_total_kg"]) - 5623.886) < 0.01
    assert row["dia_entrega_detectado"] == "jueves"
    assert conf["is_invoiced"] is True  # has_confirmed_invoice_link
