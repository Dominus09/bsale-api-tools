"""Tests control de precios por lista (sin PG, sin ventas)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from backend.services.analytics.money import D
from backend.services.analytics.price_list_control import (
    PriceControlStatus,
    actual_markup_pct,
    compute_price_list_control_row,
    gross_margin_pct,
    recommended_gross_price,
)
from backend.services.price_list_control_service import (
    list_price_list_control_rows,
    summarize_price_list_control,
)


def test_markup_and_margin_on_price():
    # 1–4: costo 10000, precio 12500 → recargo 25%, margen sobre precio 20%
    cost = D("10000")
    price = D("12500")
    assert actual_markup_pct(price, cost) == D("25.00")
    assert gross_margin_pct(price, cost) == D("20.00")


def test_within_policy_22_30():
    # 5: regla 22–30 → within_policy
    m = compute_price_list_control_row(
        gross_price=D("12500"),
        reference_gross_cost=D("10000"),
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
    )
    assert m.status == PriceControlStatus.WITHIN_POLICY
    assert m.actual_markup_pct == D("25.00")


def test_below_minimum():
    # 6
    m = compute_price_list_control_row(
        gross_price=D("11000"),
        reference_gross_cost=D("10000"),
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
    )
    assert m.status == PriceControlStatus.BELOW_MINIMUM


def test_above_maximum():
    # 7
    m = compute_price_list_control_row(
        gross_price=D("14000"),
        reference_gross_cost=D("10000"),
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
    )
    assert m.status == PriceControlStatus.ABOVE_MAXIMUM


def test_missing_cost():
    # 8
    m = compute_price_list_control_row(
        gross_price=D("12500"),
        reference_gross_cost=None,
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
    )
    assert m.status == PriceControlStatus.MISSING_COST


def test_missing_price():
    # 9
    m = compute_price_list_control_row(
        gross_price=None,
        reference_gross_cost=D("10000"),
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
    )
    assert m.status == PriceControlStatus.MISSING_PRICE


def test_missing_rule():
    # 10
    m = compute_price_list_control_row(
        gross_price=D("12500"),
        reference_gross_cost=D("10000"),
        min_markup_pct=None,
        max_markup_pct=None,
        has_rule=False,
    )
    assert m.status == PriceControlStatus.MISSING_RULE


def test_cost_fallback_resolution_via_service_mock():
    # 11: sin recepción → variant_cost.average_cost_gross
    base = [
        {
            "company_id": 1,
            "product_type_id": 10,
            "product_type_name": "Bebidas",
            "product_name": "Producto A",
            "variant_id": 100,
            "variant_name": "Var A",
            "barcode": "779",
            "sku": "SKU-A",
            "price_list_id": 1,
            "price_list_name": "Mayorista",
            "stock_quantity": Decimal("5"),
            "gross_price": Decimal("12500"),
            "min_markup_pct": Decimal("22"),
            "max_markup_pct": Decimal("30"),
            "has_rule": True,
        }
    ]
    vc = [
        {
            "variant_id": 100,
            "average_cost_net": Decimal("8403"),
            "average_cost_gross": Decimal("10000"),
            "last_update": date(2026, 7, 1),
            "cost_source": "bsale",
        }
    ]

    def executor(sql: str, params: tuple):
        if "FROM bsale.variant_prices" in sql:
            return base
        if "FROM analytics.cost_reception_history" in sql:
            return []
        if "FROM bsale.variant_cost" in sql:
            return vc
        raise AssertionError(f"Unexpected SQL: {sql[:80]}")

    rows = list_price_list_control_rows(
        executor, company_id=1, as_of=date(2026, 7, 20)
    )
    assert len(rows) == 1
    assert rows[0]["reference_gross_cost"] == 10000.0
    assert rows[0]["cost_source"] == "variant_cost.average_cost_gross"
    assert rows[0]["resolution_reason"] == "variant_cost_fallback"
    assert rows[0]["actual_markup_pct"] == 25.0
    assert "units_sold" not in rows[0]
    assert "sold_units" not in rows[0]


def test_cost_outlier_status_priority():
    # 12
    m = compute_price_list_control_row(
        gross_price=D("12500"),
        reference_gross_cost=D("10000"),
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
        is_outlier=True,
    )
    assert m.status == PriceControlStatus.COST_OUTLIER
    assert m.policy_compliance == PriceControlStatus.WITHIN_POLICY


def test_recommended_prices():
    # 13–14
    assert recommended_gross_price(D("10000"), D("22")) == D("12200")
    assert recommended_gross_price(D("10000"), D("30")) == D("13000")
    m = compute_price_list_control_row(
        gross_price=D("12000"),
        reference_gross_cost=D("10000"),
        min_markup_pct=D("22"),
        max_markup_pct=D("30"),
        has_rule=True,
    )
    assert m.minimum_recommended_gross_price == D("12200")
    assert m.maximum_recommended_gross_price == D("13000")
    assert m.price_adjustment_to_minimum == D("200.0000")


def test_one_row_per_price_list():
    # 15
    base = [
        {
            "company_id": 1,
            "product_type_id": 10,
            "product_type_name": "Bebidas",
            "product_name": "Producto A",
            "variant_id": 100,
            "variant_name": "Var A",
            "barcode": "779",
            "sku": "SKU-A",
            "price_list_id": 1,
            "price_list_name": "Lista 1",
            "stock_quantity": Decimal("5"),
            "gross_price": Decimal("12500"),
            "min_markup_pct": Decimal("22"),
            "max_markup_pct": Decimal("30"),
            "has_rule": True,
        },
        {
            "company_id": 1,
            "product_type_id": 10,
            "product_type_name": "Bebidas",
            "product_name": "Producto A",
            "variant_id": 100,
            "variant_name": "Var A",
            "barcode": "779",
            "sku": "SKU-A",
            "price_list_id": 2,
            "price_list_name": "Lista 2",
            "stock_quantity": Decimal("5"),
            "gross_price": Decimal("13000"),
            "min_markup_pct": Decimal("22"),
            "max_markup_pct": Decimal("30"),
            "has_rule": True,
        },
    ]
    receptions = [
        {
            "id": 1,
            "variant_id": 100,
            "cost_net": Decimal("8403"),
            "admission_date": date(2026, 6, 1),
            "reception_id": 9,
            "iva_amount": Decimal("1597"),
            "other_taxes": Decimal("0"),
            "cost_bruto_erp": Decimal("10000"),
            "reception_type": "recepcion_normal",
        }
    ]

    def executor(sql: str, params: tuple):
        if "FROM bsale.variant_prices" in sql:
            return base
        if "FROM analytics.cost_reception_history" in sql:
            return receptions
        if "FROM bsale.variant_cost" in sql:
            return []
        raise AssertionError(sql[:80])

    rows = list_price_list_control_rows(
        executor, company_id=1, as_of=date(2026, 7, 20)
    )
    assert len(rows) == 2
    assert {r["price_list_id"] for r in rows} == {1, 2}
    assert all(r["reference_gross_cost"] == 10000.0 for r in rows)
    # 16: nunca unidades vendidas
    for r in rows:
        assert "units_sold" not in r
        assert "quantity_sold" not in r
        assert "sold_units" not in r


def test_summary_kpis_no_sales_keys():
    summary = summarize_price_list_control(
        [
            {"status": "within_policy"},
            {"status": "below_minimum"},
            {"status": "missing_cost"},
        ]
    )
    assert summary["evaluated_pairs"] == 3
    assert summary["below_minimum"] == 1
    assert "units_sold" not in summary
    assert "revenue" not in summary
