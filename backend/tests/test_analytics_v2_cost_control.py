"""Analítica V2 — Control de Costos: servicio, contratos SQL sobre RAW y paginación."""

from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from backend.services.analytics_v2 import cost_control_repo as repo_mod
from backend.services.analytics_v2.access import CompanyScope
from backend.services.analytics_v2.contracts import HISTORY_START, MAX_PAGE_SIZE
from backend.services.analytics_v2.cost_control_repo import CostControlV2Repository
from backend.services.analytics_v2.cost_control_service import (
    CostControlNotFound,
    CostControlV2Service,
    CostControlValidationError,
)

D = Decimal
TODAY = date(2026, 10, 13)
SCOPE = CompanyScope(company_id=3, company_name="La Quillotana SpA")

ALL_SQL = {
    name: value
    for name, value in vars(repo_mod).items()
    if name.endswith("_SQL") and isinstance(value, str)
}


def _line_cols(prefix: str, **values: Any) -> dict[str, Any]:
    cols = (
        "reception_id",
        "detail_id",
        "admission_date",
        "office_id",
        "document",
        "document_number",
        "movement_class",
        "cost",
        "quantity",
        "details_complete",
    )
    return {f"{prefix}{c}": values.get(c) for c in cols}


def fact(variant_id: int, **kw: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "variant_id": variant_id,
        "code": f"SKU{variant_id}",
        "bar_code": f"780{variant_id}",
        "variant_name": None,
        "variant_state": 0,
        "product_id": variant_id * 10,
        "product_name": f"Producto {variant_id}",
        "product_state": 0,
        "product_type_id": 1,
        "product_type_name": "ABARROTES",
        "tax_ids": [1],
        "average_cost": None,
        "average_cost_fetched_at": None,
        "lc_cost_modified": False,
        "lc_quantity_modified": False,
        "lc_variant_modified": False,
        "lines_total": 0,
        "lines_comparable": 0,
        "receptions_total": 0,
        "median_comparable_quantity": None,
        "first_admission_date": None,
    }
    row.update(_line_cols("last_"))
    row.update(_line_cols("lc_"))
    row.update(_line_cols("pc_"))
    row.update(kw)
    return row


def purchase(prefix: str, reception_id: int, cost: str, when: date, *, qty: str = "12", office: int = 1):
    return _line_cols(
        prefix,
        reception_id=reception_id,
        detail_id=reception_id * 100,
        admission_date=when,
        office_id=office,
        document="Factura",
        document_number="1234",
        movement_class="factura",
        cost=D(cost),
        quantity=D(qty),
        details_complete=True,
    )


class FakeRepo(CostControlV2Repository):
    def __init__(self, facts=None, lines=None, alerts=None, reception=None, packs=None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._facts = facts or []
        self._lines = lines or []
        self._alerts = alerts or []
        self._reception = reception
        self._packs = packs or []

    def _rec(self, name, **kw):
        self.calls.append((name, kw))

    def variant_facts(self, **kw):
        self._rec("variant_facts", **kw)
        return self._facts

    def variant_lines(self, **kw):
        self._rec("variant_lines", **kw)
        return self._lines

    def alert_candidates(self, **kw):
        self._rec("alert_candidates", **kw)
        return self._alerts

    def movement_summary(self, **kw):
        self._rec("movement_summary", **kw)
        return [{"movement_class": "factura", "lines": 10, "receptions": 2, "comparable_lines": 9}]

    def reception_header(self, **kw):
        self._rec("reception_header", **kw)
        return self._reception

    def reception_lines(self, **kw):
        self._rec("reception_lines", **kw)
        return []

    def reception_events(self, **kw):
        self._rec("reception_events", **kw)
        return []

    def pack_details(self, **kw):
        self._rec("pack_details", **kw)
        return self._packs

    def tax_catalog(self, **kw):
        self._rec("tax_catalog", **kw)
        return [{"tax_id": 1, "name": "IVA 19%", "code": "14", "percentage": D("19"), "state": 0}]

    def offices(self, **kw):
        self._rec("offices", **kw)
        return [{"office_id": 1, "name": "Casa Matriz"}, {"office_id": 3, "name": "Sucursal 3"}]

    def product_types(self, **kw):
        self._rec("product_types", **kw)
        return []

    def sync_state(self, **kw):
        self._rec("sync_state", **kw)
        return []

    def stock_totals(self, **kw):
        self._rec("stock_totals", **kw)
        # Default: stock global positivo para no vaciar el universo CON STOCK.
        return [
            {"variant_id": int(r["variant_id"]), "stock_total": D("10"), "stock_offices": 1}
            for r in self._facts
            if r.get("variant_id") is not None
        ]


def service(repo: FakeRepo) -> CostControlV2Service:
    return CostControlV2Service(repo, SCOPE, today=TODAY, use_cache=False)


def test_stock_filter_default_con_stock_and_counts():
    """Universo filtrado antes de paginar; default CON STOCK; sin fila = SIN STOCK."""
    rows = [
        fact(1, average_cost=D("1000")),
        fact(2, average_cost=D("2000")),
        fact(3, average_cost=D("3000")),
    ]
    repo = FakeRepo(rows)

    def stock_totals(**kw):
        return [
            {"variant_id": 1, "stock_total": D("50"), "stock_offices": 2},
            {"variant_id": 2, "stock_total": D("0"), "stock_offices": 1},
            # 3: sin fila
        ]

    repo.stock_totals = stock_totals  # type: ignore[method-assign]
    svc = service(repo)

    con = svc.list_products(stock_filter="with", page_size=50)
    assert con["stock_filter"] == "with"
    assert {i["variant_id"] for i in con["items"]} == {1}
    assert con["total"] == 1
    assert con["items"][0]["stock"]["state"] == "con_stock"

    sin = svc.list_products(stock_filter="without", page_size=50)
    assert {i["variant_id"] for i in sin["items"]} == {2, 3}
    assert {i["stock"]["state"] for i in sin["items"]} == {"cero", "sin_sincronizar"}

    todos = svc.list_products(stock_filter="all", page_size=50)
    assert todos["total"] == 3

    default = svc.list_products(page_size=50)
    assert default["stock_filter"] == "with" and default["total"] == 1



# ---------------------------------------------------------------- SQL sobre RAW


def test_sql_reads_only_bsale_raw_and_never_legacy_tables():
    for name, sql in ALL_SQL.items():
        schemas = set(re.findall(r"\b(bsale|bsale_raw|analytics|app|distribuidora)\.\w+", sql))
        assert schemas <= {"bsale_raw"}, (name, schemas)


def test_sql_never_selects_payload_note_or_user():
    for name, sql in ALL_SQL.items():
        lowered = sql.lower()
        if name == "PACK_DETAILS_SQL":
            # única excepción: solo la clave pack_details de productos pack, nunca el payload completo
            assert lowered.count("payload") == 1 and "p.payload -> 'pack_details'" in lowered
            assert "classification = 3" in lowered and "from bsale_raw.products p" in lowered
        else:
            assert "payload" not in lowered, name
        assert re.search(r"\bnote\b", lowered) is None, name
        assert re.search(r"\.user_id\b|\buser\b", lowered) is None, name


def test_sql_is_read_only():
    from backend.services.analytics.validate_distribuidora_source import assert_sql_is_read_only

    for sql in ALL_SQL.values():
        assert_sql_is_read_only(sql)


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple]] = []

    def __call__(self, sql: str, params: tuple) -> list[dict[str, Any]]:
        assert sql.count("%s") == len(params), sql[:120]
        self.calls.append((sql, params))
        return []


def test_every_query_is_scoped_by_company_and_params_align():
    ex = RecordingExecutor()
    repo = CostControlV2Repository(ex)
    repo.variant_facts(company_id=3, history_start=HISTORY_START)
    repo.variant_lines(company_id=3, variant_id=55, history_start=HISTORY_START)
    repo.alert_candidates(
        company_id=3,
        history_start=HISTORY_START,
        date_from=date(2026, 7, 1),
        date_to=date(2026, 10, 1),
        extreme_quantity_factor=D("10"),
        increase_pct=D("30"),
        decrease_pct=D("-30"),
    )
    repo.movement_summary(company_id=3, history_start=HISTORY_START)
    repo.reception_header(company_id=3, reception_id=9)
    repo.reception_lines(company_id=3, reception_id=9)
    repo.reception_events(company_id=3, reception_id=9)
    repo.tax_catalog(company_id=3)
    repo.offices(company_id=3)
    repo.product_types(company_id=3)
    repo.sync_state(company_id=3)
    assert len(ex.calls) == 11
    for sql, params in ex.calls:
        assert params[0] == 3
        # cada tabla RAW referenciada en FROM/JOIN debe quedar acotada por company_id
        tables = re.findall(r"(?:FROM|JOIN)\s+bsale_raw\.(\w+)\s+(\w+)?", sql)
        for _table, alias in tables:
            if alias and alias.upper() not in ("ON", "WHERE", "CROSS"):
                assert f"{alias}.company_id" in sql, (sql[:80], alias)


def test_history_and_alerts_use_indexed_company_date_filter():
    for sql in (repo_mod.VARIANT_FACTS_SQL, repo_mod.ALERT_CANDIDATES_SQL, repo_mod.MOVEMENT_SUMMARY_SQL):
        assert "r.company_id = %s" in sql
        assert "r.admission_date >= %s" in sql
        assert "OFFSET" not in sql.upper()
    assert "AND d.variant_id = %s" in repo_mod.VARIANT_LINES_SQL


# ---------------------------------------------------------------- SKU


def test_average_vs_last_cost_are_distinct_fields():
    row = fact(
        1,
        average_cost=D("900"),
        **purchase("last_", 20, "1100", date(2026, 10, 1)),
        **purchase("lc_", 20, "1100", date(2026, 10, 1)),
        **purchase("pc_", 10, "1000", date(2026, 9, 1)),
    )
    item = service(FakeRepo([row])).get_product(1)
    assert item["bsale_average_cost"]["value"] == "900"
    assert item["bsale_average_cost"]["source"] == "bsale_raw.variant_costs"
    assert item["last_comparable"]["cost"] == "1100"
    assert item["previous_comparable"]["cost"] == "1000"
    assert item["change"] == {"amount": "100", "pct": "10.00"}
    assert item["last_comparable"]["office_name"] == "Casa Matriz"
    assert item["reliability"]["status"] == "consistente"
    assert item["gross_cost_preliminary"] == {
        "value": "1309.00",
        "status": "preliminar_no_verificado",
        "base": "ultimo_costo_comparable",
    }
    assert item["meta"]["net_cost_verified"] is False


def test_zero_average_and_no_reception_never_produces_a_cost():
    item = service(FakeRepo([fact(2, average_cost=D("0"))])).get_product(2)
    assert item["reliability"]["status"] == "sin_costo"
    assert item["last_comparable"] is None
    assert item["change"] is None
    assert item["gross_cost_preliminary"]["value"] is None
    assert item["gross_cost_preliminary"]["status"] == "sin_costo_base"


def test_only_bsale_average_when_no_comparable_purchase():
    row = fact(
        3,
        average_cost=D("500"),
        **_line_cols(
            "last_",
            reception_id=7,
            detail_id=700,
            admission_date=date(2026, 9, 1),
            office_id=1,
            document="NOTA DE CRÉDITO ELECTRÓNICA T",
            movement_class="nota_credito",
            cost=D("480"),
            quantity=D("1"),
            details_complete=True,
        ),
    )
    item = service(FakeRepo([row])).get_product(3)
    assert item["reliability"]["status"] == "solo_promedio_bsale"
    assert item["last_reception"]["movement_class"] == "nota_credito"
    assert item["gross_cost_preliminary"]["base"] == "promedio_bsale"


def test_abnormal_increase_requires_review():
    row = fact(
        4,
        average_cost=D("1000"),
        median_comparable_quantity=D("12"),
        **purchase("lc_", 30, "2500", date(2026, 10, 1)),
        **purchase("pc_", 20, "1000", date(2026, 9, 1)),
    )
    item = service(FakeRepo([row])).get_product(4)
    codes = {a["code"] for a in item["anomalies"]}
    assert codes == {"alza_anormal", "diferencia_promedio_bsale"}
    assert item["reliability"]["status"] == "requiere_revision"


def test_anticipated_with_iva_separates_payable_gross_and_margin_base():
    """IVA + anticipo carne: pagadero 1.240 (1,24); base márgenes 1.190 (1,19)."""
    row = fact(5, tax_ids=[1, 7], average_cost=D("1000"), **purchase("lc_", 30, "1000", date(2026, 10, 1)))
    repo = FakeRepo([row])
    repo.tax_catalog = lambda **kw: [  # type: ignore[method-assign]
        {"tax_id": 1, "name": "IVA 19%", "code": "14", "percentage": D("19"), "state": 0},
        {"tax_id": 7, "name": "IVA CARN. 5%", "code": "18", "percentage": D("5"), "state": 0},
    ]
    item = service(repo).get_product(5)
    assert item["tax_profile"]["status"] == "iva_anticipado"
    assert item["tax_profile"]["total_rate_pct"] == "24"
    assert item["tax_profile"]["margin_rate_pct"] == "19"
    cgc = item["commercial_gross_cost"]
    assert D(cgc["gross"]) == D("1240") and D(cgc["factor"]) == D("1.24")
    assert D(cgc["margin_gross"]) == D("1190") and D(cgc["margin_factor"]) == D("1.19")
    assert cgc["reason"] == "bruto_pagadero_incluye_anticipo_base_margen_sin_anticipo"
    assert cgc["confirmed_by_document"] is False
    bd = item["tax_breakdown"]
    assert D(bd["payable_gross_total"]) == D("1240")
    assert D(bd["margin_gross"]) == D("1190")


def test_unknown_sku_is_not_found():
    with pytest.raises(CostControlNotFound):
        service(FakeRepo([fact(1)])).get_product(999)


def test_service_always_queries_its_own_company():
    repo = FakeRepo([fact(1)])
    service(repo).list_products()
    service(repo).summary()
    assert repo.calls
    for _name, kw in repo.calls:
        assert kw["company_id"] == 3


# ---------------------------------------------------------------- listado


def _many(n: int) -> list[dict[str, Any]]:
    return [fact(i, product_name=f"Producto {i:03d}") for i in range(1, n + 1)]


def test_pagination_in_backend():
    svc = service(FakeRepo(_many(120)))
    first = svc.list_products(sort="product", page=1, page_size=50)
    assert (first["total"], first["total_pages"], len(first["items"])) == (120, 3, 50)
    assert first["items"][0]["variant_id"] == 1
    last = svc.list_products(sort="product", page=3, page_size=50)
    assert len(last["items"]) == 20
    beyond = svc.list_products(sort="product", page=9, page_size=50)
    assert beyond["items"] == [] and beyond["total"] == 120
    assert all("_sort" not in i for i in first["items"])
    assert MAX_PAGE_SIZE == 200


def test_search_is_accent_and_case_insensitive_on_code_barcode_and_name():
    rows = [fact(1, product_name="Café Tostado"), fact(2, product_name="Azúcar"), fact(3, code="ABC-77")]
    svc = service(FakeRepo(rows))
    assert [i["variant_id"] for i in svc.list_products(q="cafe")["items"]] == [1]
    assert [i["variant_id"] for i in svc.list_products(q="abc-77")["items"]] == [3]
    assert [i["variant_id"] for i in svc.list_products(q="7802")["items"]] == [2]


def test_filters_variant_state_reliability_and_sort_validation():
    rows = [
        fact(1, average_cost=D("100")),
        fact(2, variant_state=1, average_cost=D("100")),
        fact(3),
    ]
    svc = service(FakeRepo(rows))
    assert {i["variant_id"] for i in svc.list_products()["items"]} == {1, 3}
    assert {i["variant_id"] for i in svc.list_products(variant_state="all")["items"]} == {1, 2, 3}
    only_avg = svc.list_products(reliability="solo_promedio_bsale")
    assert [i["variant_id"] for i in only_avg["items"]] == [1]
    with pytest.raises(CostControlValidationError):
        svc.list_products(sort="DROP TABLE")
    with pytest.raises(CostControlValidationError):
        svc.list_products(reliability="inventado")
    with pytest.raises(CostControlValidationError):
        svc.list_products(tax_status="x")


def test_sort_by_change_puts_missing_values_last():
    rows = [
        fact(1, **purchase("lc_", 2, "1500", TODAY), **purchase("pc_", 1, "1000", TODAY)),
        fact(2),
        fact(3, **purchase("lc_", 4, "1100", TODAY), **purchase("pc_", 3, "1000", TODAY)),
    ]
    items = service(FakeRepo(rows)).list_products(sort="change_pct_desc")["items"]
    assert [i["variant_id"] for i in items] == [1, 3, 2]


# ---------------------------------------------------------------- historial


def _hline(rid: int, when: date, cost: str | None, *, movement="factura", document="Factura", qty="10", office=1, complete=True):
    return {
        "reception_id": rid,
        "detail_id": rid * 10,
        "admission_date": when,
        "office_id": office,
        "document": document,
        "document_number": str(rid),
        "internal_dispatch_id": 0,
        "movement_class": movement,
        "cost": D(cost) if cost is not None else None,
        "quantity": D(qty),
        "details_complete": complete,
        "comparable": movement in ("factura", "guia_despacho") and cost not in (None, "0") and complete,
        "cost_modified": False,
        "quantity_modified": False,
        "variant_modified": False,
    }


def test_history_compares_against_previous_comparable_and_skips_non_purchases():
    lines = [
        _hline(1, date(2026, 1, 5), "1000"),
        _hline(2, date(2026, 2, 5), "50", movement="nota_credito", document="NOTA DE CRÉDITO ELECTRÓNICA T"),
        _hline(3, date(2026, 3, 5), "1500", office=3),
        _hline(4, date(2026, 4, 5), "0"),
    ]
    result = service(FakeRepo([fact(9)], lines=lines)).product_history(9)
    items = result["items"]
    assert [i["reception_id"] for i in items] == [4, 3, 2, 1]
    by_id = {i["reception_id"]: i for i in items}
    assert by_id[3]["previous_comparable_cost"] == "1000"
    assert by_id[3]["change_pct"] == "50.00"
    assert {a["code"] for a in by_id[3]["anomalies"]} == {"alza_anormal"}
    assert by_id[2]["comparable"] is False and by_id[2]["anomalies"] == []
    assert {a["code"] for a in by_id[4]["anomalies"]} == {"costo_cero"}
    assert [p["reception_id"] for p in result["series"]] == [1, 3]
    filtered = service(FakeRepo([fact(9)], lines=lines)).product_history(9, office_id=3)
    assert [i["reception_id"] for i in filtered["items"]] == [3]
    assert filtered["items"][0]["previous_comparable_cost"] == "1000"


def test_history_for_foreign_variant_is_not_found():
    repo = FakeRepo([fact(1)], lines=[_hline(1, TODAY, "10")])
    with pytest.raises(CostControlNotFound):
        service(repo).product_history(77)
    assert all(name != "variant_lines" for name, _ in repo.calls)


# ---------------------------------------------------------------- alertas


def _alert(rid: int, cost: str, prev: str | None, **kw):
    row = {
        "variant_id": 1,
        "code": "SKU1",
        "bar_code": "7801",
        "variant_name": None,
        "product_name": "Producto 1",
        "reception_id": rid,
        "detail_id": rid * 10,
        "admission_date": date(2026, 9, rid),
        "office_id": 1,
        "document": "Factura",
        "document_number": "99",
        "movement_class": "factura",
        "cost": D(cost),
        "quantity": D("5"),
        "details_complete": True,
        "previous_comparable_cost": D(prev) if prev else None,
        "median_comparable_quantity": D("5"),
        "cost_modified": False,
        "quantity_modified": False,
        "variant_modified": False,
    }
    row.update(kw)
    return row


def test_alerts_locate_original_reception_and_filter_by_code():
    rows = [_alert(1, "2000", "1000"), _alert(2, "0", None), _alert(3, "1000", "1000", cost_modified=True)]
    svc = service(FakeRepo([fact(1)], alerts=rows))
    result = svc.list_alerts(date_from=date(2026, 9, 1), date_to=date(2026, 9, 30))
    assert result["total"] == 3
    first = result["items"][0]
    assert (first["reception_id"], first["detail_id"], first["document"], first["document_number"]) == (
        1,
        10,
        "Factura",
        "99",
    )
    assert first["max_severity"] == "alta"
    assert result["counts_by_code"] == {"alza_anormal": 1, "costo_cero": 1, "recepcion_modificada": 1}
    only_zero = svc.list_alerts(date_from=date(2026, 9, 1), date_to=date(2026, 9, 30), anomaly_code="costo_cero")
    assert [a["reception_id"] for a in only_zero["items"]] == [2]


def test_alert_validation():
    svc = service(FakeRepo([fact(1)]))
    with pytest.raises(CostControlValidationError):
        svc.list_alerts(date_from=date(2026, 9, 30), date_to=date(2026, 9, 1))
    with pytest.raises(CostControlValidationError):
        svc.list_alerts(date_from=date(2024, 12, 31), date_to=date(2026, 9, 1))
    with pytest.raises(CostControlValidationError):
        svc.list_alerts(date_from=date(2026, 9, 1), date_to=date(2026, 9, 2), severity="critica")


def test_reception_not_found_in_company():
    with pytest.raises(CostControlNotFound):
        service(FakeRepo([fact(1)])).get_reception(123)


def test_summary_counts_active_skus_only():
    rows = [
        fact(1, average_cost=D("100")),
        fact(2, variant_state=1, average_cost=D("100")),
        fact(3, average_cost=D("0")),
    ]
    s = service(FakeRepo(rows)).summary()
    assert (s["skus_total"], s["skus_active"], s["skus_active_with_bsale_average"]) == (3, 2, 1)
    assert s["skus_active_by_reliability"]["sin_costo"] == 1
    assert s["skus_active_by_reliability"]["solo_promedio_bsale"] == 1
    assert s["movements"][0]["movement_class"] == "factura"
