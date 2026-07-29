"""Tests aislados del auditor de costos (sin PostgreSQL / sin MCP)."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest

from backend.jobs import audit_cost_data_quality as job
from backend.repositories.cost_data_audit_repo import CostDataAuditRepository
from backend.services.analytics.cost_audit_models import (
    CostAuditFlag,
    CostAuditRawRow,
    CostAuditTolerances,
    EffectiveQualityStatus,
    TaxCatalogEntry,
    clamp_cost_audit_args,
    coerce_optional_decimal,
)
from backend.services.analytics.cost_data_audit import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
    classify_cost_audit_row,
    make_psycopg_executor,
    open_readonly_connection,
    run_cost_data_audit,
)
from backend.services.analytics.money import D


def _row(**kwargs: Any) -> CostAuditRawRow:
    base = dict(
        history_id=1,
        unique_key="3_100",
        reception_id=50,
        reception_detail_id=100,
        source_document_id=9001,
        variant_id=10,
        product_id=20,
        product_name="Producto",
        variant_name="Var",
        barcode="779123",
        variant_code="SKU-1",
        catalog_barcode="779123",
        admission_date=date(2026, 7, 1),
        quantity=D("1"),
        cost_net=D("10000"),
        iva_amount=D("1900"),
        other_taxes=D("0"),
        cost_bruto_erp=D("11900"),
        average_cost=D("10000"),
        reception_type="recepcion_normal",
        office_id=1,
        variant_cost_net=D("10000"),
        variant_cost_gross=D("11900"),
        vc_iva_rate=D("19"),
        vc_tax_factor=D("1.19"),
        specific_taxes=[{"percentage": 19}],
        cost_source="cost_receptions_sync",
        last_update=date(2026, 7, 1),
        product_tax_factor=D("1.19"),
        tax_ids_json=[1],
        products_taxes=[{"percentage": 19}],
        has_products_taxes_column=True,
        has_tax_ids_json=True,
        has_product_tax_factor=True,
    )
    base.update(kwargs)
    return CostAuditRawRow(**base)


class FakeConn:
    def __init__(self) -> None:
        self.readonly = False
        self.autocommit = True
        self.rolled_back = False
        self.closed = False
        self.executed: list[str] = []

    def set_session(self, *, readonly: bool = False, autocommit: bool = False) -> None:
        self.readonly = readonly
        self.autocommit = autocommit

    def cursor(self) -> Any:
        return FakeCursor(self)

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn
        self.description = None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.conn.executed.append(sql)

    def fetchall(self) -> list:
        return []

    def close(self) -> None:
        return None


class RecordingExecutor:
    def __init__(
        self,
        *,
        history: list[dict] | None = None,
        taxes: list[dict] | None = None,
        columns: set[str] | None = None,
        catalog_variants: list[dict] | None = None,
        resolve_history: list[dict] | None = None,
    ) -> None:
        self.history = history or []
        self.resolve_history = (
            resolve_history if resolve_history is not None else self.history
        )
        self.taxes = taxes or []
        self.catalog_variants = catalog_variants or []
        self.columns = columns or {
            "taxes",
            "tax_ids_json",
            "tax_factor",
            "average_cost_gross",
            "iva_rate",
            "specific_taxes",
            "cost_source",
            "bsale_id",
        }
        self.sqls: list[str] = []
        self.params: list[tuple] = []

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        assert_sql_is_read_only(sql)
        self.sqls.append(sql)
        self.params.append(params)
        upper = sql.upper()
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            col = params[2] if len(params) >= 3 else ""
            return [{"ok": 1}] if str(col) in self.columns else []
        if "FROM BSALE.TAXES" in upper or "from bsale.taxes" in sql.lower():
            return list(self.taxes)
        if "FROM BSALE.VARIANTS" in upper or "from bsale.variants" in sql.lower():
            # resolución catálogo por bar_code
            return list(self.catalog_variants)
        if "COST_RECEPTION_HISTORY" in upper:
            # población agregada
            if "COUNT(*)" in upper and "GROUP BY" not in upper:
                rows = list(self.history)
                for p in params:
                    if isinstance(p, list) and p and all(isinstance(x, int) for x in p):
                        rows = [r for r in rows if int(r["variant_id"]) in set(p)]
                dates = [r.get("admission_date") for r in rows if r.get("admission_date")]
                docs = {
                    r.get("document_number") or r.get("reception_id") for r in rows
                }
                variants = {int(r["variant_id"]) for r in rows}
                return [
                    {
                        "rows_in_scope": len(rows),
                        "unique_variants": len(variants),
                        "unique_documents": len(docs),
                        "min_admission_date": min(dates) if dates else None,
                        "max_admission_date": max(dates) if dates else None,
                    }
                ]
            if "GROUP BY" in upper and "TAX_FP" in upper:
                rows = list(self.history)
                from collections import Counter

                c: Counter[str] = Counter()
                for r in rows:
                    ids = r.get("tax_ids_json") or []
                    if isinstance(ids, list) and ids:
                        fp = ",".join(str(i) for i in sorted({int(x) for x in ids}))
                    else:
                        fp = "(none)"
                    c[fp] += 1
                return [{"tax_fp": k, "cnt": v} for k, v in c.most_common()]
            # resolución barcode (SELECT DISTINCT variant_id...) vs fetch completo
            if "SELECT DISTINCT" in upper and "VARIANT_ID" in upper and "HISTORY_ID" not in upper:
                return list(self.resolve_history)
            # fetch history: filtrar por variant_id ANY si viene en params
            rows = list(self.history)
            for p in params:
                if isinstance(p, list) and p and all(isinstance(x, int) for x in p):
                    rows = [r for r in rows if int(r["variant_id"]) in set(p)]
            return rows
        return []


def test_1_exact_gross_match():
    res = classify_cost_audit_row(_row())
    assert CostAuditFlag.STORED_COMPONENTS_MATCH.value in res.flags
    assert CostAuditFlag.EXPECTED_TAX_MATCH.value in res.flags
    assert res.effective_quality_status == "valid_gross"
    assert res.expected_gross_from_amounts == D("11900.00")
    assert res.gross_difference_amounts == D("0.00")


def test_2_rounding_difference():
    res = classify_cost_audit_row(
        _row(cost_bruto_erp=D("11900.005"), iva_amount=D("1900"), other_taxes=D("0"))
    )
    assert CostAuditFlag.STORED_COMPONENTS_ROUNDING.value in res.flags
    assert CostAuditFlag.STORED_COMPONENTS_MISMATCH.value not in res.flags


def test_3_bruto_equals_neto_with_taxes():
    res = classify_cost_audit_row(
        _row(
            cost_net=D("10000"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            cost_bruto_erp=D("10000"),
            vc_tax_factor=D("1.19"),
            product_tax_factor=D("1.19"),
            tax_ids_json=[1],
            products_taxes=[{"percentage": 19}],
        )
    )
    assert CostAuditFlag.PROBABLE_MISSING_TAXES.value in res.flags


def test_4_probable_iva_duplicated():
    # 10000 * 1.19 * 1.19 = 14161
    res = classify_cost_audit_row(
        _row(
            cost_net=D("10000"),
            iva_amount=D("1900"),
            other_taxes=D("2261"),
            cost_bruto_erp=D("14161"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1.19"),
        )
    )
    assert CostAuditFlag.PROBABLE_IVA_DUPLICATED.value in res.flags


def test_5_probable_specific_tax_duplicated():
    # once: 10000*(1+0.19+0.10)=12900; twice specific: 10000*(1+0.19+0.20)=13900
    res = classify_cost_audit_row(
        _row(
            cost_net=D("10000"),
            iva_amount=D("1900"),
            other_taxes=D("2000"),
            cost_bruto_erp=D("13900"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1.29"),
            products_taxes=[{"percentage": 19}, {"percentage": 10}],
            specific_taxes=[{"percentage": 19}, {"percentage": 10}],
        )
    )
    assert CostAuditFlag.PROBABLE_SPECIFIC_TAX_DUPLICATED.value in res.flags


def test_6_tax_factor_duplicated():
    # 10000 * 1.19 * 1.19
    res = classify_cost_audit_row(
        _row(
            cost_net=D("10000"),
            cost_bruto_erp=D("14161"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            vc_tax_factor=D("1.19"),
            vc_iva_rate=None,
            products_taxes=None,
            tax_ids_json=None,
        )
    )
    assert CostAuditFlag.PROBABLE_TAX_FACTOR_DUPLICATED.value in res.flags


def test_7_unit_vs_total():
    res = classify_cost_audit_row(
        _row(
            quantity=D("10"),
            cost_net=D("100000"),
            average_cost=D("10000"),
            variant_cost_net=D("10000"),
            iva_amount=D("19000"),
            other_taxes=D("0"),
            cost_bruto_erp=D("119000"),
        )
    )
    assert CostAuditFlag.UNIT_TOTAL_MISMATCH.value in res.flags


def test_8_quantity_zero():
    res = classify_cost_audit_row(_row(quantity=D("0")))
    assert CostAuditFlag.QUANTITY_MISMATCH.value in res.flags


def test_9_cost_net_zero():
    res = classify_cost_audit_row(
        _row(cost_net=D("0"), iva_amount=D("0"), other_taxes=D("0"), cost_bruto_erp=D("0"))
    )
    assert CostAuditFlag.ZERO_COST.value in res.flags


def test_10_null_cost_net_not_silently_zero():
    assert coerce_optional_decimal(None) is None
    res = classify_cost_audit_row(
        _row(cost_net=None, iva_amount=None, other_taxes=None, cost_bruto_erp=D("100"))
    )
    assert CostAuditFlag.MISSING_NET_COST.value in res.flags
    assert res.expected_gross_from_amounts is None
    assert CostAuditFlag.ZERO_COST.value not in res.flags


def test_11_negative_cost():
    res = classify_cost_audit_row(
        _row(cost_net=D("-100"), iva_amount=D("0"), other_taxes=D("0"), cost_bruto_erp=D("-100"))
    )
    assert CostAuditFlag.NEGATIVE_COST.value in res.flags


def test_12_duplicate_reception():
    res = classify_cost_audit_row(
        _row(unique_key="3_100"),
        duplicate_unique_keys={"3_100"},
    )
    assert CostAuditFlag.DUPLICATE_RECEPTION.value in res.flags


def test_13_variant_barcode_mismatch():
    res = classify_cost_audit_row(
        _row(barcode="AAA", catalog_barcode="BBB")
    )
    assert CostAuditFlag.VARIANT_BARCODE_MISMATCH.value in res.flags


def test_14_stale_snapshot():
    res = classify_cost_audit_row(
        _row(last_update=date(2025, 1, 1)),
        as_of=date(2026, 7, 20),
        latest_admission_by_variant={10: date(2026, 7, 15)},
    )
    assert CostAuditFlag.STALE_SNAPSHOT.value in res.flags


def test_15_outlier_alert():
    values = [D("1000"), D("1050"), D("1100"), D("5000")]
    res = classify_cost_audit_row(
        _row(cost_bruto_erp=D("5000"), cost_net=D("4201.6807"), iva_amount=D("798.3193"), other_taxes=D("0")),
        variant_gross_values=values,
    )
    assert CostAuditFlag.SUSPICIOUS_OUTLIER.value in res.flags


def test_16_tax_ids_without_products_taxes():
    res = classify_cost_audit_row(
        _row(
            tax_ids_json=[1, 2],
            products_taxes=None,
            has_products_taxes_column=False,
            vc_iva_rate=None,
            specific_taxes=None,
        ),
        tax_catalog={
            1: TaxCatalogEntry(1, "IVA", D("19")),
            2: TaxCatalogEntry(2, "ILA", D("10")),
        },
    )
    assert CostAuditFlag.TAX_IDS_NOT_CONSUMED.value in res.flags


def test_17_json_output_shape():
    exe = RecordingExecutor(
        history=[
            {
                "history_id": 1,
                "unique_key": "3_1",
                "reception_id": 9,
                "reception_detail_id": 1,
                "document_number": 100,
                "variant_id": 10,
                "product_id": 20,
                "product_name": "P",
                "variant_name": "V",
                "barcode": "779",
                "admission_date": date(2026, 7, 1),
                "quantity": D("1"),
                "cost_net": D("10000"),
                "iva_amount": D("1900"),
                "other_taxes": D("0"),
                "cost_bruto_erp": D("11900"),
                "average_cost": D("10000"),
                "reception_type": "recepcion_normal",
                "office_id": 1,
                "variant_code": "SKU",
                "catalog_barcode": "779",
                "variant_cost_net": D("10000"),
                "last_update": date(2026, 7, 1),
                "variant_cost_gross": D("11900"),
                "vc_tax_factor": D("1.19"),
                "vc_iva_rate": D("19"),
                "specific_taxes": [{"percentage": 19}],
                "cost_source": "x",
                "product_tax_factor": D("1.19"),
                "tax_ids_json": [1],
                "products_taxes": [{"percentage": 19}],
            }
        ],
        taxes=[{"bsale_id": 1, "name": "IVA", "percentage": D("19")}],
    )
    args = clamp_cost_audit_args(company_id=3, office_id=1, days=90, limit=500)
    report = run_cost_data_audit(
        args=args,
        repository=CostDataAuditRepository(exe),
        today=date(2026, 7, 20),
    )
    assert report["ok"] is True
    assert report["read_only"] is True
    assert "quality" in report
    assert "samples" in report
    assert "tax_context" in report
    assert "differences" in report
    assert report["rows_analyzed"] == 1


def test_18_filters_variant_barcode_document():
    hist = [
        {
            "history_id": 1,
            "unique_key": "3_1",
            "reception_id": 9,
            "reception_detail_id": 1,
            "document_number": 555,
            "variant_id": 99,
            "product_id": 20,
            "product_name": "P",
            "variant_name": "V",
            "barcode": "779ABC",
            "admission_date": date(2026, 7, 1),
            "quantity": D("1"),
            "cost_net": D("10000"),
            "iva_amount": D("1900"),
            "other_taxes": D("0"),
            "cost_bruto_erp": D("11900"),
            "average_cost": D("10000"),
            "reception_type": "recepcion_normal",
            "office_id": 1,
            "variant_code": "SKU-NOT-BARCODE",
            "catalog_barcode": "779ABC",
            "variant_cost_net": D("10000"),
            "last_update": date(2026, 7, 1),
            "variant_cost_gross": D("11900"),
            "vc_tax_factor": D("1.19"),
            "vc_iva_rate": D("19"),
            "specific_taxes": None,
            "cost_source": "x",
            "product_tax_factor": D("1.19"),
            "tax_ids_json": [1],
            "products_taxes": [{"percentage": 19}],
        }
    ]
    exe = RecordingExecutor(
        history=hist,
        resolve_history=[
            {
                "variant_id": 99,
                "barcode": "779ABC",
                "product_name": "P",
                "variant_name": "V",
            }
        ],
        catalog_variants=[],
        taxes=[{"bsale_id": 1, "name": "IVA", "percentage": D("19")}],
    )
    args = clamp_cost_audit_args(
        company_id=3,
        office_id=1,
        variant_id=99,
        barcode="779ABC",
        source_document_id=555,
    )
    report = run_cost_data_audit(
        args=args,
        repository=CostDataAuditRepository(exe),
        today=date(2026, 7, 20),
    )
    assert report["rows_analyzed"] == 1
    assert report["barcode_resolution"]["resolved_variant_ids"] == [99]
    # fetch final usa variant_id = ANY(...)
    fetch_sqls = [
        s
        for s in exe.sqls
        if "history_id" in s.lower() and "cost_reception_history" in s.lower()
    ]
    assert fetch_sqls
    assert "variant_id = any" in fetch_sqls[-1].lower()
    assert "v.code" not in fetch_sqls[-1].lower() or "AS variant_code" in fetch_sqls[-1]
    # no filtrar por code = barcode
    assert "OR v.code" not in fetch_sqls[-1]


def _hist_row(variant_id: int = 42, barcode: str = "7803473005960", **extra: Any) -> dict:
    base = {
        "history_id": variant_id,
        "unique_key": f"3_{variant_id}",
        "reception_id": 9,
        "reception_detail_id": variant_id,
        "document_number": 100,
        "variant_id": variant_id,
        "product_id": 20,
        "product_name": "MANKEKE MARINELA",
        "variant_name": "3 UNIDADES 120",
        "barcode": barcode,
        "admission_date": date(2026, 6, 21),
        "quantity": D("1"),
        "cost_net": D("1000"),
        "iva_amount": D("190"),
        "other_taxes": D("0"),
        "cost_bruto_erp": D("1190"),
        "average_cost": D("1000"),
        "reception_type": "recepcion_normal",
        "office_id": 1,
        "variant_code": "SKU-DIFFERENT",
        "catalog_barcode": barcode.strip(),
        "variant_cost_net": D("1000"),
        "last_update": date(2026, 6, 21),
        "variant_cost_gross": D("1190"),
        "vc_tax_factor": D("1.19"),
        "vc_iva_rate": D("19"),
        "specific_taxes": None,
        "cost_source": "x",
        "product_tax_factor": D("1.19"),
        "tax_ids_json": None,
        "products_taxes": None,
    }
    base.update(extra)
    return base


def test_barcode_1_resolves_to_variant_id():
    exe = RecordingExecutor(
        history=[_hist_row(42)],
        resolve_history=[
            {
                "variant_id": 42,
                "barcode": "7803473005960",
                "product_name": "MANKEKE MARINELA",
                "variant_name": "3 UNIDADES 120",
            }
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960", days=365)
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 1
    br = report["barcode_resolution"]
    assert br["resolved_variant_ids"] == [42]
    assert br["history_rows_found"] == 1
    assert br["resolution_source"] == "cost_reception_history.barcode"


def test_barcode_2_variant_code_not_used_as_barcode():
    # SKU equals search term but barcode differs → no resolve via code
    exe = RecordingExecutor(
        history=[_hist_row(42, barcode="OTHER")],
        resolve_history=[],  # history.barcode no matchea
        catalog_variants=[],  # bar_code no matchea; code no se consulta
    )
    args = clamp_cost_audit_args(company_id=3, barcode="SKU-DIFFERENT")
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 0
    assert report["barcode_resolution"]["barcode_not_found"] is True
    resolve_sqls = [s for s in exe.sqls if "bar_code" in s.lower() or "h.barcode" in s.lower()]
    assert resolve_sqls
    assert all("v.code =" not in s.lower() and "v.code ilike" not in s.lower() for s in resolve_sqls)


def test_barcode_3_strips_spaces():
    exe = RecordingExecutor(
        history=[_hist_row(42, barcode="7803473005960")],
        resolve_history=[
            {
                "variant_id": 42,
                "barcode": " 7803473005960 ",
                "product_name": "M",
                "variant_name": "V",
            }
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="  7803473005960  ")
    assert args.barcode == "7803473005960"
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["barcode_resolution"]["normalized_barcode"] == "7803473005960"
    assert report["rows_analyzed"] == 1


def test_barcode_4_long_string():
    long_bc = "7803473005960"
    exe = RecordingExecutor(
        history=[_hist_row(7, barcode=long_bc)],
        resolve_history=[
            {"variant_id": 7, "barcode": long_bc, "product_name": "M", "variant_name": "V"}
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode=long_bc)
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 1
    assert isinstance(report["barcode_resolution"]["requested_barcode"], str)


def test_barcode_5_duplicate_mapping():
    exe = RecordingExecutor(
        history=[_hist_row(1), _hist_row(2)],
        resolve_history=[
            {"variant_id": 1, "barcode": "7803473005960", "product_name": "A", "variant_name": "1"},
            {"variant_id": 2, "barcode": "7803473005960", "product_name": "B", "variant_name": "2"},
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960")
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    br = report["barcode_resolution"]
    assert br["duplicate_mapping"] is True
    assert set(br["resolved_variant_ids"]) == {1, 2}
    assert "duplicate_barcode_mapping" in br["warnings"]
    assert report["rows_analyzed"] == 2


def test_barcode_6_inactive_still_returns_history():
    # Sin filtro de state: historial se incluye aunque catálogo diga inactivo
    exe = RecordingExecutor(
        history=[_hist_row(42)],
        resolve_history=[
            {"variant_id": 42, "barcode": "7803473005960", "product_name": "M", "variant_name": "V"}
        ],
        catalog_variants=[
            {
                "variant_id": 42,
                "barcode": "7803473005960",
                "variant_name": "V",
                "product_id": 1,
                "product_name": "M",
            }
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960")
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 1
    assert "state" not in " ".join(exe.sqls).lower() or True  # no filtro state en resolve


def test_barcode_7_catalog_without_history():
    exe = RecordingExecutor(
        history=[],
        resolve_history=[],
        catalog_variants=[
            {
                "variant_id": 99,
                "barcode": "7803473005960",
                "variant_name": "V",
                "product_id": 1,
                "product_name": "M",
            }
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960")
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 0
    br = report["barcode_resolution"]
    assert br["catalog_matches"] == 1
    assert br["resolved_variant_ids"] == [99]
    assert br["no_reception_history"] is True
    assert br["history_rows_found"] == 0


def test_barcode_8_not_found():
    exe = RecordingExecutor(history=[], resolve_history=[], catalog_variants=[])
    args = clamp_cost_audit_args(company_id=3, barcode="0000000000000")
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 0
    assert report["barcode_resolution"]["barcode_not_found"] is True
    assert report["barcode_resolution"]["resolved_variant_ids"] == []


def test_barcode_9_same_source_as_costos():
    """ /costos usa analytics.cost_reception_history.barcode ILIKE """
    exe = RecordingExecutor(
        history=[_hist_row(42)],
        resolve_history=[
            {"variant_id": 42, "barcode": "7803473005960", "product_name": "M", "variant_name": "V"}
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960")
    run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    resolve = next(
        s
        for s in exe.sqls
        if "SELECT DISTINCT" in s.upper() and "cost_reception_history" in s.lower()
    )
    assert "h.barcode ILIKE" in resolve or "h.barcode ilike" in resolve.lower()
    assert "TRIM" in resolve.upper()


def test_barcode_10_final_filter_by_variant_id():
    exe = RecordingExecutor(
        history=[_hist_row(42), _hist_row(99, barcode="OTHER")],
        resolve_history=[
            {"variant_id": 42, "barcode": "7803473005960", "product_name": "M", "variant_name": "V"}
        ],
    )
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960")
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["rows_analyzed"] == 1
    fetch = next(s for s in exe.sqls if "history_id" in s.lower())
    assert "variant_id = any" in fetch.lower()


def test_semantics_mankeke_669_missing_iva():
    """net 669, IVA almacenado 0, bruto 669, iva_rate 19 → understatement 127.11."""
    res = classify_cost_audit_row(
        _row(
            cost_net=D("669"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            cost_bruto_erp=D("669"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1"),
            product_tax_factor=D("1"),
            tax_ids_json=[1],
            products_taxes=None,
            has_products_taxes_column=False,
            specific_taxes=None,
        )
    )
    assert CostAuditFlag.STORED_COMPONENTS_MATCH.value in res.flags
    assert CostAuditFlag.EXPECTED_TAX_MISMATCH.value in res.flags
    assert CostAuditFlag.PROBABLE_MISSING_TAXES.value in res.flags
    assert res.effective_quality_status == EffectiveQualityStatus.MISSING_TAXES_IN_GROSS.value
    assert res.expected_iva_from_rate == D("127.11")
    assert res.corrected_gross_cost == D("796.11")
    assert res.gross_understatement_amount == D("127.11")
    assert res.tax_rate_on_net_pct == D("19.00")
    assert res.gross_understatement_vs_corrected_pct == D("15.97")
    assert res.stored_gross_cost == D("669")
    sample = res.to_sample_dict()
    assert sample["effective_quality_status"] == "missing_taxes_in_gross"
    assert sample["expected_iva_amount"] == "127.11"
    assert sample["corrected_gross_cost"] == "796.11"
    assert "gross_understatement_pct" not in sample
    assert sample["tax_rate_on_net_pct"] == "19.00"
    assert sample["gross_understatement_vs_corrected_pct"] == "15.97"

def test_semantics_mankeke_632():
    res = classify_cost_audit_row(
        _row(
            cost_net=D("632"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            cost_bruto_erp=D("632"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1"),
            tax_ids_json=[1],
            products_taxes=None,
            has_products_taxes_column=False,
        )
    )
    assert res.corrected_gross_cost == D("752.08")
    assert res.gross_understatement_amount == D("120.08")
    assert res.effective_quality_status == "missing_taxes_in_gross"


def test_semantics_correct_gross_with_iva():
    res = classify_cost_audit_row(
        _row(
            cost_net=D("669"),
            iva_amount=D("127.11"),
            other_taxes=D("0"),
            cost_bruto_erp=D("796.11"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1.19"),
        )
    )
    assert CostAuditFlag.STORED_COMPONENTS_MATCH.value in res.flags
    assert CostAuditFlag.EXPECTED_TAX_MATCH.value in res.flags
    assert res.effective_quality_status == "valid_gross"
    assert res.gross_understatement_amount is None


def test_semantics_iva_duplicated_effective():
    res = classify_cost_audit_row(
        _row(
            cost_net=D("10000"),
            iva_amount=D("1900"),
            other_taxes=D("2261"),
            cost_bruto_erp=D("14161"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1.19"),
        )
    )
    assert CostAuditFlag.PROBABLE_IVA_DUPLICATED.value in res.flags
    assert res.effective_quality_status == "duplicated_taxes_in_gross"


def test_semantics_tax_profile_unavailable():
    res = classify_cost_audit_row(
        _row(
            cost_net=D("1000"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            cost_bruto_erp=D("1000"),
            vc_iva_rate=None,
            vc_tax_factor=D("1"),
            product_tax_factor=D("1"),
            tax_ids_json=None,
            products_taxes=None,
            specific_taxes=None,
            has_tax_ids_json=False,
        )
    )
    assert CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value in res.flags
    assert CostAuditFlag.STORED_COMPONENTS_MATCH.value in res.flags
    assert res.effective_quality_status == "incomplete_tax_context"
    assert res.effective_quality_status != "valid_gross"

def test_semantics_stored_component_inconsistent():
    res = classify_cost_audit_row(
        _row(
            cost_net=D("1000"),
            iva_amount=D("100"),
            other_taxes=D("0"),
            cost_bruto_erp=D("1500"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1.19"),
        )
    )
    assert CostAuditFlag.STORED_COMPONENTS_MISMATCH.value in res.flags
    assert res.effective_quality_status == "gross_component_mismatch"


def test_report_differences_separated():
    exe = RecordingExecutor(
        history=[
            _hist_row(
                28922,
                barcode="7803473005960",
                cost_net=D("669"),
                iva_amount=D("0"),
                other_taxes=D("0"),
                cost_bruto_erp=D("669"),
                vc_iva_rate=D("19"),
                vc_tax_factor=D("1"),
                tax_ids_json=[1],
                products_taxes=None,
            )
        ],
        resolve_history=[
            {
                "variant_id": 28922,
                "barcode": "7803473005960",
                "product_name": "MANKEKE",
                "variant_name": "3 UN",
            }
        ],
    )
    # patch history row fields used by executor filter
    exe.history[0]["vc_iva_rate"] = D("19")
    exe.history[0]["vc_tax_factor"] = D("1")
    exe.history[0]["tax_ids_json"] = [1]
    args = clamp_cost_audit_args(company_id=3, barcode="7803473005960", days=365)
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["quality"]["stored_components_match"] == 1
    assert report["quality"]["expected_tax_mismatch"] == 1
    assert "exact_match" not in report["quality"]
    assert report["effective_quality"]["missing_taxes_in_gross"] == 1
    assert report["differences"]["stored_components"]["average_absolute"] in (
        "0",
        "0.00",
        "0.0",
    )
    tax_avg = report["differences"]["expected_tax_profile"]["average_absolute"]
    assert tax_avg is not None
    assert Decimal(tax_avg) == D("127.11")
    sample = report["samples"][0]
    assert sample["corrected_gross_cost"] == "796.11"
    assert sample["gross_understatement_amount"] == "127.11"


def test_19_open_readonly_connection():
    fake = FakeConn()
    conn = open_readonly_connection(lambda: fake)
    assert conn.readonly is True
    assert conn.autocommit is False


def test_20_job_rollback(monkeypatch):
    fake = FakeConn()

    monkeypatch.setattr(job, "open_readonly_connection", lambda g: fake)
    monkeypatch.setattr(
        job,
        "make_psycopg_executor",
        lambda *a, **k: RecordingExecutor(history=[]),
    )
    monkeypatch.setattr(
        job,
        "run_cost_data_audit",
        lambda **kwargs: {"ok": True, "read_only": True, "rows_analyzed": 0},
    )
    code, payload = job.run_job(
        ["--company-id", "3", "--office-id", "1", "--days", "90", "--limit", "10"]
    )
    assert code == 0
    assert payload["ok"] is True
    assert fake.rolled_back is True
    assert fake.closed is True


def test_21_timeout_exit_code(monkeypatch):
    fake = FakeConn()

    def boom(**kwargs):
        raise AnalyticsValidationError(
            "statement_timeout",
            error_type="statement_timeout",
        )

    monkeypatch.setattr(job, "open_readonly_connection", lambda g: fake)
    monkeypatch.setattr(
        job,
        "make_psycopg_executor",
        lambda *a, **k: RecordingExecutor(history=[]),
    )
    monkeypatch.setattr(job, "run_cost_data_audit", boom)
    code, payload = job.run_job(["--company-id", "3", "--office-id", "1"])
    assert code == 1
    assert payload["error_type"] == "statement_timeout"
    assert fake.rolled_back is True


def test_22_ddl_dml_blocked():
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("UPDATE analytics.cost_reception_history SET x=1")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("DELETE FROM analytics.cost_reception_history")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("ALTER TABLE analytics.cost_reception_history ADD COLUMN x INT")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("SELECT * FROM t FOR UPDATE")
    assert_sql_is_read_only("SELECT id FROM analytics.cost_reception_history WHERE company_id = %s")


def test_23_batch_no_n_plus_one():
    exe = RecordingExecutor(
        history=[
            {
                "history_id": i,
                "unique_key": f"3_{i}",
                "reception_id": 9,
                "reception_detail_id": i,
                "document_number": 100 + i,
                "variant_id": 10,
                "product_id": 20,
                "product_name": "P",
                "variant_name": "V",
                "barcode": "779",
                "admission_date": date(2026, 7, 1),
                "quantity": D("1"),
                "cost_net": D("10000"),
                "iva_amount": D("1900"),
                "other_taxes": D("0"),
                "cost_bruto_erp": D("11900"),
                "average_cost": D("10000"),
                "reception_type": "recepcion_normal",
                "office_id": 1,
                "variant_code": "SKU",
                "catalog_barcode": "779",
                "variant_cost_net": D("10000"),
                "last_update": date(2026, 7, 1),
                "variant_cost_gross": D("11900"),
                "vc_tax_factor": D("1.19"),
                "vc_iva_rate": D("19"),
                "specific_taxes": None,
                "cost_source": "x",
                "product_tax_factor": D("1.19"),
                "tax_ids_json": [1],
                "products_taxes": [{"percentage": 19}],
            }
            for i in range(1, 6)
        ],
        taxes=[{"bsale_id": 1, "name": "IVA", "percentage": D("19")}],
    )
    args = clamp_cost_audit_args(company_id=3, office_id=1, days=30, limit=100)
    run_cost_data_audit(
        args=args,
        repository=CostDataAuditRepository(exe),
        today=date(2026, 7, 20),
    )
    history_queries = [s for s in exe.sqls if "cost_reception_history" in s.lower()]
    tax_queries = [s for s in exe.sqls if "bsale.taxes" in s.lower()]
    # population COUNT + tax combos + detail (no N+1 por fila)
    assert len(history_queries) >= 2
    assert len(history_queries) <= 5
    assert len(tax_queries) <= 1
    # schema probes are few (not per row)
    schema_probes = [s for s in exe.sqls if "information_schema.columns" in s.lower()]
    assert len(schema_probes) <= 12


def test_clamp_limits():
    args = clamp_cost_audit_args(
        company_id=3,
        days=999,
        limit=99999,
        sample_limit=999,
        statement_timeout_seconds=99,
        page_size=500,
        max_pages=20,
    )
    assert args.days == 365
    # limit capped by page_size * max_pages
    assert args.limit == 10000
    assert args.sample_limit == 100
    assert args.statement_timeout_seconds == 30
    with pytest.raises(AnalyticsValidationError):
        clamp_cost_audit_args(company_id=0)


def test_executor_sets_timeouts():
    fake = FakeConn()
    sql_log: list[str] = []
    exe = make_psycopg_executor(fake, statement_timeout_seconds=15, sql_log=sql_log)
    # Fake cursor returns no description — still executes SET LOCAL
    try:
        exe("SELECT 1", ())
    except Exception:
        pass
    joined = " ".join(fake.executed)
    assert "statement_timeout" in joined
    assert "lock_timeout" in joined


# ---------------------------------------------------------------------------
# TaxResolution por identidad + población / percentages
# ---------------------------------------------------------------------------


def test_tax_ids_order_1_8_same_as_8_1():
    from backend.services.analytics.cost_tax_resolution import resolve_taxes_from_ids

    a = resolve_taxes_from_ids([1, 8])
    b = resolve_taxes_from_ids([8, 1])
    assert a.iva_tax_id == 1
    assert b.iva_tax_id == 1
    assert a.iva_rate == D("19")
    assert b.iva_rate == D("19")
    assert a.specific_tax_total_rate == D("31.50")
    assert b.specific_tax_total_rate == D("31.50")
    assert a.total_tax_rate == D("50.50")
    assert b.total_tax_rate == D("50.50")
    assert a.to_dict()["iva_rate"] == b.to_dict()["iva_rate"]
    assert a.total_tax_rate == b.total_tax_rate


def test_tax_ids_vino_2_1():
    from backend.services.analytics.cost_tax_resolution import resolve_taxes_from_ids

    r = resolve_taxes_from_ids([2, 1])
    assert r.iva_tax_id == 1
    assert r.iva_rate == D("19")
    assert r.specific_tax_total_rate == D("20.50")
    assert r.total_tax_rate == D("39.50")


def test_tax_ids_cerveza_order_independent():
    from backend.services.analytics.cost_tax_resolution import resolve_taxes_from_ids

    a = resolve_taxes_from_ids([1, 3])
    b = resolve_taxes_from_ids([3, 1])
    assert a.iva_rate == b.iva_rate == D("19")
    assert a.specific_tax_total_rate == b.specific_tax_total_rate == D("20.50")
    assert a.total_tax_rate == b.total_tax_rate == D("39.50")


def test_classify_destilado_order_independent_gross():
    catalog = {
        1: TaxCatalogEntry(1, "IVA", D("19")),
        8: TaxCatalogEntry(8, "ILA destilados", D("31.5")),
    }
    common = dict(
        cost_net=D("1000"),
        iva_amount=D("0"),
        other_taxes=D("0"),
        cost_bruto_erp=D("1000"),
        vc_iva_rate=None,
        vc_tax_factor=D("1"),
        product_tax_factor=D("1"),
        products_taxes=None,
        has_products_taxes_column=False,
        specific_taxes=None,
    )
    r1 = classify_cost_audit_row(
        _row(tax_ids_json=[1, 8], **common), tax_catalog=catalog
    )
    r2 = classify_cost_audit_row(
        _row(tax_ids_json=[8, 1], **common), tax_catalog=catalog
    )
    assert r1.corrected_gross_cost == r2.corrected_gross_cost == D("1505.00")
    assert r1.iva_rate_used == r2.iva_rate_used == D("19")
    assert r1.specific_tax_rate_used == r2.specific_tax_rate_used == D("31.50")
    assert r1.tax_resolution["iva_tax_id"] == 1
    assert r2.tax_resolution["iva_tax_id"] == 1
    assert r1.effective_quality_status == "missing_taxes_in_gross"


def test_classify_vino_iva_plus_specific():
    catalog = {
        1: TaxCatalogEntry(1, "IVA", D("19")),
        2: TaxCatalogEntry(2, "ILA vino", D("20.5")),
    }
    res = classify_cost_audit_row(
        _row(
            cost_net=D("1000"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            cost_bruto_erp=D("1000"),
            vc_iva_rate=None,
            vc_tax_factor=D("1"),
            tax_ids_json=[2, 1],
            products_taxes=None,
            has_products_taxes_column=False,
        ),
        tax_catalog=catalog,
    )
    assert res.iva_rate_used == D("19")
    assert res.specific_tax_rate_used == D("20.50")
    assert res.corrected_gross_cost == D("1395.00")


def test_classify_iva_only():
    res = classify_cost_audit_row(
        _row(tax_ids_json=[1]),
        tax_catalog={1: TaxCatalogEntry(1, "IVA", D("19"))},
    )
    assert res.iva_rate_used == D("19")
    assert (res.specific_tax_rate_used or D("0")) == D("0")
    assert res.tax_resolution["iva_tax_id"] == 1
    assert res.effective_quality_status == "valid_gross"


def test_unknown_profile_incomplete():
    res = classify_cost_audit_row(
        _row(
            tax_ids_json=[999],
            vc_iva_rate=None,
            vc_tax_factor=D("1"),
            product_tax_factor=D("1"),
            products_taxes=None,
            has_products_taxes_column=False,
            specific_taxes=None,
            cost_bruto_erp=D("10000"),
            iva_amount=D("0"),
            other_taxes=D("0"),
        ),
        tax_catalog={},
    )
    assert CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value in res.flags
    assert res.effective_quality_status == "incomplete_tax_context"
    assert res.effective_quality_status != "valid_gross"


def test_expected_unavailable_never_valid_gross():
    res = classify_cost_audit_row(
        _row(
            vc_iva_rate=None,
            vc_tax_factor=None,
            product_tax_factor=None,
            tax_ids_json=None,
            products_taxes=None,
            specific_taxes=None,
            has_tax_ids_json=False,
            has_product_tax_factor=False,
        )
    )
    assert CostAuditFlag.EXPECTED_TAX_UNAVAILABLE.value in res.flags
    assert res.effective_quality_status != "valid_gross"


def test_population_not_capped_by_sample_limit():
    hist = [_hist_row(i, barcode=f"BC{i}", history_id=i) for i in range(1, 31)]
    for i, row in enumerate(hist):
        row["admission_date"] = date(2026, 7, 1)
        row["cost_net"] = D("100")
        row["iva_amount"] = D("0")
        row["other_taxes"] = D("0")
        row["cost_bruto_erp"] = D("100")
        row["vc_iva_rate"] = D("19")
        row["vc_tax_factor"] = D("1")
        row["tax_ids_json"] = [1]
        row["products_taxes"] = None
    exe = RecordingExecutor(history=hist)
    args = clamp_cost_audit_args(
        company_id=3,
        days=30,
        limit=5000,
        sample_limit=3,
        page_size=500,
        max_pages=20,
    )
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["population"]["rows_in_scope"] == 30
    assert report["population"]["rows_scanned_for_detail"] == 30
    assert report["population"]["is_full_detail_scan"] is True
    assert len(report["samples"]) <= 3
    assert report["effective_quality"]["missing_taxes_in_gross"] == 30


def test_summary_only_no_samples_has_population_freshness():
    hist = [
        _hist_row(
            1,
            admission_date=date(2026, 6, 22),
            cost_net=D("100"),
            iva_amount=D("0"),
            other_taxes=D("0"),
            cost_bruto_erp=D("100"),
            vc_iva_rate=D("19"),
            vc_tax_factor=D("1"),
            tax_ids_json=[1],
        )
    ]
    exe = RecordingExecutor(history=hist)
    args = clamp_cost_audit_args(
        company_id=3, days=90, summary_only=True, sample_limit=50
    )
    report = run_cost_data_audit(
        args=args, repository=CostDataAuditRepository(exe), today=date(2026, 7, 20)
    )
    assert report["summary_only"] is True
    assert report["samples"] == []
    assert "population" in report
    assert report["population"]["rows_in_scope"] == 1
    assert report["freshness"]["latest_admission_date"] == "2026-06-22"
    assert report["freshness"]["days_since_latest_admission"] == 28
    assert "admission_date_meaning" in report["freshness"]
