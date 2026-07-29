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
    ) -> None:
        self.history = history or []
        self.taxes = taxes or []
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
            if col in self.columns or params[1] == "taxes" and "bsale_id" in self.columns:
                # column probe: params = schema, table, column
                return [{"ok": 1}] if col in self.columns else []
            return [{"ok": 1}] if str(col) in self.columns else []
        if "FROM BSALE.TAXES" in upper or "FROM bsale.taxes" in sql:
            return list(self.taxes)
        if "COST_RECEPTION_HISTORY" in upper:
            return list(self.history)
        return []


def test_1_exact_gross_match():
    res = classify_cost_audit_row(_row())
    assert CostAuditFlag.EXACT_MATCH.value in res.flags
    assert res.expected_gross_from_amounts == D("11900")
    assert res.gross_difference_amounts == D("0")


def test_2_rounding_difference():
    res = classify_cost_audit_row(
        _row(cost_bruto_erp=D("11900.005"), iva_amount=D("1900"), other_taxes=D("0"))
    )
    assert CostAuditFlag.ROUNDING_DIFFERENCE.value in res.flags
    assert CostAuditFlag.GROSS_MISMATCH.value not in res.flags


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
    exe = RecordingExecutor(history=[])
    args = clamp_cost_audit_args(
        company_id=3,
        office_id=1,
        variant_id=99,
        barcode="779ABC",
        source_document_id=555,
    )
    CostDataAuditRepository(exe).fetch_history_rows(
        args, date_from=date(2026, 1, 1), date_to=date(2026, 7, 1)
    )
    assert any("variant_id" in s.lower() for s in exe.sqls if "cost_reception_history" in s.lower())
    hist_sql = next(s for s in exe.sqls if "cost_reception_history" in s.lower())
    assert "barcode" in hist_sql.lower() or "bar_code" in hist_sql.lower()
    assert "document_number" in hist_sql.lower()
    # params include filters
    hist_params = exe.params[exe.sqls.index(hist_sql)]
    assert 99 in hist_params
    assert "779ABC" in hist_params
    assert 555 in hist_params


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
    assert len(history_queries) == 1
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
    )
    assert args.days == 365
    assert args.limit == 5000
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
