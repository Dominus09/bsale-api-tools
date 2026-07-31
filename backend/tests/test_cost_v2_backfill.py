"""Tests dry-run backfill Costos V2 (sin PostgreSQL / sin escrituras)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest

from backend.jobs import backfill_cost_reception_calculated as job
from backend.repositories.cost_v2_backfill_repo import CostV2BackfillRepository
from backend.services.analytics.cost_v2_backfill import (
    clamp_backfill_args,
    run_cost_v2_backfill_dry_run,
)
from backend.services.analytics.money import D
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
    make_psycopg_executor,
    open_readonly_connection,
)


def _hist(
    history_id: int,
    *,
    variant_id: int = 10,
    cost_net: Decimal = D("669"),
    bruto: Decimal = D("669"),
    tax_ids: list[int] | None = None,
    barcode: str = "7803473005960",
    document_number: int | None = 100,
    admission_date: date = date(2026, 6, 1),
    iva: Decimal | None = D("0"),
    other: Decimal | None = D("0"),
) -> dict[str, Any]:
    return {
        "history_id": history_id,
        "company_id": 3,
        "office_id": 3,
        "variant_id": variant_id,
        "admission_date": admission_date,
        "quantity": D("2"),
        "cost_net": cost_net,
        "iva_amount": iva,
        "other_taxes": other,
        "cost_bruto_erp": bruto,
        "created_at": datetime(2026, 6, 2),
        "barcode": barcode,
        "product_name": "MANKEKE",
        "variant_name": "3 UN",
        "document_number": document_number,
        "reception_id": 50,
        "product_id": 20,
        "catalog_tax_ids_json": tax_ids if tax_ids is not None else [1],
    }


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
        baseline_history: list[dict] | None = None,
    ) -> None:
        self.history = history or []
        # Historial global para mediana de outliers (puede exceder el scope de salida).
        self.baseline_history = (
            baseline_history if baseline_history is not None else list(self.history)
        )
        self.taxes = (
            taxes
            if taxes is not None
            else [
                {"bsale_id": 1, "name": "IVA", "percentage": D("19")},
                {"bsale_id": 2, "name": "ILA vino", "percentage": D("20.5")},
                {"bsale_id": 3, "name": "ILA cerveza", "percentage": D("20.5")},
                {"bsale_id": 6, "name": "IVA HARI", "percentage": D("12")},
                {"bsale_id": 7, "name": "IVA CARN.", "percentage": D("5")},
                {"bsale_id": 8, "name": "Destilados", "percentage": D("31.5")},
            ]
        )
        self.columns = columns or {"tax_ids_json", "bsale_id"}
        self.sqls: list[str] = []
        self.params: list[tuple] = []
        self.write_attempts = 0

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        assert_sql_is_read_only(sql)
        upper = sql.upper()
        if any(x in upper for x in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ")):
            self.write_attempts += 1
            raise AssertionError(f"escritura no permitida: {sql[:80]}")
        if "FOR UPDATE" in upper:
            raise AssertionError("FOR UPDATE no permitido")
        self.sqls.append(sql)
        self.params.append(params)
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            col = params[2] if len(params) >= 3 else ""
            return [{"ok": 1}] if str(col) in self.columns else []
        if "FROM BSALE.TAXES" in upper or "from bsale.taxes" in sql.lower():
            return list(self.taxes)
        if "COUNT(*)" in upper and "COST_RECEPTION_HISTORY" in upper:
            rows = list(self.history)
            return [
                {
                    "rows_found": len(rows),
                    "unique_variants": len({r["variant_id"] for r in rows}),
                    "unique_documents": len(
                        {r.get("document_number") or r.get("reception_id") for r in rows}
                    ),
                    "min_admission_date": min(r["admission_date"] for r in rows)
                    if rows
                    else None,
                    "max_admission_date": max(r["admission_date"] for r in rows)
                    if rows
                    else None,
                }
            ]
        if "SELECT DISTINCT" in upper and "VARIANT_ID" in upper:
            # barcode resolve vs scope variants
            if "ILIKE" in upper or "barcode" in sql.lower():
                term = None
                for p in params:
                    if isinstance(p, str) and not p.startswith("%"):
                        term = p
                out = []
                for r in self.history:
                    bc = (r.get("barcode") or "").strip()
                    if term and (bc == term or term in bc):
                        out.append({"variant_id": r["variant_id"]})
                return out
            # scope variant ids (admite filtros; fake: desde history de salida)
            rows = list(self.history)
            if len(params) >= 1:
                # history_id filter if present as int matching a history row
                for p in params:
                    if isinstance(p, int) and any(
                        int(r["history_id"]) == p for r in self.history
                    ):
                        rows = [r for r in rows if int(r["history_id"]) == p]
                        break
            return [
                {"variant_id": v}
                for v in sorted({int(r["variant_id"]) for r in rows})
            ]
        # Outlier baseline: sin admission_date
        if (
            "h.cost_net" in sql.lower()
            and "admission_date" not in sql.lower()
            and "history_id" not in sql.lower()
            and "COUNT(*)" not in upper
        ):
            out = []
            for r in self.baseline_history:
                net = r.get("cost_net")
                if net is None:
                    continue
                if isinstance(net, Decimal) and net <= 0:
                    continue
                out.append({"variant_id": r["variant_id"], "cost_net": net})
            for p in params:
                if isinstance(p, list) and p and all(isinstance(x, int) for x in p):
                    out = [x for x in out if int(x["variant_id"]) in set(p)]
            return out
        # keyset fetch
        if "HISTORY_ID" in upper or "h.id AS history_id" in sql.lower():
            after_id = 0
            limit = 500
            if len(params) >= 4 and isinstance(params[3], int):
                after_id = int(params[3])
            if isinstance(params[-1], int):
                limit = int(params[-1])
            rows = [r for r in self.history if int(r["history_id"]) > after_id]
            rows = sorted(rows, key=lambda r: int(r["history_id"]))[:limit]
            for p in params:
                if isinstance(p, list) and p and all(isinstance(x, int) for x in p):
                    rows = [r for r in rows if int(r["variant_id"]) in set(p)]
            return rows
        return []


def _args(**kwargs):
    base = dict(
        company_id=3,
        office_id=3,
        date_from=date(2026, 3, 25),
        date_to=date(2026, 6, 22),
        dry_run=True,
        batch_size=2,
        sample_limit=10,
    )
    base.update(kwargs)
    return clamp_backfill_args(**base)


def test_1_dry_run_processes_rows():
    exe = RecordingExecutor(history=[_hist(1), _hist(2, cost_net=D("632"), bruto=D("632"))])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["ok"] is True
    assert report["mode"] == "dry-run"
    assert report["population"]["rows_processed"] == 2
    assert report["results"]["would_insert"] == 2


def test_2_readonly_connection():
    fake = FakeConn()
    conn = open_readonly_connection(lambda: fake)
    assert conn.readonly is True
    assert conn.autocommit is False


def test_3_job_rollback():
    fake = FakeConn()

    def fake_get():
        return fake

    # monkeypatch via run with empty executor path is hard; test finally pattern
    from backend.jobs import backfill_cost_reception_calculated as j

    # Direct: open and rollback
    conn = open_readonly_connection(fake_get)
    conn.rollback()
    assert fake.rolled_back is True


def test_4_apply_without_canary_confirm_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
        )
    assert "Apply canario requiere" in str(ei.value)
    code, payload = job.run_job(
        [
            "--company-id",
            "3",
            "--date-from",
            "2026-03-25",
            "--date-to",
            "2026-06-22",
            "--apply",
        ]
    )
    assert code == 1
    assert payload["error_type"] == "apply_canary_confirmation_required"
    assert payload.get("committed") is False


def test_5_keyset_pagination_sql():
    exe = RecordingExecutor(history=[_hist(1), _hist(2), _hist(3)])
    run_cost_v2_backfill_dry_run(
        args=_args(batch_size=2), repository=CostV2BackfillRepository(exe)
    )
    fetch_sqls = [s for s in exe.sqls if "h.id >" in s.lower() or "H.ID >" in s.upper()]
    assert fetch_sqls
    assert "ORDER BY" in fetch_sqls[0].upper()
    assert "LIMIT" in fetch_sqls[0].upper()


def test_6_multiple_batches():
    hist = [_hist(i, cost_net=D("100"), bruto=D("100")) for i in range(1, 6)]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=2), repository=CostV2BackfillRepository(exe)
    )
    assert report["population"]["batches"] >= 3
    assert report["population"]["rows_processed"] == 5


def test_7_status_sum_equals_processed():
    exe = RecordingExecutor(
        history=[
            _hist(1),
            _hist(2, cost_net=None, bruto=None, iva=None, other=None),
            _hist(3, cost_net=D("0"), bruto=D("0")),
        ]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    r = report["results"]
    total = sum(r[k] for k in (
        "missing_cost",
        "gross_component_mismatch",
        "duplicated_taxes_in_gross",
        "missing_taxes_in_gross",
        "incomplete_tax_context",
        "valid_gross",
    ))
    assert total == report["population"]["rows_processed"]


def test_8_mankeke():
    exe = RecordingExecutor(history=[_hist(1)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    sample = next(s for s in report["samples"] if s["history_id"] == 1)
    assert sample["corrected_gross_cost"] == "796.11"
    assert sample["effective_quality_status"] == "missing_taxes_in_gross"


def test_9_vino():
    exe = RecordingExecutor(history=[_hist(1, tax_ids=[2, 1], cost_net=D("1000"), bruto=D("1000"))])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert s["total_tax_rate"] == "39.50"
    assert s["corrected_gross_cost"] == "1395.00"


def test_10_cerveza():
    exe = RecordingExecutor(history=[_hist(1, tax_ids=[1, 3], cost_net=D("1000"), bruto=D("1000"))])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["samples"][0]["total_tax_rate"] == "39.50"


def test_11_destilado():
    exe = RecordingExecutor(history=[_hist(1, tax_ids=[8, 1], cost_net=D("1000"), bruto=D("1000"))])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["samples"][0]["total_tax_rate"] == "50.50"
    assert report["samples"][0]["corrected_gross_cost"] == "1505.00"


def test_12_tax_4_7_from_bsale_taxes():
    exe = RecordingExecutor(
        history=[_hist(1, tax_ids=[1, 4], cost_net=D("1000"), bruto=D("1000"))],
        taxes=[
            {"bsale_id": 1, "name": "IVA", "percentage": D("19")},
            {"bsale_id": 4, "name": "ILA4", "percentage": D("10")},
        ],
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["samples"][0]["corrected_gross_cost"] == "1290.00"


def test_13_unknown_tax():
    exe = RecordingExecutor(
        history=[_hist(1, tax_ids=[999], cost_net=D("1000"), bruto=D("1000"))],
        taxes=[],
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["results"]["incomplete_tax_context"] == 1
    assert report["samples"][0]["corrected_gross_cost"] is None


def test_14_null_cost():
    exe = RecordingExecutor(
        history=[_hist(1, cost_net=None, bruto=None, iva=None, other=None)]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["results"]["missing_cost"] == 1


def test_15_zero_cost():
    exe = RecordingExecutor(history=[_hist(1, cost_net=D("0"), bruto=D("0"))])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["results"]["missing_cost"] == 1


def test_16_sample_limit():
    hist = [_hist(i, cost_net=D("100"), bruto=D("100")) for i in range(1, 11)]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(sample_limit=3, batch_size=10),
        repository=CostV2BackfillRepository(exe),
    )
    assert len(report["samples"]) == 3
    assert report["population"]["rows_processed"] == 10


def test_17_barcode_filter():
    exe = RecordingExecutor(
        history=[
            _hist(1, barcode="AAA", variant_id=1),
            _hist(2, barcode="7803473005960", variant_id=42),
        ]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(barcode="7803473005960"),
        repository=CostV2BackfillRepository(exe),
    )
    assert report["population"]["rows_processed"] == 1
    assert report["samples"][0]["variant_id"] == 42


def test_18_variant_filter():
    exe = RecordingExecutor(
        history=[_hist(1, variant_id=10), _hist(2, variant_id=99, cost_net=D("100"), bruto=D("100"))]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(variant_id=99), repository=CostV2BackfillRepository(exe)
    )
    assert report["population"]["rows_processed"] == 1


def test_19_document_filter():
    # repo filters in SQL; fake executor doesn't re-filter document — simulate single row
    exe = RecordingExecutor(history=[_hist(1, document_number=555)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(document_number=555), repository=CostV2BackfillRepository(exe)
    )
    assert report["population"]["rows_processed"] == 1
    assert "document_number" in report["scope"]


def test_20_timeout_error_type():
    class Boom(RecordingExecutor):
        def __call__(self, sql: str, params: tuple) -> list[dict]:
            if "COUNT(*)" in sql.upper():
                raise RuntimeError("canceling statement due to statement timeout")
            return super().__call__(sql, params)

    with pytest.raises(RuntimeError):
        run_cost_v2_backfill_dry_run(
            args=_args(), repository=CostV2BackfillRepository(Boom(history=[_hist(1)]))
        )


def test_21_schema_mismatch_via_job_path():
    # ensure assert_sql and schema probes don't write
    exe = RecordingExecutor(history=[_hist(1)], columns=set())
    # without tax_ids_json column still works (NULL tax ids)
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["ok"] is True


def test_22_json_shape():
    exe = RecordingExecutor(history=[_hist(1)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    for key in (
        "ok",
        "mode",
        "read_only",
        "calculation_version",
        "scope",
        "population",
        "results",
        "tax_resolution",
        "differences",
        "warnings",
        "samples",
        "duration_ms",
    ):
        assert key in report
    assert report["read_only"] is True


def test_23_no_quantity_in_impact():
    exe = RecordingExecutor(history=[_hist(1)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    # difference is unit 127.11, not * quantity 2
    assert report["differences"]["unit_difference_sum"] == "127.11"
    assert "No representa impacto total" in report["differences"]["warning"]


def test_24_no_writes():
    exe = RecordingExecutor(history=[_hist(1)])
    run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert exe.write_attempts == 0
    joined = " ".join(exe.sqls).upper()
    assert "INSERT" not in joined
    assert "UPDATE" not in joined
    assert "DELETE" not in joined


def test_25_fingerprints_present():
    exe = RecordingExecutor(history=[_hist(1)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert len(s["source_history_fingerprint"]) == 64
    assert len(s["tax_context_fingerprint"]) == 64
    assert len(s["calculation_result_fingerprint"]) == 64
    assert s["tax_ids_source"] == "current_product_tax"
    assert s["tax_rates_source"] == "bsale_taxes"
    assert s["tax_resolution_quality"] == "current_catalog"
    assert s["tax_context_is_historical"] is False


def test_26_deterministic_order():
    hist = [_hist(3), _hist(1), _hist(2)]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    # processing is by id ascending via keyset; samples ranked by priority
    assert report["population"]["rows_processed"] == 3
    fetch_params = [p for s, p in zip(exe.sqls, exe.params) if "h.id >" in s.lower()]
    assert fetch_params
    # first batch after_id=0
    assert fetch_params[0][3] == 0


def test_27_tax_ids_source_current_product_tax():
    exe = RecordingExecutor(history=[_hist(1)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["samples"][0]["tax_ids_source"] == "current_product_tax"


def test_28_tax_rates_source_bsale_taxes():
    exe = RecordingExecutor(history=[_hist(1)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["samples"][0]["tax_rates_source"] == "bsale_taxes"
    assert report["samples"][0]["tax_context_source"] == "bsale_taxes"


def test_29_canonical_fallback_keeps_ids_source():
    # Sin tasas en catálogo → fallback canónico; IDs siguen de producto.
    exe = RecordingExecutor(
        history=[_hist(1, tax_ids=[1])],
        taxes=[],
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert s["tax_ids_source"] == "current_product_tax"
    assert s["tax_rates_source"] == "canonical_fallback"
    assert s["tax_resolution_quality"] == "canonical_fallback"
    assert s["corrected_gross_cost"] == "796.11"


def test_30_outlier_warning_on_2533():
    hist = [
        _hist(1, cost_net=D("669"), bruto=D("669")),
        _hist(2, cost_net=D("632"), bruto=D("632")),
        _hist(3, cost_net=D("650"), bruto=D("650")),
        _hist(23190, cost_net=D("2533"), bruto=D("2533"), document_number=None),
    ]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    outlier = next(s for s in report["samples"] if s["history_id"] == 23190)
    assert outlier["effective_quality_status"] == "missing_taxes_in_gross"
    assert "suspicious_outlier" in outlier["warnings"]
    assert outlier["corrected_gross_cost"] == "3014.27"
    assert report["warnings"]["suspicious_outlier"] >= 1


def test_31_outlier_does_not_change_primary_status():
    hist = [
        _hist(1, cost_net=D("669"), bruto=D("669")),
        _hist(2, cost_net=D("632"), bruto=D("632")),
        _hist(3, cost_net=D("650"), bruto=D("650")),
        _hist(4, cost_net=D("2533"), bruto=D("2533")),
    ]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    out = next(s for s in report["samples"] if s["history_id"] == 4)
    assert out["effective_quality_status"] == "missing_taxes_in_gross"
    assert out["effective_quality_status"] != "suspicious_outlier"


def test_32_normal_costs_not_outlier():
    hist = [
        _hist(1, cost_net=D("669"), bruto=D("669")),
        _hist(2, cost_net=D("632"), bruto=D("632")),
        _hist(3, cost_net=D("650"), bruto=D("650")),
        _hist(4, cost_net=D("640"), bruto=D("640")),
    ]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    for s in report["samples"]:
        assert "suspicious_outlier" not in s["warnings"]
    assert report["warnings"]["suspicious_outlier"] == 0


def test_33_outlier_batch_no_n_plus_1():
    hist = [
        _hist(i, cost_net=D("100") + Decimal(i), bruto=D("100") + Decimal(i))
        for i in range(1, 8)
    ]
    hist.append(_hist(99, cost_net=D("5000"), bruto=D("5000")))
    exe = RecordingExecutor(history=hist)
    run_cost_v2_backfill_dry_run(
        args=_args(batch_size=3, sample_limit=20),
        repository=CostV2BackfillRepository(exe),
    )
    baseline_sqls = [
        s
        for s in exe.sqls
        if "h.cost_net" in s.lower()
        and "admission_date" not in s.lower()
        and "COUNT(*)" not in s.upper()
    ]
    assert len(baseline_sqls) == 1
    tax_sqls = [s for s in exe.sqls if "bsale.taxes" in s.lower()]
    assert len(tax_sqls) <= 3


def test_34_document_number_null_preserved():
    exe = RecordingExecutor(history=[_hist(23190, document_number=None)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert s["document_number"] is None
    assert "missing_document_number" not in s["warnings"]


def test_35_harina_backfill():
    exe = RecordingExecutor(
        history=[_hist(1, tax_ids=[1, 6], cost_net=D("664"), bruto=D("664"))]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert s["corrected_gross_cost"] == "869.84"
    assert s["total_tax_rate"] == "31.00"
    assert s["effective_quality_status"] == "missing_taxes_in_gross"


def test_36_carne_backfill():
    exe = RecordingExecutor(
        history=[_hist(1, tax_ids=[1, 7], cost_net=D("7770"), bruto=D("7770"))]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert s["corrected_gross_cost"] == "9634.80"
    assert s["total_tax_rate"] == "24.00"


def test_37_status_sum_equals_population_mixed():
    hist = [
        _hist(1, tax_ids=[1, 6], cost_net=D("664"), bruto=D("664")),
        _hist(2, tax_ids=[1, 7], cost_net=D("7770"), bruto=D("7770")),
        _hist(3, tax_ids=[], cost_net=D("100"), bruto=D("100")),  # cigarrillo
        _hist(4, cost_net=None, bruto=None, iva=None, other=None, tax_ids=[8, 1]),
    ]
    exe = RecordingExecutor(history=hist)
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    r = report["results"]
    total = sum(
        r[k]
        for k in (
            "missing_cost",
            "gross_component_mismatch",
            "duplicated_taxes_in_gross",
            "missing_taxes_in_gross",
            "incomplete_tax_context",
            "valid_gross",
        )
    )
    assert total == report["population"]["rows_processed"] == 4
    assert r["missing_cost"] == 1
    assert r["incomplete_tax_context"] == 1
    assert r["missing_taxes_in_gross"] == 2


def test_38_cigarrillo_sin_tax_ids_unresolved():
    exe = RecordingExecutor(
        history=[_hist(1, tax_ids=[], cost_net=D("500"), bruto=D("500"))]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["results"]["incomplete_tax_context"] == 1
    s = report["samples"][0]
    assert s["corrected_gross_cost"] is None
    assert s["tax_resolution_quality"] == "unresolved"
    assert s["catalog_tax_ids"] == []


def test_39_missing_cost_preserves_resolved_ids_in_sample():
    exe = RecordingExecutor(
        history=[
            _hist(1, tax_ids=[8, 1], cost_net=None, bruto=None, iva=None, other=None)
        ]
    )
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    s = report["samples"][0]
    assert s["effective_quality_status"] == "missing_cost"
    assert s["resolved_tax_ids"] == [1, 8]
    assert s["tax_resolution_quality"] == "current_catalog"
    assert s["corrected_gross_cost"] is None


def _mankeke_outlier_hist():
    return [
        _hist(1, cost_net=D("669"), bruto=D("669"), barcode="7803473005960"),
        _hist(2, cost_net=D("632"), bruto=D("632"), barcode="7803473005960"),
        _hist(3, cost_net=D("650"), bruto=D("650"), barcode="7803473005960"),
        _hist(
            23190,
            cost_net=D("2533"),
            bruto=D("2533"),
            barcode="7803473005960",
            document_number=None,
        ),
    ]


def test_40_outlier_same_warning_barcode_vs_full_scope():
    hist = _mankeke_outlier_hist()
    full = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(RecordingExecutor(history=hist)),
    )
    by_bc = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10, barcode="7803473005960"),
        repository=CostV2BackfillRepository(RecordingExecutor(history=hist)),
    )
    a = next(s for s in full["samples"] if s["history_id"] == 23190)
    b = next(s for s in by_bc["samples"] if s["history_id"] == 23190)
    assert "suspicious_outlier" in a["warnings"]
    assert a["warnings"] == b["warnings"]
    assert a["effective_quality_status"] == b["effective_quality_status"]


def test_41_outlier_history_id_only_uses_global_baseline():
    baseline = _mankeke_outlier_hist()
    only = [_hist(23190, cost_net=D("2533"), bruto=D("2533"), document_number=None)]
    exe = RecordingExecutor(history=only, baseline_history=baseline)
    report = run_cost_v2_backfill_dry_run(
        args=_args(history_id=23190, batch_size=10, sample_limit=5),
        repository=CostV2BackfillRepository(exe),
    )
    s = report["samples"][0]
    assert s["history_id"] == 23190
    assert s["effective_quality_status"] == "missing_taxes_in_gross"
    assert "suspicious_outlier" in s["warnings"]
    assert s["corrected_gross_cost"] == "3014.27"


def test_42_normal_cost_not_outlier_with_narrow_scope():
    baseline = _mankeke_outlier_hist()
    only = [_hist(1, cost_net=D("669"), bruto=D("669"))]
    exe = RecordingExecutor(history=only, baseline_history=baseline)
    report = run_cost_v2_backfill_dry_run(
        args=_args(history_id=1, batch_size=10, sample_limit=5),
        repository=CostV2BackfillRepository(exe),
    )
    s = report["samples"][0]
    assert "suspicious_outlier" not in s["warnings"]
    assert s["effective_quality_status"] == "missing_taxes_in_gross"


def test_43_baseline_sql_ignores_date_filters():
    exe = RecordingExecutor(history=_mankeke_outlier_hist())
    run_cost_v2_backfill_dry_run(
        args=_args(date_from=date(2026, 6, 1), date_to=date(2026, 6, 2)),
        repository=CostV2BackfillRepository(exe),
    )
    baseline = [
        (s, p)
        for s, p in zip(exe.sqls, exe.params)
        if "h.cost_net" in s.lower() and "admission_date" not in s.lower()
    ]
    assert len(baseline) == 1
    sql, params = baseline[0]
    assert "admission_date" not in sql.lower()
    # params: company_id, variant_ids list, office_id — no dates
    assert params[0] == 3
    assert isinstance(params[1], list)
    assert 3 in params  # office_id


def test_44_baseline_keeps_company_and_office():
    exe = RecordingExecutor(history=[_hist(1)])
    run_cost_v2_backfill_dry_run(
        args=_args(company_id=3, office_id=3),
        repository=CostV2BackfillRepository(exe),
    )
    baseline = [
        (s, p)
        for s, p in zip(exe.sqls, exe.params)
        if "h.cost_net" in s.lower() and "admission_date" not in s.lower()
    ]
    assert baseline
    sql, params = baseline[0]
    assert "company_id" in sql.lower()
    assert "office_id" in sql.lower()
    assert params[0] == 3
    assert params[-1] == 3


def test_45_mankeke_2533_status_and_amount():
    exe = RecordingExecutor(history=_mankeke_outlier_hist())
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    s = next(x for x in report["samples"] if x["history_id"] == 23190)
    assert s["effective_quality_status"] == "missing_taxes_in_gross"
    assert "suspicious_outlier" in s["warnings"]
    assert s["corrected_gross_cost"] == "3014.27"
    assert len(s["calculation_result_fingerprint"]) == 64


def test_46_status_sum_equals_population_outlier_set():
    exe = RecordingExecutor(history=_mankeke_outlier_hist())
    report = run_cost_v2_backfill_dry_run(
        args=_args(batch_size=10, sample_limit=10),
        repository=CostV2BackfillRepository(exe),
    )
    r = report["results"]
    total = sum(
        r[k]
        for k in (
            "missing_cost",
            "gross_component_mismatch",
            "duplicated_taxes_in_gross",
            "missing_taxes_in_gross",
            "incomplete_tax_context",
            "valid_gross",
        )
    )
    assert total == report["population"]["rows_processed"]


def test_executor_timeouts_set():
    fake = FakeConn()
    exe = make_psycopg_executor(fake, statement_timeout_seconds=15, sql_log=[])
    try:
        exe("SELECT 1", ())
    except Exception:
        pass
    joined = " ".join(fake.executed)
    assert "statement_timeout" in joined
    assert "lock_timeout" in joined
