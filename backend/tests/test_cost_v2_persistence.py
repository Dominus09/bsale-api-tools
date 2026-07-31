"""Tests persistencia canaria Costos V2 (sin PostgreSQL real / sin prod)."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from backend.repositories.cost_v2_backfill_repo import (
    UPSERT_CALCULATION_SQL,
    CostV2BackfillRepository,
)
from backend.services.analytics.cost_v2_backfill import (
    clamp_backfill_args,
    run_cost_v2_backfill_dry_run,
    run_cost_v2_canary_apply,
)
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.money import D
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
)


def _hist(
    history_id: int,
    *,
    variant_id: int = 10,
    cost_net: Decimal = D("2533"),
    bruto: Decimal = D("2533"),
    tax_ids: list[int] | None = None,
    admission_date: date = date(2026, 6, 1),
) -> dict[str, Any]:
    return {
        "history_id": history_id,
        "company_id": 3,
        "office_id": 3,
        "variant_id": variant_id,
        "admission_date": admission_date,
        "quantity": D("1"),
        "cost_net": cost_net,
        "iva_amount": D("0"),
        "other_taxes": D("0"),
        "cost_bruto_erp": bruto,
        "created_at": datetime(2026, 6, 2),
        "barcode": "7803473005960",
        "product_name": "MANKEKE",
        "variant_name": "3 UN",
        "document_number": None,
        "reception_id": 50,
        "product_id": 20,
        "catalog_tax_ids_json": tax_ids if tax_ids is not None else [1],
    }


class PersistFake:
    """Executor RW en memoria: history + tabla calculated."""

    def __init__(
        self,
        *,
        history: list[dict],
        baseline_history: list[dict] | None = None,
        table_exists: bool = True,
        fail_on_insert: bool = False,
        mutate_source_after_calc: bool = False,
        corrupt_readback: bool = False,
        corrupt_latest: bool = False,
    ) -> None:
        self.history = history
        self.baseline_history = (
            baseline_history if baseline_history is not None else list(history)
        )
        self.table_exists = table_exists
        self.fail_on_insert = fail_on_insert
        self.mutate_source_after_calc = mutate_source_after_calc
        self.corrupt_readback = corrupt_readback
        self.corrupt_latest = corrupt_latest
        self.calculated: dict[tuple[int, str], dict[str, Any]] = {}
        self.sqls: list[str] = []
        self.writes = 0
        self.committed = False
        self.rolled_back = False
        self._calc_started = False
        self._clock = 0
        self.taxes = [
            {"bsale_id": 1, "name": "IVA", "percentage": D("19")},
        ]
        self.columns = {"tax_ids_json", "bsale_id"}

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True
        # Simula TX: vaciar escrituras no confirmadas — tests usan commit_fn explícito
        # aquí solo marcamos flag; el store se limpia en tests de rollback vía re-init.

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        self.sqls.append(sql)
        upper = sql.upper()
        if "TO_REGCLASS" in upper:
            return [{"ok": self.table_exists}]
        if "INFORMATION_SCHEMA.COLUMNS" in upper:
            col = params[2] if len(params) >= 3 else ""
            return [{"ok": 1}] if str(col) in self.columns else []
        if "FROM BSALE.TAXES" in upper or "from bsale.taxes" in sql.lower():
            return list(self.taxes)
        if "COUNT(*)" in upper and "COST_RECEPTION_CALCULATED" in upper:
            hid = int(params[0]) if params else None
            n = sum(1 for k in self.calculated if k[0] == hid)
            return [{"n": n}]
        if "COUNT(*)" in upper and "COST_RECEPTION_HISTORY" in upper:
            rows = list(self.history)
            if params and len(params) >= 4:
                # may include history_id
                for p in params:
                    if isinstance(p, int) and any(
                        int(r["history_id"]) == p for r in self.history
                    ):
                        rows = [r for r in rows if int(r["history_id"]) == p]
                        break
            return [
                {
                    "rows_found": len(rows),
                    "unique_variants": len({r["variant_id"] for r in rows}),
                    "unique_documents": 1,
                    "min_admission_date": min(r["admission_date"] for r in rows)
                    if rows
                    else None,
                    "max_admission_date": max(r["admission_date"] for r in rows)
                    if rows
                    else None,
                }
            ]
        if "SELECT DISTINCT" in upper and "VARIANT_ID" in upper:
            rows = list(self.history)
            for p in params:
                if isinstance(p, int) and any(
                    int(r["history_id"]) == p for r in self.history
                ):
                    rows = [r for r in rows if int(r["history_id"]) == p]
            return [
                {"variant_id": v}
                for v in sorted({int(r["variant_id"]) for r in rows})
            ]
        if (
            "h.cost_net" in sql.lower()
            and "admission_date" not in sql.lower()
            and "COUNT(*)" not in upper
            and "cost_reception_calculated" not in sql.lower()
        ):
            out = []
            for r in self.baseline_history:
                net = r.get("cost_net")
                if net is None or (isinstance(net, Decimal) and net <= 0):
                    continue
                out.append({"variant_id": r["variant_id"], "cost_net": net})
            for p in params:
                if isinstance(p, list):
                    out = [x for x in out if int(x["variant_id"]) in set(p)]
            return out
        if "INSERT INTO ANALYTICS.COST_RECEPTION_CALCULATED" in upper or (
            "insert into analytics.cost_reception_calculated" in sql.lower()
        ):
            if self.fail_on_insert:
                raise RuntimeError("insert boom")
            self.writes += 1
            history_id = int(params[0])
            version = str(params[1])
            key = (history_id, version)
            was_inserted = key not in self.calculated
            self._clock += 1
            now = datetime(2026, 7, 31, 12, 0, self._clock)
            import json

            row = {
                "history_id": history_id,
                "calculation_version": version,
                "calculation_batch_id": str(params[2]),
                "company_id": params[3],
                "office_id": params[4],
                "variant_id": params[5],
                "admission_date": params[6],
                "stored_cost_net": params[7],
                "stored_quantity": params[8],
                "stored_iva_amount": params[9],
                "stored_other_taxes": params[10],
                "stored_gross_cost": params[11],
                "reception_tax_ids_json": params[12],
                "catalog_tax_ids_json": params[13],
                "resolved_tax_ids_json": params[14],
                "iva_tax_id": params[15],
                "iva_rate": params[16],
                "calculated_iva_amount": params[17],
                "additional_taxes_json": params[18],
                "additional_tax_rate_total": params[19],
                "additional_tax_amount_total": params[20],
                "total_tax_rate": params[21],
                "corrected_gross_cost": params[22],
                "gross_difference_amount": params[23],
                "tax_rate_on_net_pct": params[24],
                "gross_understatement_vs_corrected_pct": params[25],
                "tax_context_source": params[26],
                "tax_ids_source": params[27],
                "tax_rates_source": params[28],
                "tax_context_as_of": params[29],
                "tax_context_is_historical": params[30],
                "tax_context_fingerprint": params[31],
                "tax_resolution_quality": params[32],
                "effective_quality_status": params[33],
                "warnings_json": params[34],
                "source_history_created_at": params[35],
                "source_history_fingerprint": params[36],
                "calculation_result_fingerprint": params[37],
                "calculated_at": now,
            }
            for k in (
                "reception_tax_ids_json",
                "catalog_tax_ids_json",
                "resolved_tax_ids_json",
                "additional_taxes_json",
                "warnings_json",
            ):
                v = row[k]
                if isinstance(v, str):
                    row[k] = json.loads(v)
            self.calculated[key] = row
            self._calc_started = True
            return [
                {
                    "history_id": history_id,
                    "calculation_version": version,
                    "calculation_batch_id": row["calculation_batch_id"],
                    "calculated_at": now,
                    "was_inserted": was_inserted,
                }
            ]
        # Tabla calculated (antes de coincidir HISTORY_ID genérico)
        if "cost_reception_calculated" in sql.lower() and "INSERT" not in upper:
            if "v_cost_reception_calculated_latest" in sql.lower():
                hid = int(params[0])
                rows = [r for (h, _), r in self.calculated.items() if h == hid]
                rows = sorted(
                    rows,
                    key=lambda r: (
                        r["calculated_at"],
                        r.get("calculation_version") or "",
                    ),
                    reverse=True,
                )
                out = rows[:1]
                if self.corrupt_latest and out:
                    out = [dict(out[0])]
                    out[0]["corrected_gross_cost"] = D("1")
                return out
            if len(params) >= 2 and "calculation_version" in sql.lower():
                key = (int(params[0]), str(params[1]))
                row = self.calculated.get(key)
                if not row:
                    return []
                out = dict(row)
                if self.corrupt_readback:
                    out["corrected_gross_cost"] = D("0.01")
                return [out]
            return []
        # history keyset / source verify
        if (
            "cost_reception_history" in sql.lower()
            or "h.id AS history_id" in sql.lower()
            or "h.id =" in sql.lower()
            or "h.id >" in sql.lower()
        ):
            rows = list(self.history)
            if self.mutate_source_after_calc and self._calc_started:
                rows = [dict(r) for r in rows]
                for r in rows:
                    r["cost_net"] = D("9999")
            for p in params:
                if isinstance(p, int) and any(
                    int(r["history_id"]) == p for r in self.history
                ):
                    rows = [r for r in rows if int(r["history_id"]) == p]
            if "h.id >" in sql.lower():
                after_id = int(params[3]) if len(params) >= 4 else 0
                limit = int(params[-1]) if params else 500
                rows = [r for r in rows if int(r["history_id"]) > after_id]
                rows = sorted(rows, key=lambda r: int(r["history_id"]))[:limit]
            return rows
        return []


def _canary_args(**kwargs):
    base = dict(
        company_id=3,
        office_id=3,
        date_from=date(2026, 3, 25),
        date_to=date(2026, 6, 22),
        dry_run=False,
        apply=True,
        history_id=23190,
        confirm_history_id=23190,
        statement_timeout_seconds=30,
    )
    base.update(kwargs)
    return clamp_backfill_args(**base)


def _mankeke_baseline():
    return [
        _hist(1, cost_net=D("669"), bruto=D("669")),
        _hist(2, cost_net=D("632"), bruto=D("632")),
        _hist(3, cost_net=D("650"), bruto=D("650")),
        _hist(23190, cost_net=D("2533"), bruto=D("2533")),
    ]


def test_1_apply_without_history_id_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            confirm_history_id=23190,
        )
    assert "Apply canario requiere" in str(ei.value)


def test_2_apply_without_confirm_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            history_id=23190,
        )
    assert "Apply canario requiere" in str(ei.value)


def test_3_different_ids_rejected():
    with pytest.raises(AnalyticsValidationError):
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            history_id=23190,
            confirm_history_id=1,
        )


def test_4_dry_run_and_apply_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=True,
            apply=True,
            history_id=23190,
            confirm_history_id=23190,
        )
    assert "no pueden usarse juntos" in str(ei.value)


def test_5_apply_barcode_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            history_id=23190,
            confirm_history_id=23190,
            barcode="x",
        )
    assert "barcode" in str(ei.value).lower()


def test_6_apply_variant_rejected():
    with pytest.raises(AnalyticsValidationError):
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            history_id=23190,
            confirm_history_id=23190,
            variant_id=10,
        )


def test_7_apply_document_rejected():
    with pytest.raises(AnalyticsValidationError):
        clamp_backfill_args(
            company_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            history_id=23190,
            confirm_history_id=23190,
            document_number=1,
        )


def test_8_zero_rows_rejected():
    fake = PersistFake(history=[])
    args = _canary_args()
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_canary_apply(
            args=args, repository=repo, commit_fn=fake.commit, rollback_fn=fake.rollback
        )
    assert "exactamente una fila" in str(ei.value)
    assert fake.committed is False


def test_9_more_than_one_row_rejected():
    fake = PersistFake(history=[_hist(23190), _hist(23191)])
    # count returns 2 if filter broken — force history without unique filter match
    # Use same history_id twice? Better: count_population with no history filter
    # Our fake filters by history_id when present. Simulate by two rows same id impossible.
    # Instead monkey: history with one id but count returns 2 via custom.
    class Boom(PersistFake):
        def __call__(self, sql: str, params: tuple) -> list[dict]:
            if "COUNT(*)" in sql.upper() and "COST_RECEPTION_HISTORY" in sql.upper():
                return [
                    {
                        "rows_found": 2,
                        "unique_variants": 1,
                        "unique_documents": 1,
                        "min_admission_date": date(2026, 6, 1),
                        "max_admission_date": date(2026, 6, 1),
                    }
                ]
            return super().__call__(sql, params)

    fake = Boom(history=[_hist(23190)])
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_canary_apply(
            args=_canary_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "exactamente una fila" in str(ei.value)


def test_10_table_missing_rejected():
    fake = PersistFake(history=[_hist(23190)], table_exists=False)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_canary_apply(
            args=_canary_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "no existe" in str(ei.value).lower()


def test_11_to_28_insert_mankeke_and_verifications():
    baseline = _mankeke_baseline()
    fake = PersistFake(
        history=[_hist(23190)],
        baseline_history=baseline,
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    assert report["mode"] == "apply-canary"
    assert report["committed"] is True
    assert fake.committed is True
    assert report["persistence"]["inserted"] == 1
    assert sum(report["persistence"].values()) == 1
    assert report["verification"]["table_readback_ok"] is True
    assert report["verification"]["latest_view_ok"] is True
    assert report["result"]["effective_quality_status"] == "missing_taxes_in_gross"
    assert "suspicious_outlier" in report["result"]["warnings"]
    assert report["result"]["corrected_gross_cost"] == "3014.27"
    assert report["result"]["calculated_iva_amount"] == "481.27"
    stored = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )
    assert stored is not None
    assert stored["stored_cost_net"] == D("2533.0000") or stored["stored_cost_net"] == D(
        "2533"
    )
    assert isinstance(stored["corrected_gross_cost"], Decimal)
    assert not isinstance(stored["corrected_gross_cost"], float)
    # JSONB arrays
    assert stored["resolved_tax_ids_json"] == [1]
    assert isinstance(stored["warnings_json"], list)


def test_15_rollback_on_insert_error():
    fake = PersistFake(history=[_hist(23190)], baseline_history=_mankeke_baseline(), fail_on_insert=True)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(RuntimeError):
        run_cost_v2_canary_apply(
            args=_canary_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert fake.rolled_back is True
    assert fake.committed is False
    assert fake.calculated == {}


def test_16_rollback_on_readback_mismatch():
    fake = PersistFake(
        history=[_hist(23190)],
        baseline_history=_mankeke_baseline(),
        corrupt_readback=True,
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError):
        run_cost_v2_canary_apply(
            args=_canary_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert fake.rolled_back is True
    assert fake.committed is False


def test_17_rollback_on_latest_mismatch():
    fake = PersistFake(
        history=[_hist(23190)],
        baseline_history=_mankeke_baseline(),
        corrupt_latest=True,
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError):
        run_cost_v2_canary_apply(
            args=_canary_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert fake.rolled_back is True


def test_18_rollback_if_source_fingerprint_changes():
    fake = PersistFake(
        history=[_hist(23190)],
        baseline_history=_mankeke_baseline(),
        mutate_source_after_calc=True,
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_canary_apply(
            args=_canary_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert ei.value.error_type == "source_fingerprint_changed"
    assert fake.committed is False


def test_19_20_21_second_run_unchanged():
    baseline = _mankeke_baseline()
    fake = PersistFake(history=[_hist(23190)], baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    r1 = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    batch1 = r1["calculation_batch_id"]
    calc_at = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )["calculated_at"]
    writes_after_first = fake.writes
    fake.committed = False
    r2 = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["persistence"]["unchanged"] == 1
    assert r2["calculation_batch_id"] == batch1
    assert fake.writes == writes_after_first  # no UPSERT
    stored = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )
    assert stored["calculated_at"] == calc_at
    assert stored["calculation_batch_id"] == batch1


def test_22_result_fingerprint_change_updates():
    baseline = _mankeke_baseline()
    fake = PersistFake(history=[_hist(23190)], baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    # mutate stored fingerprint to force update path
    key = (23190, CALCULATION_VERSION)
    fake.calculated[key]["calculation_result_fingerprint"] = "0" * 64
    old_batch = fake.calculated[key]["calculation_batch_id"]
    fake.committed = False
    r = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r["persistence"]["updated"] == 1
    assert r["calculation_batch_id"] != old_batch


def test_23_24_different_version_inserts_new_row():
    baseline = _mankeke_baseline()
    fake = PersistFake(history=[_hist(23190)], baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    fake.committed = False
    run_cost_v2_canary_apply(
        args=_canary_args(calculation_version="cost-v2.0.1"),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert repo.count_calculations_for_history(history_id=23190) == 2
    assert (23190, CALCULATION_VERSION) in fake.calculated
    assert (23190, "cost-v2.0.1") in fake.calculated


def test_26_null_monetary_preserved_missing_cost():
    fake = PersistFake(
        history=[
            _hist(23190, cost_net=None, bruto=None),
        ],
        baseline_history=_mankeke_baseline(),
    )
    # fix history nulls
    fake.history[0]["cost_net"] = None
    fake.history[0]["cost_bruto_erp"] = None
    fake.history[0]["iva_amount"] = None
    fake.history[0]["other_taxes"] = None
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    r = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r["result"]["effective_quality_status"] == "missing_cost"
    assert r["result"]["corrected_gross_cost"] is None
    stored = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )
    assert stored["corrected_gross_cost"] is None
    assert stored["calculated_iva_amount"] is None


def test_30_dry_run_no_writes():
    from backend.tests.test_cost_v2_backfill import RecordingExecutor, _args, _hist as h

    # reuse dry-run path
    exe = RecordingExecutor(history=[h(1)])
    # patch RecordingExecutor for to_regclass if called — dry-run shouldn't call it
    report = run_cost_v2_backfill_dry_run(
        args=_args(), repository=CostV2BackfillRepository(exe)
    )
    assert report["mode"] == "dry-run"
    assert exe.write_attempts == 0


def test_upsert_sql_has_on_conflict():
    assert "ON CONFLICT (history_id, calculation_version)" in UPSERT_CALCULATION_SQL
    assert "calculated_at = NOW()" in UPSERT_CALCULATION_SQL
    assert_sql_is_read_only("SELECT 1")
    with pytest.raises(Exception):
        assert_sql_is_read_only(UPSERT_CALCULATION_SQL)
