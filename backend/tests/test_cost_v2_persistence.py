"""Tests persistencia canaria Costos V2 (sin PostgreSQL real / sin prod)."""

from __future__ import annotations

import copy
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import patch
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
    run_cost_v2_scope_apply,
    validate_calculation_before_persist,
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
        view_exists: bool = True,
        fail_on_insert: bool = False,
        fail_after_writes: int | None = None,
        mutate_source_after_calc: bool = False,
        corrupt_readback: bool = False,
        corrupt_latest: bool = False,
        corrupt_result_fp: bool = False,
        barcode_variants: list[int] | None = None,
    ) -> None:
        self.history = history
        self.baseline_history = (
            baseline_history if baseline_history is not None else list(history)
        )
        self.table_exists = table_exists
        self.view_exists = view_exists
        self.fail_on_insert = fail_on_insert
        self.fail_after_writes = fail_after_writes
        self.mutate_source_after_calc = mutate_source_after_calc
        self.corrupt_readback = corrupt_readback
        self.corrupt_latest = corrupt_latest
        self.corrupt_result_fp = corrupt_result_fp
        self.barcode_variants = barcode_variants
        self.calculated: dict[tuple[int, str], dict[str, Any]] = {}
        self._committed: dict[tuple[int, str], dict[str, Any]] = {}
        self.sqls: list[str] = []
        self.writes = 0
        self.commit_count = 0
        self.committed = False
        self.rolled_back = False
        self._calc_started = False
        self._clock = 0
        self.taxes = [
            {"bsale_id": 1, "name": "IVA", "percentage": D("19")},
        ]
        self.columns = {"tax_ids_json", "bsale_id"}

    def seed_calculated(self, row: dict[str, Any]) -> None:
        key = (int(row["history_id"]), str(row["calculation_version"]))
        stored = copy.deepcopy(row)
        self.calculated[key] = stored
        self._committed[key] = copy.deepcopy(stored)

    def commit(self) -> None:
        self.committed = True
        self.commit_count += 1
        self._committed = copy.deepcopy(self.calculated)

    def rollback(self) -> None:
        self.rolled_back = True
        self.calculated = copy.deepcopy(self._committed)

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        self.sqls.append(sql)
        upper = sql.upper()
        if "TO_REGCLASS" in upper:
            if "v_cost_reception_calculated_latest" in sql.lower():
                return [{"ok": self.view_exists}]
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
            for p in params:
                if isinstance(p, list) and p and all(isinstance(x, int) for x in p):
                    rows = [r for r in rows if int(r["variant_id"]) in set(p)]
            if "h.id =" in sql.lower() or " AND h.id = %s" in sql:
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
            if "ILIKE" in upper or "barcode" in sql.lower():
                if self.barcode_variants is not None:
                    return [{"variant_id": v} for v in self.barcode_variants]
                term = None
                for p in params:
                    if isinstance(p, str) and not p.startswith("%"):
                        term = p
                out = []
                seen: set[int] = set()
                for r in self.history:
                    bc = (r.get("barcode") or "").strip()
                    if term and (bc == term or term in bc):
                        vid = int(r["variant_id"])
                        if vid not in seen:
                            seen.add(vid)
                            out.append({"variant_id": vid})
                return out
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
            if (
                self.fail_after_writes is not None
                and self.writes >= self.fail_after_writes
            ):
                raise RuntimeError("insert boom mid-batch")
            self.writes += 1
            history_id = int(params[0])
            version = str(params[1])
            key = (history_id, version)
            was_inserted = key not in self.calculated
            self._clock += 1
            now = datetime(2026, 7, 31, 12, 0, self._clock)

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
                if self.corrupt_result_fp:
                    out["calculation_result_fingerprint"] = "deadbeef"
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
                if isinstance(p, list) and p and all(isinstance(x, int) for x in p):
                    rows = [r for r in rows if int(r["variant_id"]) in set(p)]
            if "h.id >" in sql.lower():
                after_id = int(params[3]) if len(params) >= 4 else 0
                limit = int(params[-1]) if params else 500
                rows = [r for r in rows if int(r["history_id"]) > after_id]
                if " AND h.id = %s" in sql or (
                    "h.id =" in sql.lower() and "h.id >" in sql.lower()
                ):
                    # history_id filter: param after after_id / office
                    for p in params[4:]:
                        if isinstance(p, int) and any(
                            int(r["history_id"]) == p for r in self.history
                        ):
                            rows = [r for r in rows if int(r["history_id"]) == p]
                            break
                rows = sorted(rows, key=lambda r: int(r["history_id"]))[:limit]
                return rows
            # source verify / exact id
            for p in params:
                if isinstance(p, int) and any(
                    int(r["history_id"]) == p for r in self.history
                ):
                    rows = [r for r in rows if int(r["history_id"]) == p]
                    break
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


# ---------------------------------------------------------------------------
# E.3 — apply-scope canary
# ---------------------------------------------------------------------------


def _scope_args(**kwargs):
    base = dict(
        company_id=3,
        office_id=3,
        date_from=date(2026, 3, 25),
        date_to=date(2026, 6, 22),
        dry_run=False,
        apply=True,
        apply_scope=True,
        barcode="7803473005960",
        confirm_row_count=14,
        max_apply_rows=100,
        statement_timeout_seconds=30,
        batch_size=500,
    )
    base.update(kwargs)
    return clamp_backfill_args(**base)


def _fourteen_scope_history(*, quantity: Decimal = D("2")) -> list[dict]:
    """14 filas mismo barcode/variant; 23190 outlier; quantity≠1 para impacto unitario."""
    rows: list[dict] = []
    for i, hid in enumerate(range(23177, 23191)):
        net = D("2533") if hid == 23190 else D(str(600 + (i % 8) * 7))
        row = _hist(hid, cost_net=net, bruto=net)
        row["quantity"] = quantity
        rows.append(row)
    return rows


def test_e3_01_apply_scope_without_apply_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=True,
            apply=False,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=14,
        )
    assert "--apply-scope requiere --apply" in str(ei.value)


def test_e3_02_apply_scope_with_history_id_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            history_id=23190,
            barcode="7803473005960",
            confirm_row_count=14,
        )
    assert "no puede combinarse con --history-id" in str(ei.value)


def test_e3_03_without_barcode_or_variant_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            confirm_row_count=14,
        )
    assert "Apply scope requiere --barcode o --variant-id, pero no ambos" in str(
        ei.value
    )


def test_e3_04_barcode_and_variant_together_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            variant_id=10,
            confirm_row_count=14,
        )
    assert "Apply scope requiere --barcode o --variant-id, pero no ambos" in str(
        ei.value
    )


def test_e3_05_without_office_id_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=None,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=14,
        )
    assert "Apply scope requiere --office-id" in str(ei.value)


def test_e3_06_without_dates_rejected_by_parser_contract():
    # clamp exige date_from/date_to; sin ellos el job argparse falla.
    # Aquí validamos que apply-scope no relaja el contrato de fechas.
    with pytest.raises(TypeError):
        clamp_backfill_args(  # type: ignore[call-arg]
            company_id=3,
            office_id=3,
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=14,
        )


def test_e3_07_without_confirm_row_count_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
        )
    assert "Apply scope requiere --confirm-row-count" in str(ei.value)


def test_e3_08_confirm_row_count_zero_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=0,
        )
    assert "confirm-row-count debe ser > 0" in str(ei.value)


def test_e3_09_real_count_mismatch_rejected():
    hist = _fourteen_scope_history()
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_scope_apply(
            args=_scope_args(confirm_row_count=13),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "Cantidad real no coincide con --confirm-row-count" in str(ei.value)
    assert fake.committed is False
    assert fake.writes == 0


def test_e3_10_more_than_100_rows_rejected():
    hist = [
        _hist(i, cost_net=D("600"), bruto=D("600")) for i in range(1, 102)
    ]
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_scope_apply(
            args=_scope_args(confirm_row_count=101, max_apply_rows=100),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "Apply scope excede --max-apply-rows" in str(ei.value)
    assert fake.committed is False


def test_e3_11_max_apply_rows_over_100_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=14,
            max_apply_rows=101,
        )
    assert "Apply scope excede --max-apply-rows" in str(ei.value)


def test_e3_12_barcode_ambiguous_rejected():
    hist = _fourteen_scope_history()
    fake = PersistFake(
        history=hist, baseline_history=hist, barcode_variants=[10, 11]
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_scope_apply(
            args=_scope_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "exactamente una variante" in str(ei.value).lower()
    assert fake.writes == 0


def test_e3_13_zero_rows_rejected():
    fake = PersistFake(history=[])
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_scope_apply(
            args=_scope_args(
                barcode=None, variant_id=10, confirm_row_count=1
            ),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "Cantidad real no coincide con --confirm-row-count" in str(ei.value)


def test_e3_14_results_ordered_by_history_id():
    hist = _fourteen_scope_history()
    hist = list(reversed(hist))
    fake = PersistFake(history=hist, baseline_history=list(hist))
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    order: list[int] = []
    real_validate = validate_calculation_before_persist

    def _v(calc):
        order.append(calc.history_id)
        return real_validate(calc)

    with patch(
        "backend.services.analytics.cost_v2_backfill.validate_calculation_before_persist",
        side_effect=_v,
    ):
        run_cost_v2_scope_apply(
            args=_scope_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert order == sorted(order)
    assert order[0] == 23177
    assert order[-1] == 23190


def test_e3_15_full_calc_before_any_write():
    hist = _fourteen_scope_history()
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    validated = {"n": 0}
    real_validate = validate_calculation_before_persist

    def _v(calc):
        assert fake.writes == 0
        validated["n"] += 1
        return real_validate(calc)

    with patch(
        "backend.services.analytics.cost_v2_backfill.validate_calculation_before_persist",
        side_effect=_v,
    ):
        run_cost_v2_scope_apply(
            args=_scope_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert validated["n"] == 14
    assert fake.writes == 14


def test_e3_16_one_row_error_full_rollback():
    hist = _fourteen_scope_history()
    fake = PersistFake(
        history=hist, baseline_history=hist, fail_after_writes=5
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(RuntimeError):
        run_cost_v2_scope_apply(
            args=_scope_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert fake.rolled_back is True
    assert fake.committed is False
    assert fake.calculated == {}


def test_e3_17_to_35_initial_lot_with_one_preseeded():
    hist = _fourteen_scope_history()
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)

    # Pre-seed 23190 como canario E.2
    canary = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert canary["persistence"]["inserted"] == 1
    prior_batch = canary["run_batch_id"]
    prior_at = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )["calculated_at"]

    fake.committed = False
    fake.commit_count = 0
    report = run_cost_v2_scope_apply(
        args=_scope_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    assert report["mode"] == "apply-scope-canary"
    assert report["committed"] is True
    assert report["scope"]["rows_found"] == 14
    assert report["scope"]["rows_processed"] == 14
    assert report["scope"]["unique_variants"] == 1
    assert report["persistence"]["inserted"] == 13
    assert report["persistence"]["updated"] == 0
    assert report["persistence"]["unchanged"] == 1
    assert sum(report["persistence"].values()) == report["scope"]["rows_processed"]
    assert report["results"]["missing_taxes_in_gross"] == 14
    assert report["results"]["warnings"]["suspicious_outlier"] == 1
    assert report["verification"]["table_readback_ok"] is True
    assert report["verification"]["latest_view_ok"] is True
    assert report["verification"]["unchanged_metadata_preserved"] is True
    assert fake.commit_count == 1

    # inserted usan run_batch_id; unchanged conserva batch previo
    run_bid = report["run_batch_id"]
    for hid in range(23177, 23190):
        row = repo.read_calculation(
            history_id=hid, calculation_version=CALCULATION_VERSION
        )
        assert row is not None
        assert str(row["calculation_batch_id"]) == run_bid
        latest = repo.read_latest_view(history_id=hid)
        assert len(latest) == 1

    unchanged = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )
    assert str(unchanged["calculation_batch_id"]) == prior_batch
    assert unchanged["calculated_at"] == prior_at
    assert "suspicious_outlier" in (
        unchanged["warnings_json"]
        if isinstance(unchanged["warnings_json"], list)
        else json.loads(unchanged["warnings_json"])
    )

    payload = json.dumps(report, default=str)
    assert "DATABASE_URL" not in payload
    assert "password" not in payload.lower()
    assert "postgres://" not in payload.lower()

    # quantity no multiplica impacto: diferencia unitaria 2533*0.19
    assert unchanged["gross_difference_amount"] in (D("481.27"), D("481.2700"))
    assert all("variant_cost" not in s.lower() for s in fake.sqls)

    # segunda ejecución = 14 unchanged
    fake.committed = False
    fake.commit_count = 0
    r2 = run_cost_v2_scope_apply(
        args=_scope_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["persistence"] == {"inserted": 0, "updated": 0, "unchanged": 14}
    assert r2["run_batch_id"] != run_bid  # nuevo UUID de corrida
    # metadata de 23190 intacta
    still = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )
    assert str(still["calculation_batch_id"]) == prior_batch
    assert still["calculated_at"] == prior_at


def test_e3_22_updated_uses_run_batch_id():
    hist = _fourteen_scope_history()
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    r1 = run_cost_v2_scope_apply(
        args=_scope_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    # forzar update en una fila: fingerprint distinto
    key = (23177, CALCULATION_VERSION)
    fake.calculated[key]["calculation_result_fingerprint"] = "old-fp"
    fake._committed[key]["calculation_result_fingerprint"] = "old-fp"
    old_batch = fake.calculated[key]["calculation_batch_id"]

    fake.committed = False
    r2 = run_cost_v2_scope_apply(
        args=_scope_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["persistence"]["updated"] == 1
    assert r2["persistence"]["unchanged"] == 13
    row = repo.read_calculation(
        history_id=23177, calculation_version=CALCULATION_VERSION
    )
    assert str(row["calculation_batch_id"]) == r2["run_batch_id"]
    assert str(row["calculation_batch_id"]) != old_batch
    assert r2["run_batch_id"] != r1["run_batch_id"]


def test_e3_25_bad_fingerprint_readback_rollback():
    hist = _fourteen_scope_history()
    fake = PersistFake(
        history=hist, baseline_history=hist, corrupt_result_fp=True
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError):
        run_cost_v2_scope_apply(
            args=_scope_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert fake.rolled_back is True
    assert fake.committed is False
    assert fake.calculated == {}


def test_e3_26_source_change_rollback():
    hist = _fourteen_scope_history()
    fake = PersistFake(
        history=hist,
        baseline_history=hist,
        mutate_source_after_calc=True,
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_scope_apply(
            args=_scope_args(),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "source_history_fingerprint" in str(ei.value).lower() or ei.value.error_type == (
        "source_fingerprint_changed"
    )
    assert fake.rolled_back is True
    assert fake.committed is False
    assert fake.calculated == {}


def test_e3_29_dry_run_still_read_only():
    from backend.tests.test_cost_v2_backfill import RecordingExecutor, _args, _hist as h

    exe = RecordingExecutor(history=[h(1), h(2)])
    report = run_cost_v2_backfill_dry_run(
        args=_args(barcode="7803473005960"),
        repository=CostV2BackfillRepository(exe),
    )
    assert report["mode"] == "dry-run"
    assert exe.write_attempts == 0


def test_e3_30_individual_canary_still_works():
    baseline = _mankeke_baseline()
    fake = PersistFake(history=[_hist(23190)], baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["mode"] == "apply-canary"
    assert report["persistence"]["inserted"] == 1
    assert report["committed"] is True


def test_e3_validation_error_before_write_no_partial():
    hist = _fourteen_scope_history()
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    n = {"i": 0}
    real = validate_calculation_before_persist

    def _v(calc):
        n["i"] += 1
        if n["i"] == 7:
            raise AnalyticsValidationError(
                "fila inválida", error_type="persist_validation"
            )
        return real(calc)

    with patch(
        "backend.services.analytics.cost_v2_backfill.validate_calculation_before_persist",
        side_effect=_v,
    ):
        with pytest.raises(AnalyticsValidationError):
            run_cost_v2_scope_apply(
                args=_scope_args(),
                repository=repo,
                commit_fn=fake.commit,
                rollback_fn=fake.rollback,
            )
    assert fake.writes == 0
    assert fake.rolled_back is True
    assert fake.committed is False
    assert fake.calculated == {}


def test_e3_document_number_forbidden():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=14,
            document_number=99,
        )
    assert "document-number" in str(ei.value).lower()


def test_e3_dry_run_and_apply_mutually_exclusive():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=True,
            apply=True,
            apply_scope=True,
            barcode="7803473005960",
            confirm_row_count=14,
        )
    assert "no pueden usarse juntos" in str(ei.value)
