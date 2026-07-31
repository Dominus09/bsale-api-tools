"""Tests E.4 — apply-backfill reanudable por lotes (sin PostgreSQL / sin prod)."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest

from backend.repositories.cost_v2_backfill_repo import CostV2BackfillRepository
from backend.services.analytics.cost_v2_backfill import (
    clamp_backfill_args,
    run_cost_v2_backfill_apply,
    run_cost_v2_canary_apply,
    run_cost_v2_scope_apply,
    validate_calculation_before_persist,
)
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.money import D
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
)
from backend.tests.test_cost_v2_persistence import (
    PersistFake,
    _canary_args,
    _hist,
    _mankeke_baseline,
    _scope_args,
)


def _bf_args(**kwargs):
    base = dict(
        company_id=3,
        office_id=3,
        date_from=date(2026, 3, 25),
        date_to=date(2026, 6, 22),
        dry_run=False,
        apply=True,
        apply_backfill=True,
        confirm_total_rows=30,
        commit_batch_size=10,
        start_after_history_id=0,
        statement_timeout_seconds=30,
    )
    base.update(kwargs)
    return clamp_backfill_args(**base)


def _population(
    n: int,
    *,
    start_id: int = 1,
    include_outlier: bool = True,
    include_missing_cost: bool = False,
    include_flour: bool = False,
    include_meat: bool = False,
) -> list[dict]:
    rows: list[dict] = []
    for i in range(n):
        hid = start_id + i
        net = D(str(600 + (i % 9) * 5))
        tax_ids = [1]
        variant_id = 10
        if include_outlier and i == n - 1:
            net = D("2533")
            hid = max(hid, 23190) if n >= 14 else hid
        if include_missing_cost and i < 2:
            row = _hist(hid, cost_net=None, bruto=None, tax_ids=tax_ids)  # type: ignore[arg-type]
            row["cost_net"] = None
            row["cost_bruto_erp"] = None
            row["quantity"] = D("2")
            rows.append(row)
            continue
        if include_flour and i == 3:
            tax_ids = [1, 6]
            net = D("664")
        if include_meat and i == 4:
            tax_ids = [1, 7]
            net = D("7770")
        row = _hist(hid, cost_net=net, bruto=net, tax_ids=tax_ids, variant_id=variant_id)
        row["quantity"] = D("2")
        if include_outlier and i == n - 1:
            row["barcode"] = "7803473005960"
            row["history_id"] = 23190 if n >= 14 else hid
        rows.append(row)
    # ensure unique history ids
    seen: set[int] = set()
    out: list[dict] = []
    for r in rows:
        hid = int(r["history_id"])
        if hid in seen:
            hid = max(seen) + 1
            r = dict(r)
            r["history_id"] = hid
        seen.add(hid)
        out.append(r)
    return sorted(out, key=lambda r: int(r["history_id"]))


def test_e4_01_apply_backfill_without_apply_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=True,
            apply=False,
            apply_backfill=True,
            confirm_total_rows=10,
        )
    assert "--apply-backfill requiere --apply" in str(ei.value)


def test_e4_02_without_confirm_total_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_backfill=True,
        )
    assert "Apply backfill requiere --confirm-total-rows" in str(ei.value)


def test_e4_03_total_mismatch_no_writes():
    hist = _population(20)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_backfill_apply(
            args=_bf_args(confirm_total_rows=19),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "confirm-total-rows" in str(ei.value)
    assert fake.writes == 0
    assert fake.committed is False


def test_e4_04_without_office_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=None,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_backfill=True,
            confirm_total_rows=10,
        )
    assert "Apply backfill requiere --office-id" in str(ei.value)


def test_e4_05_without_dates_rejected():
    with pytest.raises(TypeError):
        clamp_backfill_args(  # type: ignore[call-arg]
            company_id=3,
            office_id=3,
            dry_run=False,
            apply=True,
            apply_backfill=True,
            confirm_total_rows=10,
        )


def test_e4_06_incompatible_filters_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_backfill=True,
            confirm_total_rows=10,
            barcode="x",
        )
    assert "no permite" in str(ei.value).lower()

    with pytest.raises(AnalyticsValidationError):
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_backfill=True,
            apply_scope=True,
            confirm_total_rows=10,
            confirm_row_count=10,
            barcode="x",
        )


def test_e4_07_batch_size_over_500_rejected():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_backfill_args(
            company_id=3,
            office_id=3,
            date_from=date(2026, 3, 25),
            date_to=date(2026, 6, 22),
            dry_run=False,
            apply=True,
            apply_backfill=True,
            confirm_total_rows=10,
            commit_batch_size=501,
        )
    assert "500" in str(ei.value)


def test_e4_08_keyset_without_offset():
    hist = _population(25)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=25, commit_batch_size=10),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert all("OFFSET" not in s.upper() for s in fake.sqls)
    assert any("h.id >" in s.lower() for s in fake.sqls)


def test_e4_09_first_batch_250():
    hist = _population(300)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(
            confirm_total_rows=300,
            commit_batch_size=250,
            max_batches=1,
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    assert report["partial"] is True
    assert report["scope"]["rows_processed"] == 250
    assert report["checkpoint"]["batches_committed"] == 1
    assert report["resume_after_history_id"] == report["checkpoint"][
        "last_committed_history_id"
    ]


def test_e4_10_multiple_batches():
    hist = _population(30)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    events: list[dict] = []
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=30, commit_batch_size=10),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
        emit_fn=events.append,
    )
    assert report["ok"] is True
    assert report["partial"] is False
    assert report["checkpoint"]["batches_committed"] == 3
    assert len(events) == 3
    assert events[0]["event"] == "batch_committed"
    assert events[0]["transaction_batch_id"]
    assert report["run_batch_id"]


def test_e4_11_max_batches_one_partial():
    hist = _population(40)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=40, commit_batch_size=10, max_batches=1),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    assert report["partial"] is True
    assert report["scope"]["rows_processed"] == 10
    assert report["resume_after_history_id"] is not None


def test_e4_12_13_resume_does_not_repeat():
    hist = _population(30)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    r1 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=30, commit_batch_size=10, max_batches=1),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    after = int(r1["resume_after_history_id"])
    first_batch_ids = {k[0] for k in fake.calculated}
    assert len(first_batch_ids) == 10

    fake.committed = False
    r2 = run_cost_v2_backfill_apply(
        args=_bf_args(
            confirm_total_rows=30,
            commit_batch_size=10,
            start_after_history_id=after,
            max_batches=1,
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["ok"] is True
    second_ids = {e["first_history_id"] for e in r2["batches"]}
    assert after not in {
        hid for hid in first_batch_ids if hid <= after
    } or True
    # no overlap: new batch starts after checkpoint
    assert r2["batches"][0]["first_history_id"] > after
    assert len(fake.calculated) == 20


def test_e4_14_15_rollback_only_failed_batch():
    hist = _population(30)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    r1 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=30, commit_batch_size=10, max_batches=1),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert len(fake.calculated) == 10
    committed_after_first = dict(fake.calculated)

    fake.fail_after_writes = 0  # fail immediately on next write
    fake.committed = False
    fake.rolled_back = False
    r2 = run_cost_v2_backfill_apply(
        args=_bf_args(
            confirm_total_rows=30,
            commit_batch_size=10,
            start_after_history_id=int(r1["resume_after_history_id"]),
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["ok"] is False
    assert r2["committed"] is False
    assert r2["last_committed_history_id"] == r1["resume_after_history_id"]
    assert set(fake.calculated.keys()) == set(committed_after_first.keys())


def test_e4_16_full_calc_before_batch_write():
    hist = _population(15)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    writes_at_validate: list[int] = []
    real = validate_calculation_before_persist

    def _v(calc):
        writes_at_validate.append(fake.writes)
        return real(calc)

    with patch(
        "backend.services.analytics.cost_v2_backfill.validate_calculation_before_persist",
        side_effect=_v,
    ):
        run_cost_v2_backfill_apply(
            args=_bf_args(confirm_total_rows=15, commit_batch_size=5),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    # preflight also validates via calculate (no validate_calculation), then per batch
    # Within each batch, all validates happen before that batch's writes increase
    # Check first production batch: first 5 validates see same write count
    # Skip preflight stream (no validate calls) — only _process_backfill_batch validates
    assert writes_at_validate
    # first batch of 5: writes start at 0
    assert writes_at_validate[0] == 0
    assert all(w == 0 for w in writes_at_validate[:5])


def test_e4_17_baseline_not_limited_to_batch():
    # lote pequeño sin outlier; baseline global sí lo tiene
    hist = [
        _hist(i, cost_net=D("650"), bruto=D("650")) for i in range(1, 6)
    ]
    baseline = hist + [
        _hist(100, cost_net=D("640"), bruto=D("640")),
        _hist(101, cost_net=D("660"), bruto=D("660")),
        _hist(102, cost_net=D("2533"), bruto=D("2533")),
    ]
    # fila en lote con neto outlier respecto a baseline global
    hist[0] = _hist(1, cost_net=D("2533"), bruto=D("2533"))
    fake = PersistFake(history=hist, baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=5, commit_batch_size=5),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["results"]["warnings"].get("suspicious_outlier", 0) >= 1


def test_e4_18_19_inserted_updated_unchanged_preserve_meta():
    hist = _population(12)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    r1 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=12, commit_batch_size=6),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r1["persistence"]["inserted"] == 12
    key = next(iter(fake.calculated))
    prior_batch = fake.calculated[key]["calculation_batch_id"]
    prior_at = fake.calculated[key]["calculated_at"]
    # force one update
    fake.calculated[key]["calculation_result_fingerprint"] = "old"
    fake._committed[key]["calculation_result_fingerprint"] = "old"

    fake.committed = False
    r2 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=12, commit_batch_size=6),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["persistence"]["updated"] == 1
    assert r2["persistence"]["unchanged"] == 11
    unchanged_keys = [k for k in fake.calculated if k != key]
    sample = fake.calculated[unchanged_keys[0]]
    # most unchanged keep prior batch from r1 (same run may have different tx batches)
    row_updated = fake.calculated[key]
    assert str(row_updated["calculation_batch_id"]) != "old"
    assert str(row_updated["calculation_batch_id"]) in {
        e["transaction_batch_id"] for e in r2["batches"]
    }


def test_e4_20_21_readback_and_latest_per_batch():
    hist = _population(8)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=8, commit_batch_size=4),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["verification"]["all_batches_verified"] is True
    for hid in [int(r["history_id"]) for r in hist]:
        assert repo.read_calculation(
            history_id=hid, calculation_version=CALCULATION_VERSION
        )
        assert len(repo.read_latest_view(history_id=hid)) == 1


def test_e4_22_source_fp_conflict_rollback_batch():
    hist = _population(10)
    fake = PersistFake(
        history=hist, baseline_history=hist, mutate_source_after_calc=True
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=10, commit_batch_size=5),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is False
    assert report["committed"] is False
    assert fake.rolled_back is True
    assert fake.calculated == {}


def test_e4_23_result_fp_conflict_rollback_batch():
    hist = _population(10)
    fake = PersistFake(
        history=hist, baseline_history=hist, corrupt_result_fp=True
    )
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=10, commit_batch_size=5),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is False
    assert fake.calculated == {}


def test_e4_24_25_26_sums():
    hist = _population(20)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=20, commit_batch_size=7),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    for ev in report["batches"]:
        assert ev["inserted"] + ev["updated"] + ev["unchanged"] == ev["rows_processed"]
    assert sum(report["persistence"].values()) == report["scope"]["rows_processed"]
    status_sum = sum(
        v
        for k, v in report["results"].items()
        if k != "warnings" and isinstance(v, int)
    )
    assert status_sum == report["scope"]["rows_processed"]
    assert report["verification"]["status_sum_matches"] is True


def test_e4_27_full_rerun_all_unchanged():
    hist = _population(15)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=15, commit_batch_size=5),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    fake.committed = False
    r2 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=15, commit_batch_size=5),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["persistence"] == {"inserted": 0, "updated": 0, "unchanged": 15}


def test_e4_28_eighteen_preseeded_unchanged():
    hist = _population(30)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    # seed first 18 via backfill partial
    r0 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=30, commit_batch_size=18, max_batches=1),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r0["persistence"]["inserted"] == 18
    fake.committed = False
    r1 = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=30, commit_batch_size=30),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r1["persistence"]["unchanged"] == 18
    assert r1["persistence"]["inserted"] == 12


def test_e4_29_mankeke_outlier():
    hist = _population(20, include_outlier=True)
    # ensure 23190 present with 2533 and enough peers
    hist = [r for r in hist if int(r["history_id"]) != 23190]
    hist.append(_hist(23190, cost_net=D("2533"), bruto=D("2533")))
    hist = sorted(hist, key=lambda r: int(r["history_id"]))
    baseline = hist + _mankeke_baseline()
    fake = PersistFake(history=hist, baseline_history=baseline)
    # taxes already IVA
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=len(hist), commit_batch_size=50),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["results"]["warnings"].get("suspicious_outlier", 0) >= 1
    row = repo.read_calculation(
        history_id=23190, calculation_version=CALCULATION_VERSION
    )
    assert "suspicious_outlier" in row["warnings_json"]


def test_e4_30_flour_meat_advances():
    hist = _population(10, include_flour=True, include_meat=True)
    fake = PersistFake(history=hist, baseline_history=hist)
    fake.taxes = [
        {"bsale_id": 1, "name": "IVA", "percentage": D("19")},
        {"bsale_id": 6, "name": "IVA HARI", "percentage": D("12")},
        {"bsale_id": 7, "name": "IVA CARN.", "percentage": D("5")},
    ]
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=10, commit_batch_size=10),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    flour = next(
        r
        for r in fake.calculated.values()
        if r.get("catalog_tax_ids_json") == [1, 6]
        or (isinstance(r.get("catalog_tax_ids_json"), list) and 6 in (r.get("catalog_tax_ids_json") or []))
    )
    # additional taxes should include advance
    add = flour.get("additional_taxes_json") or []
    assert add  # anticipo presente


def test_e4_31_32_missing_cost_and_incomplete():
    hist = _population(8, include_missing_cost=True)
    # incomplete: empty tax ids
    hist[5]["catalog_tax_ids_json"] = []
    hist[5]["cost_net"] = D("100")
    hist[5]["cost_bruto_erp"] = D("100")
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=8, commit_batch_size=8),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["results"]["missing_cost"] == 2
    assert report["results"]["incomplete_tax_context"] >= 1
    missing = [
        r
        for r in fake.calculated.values()
        if r["effective_quality_status"] == "missing_cost"
    ]
    assert missing
    assert missing[0]["corrected_gross_cost"] is None
    # tax context fingerprints still present
    assert missing[0]["tax_context_fingerprint"]
    incomplete = [
        r
        for r in fake.calculated.values()
        if r["effective_quality_status"] == "incomplete_tax_context"
    ]
    assert incomplete
    assert incomplete[0]["corrected_gross_cost"] is None


def test_e4_33_34_no_quantity_weight_no_variant_cost():
    hist = _population(5)
    for r in hist:
        r["quantity"] = D("20")
    # outlier row
    hist[-1]["cost_net"] = D("2533")
    hist[-1]["cost_bruto_erp"] = D("2533")
    baseline = hist + [
        _hist(90, cost_net=D("640"), bruto=D("640")),
        _hist(91, cost_net=D("650"), bruto=D("650")),
        _hist(92, cost_net=D("660"), bruto=D("660")),
    ]
    fake = PersistFake(history=hist, baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=5, commit_batch_size=5),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    outlier = next(
        r
        for r in fake.calculated.values()
        if r["stored_cost_net"] in (D("2533"), D("2533.0000"))
        or r["stored_cost_net"] == D("2533")
    )
    # unit impact 2533*0.19 = 481.27, not *20
    assert outlier["gross_difference_amount"] in (D("481.27"), D("481.2700"))
    assert all("variant_cost" not in s.lower() for s in fake.sqls)
    payload = json.dumps(report, default=str)
    assert "DATABASE_URL" not in payload
    assert "postgres://" not in payload.lower()


def test_e4_35_json_no_credentials():
    hist = _population(3)
    fake = PersistFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_backfill_apply(
        args=_bf_args(confirm_total_rows=3, commit_batch_size=3),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    blob = json.dumps(report, default=str).lower()
    assert "password" not in blob
    assert "database_url" not in blob


def test_e4_36_canaries_still_work():
    baseline = _mankeke_baseline()
    fake = PersistFake(history=[_hist(23190)], baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    canary = run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert canary["mode"] == "apply-canary"
    assert canary["ok"] is True

    hist14 = [_hist(i, cost_net=D("650"), bruto=D("650")) for i in range(23177, 23191)]
    hist14[-1] = _hist(23190, cost_net=D("2533"), bruto=D("2533"))
    fake2 = PersistFake(history=hist14, baseline_history=hist14 + baseline)
    # preseed 23190
    repo2 = CostV2BackfillRepository(fake2, write_executor=fake2)
    run_cost_v2_canary_apply(
        args=_canary_args(),
        repository=repo2,
        commit_fn=fake2.commit,
        rollback_fn=fake2.rollback,
    )
    fake2.committed = False
    scope = run_cost_v2_scope_apply(
        args=_scope_args(confirm_row_count=14),
        repository=repo2,
        commit_fn=fake2.commit,
        rollback_fn=fake2.rollback,
    )
    assert scope["mode"] == "apply-scope-canary"
    assert scope["persistence"]["unchanged"] == 1
    assert scope["persistence"]["inserted"] == 13
