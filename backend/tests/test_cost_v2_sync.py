"""Tests sync incremental/catchup Costos V2 (sin PostgreSQL / sin prod)."""

from __future__ import annotations

import copy
import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest

from backend.repositories.cost_v2_backfill_repo import CostV2BackfillRepository
from backend.services.analytics.cost_v2_backfill import (
    _calculate_single_row,
    build_variant_net_outlier_stats,
)
from backend.services.analytics.cost_v2_calculator import CALCULATION_VERSION
from backend.services.analytics.cost_v2_sync import (
    clamp_sync_args,
    discover_candidates,
    run_cost_v2_sync,
)
from backend.services.analytics.money import D
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
)
from backend.tests.test_cost_v2_persistence import PersistFake, _hist, _mankeke_baseline


def _sync_args(**kwargs):
    base = dict(
        mode="catchup",
        company_id=3,
        office_id=3,
        dry_run=True,
        apply=False,
        date_from=date(2026, 6, 23),
        date_to=date(2026, 7, 31),
        commit_batch_size=10,
        statement_timeout_seconds=30,
    )
    base.update(kwargs)
    return clamp_sync_args(**base)


class SyncFake(PersistFake):
    """Extiende PersistFake con LEFT/INNER JOIN para missing/window."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # taxes con anticipos para harina/carne
        self.taxes = [
            {"bsale_id": 1, "name": "IVA", "percentage": D("19")},
            {"bsale_id": 6, "name": "IVA HARI", "percentage": D("12")},
            {"bsale_id": 7, "name": "IVA CARN.", "percentage": D("5")},
        ]

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        upper = sql.upper()
        if any(x in upper for x in ("INSERT ", "UPDATE ", "DELETE ", "DROP ", "ALTER ")):
            if "INSERT INTO ANALYTICS.COST_RECEPTION_CALCULATED" in upper or (
                "insert into analytics.cost_reception_calculated" in sql.lower()
            ):
                return PersistFake.__call__(self, sql, params)
            self.write_attempts = getattr(self, "write_attempts", 0) + 1
            raise AssertionError(f"escritura no permitida: {sql[:80]}")

        # Audit
        if "rows_after_cutoff" in sql.lower() or "missing_calculation" in sql.lower():
            self.sqls.append(sql)
            cutoff = params[0] if params else date(2026, 6, 23)
            version = params[1] if len(params) > 1 else CALCULATION_VERSION
            rows = [
                r
                for r in self.history
                if int(r["company_id"]) == 3 and int(r["office_id"]) == 3
            ]
            with_c = 0
            missing = 0
            after = 0
            for r in rows:
                key = (int(r["history_id"]), str(version))
                if key in self.calculated:
                    with_c += 1
                else:
                    missing += 1
                adm = r["admission_date"]
                if isinstance(adm, datetime):
                    adm = adm.date()
                if adm >= cutoff:
                    after += 1
            return [
                {
                    "total_rows": len(rows),
                    "rows_after_cutoff": after,
                    "min_admission_date": min((r["admission_date"] for r in rows), default=None),
                    "max_admission_date": max((r["admission_date"] for r in rows), default=None),
                    "min_history_id": min((int(r["history_id"]) for r in rows), default=None),
                    "max_history_id": max((int(r["history_id"]) for r in rows), default=None),
                    "missing_calculation": missing,
                    "with_calculation": with_c,
                }
            ]

        # Missing batch: LEFT JOIN ... c.history_id IS NULL
        if "c.history_id IS NULL" in sql or "c.history_id is null" in sql.lower():
            self.sqls.append(sql)
            assert "OFFSET" not in upper
            version = str(params[0])
            after_id = int(params[3])
            limit = int(params[-1])
            date_from = date_to_excl = None
            if len(params) >= 6 and isinstance(params[4], date):
                date_from = params[4]
                date_to_excl = params[5]
            out = []
            for r in sorted(self.history, key=lambda x: int(x["history_id"])):
                hid = int(r["history_id"])
                if hid <= after_id:
                    continue
                if (hid, version) in self.calculated:
                    continue
                adm = r["admission_date"]
                if isinstance(adm, datetime):
                    adm = adm.date()
                if date_from and adm < date_from:
                    continue
                if date_to_excl and adm >= date_to_excl:
                    continue
                mapped = dict(r)
                mapped["catalog_tax_ids"] = list(r.get("catalog_tax_ids_json") or [])
                out.append(mapped)
                if len(out) >= limit:
                    break
            return out

        # Window with calculation INNER JOIN
        if "INNER JOIN analytics.cost_reception_calculated" in sql.lower() or (
            "INNER JOIN ANALYTICS.COST_RECEPTION_CALCULATED" in upper
        ):
            self.sqls.append(sql)
            assert "OFFSET" not in upper
            version = str(params[0])
            date_from = params[3]
            date_to_excl = params[4]
            after_id = int(params[5])
            limit = int(params[6])
            out = []
            for r in sorted(self.history, key=lambda x: int(x["history_id"])):
                hid = int(r["history_id"])
                if hid <= after_id:
                    continue
                if (hid, version) not in self.calculated:
                    continue
                adm = r["admission_date"]
                if isinstance(adm, datetime):
                    adm = adm.date()
                if adm < date_from or adm >= date_to_excl:
                    continue
                mapped = dict(r)
                mapped["catalog_tax_ids"] = list(r.get("catalog_tax_ids_json") or [])
                out.append(mapped)
                if len(out) >= limit:
                    break
            return out

        # fetch by ids ANY
        if "h.id = ANY(%s)" in sql or "H.ID = ANY(%s)" in upper:
            self.sqls.append(sql)
            ids = set(params[2]) if len(params) >= 3 and isinstance(params[2], list) else set()
            out = []
            for r in sorted(self.history, key=lambda x: int(x["history_id"])):
                if int(r["history_id"]) in ids:
                    mapped = dict(r)
                    mapped["catalog_tax_ids"] = list(r.get("catalog_tax_ids_json") or [])
                    out.append(mapped)
            return out

        return PersistFake.__call__(self, sql, params)


def _seed_calc(fake: SyncFake, history_row: dict, *, batch: str | None = None) -> None:
    """Persiste un cálculo real vía motor para fingerprints coherentes."""
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    tax_catalog = repo.fetch_taxes_for_ids(
        company_id=3, tax_ids=history_row.get("catalog_tax_ids_json") or [1]
    )
    # Normalizar catalog_tax_ids como hace el repo
    row = dict(history_row)
    row["catalog_tax_ids"] = list(row.get("catalog_tax_ids_json") or [1])
    outlier = build_variant_net_outlier_stats(
        repo.fetch_outlier_baseline_cost_nets(
            company_id=3, office_id=3, variant_ids=[int(row["variant_id"])]
        )
    )
    calc = _calculate_single_row(
        row=row,
        tax_catalog=tax_catalog,
        outlier_stats=outlier,
        calculation_version=CALCULATION_VERSION,
    )
    bid = batch or str(uuid4())
    repo.persist_calculation(calc=calc, calculation_batch_id=bid)
    fake.commit()


def test_01_dry_run_no_writes():
    hist = [_hist(i, admission_date=date(2026, 7, 1)) for i in range(1, 6)]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_sync(args=_sync_args(), repository=repo)
    assert report["ok"] is True
    assert report["dry_run"] is True
    assert fake.writes == 0
    assert all("OFFSET" not in s.upper() for s in fake.sqls)


def test_02_catchup_requires_dates():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_sync_args(
            mode="catchup",
            company_id=3,
            office_id=3,
            dry_run=True,
        )
    assert "date-from" in str(ei.value)


def test_03_catchup_apply_requires_confirm():
    with pytest.raises(AnalyticsValidationError) as ei:
        clamp_sync_args(
            mode="catchup",
            company_id=3,
            office_id=3,
            dry_run=False,
            apply=True,
            date_from=date(2026, 6, 23),
            date_to=date(2026, 7, 31),
        )
    assert "confirm-candidate-count" in str(ei.value)


def test_04_wrong_confirm_no_writes():
    hist = [_hist(i, admission_date=date(2026, 7, 1)) for i in range(1, 6)]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    with pytest.raises(AnalyticsValidationError) as ei:
        run_cost_v2_sync(
            args=_sync_args(
                dry_run=False,
                apply=True,
                confirm_candidate_count=99,
            ),
            repository=repo,
            commit_fn=fake.commit,
            rollback_fn=fake.rollback,
        )
    assert "confirm-candidate-count" in str(ei.value)
    assert fake.writes == 0


def test_05_incremental_detects_new():
    old = [_hist(i, admission_date=date(2026, 5, 1)) for i in range(1, 4)]
    new = [_hist(i, admission_date=date(2026, 7, 10)) for i in range(100, 103)]
    hist = old + new
    fake = SyncFake(history=hist, baseline_history=hist)
    for r in old:
        _seed_calc(fake, r)
    fake.committed = False
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    args = _sync_args(
        mode="incremental",
        date_from=None,
        date_to=None,
        lookback_days=45,
        max_candidates=100,
    )
    d = discover_candidates(args=args, repository=repo)
    assert d["new"] >= 3
    assert set(d["candidate_ids"]) >= {100, 101, 102}


def test_06_07_fingerprint_change_detection():
    hist = [_hist(10, admission_date=date.today() - timedelta(days=5), cost_net=D("650"), bruto=D("650"))]
    fake = SyncFake(history=hist, baseline_history=hist)
    _seed_calc(fake, hist[0], batch="old-batch")
    # Mutar source en history → source fingerprint change
    hist[0]["cost_net"] = D("700")
    hist[0]["cost_bruto_erp"] = D("700")
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    args = _sync_args(
        mode="incremental",
        date_from=None,
        date_to=None,
        lookback_days=45,
    )
    d = discover_candidates(args=args, repository=repo)
    assert d["changed"] >= 1
    assert 10 in d["candidate_ids"]

    # tax fingerprint: cambiar catalog tax ids
    hist2 = [
        _hist(11, admission_date=date.today() - timedelta(days=3), cost_net=D("664"), bruto=D("664"), tax_ids=[1])
    ]
    fake2 = SyncFake(history=hist2, baseline_history=hist2)
    _seed_calc(fake2, hist2[0])
    hist2[0]["catalog_tax_ids_json"] = [1, 6]
    repo2 = CostV2BackfillRepository(fake2, write_executor=fake2)
    d2 = discover_candidates(args=args, repository=repo2)
    assert d2["changed"] >= 1


def test_08_09_10_11_unchanged_inserted_updated_meta():
    hist = [_hist(i, admission_date=date(2026, 7, 1), cost_net=D("650"), bruto=D("650")) for i in range(1, 6)]
    fake = SyncFake(history=hist, baseline_history=hist)
    _seed_calc(fake, hist[0], batch="keep-me")
    prior = fake.calculated[(1, CALCULATION_VERSION)]
    prior_batch = prior["calculation_batch_id"]
    prior_at = prior["calculated_at"]

    repo = CostV2BackfillRepository(fake, write_executor=fake)
    # dry-run discover
    args_dry = _sync_args(confirm_candidate_count=None)
    d = discover_candidates(args=args_dry, repository=repo)
    n = d["total"]
    fake.committed = False
    report = run_cost_v2_sync(
        args=_sync_args(dry_run=False, apply=True, confirm_candidate_count=n, commit_batch_size=10),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    assert report["persistence"]["inserted"] >= 4
    # row 1 unchanged if still matching
    still = fake.calculated[(1, CALCULATION_VERSION)]
    assert str(still["calculation_batch_id"]) == prior_batch
    assert still["calculated_at"] == prior_at


def test_12_13_keyset_and_batch_cap():
    with pytest.raises(AnalyticsValidationError):
        clamp_sync_args(
            mode="incremental",
            company_id=3,
            office_id=3,
            commit_batch_size=501,
        )
    hist = [_hist(i, admission_date=date(2026, 7, 1)) for i in range(1, 20)]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    run_cost_v2_sync(args=_sync_args(), repository=repo)
    assert all("OFFSET" not in s.upper() for s in fake.sqls)


def test_14_15_rollback_keeps_prior_batches():
    hist = [_hist(i, admission_date=date(2026, 7, 1)) for i in range(1, 16)]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    d = discover_candidates(args=_sync_args(), repository=repo)
    n = d["total"]
    # primer lote ok
    r1 = run_cost_v2_sync(
        args=_sync_args(
            dry_run=False,
            apply=True,
            confirm_candidate_count=n,
            commit_batch_size=5,
            max_batches=1,
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r1["ok"] is True
    assert r1["partial"] is True
    kept = dict(fake.calculated)
    # segundo lote falla
    fake.fail_after_writes = 0
    fake.committed = False
    r2 = run_cost_v2_sync(
        args=_sync_args(
            dry_run=False,
            apply=True,
            confirm_candidate_count=n,
            commit_batch_size=5,
            start_after_history_id=int(r1["resume_after_history_id"]),
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["ok"] is False
    assert set(fake.calculated.keys()) == set(kept.keys())


def test_16_resume_by_history_id():
    hist = [_hist(i, admission_date=date(2026, 7, 1)) for i in range(1, 21)]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    n = discover_candidates(args=_sync_args(), repository=repo)["total"]
    r1 = run_cost_v2_sync(
        args=_sync_args(
            dry_run=False,
            apply=True,
            confirm_candidate_count=n,
            commit_batch_size=7,
            max_batches=1,
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    after = int(r1["resume_after_history_id"])
    fake.committed = False
    r2 = run_cost_v2_sync(
        args=_sync_args(
            dry_run=False,
            apply=True,
            confirm_candidate_count=n,
            commit_batch_size=7,
            start_after_history_id=after,
            max_batches=1,
        ),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert r2["batches"][0]["first_history_id"] > after


def test_17_no_candidates_ok():
    hist = [_hist(1, admission_date=date(2026, 7, 1))]
    fake = SyncFake(history=hist, baseline_history=hist)
    _seed_calc(fake, hist[0])
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    # catchup window without rows
    report = run_cost_v2_sync(
        args=_sync_args(date_from=date(2026, 8, 1), date_to=date(2026, 8, 10)),
        repository=repo,
    )
    assert report["ok"] is True
    assert report["candidates"]["total"] == 0
    assert report["committed"] is True


def test_18_19_lookback_limits_changes_new_outside_ok():
    today = date.today()
    old_change = _hist(50, admission_date=today - timedelta(days=120), cost_net=D("600"), bruto=D("600"))
    new_far = _hist(60, admission_date=today - timedelta(days=200), cost_net=D("600"), bruto=D("600"))
    recent = _hist(70, admission_date=today - timedelta(days=3), cost_net=D("600"), bruto=D("600"))
    hist = [old_change, new_far, recent]
    fake = SyncFake(history=hist, baseline_history=hist)
    _seed_calc(fake, old_change)
    _seed_calc(fake, recent)
    # mutate old outside lookback
    old_change["cost_net"] = D("999")
    old_change["cost_bruto_erp"] = D("999")
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    args = _sync_args(
        mode="incremental",
        date_from=None,
        date_to=None,
        lookback_days=45,
        max_candidates=50,
    )
    d = discover_candidates(args=args, repository=repo)
    assert 60 in d["candidate_ids"]  # new outside lookback
    assert 50 not in d["candidate_ids"]  # changed outside lookback ignored


def test_20_21_22_no_forbidden():
    hist = [_hist(1, admission_date=date(2026, 7, 1))]
    hist[0]["quantity"] = D("20")
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    run_cost_v2_sync(args=_sync_args(), repository=repo)
    joined = " ".join(fake.sqls).lower()
    assert "variant_cost" not in joined
    assert "products.taxes" not in joined
    assert "offset" not in joined


def test_23_baseline_global_not_batch_only():
    hist = [_hist(1, admission_date=date(2026, 7, 1), cost_net=D("2533"), bruto=D("2533"))]
    baseline = hist + _mankeke_baseline()
    fake = SyncFake(history=hist, baseline_history=baseline)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    n = discover_candidates(args=_sync_args(), repository=repo)["total"]
    report = run_cost_v2_sync(
        args=_sync_args(dry_run=False, apply=True, confirm_candidate_count=n),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert report["ok"] is True
    row = fake.calculated[(1, CALCULATION_VERSION)]
    assert "suspicious_outlier" in (row.get("warnings_json") or [])


def test_24_25_missing_incomplete_null():
    hist = [
        _hist(1, admission_date=date(2026, 7, 1)),
        _hist(2, admission_date=date(2026, 7, 1), tax_ids=[]),
    ]
    hist[0]["cost_net"] = None
    hist[0]["cost_bruto_erp"] = None
    hist[0]["catalog_tax_ids_json"] = [1]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    n = discover_candidates(args=_sync_args(), repository=repo)["total"]
    run_cost_v2_sync(
        args=_sync_args(dry_run=False, apply=True, confirm_candidate_count=n),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    assert fake.calculated[(1, CALCULATION_VERSION)]["corrected_gross_cost"] is None
    assert fake.calculated[(2, CALCULATION_VERSION)]["corrected_gross_cost"] is None


def test_26_27_flour_meat_advances():
    flour = _hist(15978, admission_date=date(2026, 7, 1), cost_net=D("664"), bruto=D("664"), tax_ids=[1, 6])
    meat = _hist(19076, admission_date=date(2026, 7, 1), cost_net=D("7770"), bruto=D("7770"), tax_ids=[1, 7])
    hist = [flour, meat]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    n = discover_candidates(args=_sync_args(), repository=repo)["total"]
    run_cost_v2_sync(
        args=_sync_args(dry_run=False, apply=True, confirm_candidate_count=n),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    f = fake.calculated[(15978, CALCULATION_VERSION)]
    m = fake.calculated[(19076, CALCULATION_VERSION)]
    assert f["iva_tax_id"] == 1
    assert any(t.get("tax_id") == 6 for t in (f.get("additional_taxes_json") or []))
    assert m["iva_tax_id"] == 1
    assert any(t.get("tax_id") == 7 for t in (m.get("additional_taxes_json") or []))


def test_28_mankeke_warning():
    hist = [_hist(23190, admission_date=date(2026, 7, 1), cost_net=D("2533"), bruto=D("2533"))]
    fake = SyncFake(history=hist, baseline_history=hist + _mankeke_baseline())
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    n = discover_candidates(args=_sync_args(), repository=repo)["total"]
    run_cost_v2_sync(
        args=_sync_args(dry_run=False, apply=True, confirm_candidate_count=n),
        repository=repo,
        commit_fn=fake.commit,
        rollback_fn=fake.rollback,
    )
    w = fake.calculated[(23190, CALCULATION_VERSION)].get("warnings_json") or []
    assert "suspicious_outlier" in w


def test_29_json_no_secrets():
    hist = [_hist(1, admission_date=date(2026, 7, 1))]
    fake = SyncFake(history=hist, baseline_history=hist)
    repo = CostV2BackfillRepository(fake, write_executor=fake)
    report = run_cost_v2_sync(args=_sync_args(), repository=repo)
    blob = json.dumps(report, default=str).lower()
    assert "database_url" not in blob
    assert "password" not in blob
    assert "postgres://" not in blob


def test_30_office_required_and_unknown_mode():
    with pytest.raises(AnalyticsValidationError):
        clamp_sync_args(mode="incremental", company_id=3, office_id=None)
    with pytest.raises(AnalyticsValidationError):
        clamp_sync_args(mode="full", company_id=3, office_id=3)
    with pytest.raises(AnalyticsValidationError):
        clamp_sync_args(
            mode="incremental",
            company_id=3,
            office_id=3,
            max_candidates=0,
        )
