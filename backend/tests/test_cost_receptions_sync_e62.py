"""Tests E.6.2: diagnóstico + sync history-only / piloto controlado."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from backend.scripts import diagnose_cost_receptions_sync as diag
from backend.services.sync_cost_receptions import (
    CostReceptionSyncError,
    _sync_company_receptions,
    date_from_to_timestamps,
    resolve_write_mode,
    sync_cost_receptions,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeCur:
    def __init__(self, existing_detail_ids: set[int] | None = None):
        self.existing = set(existing_detail_ids or [])
        self.executed: list[tuple[str, tuple | None]] = []
        self.inserted_keys: list[str] = []
        self.savepoints = 0

    def execute(self, sql: str, params=None):
        self.executed.append((sql, params))
        if "SAVEPOINT" in sql.upper():
            self.savepoints += 1

    @property
    def connection(self):
        return MagicMock()


class FakeClient:
    def __init__(
        self,
        receptions: list[dict] | None = None,
        details_by_rec: dict[int, list[dict]] | None = None,
        costs_calls: list[int] | None = None,
        fail_details_for: set[int] | None = None,
    ):
        self.receptions = list(receptions or [])
        self.details_by_rec = dict(details_by_rec or {})
        self.costs_calls = costs_calls if costs_calls is not None else []
        self.fail_details_for = set(fail_details_for or [])
        self.paths: list[str] = []

    def get(self, path: str, params: dict | None = None):
        self.paths.append(path)
        params = params or {}
        if path == "/stocks/receptions.json":
            # Day filter or offset: return all once (simple)
            offset = int(params.get("offset") or 0)
            limit = int(params.get("limit") or 50)
            day = params.get("admissiondate")
            items = self.receptions
            if day is not None:
                day_i = int(day)
                items = [
                    r
                    for r in self.receptions
                    if datetime.fromtimestamp(
                        int(r["admissionDate"]), tz=timezone.utc
                    ).date()
                    == datetime.fromtimestamp(day_i, tz=timezone.utc).date()
                ]
            page = items[offset : offset + limit]
            return {"items": page, "count": len(items)}
        if path.endswith("/details.json"):
            rid = int(path.split("/")[3])
            if rid in self.fail_details_for:
                raise RuntimeError(f"details boom reception={rid}")
            offset = int(params.get("offset") or 0)
            limit = int(params.get("limit") or 50)
            items = self.details_by_rec.get(rid, [])
            return {"items": items[offset : offset + limit], "count": len(items)}
        if path.endswith("/costs.json"):
            vid = int(path.split("/")[2])
            self.costs_calls.append(vid)
            return {"averageCost": 100.0}
        raise AssertionError(f"unexpected path {path}")


def _ts(y: int, m: int, d: int, h: int = 12) -> int:
    return int(datetime(y, m, d, h, tzinfo=timezone.utc).timestamp())


def _rec(rid: int, adm: int, office_id: int = 3) -> dict:
    return {
        "id": rid,
        "admissionDate": adm,
        "document": "FC",
        "documentNumber": 1,
        "office": {"id": office_id, "name": f"O{office_id}"},
    }


def _line(detail_id: int, variant_id: int = 10, cost: float = 5.0) -> dict:
    return {
        "id": detail_id,
        "cost": cost,
        "quantity": 2,
        "variant": {"id": variant_id},
    }


# ---------------------------------------------------------------------------
# Diagnóstico
# ---------------------------------------------------------------------------


def test_diagnose_no_undefined_limit_constant():
    import re

    src = open(diag.__file__, encoding="utf-8").read()
    # Bug E.6.2: bare LIMIT caused NameError in _dry_run_line_skips
    assert not re.search(r"\bLIMIT\b", src), "bare LIMIT must not appear"
    assert "LIST_LIMIT" in src
    assert "DETAIL_PAGE_LIMIT" in src
    assert '{"limit": LIMIT' not in src
    assert "offset += LIMIT" not in src


def test_detail_sample_limit_default_and_max():
    assert diag.clamp_detail_sample_limit(None) == 100
    assert diag.clamp_detail_sample_limit(1) == 1
    assert diag.clamp_detail_sample_limit(500) == 500
    with pytest.raises(ValueError):
        diag.clamp_detail_sample_limit(0)
    with pytest.raises(ValueError):
        diag.clamp_detail_sample_limit(501)


def test_scan_all_details_and_sample_do_not_write_or_call_costs():
    cur = FakeCur(existing_detail_ids={101})
    costs: list[int] = []
    client = FakeClient(
        receptions=[
            _rec(1, _ts(2026, 6, 23)),
            _rec(2, _ts(2026, 6, 24)),
        ],
        details_by_rec={
            1: [_line(101), _line(102)],
            2: [_line(201)],
        },
        costs_calls=costs,
    )

    with patch.object(diag.repo, "line_exists", side_effect=lambda c, cid, did: did in cur.existing):
        sample = diag.build_detail_report(
            cur,
            client,
            company_id=3,
            receptions=client.receptions,
            detail_sample_limit=1,
            scan_all_details=False,
            max_receptions=5000,
        )
        full = diag.build_detail_report(
            cur,
            client,
            company_id=3,
            receptions=client.receptions,
            detail_sample_limit=1,
            scan_all_details=True,
            max_receptions=5000,
        )

    assert costs == []
    assert not any("/costs.json" in p for p in client.paths)
    assert not any("INSERT" in s[0].upper() for s in cur.executed)
    assert sample["receptions_fetched_total"] == 2
    assert sample["receptions_sampled_for_details"] == 1
    assert sample["details_would_insert_sample"] == 1  # only 102
    assert sample["details_already_in_history_sample"] == 1
    assert sample["extrapolation_warning"]
    assert full["receptions_sampled_for_details"] == 2
    assert full["would_insert"] == 2  # 102 + 201
    assert full["already_in_history"] == 1
    assert full["extrapolation_warning"] is None


def test_diagnose_exact_would_insert_and_collisions():
    cur = FakeCur()
    client = FakeClient(
        receptions=[_rec(1, _ts(2026, 7, 1))],
        details_by_rec={
            1: [_line(1), _line(1), _line(2)],  # duplicate detail id 1
        },
    )
    with patch.object(diag.repo, "line_exists", return_value=False):
        report = diag.analyze_details_for_receptions(
            cur, client, company_id=3, receptions=client.receptions, progress_every=0
        )
    assert report["would_insert"] == 3  # classified per occurrence
    assert report["duplicate_detail_ids_in_api_sample"]
    assert report["potential_company_detail_collisions"]
    assert report["potential_company_detail_collisions"][0]["reception_detail_id"] == 1


def test_diagnose_detects_already_existing():
    cur = FakeCur(existing_detail_ids={5})
    client = FakeClient(
        receptions=[_rec(9, _ts(2026, 7, 2))],
        details_by_rec={9: [_line(5), _line(6)]},
    )
    with patch.object(diag.repo, "line_exists", side_effect=lambda c, cid, did: did in {5}):
        report = diag.classify_detail_lines(
            cur, company_id=3, detail_items=client.details_by_rec[9]
        )
    assert report["already_in_history"] == 1
    assert report["would_insert"] == 1


# ---------------------------------------------------------------------------
# history-only + fechas
# ---------------------------------------------------------------------------


def test_date_range_inclusive():
    since, until = date_from_to_timestamps(date(2026, 6, 23), date(2026, 7, 31))
    assert datetime.fromtimestamp(since, tz=timezone.utc).date() == date(2026, 6, 23)
    assert datetime.fromtimestamp(until, tz=timezone.utc).date() == date(2026, 7, 31)
    # inclusive end of day
    assert until >= _ts(2026, 7, 31, 23)


def test_resolve_write_mode_dates_default_dry_run():
    assert resolve_write_mode(
        date_from=date(2026, 6, 23),
        date_to=date(2026, 7, 31),
        dry_run=False,
        apply=False,
    ) is False
    assert resolve_write_mode(
        date_from=date(2026, 6, 23),
        date_to=date(2026, 7, 31),
        dry_run=False,
        apply=True,
    ) is True
    with pytest.raises(CostReceptionSyncError):
        resolve_write_mode(
            date_from=date(2026, 6, 23),
            date_to=None,
            dry_run=False,
            apply=False,
        )


def test_history_only_no_costs_no_variant_cost_but_inserts_history():
    cur = FakeCur()
    costs: list[int] = []
    client = FakeClient(
        receptions=[_rec(1, _ts(2026, 6, 23))],
        details_by_rec={1: [_line(1001)]},
        costs_calls=costs,
    )
    inserts: list[dict] = []

    def _insert(**kwargs):
        inserts.append(kwargs)
        return True

    with (
        patch(
            "backend.services.sync_cost_receptions.repo.line_exists",
            return_value=False,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.variant_tax_context",
            return_value={
                "tax_context_available": False,
                "product_id": 1,
                "barcode": "x",
                "product_name": "p",
                "variant_name": "v",
            },
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.previous_cost_for_variant",
            return_value=None,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.insert_history_line",
            side_effect=lambda *a, **k: _insert(**k),
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.upsert_variant_cost_snapshot"
        ) as upsert_vc,
        patch("backend.services.sync_cost_receptions.time.sleep"),
    ):
        rec_n, line_n, max_ts, stats = _sync_company_receptions(
            cur,
            company_id=3,
            company_name="SPA",
            client=client,
            since_ts=_ts(2026, 6, 23, 0),
            until_ts=_ts(2026, 6, 23, 23),
            history_only=True,
            dry_run=False,
        )

    assert costs == []
    assert not any(p.endswith("/costs.json") for p in client.paths)
    upsert_vc.assert_not_called()
    assert line_n == 1
    assert rec_n == 1
    assert inserts and inserts[0]["reception_detail_id"] == 1001
    assert stats["history_only"] is True


def test_legacy_without_history_only_calls_costs_and_variant_cost():
    cur = FakeCur()
    costs: list[int] = []
    client = FakeClient(
        receptions=[_rec(1, _ts(2026, 6, 23))],
        details_by_rec={1: [_line(2002)]},
        costs_calls=costs,
    )
    with (
        patch(
            "backend.services.sync_cost_receptions.repo.line_exists",
            return_value=False,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.variant_tax_context",
            return_value={"tax_context_available": False},
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.previous_cost_for_variant",
            return_value=None,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.insert_history_line",
            return_value=True,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.upsert_variant_cost_snapshot"
        ) as upsert_vc,
        patch("backend.services.sync_cost_receptions.time.sleep"),
    ):
        _sync_company_receptions(
            cur,
            company_id=3,
            company_name="SPA",
            client=client,
            since_ts=_ts(2026, 6, 23, 0),
            until_ts=_ts(2026, 6, 23, 23),
            history_only=False,
            dry_run=False,
        )
    assert costs == [10]
    upsert_vc.assert_called_once()


def test_max_receptions_limits_pilot_and_watermark_partial():
    cur = FakeCur()
    t1, t2, t3 = _ts(2026, 6, 23), _ts(2026, 6, 24), _ts(2026, 6, 25)
    client = FakeClient(
        receptions=[_rec(1, t1), _rec(2, t2), _rec(3, t3)],
        details_by_rec={
            1: [_line(11)],
            2: [_line(22)],
            3: [_line(33)],
        },
    )
    with (
        patch(
            "backend.services.sync_cost_receptions.repo.line_exists",
            return_value=False,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.variant_tax_context",
            return_value={"tax_context_available": False},
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.previous_cost_for_variant",
            return_value=None,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.insert_history_line",
            return_value=True,
        ),
        patch("backend.services.sync_cost_receptions.time.sleep"),
    ):
        _rec_n, line_n, _max_ts, stats = _sync_company_receptions(
            cur,
            company_id=3,
            company_name="SPA",
            client=client,
            since_ts=t1 - 10,
            until_ts=t3 + 10,
            history_only=True,
            max_receptions=2,
            dry_run=False,
        )
    assert stats["receptions_processed"] == 2
    assert line_n == 2
    assert stats["last_processed_admission_ts"] == t2


def test_on_conflict_idempotency_already_existing():
    cur = FakeCur(existing_detail_ids={77})
    client = FakeClient(
        receptions=[_rec(1, _ts(2026, 6, 23))],
        details_by_rec={1: [_line(77), _line(78)]},
    )
    with (
        patch(
            "backend.services.sync_cost_receptions.repo.line_exists",
            side_effect=lambda c, cid, did: did == 77,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.variant_tax_context",
            return_value={"tax_context_available": False},
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.previous_cost_for_variant",
            return_value=None,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.insert_history_line",
            return_value=True,
        ) as ins,
        patch("backend.services.sync_cost_receptions.time.sleep"),
    ):
        _r, line_n, _m, stats = _sync_company_receptions(
            cur,
            company_id=3,
            company_name="SPA",
            client=client,
            since_ts=_ts(2026, 6, 23, 0),
            until_ts=_ts(2026, 6, 23, 23),
            history_only=True,
            dry_run=False,
        )
    assert stats["already_existing"] == 1
    assert line_n == 1
    assert ins.call_count == 1


def test_errors_are_reported():
    cur = FakeCur()
    client = FakeClient(
        receptions=[_rec(1, _ts(2026, 6, 23))],
        details_by_rec={},
        fail_details_for={1},
    )
    with patch("backend.services.sync_cost_receptions.time.sleep"):
        _r, line_n, _m, stats = _sync_company_receptions(
            cur,
            company_id=3,
            company_name="SPA",
            client=client,
            since_ts=_ts(2026, 6, 23, 0),
            until_ts=_ts(2026, 6, 23, 23),
            history_only=True,
            dry_run=False,
        )
    assert line_n == 0
    assert stats["failures"]
    assert stats["failures"][0]["reception_id"] == 1
    assert stats["failures"][0]["stage"] == "fetch_details"


def test_confirm_mismatch_does_not_write():
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    state = {"last_admission_ts": _ts(2026, 6, 22)}
    client = FakeClient(
        receptions=[
            _rec(1, _ts(2026, 6, 23)),
            _rec(2, _ts(2026, 6, 24)),
        ],
        details_by_rec={1: [_line(1)], 2: [_line(2)]},
    )

    with (
        patch(
            "backend.services.sync_cost_receptions.get_connection",
            return_value=fake_conn,
        ),
        patch(
            "backend.services.sync_cost_receptions._load_companies",
            return_value=[{"company_id": 3, "name": "SPA", "token": "t"}],
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.get_sync_state",
            return_value=state,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.reset_product_column_cache"
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.log_bsale_products_schema"
        ),
        patch(
            "backend.services.sync_cost_receptions.BsaleClient",
            return_value=client,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.upsert_sync_state"
        ) as upsert_state,
        patch(
            "backend.services.sync_cost_receptions._sync_company_receptions"
        ) as sync_inner,
    ):
        result = sync_cost_receptions(
            company_id=3,
            date_from=date(2026, 6, 23),
            date_to=date(2026, 6, 24),
            confirm_reception_count=99,
            history_only=True,
            apply=True,
        )

    assert result["ok"] is False
    assert result["error_type"] == "confirm_mismatch"
    sync_inner.assert_not_called()
    upsert_state.assert_not_called()


def test_history_only_apply_updates_sync_state_and_partial_watermark():
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    prev_wm = _ts(2026, 6, 22)
    state = {"last_admission_ts": prev_wm}
    t1, t2 = _ts(2026, 6, 23), _ts(2026, 6, 24)
    client = FakeClient(
        receptions=[_rec(1, t1), _rec(2, t2)],
        details_by_rec={1: [_line(1)], 2: [_line(2)]},
    )
    upsert_calls: list[dict] = []

    def _upsert(**kwargs):
        upsert_calls.append(kwargs)

    with (
        patch(
            "backend.services.sync_cost_receptions.get_connection",
            return_value=fake_conn,
        ),
        patch(
            "backend.services.sync_cost_receptions._load_companies",
            return_value=[{"company_id": 3, "name": "SPA", "token": "t"}],
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.get_sync_state",
            return_value=state,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.reset_product_column_cache"
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.log_bsale_products_schema"
        ),
        patch(
            "backend.services.sync_cost_receptions.BsaleClient",
            return_value=client,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.upsert_sync_state",
            side_effect=lambda *a, **k: _upsert(**k),
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.line_exists",
            return_value=False,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.variant_tax_context",
            return_value={"tax_context_available": False},
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.previous_cost_for_variant",
            return_value=None,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.insert_history_line",
            return_value=True,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.upsert_variant_cost_snapshot"
        ) as upsert_vc,
        patch("backend.services.sync_cost_receptions.time.sleep"),
    ):
        result = sync_cost_receptions(
            company_id=3,
            date_from=date(2026, 6, 23),
            date_to=date(2026, 6, 24),
            confirm_reception_count=2,
            max_receptions=1,
            history_only=True,
            apply=True,
        )

    assert result["ok"] is True
    assert upsert_vc.call_count == 0
    assert upsert_calls
    assert upsert_calls[0]["last_admission_ts"] == t1  # última procesada (cap=1)
    co = result["companies"][0]
    assert co["watermark_before"] == prev_wm
    assert co["watermark_after"] == t1
    assert co["receptions_processed"] == 1


def test_dry_run_with_dates_does_not_upsert_state():
    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur
    client = FakeClient(
        receptions=[_rec(1, _ts(2026, 6, 23))],
        details_by_rec={1: [_line(1)]},
    )
    with (
        patch(
            "backend.services.sync_cost_receptions.get_connection",
            return_value=fake_conn,
        ),
        patch(
            "backend.services.sync_cost_receptions._load_companies",
            return_value=[{"company_id": 3, "name": "SPA", "token": "t"}],
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.get_sync_state",
            return_value={"last_admission_ts": _ts(2026, 6, 22)},
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.reset_product_column_cache"
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.log_bsale_products_schema"
        ),
        patch(
            "backend.services.sync_cost_receptions.BsaleClient",
            return_value=client,
        ),
        patch(
            "backend.services.sync_cost_receptions.repo.upsert_sync_state"
        ) as upsert_state,
        patch(
            "backend.services.sync_cost_receptions.repo.line_exists",
            return_value=False,
        ),
        patch("backend.services.sync_cost_receptions.time.sleep"),
    ):
        result = sync_cost_receptions(
            company_id=3,
            date_from=date(2026, 6, 23),
            date_to=date(2026, 6, 23),
            history_only=True,
            dry_run=True,
        )
    assert result["dry_run"] is True
    upsert_state.assert_not_called()
    fake_conn.rollback.assert_called()
    co = result["companies"][0]
    assert co["would_insert"] == 1
    assert co["inserted"] == 0
