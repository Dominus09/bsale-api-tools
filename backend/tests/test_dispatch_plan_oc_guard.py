"""Locks y conflictos de OC en confirmación."""

from __future__ import annotations

import pytest

from backend.services.distribuidora.dispatch_plan_oc_guard import (
    CONFIRM_BLOCKING_PLAN_STATUSES,
    OcConfirmConflictError,
    acquire_oc_confirm_locks,
    assert_ocs_available_for_confirm,
    find_blocking_oc_assignments,
)


class _Cur:
    def __init__(self, rows=None):
        self.executed = []
        self._rows = list(rows or [])
        self.description = [
            ("oc_document_id",),
            ("dispatch_plan_id",),
            ("planning_code",),
            ("status",),
            ("planning_date",),
            ("truck_id",),
            ("plan_session_id",),
        ]

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self._rows


def test_blocking_statuses_include_delivered_exclude_closed():
    assert "delivered" in CONFIRM_BLOCKING_PLAN_STATUSES
    assert "closed" not in CONFIRM_BLOCKING_PLAN_STATUSES
    assert "squared" not in CONFIRM_BLOCKING_PLAN_STATUSES


def test_acquire_locks_sorted_advisory_and_for_update():
    cur = _Cur()
    acquire_oc_confirm_locks(cur, [30, 10, 20])
    # 3 advisory + 1 FOR UPDATE
    assert len(cur.executed) == 4
    assert "pg_advisory_xact_lock" in cur.executed[0][0]
    assert cur.executed[0][1][1] == 10
    assert "FOR UPDATE" in cur.executed[-1][0]


def test_assert_ocs_available_raises_on_conflict():
    cur = _Cur(
        rows=[
            (101, 9, "PLAN-00009", "delivered", None, 2, "sess"),
        ]
    )
    with pytest.raises(OcConfirmConflictError) as ei:
        assert_ocs_available_for_confirm(cur, [101])
    assert ei.value.conflicts


def test_find_blocking_empty():
    cur = _Cur(rows=[])
    assert find_blocking_oc_assignments(cur, [1, 2]) == []
