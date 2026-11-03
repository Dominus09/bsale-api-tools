"""Locks y conflictos de OC en confirmación."""

from __future__ import annotations

import pytest

from backend.services.distribuidora.dispatch_plan_oc_guard import (
    CONFIRM_BLOCKING_PLAN_STATUSES,
    REPLAN_MODEL_GAP,
    REPROGRAMMABLE_PLAN_STATUSES,
    OcConfirmConflictError,
    acquire_oc_confirm_locks,
    assert_ocs_available_for_confirm,
    find_blocking_oc_assignments,
    find_delivery_evidence,
)


class _Cur:
    def __init__(self, rows=None, fetchall_map=None):
        self.executed = []
        self._rows = list(rows or [])
        self._fetchall_map = fetchall_map or {}
        self.description = [
            ("oc_document_id",),
            ("dispatch_plan_id",),
            ("planning_code",),
            ("status",),
            ("planning_date",),
            ("truck_id",),
            ("plan_session_id",),
            ("block_reason",),
        ]
        self._last_sql = ""

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self._last_sql = sql

    def fetchall(self):
        for key, rows in self._fetchall_map.items():
            if key in self._last_sql:
                return rows
        return self._rows

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None


def test_blocking_includes_closed_squared_delivered():
    assert "delivered" in CONFIRM_BLOCKING_PLAN_STATUSES
    assert "closed" in CONFIRM_BLOCKING_PLAN_STATUSES
    assert "squared" in CONFIRM_BLOCKING_PLAN_STATUSES
    assert REPROGRAMMABLE_PLAN_STATUSES == frozenset({"draft"})
    assert "closed" not in REPROGRAMMABLE_PLAN_STATUSES
    assert "reprogram" in REPLAN_MODEL_GAP.lower() or "saldos" in REPLAN_MODEL_GAP.lower()


def test_acquire_locks_sorted_advisory_and_for_update():
    cur = _Cur(
        fetchall_map={
            "source_document_id": [],
            "FOR UPDATE": [],
            "DISTINCT d2.document_id": [],
        }
    )
    # expand returns empty extra → same ids; then source query empty; then FOR UPDATE
    acquire_oc_confirm_locks(cur, [30, 10, 20])
    advisory = [e for e in cur.executed if "pg_advisory_xact_lock" in e[0]]
    assert len(advisory) >= 3
    assert advisory[0][1][1] == 10


def test_assert_ocs_available_raises_on_closed():
    cur = _Cur(
        rows=[
            (101, 9, "PLAN-00009", "closed", None, 2, "sess", "plan_status"),
        ]
    )
    with pytest.raises(OcConfirmConflictError) as ei:
        assert_ocs_available_for_confirm(cur, [101])
    assert ei.value.conflicts
    assert "saldos" in str(ei.value).lower() or "reprogram" in str(ei.value).lower()


def test_assert_ocs_available_raises_on_delivery_evidence():
    class _Cur2(_Cur):
        def fetchall(self):
            sql = self._last_sql.lower()
            if "dispatch_plan_order_events" in sql:
                return [
                    (
                        101,
                        9,
                        "PLAN-9",
                        "closed",
                        "delivery_stop_status",
                        "delivered",
                        "delivery_event",
                    )
                ]
            return []

    cur = _Cur2()
    cur.description = [
        ("oc_document_id",),
        ("dispatch_plan_id",),
        ("planning_code",),
        ("status",),
        ("action",),
        ("delivery_status",),
        ("block_reason",),
    ]
    with pytest.raises(OcConfirmConflictError) as ei:
        assert_ocs_available_for_confirm(cur, [101])
    assert ei.value.code == "oc_delivery_evidence"


def test_find_blocking_empty():
    cur = _Cur(rows=[])
    assert find_blocking_oc_assignments(cur, [1, 2]) == []


def test_find_delivery_evidence_empty_on_error():
    class Boom(_Cur):
        def execute(self, sql, params=None):
            raise RuntimeError("missing table")

    assert find_delivery_evidence(Boom(), [1]) == []
