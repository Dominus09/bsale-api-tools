"""Tests picking múltiple y estados operacionales."""

from backend.services.distribuidora.dispatch_plan_load_batch_service import (
    BLOCKED_ADD_ORDER_STATUSES,
)


def test_blocked_add_order_statuses():
    assert "dispatched" in BLOCKED_ADD_ORDER_STATUSES
    assert "delivered" in BLOCKED_ADD_ORDER_STATUSES
    assert "picking_generated" not in BLOCKED_ADD_ORDER_STATUSES


def test_operational_status_mapping_frontend_parity():
    """Paridad con dispatch-plan-operational-status.ts."""
    open_statuses = {"draft", "planned", "invoicing", "ready_for_picking"}
    assert "closed" not in open_statuses
    assert "squared" not in open_statuses
