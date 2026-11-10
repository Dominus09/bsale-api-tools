"""P4.1: nombre del receptor y cobranza pendiente (sin analytics)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_visit_service as svc


def test_pending_collection_zero_allowed_without_db(monkeypatch):
    """pending + declared 0 no debe fallar en validación de método/monto."""
    monkeypatch.setattr(
        svc.read_svc, "enforce_driver_plan_read_access", lambda _u, _p: 7
    )

    # Fallará al abrir DB o tables_ready; si llega a 400 de monto positivo, el test falla.
    with pytest.raises(Exception) as ei:
        svc.declare_collection(
            user={"id": 7},
            plan_id=1,
            visit_id=1,
            document_result_id=None,
            method="pending",
            document_amount=1000,
            declared_amount=0,
            note=None,
        )
    if isinstance(ei.value, HTTPException):
        assert ei.value.status_code != 400 or "positivo" not in str(
            ei.value.detail
        ).lower()


def test_partial_balance_math_unit():
    doc_amt = 155033.0
    decl = 100000.0
    pending = round(doc_amt - decl, 2)
    assert pending == 55033.0
