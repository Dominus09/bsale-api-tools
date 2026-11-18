"""Confirmación masiva de documento (modo simple)."""

from __future__ import annotations

import pytest
from fastapi import HTTPException


def test_confirm_full_builds_lines_from_snapshot(monkeypatch):
    from backend.services.distribuidora import delivery_visit_service as svc

    monkeypatch.setattr(
        svc.read_svc,
        "_document_detail_lines",
        lambda _doc_id: [
            {
                "source_detail_id": 11,
                "line_number": 1,
                "variant_id": 5,
                "unidades": 24,
                "producto": "Cerveza 1L",
                "codigo_barras": "7801",
            },
            {
                "source_detail_id": 12,
                "line_number": 2,
                "variant_id": 6,
                "unidades": 12,
                "producto": "Bebida 2L",
                "codigo_barras": "7802",
            },
        ],
    )

    captured: dict = {}

    def fake_save(**kwargs):
        captured.update(kwargs)
        return {"document_complete": True, "lines_registered": 2}

    monkeypatch.setattr(svc, "save_document_lines", fake_save)

    out = svc.confirm_document_delivery(
        user={"id": 1},
        plan_id=1,
        visit_id=9,
        related_document_id=100,
        mode="full",
        document_number=500,
        document_type="Factura",
    )
    assert out["confirm_mode"] == "full"
    assert captured["result_status"] == "full"
    assert len(captured["lines"]) == 2
    assert captured["lines"][0]["delivered_units"] == 24
    assert captured["lines"][1]["delivered_units"] == 12
    assert captured["lines"][0]["planned_units"] == 24


def test_confirm_partial_requires_diffs_or_rest_flag(monkeypatch):
    from backend.services.distribuidora import delivery_visit_service as svc

    monkeypatch.setattr(
        svc.read_svc,
        "_document_detail_lines",
        lambda _doc_id: [
            {"source_detail_id": 1, "unidades": 10, "producto": "A"},
            {"source_detail_id": 2, "unidades": 5, "producto": "B"},
        ],
    )
    monkeypatch.setattr(svc, "save_document_lines", lambda **_k: {})

    with pytest.raises(HTTPException) as ei:
        svc.confirm_document_delivery(
            user={"id": 1},
            plan_id=1,
            visit_id=9,
            related_document_id=100,
            mode="partial",
            difference_lines=[],
        )
    assert ei.value.status_code == 400

    with pytest.raises(HTTPException) as ei2:
        svc.confirm_document_delivery(
            user={"id": 1},
            plan_id=1,
            visit_id=9,
            related_document_id=100,
            mode="partial",
            difference_lines=[
                {
                    "source_detail_id": 1,
                    "delivered_units": 0,
                    "undelivered_reason": "physical_shortage",
                }
            ],
            rest_delivered_confirmed=False,
        )
    assert ei2.value.status_code == 400


def test_confirm_partial_with_rest_ok(monkeypatch):
    from backend.services.distribuidora import delivery_visit_service as svc

    monkeypatch.setattr(
        svc.read_svc,
        "_document_detail_lines",
        lambda _doc_id: [
            {"source_detail_id": 1, "unidades": 10, "producto": "A"},
            {"source_detail_id": 2, "unidades": 5, "producto": "B"},
        ],
    )
    captured: dict = {}
    monkeypatch.setattr(
        svc,
        "save_document_lines",
        lambda **kwargs: captured.update(kwargs) or {"ok": True},
    )

    svc.confirm_document_delivery(
        user={"id": 1},
        plan_id=1,
        visit_id=9,
        related_document_id=100,
        mode="partial",
        difference_lines=[
            {
                "source_detail_id": 1,
                "delivered_units": 0,
                "undelivered_reason": "physical_shortage",
                "undelivered_note": "faltante",
            }
        ],
        rest_delivered_confirmed=True,
    )
    assert captured["result_status"] == "partial"
    by_id = {ln["source_detail_id"]: ln for ln in captured["lines"]}
    assert by_id[1]["delivered_units"] == 0
    assert by_id[2]["delivered_units"] == 5


def test_confirm_partial_keeps_real_quantities_example(monkeypatch):
    """A=35, B=48 (completo), C=0 — no forzar 0 en A solo por diferencia."""
    from backend.services.distribuidora import delivery_visit_service as svc

    monkeypatch.setattr(
        svc.read_svc,
        "_document_detail_lines",
        lambda _doc_id: [
            {"source_detail_id": 101, "unidades": 60, "producto": "A"},
            {"source_detail_id": 102, "unidades": 48, "producto": "B"},
            {"source_detail_id": 103, "unidades": 24, "producto": "C"},
        ],
    )
    captured: dict = {}
    monkeypatch.setattr(
        svc,
        "save_document_lines",
        lambda **kwargs: captured.update(kwargs) or {"ok": True},
    )

    svc.confirm_document_delivery(
        user={"id": 1},
        plan_id=1,
        visit_id=9,
        related_document_id=100,
        mode="partial",
        difference_lines=[
            {
                "source_detail_id": 101,
                "delivered_units": 35,
                "undelivered_reason": "physical_shortage",
            },
            {
                "source_detail_id": 103,
                "delivered_units": 0,
                "undelivered_reason": "physical_shortage",
            },
        ],
        rest_delivered_confirmed=True,
    )
    by_id = {ln["source_detail_id"]: ln["delivered_units"] for ln in captured["lines"]}
    assert by_id[101] == 35
    assert by_id[102] == 48
    assert by_id[103] == 0
    assert captured["result_status"] == "partial"


def test_confirm_partial_rejects_delivered_eq_planned(monkeypatch):
    from backend.services.distribuidora import delivery_visit_service as svc

    monkeypatch.setattr(
        svc.read_svc,
        "_document_detail_lines",
        lambda _doc_id: [{"source_detail_id": 1, "unidades": 60, "producto": "A"}],
    )
    with pytest.raises(HTTPException) as ei:
        svc.confirm_document_delivery(
            user={"id": 1},
            plan_id=1,
            visit_id=9,
            related_document_id=100,
            mode="partial",
            difference_lines=[{"source_detail_id": 1, "delivered_units": 60}],
            rest_delivered_confirmed=True,
        )
    assert ei.value.status_code == 400


def test_confirm_partial_rejects_unknown_quantity(monkeypatch):
    from backend.services.distribuidora import delivery_visit_service as svc

    monkeypatch.setattr(
        svc.read_svc,
        "_document_detail_lines",
        lambda _doc_id: [{"source_detail_id": 1, "unidades": 10, "producto": "A"}],
    )
    with pytest.raises(HTTPException) as ei:
        svc.confirm_document_delivery(
            user={"id": 1},
            plan_id=1,
            visit_id=9,
            related_document_id=100,
            mode="partial",
            difference_lines=[{"source_detail_id": 1}],
            rest_delivered_confirmed=True,
        )
    assert ei.value.status_code == 400
