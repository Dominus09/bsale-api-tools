"""Revisiones humanas de costos — servicio (sin DB real)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from backend.services.analytics_v2.cost_reviews_service import (
    CostReviewsService,
    can_mutate_cost_reviews,
    snapshot_from_cost_product,
)


def _row(**overrides: Any) -> dict[str, Any]:
    base = {
        "id": 1,
        "company_id": 3,
        "company_name": "La Quillotana",
        "variant_id": 100,
        "anomaly_code": "alza_anormal",
        "reason_label": "Alza anormal",
        "priority": "alta",
        "status": "pendiente",
        "product_name": "Producto X",
        "sku_code": "SEC-1",
        "barcode": "780123",
        "current_cost_ref": 1000,
        "previous_cost_ref": 800,
        "change_pct_ref": 25,
        "assignee_user_id": None,
        "assignee_email": None,
        "started_at": None,
        "resolved_at": None,
        "observations": None,
        "conclusion": None,
        "source": "dashboard_alert",
        "source_ref": {},
        "created_by_user_id": 7,
        "created_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
        "updated_at": datetime(2026, 10, 1, tzinfo=timezone.utc),
    }
    base.update(overrides)
    return base


def _svc(repo: MagicMock, **kwargs: Any) -> CostReviewsService:
    snap = kwargs.pop("snapshot_fn", None) or (
        lambda _vid: {
            "product_name": "Producto X — Var",
            "sku_code": "SEC-1",
            "barcode": "780123",
            "current_cost_ref": Decimal("1000"),
            "previous_cost_ref": Decimal("800"),
            "change_pct_ref": Decimal("25"),
        }
    )
    return CostReviewsService(
        company_id=3,
        company_name="La Quillotana",
        actor=kwargs.pop("actor", {"id": 7, "email": "a@x.cl", "role": "gerencia"}),
        snapshot_fn=snap,
        repo=repo,
    )


@pytest.fixture()
def repo():
    fake = MagicMock()
    fake.table_ready.return_value = True
    return fake


def test_can_mutate_roles():
    assert can_mutate_cost_reviews({"role": "gerencia", "id": 1})
    assert can_mutate_cost_reviews({"role": "adm", "id": 1})
    assert not can_mutate_cost_reviews({"role": "operativo", "id": 1})


def test_snapshot_from_cost_product():
    snap = snapshot_from_cost_product(
        {
            "product_name": "Aceite",
            "variant_name": "1L",
            "code": "SEC9",
            "barcode": "780",
            "last_comparable": {"cost": "1200"},
            "previous_comparable": {"cost": "1000"},
            "change": {"pct": "20"},
        }
    )
    assert snap["product_name"] == "Aceite — 1L"
    assert snap["sku_code"] == "SEC9"
    assert snap["current_cost_ref"] == Decimal("1200")


def test_create_idempotent(repo):
    existing = _row()
    repo.find_open.return_value = existing
    out = _svc(repo).create_or_get(variant_id=100, anomaly_code="alza_anormal")
    assert out["created"] is False
    assert out["item"]["company_name"] == "La Quillotana"
    repo.insert_review_with_event.assert_not_called()


def test_create_uses_backend_snapshot_not_client_costs(repo):
    repo.find_open.return_value = None
    inserted = _row(current_cost_ref=Decimal("999"))
    repo.insert_review_with_event.return_value = inserted

    def snap(_vid: int) -> dict[str, Any]:
        return {
            "product_name": "Desde V2",
            "sku_code": "S1",
            "barcode": "B1",
            "current_cost_ref": Decimal("999"),
            "previous_cost_ref": Decimal("500"),
            "change_pct_ref": Decimal("99.8"),
        }

    out = _svc(repo, snapshot_fn=snap).create_or_get(
        variant_id=100,
        anomaly_code="alza_anormal",
        reason_label="Alza",
        source="dashboard_alert",
        source_ref={"reception_id": 5, "evil_cost": 1},
    )
    assert out["created"] is True
    assert out["item"]["assignee_user_id"] is None
    assert out["item"]["is_human_review"] is True
    assert out["item"]["bsale_costs_editable"] is False
    values, event = repo.insert_review_with_event.call_args[0]
    assert values["current_cost_ref"] == Decimal("999")
    assert values["company_name"] == "La Quillotana"
    assert values["status"] == "pendiente"
    assert "assignee_user_id" not in values
    # source_ref filtrado: no acepta claves arbitrarias de costo
    from psycopg2.extras import Json

    assert isinstance(values["source_ref"], Json)
    assert values["source_ref"].adapted == {"reception_id": 5}
    assert event["action"] == "created"


def test_create_missing_variant(repo):
    repo.find_open.return_value = None

    def boom(_vid: int) -> dict[str, Any]:
        raise HTTPException(status_code=404, detail="Variante inexistente en la empresa")

    with pytest.raises(HTTPException) as exc:
        _svc(repo, snapshot_fn=boom).create_or_get(variant_id=999, anomaly_code="costo_cero")
    assert exc.value.status_code == 404


def test_operativo_cannot_create(repo):
    with pytest.raises(HTTPException) as exc:
        _svc(repo, actor={"id": 9, "email": "op@x.cl", "role": "operativo"}).create_or_get(
            variant_id=1, anomaly_code="costo_cero"
        )
    assert exc.value.status_code == 403


def test_start_assigns_authenticated_user(repo):
    repo.get.return_value = _row()
    started = _row(
        status="en_revision",
        assignee_user_id=7,
        assignee_email="a@x.cl",
        started_at=datetime.now(timezone.utc),
    )
    repo.update_review_with_event.return_value = started
    out = _svc(repo).start(1)
    assert out["assignee_user_id"] == 7
    kwargs = repo.update_review_with_event.call_args.kwargs
    assert kwargs["expected_status"] == "pendiente"
    assert kwargs["expected_assignee_null"] is True
    assert kwargs["sets"]["assignee_user_id"] == 7
    assert kwargs["event"]["action"] == "started"


def test_start_concurrency_conflict(repo):
    repo.get.side_effect = [
        _row(),
        _row(status="en_revision", assignee_user_id=99, assignee_email="otro@x.cl"),
    ]
    repo.update_review_with_event.return_value = None
    with pytest.raises(HTTPException) as exc:
        _svc(repo).start(1)
    assert exc.value.status_code == 409


def test_start_already_mine_is_idempotent(repo):
    repo.get.return_value = _row(status="en_revision", assignee_user_id=7, assignee_email="a@x.cl")
    out = _svc(repo).start(1)
    assert out["status"] == "en_revision"
    repo.update_review_with_event.assert_not_called()


def test_resolve_requires_conclusion(repo):
    repo.get.return_value = _row(status="en_revision", assignee_user_id=7)
    with pytest.raises(HTTPException) as exc:
        _svc(repo).resolve(1, conclusion="abc")
    assert exc.value.status_code == 422


def test_resolve_ok_same_transaction(repo):
    repo.get.return_value = _row(status="en_revision", assignee_user_id=7)
    closed = _row(
        status="resuelto",
        conclusion="Se validó con proveedor",
        assignee_user_id=7,
        started_at=datetime.now(timezone.utc),
        resolved_at=datetime.now(timezone.utc),
    )
    repo.update_review_with_event.return_value = closed
    out = _svc(repo).resolve(1, conclusion="Se validó con proveedor")
    assert out["status"] == "resuelto"
    kwargs = repo.update_review_with_event.call_args.kwargs
    assert kwargs["event"]["action"] == "resolved"
    assert kwargs["event"]["status_from"] == "en_revision"
    assert kwargs["sets"]["status"] == "resuelto"


def test_resolve_from_pendiente_rejected(repo):
    repo.get.return_value = _row(status="pendiente")
    with pytest.raises(HTTPException) as exc:
        _svc(repo).resolve(1, conclusion="Conclusión válida")
    assert exc.value.status_code == 409
    repo.update_review_with_event.assert_not_called()


def test_discard_from_pendiente_ok(repo):
    repo.get.return_value = _row(status="pendiente")
    repo.update_review_with_event.return_value = _row(
        status="descartado",
        conclusion="Alerta justificada ok",
        resolved_at=datetime.now(timezone.utc),
    )
    out = _svc(repo).discard(1, conclusion="Alerta justificada ok")
    assert out["status"] == "descartado"


def test_conclusion_requires_five_non_whitespace(repo):
    repo.get.return_value = _row(status="en_revision", assignee_user_id=7)
    with pytest.raises(HTTPException) as exc:
        _svc(repo).resolve(1, conclusion="  ab  ")
    assert exc.value.status_code == 422


def test_update_priority(repo):
    repo.get.return_value = _row(priority="media")
    repo.update_review_with_event.return_value = _row(priority="alta")
    out = _svc(repo).update_priority(1, "alta")
    assert out["priority"] == "alta"
    assert repo.update_review_with_event.call_args.kwargs["event"]["action"] == "priority_updated"


def test_company_isolation_on_get(repo):
    repo.get.return_value = None
    with pytest.raises(HTTPException) as exc:
        _svc(repo).get(99)
    assert exc.value.status_code == 404
    repo.get.assert_called_with(company_id=3, review_id=99)


def test_list_sort_and_company(repo):
    repo.list_reviews.return_value = ([_row()], 1)
    out = _svc(repo, actor={"id": 7, "email": "a@x.cl", "role": "gerencia"}).list(sort="priority_desc")
    assert out["company"]["name"] == "La Quillotana"
    assert out["sort"] == "priority_desc"
    assert out["items"][0]["costs_are_snapshot"] is True
    repo.list_reviews.assert_called_once()
    assert repo.list_reviews.call_args.kwargs["sort"] == "priority_desc"


def test_invalid_sort(repo):
    with pytest.raises(HTTPException) as exc:
        _svc(repo).list(sort="hack")
    assert exc.value.status_code == 422


def test_table_not_ready(repo):
    repo.table_ready.return_value = False
    with pytest.raises(HTTPException) as exc:
        _svc(repo).list()
    assert exc.value.status_code == 503


def test_observations_on_closed_rejected(repo):
    repo.get.return_value = _row(status="resuelto", conclusion="ok")
    with pytest.raises(HTTPException) as exc:
        _svc(repo).update_observations(1, "nota")
    assert exc.value.status_code == 409
