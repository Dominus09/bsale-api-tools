"""Regresión: hijos Bsale usan source id, no PK local tras reemisión por folio."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from backend.services.distribuidora.sync_service import _refresh_document_children
from backend.utils.bsale_document_ids import (
    ids_differ,
    resolve_bsale_source_document_id,
)
from backend.utils.order_weight_calc import aggregate_order_summary, compute_line_from_row


def test_resolve_prefers_raw_document_id_over_local():
    assert (
        resolve_bsale_source_document_id(
            local_document_id=3832233,
            raw_document={"id": 3832384, "number": 68199},
        )
        == 3832384
    )
    assert ids_differ(3832233, 3832384) is True


def test_resolve_falls_back_to_local_when_no_raw():
    assert (
        resolve_bsale_source_document_id(local_document_id=3832233, raw_document=None)
        == 3832233
    )


def test_refresh_children_fetches_from_bsale_source_persists_local():
    """
    Folio existente local 3832233; Bsale reemite 3832384.
    Details se GET desde 3832384 y se almacenan bajo 3832233; qty 1→20.
    """
    local_id = 3832233
    source_id = 3832384
    folio = 68199

    detail_items = [
        {
            "id": 9990001,
            "lineNumber": 0,
            "quantity": 20.0,
            "netUnitValue": 9235,
            "totalUnitValue": 10990,
            "netAmount": 184706,
            "taxAmount": 35094,
            "totalAmount": 219800,
            "netDiscount": 0,
            "totalDiscount": 0,
            "discountPercentage": 0.0,
            "relatedDetailId": 0,
            "variant": {
                "id": 27383,
                "code": "68237149926080",
                "description": "ROJO 12 KG APROX (SEC 1)",
            },
        }
    ]

    captured: dict[str, Any] = {"gets": [], "replace_calls": []}

    def fake_get(path: str, params: dict | None = None, **kwargs):
        captured["gets"].append(path)
        if path.endswith("/details.json"):
            return {"items": detail_items, "count": 1}
        if path.endswith("/attributes.json"):
            return {"items": []}
        if path.endswith("/references.json"):
            return {"items": []}
        if path.endswith("/sellers.json"):
            return {"items": []}
        return {}

    client = MagicMock()
    client.get.side_effect = fake_get

    conn = MagicMock()
    cur = MagicMock()
    stats: dict[str, Any] = {}

    with (
        patch(
            "backend.services.distribuidora.sync_service.release_transaction"
        ),
        patch("backend.services.distribuidora.sync_service.log_tx"),
        patch("backend.services.distribuidora.sync_service.safe_rollback"),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_details",
            side_effect=lambda _cur, doc_id, items: (
                captured["replace_calls"].append(
                    {"local_document_id": doc_id, "qty": items[0]["quantity"]}
                ),
                len(items),
            )[1],
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_attributes",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_references",
            return_value=0,
        ),
        patch(
            "backend.services.distribuidora.sync_service.replace_document_sellers",
            return_value=0,
        ),
    ):
        _refresh_document_children(
            client,
            cur,
            conn,
            local_id,
            33,
            stats,
            raw_document={
                "id": source_id,
                "number": folio,
                "totalAmount": 219800,
            },
            folio=folio,
        )

    detail_gets = [g for g in captured["gets"] if g.endswith("/details.json")]
    assert detail_gets == [f"/documents/{source_id}/details.json"]
    assert all(f"/documents/{local_id}/" not in g for g in captured["gets"])
    assert captured["replace_calls"] == [
        {"local_document_id": local_id, "qty": 20.0}
    ]
    assert stats["last_children_local_document_id"] == local_id
    assert stats["last_children_bsale_source_document_id"] == source_id
    assert stats["last_children_ids_differ"] is True
    assert stats["last_children_details_replaced"] == 1


def test_weight_after_qty_resync_keeps_manual_15_total_300():
    """Peso manual 15 kg se conserva; total pasa de 15 a 300 con qty 20."""
    now = datetime.now(timezone.utc)
    old_sync = datetime(2026, 6, 4, tzinfo=timezone.utc)

    def row(qty: float) -> dict:
        return {
            "detail_id": 8971875,
            "line_number": 0,
            "variant_id": 27383,
            "codigo": "68237149926080",
            "producto": "ROJO",
            "cantidad_unitaria": qty,
            "units_per_box": 1,
            "peso_unitario_kg": 15.0,
            "peso_caja_kg": 15.0,
            "products_master_id": 998,
            "variante": "15 KG",
            "logistics_completed": False,
            "join_variant_ok": False,
            "join_barcode_ok": True,
            "exists_in_pm": True,
            "pm_updated_at": now,
            "last_bsale_sync_at": old_sync,
            "height_cm": None,
            "width_cm": None,
            "length_cm": None,
            "barcode": "68237149926080",
            "codigo_interno": "68237149926080",
        }

    before = compute_line_from_row(row(1))
    after = compute_line_from_row(row(20))
    assert before["peso_linea_kg"] == 15.0
    assert after["peso_unitario_kg"] == 15.0
    assert after["fuente_peso"] == "manual"
    assert after["peso_linea_kg"] == 300.0
    summary = aggregate_order_summary([after])
    assert summary["peso_total_kg"] == 300.0
    assert summary["porcentaje_cobertura"] == 100.0
    assert summary["productos_manuales"] == 1
