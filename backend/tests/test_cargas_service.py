"""Tests servicio Cargas: add units, exceso, identidad de búsqueda."""

from __future__ import annotations

import pytest

from backend.services.cargas import service as svc
from backend.services.cargas.sec import units_from_boxes_and_loose


def test_units_calc_jagermeister_half_box_style():
    # Excel puede mostrar 0.5 cajas; oficial son unidades
    assert units_from_boxes_and_loose(boxes=0.5, loose=0, sec=6) == 3


def test_item_status_helpers():
    assert svc._item_status(24, 0) == "pending"
    assert svc._item_status(24, 12) == "partial"
    assert svc._item_status(24, 24) == "complete"
    assert svc._item_status(24, 48) == "excess"


def test_search_tokens_match_cristal(monkeypatch):
    load = {
        "items": [
            {
                "id": 1,
                "product_name": "CRISTAL LATA 470 CC (SEC 24)",
                "barcode": "7802100505323",
                "product_type": "CERVEZA",
                "status": "pending",
                "normalized_product_name": "cristal lata 470 cc sec 24",
            },
            {
                "id": 2,
                "product_name": "COCA COLA LATA 350 CC (SEC 24)",
                "barcode": "7801610001196",
                "product_type": "BEBIDAS",
                "status": "pending",
                "normalized_product_name": "coca cola lata 350 cc sec 24",
            },
        ]
    }
    monkeypatch.setattr(svc, "get_load", lambda _id: load)
    rows = svc.search_items(1, q="cristal")
    assert len(rows) == 1
    assert "CRISTAL" in rows[0]["product_name"]
    rows2 = svc.search_items(1, q="470 cristal")
    assert len(rows2) == 1
    rows3 = svc.search_items(1, q="7801610001196")
    assert len(rows3) == 1
