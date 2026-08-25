"""Tests SEC y normalización módulo Cargas."""

from backend.services.cargas.sec import (
    boxes_and_loose_from_units,
    extract_sec,
    normalize_search_text,
    units_from_boxes_and_loose,
)


def test_extract_sec_from_product_name():
    assert extract_sec("CRISTAL LATA 470 CC (SEC 24)") == 24
    assert extract_sec("JAGERMEISTER 700 CC (SEC 6)") == 6
    assert extract_sec("COCA COLA LATA 350 CC (sec 24)") == 24
    assert extract_sec("SIN SEC") is None


def test_units_from_boxes_and_loose():
    assert units_from_boxes_and_loose(boxes=1, loose=6, sec=24) == 30
    assert units_from_boxes_and_loose(boxes=5, loose=3, sec=24) == 123


def test_boxes_and_loose_interpretation():
    boxes, loose = boxes_and_loose_from_units(30, 24)
    assert boxes == 1
    assert loose == 6


def test_normalize_search_accents_and_case():
    assert normalize_search_text("  Cristál   LATA ") == "cristal lata"
