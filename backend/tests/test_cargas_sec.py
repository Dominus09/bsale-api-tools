"""Tests SEC, barcode y normalización módulo Cargas."""

from decimal import Decimal

from backend.services.cargas.sec import (
    boxes_and_loose_from_units,
    extract_sec,
    normalize_barcode,
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


def test_normalize_barcode_string():
    assert normalize_barcode("7802100505323") == "7802100505323"


def test_normalize_barcode_int():
    assert normalize_barcode(7802100505323) == "7802100505323"


def test_normalize_barcode_float_dot_zero():
    assert normalize_barcode(7802100505323.0) == "7802100505323"
    assert normalize_barcode(7802100505323.0) != "78021005053230"


def test_normalize_barcode_string_floatish():
    assert normalize_barcode("7802100505323.0") == "7802100505323"
    assert normalize_barcode("7802100505323.000") == "7802100505323"


def test_normalize_barcode_nan_none():
    assert normalize_barcode(None) is None
    assert normalize_barcode(float("nan")) is None
    assert normalize_barcode("nan") is None
    assert normalize_barcode("None") is None
    assert normalize_barcode("") is None


def test_normalize_barcode_leading_zeros_as_text():
    assert normalize_barcode("007802100505323") == "007802100505323"


def test_normalize_barcode_decimal():
    assert normalize_barcode(Decimal("7802100505323")) == "7802100505323"
    assert normalize_barcode(Decimal("7802100505323.0")) == "7802100505323"
