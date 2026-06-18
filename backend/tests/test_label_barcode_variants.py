"""Tests para variantes de barcode en etiquetas."""

from backend.utils.label_barcode_variants import barcode_lookup_candidates


def test_barcode_lookup_candidates_padding_from_excel_number():
    assert barcode_lookup_candidates("70847021964") == [
        "70847021964",
        "070847021964",
        "0070847021964",
        "00070847021964",
    ]


def test_barcode_lookup_candidates_preserves_leading_zeros_text():
    assert barcode_lookup_candidates("070847021964") == [
        "070847021964",
        "0070847021964",
        "00070847021964",
    ]


def test_barcode_lookup_candidates_strips_decimal_suffix():
    assert barcode_lookup_candidates("7806500505709.0") == [
        "7806500505709",
        "07806500505709",
    ]


def test_barcode_lookup_candidates_formula_text():
    assert barcode_lookup_candidates('="70847021964"') == [
        "70847021964",
        "070847021964",
        "0070847021964",
        "00070847021964",
    ]
