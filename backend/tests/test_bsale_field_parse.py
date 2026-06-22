"""Tests conversión segura campos Bsale."""

from backend.utils.bsale_field_parse import (
    parse_float,
    parse_int,
    parse_optional_float,
    parse_optional_int,
)


def test_parse_optional_int_empty_string():
    assert parse_optional_int("") is None
    assert parse_optional_int("   ") is None
    assert parse_optional_int(None) is None
    assert parse_optional_int("null") is None


def test_parse_optional_int_valid():
    assert parse_optional_int("12345") == 12345
    assert parse_optional_int(99) == 99


def test_parse_int_default():
    assert parse_int("") == 0
    assert parse_int("42") == 42


def test_parse_float_empty():
    assert parse_float("") == 0.0
    assert parse_float(None) == 0.0
    assert parse_float("12.5") == 12.5


def test_parse_optional_float():
    assert parse_optional_float("") is None
    assert parse_optional_float("0") == 0.0
