"""Validación y presentación de cantidad de peonetas."""

from __future__ import annotations

import pytest

from backend.services.distribuidora.peonetas import (
    PeonetasValidationError,
    format_peonetas_display,
    parse_peonetas_count,
)


@pytest.mark.parametrize("value", [0, 1, 2, 10, "3"])
def test_parse_peonetas_ok(value):
    assert parse_peonetas_count(value) >= 0


@pytest.mark.parametrize(
    "value",
    [None, -1, 1.5, "abc", True, False, 11, float("nan")],
)
def test_parse_peonetas_rejects(value):
    with pytest.raises(PeonetasValidationError):
        parse_peonetas_count(value)


def test_format_historical_zero_uninformed():
    assert (
        format_peonetas_display(assistant_count=0, peonetas_recorded=False)
        == "No informado"
    )
    assert (
        format_peonetas_display(assistant_count=0, peonetas_recorded=None)
        == "No informado"
    )


def test_format_recorded_zero():
    assert format_peonetas_display(assistant_count=0, peonetas_recorded=True) == "0"


def test_format_ors_positive_without_flag():
    assert format_peonetas_display(assistant_count=2, peonetas_recorded=False) == "2"
