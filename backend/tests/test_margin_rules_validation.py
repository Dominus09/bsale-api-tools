"""Tests validación política de márgenes."""

import pytest

from backend.utils.margin_rules_validation import (
    margin_rule_key,
    validate_margin_rule_patch,
)


def test_validate_ok():
    min_v, max_v, warnings = validate_margin_rule_patch(min_margin=10, max_margin=40)
    assert min_v == 10
    assert max_v == 40
    assert warnings == []


def test_validate_allows_zero():
    min_v, max_v, warnings = validate_margin_rule_patch(min_margin=0, max_margin=25)
    assert min_v == 0
    assert max_v == 25
    assert warnings == []


def test_validate_both_zero_warns():
    _, _, warnings = validate_margin_rule_patch(min_margin=0, max_margin=0)
    assert len(warnings) == 1


def test_validate_min_gt_max():
    with pytest.raises(ValueError, match="mínimo"):
        validate_margin_rule_patch(min_margin=50, max_margin=10)


def test_validate_non_numeric():
    with pytest.raises(ValueError, match="numérico"):
        validate_margin_rule_patch(min_margin="abc", max_margin=10)


def test_rule_key_null_product_type():
    assert margin_rule_key(3, 5, None) == "3_5_"


def test_rule_key_with_product_type():
    assert margin_rule_key(3, 5, 12) == "3_5_12"
