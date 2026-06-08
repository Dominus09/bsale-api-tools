"""Pruebas unitarias: reglas comerciales SEC / quantity_step."""

from backend.utils.sale_quantity import (
    build_commercial_rules,
    extract_sec_from_text,
    resolve_quantity_step,
    resolve_sale_type,
    validate_quantity,
)


def test_extract_sec_from_description():
    assert extract_sec_from_text("MONSTER 473 (SEC 24)") == 24
    assert extract_sec_from_text("RETORNABLE 1.2 LT (SEC 10)") == 10


def test_sin_sec_auto_unitario():
    rules = build_commercial_rules(
        variant_id=1,
        product_name="Producto X",
        units_per_box=None,
        variant_description="Sin empaque",
    )
    assert rules.sale_type == "UNITARIO"
    assert rules.quantity_step == 1
    assert rules.auto_unitario_no_sec is True


def test_entera_sec_24_default():
    rules = build_commercial_rules(
        variant_id=2,
        product_name="MONSTER 473",
        units_per_box=24,
    )
    assert rules.sale_type == "ENTERA"
    assert rules.quantity_step == 24


def test_parcial_step_configurable():
    rules = build_commercial_rules(
        variant_id=3,
        product_name="LATA 470",
        units_per_box=24,
        pm_sale_type="PARCIAL",
        pm_quantity_step=6,
    )
    assert rules.sale_type == "PARCIAL"
    assert rules.quantity_step == 6


def test_validate_entera_monster_qty_5():
    rules = build_commercial_rules(
        variant_id=2,
        product_name="MONSTER 473",
        units_per_box=24,
        pm_sale_type="ENTERA",
        pm_quantity_step=24,
    )
    result = validate_quantity(5, rules=rules)
    assert not result.ok
    assert "múltiplos de 24" in (result.message or "")


def test_validate_entera_monster_qty_48():
    rules = build_commercial_rules(
        variant_id=2,
        product_name="MONSTER 473",
        units_per_box=24,
        pm_sale_type="ENTERA",
    )
    assert validate_quantity(48, rules=rules).ok


def test_validate_parcial_sec_10_step_5():
    rules = build_commercial_rules(
        variant_id=4,
        product_name="Retornable",
        units_per_box=10,
        pm_sale_type="PARCIAL",
        pm_quantity_step=5,
    )
    assert validate_quantity(15, rules=rules).ok
    assert not validate_quantity(7, rules=rules).ok


def test_validate_unitario_libre():
    rules = build_commercial_rules(variant_id=5, product_name="Suelto")
    assert validate_quantity(5, rules=rules).ok


def test_resolve_sale_type_sin_sec():
    assert resolve_sale_type(units_per_box=None) == "UNITARIO"


def test_resolve_quantity_step_parcial():
    step, missing = resolve_quantity_step(
        sale_type="PARCIAL",
        units_per_box=48,
        pm_quantity_step=12,
    )
    assert step == 12
    assert missing is False
