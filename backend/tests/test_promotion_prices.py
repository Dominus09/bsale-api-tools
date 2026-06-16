"""Tests para snapshot de precios promocionales."""

from decimal import Decimal

import pytest

from backend.utils.promotion_price_list_map import mapped_price_list_for_company
from backend.utils.promotion_prices import calc_sale_price


def test_mapped_price_list_la_quillotana():
    assert mapped_price_list_for_company("La Quillotana Spa") == "Supermercado La Quillotana"


def test_mapped_price_list_minimarket():
    assert mapped_price_list_for_company("Minimarket") == "Minimarket"


def test_mapped_price_list_carlos_romero():
    assert mapped_price_list_for_company("Carlos Romero") == "Quillotana V"


def test_mapped_price_list_unknown():
    assert mapped_price_list_for_company("Otra Empresa") is None


def test_calc_sale_price_porcentaje():
    regular = Decimal("1490")
    sale = calc_sale_price(regular, "porcentaje", Decimal("20"))
    assert sale == Decimal("1192.00")


def test_calc_sale_price_fijo():
    regular = Decimal("1490")
    sale = calc_sale_price(regular, "precio_fijo", Decimal("990"))
    assert sale == Decimal("990.00")


def test_calc_sale_price_invalid():
    with pytest.raises(ValueError):
        calc_sale_price(Decimal("100"), "otro", Decimal("10"))
