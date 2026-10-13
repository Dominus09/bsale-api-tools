"""Catálogo web: solo listas 13 (factura/Ruta-Web) y 16 (melinka). La 14 (COMODITI) está descontinuada."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from backend.routers.catalog import _CATALOG_QUERY, get_catalog
from backend.routers.orders import (
    CreateOrderBody,
    _commercial_fields_from_row,
    _require_price_list_matches_city,
    _resolve_price_list_commercial,
)


def _order(price_list: str, price_list_id: int | None = None) -> CreateOrderBody:
    return CreateOrderBody(
        client_name="X",
        client_rut="1-9",
        items=[],
        total=0,
        price_list=price_list,
        price_list_id=price_list_id,
        payment_method="efectivo",
        document_type="boleta",
        contact_name="",
        contact_phone="1",
    )


def test_catalog_query_never_reads_price_14():
    assert "price_14" not in _CATALOG_QUERY
    assert "price_13" in _CATALOG_QUERY
    assert "price_16" in _CATALOG_QUERY


@pytest.mark.parametrize("value", ["comoditi", "COMODITI", " comoditi ", "14", "otra"])
def test_catalog_rejects_discontinued_or_unknown_lists(value):
    with pytest.raises(HTTPException) as exc:
        get_catalog(price_list=value, in_stock=None)
    assert exc.value.status_code == 400


@pytest.mark.parametrize("value", ["comoditi", "COMODITI"])
def test_new_order_rejects_comoditi(value):
    with pytest.raises(HTTPException) as exc:
        _resolve_price_list_commercial(_order(value))
    assert exc.value.status_code == 400
    assert "descontinuada" in exc.value.detail


def test_new_order_rejects_price_list_id_14():
    with pytest.raises(HTTPException):
        _resolve_price_list_commercial(_order("factura", price_list_id=14))


@pytest.mark.parametrize("slug,list_id", [("factura", 13), ("melinka", 16)])
def test_new_order_accepts_valid_lists(slug, list_id):
    assert _resolve_price_list_commercial(_order(slug, price_list_id=list_id)) == slug


@pytest.mark.parametrize(
    "city,expected",
    [
        ("MELINKA", "melinka"),
        ("Melinka", "melinka"),
        ("QUELLON", "factura"),
        ("ANCUD", "factura"),
        ("CASTRO", "factura"),
        (None, "factura"),
        ("", "factura"),
    ],
)
def test_order_price_list_must_match_client_city(city, expected):
    _require_price_list_matches_city(expected, city)
    other = "factura" if expected == "melinka" else "melinka"
    with pytest.raises(HTTPException) as exc:
        _require_price_list_matches_city(other, city)
    assert exc.value.status_code == 409
    assert exc.value.detail["expected_price_list"] == expected


def test_historical_comoditi_orders_still_readable():
    fields = _commercial_fields_from_row({"price_list": "comoditi"})
    assert fields["price_list"] == "comoditi"
    assert fields["price_list_id"] == 14
