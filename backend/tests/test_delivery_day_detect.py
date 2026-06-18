"""Tests detección día de entrega pre-despacho."""

from backend.utils.delivery_day_detect import (
    delivery_day_matches_filter,
    detect_delivery_day_from_observation,
    normalize_municipality_name,
    resolve_delivery_day,
)


def test_detect_sabado_variants():
    assert detect_delivery_day_from_observation("Entrega sábado") == "sabado"
    assert detect_delivery_day_from_observation("Sábado") == "sabado"
    assert detect_delivery_day_from_observation("Sabado") == "sabado"
    assert detect_delivery_day_from_observation("Retiro sábado") == "sabado"


def test_detect_viernes():
    assert detect_delivery_day_from_observation("Mención viernes") == "viernes"
    assert detect_delivery_day_from_observation("Entrega Viernes") == "viernes"


def test_obs_explicit_overrides_route():
    day, source = resolve_delivery_day(
        "Entrega sábado",
        comments="Viernes",
        dia_atencion="Viernes",
    )
    assert day == "sabado"
    assert source == "observacion"


def test_ancud_sabado_not_in_viernes_filter():
    day, _ = resolve_delivery_day("Entrega sábado", dia_atencion="Viernes")
    assert day == "sabado"
    assert not delivery_day_matches_filter(day, ["viernes"])
    assert delivery_day_matches_filter(day, ["sabado"])


def test_empty_obs_uses_route():
    day, source = resolve_delivery_day("", dia_atencion="Viernes")
    assert day == "viernes"
    assert source == "ruta"


def test_no_day_in_obs_uses_route_not_comments_when_obs_has_text():
    day, source = resolve_delivery_day(
        "Pedido urgente",
        comments="Entrega viernes",
        dia_atencion="Sabado",
    )
    assert day == "sabado"
    assert source == "ruta"


def test_normalize_municipality():
    assert normalize_municipality_name("ANCUD") == "Ancud"
    assert normalize_municipality_name("ancud") == "Ancud"
    assert normalize_municipality_name("Quellon") == "Quellón"
    assert normalize_municipality_name("quellon") == "Quellón"
