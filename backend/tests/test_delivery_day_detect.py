"""Tests detección día de entrega."""

from backend.utils.delivery_day_detect import resolve_delivery_day


def test_resolve_delivery_day_prefers_comments_over_client_route():
    day, source = resolve_delivery_day(
        observaciones="Pedido urgente sin día",
        comments="Entrega viernes en Miramark",
        dia_atencion="miercoles",
    )
    assert day == "viernes"
    assert source == "comentario"


def test_resolve_delivery_day_observacion_over_route():
    day, source = resolve_delivery_day(
        observaciones="Despacho para el viernes",
        comments=None,
        dia_atencion="miercoles",
    )
    assert day == "viernes"
    assert source == "observacion"


def test_resolve_delivery_day_last_mention_with_context():
    day, _ = resolve_delivery_day(
        observaciones="Entrega miercoles reprogramada entrega viernes",
        comments=None,
        dia_atencion=None,
    )
    assert day == "viernes"
