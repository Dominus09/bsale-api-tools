"""Navegación Google Maps: destino = coords del cliente seleccionado."""

from __future__ import annotations

import pytest

from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as svc


def test_lily_melinka_destination_coords():
    """Minimarket Lily: -43.89426, -73.74342 → destino explícito, no Ancud."""
    nav = svc.build_client_navigation(
        -43.894257,
        -73.743421,
        city="Melinka",
        client_name="Minimarket lily",
    )
    assert nav["available"] is True
    assert nav["destination_latitude"] == pytest.approx(-43.894257)
    assert nav["destination_longitude"] == pytest.approx(-73.743421)
    assert "destination=-43.894257%2C-73.743421" in nav["google_maps_directions_url"]
    assert "query=-43.894257%2C-73.743421" in nav["google_maps_place_url"]
    assert "origin=" not in nav["google_maps_directions_url"]
    assert nav["likely_no_road_route"] is True
    # Preferir marcador exacto en destino insular (evitar ruta continental engañosa).
    assert nav["google_maps_url"] == nav["google_maps_place_url"]
    assert nav["warning_es"]


def test_continental_prefers_directions():
    nav = svc.build_client_navigation(-41.87, -73.82, city="Ancud", client_name="X")
    assert nav["likely_no_road_route"] is False
    assert nav["google_maps_url"] == nav["google_maps_directions_url"]
    assert nav["warning_es"] is None


def test_navigation_rejects_bad_coords():
    with pytest.raises(ValueError):
        svc.build_client_navigation(float("nan"), -73.0)
    with pytest.raises(ValueError):
        svc.build_client_navigation(-91.0, 0.0)


def test_product_equivalence_pending_without_upb():
    p = svc._product_payload(
        {
            "display_name": "ZERO ZERO DESECHABLE 1.5 LT (SEC 6)",
            "producto": "ZERO ZERO",
            "unidades": 60,
            "cajas": 0,
            "units_per_box": None,
            "sin_unidad_caja": True,
            "variant_id": 31296,
        },
        load_group=None,
    )
    assert p["cajas_equivalentes"] is None
    assert p["equivalence_status"] == "pending_validation"
    assert p["equivalence_label"] == "Equivalencia por validar"
    # No inventar cajas desde el token SEC del nombre.
    assert "SEC 6" in p["description"]
