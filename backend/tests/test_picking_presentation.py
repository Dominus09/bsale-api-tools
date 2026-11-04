"""Presentación de comunas / paradas / packs (sin mutar rutas)."""

from backend.utils.picking_presentation import (
    commercial_pack_label,
    enrich_client_presentation,
    normalize_commune_key,
    parse_commune_sector,
    visible_stop_number,
)


def test_quellon_variants_share_commune_key():
    a = parse_commune_sector("Quellon")
    b = parse_commune_sector("Quellón")
    c = parse_commune_sector("Quellón rural")
    assert a["commune_key"] == b["commune_key"] == c["commune_key"] == "quellon"
    assert a["display_commune"] == "Quellón"
    assert c["display_sector"] == "Rural"
    assert normalize_commune_key("Quellón") == normalize_commune_key("Quellon")


def test_preserve_delivery_order_and_stop_numbers():
    clients = [
        {"route_order": 0, "city": "Quellon", "client_name": "A"},
        {"route_order": 1, "city": "Quellón rural", "client_name": "B"},
        {"route_order": 2, "city": "Ancud", "client_name": "C"},
        {"route_order": 3, "city": "Quellón", "client_name": "D"},
    ]
    rows = enrich_client_presentation(clients)
    assert [r["client_name"] for r in rows] == ["A", "B", "C", "D"]
    assert [r["stop_number"] for r in rows] == [1, 2, 3, 4]
    assert rows[0]["show_commune_header"] is True
    assert rows[1]["show_commune_header"] is False  # misma comuna Quellón
    assert rows[2]["show_commune_header"] is True
    assert rows[3]["show_commune_header"] is True  # vuelve Quellón tras Ancud


def test_visible_stop_from_zero():
    assert visible_stop_number(0, min_route_order=0) == 1
    assert visible_stop_number(1, min_route_order=0) == 2
    assert visible_stop_number(1, min_route_order=1) == 1


def test_commercial_pack_sec_labels():
    assert "SEC24" in (commercial_pack_label(units_per_box=24, unidades=48) or "")
    assert commercial_pack_label(units_per_box=None, unidades=10) is None
