"""P4.4: categoría + producto + variante en líneas PWA (sin inventar marcas)."""

from __future__ import annotations

from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as svc


def test_commercial_name_combines_product_and_variant():
    name = svc._commercial_product_name(
        {
            "producto_base": "ROYAL GUARD",
            "producto_variante": "RETORNABLE 1.0 LT (SEC 12)",
        }
    )
    assert name == "ROYAL GUARD — RETORNABLE 1.0 LT (SEC 12)"


def test_commercial_name_avoids_duplicating_product_inside_variant():
    name = svc._commercial_product_name(
        {
            "product_name": "Cristal",
            "producto_variante": "Cristal retornable 970 cc",
        }
    )
    assert name == "Cristal retornable 970 cc"


def test_commercial_name_fallback_variant_only():
    name = svc._commercial_product_name(
        {"producto_variante": "RETORNABLE 970 CC (SEC 12)"}
    )
    assert name == "RETORNABLE 970 CC (SEC 12)"


def test_product_payload_exposes_category_product_variant_barcode():
    p = svc._product_payload(
        {
            "producto_base": "ROYAL GUARD",
            "producto_variante": "RETORNABLE 1.0 LT (SEC 12)",
            "category_name": "CERVEZAS",
            "codigo_barras": "7802100430007",
            "variant_id": 31296,
            "unidades": 12,
            "source_detail_id": 88001,
            "line_number": 1,
            "sin_unidad_caja": True,
            "units_per_box": None,
        },
        load_group=None,
    )
    assert p["category_name"] == "CERVEZAS"
    assert p["product_name"] == "ROYAL GUARD"
    assert p["variant_name"] == "RETORNABLE 1.0 LT (SEC 12)"
    assert p["commercial_name"] == "ROYAL GUARD — RETORNABLE 1.0 LT (SEC 12)"
    assert p["codigo_barras"] == "7802100430007"
    assert p["barcode"] == "7802100430007"
    assert p["source_detail_id"] == 88001


def test_product_payload_fallback_without_category():
    p = svc._product_payload(
        {
            "producto_variante": "SOLO VARIANTE",
            "codigo_barras": None,
            "unidades": 1,
            "source_detail_id": 9,
            "sin_unidad_caja": True,
        },
        load_group=None,
    )
    assert p["category_name"] is None
    assert p["product_name"] is None
    assert p["variant_name"] == "SOLO VARIANTE"
    assert p["commercial_name"] == "SOLO VARIANTE"
    assert p["codigo_barras"] is None


def test_document_detail_lines_sql_uses_bsale_id_joins():
    """Regresión: no usar v.id/p.id (columnas inexistentes en bsale sync)."""
    import inspect

    src = inspect.getsource(svc._document_detail_lines)
    assert "v.bsale_id" in src or "VARIANTS_JOIN" in src
    assert "v.id = dd.variant_id" not in src
    assert "p.id = v.product_id" not in src
