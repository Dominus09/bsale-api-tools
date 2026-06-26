"""Tests cálculo peso por línea."""

from backend.utils.order_weight_calc import (
    aggregate_order_summary,
    cantidad_cajas,
    classify_estado_linea,
    classify_fuente_peso,
    compute_line_from_row,
    coverage_semaphore,
    enrich_lines_peso_pct,
    split_producto_variante,
)


def test_cantidad_cajas():
    assert cantidad_cajas(36, 12) == 3.0
    assert cantidad_cajas(30, 12) == 2.5
    assert cantidad_cajas(10, None) is None


def test_compute_line_peso():
    row = {
        "detail_id": 1,
        "line_number": 1,
        "variant_id": 100,
        "codigo": "780123",
        "producto": "Coca Cola 2L",
        "cantidad_unitaria": 24,
        "units_per_box": 12,
        "peso_unitario_kg": 2.08,
        "peso_caja_kg": 24.96,
        "products_master_id": 5,
        "variante": "2L",
        "logistics_completed": True,
        "join_variant_ok": True,
        "join_barcode_ok": False,
        "exists_in_pm": True,
        "pm_updated_at": None,
        "last_bsale_sync_at": None,
        "height_cm": 10,
        "width_cm": 10,
        "length_cm": 10,
        "barcode": "780123",
        "codigo_interno": "SKU1",
    }
    line = compute_line_from_row(row)
    assert line["cantidad_cajas"] == 2.0
    assert line["peso_linea_kg"] == 49.92
    assert line["estado_linea"] == "completo"
    assert line["fuente_peso"] == "erp"


def test_aggregate_and_semaphore():
    lines = [
        {"cantidad_unitaria": 1, "peso_unitario_kg": 1.0, "peso_linea_kg": 1.0, "fuente_peso": "erp"},
        {"cantidad_unitaria": 1, "peso_unitario_kg": 0, "peso_linea_kg": 0, "fuente_peso": "sin_datos"},
    ]
    summary = aggregate_order_summary(lines)
    assert summary["productos_totales"] == 2
    assert summary["productos_con_peso"] == 1
    assert summary["porcentaje_cobertura"] == 50.0
    assert coverage_semaphore(100) == "verde"
    assert coverage_semaphore(92) == "amarillo"
    assert coverage_semaphore(75) == "rojo"


def test_split_product_variant_no_dup():
    pn, vn = split_producto_variante(
        line_description="LATA 710 CC",
        product_name="COCA COLA",
        variant_name="LATA 710 CC",
    )
    assert pn == "COCA COLA"
    assert vn == "LATA 710 CC" or vn is None or vn != pn


def test_enrich_peso_pct():
    lines = [{"peso_linea_kg": 25.0}, {"peso_linea_kg": 75.0}]
    enrich_lines_peso_pct(lines, 100.0)
    assert lines[0]["peso_pct_total"] == 25.0
    assert lines[1]["peso_pct_total"] == 75.0


def test_manual_fuente():
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    assert (
        classify_fuente_peso(
            peso_unitario=1.0,
            pm_id=1,
            pm_updated_at=now,
            last_bsale_sync_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
            join_variant_ok=True,
            join_barcode_ok=False,
        )
        == "manual"
    )
    assert (
        classify_estado_linea(
            fuente="manual",
            peso_unitario=1.0,
            logistics_completed=False,
            pm_id=1,
            variant_id=1,
        )
        == "manual"
    )
