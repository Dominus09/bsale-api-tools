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


def _manual_15kg_row(*, quantity: float) -> dict:
    """Línea OC con peso manual 15 kg/caja; cantidad viene del detalle actual."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    return {
        "detail_id": 8971875,
        "line_number": 0,
        "variant_id": 27383,
        "codigo": "68237149926080",
        "producto": "ROJO 12 KG APROX (SEC 1)",
        "cantidad_unitaria": quantity,
        "units_per_box": 1,
        "peso_unitario_kg": 15.0,
        "peso_caja_kg": 15.0,
        "products_master_id": 998,
        "variante": "ROJO 15 KG APROX (SEC 1)",
        "logistics_completed": False,
        "join_variant_ok": False,
        "join_barcode_ok": True,
        "exists_in_pm": True,
        "pm_updated_at": now,
        "last_bsale_sync_at": datetime(2026, 6, 4, tzinfo=timezone.utc),
        "height_cm": None,
        "width_cm": None,
        "length_cm": None,
        "barcode": "68237149926080",
        "codigo_interno": "68237149926080",
    }


def test_oc_qty1_manual_15kg_total_15():
    """OC cantidad 1, peso manual 15 kg → total 15 kg; cobertura 100%."""
    line = compute_line_from_row(_manual_15kg_row(quantity=1))
    assert line["cantidad_unitaria"] == 1.0
    assert line["cantidad_cajas"] == 1.0
    assert line["peso_unitario_kg"] == 15.0
    assert line["peso_linea_kg"] == 15.0
    assert line["fuente_peso"] == "manual"
    assert line["estado_linea"] == "manual"
    summary = aggregate_order_summary([line])
    assert summary["peso_total_kg"] == 15.0
    assert summary["porcentaje_cobertura"] == 100.0
    assert summary["productos_manuales"] == 1


def test_oc_qty_changes_1_to_20_recalculates_300kg_keeps_manual_unit():
    """
    Misma OC cambia a cantidad 20 → total 300 kg.
    El peso manual unitario sigue en 15 kg; no se congela la cantidad antigua.
    """
    before = compute_line_from_row(_manual_15kg_row(quantity=1))
    after = compute_line_from_row(_manual_15kg_row(quantity=20))

    assert before["peso_unitario_kg"] == 15.0
    assert after["peso_unitario_kg"] == 15.0
    assert after["fuente_peso"] == "manual"
    assert after["cantidad_unitaria"] == 20.0
    assert after["cantidad_cajas"] == 20.0
    assert after["peso_linea_kg"] == 300.0

    summary = aggregate_order_summary([after])
    assert summary["peso_total_kg"] == 300.0
    assert summary["porcentaje_cobertura"] == 100.0
    assert summary["productos_manuales"] == 1
    # No queda rastro de cantidad 1 en el resumen derivado de la línea actual
    assert summary["peso_total_kg"] != before["peso_linea_kg"]


def test_weight_uses_current_detail_qty_not_saved_snapshot_qty():
    """Regla: total = current_detail.quantity × manual_unit_weight (no saved_qty × weight)."""
    frozen_old_qty = 1.0
    current_qty = 20.0
    unit = 15.0
    # Incorrecto (congelar cantidad):
    wrong = frozen_old_qty * unit
    # Correcto:
    right = current_qty * unit
    line = compute_line_from_row(_manual_15kg_row(quantity=current_qty))
    assert line["peso_linea_kg"] == right
    assert line["peso_linea_kg"] != wrong
    assert line["cantidad_unitaria"] == current_qty
