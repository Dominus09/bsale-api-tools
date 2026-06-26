"""Cálculo de peso por línea de OC y clasificación de estado."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any


def _f(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        value = float(value)
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n == n else None


def _round3(value: float) -> float:
    return round(value, 3)


def _round6(value: float) -> float:
    return round(value, 6)


def cantidad_cajas(cantidad: float, units_per_box: int | None) -> float | None:
    if units_per_box is None or units_per_box <= 0:
        return None
    return round(cantidad / units_per_box, 4)


def classify_fuente_peso(
    *,
    peso_unitario: float | None,
    pm_id: int | None,
    pm_updated_at: datetime | None,
    last_bsale_sync_at: datetime | None,
    join_variant_ok: bool,
    join_barcode_ok: bool,
) -> str:
    if peso_unitario is None or peso_unitario <= 0:
        return "sin_datos"
    if pm_id is None:
        return "sin_datos"
    if pm_updated_at and last_bsale_sync_at and pm_updated_at > last_bsale_sync_at:
        return "manual"
    if join_barcode_ok and not join_variant_ok:
        return "erp"
    return "erp"


def classify_estado_linea(
    *,
    fuente: str,
    peso_unitario: float | None,
    logistics_completed: bool | None,
    pm_id: int | None,
    variant_id: int | None,
) -> str:
    if fuente == "sin_datos" or peso_unitario is None or peso_unitario <= 0:
        return "sin_peso"
    if fuente == "manual":
        return "manual"
    if fuente == "estimado":
        return "estimado"
    if pm_id and variant_id and logistics_completed:
        return "completo"
    if pm_id and peso_unitario > 0:
        return "completo"
    return "sin_peso"


def build_line_diagnosis(row: dict[str, Any]) -> dict[str, Any]:
    peso = _f(row.get("peso_unitario_kg"))
    return {
        "existe_en_document_details": True,
        "existe_variant_id": row.get("variant_id") is not None,
        "existe_en_v_product_logistics": bool(
            row.get("join_variant_ok") or row.get("join_barcode_ok")
        ),
        "tiene_peso": peso is not None and peso > 0,
        "tiene_dimensiones": all(
            _f(row.get(k)) and _f(row.get(k)) > 0
            for k in ("height_cm", "width_cm", "length_cm")
        ),
        "tiene_cajas": (row.get("units_per_box") or 0) > 0,
        "tiene_empresa_correcta": row.get("exists_in_pm") is True,
        "join_correcto": bool(row.get("join_variant_ok")),
        "join_por_barcode": bool(row.get("join_barcode_ok") and not row.get("join_variant_ok")),
        "products_master_id": row.get("products_master_id"),
        "variant_id": row.get("variant_id"),
        "barcode": row.get("barcode"),
        "codigo_interno": row.get("codigo_interno"),
    }


def compute_line_from_row(row: dict[str, Any]) -> dict[str, Any]:
    qty = _f(row.get("cantidad_unitaria")) or 0.0
    upb = row.get("units_per_box")
    try:
        upb_int = int(upb) if upb is not None else None
    except (TypeError, ValueError):
        upb_int = None

    peso_unit = _f(row.get("peso_unitario_kg"))
    peso_caja = _f(row.get("peso_caja_kg"))
    if peso_caja is None and peso_unit is not None and upb_int and upb_int > 0:
        peso_caja = _round3(peso_unit * upb_int)

    join_v = bool(row.get("join_variant_ok"))
    join_b = bool(row.get("join_barcode_ok"))
    pm_id = row.get("products_master_id")

    fuente = classify_fuente_peso(
        peso_unitario=peso_unit,
        pm_id=int(pm_id) if pm_id is not None else None,
        pm_updated_at=row.get("pm_updated_at"),
        last_bsale_sync_at=row.get("last_bsale_sync_at"),
        join_variant_ok=join_v,
        join_barcode_ok=join_b,
    )
    estado = classify_estado_linea(
        fuente=fuente,
        peso_unitario=peso_unit,
        logistics_completed=row.get("logistics_completed"),
        pm_id=int(pm_id) if pm_id is not None else None,
        variant_id=row.get("variant_id"),
    )
    peso_linea = _round3(qty * peso_unit) if peso_unit and peso_unit > 0 else 0.0
    cajas = cantidad_cajas(qty, upb_int)

    diagnosis = build_line_diagnosis(row)
    producto, variante = split_producto_variante(
        line_description=row.get("producto"),
        product_name=row.get("product_name") or row.get("bsale_product_name"),
        variant_name=row.get("variante") or row.get("variant_name"),
    )
    return {
        "detail_id": int(row["detail_id"]),
        "line_number": row.get("line_number"),
        "codigo": row.get("codigo"),
        "producto": producto,
        "variante": variante,
        "cantidad_unitaria": qty,
        "cantidad_cajas": cajas,
        "units_per_box": upb_int,
        "peso_unitario_kg": _round6(peso_unit) if peso_unit else None,
        "peso_caja_kg": peso_caja,
        "peso_linea_kg": peso_linea,
        "fuente_peso": fuente,
        "estado_linea": estado,
        "products_master_id": pm_id,
        "variant_id": row.get("variant_id"),
        "join_debug": diagnosis,
        "has_logistics_record": pm_id is not None,
    }


def aggregate_order_summary(lines: list[dict[str, Any]]) -> dict[str, Any]:
    active = [ln for ln in lines if (ln.get("cantidad_unitaria") or 0) > 0]
    total = len(active)
    con_peso = sum(1 for ln in active if (ln.get("peso_unitario_kg") or 0) > 0)
    sin_peso = total - con_peso
    manuales = sum(1 for ln in active if ln.get("fuente_peso") == "manual")
    estimados = sum(1 for ln in active if ln.get("fuente_peso") == "estimado")
    completos = sum(1 for ln in active if ln.get("estado_linea") == "completo")
    peso_total = _round3(sum(ln.get("peso_linea_kg") or 0 for ln in active))
    cobertura = round(100.0 * con_peso / total, 1) if total > 0 else 0.0
    return {
        "productos_totales": total,
        "productos_con_peso": con_peso,
        "productos_completos": completos,
        "productos_sin_peso": sin_peso,
        "productos_manuales": manuales,
        "productos_estimados": estimados,
        "peso_total_kg": peso_total,
        "porcentaje_cobertura": cobertura,
    }


def coverage_semaphore(pct: float) -> str:
    if pct >= 100:
        return "verde"
    if pct >= 95:
        return "verde_claro"
    if pct >= 90:
        return "amarillo"
    if pct >= 80:
        return "naranja"
    return "rojo"


def _norm_label(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def split_producto_variante(
    *,
    line_description: Any = None,
    product_name: Any = None,
    variant_name: Any = None,
) -> tuple[str | None, str | None]:
    """Separa nombre de producto y variante sin repetir el mismo texto."""
    pn = _norm_label(product_name)
    vn = _norm_label(variant_name)
    dd = _norm_label(line_description)

    if not pn and dd:
        pn = dd
    if not vn and dd and dd.casefold() != pn.casefold():
        vn = dd

    if pn and vn:
        if pn.casefold() == vn.casefold():
            return pn, None
        if vn.casefold().startswith(pn.casefold()):
            rest = vn[len(pn) :].strip(" -–—:|")
            vn = rest if rest else None
        elif pn.casefold().startswith(vn.casefold()):
            pn, vn = vn, None

    return (pn or None), (vn or None)


def enrich_lines_peso_pct(
    lines: list[dict[str, Any]],
    peso_total_kg: float,
) -> list[dict[str, Any]]:
    total = peso_total_kg or 0.0
    for ln in lines:
        pl = float(ln.get("peso_linea_kg") or 0)
        ln["peso_pct_total"] = round(100.0 * pl / total, 1) if total > 0 else 0.0
    return lines
