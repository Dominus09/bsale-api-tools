"""Generador avanzado de posiciones (slice 1.3). Solo calcula; no escribe BD."""

from __future__ import annotations

from typing import Any
import math

from backend.services.warehouse.geometry import footprint_area_m2, rect_outside_bounds, rects_overlap

STANDARD_PALLET_M = {"length": 1.26, "width": 1.06}

# Presets: lista de conteos por fila (fila 0 = ancla según grow).
PRESETS: dict[str, list[int]] = {
    "4+4": [4, 4],
    "4+5": [4, 5],
    "5+5": [5, 5],
    "5+6": [5, 6],
    "6+6": [6, 6],
    "custom": [],
}

# Referencias geométricas documentadas (pasillo 2.80×6.00, sin márgenes).
REFERENCE_CALCS = [
    {
        "id": "transverse_1.06_gap",
        "note_es": (
            "Pallets ancho transversal 1,06 m y profundidad 1,26 m: "
            "dos filas en 2,80 m dejan separación nominal 0,68 m."
        ),
        "aisle_width_m": 2.80,
        "aisle_depth_m": 6.00,
        "pallet_transverse_m": 1.06,
        "pallet_longitudinal_m": 1.26,
        "row_gap_m": 0.68,
    },
    {
        "id": "4+5_extension_0.30",
        "note_es": (
            "Configuración 4+5 con esa orientación: extensión 0,30 m "
            "si las filas se anclan al fondo (5×1,26=6,30)."
        ),
        "extension_m": 0.30,
    },
    {
        "id": "transverse_1.26_gap",
        "note_es": (
            "Pallets girados ancho transversal 1,26 m y profundidad 1,06 m: "
            "dos filas dejan separación nominal 0,28 m."
        ),
        "row_gap_m": 0.28,
    },
    {
        "id": "5+5_fits",
        "note_es": "Configuración 5+5 con esa orientación: cabe nominalmente dentro de 6 m.",
        "fits": True,
    },
    {
        "id": "6+6_extension_0.36",
        "note_es": "Configuración 6+6 con esa orientación: extensión nominal 0,36 m.",
        "extension_m": 0.36,
    },
]


def resolve_row_counts(
    *,
    preset: str | None,
    row_counts: list[int] | None,
) -> list[int]:
    key = (preset or "custom").strip()
    if key != "custom" and key in PRESETS and PRESETS[key]:
        return list(PRESETS[key])
    counts = list(row_counts or [])
    if not counts or any(not isinstance(c, int) or isinstance(c, bool) or c <= 0 for c in counts):
        raise ValueError("Indique conteos enteros positivos por fila")
    return counts


def _pallet_dims_for_row(
    *,
    base_length: float,
    base_width: float,
    orientation: str,
) -> tuple[float, float]:
    """Devuelve (along_longitudinal / Y del padre, along_transverse / X del padre).

    Convención generador sobre un sector/pasillo alineado a ejes:
    - Eje longitudinal del padre = +Y (depth_m del padre) → posiciones en fila crecen en Y.
    - Eje transversal = +X (width_m del padre) → filas paralelas crecen en X.
    orientation 'standard': pallet length (1.26) en Y, width (1.06) en X.
    orientation 'rotated': pallet width (1.06) en Y, length (1.26) en X.
    """
    if orientation == "rotated":
        return base_width, base_length  # along Y, along X
    return base_length, base_width


def generate_positions(config: dict[str, Any]) -> dict[str, Any]:
    """Genera geometrías candidatas. No persiste.

    Config principal:
    - parent: {x_m,y_m,width_m,depth_m,rotation_deg,code,element_type}
    - pallet_length_m / pallet_width_m (default 1.26/1.06)
    - row_axis: 'x' (largo de bodega/IZ) o 'y' (pasillo derecho, por defecto)
    - auto_row_gap: repartir espacio transversal restante entre filas
    - preset | row_counts
    - row_orientations: lista 'standard'|'rotated' por fila (default standard)
    - gap_longitudinal_m / gap_transverse_m
    - margin_start_m / margin_end_m / margin_near_m / margin_far_m
      (start/end en eje de filas; near/far en el otro eje, coordenadas bajas/altas)
    - anchor: 'start'|'end' — default 'start'; 'end' = anclar al extremo alto
    - grow_rows: 'near_to_far' | 'far_to_near' (eje transversal)
    - code_prefix, code_suffix_mode: 'numeric' | 'iz_da' (D/A)
    - start_index, height_clear_m, constraints_json, observations
    - allow_outside_parent: bool
    """
    parent = config.get("parent") or {}
    px = float(parent["x_m"])
    py = float(parent["y_m"])
    width_x = float(parent["width_m"])
    depth_y = float(parent["depth_m"])
    prot = float(parent.get("rotation_deg") or 0)
    if not all(math.isfinite(v) for v in (px, py, width_x, depth_y, prot)) or min(width_x, depth_y) <= 0:
        raise ValueError("Geometría del sector inválida")
    if abs(prot % 360) > 1e-6 and abs(prot % 360 - 360) > 1e-6:
        raise ValueError("El generador requiere un sector alineado a los ejes. Ajuste su rotación primero.")

    # Mantener la semántica histórica de P1-P7: filas en +Y.
    # IZ recorre el largo de bodega (+X); sus filas paralelas se separan en Y.
    axis = config.get("row_axis") or ("x" if parent.get("code") == "IZ" else "y")
    if axis not in ("x", "y"):
        raise ValueError("Eje de filas inválido")
    pw, pd = (depth_y, width_x) if axis == "x" else (width_x, depth_y)
    pallet_l = float(config.get("pallet_length_m", STANDARD_PALLET_M["length"]))
    pallet_w = float(config.get("pallet_width_m", STANDARD_PALLET_M["width"]))
    if not all(math.isfinite(v) and v > 0 for v in (pallet_l, pallet_w)):
        raise ValueError("Dimensiones de pallet deben ser finitas y > 0")

    row_counts = resolve_row_counts(
        preset=config.get("preset"),
        row_counts=config.get("row_counts"),
    )
    n_rows = len(row_counts)
    if n_rows > 50 or sum(row_counts) > 500:
        raise ValueError("Máximo 50 filas y 500 posiciones por generación")
    orients = list(config.get("row_orientations") or [])
    while len(orients) < n_rows:
        orients.append(config.get("default_orientation") or "standard")
    if len(orients) != n_rows or any(o not in ("standard", "rotated") for o in orients):
        raise ValueError("Orientaciones por fila inválidas")

    gap_long = float(config.get("gap_longitudinal_m") or 0)
    gap_trans = float(config.get("gap_transverse_m") or 0)
    m_start = float(config.get("margin_start_m") or 0)
    m_end = float(config.get("margin_end_m") or 0)
    m_near = float(config.get("margin_near_m") or 0)
    m_far = float(config.get("margin_far_m") or 0)

    if not all(math.isfinite(v) and v >= 0 for v in (gap_long, gap_trans, m_start, m_end, m_near, m_far)):
        raise ValueError("Separaciones y márgenes deben ser finitos y no negativos")
    if m_start + m_end >= pd or m_near + m_far >= pw:
        raise ValueError("Los márgenes consumen todo el sector")
    anchor = (config.get("anchor") or "start").strip()
    grow_rows = (config.get("grow_rows") or ("far_to_near" if axis == "x" else "near_to_far")).strip()
    if anchor not in ("start", "end") or grow_rows not in ("near_to_far", "far_to_near"):
        raise ValueError("Anclaje o dirección inválidos")
    prefix = (config.get("code_prefix") or parent.get("code") or "POS").strip()
    suffix_mode = (config.get("code_suffix_mode") or "numeric").strip()
    start_index = int(config.get("start_index") or 1)
    if start_index < 1 or not prefix or suffix_mode not in ("numeric", "iz_da"):
        raise ValueError("Nomenclatura inválida")
    if suffix_mode == "iz_da" and (axis != "x" or n_rows > 2 or len(set(orients)) > 1):
        raise ValueError("D/A requiere eje X, una o dos filas y la misma orientación de pallet")
    height_clear = config.get("height_clear_m")
    if height_clear is not None and (not math.isfinite(float(height_clear)) or float(height_clear) <= 0):
        raise ValueError("Altura libre debe ser positiva o desconocida")
    constraints = dict(config.get("constraints_json") or {})
    observations = config.get("observations") or (
        "Generada por editor — pending_validation. No autoriza apilamiento ni invasión de circulación."
    )
    allow_outside = bool(config.get("allow_outside_parent"))

    # Dimensiones por fila (along_y, along_x)
    row_dims: list[tuple[float, float]] = [
        _pallet_dims_for_row(
            base_length=pallet_l, base_width=pallet_w, orientation=orients[i]
        )
        for i in range(n_rows)
    ]

    # Distribuir las filas en el eje transversal elegido.
    # La opción automática reparte el espacio sobrante entre filas, sin certificar acceso.
    if config.get("auto_row_gap") and n_rows > 1:
        gap_trans = max(0.0, (pw - m_near - m_far - sum(d[1] for d in row_dims)) / (n_rows - 1))
    row_x_offsets: list[float] = []
    if grow_rows == "far_to_near":
        cursor = pw - m_far
        for _, across in row_dims:
            cursor -= across
            row_x_offsets.append(cursor)
            cursor -= gap_trans
    else:
        cursor = m_near
        for _, across in row_dims:
            row_x_offsets.append(cursor)
            cursor += across + gap_trans

    positions: list[dict[str, Any]] = []
    idx = start_index
    for ri, count in enumerate(row_counts):
        along_y, along_x = row_dims[ri]
        row_span = count * along_y + gap_long * max(0, count - 1)
        if anchor == "end":
            y0 = pd - m_end - row_span
        else:
            y0 = m_start
        for ci in range(count):
            local_x = row_x_offsets[ri]
            local_y = y0 + ci * (along_y + gap_long)
            # Coordenadas persistidas: width=+X, depth=+Y.
            if axis == "x":
                gx, gy = px + local_y, py + local_x
                width_m, depth_m = along_y, along_x
            else:
                gx, gy = px + local_x, py + local_y
                width_m, depth_m = along_x, along_y
            if suffix_mode == "iz_da":
                # D es la fila de mayor Y (cerca de circulación), A la de menor Y.
                side = ("D" if ri == 0 else "A") if grow_rows == "far_to_near" else ("A" if ri == 0 else "D")
                # Numeración desde entrada; al anclar al fondo, alinear índices por tramo.
                pair_idx = start_index + ci + (max(row_counts) - count if anchor == "end" else 0)
                code = f"{prefix}-{pair_idx:02d}-{side}"
                name = f"Posición {prefix} {pair_idx:02d} {'delante' if side == 'D' else 'atrás'}"
            else:
                code = f"{prefix}-{idx:02d}"
                name = f"Posición {code}"
            positions.append(
                {
                    "code": code,
                    "name": name,
                    "element_type": "position",
                    "parent_code": parent.get("code"),
                    "row_index": ri,
                    "slot_index": ci,
                    "geometry": {
                        "x_m": round(gx, 4),
                        "y_m": round(gy, 4),
                        "width_m": round(width_m, 4),
                        "depth_m": round(depth_m, 4),
                        "rotation_deg": 0.0,
                        "elevation_m": 0.0,
                        "height_clear_m": float(height_clear) if height_clear is not None else None,
                        "validation_status": "pending_validation",
                        "constraints_json": {
                            **constraints,
                            "generated": True,
                            "storage_kind": "pallet_standard",
                            "stacking_authorized": False,
                        },
                        "observations": observations,
                        "meta_json": {
                            "provisional": True,
                            "generator_preset": config.get("preset") or "custom",
                            "row_orientation": orients[ri],
                            "row_axis": axis,
                        },
                        "stack_levels_max": None,
                    },
                }
            )
            idx += 1

    # Métricas de preview
    occupied = footprint_area_m2(p["geometry"] for p in positions)
    # Extensión fuera del padre (eje Y / X)
    max_y = max((p["geometry"]["y_m"] + p["geometry"]["depth_m"] for p in positions), default=py)
    min_y = min((p["geometry"]["y_m"] for p in positions), default=py)
    max_x = max((p["geometry"]["x_m"] + p["geometry"]["width_m"] for p in positions), default=px)
    min_x = min((p["geometry"]["x_m"] for p in positions), default=px)
    extension_y_m = max(0.0, max_y - (py + depth_y), py - min_y)
    extension_x_m = max(0.0, max_x - (px + width_x), px - min_x)
    extension_m = max(extension_y_m, extension_x_m)

    # Separación interior entre filas (si 2+) y holgura transversal residual en el padre
    interior_gap_m = None
    if n_rows >= 2 and positions:
        interior_gap_m = round(gap_trans, 4)
    rows_span_x = sum(d[1] for d in row_dims) + gap_trans * max(0, n_rows - 1)
    remaining_transverse_m = round(pw - m_near - m_far - rows_span_x, 4)

    warnings: list[dict[str, Any]] = []
    if extension_m > 1e-6:
        warnings.append(
            {
                "severity": "warning",
                "code": "extends_outside_parent",
                "message": (
                    f"Extensión fuera del límite nominal del sector/pasillo: "
                    f"{extension_m:.2f} m (no autoriza invasión de circulación)."
                ),
            }
        )
    if not allow_outside and extension_m > 1e-6:
        warnings.append(
            {
                "severity": "warning",
                "code": "outside_not_auto_authorized",
                "message": "Los 60 cm observados no constituyen permiso automático.",
            }
        )

    return {
        "preview": True,
        "persists": False,
        "preset": config.get("preset") or "custom",
        "row_axis": axis,
        "position_count": len(positions),
        "occupied_area_m2": round(occupied, 4),
        "interior_gap_m": interior_gap_m,
        "remaining_transverse_m": remaining_transverse_m,
        "extension_outside_parent_m": round(extension_m, 4),
        "positions": positions,
        "warnings": warnings,
        "reference_calcs": REFERENCE_CALCS,
        "note_es": (
            "Vista previa local al borrador. No escribe en BD hasta Guardar borrador. "
            "Cálculos geométricos de referencia; no capacidades seguras autorizadas. "
            "No autoriza apilamiento."
        ),
    }


def verify_reference_calcs() -> list[dict[str, Any]]:
    """Comprueba las referencias §4 del prompt (sin márgenes)."""
    results = []
    # 1) gap transversal 0.68
    gap = 2.80 - 2 * 1.06
    results.append({"id": "transverse_1.06_gap", "ok": abs(gap - 0.68) < 1e-9, "value": gap})
    # 2) 4+5 extension
    ext = 5 * 1.26 - 6.0
    results.append({"id": "4+5_extension_0.30", "ok": abs(ext - 0.30) < 1e-9, "value": ext})
    # 3) rotated gap 0.28
    gap2 = 2.80 - 2 * 1.26
    results.append({"id": "transverse_1.26_gap", "ok": abs(gap2 - 0.28) < 1e-9, "value": gap2})
    # 4) 5+5 fits
    span = 5 * 1.06
    results.append({"id": "5+5_fits", "ok": span <= 6.0 + 1e-9, "value": span})
    # 5) 6+6 extension
    ext2 = 6 * 1.06 - 6.0
    results.append({"id": "6+6_extension_0.36", "ok": abs(ext2 - 0.36) < 1e-9, "value": ext2})
    return results
