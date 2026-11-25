"""Validación geométrica avanzada (slice 1.3)."""

from __future__ import annotations

from typing import Any

from backend.services.warehouse.geometry import (
    rect_contained_in,
    rect_outside_bounds,
    rects_overlap,
)

STORAGE_TYPES = frozenset({"position", "loose_storage"})
STRUCTURE_TYPES = frozenset({"wall", "pillar", "obstacle", "beam", "stair", "gate", "door", "office"})
CONTAINER_TYPES = frozenset({"facility_boundary", "sector", "aisle", "circulation"})


def _geom(el: dict[str, Any]) -> dict[str, Any]:
    g = el.get("geometry") or el
    return {
        "x_m": float(g["x_m"]),
        "y_m": float(g["y_m"]),
        "width_m": float(g["width_m"]),
        "depth_m": float(g["depth_m"]),
        "rotation_deg": float(g.get("rotation_deg") or 0),
    }


def _phys(el: dict[str, Any]) -> dict[str, Any]:
    return el.get("physical") or el


def _type(el: dict[str, Any]) -> str:
    p = _phys(el)
    return str(p.get("element_type") or el.get("element_type") or "")


def _code(el: dict[str, Any]) -> str:
    p = _phys(el)
    return str(p.get("code") or el.get("code") or "")


def _id(el: dict[str, Any]) -> int | None:
    p = _phys(el)
    if p.get("id") is not None:
        return int(p["id"])
    if el.get("physical_element_id") is not None:
        return int(el["physical_element_id"])
    return None


def _parent_id(el: dict[str, Any]) -> int | None:
    p = _phys(el)
    pid = p.get("parent_id")
    return int(pid) if pid is not None else None


def validate_layout(
    elements: list[dict[str, Any]],
    *,
    floor_length_m: float | None,
    floor_width_m: float | None,
) -> dict[str, Any]:
    """Clasifica: integrity_error | geom_conflict | warning | pending_validation."""
    issues: list[dict[str, Any]] = []
    active = [
        e
        for e in elements
        if not e.get("pendingRetire") and not e.get("pendingRemoveGeometry")
    ]

    # Códigos duplicados / dims
    seen_codes: dict[str, str] = {}
    by_id: dict[int, dict[str, Any]] = {}
    for e in active:
        code = _code(e).strip()
        key = code.lower()
        cid = str(_id(e) or e.get("clientKey") or code)
        if not code:
            issues.append(
                {
                    "severity": "integrity_error",
                    "code": "missing_code",
                    "message": "Código obligatorio",
                    "element_code": code,
                    "element_ref": cid,
                }
            )
        elif key in seen_codes:
            issues.append(
                {
                    "severity": "integrity_error",
                    "code": "duplicate_code",
                    "message": f"Código duplicado en el piso: {code}",
                    "element_code": code,
                    "element_ref": cid,
                }
            )
        else:
            seen_codes[key] = cid

        g = _geom(e)
        if g["width_m"] <= 0 or g["depth_m"] <= 0:
            issues.append(
                {
                    "severity": "integrity_error",
                    "code": "invalid_dimensions",
                    "message": f"{code}: dimensiones deben ser > 0",
                    "element_code": code,
                    "element_ref": cid,
                }
            )
        pid = _id(e)
        if pid is not None and pid > 0:
            by_id[pid] = e

        parent = _parent_id(e)
        if parent is not None and parent > 0 and parent not in by_id:
            # parent puede aparecer más tarde; se revalida abajo
            pass

        vs = (e.get("geometry") or {}).get("validation_status") or e.get("validation_status")
        if vs == "pending_validation":
            issues.append(
                {
                    "severity": "pending_validation",
                    "code": "pending_validation",
                    "message": f"{code}: geometría pendiente de validación en terreno",
                    "element_code": code,
                    "element_ref": cid,
                }
            )

    # Reindex parents after full pass
    by_id = {}
    for e in active:
        pid = _id(e)
        if pid is not None and pid > 0:
            by_id[pid] = e

    for e in active:
        parent = _parent_id(e)
        code = _code(e)
        cid = str(_id(e) or e.get("clientKey") or code)
        if parent is not None and parent > 0 and parent not in by_id:
            issues.append(
                {
                    "severity": "integrity_error",
                    "code": "invalid_parent",
                    "message": f"{code}: parent_id={parent} no está en la revisión",
                    "element_code": code,
                    "element_ref": cid,
                }
            )

    # Perímetro
    if floor_length_m and floor_width_m:
        for e in active:
            if _type(e) == "facility_boundary":
                continue
            if rect_outside_bounds(
                _geom(e), length_m=float(floor_length_m), width_m=float(floor_width_m)
            ):
                issues.append(
                    {
                        "severity": "geom_conflict",
                        "code": "outside_facility",
                        "message": f"{_code(e)}: fuera del perímetro de la instalación",
                        "element_code": _code(e),
                        "element_ref": str(_id(e) or e.get("clientKey")),
                    }
                )

    # Contención en sector/pasillo padre
    for e in active:
        if _type(e) not in STORAGE_TYPES:
            continue
        parent = _parent_id(e)
        if parent is None or parent not in by_id:
            continue
        p = by_id[parent]
        if _type(p) not in ("sector", "aisle", "loose_storage"):
            continue
        if not rect_contained_in(_geom(e), _geom(p)):
            issues.append(
                {
                    "severity": "warning",
                    "code": "outside_parent_sector",
                    "message": (
                        f"{_code(e)}: fuera del límite nominal de {_code(p)} "
                        "(puede ser extensión deliberada; no autoriza circulación)."
                    ),
                    "element_code": _code(e),
                    "element_ref": str(_id(e) or e.get("clientKey")),
                }
            )

    # Conflictos físicos
    for i in range(len(active)):
        for j in range(i + 1, len(active)):
            a, b = active[i], active[j]
            ta, tb = _type(a), _type(b)
            ida, idb = _id(a), _id(b)

            # Jerarquía válida: contenedor ⊇ contenido
            if ta in CONTAINER_TYPES and ida is not None and _parent_id(b) == ida:
                continue
            if tb in CONTAINER_TYPES and idb is not None and _parent_id(a) == idb:
                continue
            if ta == "facility_boundary" or tb == "facility_boundary":
                continue
            # Sector/aisle overlapping circulation is expected for layout framing — warn only if both storage
            if not rects_overlap(_geom(a), _geom(b)):
                continue

            # Posición dentro de sector/pasillo (aunque parent_id no coincida) → no colisión
            if ta in STORAGE_TYPES and tb in ("sector", "aisle"):
                continue
            if tb in STORAGE_TYPES and ta in ("sector", "aisle"):
                continue
            if ta in ("sector", "aisle", "circulation") and tb in ("sector", "aisle", "circulation"):
                # Marcos de layout suelen compartir bordes; no marcar conflicto duro
                continue

            # Storage vs storage
            if ta in STORAGE_TYPES and tb in STORAGE_TYPES:
                issues.append(
                    {
                        "severity": "geom_conflict",
                        "code": "storage_overlap",
                        "message": f"Superposición entre {_code(a)} y {_code(b)}",
                        "element_code": _code(a),
                        "element_ref": str(ida or a.get("clientKey")),
                        "other_code": _code(b),
                    }
                )
                continue

            # Storage vs structure / stair / circulation
            if ta in STORAGE_TYPES and tb in STRUCTURE_TYPES | {"circulation", "unusable"}:
                sev = "geom_conflict" if tb in STRUCTURE_TYPES | {"stair", "unusable"} else "warning"
                code = "structure_collision" if tb in STRUCTURE_TYPES | {"stair"} else "circulation_invasion"
                issues.append(
                    {
                        "severity": sev,
                        "code": code,
                        "message": f"{_code(a)} interfiere con {_code(b)} ({tb})",
                        "element_code": _code(a),
                        "element_ref": str(ida or a.get("clientKey")),
                        "other_code": _code(b),
                    }
                )
                continue
            if tb in STORAGE_TYPES and ta in STRUCTURE_TYPES | {"circulation", "unusable"}:
                sev = "geom_conflict" if ta in STRUCTURE_TYPES | {"stair", "unusable"} else "warning"
                code = "structure_collision" if ta in STRUCTURE_TYPES | {"stair"} else "circulation_invasion"
                issues.append(
                    {
                        "severity": sev,
                        "code": code,
                        "message": f"{_code(b)} interfiere con {_code(a)} ({ta})",
                        "element_code": _code(b),
                        "element_ref": str(idb or b.get("clientKey")),
                        "other_code": _code(a),
                    }
                )

    # Acceso escalera: storage overlapping stair clearance already covered; add note
    return {
        "issues": issues,
        "counts": {
            "integrity_error": sum(1 for i in issues if i["severity"] == "integrity_error"),
            "geom_conflict": sum(1 for i in issues if i["severity"] == "geom_conflict"),
            "warning": sum(1 for i in issues if i["severity"] == "warning"),
            "pending_validation": sum(1 for i in issues if i["severity"] == "pending_validation"),
        },
        "note_es": (
            "Guardar un borrador con conflictos documenta el estado actual; "
            "no autoriza condiciones inseguras ni apilamiento."
        ),
    }
