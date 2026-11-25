"""Geometría métrica con rotación (slice 1.3).

Rectángulos definidos por esquina SW (x_m, y_m), width_m (+X), depth_m (+Y)
y rotación ° CCW respecto del centro. Usado por generador y validación.
"""

from __future__ import annotations

import math
from typing import Any, Iterable


def rect_corners(
    *,
    x_m: float,
    y_m: float,
    width_m: float,
    depth_m: float,
    rotation_deg: float = 0.0,
) -> list[tuple[float, float]]:
    """Esquinas en orden, rotadas CCW alrededor del centro."""
    cx = x_m + width_m / 2.0
    cy = y_m + depth_m / 2.0
    local = (
        (-width_m / 2.0, -depth_m / 2.0),
        (width_m / 2.0, -depth_m / 2.0),
        (width_m / 2.0, depth_m / 2.0),
        (-width_m / 2.0, depth_m / 2.0),
    )
    rad = math.radians(rotation_deg or 0.0)
    cos_a, sin_a = math.cos(rad), math.sin(rad)
    out: list[tuple[float, float]] = []
    for lx, ly in local:
        out.append((cx + lx * cos_a - ly * sin_a, cy + lx * sin_a + ly * cos_a))
    return out


def _axes(poly: list[tuple[float, float]]) -> list[tuple[float, float]]:
    axes: list[tuple[float, float]] = []
    n = len(poly)
    for i in range(n):
        x1, y1 = poly[i]
        x2, y2 = poly[(i + 1) % n]
        edge_x, edge_y = x2 - x1, y2 - y1
        # Normal
        nx, ny = -edge_y, edge_x
        norm = math.hypot(nx, ny) or 1.0
        axes.append((nx / norm, ny / norm))
    return axes


def _project(poly: list[tuple[float, float]], axis: tuple[float, float]) -> tuple[float, float]:
    ax, ay = axis
    dots = [p[0] * ax + p[1] * ay for p in poly]
    return min(dots), max(dots)


def polygons_overlap(a: list[tuple[float, float]], b: list[tuple[float, float]], *, eps: float = 1e-9) -> bool:
    """SAT: True si los polígonos convexos se solapan (área > 0)."""
    for poly in (a, b):
        for axis in _axes(poly):
            amin, amax = _project(a, axis)
            bmin, bmax = _project(b, axis)
            if amax <= bmin + eps or bmax <= amin + eps:
                return False
    return True


def rects_overlap(ra: dict[str, Any], rb: dict[str, Any], *, eps: float = 1e-9) -> bool:
    return polygons_overlap(
        rect_corners(
            x_m=float(ra["x_m"]),
            y_m=float(ra["y_m"]),
            width_m=float(ra["width_m"]),
            depth_m=float(ra["depth_m"]),
            rotation_deg=float(ra.get("rotation_deg") or 0),
        ),
        rect_corners(
            x_m=float(rb["x_m"]),
            y_m=float(rb["y_m"]),
            width_m=float(rb["width_m"]),
            depth_m=float(rb["depth_m"]),
            rotation_deg=float(rb.get("rotation_deg") or 0),
        ),
        eps=eps,
    )


def point_in_rect(px: float, py: float, rect: dict[str, Any], *, eps: float = 1e-9) -> bool:
    """Punto dentro del rect rotado (incluye borde), en marco local del rect."""
    x_m = float(rect["x_m"])
    y_m = float(rect["y_m"])
    width_m = float(rect["width_m"])
    depth_m = float(rect["depth_m"])
    cx = x_m + width_m / 2.0
    cy = y_m + depth_m / 2.0
    rad = math.radians(-(float(rect.get("rotation_deg") or 0)))
    cos_a, sin_a = math.cos(rad), math.sin(rad)
    dx, dy = px - cx, py - cy
    lx = dx * cos_a - dy * sin_a
    ly = dx * sin_a + dy * cos_a
    return abs(lx) <= width_m / 2.0 + eps and abs(ly) <= depth_m / 2.0 + eps


def rect_contained_in(inner: dict[str, Any], outer: dict[str, Any], *, eps: float = 1e-6) -> bool:
    """True si las 4 esquinas de inner están dentro de outer."""
    for px, py in rect_corners(
        x_m=float(inner["x_m"]),
        y_m=float(inner["y_m"]),
        width_m=float(inner["width_m"]),
        depth_m=float(inner["depth_m"]),
        rotation_deg=float(inner.get("rotation_deg") or 0),
    ):
        if not point_in_rect(px, py, outer, eps=eps):
            return False
    return True


def rect_outside_bounds(
    rect: dict[str, Any],
    *,
    length_m: float,
    width_m: float,
    eps: float = 1e-6,
) -> bool:
    """True si alguna esquina sale del perímetro [0,length]×[0,width]."""
    for px, py in rect_corners(
        x_m=float(rect["x_m"]),
        y_m=float(rect["y_m"]),
        width_m=float(rect["width_m"]),
        depth_m=float(rect["depth_m"]),
        rotation_deg=float(rect.get("rotation_deg") or 0),
    ):
        if px < -eps or py < -eps or px > length_m + eps or py > width_m + eps:
            return True
    return False


def footprint_area_m2(rects: Iterable[dict[str, Any]]) -> float:
    return sum(float(r["width_m"]) * float(r["depth_m"]) for r in rects)
