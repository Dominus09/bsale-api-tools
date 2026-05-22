"""Camiones Distribuidora (pre-planificación / rutas)."""

from __future__ import annotations

import logging

from fastapi import APIRouter

from backend.db import get_connection

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora camiones"])


@router.get("/trucks")
def get_trucks():
    """Lista camiones activos; ante error de BD devuelve lista vacía (evita 500)."""
    conn = None
    try:
        conn = get_connection()
    except Exception as e:
        logger.error("Error conectando a BD para trucks: %s", e, exc_info=True)
        return {"items": []}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, plate, max_weight_kg,
                   COALESCE(km_per_liter, 8.0) AS km_per_liter,
                   COALESCE(fuel_type, 'diesel') AS fuel_type
            FROM distribuidora.trucks
            WHERE active = TRUE
            ORDER BY name ASC
            """
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        for r in rows:
            v = r.get("max_weight_kg")
            if v is not None:
                r["max_weight_kg"] = int(v)
            rid = r.get("id")
            if rid is not None:
                r["id"] = int(rid)
            kpl = r.get("km_per_liter")
            if kpl is not None:
                r["km_per_liter"] = float(kpl)
        return {"items": rows}
    except Exception as e:
        logger.error("Error cargando trucks: %s", e, exc_info=True)
        return {"items": []}
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
