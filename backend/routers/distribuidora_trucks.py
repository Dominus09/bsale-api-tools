"""Camiones Distribuidora (pre-planificación / rutas)."""

from __future__ import annotations

from fastapi import APIRouter

from backend.db import get_connection

router = APIRouter(prefix="/distribuidora", tags=["Distribuidora camiones"])


@router.get("/trucks")
def get_trucks():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, name, plate, max_weight_kg
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
        return {"items": rows}
    finally:
        conn.close()
