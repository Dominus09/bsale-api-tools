"""API listado y operaciones globales de cuadratura distribuidora."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from backend.services.distribuidora import dispatch_plan_cuadratura_service as svc
from backend.utils.ors_stability import log_error

router = APIRouter(prefix="/distribuidora/cuadraturas", tags=["Distribuidora cuadraturas"])


@router.get("")
def list_cuadraturas(
    status: str = Query(
        "all",
        description="all|pending|squared|difference|with_diff",
    ),
    search: str | None = Query(None, max_length=120),
    limit: int = Query(100, ge=1, le=300),
    offset: int = Query(0, ge=0),
):
    try:
        return svc.list_cuadraturas(
            status=status,
            search=search,
            limit=limit,
            offset=offset,
        )
    except Exception as exc:
        log_error("GET /distribuidora/cuadraturas", exc)
        raise HTTPException(status_code=500, detail="Error al listar cuadraturas") from exc
