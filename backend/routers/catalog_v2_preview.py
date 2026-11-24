"""Preview interno Catálogo V2 — no reemplaza GET /api/catalog."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.auth.permissions import require_management_access
from backend.services.catalog_v2 import (
    CatalogV2ProjectionError,
    catalog_source_status,
    compare_catalog_sample,
    list_catalog_v2,
    raw_sync_integrity_report,
    upb_coverage_report,
    upb_missing_priority_report,
)

router = APIRouter(prefix="/catalog-v2", tags=["catalog-v2-preview"])


def _http_err(exc: CatalogV2ProjectionError) -> HTTPException:
    return HTTPException(
        status_code=400,
        detail={"message": str(exc), "code": getattr(exc, "code", "catalog_v2_error")},
    )


@router.get("/preview")
def catalog_v2_preview(
    price_list: str = Query(..., description="factura | melinka | id lista"),
    in_stock: bool | None = Query(default=None),
    office_id: int = Query(default=1, ge=1, le=20),
    limit: int | None = Query(default=100, ge=1, le=5000),
    q: str | None = Query(default=None, max_length=120),
    _user: dict = Depends(require_management_access),
):
    """Listado proyección V2 (RAW + PM). Solo gerencia/OWNER. Preview."""
    try:
        return list_catalog_v2(
            price_list=price_list,
            in_stock=in_stock,
            office_id=office_id,
            limit=limit,
            q=q,
        )
    except CatalogV2ProjectionError as e:
        raise _http_err(e) from e
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={
                "message": f"No se pudo proyectar catálogo V2: {e}",
                "code": "catalog_v2_unavailable",
            },
        ) from e


@router.get("/compare")
def catalog_v2_compare(
    price_list: str = Query(default="factura"),
    q: str | None = Query(
        default=None,
        description="Términos separados por | p.ej. harina|sidra|sol",
    ),
    variant_ids: str | None = Query(
        default=None, description="IDs Bsale separados por coma"
    ),
    limit: int = Query(default=30, ge=1, le=80),
    _user: dict = Depends(require_management_access),
):
    """Compara muestra V2 vs legacy catalog_view."""
    terms = [t.strip() for t in (q or "").split("|") if t.strip()]
    ids: list[int] = []
    if variant_ids:
        for part in variant_ids.split(","):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))
    try:
        return compare_catalog_sample(
            price_list=price_list,
            variant_ids=ids or None,
            search_terms=terms or None,
            limit=limit,
        )
    except CatalogV2ProjectionError as e:
        raise _http_err(e) from e
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"message": str(e), "code": "catalog_v2_compare_failed"},
        ) from e


@router.get("/integrity/raw-sync")
def catalog_v2_raw_integrity(_user: dict = Depends(require_management_access)):
    """SUCCESS con last_error residual / atraso (solo lectura sync_state)."""
    try:
        return raw_sync_integrity_report()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"message": str(e), "code": "raw_integrity_unavailable"},
        ) from e


@router.get("/coverage/upb")
def catalog_v2_upb_coverage(_user: dict = Depends(require_management_access)):
    try:
        return upb_coverage_report()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"message": str(e), "code": "upb_coverage_unavailable"},
        ) from e


@router.get("/coverage/upb-missing")
def catalog_v2_upb_missing(
    limit: int = Query(default=100, ge=1, le=500),
    _user: dict = Depends(require_management_access),
):
    """Faltantes UPB prioritarios (no deduce xN desde el nombre)."""
    try:
        return upb_missing_priority_report(limit=limit)
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail={"message": str(e), "code": "upb_missing_unavailable"},
        ) from e


@router.get("/source-flag")
def catalog_v2_source_flag(_user: dict = Depends(require_management_access)):
    """Estado del feature flag reversible (no activa nada por sí solo)."""
    return catalog_source_status()
