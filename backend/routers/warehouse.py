"""API Inteligencia de Bodega — layout (slices 1.1–1.4). Prefijo /warehouse."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from backend.auth.permissions import require_management_access
from backend.services.warehouse.errors import (
    WarehouseConflictError,
    WarehouseError,
    WarehouseNotFoundError,
    WarehouseValidationError,
)
from backend.services.warehouse.service import WarehouseLayoutService, default_service
from backend.services.warehouse.pallet_models import PalletRegisterIn, PalletLocationIn, PalletVoidIn
from backend.services.warehouse.pallets import default_pallet_service, PalletSchemaUnavailable

router = APIRouter(prefix="/warehouse", tags=["inteligencia-bodega"])


def _svc() -> WarehouseLayoutService:
    return default_service()


def _actor(user: dict[str, Any]) -> tuple[str | None, int | None]:
    uid = user.get("id")
    try:
        uid_i = int(uid) if uid is not None else None
    except (TypeError, ValueError):
        uid_i = None
    label = user.get("email") or user.get("name") or (f"user:{uid_i}" if uid_i else None)
    return (str(label) if label else None), uid_i


def _http(exc: WarehouseError) -> HTTPException:
    status = 400
    if isinstance(exc, WarehouseNotFoundError):
        status = 404
    elif isinstance(exc, WarehouseConflictError):
        status = 409
    elif isinstance(exc, WarehouseValidationError):
        status = 400
    return HTTPException(
        status_code=status,
        detail={"message": str(exc), "code": getattr(exc, "code", "warehouse_error")},
    )


class GeometryIn(BaseModel):
    x_m: float = 0
    y_m: float = 0
    elevation_m: float = 0
    width_m: float = Field(..., gt=0)
    depth_m: float = Field(..., gt=0)
    height_clear_m: float | None = Field(default=None, gt=0)
    rotation_deg: float = 0
    geom_type: str = "rect"
    polygon_m: Any | None = None
    stack_levels_max: int | None = Field(default=None, gt=0)
    validation_status: str = "pending_validation"
    constraints_json: dict[str, Any] = Field(default_factory=dict)
    observations: str | None = None
    meta_json: dict[str, Any] = Field(default_factory=dict)


class ElementUpsertIn(BaseModel):
    physical_element_id: int | None = None
    parent_id: int | None = None
    element_type: str
    code: str
    name: str
    meta_json: dict[str, Any] = Field(default_factory=dict)
    geometry: GeometryIn
    observations: str | None = None


class BatchIn(BaseModel):
    expected_row_version: int | None = None
    upserts: list[ElementUpsertIn] = Field(default_factory=list)
    retire_physical_ids: list[int] = Field(default_factory=list)
    remove_geometry_physical_ids: list[int] = Field(default_factory=list)


class DraftCreateIn(BaseModel):
    label: str | None = None


class GeneratePreviewIn(BaseModel):
    """Vista previa del generador. No persiste."""

    parent: dict[str, Any]
    parent_physical_id: int | None = None
    row_axis: str | None = None
    auto_row_gap: bool = False
    pallet_length_m: float = 1.26
    pallet_width_m: float = 1.06
    preset: str | None = "custom"
    row_counts: list[int] = Field(default_factory=list)
    row_orientations: list[str] = Field(default_factory=list)
    default_orientation: str = "standard"
    gap_longitudinal_m: float = 0
    gap_transverse_m: float = 0
    margin_start_m: float = 0
    margin_end_m: float = 0
    margin_near_m: float = 0
    margin_far_m: float = 0
    anchor: str = "start"
    grow_rows: str = "near_to_far"
    code_prefix: str | None = None
    code_suffix_mode: str = "numeric"
    start_index: int = 1
    height_clear_m: float | None = None
    constraints_json: dict[str, Any] = Field(default_factory=dict)
    observations: str | None = None
    allow_outside_parent: bool = False
    floor_length_m: float | None = None
    floor_width_m: float | None = None
    existing_elements: list[dict[str, Any]] = Field(default_factory=list)
    obstacles: list[dict[str, Any]] = Field(default_factory=list)


class ValidateIn(BaseModel):
    """Validación geométrica. Si elements se omite, usa la revisión persistida."""

    elements: list[dict[str, Any]] | None = None


class PublishIn(BaseModel):
    expected_row_version: int
    label: str | None = None


class DraftFromRevisionIn(BaseModel):
    label: str | None = None


@router.get("/meta/coordinate-convention")
def warehouse_coordinate_convention(_user: dict = Depends(require_management_access)):
    return _svc().coordinate_convention()


@router.post("/meta/generate-positions/preview")
def warehouse_generate_preview(
    body: GeneratePreviewIn,
    _user: dict = Depends(require_management_access),
):
    """Genera preview de posiciones. Nunca escribe en BD."""
    try:
        return _svc().preview_generate_positions(body.model_dump())
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/revisions/{revision_id}/validate")
def warehouse_validate_revision(
    revision_id: int,
    body: ValidateIn | None = None,
    _user: dict = Depends(require_management_access),
):
    try:
        return _svc().validate_revision_elements(
            revision_id,
            elements_override=(body.elements if body else None),
        )
    except WarehouseError as e:
        raise _http(e) from e


@router.get("/facilities")
def warehouse_list_facilities(_user: dict = Depends(require_management_access)):
    return {"items": _svc().list_facilities()}


@router.get("/facilities/{facility_id}/floors")
def warehouse_list_floors(facility_id: int, _user: dict = Depends(require_management_access)):
    try:
        return {"items": _svc().list_floors(facility_id)}
    except WarehouseError as e:
        raise _http(e) from e


@router.get("/floors/{floor_id}/revisions")
def warehouse_list_revisions(floor_id: int, _user: dict = Depends(require_management_access)):
    try:
        return {"items": _svc().list_revisions(floor_id)}
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/floors/{floor_id}/draft")
def warehouse_get_or_create_draft(
    floor_id: int,
    body: DraftCreateIn | None = None,
    user: dict = Depends(require_management_access),
):
    actor, uid = _actor(user)
    try:
        return _svc().get_or_create_draft(
            floor_id,
            label=(body.label if body else None),
            actor=actor,
            actor_user_id=uid,
        )
    except WarehouseError as e:
        raise _http(e) from e


@router.get("/revisions/{revision_id}")
def warehouse_get_revision(revision_id: int, _user: dict = Depends(require_management_access)):
    try:
        return _svc().get_revision(revision_id)
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/revisions/{revision_id}/elements/batch")
def warehouse_batch(
    revision_id: int,
    body: BatchIn,
    user: dict = Depends(require_management_access),
):
    """Guardado atómico. Ausentes en payload NO se eliminan."""
    actor, uid = _actor(user)
    upserts = []
    for u in body.upserts:
        upserts.append(
            {
                "physical_element_id": u.physical_element_id,
                "parent_id": u.parent_id,
                "element_type": u.element_type,
                "code": u.code,
                "name": u.name,
                "meta_json": u.meta_json,
                "observations": u.observations,
                "geometry": u.geometry.model_dump(),
            }
        )
    try:
        return _svc().apply_batch(
            revision_id,
            upserts=upserts,
            retire_physical_ids=body.retire_physical_ids,
            remove_geometry_physical_ids=body.remove_geometry_physical_ids,
            actor=actor,
            actor_user_id=uid,
            expected_row_version=body.expected_row_version,
        )
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/revisions/{revision_id}/elements")
def warehouse_create_element(
    revision_id: int,
    body: ElementUpsertIn,
    user: dict = Depends(require_management_access),
):
    actor, uid = _actor(user)
    try:
        return _svc().create_element(
            revision_id,
            payload={
                "physical_element_id": body.physical_element_id,
                "parent_id": body.parent_id,
                "element_type": body.element_type,
                "code": body.code,
                "name": body.name,
                "meta_json": body.meta_json,
                "observations": body.observations,
                "geometry": body.geometry.model_dump(),
            },
            actor=actor,
            actor_user_id=uid,
        )
    except WarehouseError as e:
        raise _http(e) from e


@router.get("/revisions/{revision_id}/audit")
def warehouse_audit(
    revision_id: int,
    limit: int = 100,
    _user: dict = Depends(require_management_access),
):
    try:
        return {"items": _svc().list_audit(revision_id, limit=min(max(limit, 1), 500))}
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/revisions/{revision_id}/evaluate-publish")
def warehouse_evaluate_publish(
    revision_id: int,
    body: ValidateIn | None = None,
    _user: dict = Depends(require_management_access),
):
    """Pre-chequeo de publicación. No escribe."""
    try:
        return _svc().evaluate_publish(
            revision_id,
            elements_override=(body.elements if body else None),
        )
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/revisions/{revision_id}/publish")
def warehouse_publish(
    revision_id: int,
    body: PublishIn,
    user: dict = Depends(require_management_access),
):
    """Publica draft → published (atómico). Archiva published previa del piso."""
    actor, uid = _actor(user)
    try:
        return _svc().publish_revision(
            revision_id,
            expected_row_version=body.expected_row_version,
            actor=actor,
            actor_user_id=uid,
            label=body.label,
        )
    except WarehouseError as e:
        raise _http(e) from e


@router.post("/revisions/{revision_id}/create-draft")
def warehouse_create_draft_from_revision(
    revision_id: int,
    body: DraftFromRevisionIn | None = None,
    user: dict = Depends(require_management_access),
):
    """Nuevo borrador desde published/archived. No modifica el histórico."""
    actor, uid = _actor(user)
    try:
        return _svc().create_draft_from_revision(
            revision_id,
            label=(body.label if body else None),
            actor=actor,
            actor_user_id=uid,
        )
    except WarehouseError as e:
        raise _http(e) from e


# Fase 2A — registro físico; mismas credenciales gerenciales ERP que layout.


def _pallet_call(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except PalletSchemaUnavailable as e:
        raise HTTPException(status_code=503, detail={"message":str(e),"code":e.code}) from e
    except WarehouseError as e:
        raise _http(e) from e


@router.get("/pallets/locations")
def pallet_locations(floor_id: int = Query(gt=0), _user: dict = Depends(require_management_access)):
    return _pallet_call(default_pallet_service().locations,floor_id)


@router.get("/pallets/products")
def pallet_products(floor_id: int = Query(gt=0), q: str = Query(default="",max_length=120),
                    _user: dict = Depends(require_management_access)):
    return _pallet_call(default_pallet_service().search_products,floor_id,q)


@router.get("/pallets")
def pallet_list(floor_id: int = Query(gt=0), q: str = Query(default="",max_length=120),
                limit: int = Query(default=50,ge=1,le=100), offset: int = Query(default=0,ge=0,le=100000),
                _user: dict = Depends(require_management_access)):
    return _pallet_call(default_pallet_service().list,floor_id,query=q,limit=limit,offset=offset)


@router.post("/pallets", status_code=201)
def pallet_register(body: PalletRegisterIn, user: dict = Depends(require_management_access)):
    actor,uid = _actor(user)
    return _pallet_call(default_pallet_service().register,body.model_dump(mode="json"),actor=actor or "ERP",uid=uid)


@router.get("/pallets/{pallet_id}")
def pallet_get(pallet_id: int, _user: dict = Depends(require_management_access)):
    return _pallet_call(default_pallet_service().get,pallet_id)


@router.post("/pallets/{pallet_id}/observe-location")
def pallet_observe_location(pallet_id: int, body: PalletLocationIn, user: dict = Depends(require_management_access)):
    actor,uid = _actor(user)
    return _pallet_call(default_pallet_service().observe_location,pallet_id,body.model_dump(mode="json"),actor=actor or "ERP",uid=uid)


@router.post("/pallets/{pallet_id}/void")
def pallet_void(pallet_id: int, body: PalletVoidIn, user: dict = Depends(require_management_access)):
    actor,uid = _actor(user)
    return _pallet_call(default_pallet_service().void,pallet_id,body.model_dump(mode="json"),actor=actor or "ERP",uid=uid)
