"""API Cargas — importación Excel/PDF y certificación física mobile-first."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel, Field

from backend.services.cargas import service as cargas_svc
from backend.services.cargas.parse_excel import PickingParseError
from backend.utils.auth_staff import require_staff_user

router = APIRouter(prefix="/cargas", tags=["Cargas"])


def _email(user: dict) -> str:
    return str(user.get("email") or "").strip()


class AddUnitsBody(BaseModel):
    boxes: float = Field(0, ge=0)
    loose_units: float = Field(0)
    allow_excess: bool = False
    notes: str | None = Field(None, max_length=500)
    complete_remaining: bool = False


class IssueBody(BaseModel):
    issue_type: str = Field(
        ...,
        pattern=r"^(not_found|insufficient_stock|wrong_product|damaged|excess|picking_error|other)$",
    )
    description: str | None = Field(None, max_length=1000)


class ResolveIssueBody(BaseModel):
    issue_id: int | None = None


class ReopenBody(BaseModel):
    notes: str | None = Field(None, max_length=500)


@router.post("/import-preview")
async def import_preview(
    file: UploadFile = File(...),
    user: dict = Depends(require_staff_user),
):
    _ = user
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Archivo vacío")
    try:
        return cargas_svc.preview_import(data=raw, filename=file.filename or "picking")
    except PickingParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/import")
async def import_confirm(
    file: UploadFile = File(...),
    picking_number: str | None = Form(None),
    user: dict = Depends(require_staff_user),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Archivo vacío")
    try:
        return cargas_svc.confirm_import(
            data=raw,
            filename=file.filename or "picking",
            user_email=_email(user),
            picking_number_override=picking_number,
        )
    except PickingParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("")
def list_cargas(
    status: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    user: dict = Depends(require_staff_user),
):
    _ = user
    try:
        return cargas_svc.list_loads(limit=limit, status=status)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{load_id}")
def get_carga(
    load_id: int,
    user: dict = Depends(require_staff_user),
):
    _ = user
    try:
        return cargas_svc.get_load(load_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{load_id}/items")
def list_items(
    load_id: int,
    status: str | None = Query(None),
    product_type: str | None = Query(None),
    user: dict = Depends(require_staff_user),
):
    _ = user
    try:
        return cargas_svc.search_items(
            load_id, q="", status=status, product_type=product_type
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/{load_id}/items/search")
def search_items(
    load_id: int,
    q: str = Query("", max_length=120),
    status: str | None = Query(None),
    product_type: str | None = Query(None),
    user: dict = Depends(require_staff_user),
):
    _ = user
    try:
        return cargas_svc.search_items(
            load_id, q=q, status=status, product_type=product_type
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{load_id}/start")
def start_carga(
    load_id: int,
    user: dict = Depends(require_staff_user),
):
    try:
        return cargas_svc.start_loading(load_id, user_email=_email(user))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{load_id}/items/{item_id}/add")
def add_units(
    load_id: int,
    item_id: int,
    body: AddUnitsBody,
    user: dict = Depends(require_staff_user),
):
    try:
        return cargas_svc.add_units(
            load_id=load_id,
            item_id=item_id,
            user_email=_email(user),
            boxes=body.boxes,
            loose_units=body.loose_units,
            allow_excess=body.allow_excess,
            notes=body.notes,
            complete_remaining=body.complete_remaining,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{load_id}/items/{item_id}/issue")
def report_issue(
    load_id: int,
    item_id: int,
    body: IssueBody,
    user: dict = Depends(require_staff_user),
):
    try:
        return cargas_svc.report_issue(
            load_id=load_id,
            item_id=item_id,
            user_email=_email(user),
            issue_type=body.issue_type,
            description=body.description,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{load_id}/items/{item_id}/resolve")
def resolve_issue(
    load_id: int,
    item_id: int,
    body: ResolveIssueBody,
    user: dict = Depends(require_staff_user),
):
    try:
        return cargas_svc.resolve_issue(
            load_id=load_id,
            item_id=item_id,
            user_email=_email(user),
            issue_id=body.issue_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{load_id}/certify")
def certify(
    load_id: int,
    user: dict = Depends(require_staff_user),
):
    try:
        return cargas_svc.certify_load(load_id=load_id, user_email=_email(user))
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/{load_id}/reopen")
def reopen(
    load_id: int,
    body: ReopenBody,
    user: dict = Depends(require_staff_user),
):
    try:
        return cargas_svc.reopen_load(
            load_id=load_id, user_email=_email(user), notes=body.notes
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
