"""Private, explicit OWNER-only system settings API."""
from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field, StrictBool
from backend.auth.hierarchy import is_owner
from backend.auth.multiapp import require_erp_app
from backend.services import system_feature_controls as controls

router = APIRouter(prefix="/admin/system-features", tags=["system-feature-controls"])


def require_owner(user: dict = Depends(require_erp_app)) -> dict:
    if not is_owner(user):
        raise HTTPException(403, "Configuración del sistema exclusiva de OWNER.")
    return user


class FeaturePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled_override: StrictBool | None
    expected_version: int = Field(..., strict=True, ge=0, le=9223372036854775807)


@router.get("")
def list_features(response: Response, user: dict = Depends(require_owner)):
    response.headers["Cache-Control"] = "no-store"
    return controls.get_state()


@router.patch("/{key}")
def update_feature(key: str, body: FeaturePatch, response: Response, user: dict = Depends(require_owner)):
    response.headers["Cache-Control"] = "no-store"
    actor = int(user["id"] if user.get("id") is not None else user["uid"])
    return controls.set_override(key, override=body.enabled_override,
                                 expected_version=body.expected_version, actor=actor)
