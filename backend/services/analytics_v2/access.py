"""Acceso a Analítica V2: permiso por módulo y empresa verificada en backend.

El selector de empresa del frontend no es una autorización: cada request valida que la empresa
exista, esté activa, esté dentro del alcance del token (claim ``company_ids`` si viene) y tenga
RAW completo. Nunca se lee ``bsale.companies.bsale_token``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import HTTPException, Request

from backend.auth.jwt import decode_staff_token
from backend.auth.permissions import (
    BearerDep,
    has_management_access,
    normalized_role,
    resolve_executive_permissions,
)
from backend.auth.staff_session import enrich_staff_payload
from backend.services.analytics_v2.contracts import RAW_READY_COMPANY_IDS

logger = logging.getLogger(__name__)

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

COMPANY_SQL = "SELECT company_id, name, active FROM bsale.companies WHERE company_id = %s"


class AnalyticsV2AccessError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True, slots=True)
class CompanyScope:
    company_id: int
    company_name: str


def _deny(user: dict[str, Any], path: str, permission: str, reason: str) -> None:
    logger.warning(
        "[AUTH] analytics_v2 user_id=%s role=%s endpoint=%s required_permission=%s timestamp=%s reason=%s",
        user.get("id"),
        user.get("role"),
        path,
        permission,
        datetime.now(timezone.utc).isoformat(),
        reason,
    )


def _require_permission(request: Request, authorization: str | None, permission: str, label: str) -> dict[str, Any]:
    user = enrich_staff_payload(decode_staff_token(authorization))
    if not resolve_executive_permissions(user).get(permission):
        _deny(user, request.url.path, permission, f"Rol sin permiso de {label}")
        raise HTTPException(status_code=403, detail=f"Acceso restringido: requiere permiso de {label}")
    return user


def require_cost_control_v2(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    """Dependencia FastAPI: Control de Costos V2 (permiso ejecutivo ``costs``)."""
    return _require_permission(request, authorization, "costs", "costos")


def require_margins_v2(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    """Dependencia FastAPI: Márgenes V2 (permiso ejecutivo ``margins``)."""
    return _require_permission(request, authorization, "margins", "márgenes")


# Promociones V2: permisos separados por acción. Aprobar y cancelar son exclusivos de gerencia y
# administración; "comercial" puede preparar campañas pero no aprobarlas; finanzas solo consulta.
PROMOTION_EDITOR_EXTRA_ROLES = frozenset({"comercial"})
PROMOTION_VIEWER_EXTRA_ROLES = frozenset({"finanzas", "finance"}) | PROMOTION_EDITOR_EXTRA_ROLES
PROMOTION_ACTIONS = ("view", "create", "edit", "approve", "cancel")


def promotion_capabilities(user: dict[str, Any] | None) -> dict[str, bool]:
    mgmt = has_management_access(user)
    role = normalized_role(user)
    editor = mgmt or role in PROMOTION_EDITOR_EXTRA_ROLES
    return {
        "view": mgmt or role in PROMOTION_VIEWER_EXTRA_ROLES,
        "create": editor,
        "edit": editor,
        "approve": mgmt,
        "cancel": mgmt,
    }


def _require_promotion_action(request: Request, authorization: str | None, action: str) -> dict[str, Any]:
    user = enrich_staff_payload(decode_staff_token(authorization))
    if not promotion_capabilities(user).get(action):
        _deny(user, request.url.path, f"promotions_{action}", f"Rol sin permiso promotions_{action}")
        raise HTTPException(status_code=403, detail="Acceso restringido: su rol no permite esta acción en Promociones V2")
    return user


def require_promotions_view(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    return _require_promotion_action(request, authorization, "view")


def require_promotions_create(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    return _require_promotion_action(request, authorization, "create")


def require_promotions_edit(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    return _require_promotion_action(request, authorization, "edit")


def require_promotions_approve(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    return _require_promotion_action(request, authorization, "approve")


def require_promotions_cancel(request: Request, authorization: BearerDep = None) -> dict[str, Any]:
    return _require_promotion_action(request, authorization, "cancel")


def token_company_ids(user: dict[str, Any]) -> frozenset[int] | None:
    """Empresas declaradas en el token; None = el token no restringe empresas."""
    raw = user.get("company_ids")
    if raw is None:
        return None
    if not isinstance(raw, (list, tuple, set, frozenset)):
        return frozenset()
    out: set[int] = set()
    for value in raw:
        try:
            out.add(int(value))
        except (TypeError, ValueError):
            continue
    return frozenset(out)


def resolve_company_scope(
    user: dict[str, Any], company_id: int, execute: QueryExecutor
) -> CompanyScope:
    allowed = token_company_ids(user)
    if allowed is not None and company_id not in allowed:
        raise AnalyticsV2AccessError(403, "Empresa fuera del alcance del usuario")
    rows = execute(COMPANY_SQL, (company_id,))
    if not rows:
        raise AnalyticsV2AccessError(404, "Empresa inexistente")
    row = rows[0]
    if row.get("active") is False:
        raise AnalyticsV2AccessError(403, "Empresa inactiva")
    if company_id not in RAW_READY_COMPANY_IDS:
        raise AnalyticsV2AccessError(
            409, "La empresa aún no tiene BSALE RAW completo; Analítica V2 no disponible"
        )
    return CompanyScope(company_id=int(row["company_id"]), company_name=str(row.get("name") or ""))
