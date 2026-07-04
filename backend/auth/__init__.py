"""Autenticación y autorización centralizada del ERP."""

from backend.auth.jwt import (
    ALGORITHM,
    SECRET,
    create_staff_token,
    decode_jwt_token,
    decode_staff_token,
)
from backend.auth.permissions import (
    ADMIN_ROLES,
    MANAGEMENT_ROLES,
    build_auth_me_response,
    has_admin_access,
    has_management_access,
    has_margin_view_access,
    has_operational_access,
    has_sales_access,
    require_management_access,
    require_admin_access,
)

__all__ = [
    "ALGORITHM",
    "SECRET",
    "ADMIN_ROLES",
    "MANAGEMENT_ROLES",
    "build_auth_me_response",
    "create_staff_token",
    "decode_jwt_token",
    "decode_staff_token",
    "has_admin_access",
    "has_management_access",
    "has_margin_view_access",
    "has_operational_access",
    "has_sales_access",
    "require_management_access",
    "require_admin_access",
]
