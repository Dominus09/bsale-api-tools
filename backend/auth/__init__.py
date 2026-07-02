"""Autenticación y autorización centralizada del ERP."""

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
)

__all__ = [
    "ADMIN_ROLES",
    "MANAGEMENT_ROLES",
    "build_auth_me_response",
    "has_admin_access",
    "has_management_access",
    "has_margin_view_access",
    "has_operational_access",
    "has_sales_access",
    "require_management_access",
]
