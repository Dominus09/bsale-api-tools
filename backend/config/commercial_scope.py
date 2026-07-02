"""Scope fijo del módulo Comercial Vendedores — La Quillotana."""

from __future__ import annotations

from typing import Any

COMPANY_ID = 3
OFFICE_ID = 1
COMPANY_NAME = "La Quillotana Spa"
OFFICE_NAME = "Bodega Central"

DOC_BOLETA = 1
DOC_FACTURA = 6
DOC_NC = 9

ALLOWED_DOCUMENT_TYPES: tuple[int, ...] = (DOC_BOLETA, DOC_FACTURA, DOC_NC)
SALE_DOCUMENT_TYPES: tuple[int, ...] = (DOC_BOLETA, DOC_FACTURA)

ACTIVE_SELLERS: tuple[dict[str, Any], ...] = (
    {"id": 89, "name": "Ventas Ruta Ancud"},
    {"id": 80, "name": "Cristofer Saldivia"},
    {"id": 85, "name": "Álvaro Vargas"},
    {"id": 59, "name": "Vendedor Vacacionista"},
)

ACTIVE_SELLER_IDS: tuple[int, ...] = tuple(int(s["id"]) for s in ACTIVE_SELLERS)
ACTIVE_SELLER_NAMES: tuple[str, ...] = tuple(str(s["name"]) for s in ACTIVE_SELLERS)
SELLER_ID_BY_NAME: dict[str, int] = {str(s["name"]): int(s["id"]) for s in ACTIVE_SELLERS}
SELLER_NAME_BY_ID: dict[int, str] = {int(s["id"]): str(s["name"]) for s in ACTIVE_SELLERS}

ENGINE_VERSION = "commercial-engine-1.0.0"
SALES_SCOPE_VERSION = "commercial-scope-1.0.0"


def resolve_document_types(document_type: str | None) -> tuple[int, ...]:
    """Tipos incluidos en sales_base según filtro de documento."""
    doc = (document_type or "all").lower().strip()
    if doc == "factura":
        return (DOC_FACTURA,)
    if doc == "boleta":
        return (DOC_BOLETA,)
    return ALLOWED_DOCUMENT_TYPES


def resolve_seller_ids(seller_name: str | None) -> tuple[int, ...]:
    """IDs de vendedores activos; uno solo si hay filtro por nombre."""
    if seller_name and str(seller_name).strip():
        sid = SELLER_ID_BY_NAME.get(str(seller_name).strip())
        if sid is not None:
            return (sid,)
    return ACTIVE_SELLER_IDS


def analysis_scope_payload() -> dict[str, Any]:
    return {
        "company": COMPANY_NAME,
        "office": OFFICE_NAME,
        "seller_count": len(ACTIVE_SELLERS),
        "document_types": list(ALLOWED_DOCUMENT_TYPES),
        "credit_notes_discount_sales": True,
        "active_sellers": list(ACTIVE_SELLER_NAMES),
    }


def validation_scope_payload() -> dict[str, Any]:
    return {
        "company_id": COMPANY_ID,
        "company_name": COMPANY_NAME,
        "office_id": OFFICE_ID,
        "office_name": OFFICE_NAME,
        "active_sellers": [
            {"id": int(s["id"]), "name": str(s["name"])} for s in ACTIVE_SELLERS
        ],
    }


def profile_sales_where(
    *,
    client_id: int | None = None,
    seller_name: str | None = None,
    document_type: str | None = None,
    alias: str = "v",
) -> tuple[str, list[Any]]:
    """Cláusula WHERE reutilizable para fichas cliente/vendedor."""
    seller_ids = resolve_seller_ids(seller_name)
    doc_types = resolve_document_types(document_type)
    parts = [f"{alias}.seller_id IN %s", f"{alias}.document_type_id IN %s"]
    params: list[Any] = [seller_ids, doc_types]
    if client_id is not None:
        parts.append(f"{alias}.client_id = %s")
        params.append(int(client_id))
    return " AND ".join(parts), params


def filter_options_payload() -> dict[str, Any]:
    return {
        "sellers": list(ACTIVE_SELLER_NAMES),
        "cities": [],
        "document_types": [
            {"id": "all", "label": "Ventas netas (F + B − NC)"},
            {"id": "factura", "label": "Factura", "document_type_id": DOC_FACTURA},
            {"id": "boleta", "label": "Boleta", "document_type_id": DOC_BOLETA},
        ],
    }


# Expresiones SQL reutilizables (document_details con NC como reversa)
DD_SIGNED_QTY = (
    "CASE WHEN sb.document_type_id = 9 THEN -ABS(dd.quantity) ELSE dd.quantity END"
)
DD_SIGNED_AMOUNT = (
    "CASE WHEN sb.document_type_id = 9 THEN -ABS(dd.total_amount) ELSE dd.total_amount END"
)
