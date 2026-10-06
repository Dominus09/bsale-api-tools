"""Rol ERP de los tipos de documento Bsale (``distribuidora.document_type_roles``, 049).

Fuente canónica para preguntar "¿qué ids son facturas/boletas/NC/OC en esta empresa?" sin
números mágicos. La identidad es ``(company_id, document_type_id)``: el mismo id técnico
de Bsale puede tener otro significado en otra empresa.

El nombre visible del tipo NO vive aquí: se lee de ``bsale.document_types.name`` tal como
lo entrega Bsale (ver ``document_type_names``). Solo lectura; nunca hace DDL ni escribe.
"""

from __future__ import annotations

from enum import Enum
from typing import Iterable


class DocumentRole(str, Enum):
    BOLETA = "BOLETA"
    FACTURA = "FACTURA"
    GUIA_DESPACHO = "GUIA_DESPACHO"
    NOTA_CREDITO = "NOTA_CREDITO"
    COTIZACION = "COTIZACION"
    ORDEN_COMPRA = "ORDEN_COMPRA"


# Boleta o factura: "OC facturada". No confundir con FACTURA sola.
DOCUMENTOS_FACTURACION: tuple[DocumentRole, ...] = (DocumentRole.BOLETA, DocumentRole.FACTURA)
DOCUMENTOS_VENTA_NETA: tuple[DocumentRole, ...] = (
    DocumentRole.BOLETA,
    DocumentRole.FACTURA,
    DocumentRole.NOTA_CREDITO,
)
# Tipos que hoy pueden persistirse en ``document_related`` (sync, auditoría y cleanup
# solo admiten estos). GUIA_DESPACHO queda fuera hasta una fase que la incorpore.
DOCUMENTOS_RELACION_PERMITIDA: tuple[DocumentRole, ...] = (
    DocumentRole.BOLETA,
    DocumentRole.FACTURA,
    DocumentRole.NOTA_CREDITO,
)
# Evidencia para probar relaciones (p. ej. OC -> GUIA -> FACTURA en Bsale); la guía
# sirve como prueba, no como relación persistible.
DOCUMENTOS_EVIDENCIA_RELACION: tuple[DocumentRole, ...] = (
    DocumentRole.BOLETA,
    DocumentRole.FACTURA,
    DocumentRole.GUIA_DESPACHO,
    DocumentRole.NOTA_CREDITO,
)

ROLES_TABLE = "distribuidora.document_type_roles"

_ROLES_SQL = """
SELECT document_type_id, role, active
FROM distribuidora.document_type_roles
WHERE company_id = %s
ORDER BY document_type_id
"""

_NAMES_SQL = """
SELECT bsale_id, name
FROM bsale.document_types
WHERE company_id = %s AND bsale_id = ANY(%s)
"""

_CACHE: dict[int, tuple[tuple[int, DocumentRole, bool], ...]] = {}


class DocumentTypeRoleMissing(RuntimeError):
    """Falta la tabla de roles o un rol pedido no tiene tipo activo en la empresa."""


def clear_cache() -> None:
    _CACHE.clear()


def _coerce_roles(roles: Iterable[DocumentRole | str]) -> tuple[DocumentRole, ...]:
    out = tuple(DocumentRole(r) for r in roles)
    if not out:
        raise ValueError("Indique al menos un rol")
    return out


def _load(cur, company_id: int) -> tuple[tuple[int, DocumentRole, bool], ...]:
    key = int(company_id)
    cached = _CACHE.get(key)
    if cached is not None:
        return cached
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (ROLES_TABLE,))
    row = cur.fetchone()
    if not row or not row[0]:
        raise DocumentTypeRoleMissing(
            f"Falta {ROLES_TABLE} (049_document_type_roles.sql). "
            "Ejecutar: python -m backend.jobs.apply_distribuidora_schema"
        )
    cur.execute(_ROLES_SQL, (key,))
    rows = tuple(
        (int(type_id), DocumentRole(str(role)), bool(active))
        for type_id, role, active in (cur.fetchall() or [])
    )
    if rows:
        _CACHE[key] = rows
    return rows


def type_ids_for(
    cur,
    company_id: int,
    *roles: DocumentRole | str,
    include_inactive: bool = False,
) -> tuple[int, ...]:
    """Ids Bsale con alguno de ``roles`` en la empresa; falla si algún rol no tiene tipo."""
    wanted = _coerce_roles(roles)
    rows = [r for r in _load(cur, company_id) if include_inactive or r[2]]
    missing = [role.value for role in wanted if not any(r[1] is role for r in rows)]
    if missing:
        raise DocumentTypeRoleMissing(
            f"company_id={int(company_id)} sin tipo {'' if include_inactive else 'activo '}"
            f"para rol(es): {', '.join(missing)}"
        )
    return tuple(sorted({r[0] for r in rows if r[1] in wanted}))


def role_of(
    cur,
    company_id: int,
    document_type_id: int,
    *,
    include_inactive: bool = False,
) -> DocumentRole | None:
    for type_id, role, active in _load(cur, company_id):
        if type_id == int(document_type_id) and (active or include_inactive):
            return role
    return None


def has_role(cur, company_id: int, document_type_id: int, *roles: DocumentRole | str) -> bool:
    """Solo considera tipos activos."""
    wanted = _coerce_roles(roles)
    return role_of(cur, company_id, document_type_id) in wanted


def document_type_names(cur, company_id: int, document_type_ids: Iterable[int]) -> dict[int, str | None]:
    """Nombre visible tal como lo almacena Bsale (``bsale.document_types.name``), sin normalizar."""
    ids = sorted({int(i) for i in document_type_ids})
    if not ids:
        return {}
    cur.execute(_NAMES_SQL, (int(company_id), ids))
    return {int(bsale_id): name for bsale_id, name in (cur.fetchall() or [])}
