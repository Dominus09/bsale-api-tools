"""
Identificación del cliente del catálogo web en ``bsale.clients``.

Un mismo RUT puede existir en varias empresas (y repetido dentro de una). El login y
los pedidos deben resolver siempre la misma fila para que la ciudad —y por lo tanto la
lista de precios— sea consistente: primero la empresa del catálogo (3), luego la fila
cuyo ``bsale_id`` coincide con el de la sesión, luego por empresa y ``bsale_id``.
"""

from __future__ import annotations

from dataclasses import dataclass

CATALOG_COMPANY_ID = 3


@dataclass(frozen=True)
class CatalogClient:
    bsale_id: int
    company_id: int
    first_name: str | None
    last_name: str | None
    city: str | None
    seller_id: int | None
    seller_name: str | None

    @property
    def name(self) -> str:
        return f"{self.first_name or ''} {self.last_name or ''}".strip()


_SELECT = """
    SELECT
        c.bsale_id,
        c.company_id,
        c.first_name,
        c.last_name,
        NULLIF(BTRIM(c.city), '') AS city,
        va.id AS seller_id,
        COALESCE(
            NULLIF(BTRIM(va.nombre), ''),
            NULLIF(BTRIM(c.vendedor::text), '')
        ) AS seller_name
    FROM bsale.clients c
    LEFT JOIN bsale.vendedores_app va
        ON LOWER(TRIM(COALESCE(va.codigo, '')))
         = LOWER(TRIM(COALESCE(c.vendedor::text, '')))
       AND va.activo IS TRUE
"""


def _row_to_client(row) -> CatalogClient:
    bsale_id, company_id, first_name, last_name, city, seller_id, seller_name = row
    is_catalog_company = int(company_id) == CATALOG_COMPANY_ID
    return CatalogClient(
        bsale_id=int(bsale_id),
        company_id=int(company_id),
        first_name=first_name,
        last_name=last_name,
        city=(str(city).strip() if city else None) or None,
        # El vendedor asignado solo aplica a la ficha de la empresa del catálogo.
        seller_id=int(seller_id) if is_catalog_company and seller_id is not None else None,
        seller_name=(
            (str(seller_name).strip() or None)
            if is_catalog_company and seller_name
            else None
        ),
    )


def lookup_catalog_client(
    cur,
    *,
    rut_clean: str | None,
    client_id: int | None = None,
) -> CatalogClient | None:
    """
    Con RUT: busca en todas las empresas priorizando la del catálogo.
    Sin RUT (cuerpo legacy): solo por ``bsale_id`` dentro de la empresa del catálogo.
    """
    if rut_clean:
        cur.execute(
            _SELECT
            + """
            WHERE c.rut_clean = %s
            ORDER BY
                (c.company_id = %s) DESC,
                (%s::integer IS NOT NULL AND c.bsale_id = %s::integer) DESC,
                c.company_id,
                c.bsale_id
            LIMIT 1
            """,
            (rut_clean, CATALOG_COMPANY_ID, client_id, client_id),
        )
    elif client_id is not None:
        cur.execute(
            _SELECT
            + """
            WHERE c.company_id = %s
              AND c.bsale_id = %s
            LIMIT 1
            """,
            (CATALOG_COMPANY_ID, client_id),
        )
    else:
        return None
    row = cur.fetchone()
    return _row_to_client(row) if row else None
