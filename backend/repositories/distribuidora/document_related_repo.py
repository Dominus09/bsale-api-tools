"""
Acceso a ``document_related`` y candidatos huérfanos (related sin header en ``documents``).

Las consultas viven aquí para que jobs/servicios las invoquen vía Python; no ejecutar SQL
ad-hoc desde herramientas externas.
"""

from __future__ import annotations

from typing import Any

ORPHAN_RELATED_DOCUMENT_TYPES = frozenset({1, 6, 9})

_ORPHAN_CANDIDATES_SQL = """
SELECT
    dr.related_document_id,
    dr.related_document_type,
    COUNT(*)::int AS reference_count,
    array_agg(DISTINCT oc.document_id ORDER BY oc.document_id) AS oc_document_ids,
    array_agg(DISTINCT oc.number ORDER BY oc.number) AS oc_numbers,
    array_agg(DISTINCT dr.detail_id ORDER BY dr.detail_id) AS origin_detail_ids,
    MIN(oc.emission_date) AS earliest_oc_emission
FROM distribuidora.document_related dr
LEFT JOIN distribuidora.documents d
    ON d.document_id = dr.related_document_id
INNER JOIN distribuidora.document_details dd
    ON dd.detail_id = dr.detail_id
INNER JOIN distribuidora.documents oc
    ON oc.document_id = dd.document_id
   AND oc.document_type_id = 33
WHERE dr.related_document_type = ANY(%s)
  AND d.document_id IS NULL
  AND oc.company_id = %s
  AND oc.office_id = %s
  {related_filter}
GROUP BY dr.related_document_id, dr.related_document_type
ORDER BY dr.related_document_id ASC
LIMIT %s OFFSET %s
"""

_CN_LINKS_FOR_INVOICE_SQL = """
SELECT DISTINCT
    nc.document_id AS nc_document_id,
    nc.number AS nc_number,
    nc.total_amount AS nc_total,
    COALESCE(nc.state, 0) AS nc_state,
    invd.detail_id AS invoice_detail_id,
    inv.document_id AS invoice_document_id,
    inv.number AS invoice_number,
    inv.document_type_id AS invoice_document_type_id,
    inv.total_amount AS invoice_total,
    EXISTS (
        SELECT 1
        FROM distribuidora.document_related dr
        WHERE dr.detail_id = invd.detail_id
          AND dr.related_document_id = nc.document_id
    ) AS already_in_document_related
FROM distribuidora.documents nc
INNER JOIN distribuidora.document_details ncd
    ON ncd.document_id = nc.document_id
   AND ncd.related_detail_id IS NOT NULL
INNER JOIN distribuidora.document_details invd
    ON invd.detail_id = ncd.related_detail_id
INNER JOIN distribuidora.documents inv
    ON inv.document_id = invd.document_id
   AND inv.document_type_id IN (1, 6)
WHERE nc.company_id = %s
  AND nc.office_id = %s
  AND nc.document_type_id = 9
  AND inv.document_id = %s
ORDER BY nc.number DESC NULLS LAST
"""


def fetch_orphan_related_document_candidates(
    cur,
    *,
    company_id: int,
    office_id: int,
    limit: int = 500,
    offset: int = 0,
    related_document_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """
    Relaciones ``document_related`` tipo 1/6/9 cuyo ``related_document_id`` no tiene fila en
    ``documents``. Agrupado por related id (paginado).
    """
    related_filter = ""
    extra_params: tuple[Any, ...] = ()
    if related_document_ids:
        related_filter = "AND dr.related_document_id = ANY(%s)"
        extra_params = (list({int(x) for x in related_document_ids}),)

    sql = _ORPHAN_CANDIDATES_SQL.format(related_filter=related_filter)
    params: tuple[Any, ...] = (
        list(ORPHAN_RELATED_DOCUMENT_TYPES),
        company_id,
        office_id,
        *extra_params,
        int(limit),
        int(offset),
    )
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def document_header_exists(cur, document_id: int) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM distribuidora.documents
        WHERE document_id = %s
        LIMIT 1
        """,
        (int(document_id),),
    )
    return cur.fetchone() is not None


def fetch_credit_note_links_for_invoice(
    cur,
    *,
    company_id: int,
    office_id: int,
    invoice_document_id: int,
) -> list[dict[str, Any]]:
    """NC ya materializadas en PG vinculadas a una factura/boleta por ``related_detail_id``."""
    cur.execute(
        _CN_LINKS_FOR_INVOICE_SQL,
        (company_id, office_id, int(invoice_document_id)),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]
