"""Adaptador Distribuidora: SQL puro + executor inyectable (Etapa 2A).

No abre conexiones por sí mismo. Los tests usan FakeQueryExecutor.
La validación contra PG real queda para Etapa 2B.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from backend.services.analytics.document_models import (
    AnalyticsDocumentHeader,
    AnalyticsDocumentLine,
    DocumentReconciliationResult,
    LineNetMethod,
    classify_document_kind,
    resolve_commercial_date,
)
from backend.services.analytics.document_source import (
    CREDIT_NOTE_DOCUMENT_TYPES,
    DOC_TYPE_BOLETA,
    DOC_TYPE_CREDIT_NOTE,
    DOC_TYPE_FACTURA,
    SALE_DOCUMENT_TYPES,
)
from backend.services.analytics.line_net import allocate_line_nets
from backend.services.analytics.money import optional_decimal
from backend.services.analytics.reconciliation import reconcile_header_vs_lines

QueryExecutor = Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
DEFAULT_STATEMENT_TIMEOUT = "30s"

# Columnas confirmadas en backend/sql/distribuidora/001_schema.sql + 044_*.sql
_HEADER_COLUMNS = """
    d.document_id,
    d.source_document_id,
    d.document_type_id,
    d.number,
    d.company_id,
    d.office_id,
    d.emission_date,
    d.generation_date,
    d.client_id,
    d.seller_id,
    d.seller_name,
    d.net_amount,
    d.tax_amount,
    d.total_amount,
    d.state,
    d.commercial_state
""".strip()

_LINE_COLUMNS = """
    dd.detail_id,
    dd.document_id,
    dd.variant_id,
    dd.variant_code,
    dd.quantity,
    dd.net_amount,
    dd.tax_amount,
    dd.total_amount,
    dd.net_unit_value,
    dd.total_unit_value,
    dd.net_discount,
    dd.total_discount
""".strip()


def _require_scope(
    *,
    company_id: int,
    office_id: int,
    date_from: date,
    date_to: date,
) -> None:
    if company_id is None or int(company_id) <= 0:
        raise ValueError("company_id is required and must be > 0")
    if office_id is None or int(office_id) <= 0:
        raise ValueError("office_id is required and must be > 0")
    if date_from is None or date_to is None:
        raise ValueError("date_from and date_to are required")
    if date_to < date_from:
        raise ValueError("date_to must be >= date_from")


def _clamp_page(page: int, page_size: int) -> tuple[int, int, int]:
    page_i = max(1, int(page))
    size = min(MAX_PAGE_SIZE, max(1, int(page_size)))
    offset = (page_i - 1) * size
    return page_i, size, offset


def build_headers_query(
    *,
    company_id: int,
    office_id: int,
    date_from: date,
    date_to: date,
    document_type_ids: Sequence[int] | None = None,
    active_only: bool = True,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[str, tuple[Any, ...]]:
    """SQL de encabezados + params. Puro: no ejecuta."""
    _require_scope(
        company_id=company_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
    )
    _, size, offset = _clamp_page(page, page_size)
    types = tuple(
        document_type_ids
        if document_type_ids is not None
        else sorted(SALE_DOCUMENT_TYPES | CREDIT_NOTE_DOCUMENT_TYPES)
    )
    if not types:
        raise ValueError("document_type_ids must not be empty")

    where = [
        "d.company_id = %s",
        "d.office_id = %s",
        "COALESCE(d.emission_date, d.generation_date)::date >= %s",
        "COALESCE(d.emission_date, d.generation_date)::date <= %s",
        "d.document_type_id = ANY(%s)",
    ]
    params: list[Any] = [
        int(company_id),
        int(office_id),
        date_from,
        date_to,
        list(types),
    ]
    if active_only:
        where.append("COALESCE(d.state, 0) = 0")

    sql = f"""
SELECT {_HEADER_COLUMNS}
FROM distribuidora.documents d
WHERE {' AND '.join(where)}
ORDER BY COALESCE(d.emission_date, d.generation_date) ASC NULLS LAST,
         d.document_id ASC
LIMIT %s OFFSET %s
""".strip()
    params.extend([size, offset])
    return sql, tuple(params)


def build_lines_query(
    *,
    company_id: int,
    office_id: int,
    date_from: date,
    date_to: date,
    document_type_ids: Sequence[int] | None = None,
    active_only: bool = True,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> tuple[str, tuple[Any, ...]]:
    """SQL de líneas unidas a documentos del scope. Puro: no ejecuta."""
    _require_scope(
        company_id=company_id,
        office_id=office_id,
        date_from=date_from,
        date_to=date_to,
    )
    _, size, offset = _clamp_page(page, page_size)
    types = tuple(
        document_type_ids
        if document_type_ids is not None
        else sorted(SALE_DOCUMENT_TYPES | CREDIT_NOTE_DOCUMENT_TYPES)
    )
    if not types:
        raise ValueError("document_type_ids must not be empty")

    where = [
        "d.company_id = %s",
        "d.office_id = %s",
        "COALESCE(d.emission_date, d.generation_date)::date >= %s",
        "COALESCE(d.emission_date, d.generation_date)::date <= %s",
        "d.document_type_id = ANY(%s)",
    ]
    params: list[Any] = [
        int(company_id),
        int(office_id),
        date_from,
        date_to,
        list(types),
    ]
    if active_only:
        where.append("COALESCE(d.state, 0) = 0")

    sql = f"""
SELECT {_LINE_COLUMNS}
FROM distribuidora.document_details dd
INNER JOIN distribuidora.documents d
    ON d.document_id = dd.document_id
WHERE {' AND '.join(where)}
ORDER BY dd.document_id ASC, dd.detail_id ASC
LIMIT %s OFFSET %s
""".strip()
    params.extend([size, offset])
    return sql, tuple(params)


def build_lines_for_document_ids_query(
    *,
    company_id: int,
    office_id: int,
    document_ids: Sequence[int],
    max_lines: int = 5000,
) -> tuple[str, tuple[Any, ...]]:
    """Líneas solo para document_ids ya cargados (sin ampliar el set)."""
    if company_id is None or int(company_id) <= 0:
        raise ValueError("company_id is required and must be > 0")
    if office_id is None or int(office_id) <= 0:
        raise ValueError("office_id is required and must be > 0")
    ids = [int(x) for x in document_ids]
    if not ids:
        # Query imposible: no debe devolver filas; evita SELECT sin filtro de IDs.
        return (
            f"""
SELECT {_LINE_COLUMNS}
FROM distribuidora.document_details dd
INNER JOIN distribuidora.documents d
    ON d.document_id = dd.document_id
WHERE FALSE
LIMIT 0
""".strip(),
            (),
        )
    limit = max(1, min(int(max_lines), 20000))
    sql = f"""
SELECT {_LINE_COLUMNS}
FROM distribuidora.document_details dd
INNER JOIN distribuidora.documents d
    ON d.document_id = dd.document_id
WHERE d.company_id = %s
  AND d.office_id = %s
  AND dd.document_id = ANY(%s)
ORDER BY dd.document_id ASC, dd.detail_id ASC
LIMIT %s
""".strip()
    return sql, (int(company_id), int(office_id), ids, limit)


def _as_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def row_to_header(row: dict[str, Any]) -> AnalyticsDocumentHeader:
    emission = _as_date(row.get("emission_date"))
    generation = _as_date(row.get("generation_date"))
    commercial = resolve_commercial_date(emission, generation)
    if commercial is None:
        raise ValueError(
            f"document_id={row.get('document_id')} sin emission_date ni generation_date"
        )
    state = int(row.get("state") if row.get("state") is not None else 0)
    dtype = int(row["document_type_id"])
    return AnalyticsDocumentHeader(
        document_id=int(row["document_id"]),
        source_document_id=(
            int(row["source_document_id"])
            if row.get("source_document_id") is not None
            else None
        ),
        document_type_id=dtype,
        number=int(row["number"]) if row.get("number") is not None else None,
        company_id=int(row["company_id"]),
        office_id=int(row["office_id"]),
        emission_date=emission,
        generation_date=generation,
        commercial_date=commercial,
        client_id=int(row["client_id"]) if row.get("client_id") is not None else None,
        client_name=None,  # no confirmado en documents
        seller_id=int(row["seller_id"]) if row.get("seller_id") is not None else None,
        raw_seller_name=(
            str(row["seller_name"]) if row.get("seller_name") is not None else None
        ),
        net_amount=optional_decimal(row.get("net_amount")),
        tax_amount=optional_decimal(row.get("tax_amount")),
        total_amount=optional_decimal(row.get("total_amount")),
        state=state,
        commercial_state=(
            int(row["commercial_state"])
            if row.get("commercial_state") is not None
            else None
        ),
        kind=classify_document_kind(dtype),
        is_active=state == 0,
    )


def row_to_line(row: dict[str, Any]) -> AnalyticsDocumentLine:
    unit = optional_decimal(row.get("total_unit_value"))
    if unit is None:
        unit = optional_decimal(row.get("net_unit_value"))
    discount = optional_decimal(row.get("total_discount"))
    if discount is None:
        discount = optional_decimal(row.get("net_discount"))
    code = row.get("variant_code")
    return AnalyticsDocumentLine(
        detail_id=int(row["detail_id"]),
        document_id=int(row["document_id"]),
        variant_id=int(row["variant_id"]) if row.get("variant_id") is not None else None,
        barcode=None,  # no confirmado en document_details
        variant_code=str(code) if code is not None else None,
        quantity=optional_decimal(row.get("quantity")),
        line_net_amount=optional_decimal(row.get("net_amount")),
        line_tax_amount=optional_decimal(row.get("tax_amount")),
        line_total_amount=optional_decimal(row.get("total_amount")),
        unit_price=unit,
        discount_amount=discount,
        allocated_net_amount=None,
        net_method=LineNetMethod.UNAVAILABLE,
    )


class DistribuidoraDocumentSource:
    """Fuente viva company/office con executor inyectable."""

    source_name = "distribuidora_live"

    def __init__(
        self,
        executor: QueryExecutor,
        *,
        statement_timeout: str = DEFAULT_STATEMENT_TIMEOUT,
    ) -> None:
        self._executor = executor
        self._statement_timeout = statement_timeout

    def fetch_documents(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        document_type_ids: Sequence[int] | None = None,
        active_only: bool = True,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> list[AnalyticsDocumentHeader]:
        sql, params = build_headers_query(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            document_type_ids=document_type_ids,
            active_only=active_only,
            page=page,
            page_size=page_size,
        )
        rows = self._executor(sql, params)
        return [row_to_header(r) for r in rows]

    def fetch_document_lines(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        document_type_ids: Sequence[int] | None = None,
        active_only: bool = True,
        page: int = 1,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> list[AnalyticsDocumentLine]:
        sql, params = build_lines_query(
            company_id=company_id,
            office_id=office_id,
            date_from=date_from,
            date_to=date_to,
            document_type_ids=document_type_ids,
            active_only=active_only,
            page=page,
            page_size=page_size,
        )
        rows = self._executor(sql, params)
        return [row_to_line(r) for r in rows]

    def fetch_lines_for_documents(
        self,
        *,
        company_id: int,
        office_id: int,
        document_ids: Sequence[int],
        max_lines: int = 5000,
    ) -> list[AnalyticsDocumentLine]:
        sql, params = build_lines_for_document_ids_query(
            company_id=company_id,
            office_id=office_id,
            document_ids=document_ids,
            max_lines=max_lines,
        )
        rows = self._executor(sql, params)
        return [row_to_line(r) for r in rows]

    def iter_documents(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        document_type_ids: Sequence[int] | None = None,
        active_only: bool = True,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[AnalyticsDocumentHeader]:
        page = 1
        while True:
            batch = self.fetch_documents(
                company_id=company_id,
                office_id=office_id,
                date_from=date_from,
                date_to=date_to,
                document_type_ids=document_type_ids,
                active_only=active_only,
                page=page,
                page_size=page_size,
            )
            if not batch:
                break
            yield from batch
            if len(batch) < min(MAX_PAGE_SIZE, max(1, page_size)):
                break
            page += 1

    def iter_document_lines(
        self,
        *,
        company_id: int,
        office_id: int,
        date_from: date,
        date_to: date,
        document_type_ids: Sequence[int] | None = None,
        active_only: bool = True,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[AnalyticsDocumentLine]:
        page = 1
        while True:
            batch = self.fetch_document_lines(
                company_id=company_id,
                office_id=office_id,
                date_from=date_from,
                date_to=date_to,
                document_type_ids=document_type_ids,
                active_only=active_only,
                page=page,
                page_size=page_size,
            )
            if not batch:
                break
            yield from batch
            if len(batch) < min(MAX_PAGE_SIZE, max(1, page_size)):
                break
            page += 1

    def enrich_lines_with_header_net(
        self,
        header: AnalyticsDocumentHeader,
        lines: list[AnalyticsDocumentLine],
    ) -> list[AnalyticsDocumentLine]:
        """Asigna allocated_net_amount sin mutar el encabezado."""
        allocated, method = allocate_line_nets(
            header_net_amount=header.net_amount,
            line_nets=[ln.line_net_amount for ln in lines],
            line_totals=[ln.line_total_amount for ln in lines],
        )
        out: list[AnalyticsDocumentLine] = []
        for line, net in zip(lines, allocated, strict=True):
            out.append(
                AnalyticsDocumentLine(
                    detail_id=line.detail_id,
                    document_id=line.document_id,
                    variant_id=line.variant_id,
                    barcode=line.barcode,
                    variant_code=line.variant_code,
                    quantity=line.quantity,
                    line_net_amount=line.line_net_amount,
                    line_tax_amount=line.line_tax_amount,
                    line_total_amount=line.line_total_amount,
                    unit_price=line.unit_price,
                    discount_amount=line.discount_amount,
                    allocated_net_amount=net,
                    net_method=method,
                )
            )
        return out

    def reconcile_document(
        self,
        header: AnalyticsDocumentHeader,
        lines: list[AnalyticsDocumentLine],
    ) -> DocumentReconciliationResult:
        enriched = self.enrich_lines_with_header_net(header, lines)
        return reconcile_header_vs_lines(
            document_id=header.document_id,
            header_total_amount=header.total_amount,
            header_net_amount=header.net_amount,
            line_totals=[ln.line_total_amount for ln in enriched],
            line_nets=[ln.allocated_net_amount for ln in enriched],
            quantities=[ln.quantity for ln in enriched],
        )


# Re-export confirmed type ids for callers
CONFIRMED_DOCUMENT_TYPES = (
    DOC_TYPE_BOLETA,
    DOC_TYPE_FACTURA,
    DOC_TYPE_CREDIT_NOTE,
)
