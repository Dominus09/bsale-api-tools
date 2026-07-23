"""Validación read-only del adaptador Distribuidora (Etapa 2B).

La conexión se inyecta; el job CLI abre PG solo cuando se ejecuta fuera de tests.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from backend.services.analytics.document_models import (
    AnalyticsDocumentHeader,
    AnalyticsDocumentKind,
    AnalyticsDocumentLine,
    LineNetMethod,
    ReconciliationStatus,
)
from backend.services.analytics.distribuidora_source import (
    DOC_TYPE_BOLETA,
    DOC_TYPE_CREDIT_NOTE,
    DOC_TYPE_FACTURA,
    DistribuidoraDocumentSource,
)
from backend.services.analytics.money import ZERO, optional_decimal, quantize_money

MAX_DAYS = 7
MAX_LIMIT = 50
MAX_PAGE_SIZE = 50
MAX_TIMEOUT_SECONDS = 30
DEFAULT_DAYS = 2
DEFAULT_LIMIT = 20
DEFAULT_PAGE_SIZE = 20
DEFAULT_TIMEOUT_SECONDS = 15
DEFAULT_LOCK_TIMEOUT = "3s"
MAX_SAMPLES = 10

_FORBIDDEN_SQL = (
    "INSERT ",
    "UPDATE ",
    "DELETE ",
    "ALTER ",
    "CREATE ",
    "DROP ",
    "TRUNCATE ",
    "GRANT ",
    "REVOKE ",
    "FOR UPDATE",
)


class AnalyticsValidationError(Exception):
    def __init__(
        self,
        message: str,
        *,
        error_type: str = "validation_error",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.details = details or {}


@dataclass(frozen=True, slots=True)
class ValidateArgs:
    company_id: int
    office_id: int
    days: int = DEFAULT_DAYS
    limit: int = DEFAULT_LIMIT
    page_size: int = DEFAULT_PAGE_SIZE
    document_types: tuple[int, ...] = (
        DOC_TYPE_BOLETA,
        DOC_TYPE_FACTURA,
        DOC_TYPE_CREDIT_NOTE,
    )
    statement_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS


def clamp_validate_args(
    *,
    company_id: int,
    office_id: int,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    page_size: int = DEFAULT_PAGE_SIZE,
    document_types: Sequence[int] | None = None,
    statement_timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> ValidateArgs:
    if int(company_id) <= 0 or int(office_id) <= 0:
        raise AnalyticsValidationError(
            "company_id and office_id are required and must be > 0",
            error_type="invalid_args",
        )
    days_i = max(1, min(int(days), MAX_DAYS))
    limit_i = max(1, min(int(limit), MAX_LIMIT))
    page_i = max(1, min(int(page_size), MAX_PAGE_SIZE, limit_i))
    timeout_i = max(1, min(int(statement_timeout_seconds), MAX_TIMEOUT_SECONDS))
    types = tuple(
        int(t)
        for t in (
            document_types
            if document_types is not None
            else (DOC_TYPE_BOLETA, DOC_TYPE_FACTURA, DOC_TYPE_CREDIT_NOTE)
        )
    )
    if not types:
        raise AnalyticsValidationError(
            "document_types must not be empty",
            error_type="invalid_args",
        )
    return ValidateArgs(
        company_id=int(company_id),
        office_id=int(office_id),
        days=days_i,
        limit=limit_i,
        page_size=page_i,
        document_types=types,
        statement_timeout_seconds=timeout_i,
    )


def commercial_date_window(days: int, *, today: date | None = None) -> tuple[date, date]:
    """date_to = hoy local; date_from = hoy − (days−1)."""
    if today is None:
        try:
            from zoneinfo import ZoneInfo

            today = datetime.now(ZoneInfo("America/Santiago")).date()
        except Exception:
            today = datetime.now(timezone.utc).date()
    days_i = max(1, min(int(days), MAX_DAYS))
    date_to = today
    date_from = date_to - timedelta(days=days_i - 1)
    return date_from, date_to


def assert_sql_is_read_only(sql: str) -> None:
    upper = f" {sql.upper()} "
    for token in _FORBIDDEN_SQL:
        if token in upper:
            raise AnalyticsValidationError(
                f"Forbidden SQL token detected: {token.strip()}",
                error_type="forbidden_sql",
                details={"sql_preview": sql[:200]},
            )


def _kind_bucket(kind: AnalyticsDocumentKind, document_type_id: int) -> str:
    if kind == AnalyticsDocumentKind.CREDIT_NOTE or document_type_id == DOC_TYPE_CREDIT_NOTE:
        return "credit_note"
    if document_type_id == DOC_TYPE_FACTURA:
        return "invoice"
    if document_type_id == DOC_TYPE_BOLETA:
        return "receipt"
    if kind == AnalyticsDocumentKind.SALE:
        return "invoice"
    return "unsupported"


def _predominant_net_method(lines: list[AnalyticsDocumentLine]) -> str | None:
    if not lines:
        return None
    counts = Counter(ln.net_method.value for ln in lines)
    return counts.most_common(1)[0][0]


def _sum_dec(values: list[Decimal | None]) -> Decimal | None:
    present = [v for v in values if v is not None]
    if not present:
        return None
    return quantize_money(sum(present, ZERO))


def build_report(
    *,
    args: ValidateArgs,
    date_from: date,
    date_to: date,
    headers: list[AnalyticsDocumentHeader],
    lines_by_doc: dict[int, list[AnalyticsDocumentLine]],
    source: DistribuidoraDocumentSource,
    duration_ms: float,
) -> dict[str, Any]:
    type_counts = {
        "invoice": 0,
        "receipt": 0,
        "credit_note": 0,
        "unsupported": 0,
    }
    recon_counts = {
        "matched": 0,
        "rounding_difference": 0,
        "mismatch": 0,
        "missing_lines": 0,
    }
    method_counts = {
        "explicit_line_net": 0,
        "allocated_from_header": 0,
        "unavailable": 0,
    }
    samples: list[dict[str, Any]] = []
    header_nets: list[Decimal | None] = []
    header_taxes: list[Decimal | None] = []
    header_totals: list[Decimal | None] = []
    lines_nets: list[Decimal | None] = []
    lines_totals: list[Decimal | None] = []
    total_lines = 0

    for header in headers:
        type_counts[_kind_bucket(header.kind, header.document_type_id)] += 1
        header_nets.append(header.net_amount)
        header_taxes.append(header.tax_amount)
        header_totals.append(header.total_amount)

        raw_lines = lines_by_doc.get(header.document_id, [])
        enriched = source.enrich_lines_with_header_net(header, raw_lines)
        total_lines += len(enriched)
        for ln in enriched:
            method_counts[ln.net_method.value] = method_counts.get(ln.net_method.value, 0) + 1
            lines_nets.append(ln.allocated_net_amount)
            lines_totals.append(ln.line_total_amount)

        result = source.reconcile_document(header, raw_lines)
        recon_counts[result.reconciliation_status.value] += 1

        if len(samples) < MAX_SAMPLES:
            samples.append(
                {
                    "document_id": header.document_id,
                    "folio": header.number,
                    "document_type": _kind_bucket(header.kind, header.document_type_id),
                    "commercial_date": header.commercial_date.isoformat(),
                    "line_count": result.line_count,
                    "header_total": (
                        str(result.header_total_amount)
                        if result.header_total_amount is not None
                        else None
                    ),
                    "lines_total": (
                        str(result.lines_total_amount)
                        if result.lines_total_amount is not None
                        else None
                    ),
                    "difference_total": (
                        str(result.difference_total)
                        if result.difference_total is not None
                        else None
                    ),
                    "reconciliation_status": result.reconciliation_status.value,
                    "line_net_method": _predominant_net_method(enriched),
                }
            )

    hn = _sum_dec(header_nets)
    ht = _sum_dec(header_taxes)
    htot = _sum_dec(header_totals)
    ln = _sum_dec(lines_nets)
    lt = _sum_dec(lines_totals)

    return {
        "ok": True,
        "read_only": True,
        "company_id": args.company_id,
        "office_id": args.office_id,
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "requested_limit": args.limit,
        "documents_loaded": len(headers),
        "lines_loaded": total_lines,
        "document_types": type_counts,
        "reconciliation": recon_counts,
        "amounts": {
            "header_net": str(hn) if hn is not None else None,
            "header_tax": str(ht) if ht is not None else None,
            "header_total": str(htot) if htot is not None else None,
            "lines_net": str(ln) if ln is not None else None,
            "lines_total": str(lt) if lt is not None else None,
        },
        "line_net_methods": method_counts,
        "samples": samples,
        "duration_ms": round(duration_ms, 1),
    }


def run_validation(
    *,
    args: ValidateArgs,
    source: DistribuidoraDocumentSource,
    today: date | None = None,
) -> dict[str, Any]:
    """Ejecuta carga + reconciliación usando el source inyectado (sin abrir conn aquí)."""
    t0 = time.perf_counter()
    date_from, date_to = commercial_date_window(args.days, today=today)
    headers = source.fetch_documents(
        company_id=args.company_id,
        office_id=args.office_id,
        date_from=date_from,
        date_to=date_to,
        document_type_ids=args.document_types,
        active_only=True,
        page=1,
        page_size=min(args.page_size, args.limit),
    )
    if len(headers) > args.limit:
        headers = headers[: args.limit]

    doc_ids = [h.document_id for h in headers]
    lines = source.fetch_lines_for_documents(
        company_id=args.company_id,
        office_id=args.office_id,
        document_ids=doc_ids,
        max_lines=max(args.limit * 200, 500),
    )
    # Defensa: descartar cualquier línea fuera del set cargado.
    allowed = set(doc_ids)
    lines = [ln for ln in lines if ln.document_id in allowed]
    lines_by_doc: dict[int, list[AnalyticsDocumentLine]] = {doc_id: [] for doc_id in doc_ids}
    for ln in lines:
        lines_by_doc.setdefault(ln.document_id, []).append(ln)

    duration_ms = (time.perf_counter() - t0) * 1000.0
    return build_report(
        args=args,
        date_from=date_from,
        date_to=date_to,
        headers=headers,
        lines_by_doc=lines_by_doc,
        source=source,
        duration_ms=duration_ms,
    )


def make_psycopg_executor(
    conn: Any,
    *,
    statement_timeout_seconds: int,
    lock_timeout: str = DEFAULT_LOCK_TIMEOUT,
    sql_log: list[str] | None = None,
) -> Callable[[str, tuple[Any, ...]], list[dict[str, Any]]]:
    """Executor read-only sobre una conexión ya abierta. No hace commit."""

    def _execute(sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        assert_sql_is_read_only(sql)
        if sql_log is not None:
            sql_log.append(sql)
        cur = conn.cursor()
        try:
            cur.execute(
                f"SET LOCAL statement_timeout = '{int(statement_timeout_seconds) * 1000}'"
            )
            cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            rows = cur.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        except Exception as exc:
            msg = str(exc).lower()
            if "statement timeout" in msg or "canceling statement" in msg:
                raise AnalyticsValidationError(
                    "statement_timeout",
                    error_type="statement_timeout",
                    details={"message": str(exc)},
                ) from exc
            if "undefinedcolumn" in msg.replace(" ", "") or "column" in msg and "does not exist" in msg:
                raise AnalyticsValidationError(
                    f"Schema mismatch: {exc}",
                    error_type="schema_mismatch",
                    details={"message": str(exc), "sql_preview": sql[:240]},
                ) from exc
            raise
        finally:
            cur.close()

    return _execute


def open_readonly_connection(get_connection: Callable[[], Any]) -> Any:
    """Abre conexión y fuerza sesión read-only (sin autocommit)."""
    conn = get_connection()
    try:
        if hasattr(conn, "set_session"):
            conn.set_session(readonly=True, autocommit=False)
        else:
            conn.autocommit = False
            cur = conn.cursor()
            try:
                cur.execute("SET default_transaction_read_only = on")
            finally:
                cur.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        raise
    return conn
