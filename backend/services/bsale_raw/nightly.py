"""Orquestador nocturno ``bsale_raw``: metadata + catálogo, todas las empresas de ``sources``.

Sólo ORQUESTA ``run_entity_sync`` (FULL_RECONCILE) por (company, recurso), en serie, sin lógica de
sync propia: snapshot, fusible, frescura, ``missing_since``, locks y ``sync_runs`` son los del
motor. Escribe únicamente ``bsale_raw.*`` (vía el motor). Después de ``document_types`` lee
``distribuidora.document_type_roles`` para reportar tipos sin clasificar y drift de roles; nunca
modifica roles ni metadata.

No incluye stocks, variant_prices, variant_costs, clients ni documentos (tienen su propia estrategia).
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Protocol

from backend.services.bsale_raw.core.engine import read_token, redact, sanitize_error
from backend.services.bsale_raw.core.models import RunStatus, SyncMode
from backend.services.bsale_raw.core.registry import REGISTRY
from backend.services.bsale_raw.core.store import EntityOutcome, PgRawStore, SourceConfig, _table

logger = logging.getLogger(__name__)

TRIGGER_NIGHTLY = "NIGHTLY"

NIGHTLY_RESOURCES: tuple[str, ...] = (
    "taxes",
    "document_types",
    "product_types",
    "offices",
    "price_lists",
    "products",
    "variants",
)
# recurso -> recurso que debe terminar SUCCESS en la misma empresa para poder correr.
DEPENDENCIES: dict[str, str] = {"variants": "products"}

SUCCESS = "SUCCESS"
FAILED = "FAILED"
PARTIAL = "PARTIAL"
SKIPPED_DEPENDENCY = "SKIPPED_DEPENDENCY"

NEW_UNCLASSIFIED = "NEW_UNCLASSIFIED_DOCUMENT_TYPE"
UNCLASSIFIED = "UNCLASSIFIED_DOCUMENT_TYPE"
ROLE_METADATA_MISSING = "ROLE_METADATA_MISSING"
ROLE_CODE_SII_DRIFT = "ROLE_CODE_SII_DRIFT"


# ------------------------------------------------------------------------------------ modelos


@dataclass
class SourceRow:
    company_id: int
    name: str
    token_env: str
    active: bool


@dataclass
class ResourceResult:
    company_id: int
    resource: str
    status: str
    run_id: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    api_count: int | None = None
    fetched: int = 0
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped_newer: int = 0
    missing: int = 0
    errors: str | None = None
    duration_ms: int = 0


@dataclass
class CompanyResult:
    company_id: int
    name: str = ""
    status: str = SUCCESS
    error: str | None = None
    resources: list[ResourceResult] = field(default_factory=list)


@dataclass(frozen=True)
class DocumentTypeFinding:
    kind: str
    company_id: int
    document_type_id: int
    name: str | None
    code_sii: str | None
    state: int | None
    inactive_role: str | None = None


@dataclass(frozen=True)
class RoleDrift:
    kind: str
    company_id: int
    document_type_id: int
    role: str
    active: bool
    expected_code_sii: int | None
    bsale_code_sii: str | None
    name: str | None


@dataclass(frozen=True)
class NewPriceList:
    company_id: int
    price_list_id: int
    name: str | None
    state: int | None


@dataclass
class NightlyReport:
    status: str = SUCCESS
    dry_run: bool = False
    error: str | None = None
    companies: list[CompanyResult] = field(default_factory=list)
    inactive_sources: list[int] = field(default_factory=list)
    document_types: list[DocumentTypeFinding] = field(default_factory=list)
    role_drift: list[RoleDrift] = field(default_factory=list)
    role_validation_errors: list[str] = field(default_factory=list)
    new_price_lists: list[NewPriceList] = field(default_factory=list)
    new_products: dict[int, int] = field(default_factory=dict)
    new_variants: dict[int, int] = field(default_factory=dict)
    duration_ms: int = 0
    # Valores de token leídos del entorno, sólo para redactar la salida; nunca se imprimen.
    secrets: list[str] = field(default_factory=list, repr=False)

    @property
    def new_document_types(self) -> list[DocumentTypeFinding]:
        return [f for f in self.document_types if f.kind == NEW_UNCLASSIFIED]

    @property
    def unclassified_document_types(self) -> list[DocumentTypeFinding]:
        return [f for f in self.document_types if f.kind == UNCLASSIFIED]


# ---------------------------------------------------------------------------- lectura (BD)


class NightlyReader(Protocol):
    def list_sources(self) -> list[SourceRow]: ...

    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def new_rows(self, resource: str, company_id: int, run_id: int, columns: tuple[str, ...]) -> list[dict]: ...

    def count_new_rows(self, resource: str, company_id: int, run_id: int) -> int: ...

    def document_types(self, company_id: int) -> list[dict]: ...

    def document_type_roles(self, company_id: int) -> list[dict]: ...


def _new_rows_where(resource: str) -> str:
    """Filas insertadas por ESTA corrida: ``first_seen_at`` sólo se fija al insertar (reloj de BD)."""
    table = _table(REGISTRY.get(resource))
    return (
        f"FROM {table} t JOIN bsale_raw.sync_runs r ON r.id = %s "
        "WHERE t.company_id = %s AND t.sync_run_id = r.id AND t.first_seen_at >= r.started_at"
    )


_SOURCES_SQL = "SELECT company_id, name, token_env, active FROM bsale_raw.sources ORDER BY company_id"
_DOCUMENT_TYPES_SQL = (
    "SELECT bsale_id, name, code_sii, state, missing_since "
    "FROM bsale_raw.document_types WHERE company_id = %s ORDER BY bsale_id"
)
_ROLES_SQL = (
    "SELECT document_type_id, role, active, expected_code_sii "
    "FROM distribuidora.document_type_roles WHERE company_id = %s ORDER BY document_type_id"
)


class PgNightlyReader(PgRawStore):
    """Conexión de sólo lectura (``set_session(readonly=True)``): nunca escribe."""

    def __init__(self, connection_factory=None) -> None:
        super().__init__(connection_factory, read_only=True)

    def _fetch(self, sql: str, params: tuple = ()) -> list[tuple]:
        cur = self._work().cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def list_sources(self) -> list[SourceRow]:
        return [
            SourceRow(int(cid), name or "", token_env or "", bool(active))
            for cid, name, token_env, active in self._fetch(_SOURCES_SQL)
        ]

    def new_rows(self, resource: str, company_id: int, run_id: int, columns: tuple[str, ...]) -> list[dict]:
        allowed = {c.column for c in REGISTRY.get(resource).typed_columns}
        if not set(columns) <= allowed:
            raise ValueError(f"columnas no declaradas para {resource}: {sorted(set(columns) - allowed)}")
        cols = ", ".join(["t.bsale_id", *(f"t.{c}" for c in columns)])
        rows = self._fetch(f"SELECT {cols} {_new_rows_where(resource)} ORDER BY t.bsale_id", (run_id, company_id))
        return [dict(zip(("bsale_id", *columns), r)) for r in rows]

    def count_new_rows(self, resource: str, company_id: int, run_id: int) -> int:
        rows = self._fetch(f"SELECT COUNT(*) {_new_rows_where(resource)}", (run_id, company_id))
        return int(rows[0][0]) if rows else 0

    def document_types(self, company_id: int) -> list[dict]:
        keys = ("bsale_id", "name", "code_sii", "state", "missing_since")
        return [dict(zip(keys, r)) for r in self._fetch(_DOCUMENT_TYPES_SQL, (company_id,))]

    def document_type_roles(self, company_id: int) -> list[dict]:
        keys = ("document_type_id", "role", "active", "expected_code_sii")
        return [dict(zip(keys, r)) for r in self._fetch(_ROLES_SQL, (company_id,))]


# -------------------------------------------------------------------- clasificación (pura)


def classify_document_types(
    company_id: int,
    current_types: list[dict],
    roles: list[dict],
    new_ids: set[int],
) -> tuple[list[DocumentTypeFinding], list[RoleDrift]]:
    """Identidad por (company_id, document_type_id). Nunca usa name ni code_sii como identidad."""
    present = {int(t["bsale_id"]): t for t in current_types if t.get("missing_since") is None}
    by_type = {int(r["document_type_id"]): r for r in roles}

    findings: list[DocumentTypeFinding] = []
    for type_id, t in sorted(present.items()):
        role = by_type.get(type_id)
        if role is not None and role["active"]:
            continue
        findings.append(
            DocumentTypeFinding(
                kind=NEW_UNCLASSIFIED if type_id in new_ids else UNCLASSIFIED,
                company_id=company_id,
                document_type_id=type_id,
                name=t.get("name"),
                code_sii=t.get("code_sii"),
                state=t.get("state"),
                inactive_role=None if role is None else str(role["role"]),
            )
        )

    drift: list[RoleDrift] = []
    for type_id, role in sorted(by_type.items()):
        t = present.get(type_id)
        expected = role.get("expected_code_sii")
        received = None if t is None else (str(t["code_sii"]).strip() if t.get("code_sii") is not None else None)
        if t is None:
            kind = ROLE_METADATA_MISSING
        elif expected is not None and str(int(expected)) != (received or ""):
            kind = ROLE_CODE_SII_DRIFT
        else:
            continue
        drift.append(
            RoleDrift(
                kind=kind,
                company_id=company_id,
                document_type_id=type_id,
                role=str(role["role"]),
                active=bool(role["active"]),
                expected_code_sii=None if expected is None else int(expected),
                bsale_code_sii=received,
                name=None if t is None else t.get("name"),
            )
        )
    return findings, drift


# ------------------------------------------------------------------------- orquestación


SyncFn = Callable[[int, str, bool], EntityOutcome]


def default_sync(company_id: int, resource: str, dry_run: bool) -> EntityOutcome:
    from backend.services.bsale_raw.core.engine import run_entity_sync

    store = PgRawStore(read_only=dry_run)
    try:
        return run_entity_sync(
            store=store,
            company_id=company_id,
            resource=resource,
            mode=SyncMode.FULL_RECONCILE,
            dry_run=dry_run,
            trigger=TRIGGER_NIGHTLY,
            host=socket.gethostname(),
        )
    finally:
        store.close()


def _result_from_outcome(outcome: EntityOutcome, started_at: datetime, finished_at: datetime) -> ResourceResult:
    status = SUCCESS if outcome.status == RunStatus.SUCCESS.value else FAILED
    error = outcome.error
    if outcome.status == RunStatus.SKIPPED.value:
        error = f"no ejecutado: {error or 'lock ocupado'}"
    elif status == FAILED and not error:
        error = f"status={outcome.status}"
    return ResourceResult(
        company_id=outcome.company_id,
        resource=outcome.resource,
        status=status,
        run_id=outcome.sync_run_id,
        started_at=started_at,
        finished_at=finished_at,
        api_count=outcome.api_count,
        fetched=outcome.rows_received,
        inserted=outcome.rows_inserted,
        updated=outcome.rows_updated,
        unchanged=outcome.rows_unchanged,
        skipped_newer=outcome.rows_skipped_newer,
        missing=outcome.rows_missing,
        errors=error,
        duration_ms=outcome.duration_ms,
    )


def _run_resource(
    company_id: int, resource: str, dry_run: bool, sync: SyncFn, secrets: list[str],
    clock: Callable[[], datetime],
) -> ResourceResult:
    started = clock()
    try:
        outcome = sync(company_id, resource, dry_run)
    except Exception as exc:  # p. ej. UnsupportedSyncError o fallo de conexión: aislado al recurso
        finished = clock()
        return ResourceResult(
            company_id=company_id, resource=resource, status=FAILED, started_at=started,
            finished_at=finished, errors=sanitize_error(exc, secrets),
            duration_ms=int((finished - started).total_seconds() * 1000),
        )
    result = _result_from_outcome(outcome, started, clock())
    if result.errors:
        result.errors = redact(result.errors, secrets)
    return result


def _after_success(
    report: NightlyReport, reader: NightlyReader, result: ResourceResult, secrets: list[str],
) -> None:
    """Detección post-SUCCESS (sólo corrida real: requiere el ``sync_run_id``)."""
    if report.dry_run or result.run_id is None:
        return
    cid, run_id = result.company_id, result.run_id
    try:
        if result.resource == "document_types":
            new_ids = {int(r["bsale_id"]) for r in reader.new_rows("document_types", cid, run_id, ())}
            findings, drift = classify_document_types(
                cid, reader.document_types(cid), reader.document_type_roles(cid), new_ids
            )
            report.document_types.extend(findings)
            report.role_drift.extend(drift)
        elif result.resource == "price_lists":
            for row in reader.new_rows("price_lists", cid, run_id, ("name", "state")):
                report.new_price_lists.append(
                    NewPriceList(cid, int(row["bsale_id"]), row.get("name"), row.get("state"))
                )
        elif result.resource == "products":
            report.new_products[cid] = reader.count_new_rows("products", cid, run_id)
        elif result.resource == "variants":
            report.new_variants[cid] = reader.count_new_rows("variants", cid, run_id)
    except Exception as exc:
        report.role_validation_errors.append(
            f"company={cid} resource={result.resource}: {sanitize_error(exc, secrets)}"
        )


def _company_status(company: CompanyResult) -> str:
    if company.error or not company.resources:
        return FAILED
    statuses = [r.status for r in company.resources]
    if all(s == SUCCESS for s in statuses):
        return SUCCESS
    if SUCCESS not in statuses:
        return FAILED
    return PARTIAL


def _global_status(report: NightlyReport) -> str:
    if report.error or not report.companies:
        return FAILED
    if not any(c.status in (SUCCESS, PARTIAL) for c in report.companies):
        return FAILED
    degraded = (
        any(c.status != SUCCESS for c in report.companies)
        or report.role_drift
        or report.role_validation_errors
        or report.new_document_types
    )
    return PARTIAL if degraded else SUCCESS


def run_nightly(
    *,
    reader: NightlyReader,
    sync: SyncFn = default_sync,
    companies: list[int] | None = None,
    dry_run: bool = False,
    getenv: Callable[[str], str | None] = os.getenv,
    monotonic: Callable[[], float] = time.perf_counter,
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> NightlyReport:
    """Nunca lanza: cualquier falla queda en el reporte (recurso, empresa o global)."""
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    t0 = monotonic()
    report = NightlyReport(dry_run=dry_run)
    secrets = report.secrets
    try:
        sources = reader.list_sources()
        for src in sources:
            value = (getenv(src.token_env) or "").strip() if src.token_env else ""
            if value:
                secrets.append(value)
    except Exception as exc:
        # Sin sources no se sabe qué secretos redactar: sólo el tipo, nunca el mensaje.
        report.error = f"no se pudo leer bsale_raw.sources ({type(exc).__name__})"
        report.status = FAILED
        report.duration_ms = int((monotonic() - t0) * 1000)
        return report

    names = {s.company_id: s.name for s in sources}
    if companies:
        targets = list(dict.fromkeys(int(c) for c in companies))
    else:
        targets = [s.company_id for s in sources if s.active]
        report.inactive_sources = [s.company_id for s in sources if not s.active]
    if not targets:
        report.error = "bsale_raw.sources no tiene empresas activas"

    for cid in targets:
        company = CompanyResult(company_id=cid, name=names.get(cid, ""))
        report.companies.append(company)
        try:
            source = reader.resolve_source(cid)
            read_token(source, getenv)
        except Exception as exc:
            company.error = sanitize_error(exc, secrets)
            company.status = FAILED
            logger.error("[BSALE_RAW_NIGHTLY] company=%s FAILED (config): %s", cid, company.error)
            continue

        done: dict[str, str] = {}
        for resource in NIGHTLY_RESOURCES:
            dep = DEPENDENCIES.get(resource)
            if dep is not None and done.get(dep) != SUCCESS:
                result = ResourceResult(
                    company_id=cid, resource=resource, status=SKIPPED_DEPENDENCY,
                    errors=f"{dep} {done.get(dep, 'no ejecutado')}",
                )
            else:
                result = _run_resource(cid, resource, dry_run, sync, secrets, clock)
                if result.status == SUCCESS:
                    _after_success(report, reader, result, secrets)
            done[resource] = result.status
            company.resources.append(result)
            logger.info(
                "[BSALE_RAW_NIGHTLY] company=%s resource=%s status=%s run_id=%s fetched=%s "
                "inserted=%s updated=%s missing=%s duration_ms=%s",
                cid, resource, result.status, result.run_id, result.fetched, result.inserted,
                result.updated, result.missing, result.duration_ms,
            )
        company.status = _company_status(company)

    report.status = _global_status(report)
    report.duration_ms = int((monotonic() - t0) * 1000)
    return report


# ------------------------------------------------------------------------------- salida


def _q(value: Any) -> str:
    """Nombres exactos como los entrega Bsale (comillas para ver espacios/sufijos)."""
    return "null" if value is None else json.dumps(value, ensure_ascii=False)


def format_report(report: NightlyReport) -> str:
    lines = ["NIGHTLY BSALE SUMMARY", f"status={report.status}"]
    if report.dry_run:
        lines.append("dry_run=true (sin escrituras; detección de nuevos y validación de roles requieren corrida real)")
    if report.error:
        lines.append(f"error={report.error}")
    if report.inactive_sources:
        lines.append(f"inactive_sources={','.join(map(str, report.inactive_sources))}")

    for company in report.companies:
        lines.append("")
        header = f"company {company.company_id}"
        if company.name:
            header += f" {_q(company.name)}"
        lines.append(f"{header}  {company.status}")
        if company.error:
            lines.append(f"  config_error={company.error}")
        for r in company.resources:
            line = (
                f"  {r.resource:<15} {r.status:<18} run_id={'' if r.run_id is None else r.run_id} "
                f"api_count={'' if r.api_count is None else r.api_count} fetched={r.fetched} "
                f"inserted={r.inserted} changed={r.updated} unchanged={r.unchanged} "
                f"missing={r.missing} duration_ms={r.duration_ms}"
            )
            lines.append(line)
            if r.errors:
                lines.append(f"    error={r.errors}")

    def block(title: str, items: list[str]) -> None:
        lines.append("")
        lines.append(f"{title}:")
        lines.extend(items or ["  (ninguno)"])

    block("NEW_DOCUMENT_TYPES", [
        f"  {NEW_UNCLASSIFIED} company_id={f.company_id} document_type_id={f.document_type_id} "
        f"name={_q(f.name)} code_sii={_q(f.code_sii)} state={_q(f.state)}  <- REVISIÓN MANUAL"
        for f in report.new_document_types
    ])
    block("UNCLASSIFIED_DOCUMENT_TYPES", [
        f"  company_id={f.company_id} document_type_id={f.document_type_id} name={_q(f.name)} "
        f"code_sii={_q(f.code_sii)} state={_q(f.state)}"
        + (f" role_inactivo={f.inactive_role}" if f.inactive_role else "")
        for f in report.unclassified_document_types
    ])
    block("NEW_PRICE_LISTS", [
        f"  NEW_PRICE_LIST company_id={p.company_id} price_list_id={p.price_list_id} "
        f"name={_q(p.name)} state={_q(p.state)}"
        for p in report.new_price_lists
    ])
    block("NEW_PRODUCTS", [f"  company_id={c} count={n}" for c, n in sorted(report.new_products.items())])
    block("NEW_VARIANTS", [f"  company_id={c} count={n}" for c, n in sorted(report.new_variants.items())])
    block("FAILED_RESOURCES", [
        f"  company_id={r.company_id} resource={r.resource} status={r.status} error={r.errors or ''}"
        for c in report.companies for r in c.resources if r.status != SUCCESS
    ] + [
        f"  company_id={c.company_id} status=FAILED config_error={c.error}"
        for c in report.companies if c.error
    ])
    block("ROLE_METADATA_DRIFT", [
        f"  {d.kind} company_id={d.company_id} document_type_id={d.document_type_id} role={d.role} "
        f"active={str(d.active).lower()} expected_code_sii={_q(d.expected_code_sii)} "
        f"bsale_code_sii={_q(d.bsale_code_sii)} name={_q(d.name)}"
        for d in report.role_drift
    ] + [f"  VALIDATION_ERROR {e}" for e in report.role_validation_errors])
    lines.append("")
    lines.append(f"TOTAL_DURATION: {report.duration_ms} ms")
    return redact("\n".join(lines), report.secrets)
