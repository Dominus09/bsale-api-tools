"""Motor de recepciones ``bsale_raw.stock_receptions`` + ``stock_reception_details`` (sólo company 3).

LIVE VALIDATED C3.1A (probe ``bsale_receptions_probe``): listado ASC por id; ``admissiondate`` filtra un
día exacto; ``admissiondaterange`` se IGNORA (nunca se usa); ``officeid`` / ``documentnumber`` filtran;
``expand=[details]`` completo en las muestras (no se verificaron recepciones de más de 50 líneas);
``cost`` es costo UNITARIO (su carácter neto está pendiente de verificar contra facturas).

Fuente única: ``GET /v1/stocks/receptions.json?expand=[details]`` (y ``/receptions/{id}.json?expand=[details]``
en POINT). Cada nodo ``details`` se valida (``items`` lista, ``count`` entero == líneas, sin ``next`` ni
``offset`` > 0, ids de línea únicos y numéricos, ``variant.id``, ``quantity`` y ``cost`` numéricos o nulos).
Si falta, viene truncado o es inconsistente: ``/receptions/{id}/details.json`` paginado estricto. Una
recepción sin todas sus líneas NUNCA queda ``details_complete``: si es nueva se guarda la cabecera con
``details_complete = false`` y sin líneas; si ya existía no se toca (no se reemplaza con un detalle
incompleto). La falla queda en ``sync_cursors`` (``failures``) y se reintenta con ``--retry-failures``.

Alcance: recepciones con ``admissionDate`` desde ``MIN_ADMISSION_DATE`` (2025-01-01). No hay barrido global
del histórico (desde 2013): sólo rangos de fechas acotados y recepciones puntuales.

Modalidades:

- ``dates`` (``WINDOW_RECONCILE``): ``admissiondate`` día por día, como máximo ``MAX_DAYS`` por corrida. Cada
  día es un snapshot estricto y validado (todas las recepciones del día exacto; si Bsale ignora el filtro, el
  día falla en la primera página) y se escribe en su propia transacción: una corrida cortada se retoma con
  ``--from`` = ``resume_from`` del informe, sin duplicar (UPSERT idempotente). Es el único ámbito completo:
  las recepciones guardadas de ese día que Bsale ya no entrega se confirman una por una (404 = ausente; otra
  fecha = se actualiza) y recién entonces ``missing_since``, con fusible. Fuera de los días consultados no se
  marca nada.
- ``point`` / ``retry`` (``POINT``): recepciones explícitas, o las fallidas / incompletas.

``payload`` guarda la recepción completa tal como llegó (con su nodo ``details``) y ``payload_hash`` es su
hash. Los cambios se detectan por ``content_hash``: cabecera sin ``details`` y líneas sin ``variantStock``
(stock de la variante: cambia con cualquier movimiento posterior y generaría MODIFIED falsos).

Escrituras por día o por lote POINT en una transacción corta: lock de cada recepción
(``pg_advisory_xact_lock`` + ``FOR UPDATE``) → cabecera con frescura (``api_fetched_at``) → líneas
(UPSERT + ``missing_since`` de las que ya no vienen; nunca se borra) → ``stock_reception_change_log`` →
cursor / fallas. RAW guarda ``cost`` y ``quantity`` tal cual: no calcula IVA, ILA, bruto, promedio ni
total, y no toca ``variant_costs``. ``note`` y ``user`` viven sólo en el payload: nunca en logs ni salida.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Iterator, Protocol

from backend.services.bsale.companies import BsaleCompany
from backend.services.bsale.http_client import BsaleHttpError
from backend.services.bsale_raw.core.client import build_company_client
from backend.services.bsale_raw.core.engine import (
    TRIGGER_MANUAL,
    UnsupportedSyncError,
    _collect_request_stats,
    _fail,
    read_token,
    sanitize_error,
)
from backend.services.bsale_raw.core.models import ReceptionEventKind, RunStatus, SyncMode, payload_hash
from backend.services.bsale_raw.core.rate_limit import (
    CompanyRateLimiters,
    PriorityRateLimiter,
    RateLimitConfig,
    TokenBucket,
)
from backend.services.bsale_raw.core.reconcile import max_missing_pct
from backend.services.bsale_raw.core.registry import (
    GLOBAL_SCOPE,
    REGISTRY,
    ResourceSpec,
    optional_int,
    optional_numeric,
    optional_relation_id,
    optional_text,
    optional_unix_date,
)
from backend.services.bsale_raw.core.snapshot import (
    BSALE_PAGE_LIMIT,
    Clock,
    SnapshotValidationError,
    _bsale_id,
    fetch_snapshot,
    utc_now,
)
from backend.services.bsale_raw.core.store import (
    EntityOutcome,
    LockBusyError,
    PgRawStore,
    PgRawTx,
    RunHandle,
    SourceConfig,
    advisory_lock_keys,
)

logger = logging.getLogger(__name__)

RESOURCE = "stock_receptions"
RECEPTION_COMPANIES = frozenset({3})

LIST_ENDPOINT = "stocks/receptions.json"
ITEM_ENDPOINT = "stocks/receptions/{id}.json"
DETAILS_ENDPOINT = "stocks/receptions/{id}/details.json"
EXPAND_PARAMS = {"expand": "[details]"}

SOURCE_EXPAND = "EXPAND"
SOURCE_DETAILS = "DETAILS_ENDPOINT"

MODE_DATES = "dates"
MODE_POINT = "point"
MODE_RETRY = "retry"
SYNC_MODE = {
    MODE_DATES: SyncMode.WINDOW_RECONCILE,
    MODE_POINT: SyncMode.POINT,
    MODE_RETRY: SyncMode.POINT,
}
STATE_SCOPE = {MODE_DATES: "admission_date", MODE_POINT: "point", MODE_RETRY: "point"}

FAILURES_CURSOR = "failures"

MIN_ADMISSION_DATE = date(2025, 1, 1)
VOLATILE_DETAIL_FIELDS = frozenset({"variantStock"})

DEFAULT_RPS = 1.0
MAX_RPS = 2.0
DEFAULT_MAX_REQUESTS = 3000
MAX_DAYS = 370
MAX_DAY_PAGES = 20  # 1.000 recepciones en un día: más que eso = filtro ignorado o anomalía
MAX_POINT = 50
MAX_HTTP_429 = 5
MAX_CONSECUTIVE_FAILURES = 10
DETAIL_ATTEMPTS = 2
MIN_MISSING_FOR_FUSE = 2
MAX_SAMPLE = 50

ReceptionClientFactory = Callable[[SourceConfig, str, ResourceSpec], Any]


class ReceptionPayloadError(ValueError):
    """Recepción o línea con forma inesperada: no se guarda como completa."""


class ReceptionAbort(RuntimeError):
    """Corte de la corrida por cuota (429), errores seguidos o tope de requests."""


def reception_spec() -> ResourceSpec:
    import backend.services.bsale_raw.resources  # noqa: F401  (registra los recursos)

    return REGISTRY.get(RESOURCE)


# --- ritmo propio ---------------------------------------------------------------------------------


def reception_rate_config(getenv: Callable[[str], str | None] = os.getenv) -> RateLimitConfig:
    raw = (getenv("BSALE_RAW_RECEPTION_RPS") or "").strip()
    rps = float(raw) if raw else DEFAULT_RPS
    if not 0 < rps <= MAX_RPS:
        raise ValueError(f"BSALE_RAW_RECEPTION_RPS inválido: {rps} (rango (0, {MAX_RPS}])")
    return RateLimitConfig(requests_per_second=rps, burst=1)


def max_requests_config(getenv: Callable[[str], str | None] = os.getenv) -> int:
    raw = (getenv("BSALE_RAW_RECEPTION_MAX_REQUESTS") or "").strip()
    value = int(raw) if raw else DEFAULT_MAX_REQUESTS
    if value < 1:
        raise ValueError("BSALE_RAW_RECEPTION_MAX_REQUESTS debe ser >= 1")
    return value


_RECEPTION_LIMITERS = CompanyRateLimiters(lambda cid: PriorityRateLimiter(TokenBucket(reception_rate_config())))


def default_reception_client_factory(source: SourceConfig, token: str, spec: ResourceSpec) -> Any:
    company = BsaleCompany(company_id=source.company_id, name=source.name, token_env=source.token_env, token=token)
    return build_company_client(company, _RECEPTION_LIMITERS, spec.request_priority)


# --- filas en memoria -----------------------------------------------------------------------------


@dataclass(frozen=True)
class DetailRow:
    detail_id: int
    variant_id: int
    quantity: Decimal | None
    cost: Decimal | None
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str
    content_hash: str


@dataclass(frozen=True)
class ReceptionBundle:
    company_id: int
    reception_id: int
    typed: dict[str, Any]
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str
    content_hash: str
    api_fetched_at: datetime
    details: tuple[DetailRow, ...] | None = None
    details_count: int | None = None
    details_source: str | None = None
    details_fetched_at: datetime | None = None
    problem: str | None = None

    @property
    def complete(self) -> bool:
        return self.details is not None

    @property
    def details_hash(self) -> str | None:
        if self.details is None:
            return None
        return payload_hash(sorted([d.detail_id, d.content_hash] for d in self.details))

    @property
    def version_hash(self) -> str | None:
        d_hash = self.details_hash
        return None if d_hash is None else payload_hash([self.content_hash, d_hash])


@dataclass(frozen=True)
class StoredReception:
    reception_id: int
    content_hash: str | None
    details_hash: str | None
    version_hash: str | None
    api_fetched_at: datetime
    missing_since: datetime | None
    details_complete: bool | None
    admission_date: date | None = None


@dataclass(frozen=True)
class StoredDetail:
    detail_id: int
    content_hash: str | None
    variant_id: int | None
    quantity: Decimal | None
    cost: Decimal | None
    missing_since: datetime | None


@dataclass(frozen=True)
class ChangeEvent:
    company_id: int
    reception_id: int
    event_kind: str
    api_fetched_at: datetime | None
    detected_by: str
    admission_date: date | None
    previous_content_hash: str | None = None
    content_hash: str | None = None
    previous_details_hash: str | None = None
    details_hash: str | None = None
    previous_version_hash: str | None = None
    version_hash: str | None = None
    header_changed: bool = False
    details_changed: bool = False
    added: tuple[int, ...] = ()
    removed: tuple[int, ...] = ()
    quantity_changed: tuple[int, ...] = ()
    cost_changed: tuple[int, ...] = ()
    variant_changed: tuple[int, ...] = ()


# --- validación pura ------------------------------------------------------------------------------


def header_content(item: dict[str, Any]) -> dict[str, Any]:
    """Cabecera comparable: la recepción sin el nodo ``details`` (las líneas se comparan una por una)."""
    return {k: v for k, v in item.items() if k != "details"}


def detail_content(item: dict[str, Any]) -> dict[str, Any]:
    """Línea comparable: sin ``variantStock`` (stock vivo de la variante, no un dato de la recepción)."""
    return {k: v for k, v in item.items() if k not in VOLATILE_DETAIL_FIELDS}


def header_typed(payload: dict[str, Any]) -> dict[str, Any]:
    try:
        return {
            "office_id": optional_relation_id(payload.get("office")),
            "admission_date": optional_unix_date(payload.get("admissionDate")),
            "document_number": optional_text(payload.get("documentNumber")),
            "document": optional_text(payload.get("document")),
            "user_id": optional_relation_id(payload.get("user")),
            "internal_dispatch_id": optional_int(payload.get("internalDispatchId")),
            "admission_date_unix": optional_int(payload.get("admissionDate")),
        }
    except ValueError as exc:
        raise ReceptionPayloadError(f"cabecera inválida: {exc}") from None


def node_problem(node: Any) -> str | None:
    """``None`` = nodo ``details`` completo; si no, el motivo (nunca se interpreta como recepción vacía)."""
    if node is None:
        return "recepción sin nodo details"
    if not isinstance(node, dict):
        return "details no es objeto"
    items = node.get("items")
    if not isinstance(items, list):
        return "details no expandido"
    count = node.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        return "count de details ausente o inválido"
    if node.get("next"):
        return "details con paginación interna (next)"
    if node.get("offset") not in (None, 0):
        return f"details con offset {node.get('offset')}"
    if count != len(items):
        return f"details truncado ({len(items)} de {count})"
    return None


def build_details(reception_id: int, items: list[Any], count: int) -> tuple[DetailRow, ...]:
    if count != len(items):
        raise ReceptionPayloadError(f"recepción {reception_id}: {len(items)} líneas de {count}")
    rows: list[DetailRow] = []
    seen: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ReceptionPayloadError(f"recepción {reception_id}: línea no es objeto")
        try:
            detail_id = _bsale_id(item)
        except SnapshotValidationError:
            raise ReceptionPayloadError(f"recepción {reception_id}: línea sin id numérico") from None
        if detail_id in seen:
            raise ReceptionPayloadError(f"recepción {reception_id}: línea repetida id={detail_id}")
        seen.add(detail_id)
        try:
            variant_id = optional_relation_id(item.get("variant"))
            quantity = optional_numeric(item.get("quantity"))
            cost = optional_numeric(item.get("cost"))
        except ValueError as exc:
            raise ReceptionPayloadError(f"recepción {reception_id} línea {detail_id}: {exc}") from None
        if variant_id is None:
            raise ReceptionPayloadError(f"recepción {reception_id} línea {detail_id}: sin variant.id")
        rows.append(DetailRow(detail_id, variant_id, quantity, cost, item, payload_hash(item),
                              payload_hash(detail_content(item))))
    return tuple(rows)


def _check_page(endpoint: str, data: Any, offset: int) -> tuple[int, list[dict[str, Any]]]:
    if not isinstance(data, dict):
        raise SnapshotValidationError(f"{endpoint}: respuesta no es objeto en offset {offset}")
    count = data.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise SnapshotValidationError(f"{endpoint}: 'count' ausente o inválido en offset {offset}")
    items = data.get("items")
    if not isinstance(items, list) or any(not isinstance(it, dict) for it in items):
        raise SnapshotValidationError(f"{endpoint}: 'items' inválido en offset {offset}")
    return count, items


def day_timestamp(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


# --- plan de cambios (puro) -----------------------------------------------------------------------


@dataclass
class ReceptionPlan:
    kind: str  # created | modified | reappeared | unchanged | skipped_newer | deferred
    event: ChangeEvent | None = None


def plan_reception(
    bundle: ReceptionBundle,
    stored: StoredReception | None,
    stored_details: dict[int, StoredDetail],
    *,
    detected_by: str,
) -> ReceptionPlan:
    def event(kind: str, **kw: Any) -> ChangeEvent:
        return ChangeEvent(
            company_id=bundle.company_id, reception_id=bundle.reception_id, event_kind=kind,
            api_fetched_at=bundle.api_fetched_at, detected_by=detected_by,
            admission_date=bundle.typed.get("admission_date"),
            previous_content_hash=stored.content_hash if stored else None, content_hash=bundle.content_hash,
            previous_details_hash=stored.details_hash if stored else None, details_hash=bundle.details_hash,
            previous_version_hash=stored.version_hash if stored else None, version_hash=bundle.version_hash,
            **kw,
        )

    if stored is None:
        added = tuple(sorted(d.detail_id for d in bundle.details or ()))
        return ReceptionPlan("created", event(ReceptionEventKind.CREATED.value, header_changed=True,
                                              details_changed=bool(added), added=added))
    if stored.api_fetched_at > bundle.api_fetched_at:
        return ReceptionPlan("skipped_newer")
    if not bundle.complete:
        return ReceptionPlan("deferred")
    current = {k: v for k, v in stored_details.items() if v.missing_since is None}
    incoming = {d.detail_id: d for d in bundle.details or ()}
    added = tuple(sorted(set(incoming) - set(current)))
    removed = tuple(sorted(set(current) - set(incoming)))
    common = sorted(set(incoming) & set(current))
    quantity = tuple(i for i in common if incoming[i].quantity != current[i].quantity)
    cost = tuple(i for i in common if incoming[i].cost != current[i].cost)
    variant = tuple(i for i in common if incoming[i].variant_id != current[i].variant_id)
    header_changed = stored.content_hash != bundle.content_hash
    details_changed = (
        not stored.details_complete or stored.details_hash != bundle.details_hash
        or bool(added or removed or quantity or cost or variant)
    )
    changes = dict(header_changed=header_changed, details_changed=details_changed, added=added, removed=removed,
                   quantity_changed=quantity, cost_changed=cost, variant_changed=variant)
    if stored.missing_since is not None:
        return ReceptionPlan("reappeared", event(ReceptionEventKind.REAPPEARED.value, **changes))
    if header_changed or details_changed:
        return ReceptionPlan("modified", event(ReceptionEventKind.MODIFIED.value, **changes))
    return ReceptionPlan("unchanged")


def missing_event(company_id: int, stored: StoredReception, detected_by: str) -> ChangeEvent:
    return ChangeEvent(
        company_id=company_id, reception_id=stored.reception_id, event_kind=ReceptionEventKind.MISSING.value,
        api_fetched_at=None, detected_by=detected_by, admission_date=stored.admission_date,
        previous_content_hash=stored.content_hash, content_hash=stored.content_hash,
        previous_details_hash=stored.details_hash, details_hash=stored.details_hash,
        previous_version_hash=stored.version_hash, version_hash=stored.version_hash,
    )


def missing_fuse(present: int, missing: int, snapshot_size: int, threshold: float) -> dict[str, Any]:
    pct = round(missing * 100.0 / present, 2) if present else 0.0
    reason = None
    if snapshot_size == 0 and present:
        reason = f"día vacío con {present} recepciones guardadas"
    elif pct > threshold and missing > MIN_MISSING_FOR_FUSE:
        reason = f"{missing}/{present} recepciones pasarían a ausentes ({pct}%) > umbral {threshold}%"
    return {"threshold_pct": threshold, "present_existing": present, "would_mark_missing": missing,
            "missing_pct": pct, "tripped": reason is not None, "reason": reason}


# --- fallas persistentes --------------------------------------------------------------------------


def update_failures(
    failures: dict[int, dict[str, Any]], ok: set[int], failed: dict[int, str], now: datetime
) -> dict[int, dict[str, Any]]:
    out = {rid: dict(v) for rid, v in failures.items() if rid not in ok}
    stamp = now.isoformat()
    for rid, error in failed.items():
        prev = out.get(rid, {})
        out[rid] = {"attempts": int(prev.get("attempts", 0)) + 1, "first_failed_at": prev.get("first_failed_at", stamp),
                    "last_attempt_at": stamp, "error": error}
    return out


def failures_value(failures: dict[int, dict[str, Any]]) -> dict[str, Any]:
    return {"receptions": {str(rid): failures[rid] for rid in sorted(failures)}}


def failures_from_value(value: Any) -> dict[int, dict[str, Any]]:
    receptions = value.get("receptions") if isinstance(value, dict) else None
    if not isinstance(receptions, dict):
        return {}
    return {int(k): dict(v) for k, v in receptions.items() if str(k).isdigit() and isinstance(v, dict)}


# --- persistencia ---------------------------------------------------------------------------------

RECEPTION_TABLE = "bsale_raw.stock_receptions"
DETAIL_TABLE = "bsale_raw.stock_reception_details"
CHANGE_TABLE = "bsale_raw.stock_reception_change_log"

RECEPTION_TYPED = ("office_id", "admission_date", "document_number", "document", "user_id",
                   "internal_dispatch_id", "admission_date_unix")
RECEPTION_COLUMNS = (
    "company_id", "bsale_id", *RECEPTION_TYPED, "details_count", "details_complete", "details_hash", "version_hash",
    "details_source", "details_fetched_at", "payload", "payload_hash", "content_hash", "first_seen_at",
    "last_seen_at", "last_changed_at", "api_fetched_at", "missing_since", "last_source", "sync_run_id",
)
RECEPTION_TEMPLATE = "(" + ", ".join(
    ["%s", "%s", *("%s" for _ in RECEPTION_TYPED), "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s",
     "now()", "now()", "now()", "%s", "NULL", "%s", "%s"]
) + ")"

UPSERT_RECEPTIONS_SQL = (
    f"INSERT INTO {RECEPTION_TABLE} AS t ({', '.join(RECEPTION_COLUMNS)}) VALUES %s\n"
    "ON CONFLICT (company_id, bsale_id) DO UPDATE SET\n    "
    + ",\n    ".join([
        *(f"{c} = EXCLUDED.{c}" for c in RECEPTION_TYPED),
        "details_count = EXCLUDED.details_count",
        "details_complete = EXCLUDED.details_complete",
        "details_hash = EXCLUDED.details_hash",
        "version_hash = EXCLUDED.version_hash",
        "details_source = EXCLUDED.details_source",
        "details_fetched_at = EXCLUDED.details_fetched_at",
        "payload = EXCLUDED.payload",
        "payload_hash = EXCLUDED.payload_hash",
        "content_hash = EXCLUDED.content_hash",
        "last_seen_at = EXCLUDED.last_seen_at",
        "last_changed_at = CASE WHEN t.version_hash IS DISTINCT FROM EXCLUDED.version_hash "
        "THEN EXCLUDED.last_changed_at ELSE t.last_changed_at END",
        "api_fetched_at = EXCLUDED.api_fetched_at",
        "missing_since = NULL",
        "last_source = EXCLUDED.last_source",
        "sync_run_id = EXCLUDED.sync_run_id",
    ])
    + "\nWHERE t.api_fetched_at <= EXCLUDED.api_fetched_at\nRETURNING bsale_id"
)

# Recepción nueva con detalle pendiente: sólo se inserta; nunca pisa una fila existente.
INSERT_INCOMPLETE_SQL = (
    f"INSERT INTO {RECEPTION_TABLE} AS t ({', '.join(RECEPTION_COLUMNS)}) VALUES %s\n"
    "ON CONFLICT (company_id, bsale_id) DO NOTHING\nRETURNING bsale_id"
)

DETAIL_COLUMNS = (
    "company_id", "bsale_id", "reception_id", "variant_id", "quantity", "cost", "payload", "payload_hash",
    "content_hash", "first_seen_at", "last_seen_at", "last_changed_at", "api_fetched_at", "missing_since",
    "last_source", "sync_run_id",
)
DETAIL_TEMPLATE = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now(), now(), %s, NULL, %s, %s)"

# Sin condición de frescura por fila: la decide la cabecera (lock + frescura en la misma transacción).
UPSERT_DETAILS_SQL = (
    f"INSERT INTO {DETAIL_TABLE} AS t ({', '.join(DETAIL_COLUMNS)}) VALUES %s\n"
    "ON CONFLICT (company_id, reception_id, bsale_id) DO UPDATE SET\n    "
    + ",\n    ".join([
        "variant_id = EXCLUDED.variant_id",
        "quantity = EXCLUDED.quantity",
        "cost = EXCLUDED.cost",
        "payload = EXCLUDED.payload",
        "payload_hash = EXCLUDED.payload_hash",
        "content_hash = EXCLUDED.content_hash",
        "last_seen_at = EXCLUDED.last_seen_at",
        "last_changed_at = CASE WHEN t.content_hash IS DISTINCT FROM EXCLUDED.content_hash "
        "THEN EXCLUDED.last_changed_at ELSE t.last_changed_at END",
        "api_fetched_at = EXCLUDED.api_fetched_at",
        "missing_since = NULL",
        "last_source = EXCLUDED.last_source",
        "sync_run_id = EXCLUDED.sync_run_id",
    ])
)

MARK_DETAILS_MISSING_SQL = (
    f"UPDATE {DETAIL_TABLE} SET missing_since = now() "
    "WHERE company_id = %s AND reception_id = %s AND missing_since IS NULL "
    "AND NOT (bsale_id = ANY(%s::bigint[])) RETURNING bsale_id"
)

MARK_RECEPTIONS_MISSING_SQL = (
    f"UPDATE {RECEPTION_TABLE} SET missing_since = now() "
    "WHERE company_id = %s AND bsale_id = ANY(%s) AND missing_since IS NULL AND api_fetched_at <= %s "
    "RETURNING bsale_id"
)

_RECEPTION_SELECT = (
    "SELECT bsale_id, content_hash, details_hash, version_hash, api_fetched_at, missing_since, details_complete, "
    f"admission_date FROM {RECEPTION_TABLE} "
)
SELECT_RECEPTIONS_SQL = _RECEPTION_SELECT + "WHERE company_id = %s AND bsale_id = ANY(%s)"
SELECT_DAY_SQL = _RECEPTION_SELECT + "WHERE company_id = %s AND admission_date = %s"
SELECT_DETAILS_SQL = (
    f"SELECT reception_id, bsale_id, content_hash, variant_id, quantity, cost, missing_since FROM {DETAIL_TABLE} "
    "WHERE company_id = %s AND reception_id = ANY(%s)"
)
SELECT_INCOMPLETE_SQL = (
    f"SELECT bsale_id FROM {RECEPTION_TABLE} WHERE company_id = %s AND details_complete IS NOT TRUE "
    "AND missing_since IS NULL ORDER BY bsale_id LIMIT %s"
)
SELECT_CURSOR_SQL = (
    "SELECT cursor_value FROM bsale_raw.sync_cursors "
    "WHERE company_id = %s AND resource = %s AND scope = %s AND cursor_name = %s"
)
UPSERT_CURSOR_SQL = """
INSERT INTO bsale_raw.sync_cursors AS c
    (company_id, resource, scope, cursor_name, cursor_value, cycle_started_at, updated_at, sync_run_id)
VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
ON CONFLICT (company_id, resource, scope, cursor_name) DO UPDATE SET
    cursor_value = EXCLUDED.cursor_value,
    cycle_started_at = EXCLUDED.cycle_started_at,
    updated_at = now(),
    sync_run_id = EXCLUDED.sync_run_id
"""
INSERT_CHANGE_SQL = f"""
INSERT INTO {CHANGE_TABLE} (
    company_id, reception_id, event_kind, api_fetched_at, detected_by, sync_run_id, admission_date,
    previous_content_hash, content_hash, previous_details_hash, details_hash, previous_version_hash, version_hash,
    header_changed, details_changed, added_detail_ids, removed_detail_ids, quantity_changed_detail_ids,
    cost_changed_detail_ids, variant_changed_detail_ids
) VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s::bigint[], %s::bigint[], %s::bigint[],
    %s::bigint[], %s::bigint[]
)
"""
LOCK_RECEPTION_XACT = "SELECT pg_advisory_xact_lock(%s, %s)"


def reception_lock_scope(reception_id: int) -> str:
    return f"reception:{int(reception_id)}"


def _stored_receptions(rows: list[tuple]) -> dict[int, StoredReception]:
    return {
        int(r[0]): StoredReception(int(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7])
        for r in rows
    }


def _stored_details(rows: list[tuple]) -> dict[int, dict[int, StoredDetail]]:
    out: dict[int, dict[int, StoredDetail]] = {}
    for r in rows:
        variant = None if r[3] is None else int(r[3])
        out.setdefault(int(r[0]), {})[int(r[1])] = StoredDetail(int(r[1]), r[2], variant, r[4], r[5], r[6])
    return out


def _reception_values(bundles: list[ReceptionBundle], *, sync_run_id: int | None, last_source: str) -> list[tuple]:
    from psycopg2.extras import Json

    return [
        (
            b.company_id, b.reception_id, *(b.typed[c] for c in RECEPTION_TYPED),
            b.details_count, b.complete, b.details_hash, b.version_hash, b.details_source, b.details_fetched_at,
            Json(b.payload), b.payload_hash, b.content_hash, b.api_fetched_at, last_source, sync_run_id,
        )
        for b in bundles
    ]


class ReceptionTx(Protocol):
    def lock_receptions(
        self, company_id: int, ids: list[int]
    ) -> tuple[dict[int, StoredReception], dict[int, dict[int, StoredDetail]]]: ...

    def upsert_receptions(self, bundles: list[ReceptionBundle], *, sync_run_id: int | None, last_source: str) -> set[int]: ...

    def insert_incomplete(self, bundles: list[ReceptionBundle], *, sync_run_id: int | None, last_source: str) -> set[int]: ...

    def upsert_details(self, bundle: ReceptionBundle, *, sync_run_id: int | None, last_source: str) -> None: ...

    def mark_details_missing(self, company_id: int, reception_id: int, keep_ids: list[int]) -> list[int]: ...

    def mark_receptions_missing(self, company_id: int, ids: list[int], started_at: datetime) -> list[int]: ...

    def insert_change(self, event: ChangeEvent, *, sync_run_id: int | None) -> None: ...

    def write_cursor(
        self, company_id: int, scope: str, name: str, value: dict[str, Any], *, started_at: datetime,
        sync_run_id: int | None,
    ) -> None: ...

    def finish_success(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


class ReceptionStore(Protocol):
    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...

    def start_run(
        self, *, mode: str, trigger: str, host: str | None, company_id: int, resource: str, scope: str,
        state_scope: str | None = None,
    ) -> RunHandle: ...

    def finish_failed(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...

    def read_cursor(self, company_id: int, scope: str, name: str) -> dict[str, Any] | None: ...

    def read_receptions(
        self, company_id: int, ids: list[int]
    ) -> tuple[dict[int, StoredReception], dict[int, dict[int, StoredDetail]]]: ...

    def read_day_receptions(self, company_id: int, day: date) -> dict[int, StoredReception]: ...

    def read_incomplete_ids(self, company_id: int, limit: int) -> list[int]: ...

    def reception_transaction(self) -> Any: ...


@dataclass
class PgReceptionTx(PgRawTx):
    def lock_receptions(
        self, company_id: int, ids: list[int]
    ) -> tuple[dict[int, StoredReception], dict[int, dict[int, StoredDetail]]]:
        for rid in sorted(set(ids)):
            self.cur.execute(LOCK_RECEPTION_XACT, advisory_lock_keys(company_id, RESOURCE, reception_lock_scope(rid)))
        self.cur.execute(SELECT_RECEPTIONS_SQL + " FOR UPDATE", (company_id, list(ids)))
        stored = _stored_receptions(self.cur.fetchall())
        self.cur.execute(SELECT_DETAILS_SQL, (company_id, list(ids)))
        return stored, _stored_details(self.cur.fetchall())

    def _insert(self, sql: str, bundles: list[ReceptionBundle], sync_run_id: int | None, last_source: str) -> set[int]:
        if not bundles:
            return set()
        from psycopg2.extras import execute_values

        values = _reception_values(bundles, sync_run_id=sync_run_id, last_source=last_source)
        returned = execute_values(self.cur, sql, values, template=RECEPTION_TEMPLATE, page_size=500, fetch=True)
        return {int(r[0]) for r in returned}

    def upsert_receptions(self, bundles: list[ReceptionBundle], *, sync_run_id: int | None, last_source: str) -> set[int]:
        return self._insert(UPSERT_RECEPTIONS_SQL, bundles, sync_run_id, last_source)

    def insert_incomplete(self, bundles: list[ReceptionBundle], *, sync_run_id: int | None, last_source: str) -> set[int]:
        return self._insert(INSERT_INCOMPLETE_SQL, bundles, sync_run_id, last_source)

    def upsert_details(self, bundle: ReceptionBundle, *, sync_run_id: int | None, last_source: str) -> None:
        if not bundle.details:
            return
        from psycopg2.extras import Json, execute_values

        fetched = bundle.details_fetched_at or bundle.api_fetched_at
        values = [
            (bundle.company_id, d.detail_id, bundle.reception_id, d.variant_id, d.quantity, d.cost, Json(d.payload),
             d.payload_hash, d.content_hash, fetched, last_source, sync_run_id)
            for d in bundle.details
        ]
        execute_values(self.cur, UPSERT_DETAILS_SQL, values, template=DETAIL_TEMPLATE, page_size=500)

    def mark_details_missing(self, company_id: int, reception_id: int, keep_ids: list[int]) -> list[int]:
        self.cur.execute(MARK_DETAILS_MISSING_SQL, (company_id, reception_id, list(keep_ids)))
        return sorted(int(r[0]) for r in self.cur.fetchall())

    def mark_receptions_missing(self, company_id: int, ids: list[int], started_at: datetime) -> list[int]:
        if not ids:
            return []
        self.cur.execute(MARK_RECEPTIONS_MISSING_SQL, (company_id, list(ids), started_at))
        return sorted(int(r[0]) for r in self.cur.fetchall())

    def insert_change(self, event: ChangeEvent, *, sync_run_id: int | None) -> None:
        e = event
        self.cur.execute(
            INSERT_CHANGE_SQL,
            (
                e.company_id, e.reception_id, e.event_kind, e.api_fetched_at, e.detected_by, sync_run_id,
                e.admission_date, e.previous_content_hash, e.content_hash, e.previous_details_hash, e.details_hash,
                e.previous_version_hash, e.version_hash, e.header_changed, e.details_changed, list(e.added),
                list(e.removed), list(e.quantity_changed), list(e.cost_changed), list(e.variant_changed),
            ),
        )

    def write_cursor(
        self, company_id: int, scope: str, name: str, value: dict[str, Any], *, started_at: datetime,
        sync_run_id: int | None,
    ) -> None:
        from psycopg2.extras import Json

        self.cur.execute(UPSERT_CURSOR_SQL, (company_id, RESOURCE, scope, name, Json(value), started_at, sync_run_id))


class PgReceptionStore(PgRawStore):
    def _rows(self, sql: str, params: tuple) -> list[tuple]:
        cur = self._work().cursor()
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    def read_cursor(self, company_id: int, scope: str, name: str) -> dict[str, Any] | None:
        rows = self._rows(SELECT_CURSOR_SQL, (company_id, RESOURCE, scope, name))
        return dict(rows[0][0]) if rows and isinstance(rows[0][0], dict) else None

    def read_receptions(
        self, company_id: int, ids: list[int]
    ) -> tuple[dict[int, StoredReception], dict[int, dict[int, StoredDetail]]]:
        if not ids:
            return {}, {}
        stored = _stored_receptions(self._rows(SELECT_RECEPTIONS_SQL, (company_id, list(ids))))
        return stored, _stored_details(self._rows(SELECT_DETAILS_SQL, (company_id, list(ids))))

    def read_day_receptions(self, company_id: int, day: date) -> dict[int, StoredReception]:
        return _stored_receptions(self._rows(SELECT_DAY_SQL, (company_id, day)))

    def read_incomplete_ids(self, company_id: int, limit: int) -> list[int]:
        return [int(r[0]) for r in self._rows(SELECT_INCOMPLETE_SQL, (company_id, limit))]

    @contextmanager
    def reception_transaction(self) -> Iterator[PgReceptionTx]:
        with self.transaction() as tx:
            yield PgReceptionTx(tx.cur)


# --- HTTP -----------------------------------------------------------------------------------------


def _http_stats(client: Any) -> Any:
    return getattr(getattr(client, "session", None), "stats", None)


def _answered(exc: BaseException) -> bool:
    """La API respondió para ESTA recepción (404 / forma inválida): no indica caída ni cuota."""
    return isinstance(exc, (ReceptionPayloadError, SnapshotValidationError)) or (
        isinstance(exc, BsaleHttpError) and exc.status == 404
    )


@dataclass
class Fetcher:
    """Cliente + contadores + cortes (429, errores seguidos, tope de requests). Nunca hay transacción abierta."""

    client: Any
    company_id: int
    clock: Clock
    secrets: list[str]
    max_requests: int
    streak: int = 0
    stats: dict[str, int] = field(default_factory=lambda: {
        "expand_ok": 0, "fallback_used": 0, "fallback_ok": 0, "incomplete": 0, "lines": 0,
        "null_quantity_lines": 0, "null_cost_lines": 0, "zero_cost_lines": 0,
    })
    fallback_reasons: dict[int, str] = field(default_factory=dict)

    def requests(self) -> int:
        stats = _http_stats(self.client)
        return int(getattr(stats, "requests", 0) or 0)

    def guard(self) -> None:
        stats = _http_stats(self.client)
        if int(getattr(stats, "http_429", 0) or 0) >= MAX_HTTP_429:
            raise ReceptionAbort(f"{MAX_HTTP_429} respuestas 429: se detiene para no agotar la cuota compartida")
        if self.streak >= MAX_CONSECUTIVE_FAILURES:
            raise ReceptionAbort(f"{MAX_CONSECUTIVE_FAILURES} errores seguidos (caída, token o cuota)")
        if self.requests() >= self.max_requests:
            raise ReceptionAbort(f"tope de {self.max_requests} requests por corrida alcanzado")

    def get(self, endpoint: str, params: dict[str, Any]) -> Any:
        self.guard()
        return self.client.get_json(endpoint, params)

    def details_endpoint(self, reception_id: int) -> tuple[tuple[DetailRow, ...], int, datetime]:
        endpoint = DETAILS_ENDPOINT.format(id=reception_id)
        last: BaseException | None = None
        for _ in range(DETAIL_ATTEMPTS):
            self.guard()
            try:
                snapshot = fetch_snapshot(self.client, endpoint, clock=self.clock)
                rows = build_details(reception_id, [i.payload for i in snapshot.items], snapshot.api_count)
                fetched = max((i.fetched_at for i in snapshot.items), default=None) or self.clock()
                return rows, snapshot.api_count, fetched
            except (SnapshotValidationError, ReceptionPayloadError) as exc:
                last = exc  # count cambió entre páginas o línea inválida: un reintento
            except BsaleHttpError as exc:
                last = exc
                if exc.status == 404:
                    break
        assert last is not None
        raise last

    def bundle(self, item: dict[str, Any], fetched_at: datetime) -> ReceptionBundle:
        """Cabecera + líneas validadas. Si el detalle no se pudo completar: ``details=None`` y ``problem``."""
        reception_id = _bsale_id(item)
        typed = header_typed(item)
        base = dict(company_id=self.company_id, reception_id=reception_id, typed=typed, payload=item,
                    payload_hash=payload_hash(item), content_hash=payload_hash(header_content(item)),
                    api_fetched_at=fetched_at)
        node = item.get("details")
        problem = node_problem(node)
        if problem is None:
            try:
                rows = build_details(reception_id, node["items"], node["count"])
            except ReceptionPayloadError as exc:
                problem = f"expand inconsistente: {exc}"
            else:
                self.stats["expand_ok"] += 1
                return self._count(ReceptionBundle(**base, details=rows, details_count=node["count"],
                                                   details_source=SOURCE_EXPAND, details_fetched_at=fetched_at))
        self.stats["fallback_used"] += 1
        self.fallback_reasons[reception_id] = problem
        try:
            rows, count, details_at = self.details_endpoint(reception_id)
        except ReceptionAbort:
            raise
        except Exception as exc:
            self.streak = 0 if _answered(exc) else self.streak + 1
            self.stats["incomplete"] += 1
            return ReceptionBundle(**base, problem=f"{problem}; details.json: {sanitize_error(exc, self.secrets)}")
        self.streak = 0
        self.stats["fallback_ok"] += 1
        return self._count(ReceptionBundle(**base, details=rows, details_count=count, details_source=SOURCE_DETAILS,
                                           details_fetched_at=details_at))

    def _count(self, bundle: ReceptionBundle) -> ReceptionBundle:
        for d in bundle.details or ():
            self.stats["lines"] += 1
            self.stats["null_quantity_lines"] += d.quantity is None
            self.stats["null_cost_lines"] += d.cost is None
            self.stats["zero_cost_lines"] += d.cost is not None and d.cost == 0
        return bundle

    def item(self, reception_id: int) -> tuple[dict[str, Any], datetime]:
        data = self.get(ITEM_ENDPOINT.format(id=reception_id), dict(EXPAND_PARAMS))
        fetched_at = self.clock()
        if not isinstance(data, dict) or _bsale_id(data) != reception_id:
            raise SnapshotValidationError(f"recepción {reception_id}: respuesta de otra recepción o inválida")
        return data, fetched_at

    def day(self, day: date) -> list[tuple[dict[str, Any], datetime]]:
        """Snapshot estricto de UN día. Un ítem de otra fecha (filtro ignorado) invalida el día en esa página."""
        params = {**EXPAND_PARAMS, "admissiondate": day_timestamp(day)}
        out: list[tuple[dict[str, Any], datetime]] = []
        first: int | None = None
        offset = 0
        for _ in range(MAX_DAY_PAGES + 1):
            data = self.get(LIST_ENDPOINT, {**params, "limit": BSALE_PAGE_LIMIT, "offset": offset})
            fetched_at = self.clock()
            count, items = _check_page(LIST_ENDPOINT, data, offset)
            if first is None:
                first = count
                if count > MAX_DAY_PAGES * BSALE_PAGE_LIMIT:
                    raise SnapshotValidationError(f"{day}: count {count} sospechoso (¿filtro admissiondate ignorado?)")
            elif count != first:
                raise SnapshotValidationError(f"{day}: count cambió durante el snapshot ({first} -> {count})")
            for it in items:
                try:
                    item_day = optional_unix_date(it.get("admissionDate"))
                except ValueError:
                    item_day = None
                if item_day != day:
                    raise SnapshotValidationError(f"{day}: recepción de otra fecha en el listado (filtro ignorado)")
                out.append((it, fetched_at))
            if len(out) >= count:
                break
            if not items:
                raise SnapshotValidationError(f"{day}: respuesta truncada ({len(out)} de {count})")
            offset += len(items)
        else:
            raise SnapshotValidationError(f"{day}: se excedió el máximo de páginas")
        if len(out) != first:
            raise SnapshotValidationError(f"{day}: recibidas {len(out)} recepciones, count={first}")
        ids = [_bsale_id(it) for it, _ in out]
        if len(set(ids)) != len(ids):
            raise SnapshotValidationError(f"{day}: recepciones repetidas en el snapshot")
        return out


# --- aplicación ------------------------------------------------------------------------------------


COUNT_KINDS = ("created", "modified", "reappeared", "unchanged", "skipped_newer", "deferred")


@dataclass
class Progress:
    counts: dict[str, int] = field(default_factory=lambda: {k: 0 for k in COUNT_KINDS})
    events: dict[str, int] = field(default_factory=lambda: {k.value: 0 for k in ReceptionEventKind})
    details_missing: int = 0
    receptions_missing: int = 0
    ok: set[int] = field(default_factory=set)
    failed: dict[int, str] = field(default_factory=dict)
    received: int = 0


def _plans(
    bundles: list[ReceptionBundle], stored: dict[int, StoredReception], details: dict[int, dict[int, StoredDetail]],
    detected_by: str,
) -> dict[int, ReceptionPlan]:
    return {b.reception_id: plan_reception(b, stored.get(b.reception_id), details.get(b.reception_id, {}),
                                           detected_by=detected_by) for b in bundles}


def _track(progress: Progress, bundles: list[ReceptionBundle], plans: dict[int, ReceptionPlan]) -> None:
    for b in bundles:
        progress.received += 1
        if b.complete:
            progress.ok.add(b.reception_id)
        else:
            progress.failed[b.reception_id] = b.problem or "detalle incompleto"
        plan = plans[b.reception_id]
        progress.counts[plan.kind] += 1
        if plan.event is not None:
            progress.events[plan.event.event_kind] += 1


def simulate(store: ReceptionStore, company_id: int, bundles: list[ReceptionBundle], detected_by: str,
             progress: Progress) -> None:
    """Dry-run: mismo plan que la escritura real, leyendo sin locks."""
    if not bundles:
        return
    stored, details = store.read_receptions(company_id, [b.reception_id for b in bundles])
    plans = _plans(bundles, stored, details, detected_by)
    _track(progress, bundles, plans)
    for b in bundles:
        event = plans[b.reception_id].event
        if event is not None and b.complete:
            progress.details_missing += len(event.removed)


def apply_bundles(
    tx: ReceptionTx, company_id: int, bundles: list[ReceptionBundle], *, sync_run_id: int | None, mode: SyncMode,
    progress: Progress,
) -> None:
    if not bundles:
        return
    detected_by = last_source = mode.value
    stored, details = tx.lock_receptions(company_id, [b.reception_id for b in bundles])
    plans = _plans(bundles, stored, details, detected_by)
    complete = [b for b in bundles if b.complete and plans[b.reception_id].kind != "skipped_newer"]
    new_incomplete = [b for b in bundles if not b.complete and b.reception_id not in stored]
    applied = tx.upsert_receptions(complete, sync_run_id=sync_run_id, last_source=last_source)
    inserted = tx.insert_incomplete(new_incomplete, sync_run_id=sync_run_id, last_source=last_source)
    for b in bundles:
        plan = plans[b.reception_id]
        if b.reception_id in applied:
            tx.upsert_details(b, sync_run_id=sync_run_id, last_source=last_source)
            progress.details_missing += len(
                tx.mark_details_missing(company_id, b.reception_id, [d.detail_id for d in b.details or ()])
            )
        elif b.reception_id not in inserted and plan.kind not in ("deferred", "skipped_newer"):
            plan.kind, plan.event = "skipped_newer", None  # una versión más nueva llegó entre lock y UPSERT
        if plan.event is not None and (b.reception_id in applied or b.reception_id in inserted):
            tx.insert_change(plan.event, sync_run_id=sync_run_id)
    _track(progress, bundles, plans)


def mark_missing(
    tx: ReceptionTx, company_id: int, stored: dict[int, StoredReception], ids: list[int], started: datetime, *,
    sync_run_id: int | None, mode: SyncMode, progress: Progress,
) -> None:
    for rid in tx.mark_receptions_missing(company_id, ids, started):
        tx.insert_change(missing_event(company_id, stored[rid], mode.value), sync_run_id=sync_run_id)
        progress.receptions_missing += 1
        progress.events[ReceptionEventKind.MISSING.value] += 1


# --- orquestación ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class ReceptionRequest:
    mode: str
    date_from: date | None = None
    date_to: date | None = None
    reception_ids: tuple[int, ...] = ()
    limit: int = MAX_POINT
    max_requests: int | None = None

    def scope(self) -> str:
        if self.mode == MODE_DATES:
            return f"admission_date:{self.date_from}..{self.date_to}"
        if self.mode == MODE_POINT:
            ids = self.reception_ids
            return reception_lock_scope(ids[0]) if len(ids) == 1 else f"receptions:{len(ids)}"
        return "retry"


def validate_request(company_id: int, req: ReceptionRequest) -> None:
    if company_id not in RECEPTION_COMPANIES:
        raise UnsupportedSyncError(f"recepciones habilitadas sólo para company_id {sorted(RECEPTION_COMPANIES)}")
    if req.mode not in SYNC_MODE:
        raise UnsupportedSyncError(f"modalidad inválida: {req.mode!r}")
    if req.mode == MODE_DATES:
        if req.date_from is None or req.date_to is None or req.date_from > req.date_to:
            raise UnsupportedSyncError("rango de fechas inválido")
        if req.date_from < MIN_ADMISSION_DATE:
            raise UnsupportedSyncError(f"alcance de recepciones desde {MIN_ADMISSION_DATE.isoformat()}")
        if (req.date_to - req.date_from).days + 1 > MAX_DAYS:
            raise UnsupportedSyncError(f"máximo {MAX_DAYS} días por corrida")
    if req.mode == MODE_POINT and not 1 <= len(set(req.reception_ids)) <= MAX_POINT:
        raise UnsupportedSyncError(f"entre 1 y {MAX_POINT} recepciones por POINT")
    if any(r <= 0 for r in req.reception_ids):
        raise UnsupportedSyncError("ids de recepción deben ser positivos")
    if req.mode == MODE_RETRY and not 1 <= req.limit <= MAX_POINT:
        raise UnsupportedSyncError(f"--limit entre 1 y {MAX_POINT}")
    if req.max_requests is not None and req.max_requests < 1:
        raise UnsupportedSyncError("--max-requests debe ser >= 1")


def run_reception_sync(
    *,
    store: ReceptionStore,
    company_id: int,
    request: ReceptionRequest,
    dry_run: bool = False,
    client_factory: ReceptionClientFactory = default_reception_client_factory,
    clock: Clock = utc_now,
    monotonic: Callable[[], float] = time.perf_counter,
    getenv: Callable[[str], str | None] = os.getenv,
    trigger: str = TRIGGER_MANUAL,
    host: str | None = None,
) -> EntityOutcome:
    """Nunca lanza por fallas de API/BD: devuelve el ``EntityOutcome`` con status y error saneado."""
    validate_request(company_id, request)
    spec = reception_spec()
    mode = SYNC_MODE[request.mode]
    outcome = EntityOutcome(
        company_id=company_id, resource=RESOURCE, scope=request.scope(), mode=mode.value, dry_run=dry_run,
        state_scope=STATE_SCOPE[request.mode], point={"modality": request.mode},
    )
    t0 = monotonic()

    def elapsed_ms() -> int:
        return int((monotonic() - t0) * 1000)

    try:
        src = store.resolve_source(company_id)
        token = read_token(src, getenv)
        threshold = max_missing_pct(RESOURCE, getenv)
        rate = reception_rate_config(getenv)
        budget = request.max_requests or max_requests_config(getenv)
    except Exception as exc:
        return _fail(store, None, outcome, exc, [], elapsed_ms())
    secrets = [token]
    outcome.point.update(rps=rate.requests_per_second, max_requests=budget)

    # POINT: sin lock de corrida (cada recepción se bloquea en su transacción); el resto, uno por empresa.
    exclusive = request.mode != MODE_POINT and not dry_run
    lock = store.advisory_lock(company_id, RESOURCE, GLOBAL_SCOPE) if exclusive else nullcontext()
    try:
        with lock:
            return _run_locked(
                store=store, spec=spec, source=src, token=token, request=request, threshold=threshold,
                budget=budget, outcome=outcome, mode=mode, client_factory=client_factory, clock=clock,
                elapsed_ms=elapsed_ms, trigger=trigger, host=host, secrets=secrets,
            )
    except LockBusyError as exc:
        outcome.status = RunStatus.SKIPPED.value
        outcome.error = sanitize_error(exc, secrets)
        outcome.duration_ms = elapsed_ms()
        logger.warning("[BSALE_RAW] %s", outcome.error)
        return outcome
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())


@dataclass
class _Ctx:
    store: ReceptionStore
    company_id: int
    mode: SyncMode
    outcome: EntityOutcome
    fetcher: Fetcher
    handle: RunHandle | None
    started: datetime
    threshold: float
    progress: Progress = field(default_factory=Progress)
    failures: dict[int, dict[str, Any]] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    aborted: str | None = None

    @property
    def dry_run(self) -> bool:
        return self.outcome.dry_run

    @property
    def run_id(self) -> int | None:
        return None if self.handle is None else self.handle.run_id

    def next_failures(self) -> dict[int, dict[str, Any]]:
        return update_failures(self.failures, self.progress.ok, self.progress.failed, self.fetcher.clock())

    def write(self, bundles: list[ReceptionBundle], *, extra: Callable[[Any], None] | None = None,
              keep_failures: bool = True) -> None:
        """Una transacción: recepciones + líneas + bitácora (+ ausencias / cursor) + fallas acumuladas."""
        if self.dry_run:
            simulate(self.store, self.company_id, bundles, self.mode.value, self.progress)
            return
        with self.store.reception_transaction() as tx:
            apply_bundles(tx, self.company_id, bundles, sync_run_id=self.run_id, mode=self.mode, progress=self.progress)
            if extra is not None:
                extra(tx)
            if keep_failures:
                tx.write_cursor(self.company_id, GLOBAL_SCOPE, FAILURES_CURSOR, failures_value(self.next_failures()),
                                started_at=self.started, sync_run_id=self.run_id)


def _run_locked(
    *,
    store: ReceptionStore,
    spec: ResourceSpec,
    source: SourceConfig,
    token: str,
    request: ReceptionRequest,
    threshold: float,
    budget: int,
    outcome: EntityOutcome,
    mode: SyncMode,
    client_factory: ReceptionClientFactory,
    clock: Clock,
    elapsed_ms: Callable[[], int],
    trigger: str,
    host: str | None,
    secrets: list[str],
) -> EntityOutcome:
    company_id = source.company_id
    try:
        failures = failures_from_value(store.read_cursor(company_id, GLOBAL_SCOPE, FAILURES_CURSOR))
        retry_ids: list[int] = []
        if request.mode == MODE_RETRY:
            incomplete = store.read_incomplete_ids(company_id, request.limit)
            retry_ids = sorted(set(failures) | set(incomplete))[:request.limit]
    except Exception as exc:
        return _fail(store, None, outcome, exc, secrets, elapsed_ms())

    handle: RunHandle | None = None
    if not outcome.dry_run:
        try:
            handle = store.start_run(
                mode=mode.value, trigger=trigger, host=host, company_id=company_id, resource=RESOURCE,
                scope=outcome.scope, state_scope=outcome.state_scope,
            )
        except Exception as exc:
            return _fail(store, None, outcome, exc, secrets, elapsed_ms())
        outcome.sync_run_id = handle.run_id

    started = clock()
    outcome.snapshot_started_at = started
    client = None
    try:
        client = client_factory(source, token, spec)
        fetcher = Fetcher(client=client, company_id=company_id, clock=clock, secrets=secrets, max_requests=budget)
        ctx = _Ctx(store=store, company_id=company_id, mode=mode, outcome=outcome, fetcher=fetcher, handle=handle,
                   started=started, threshold=threshold, failures=failures)
        try:
            if request.mode == MODE_DATES:
                _run_dates(ctx, request)
            else:
                _run_point(ctx, list(dict.fromkeys(request.reception_ids)) if request.mode == MODE_POINT else retry_ids,
                           keep_failures=request.mode == MODE_RETRY)
        except ReceptionAbort as exc:
            ctx.aborted = str(exc)
    except Exception as exc:
        _collect_request_stats(client, outcome)
        failure = RuntimeError(f"{sanitize_error(exc, secrets)}; lo escrito en transacciones anteriores quedó guardado")
        return _fail(store, handle, outcome, failure, secrets, elapsed_ms())
    _collect_request_stats(client, outcome)
    _record(ctx)

    if outcome.dry_run:
        outcome.duration_ms = elapsed_ms()
        return outcome
    assert handle is not None
    try:
        with store.reception_transaction() as tx:
            outcome.duration_ms = elapsed_ms()
            tx.finish_success(handle, outcome)
    except Exception as exc:
        return _fail(store, handle, outcome, exc, secrets, elapsed_ms())
    _log(outcome)
    return outcome


def _bundle_or_fail(ctx: _Ctx, item: dict[str, Any], fetched_at: datetime) -> ReceptionBundle | None:
    try:
        return ctx.fetcher.bundle(item, fetched_at)
    except ReceptionAbort:
        raise
    except (ReceptionPayloadError, SnapshotValidationError) as exc:
        try:
            rid: int | None = _bsale_id(item)
        except SnapshotValidationError:
            rid = None
        if rid is not None:
            ctx.progress.failed[rid] = sanitize_error(exc, ctx.fetcher.secrets)
        else:
            ctx.problems.append(f"recepción sin id válido: {sanitize_error(exc, ctx.fetcher.secrets)}")
        return None


def _run_dates(ctx: _Ctx, req: ReceptionRequest) -> None:
    assert req.date_from is not None and req.date_to is not None
    days = [req.date_from + timedelta(days=i) for i in range((req.date_to - req.date_from).days + 1)]
    done: list[str] = []
    failed_days: dict[str, str] = {}
    fuses: dict[str, dict[str, Any]] = {}
    unverified: list[int] = []
    ctx.outcome.point.update(days=len(days), days_done=done, days_failed=failed_days, fuses=fuses)
    try:
        _dates_loop(ctx, days, done, failed_days, fuses, unverified)
    finally:
        finished = set(done)
        pending = [d for d in days if d.isoformat() not in finished]
        ctx.outcome.point["resume_from"] = pending[0].isoformat() if pending else None
    if failed_days:
        first = min(failed_days)
        ctx.problems.append(f"{len(failed_days)} días sin snapshot válido (p. ej. {first}: {failed_days[first]})")
    if fuses:
        ctx.problems.append(f"fusible de ausencias en {len(fuses)} días; no se marcó ninguna en esos días")
    if unverified:
        ctx.problems.append(f"{len(unverified)} recepciones ausentes sin confirmar (no se marcaron)")
    ctx.outcome.point["unverified_absent"] = unverified[:MAX_SAMPLE]


def _dates_loop(ctx: _Ctx, days: list[date], done: list[str], failed_days: dict[str, str],
                fuses: dict[str, dict[str, Any]], unverified: list[int]) -> None:
    for day in days:
        try:
            listing = ctx.fetcher.day(day)
        except ReceptionAbort:
            raise
        except Exception as exc:
            failed_days[day.isoformat()] = sanitize_error(exc, ctx.fetcher.secrets)
            ctx.fetcher.streak = 0 if _answered(exc) else ctx.fetcher.streak + 1
            continue
        ctx.fetcher.streak = 0
        ctx.outcome.pages += 1
        bundles = [b for b in (_bundle_or_fail(ctx, it, at) for it, at in listing) if b is not None]
        seen = {_bsale_id(it) for it, _ in listing}
        stored_day = ctx.store.read_day_receptions(ctx.company_id, day)
        present = {rid: s for rid, s in stored_day.items() if s.missing_since is None}
        absent = sorted(rid for rid, s in present.items() if rid not in seen and s.api_fetched_at <= ctx.started)
        fuse = missing_fuse(len(present), len(absent), len(listing), ctx.threshold)
        confirmed: list[int] = []
        if absent and fuse["tripped"]:
            fuses[day.isoformat()] = fuse
        elif absent:
            for rid in absent:  # se confirma una por una: 404 = ausente; otra fecha = se actualiza
                try:
                    item, at = ctx.fetcher.item(rid)
                except BsaleHttpError as exc:
                    if exc.status == 404:
                        confirmed.append(rid)
                    else:
                        unverified.append(rid)
                    continue
                except (SnapshotValidationError, ReceptionPayloadError):
                    unverified.append(rid)
                    continue
                bundle = _bundle_or_fail(ctx, item, at)
                if bundle is not None:
                    bundles.append(bundle)

        def extra(tx: Any, confirmed: list[int] = confirmed) -> None:
            mark_missing(tx, ctx.company_id, present, confirmed, ctx.started, sync_run_id=ctx.run_id, mode=ctx.mode,
                         progress=ctx.progress)

        ctx.write(bundles, extra=extra)
        if ctx.dry_run:
            ctx.progress.receptions_missing += len(confirmed)
        done.append(day.isoformat())


def _run_point(ctx: _Ctx, ids: list[int], *, keep_failures: bool) -> None:
    ctx.outcome.point.update(requested=len(ids), receptions=ids[:MAX_SAMPLE])
    bundles: list[ReceptionBundle] = []
    not_found: list[int] = []
    for rid in ids:
        try:
            item, at = ctx.fetcher.item(rid)
        except ReceptionAbort:
            raise
        except BsaleHttpError as exc:
            ctx.fetcher.streak = 0 if exc.status == 404 else ctx.fetcher.streak + 1
            if exc.status == 404:
                not_found.append(rid)
            else:
                ctx.progress.failed[rid] = sanitize_error(exc, ctx.fetcher.secrets)
            continue
        except (SnapshotValidationError, ReceptionPayloadError) as exc:
            ctx.progress.failed[rid] = sanitize_error(exc, ctx.fetcher.secrets)
            continue
        ctx.fetcher.streak = 0
        bundle = _bundle_or_fail(ctx, item, at)
        if bundle is not None:
            bundles.append(bundle)
    stored, _ = ctx.store.read_receptions(ctx.company_id, not_found) if not_found else ({}, {})
    gone = sorted(rid for rid, s in stored.items() if s.missing_since is None)
    ctx.progress.ok.update(not_found)

    def extra(tx: Any) -> None:
        mark_missing(tx, ctx.company_id, stored, gone, ctx.started, sync_run_id=ctx.run_id, mode=ctx.mode,
                     progress=ctx.progress)

    ctx.write(bundles, extra=extra, keep_failures=keep_failures)
    if ctx.dry_run:
        ctx.progress.receptions_missing += len(gone)
    ctx.outcome.point["not_found"] = not_found[:MAX_SAMPLE]


def _record(ctx: _Ctx) -> None:
    o, p, f = ctx.outcome, ctx.progress, ctx.fetcher
    o.rows_received = p.received
    o.rows_inserted = p.counts["created"]
    o.rows_updated = p.counts["modified"] + p.counts["reappeared"]
    o.rows_unchanged = p.counts["unchanged"]
    o.rows_skipped_newer = p.counts["skipped_newer"]
    o.rows_missing = p.receptions_missing
    failures = ctx.next_failures()
    o.point.update(
        **f.stats, **{k: v for k, v in p.counts.items()}, events=dict(p.events),
        details_missing=p.details_missing, receptions_missing=p.receptions_missing,
        failed=len(p.failed), failed_sample={str(k): p.failed[k] for k in sorted(p.failed)[:MAX_SAMPLE]},
        fallback_sample={str(k): f.fallback_reasons[k] for k in sorted(f.fallback_reasons)[:MAX_SAMPLE]},
        failures_pending=len(failures), aborted=ctx.aborted,
    )
    problems = list(ctx.problems)
    if p.failed:
        problems.append(f"{len(p.failed)} recepciones con detalle incompleto o inválido (p. ej. {sorted(p.failed)[:10]})")
    if ctx.aborted:
        problems.append(f"corte: {ctx.aborted}")
    if not problems:
        o.status, o.error = RunStatus.SUCCESS.value, None
    elif p.received == 0 and not p.receptions_missing and not o.point.get("days_done") and not o.pages:
        o.status, o.error = RunStatus.FAILED.value, "; ".join(problems)
    else:
        o.status, o.error = RunStatus.PARTIAL.value, "; ".join(problems)
    if o.point.get("fuses"):
        o.fuse = {"days": o.point["fuses"]}


def _log(outcome: EntityOutcome) -> None:
    p = outcome.point or {}
    logger.info(
        "[BSALE_RAW] company=%s resource=%s modality=%s status=%s received=%s created=%s modified=%s reappeared=%s "
        "unchanged=%s skipped_newer=%s deferred=%s fallback_used=%s incomplete=%s details_missing=%s "
        "receptions_missing=%s requests=%s http_429=%s duration_ms=%s",
        outcome.company_id, outcome.resource, p.get("modality"), outcome.status, outcome.rows_received,
        p.get("created"), p.get("modified"), p.get("reappeared"), p.get("unchanged"), p.get("skipped_newer"),
        p.get("deferred"), p.get("fallback_used"), p.get("incomplete"), p.get("details_missing"),
        p.get("receptions_missing"), outcome.requests, outcome.http_429, outcome.duration_ms,
    )


REPORT_FIELDS = (
    "modality", "days", "created", "modified", "reappeared", "unchanged", "skipped_newer", "deferred", "expand_ok",
    "fallback_used", "fallback_ok", "incomplete", "lines", "null_quantity_lines", "null_cost_lines", "zero_cost_lines",
    "details_missing", "receptions_missing", "failed", "failures_pending", "aborted", "resume_from",
)


def format_reception_outcome(outcome: EntityOutcome) -> str:
    """Sólo conteos, ids y fechas: nunca payload, note ni user."""
    p = outcome.point or {}
    lines = ["dry_run=true"] if outcome.dry_run else []
    lines += [f"company={outcome.company_id}", f"resource={outcome.resource}", f"scope={outcome.scope}",
              f"mode={outcome.mode}", f"received={outcome.rows_received}", f"pages={outcome.pages}"]
    for key in REPORT_FIELDS:
        if key in p:
            value = p[key]
            lines.append(f"{key}={'' if value is None else value}")
    if p.get("days_failed"):
        lines.append(f"days_failed={','.join(sorted(p['days_failed']))}")
    if p.get("not_found"):
        lines.append(f"not_found={','.join(map(str, p['not_found']))}")
    lines += [f"requests={outcome.requests}", f"http_429={outcome.http_429}", f"duration_ms={outcome.duration_ms}",
              f"status={outcome.status}"]
    if outcome.sync_run_id is not None:
        lines.append(f"sync_run_id={outcome.sync_run_id}")
    if outcome.error:
        lines.append(f"error={outcome.error}")
    return "\n".join(lines)
