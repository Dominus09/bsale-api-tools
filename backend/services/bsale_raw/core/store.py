"""Persistencia ``bsale_raw`` en PostgreSQL (psycopg2). Toda la SQL del motor vive aquí.

Conexiones:
- lock: conexión dedicada en autocommit con ``pg_try_advisory_lock(int, int)`` de sesión durante
  toda la corrida (no deja ninguna transacción abierta mientras se hacen requests HTTP);
- trabajo: autocommit para lecturas/registros cortos; ``transaction()`` abre la ÚNICA transacción
  de escritura de datos, después del fetch completo.

Seguridad: ``sources`` guarda el NOMBRE de la variable de entorno; el token nunca pasa por SQL.
"""

from __future__ import annotations

import hashlib
import logging
import re
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Iterator, Protocol

from backend.services.bsale_raw.core.reconcile import ExistingRow
from backend.services.bsale_raw.core.registry import ResourceSpec
from backend.services.bsale_raw.core.snapshot import RawRow, StockRow

logger = logging.getLogger(__name__)

ConnectionFactory = Callable[[], Any]

ADVISORY_LOCK_NAMESPACE = 0x42524157  # "BRAW"; espacio (int, int) separado del bigint legacy
_TABLE_RE = re.compile(r"^bsale_raw\.[a-z_]+$")


class SourceConfigError(RuntimeError):
    """Fuente inexistente, inactiva o inconsistente con bsale.companies."""


class LockBusyError(RuntimeError):
    """Otra corrida tiene el advisory lock de (company, resource, scope)."""


@dataclass(frozen=True)
class SourceConfig:
    company_id: int
    cpn_id: int
    name: str
    token_env: str


@dataclass(frozen=True)
class RunHandle:
    run_id: int
    entity_run_id: int
    started_at: datetime


@dataclass
class EntityOutcome:
    company_id: int
    resource: str
    scope: str
    mode: str
    status: str = "RUNNING"
    dry_run: bool = False
    snapshot_started_at: datetime | None = None
    api_count: int | None = None
    pages: int = 0
    rows_received: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_unchanged: int = 0
    rows_skipped_newer: int = 0
    rows_missing: int = 0
    rows_deleted: int = 0
    requests: int = 0
    http_429: int = 0
    http_5xx: int = 0
    duration_ms: int = 0
    fuse: dict[str, Any] | None = None
    error: str | None = None
    sync_run_id: int | None = None
    # Fila de sync_state cuando difiere del scope de la corrida (POINT: scope detallado por
    # variante en sync_entity_runs, una sola fila agregada en sync_state).
    state_scope: str | None = None
    # POINT: variantes pedidas y resultado por variante (queda en sync_runs.summary).
    point: dict[str, Any] | None = None

    @property
    def sync_state_scope(self) -> str:
        return self.state_scope or self.scope

    def summary(self) -> dict[str, Any]:
        data = asdict(self)
        if self.snapshot_started_at is not None:
            data["snapshot_started_at"] = self.snapshot_started_at.isoformat()
        return data


def advisory_lock_keys(company_id: int, resource: str, scope: str) -> tuple[int, int]:
    digest = hashlib.sha256(f"{int(company_id)}|{resource}|{scope}".encode("utf-8")).digest()
    return ADVISORY_LOCK_NAMESPACE, int.from_bytes(digest[:4], "big", signed=True)


def _table(spec: ResourceSpec) -> str:
    if not _TABLE_RE.match(spec.raw_table):
        raise ValueError(f"raw_table inválida: {spec.raw_table}")
    return spec.raw_table


def entity_columns(spec: ResourceSpec) -> list[str]:
    return [
        "company_id",
        "bsale_id",
        *(c.column for c in spec.typed_columns),
        "payload",
        "payload_hash",
        "first_seen_at",
        "last_seen_at",
        "last_changed_at",
        "api_fetched_at",
        "missing_since",
        "last_source",
        "sync_run_id",
    ]


def build_entity_upsert(spec: ResourceSpec) -> tuple[str, str]:
    """(sql, template) para ``execute_values``. Regla de frescura: nunca pisa una fila más nueva."""
    table = _table(spec)
    typed = [c.column for c in spec.typed_columns]
    cols = ", ".join(entity_columns(spec))
    template = "(" + ", ".join(
        ["%s", "%s", *("%s" for _ in typed), "%s", "%s", "now()", "now()", "now()", "%s", "NULL", "%s", "%s"]
    ) + ")"
    sets = [f"{c} = EXCLUDED.{c}" for c in typed]
    sets += [
        "payload = EXCLUDED.payload",
        "payload_hash = EXCLUDED.payload_hash",
        "last_seen_at = EXCLUDED.last_seen_at",
        "last_changed_at = CASE WHEN t.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash "
        "THEN EXCLUDED.last_changed_at ELSE t.last_changed_at END",
        "api_fetched_at = EXCLUDED.api_fetched_at",
        "missing_since = NULL",
        "last_source = EXCLUDED.last_source",
        "sync_run_id = EXCLUDED.sync_run_id",
    ]
    sql = (
        f"INSERT INTO {table} AS t ({cols}) VALUES %s\n"
        f"ON CONFLICT (company_id, bsale_id) DO UPDATE SET\n    "
        + ",\n    ".join(sets)
        + "\nWHERE t.api_fetched_at <= EXCLUDED.api_fetched_at\nRETURNING bsale_id"
    )
    return sql, template


def build_mark_missing(spec: ResourceSpec) -> str:
    """Entidades: marcar (nunca borrar) sólo filas no refrescadas desde el inicio del snapshot."""
    return (
        f"UPDATE {_table(spec)} SET missing_since = now() "
        "WHERE company_id = %s AND bsale_id = ANY(%s) "
        "AND missing_since IS NULL AND api_fetched_at <= %s"
    )


STOCK_KEY_COLUMNS = ("company_id", "variant_id", "office_id")
UPSERT_PAGE_SIZE = 500


def stock_columns(spec: ResourceSpec) -> list[str]:
    return [
        *STOCK_KEY_COLUMNS,
        *(c.column for c in spec.typed_columns),
        "payload",
        "payload_hash",
        "first_seen_at",
        "last_seen_at",
        "last_changed_at",
        "api_fetched_at",
        "last_source",
        "sync_run_id",
    ]


def build_stock_upsert(spec: ResourceSpec) -> tuple[str, str]:
    """
    (sql, template) para stock current-state. Misma regla de frescura que entidades: ningún
    scanner/reconcile pisa una fila obtenida después (webhook / targeted refresh). Devuelve las
    claves aplicadas; las omitidas son ``skipped_newer``. Sirve igual para un refresh dirigido
    (``variantid`` y/o ``officeid``), que escribe con el mismo SQL.
    """
    table = _table(spec)
    typed = [c.column for c in spec.typed_columns]
    cols = ", ".join(stock_columns(spec))
    template = "(" + ", ".join(
        ["%s", "%s", "%s", *("%s" for _ in typed), "%s", "%s", "now()", "now()", "now()", "%s", "%s", "%s"]
    ) + ")"
    sets = [f"{c} = EXCLUDED.{c}" for c in typed]
    sets += [
        "payload = EXCLUDED.payload",
        "payload_hash = EXCLUDED.payload_hash",
        "last_seen_at = EXCLUDED.last_seen_at",
        "last_changed_at = CASE WHEN t.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash "
        "THEN EXCLUDED.last_changed_at ELSE t.last_changed_at END",
        "api_fetched_at = EXCLUDED.api_fetched_at",
        "last_source = EXCLUDED.last_source",
        "sync_run_id = EXCLUDED.sync_run_id",
    ]
    sql = (
        f"INSERT INTO {table} AS t ({cols}) VALUES %s\n"
        f"ON CONFLICT ({', '.join(STOCK_KEY_COLUMNS)}) DO UPDATE SET\n    "
        + ",\n    ".join(sets)
        + "\nWHERE t.api_fetched_at <= EXCLUDED.api_fetched_at\nRETURNING variant_id, office_id"
    )
    return sql, template


def build_stock_delete_stale(spec: ResourceSpec) -> str:
    """Sólo FULL_RECONCILE de UNA sucursal tras snapshot estricto; re-chequea frescura al borrar."""
    return (
        f"DELETE FROM {_table(spec)} "
        "WHERE company_id = %s AND office_id = %s AND variant_id = ANY(%s) AND api_fetched_at <= %s"
    )


_SELECT_EXISTING_STOCK = (
    "SELECT variant_id, office_id, payload_hash, api_fetched_at FROM {table} "
    "WHERE company_id = %s AND office_id = %s"
)

# POINT: todas las sucursales de las variantes pedidas en UNA consulta (usa la PK).
_SELECT_EXISTING_STOCK_VARIANTS = (
    "SELECT variant_id, office_id, payload_hash, api_fetched_at FROM {table} "
    "WHERE company_id = %s AND variant_id = ANY(%s)"
)

_SELECT_EXISTING = "SELECT bsale_id, payload_hash, api_fetched_at, missing_since FROM {table} WHERE company_id = %s"

_RESOLVE_SOURCE = """
SELECT s.company_id, s.cpn_id, s.name, s.token_env, s.active, c.bsale_token
FROM bsale_raw.sources s
LEFT JOIN bsale.companies c ON c.company_id = s.company_id
WHERE s.company_id = %s
"""

_UPDATE_ENTITY_RUN = """
UPDATE bsale_raw.sync_entity_runs SET
    status = %s, snapshot_started_at = %s, finished_at = now(),
    rows_received = %s, rows_inserted = %s, rows_updated = %s, rows_unchanged = %s,
    rows_skipped_newer = %s, rows_missing = %s, rows_deleted = %s,
    api_count = %s, requests = %s, http_429 = %s, http_5xx = %s,
    duration_ms = %s, fuse = %s, error = %s
WHERE id = %s
"""

_UPDATE_RUN = """
UPDATE bsale_raw.sync_runs SET status = %s, finished_at = now(), summary = %s, error = %s
WHERE id = %s
"""

_STATE_START = """
INSERT INTO bsale_raw.sync_state AS s
    (company_id, resource, scope, last_attempt_at, status, last_sync_run_id, updated_at)
VALUES (%s, %s, %s, %s, 'RUNNING', %s, now())
ON CONFLICT (company_id, resource, scope) DO UPDATE SET
    last_attempt_at = EXCLUDED.last_attempt_at,
    status = EXCLUDED.status,
    last_sync_run_id = EXCLUDED.last_sync_run_id,
    updated_at = now()
"""

_STATE_SUCCESS = """
INSERT INTO bsale_raw.sync_state AS s
    (company_id, resource, scope, last_attempt_at, last_success_at, last_full_reconcile_at,
     rows_received, duration_ms, status, last_sync_run_id, updated_at)
VALUES (%s, %s, %s, %s, now(), CASE WHEN %s THEN now() END, %s, %s, %s, %s, now())
ON CONFLICT (company_id, resource, scope) DO UPDATE SET
    last_attempt_at = EXCLUDED.last_attempt_at,
    last_success_at = EXCLUDED.last_success_at,
    last_full_reconcile_at = COALESCE(EXCLUDED.last_full_reconcile_at, s.last_full_reconcile_at),
    rows_received = EXCLUDED.rows_received,
    duration_ms = EXCLUDED.duration_ms,
    status = EXCLUDED.status,
    last_sync_run_id = EXCLUDED.last_sync_run_id,
    updated_at = now()
"""

# last_success_at no se toca en una falla.
_STATE_FAILED = """
INSERT INTO bsale_raw.sync_state AS s
    (company_id, resource, scope, last_attempt_at, last_error_at, last_error,
     rows_received, duration_ms, status, last_sync_run_id, updated_at)
VALUES (%s, %s, %s, %s, now(), %s, %s, %s, %s, %s, now())
ON CONFLICT (company_id, resource, scope) DO UPDATE SET
    last_attempt_at = EXCLUDED.last_attempt_at,
    last_error_at = EXCLUDED.last_error_at,
    last_error = EXCLUDED.last_error,
    rows_received = EXCLUDED.rows_received,
    duration_ms = EXCLUDED.duration_ms,
    status = EXCLUDED.status,
    last_sync_run_id = EXCLUDED.last_sync_run_id,
    updated_at = now()
"""


class RawTx(Protocol):
    def lock_existing(self, spec: ResourceSpec, company_id: int) -> dict[int, ExistingRow]: ...

    def upsert(self, spec: ResourceSpec, rows: list[RawRow], *, sync_run_id: int, last_source: str) -> set[int]: ...

    def mark_missing(
        self, spec: ResourceSpec, company_id: int, bsale_ids: list[int], snapshot_started_at: datetime
    ) -> int: ...

    def read_existing_stock(
        self, spec: ResourceSpec, company_id: int, office_id: int
    ) -> dict[tuple[int, int], ExistingRow]: ...

    def read_existing_stock_variants(
        self, spec: ResourceSpec, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]: ...

    def upsert_stock(
        self, spec: ResourceSpec, rows: list[StockRow], *, sync_run_id: int | None, last_source: str
    ) -> set[tuple[int, int]]: ...

    def delete_stale_stock(
        self, spec: ResourceSpec, company_id: int, office_id: int, variant_ids: list[int], snapshot_started_at: datetime
    ) -> int: ...

    def finish_success(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


class RawStore(Protocol):
    def resolve_source(self, company_id: int) -> SourceConfig: ...

    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Any: ...

    def start_run(
        self, *, mode: str, trigger: str, host: str | None, company_id: int, resource: str, scope: str,
        state_scope: str | None = None,
    ) -> RunHandle: ...

    def read_existing(self, spec: ResourceSpec, company_id: int) -> dict[int, ExistingRow]: ...

    def read_existing_stock_variants(
        self, spec: ResourceSpec, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]: ...

    def read_existing_stock(
        self, spec: ResourceSpec, company_id: int, office_id: int
    ) -> dict[tuple[int, int], ExistingRow]: ...

    def transaction(self) -> Any: ...

    def finish_failed(self, handle: RunHandle, outcome: EntityOutcome) -> None: ...


def _json(value: Any) -> Any:
    from psycopg2.extras import Json

    return None if value is None else Json(value)


def _entity_run_params(outcome: EntityOutcome, entity_run_id: int) -> tuple:
    return (
        outcome.status,
        outcome.snapshot_started_at,
        outcome.rows_received,
        outcome.rows_inserted,
        outcome.rows_updated,
        outcome.rows_unchanged,
        outcome.rows_skipped_newer,
        outcome.rows_missing,
        outcome.rows_deleted,
        outcome.api_count,
        outcome.requests,
        outcome.http_429,
        outcome.http_5xx,
        outcome.duration_ms,
        _json(outcome.fuse),
        outcome.error,
        entity_run_id,
    )


def _existing_from_rows(rows: list[tuple]) -> dict[int, ExistingRow]:
    return {
        int(r[0]): ExistingRow(bsale_id=int(r[0]), payload_hash=r[1], api_fetched_at=r[2], missing_since=r[3])
        for r in rows
    }


def _existing_stock_from_rows(rows: list[tuple]) -> dict[tuple[int, int], ExistingRow]:
    return {
        (int(r[0]), int(r[1])): ExistingRow(bsale_id=int(r[0]), payload_hash=r[2], api_fetched_at=r[3], missing_since=None)
        for r in rows
    }


def _read_existing_stock(cur: Any, spec: ResourceSpec, company_id: int, office_id: int) -> dict[tuple[int, int], ExistingRow]:
    cur.execute(_SELECT_EXISTING_STOCK.format(table=_table(spec)), (company_id, office_id))
    return _existing_stock_from_rows(cur.fetchall())


def _read_existing_stock_variants(
    cur: Any, spec: ResourceSpec, company_id: int, variant_ids: list[int]
) -> dict[tuple[int, int], ExistingRow]:
    if not variant_ids:
        return {}
    cur.execute(_SELECT_EXISTING_STOCK_VARIANTS.format(table=_table(spec)), (company_id, list(variant_ids)))
    return _existing_stock_from_rows(cur.fetchall())


@dataclass
class PgRawTx:
    cur: Any

    def lock_existing(self, spec: ResourceSpec, company_id: int) -> dict[int, ExistingRow]:
        self.cur.execute(_SELECT_EXISTING.format(table=_table(spec)) + " FOR UPDATE", (company_id,))
        return _existing_from_rows(self.cur.fetchall())

    def upsert(self, spec: ResourceSpec, rows: list[RawRow], *, sync_run_id: int, last_source: str) -> set[int]:
        if not rows:
            return set()
        from psycopg2.extras import Json, execute_values

        sql, template = build_entity_upsert(spec)
        values = [
            (
                r.company_id,
                r.bsale_id,
                *(r.typed[c.column] for c in spec.typed_columns),
                Json(r.payload),
                r.payload_hash,
                r.api_fetched_at,
                last_source,
                sync_run_id,
            )
            for r in rows
        ]
        returned = execute_values(self.cur, sql, values, template=template, page_size=UPSERT_PAGE_SIZE, fetch=True)
        return {int(r[0]) for r in returned}

    def read_existing_stock(
        self, spec: ResourceSpec, company_id: int, office_id: int
    ) -> dict[tuple[int, int], ExistingRow]:
        """Lectura MVCC sin ``FOR UPDATE``: no bloquea refresh dirigidos; la frescura la garantiza el SQL."""
        return _read_existing_stock(self.cur, spec, company_id, office_id)

    def read_existing_stock_variants(
        self, spec: ResourceSpec, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]:
        return _read_existing_stock_variants(self.cur, spec, company_id, variant_ids)

    def upsert_stock(
        self, spec: ResourceSpec, rows: list[StockRow], *, sync_run_id: int | None, last_source: str
    ) -> set[tuple[int, int]]:
        if not rows:
            return set()
        from psycopg2.extras import Json, execute_values

        sql, template = build_stock_upsert(spec)
        values = [
            (
                r.company_id,
                r.variant_id,
                r.office_id,
                *(r.typed[c.column] for c in spec.typed_columns),
                Json(r.payload),
                r.payload_hash,
                r.api_fetched_at,
                last_source,
                sync_run_id,
            )
            for r in rows
        ]
        returned = execute_values(self.cur, sql, values, template=template, page_size=UPSERT_PAGE_SIZE, fetch=True)
        return {(int(r[0]), int(r[1])) for r in returned}

    def delete_stale_stock(
        self, spec: ResourceSpec, company_id: int, office_id: int, variant_ids: list[int], snapshot_started_at: datetime
    ) -> int:
        if not variant_ids:
            return 0
        self.cur.execute(
            build_stock_delete_stale(spec), (company_id, office_id, list(variant_ids), snapshot_started_at)
        )
        return int(self.cur.rowcount or 0)

    def mark_missing(
        self, spec: ResourceSpec, company_id: int, bsale_ids: list[int], snapshot_started_at: datetime
    ) -> int:
        if not bsale_ids:
            return 0
        self.cur.execute(build_mark_missing(spec), (company_id, list(bsale_ids), snapshot_started_at))
        return int(self.cur.rowcount or 0)

    def finish_success(self, handle: RunHandle, outcome: EntityOutcome) -> None:
        self.cur.execute(_UPDATE_ENTITY_RUN, _entity_run_params(outcome, handle.entity_run_id))
        self.cur.execute(_UPDATE_RUN, (outcome.status, _json(outcome.summary()), None, handle.run_id))
        self.cur.execute(
            _STATE_SUCCESS,
            (
                outcome.company_id,
                outcome.resource,
                outcome.sync_state_scope,
                handle.started_at,
                outcome.mode == "FULL_RECONCILE",
                outcome.rows_received,
                outcome.duration_ms,
                outcome.status,
                handle.run_id,
            ),
        )


class PgRawStore:
    def __init__(
        self,
        connection_factory: ConnectionFactory | None = None,
        *,
        read_only: bool = False,
        lock_timeout: str = "10s",
        statement_timeout: str = "120s",
    ) -> None:
        if connection_factory is None:
            from backend.db import get_connection

            connection_factory = get_connection
        self._factory = connection_factory
        self.read_only = read_only
        self._lock_timeout = lock_timeout
        self._statement_timeout = statement_timeout
        self._conn: Any = None

    def _work(self) -> Any:
        if self._conn is None:
            conn = self._factory()
            if self.read_only:
                conn.set_session(readonly=True, autocommit=True)
            else:
                conn.autocommit = True
            self._conn = conn
        return self._conn

    def _require_writable(self) -> None:
        if self.read_only:
            raise RuntimeError("PgRawStore en modo sólo lectura (dry-run)")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def resolve_source(self, company_id: int) -> SourceConfig:
        cur = self._work().cursor()
        cur.execute(_RESOLVE_SOURCE, (company_id,))
        row = cur.fetchone()
        cur.close()
        if row is None:
            raise SourceConfigError(f"company_id={company_id} no existe en bsale_raw.sources")
        cid, cpn_id, name, token_env, active, legacy_token_env = row
        if not active:
            raise SourceConfigError(f"company_id={company_id} inactiva en bsale_raw.sources")
        if (legacy_token_env or "").strip() != (token_env or "").strip():
            raise SourceConfigError(
                f"company_id={company_id}: sources.token_env no coincide con bsale.companies.bsale_token"
            )
        return SourceConfig(company_id=int(cid), cpn_id=int(cpn_id), name=name or "", token_env=token_env)

    @contextmanager
    def advisory_lock(self, company_id: int, resource: str, scope: str) -> Iterator[None]:
        key1, key2 = advisory_lock_keys(company_id, resource, scope)
        conn = self._factory()
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("SELECT pg_try_advisory_lock(%s, %s)", (key1, key2))
            row = cur.fetchone()
            if not row or not row[0]:
                raise LockBusyError(f"lock ocupado company_id={company_id} resource={resource} scope={scope}")
            try:
                yield
            finally:
                try:
                    cur.execute("SELECT pg_advisory_unlock(%s, %s)", (key1, key2))
                except Exception:
                    logger.exception("[BSALE_RAW] no se pudo liberar advisory lock (se libera al cerrar la conexión)")
        finally:
            conn.close()

    def start_run(
        self, *, mode: str, trigger: str, host: str | None, company_id: int, resource: str, scope: str,
        state_scope: str | None = None,
    ) -> RunHandle:
        self._require_writable()
        with self.transaction() as tx:
            cur = tx.cur
            cur.execute(
                "INSERT INTO bsale_raw.sync_runs (mode, trigger, status, host) "
                "VALUES (%s, %s, 'RUNNING', %s) RETURNING id, started_at",
                (mode, trigger, host),
            )
            run_id, started_at = cur.fetchone()
            cur.execute(
                "INSERT INTO bsale_raw.sync_entity_runs (sync_run_id, company_id, resource, scope, status) "
                "VALUES (%s, %s, %s, %s, 'RUNNING') RETURNING id",
                (run_id, company_id, resource, scope),
            )
            (entity_run_id,) = cur.fetchone()
            cur.execute(_STATE_START, (company_id, resource, state_scope or scope, started_at, run_id))
        return RunHandle(run_id=int(run_id), entity_run_id=int(entity_run_id), started_at=started_at)

    def read_existing(self, spec: ResourceSpec, company_id: int) -> dict[int, ExistingRow]:
        cur = self._work().cursor()
        cur.execute(_SELECT_EXISTING.format(table=_table(spec)), (company_id,))
        rows = cur.fetchall()
        cur.close()
        return _existing_from_rows(rows)

    def read_existing_stock(
        self, spec: ResourceSpec, company_id: int, office_id: int
    ) -> dict[tuple[int, int], ExistingRow]:
        cur = self._work().cursor()
        try:
            return _read_existing_stock(cur, spec, company_id, office_id)
        finally:
            cur.close()

    def read_existing_stock_variants(
        self, spec: ResourceSpec, company_id: int, variant_ids: list[int]
    ) -> dict[tuple[int, int], ExistingRow]:
        cur = self._work().cursor()
        try:
            return _read_existing_stock_variants(cur, spec, company_id, variant_ids)
        finally:
            cur.close()

    @contextmanager
    def transaction(self) -> Iterator[PgRawTx]:
        self._require_writable()
        conn = self._work()
        conn.autocommit = False
        cur = conn.cursor()
        try:
            cur.execute("SET LOCAL lock_timeout = %s", (self._lock_timeout,))
            cur.execute("SET LOCAL statement_timeout = %s", (self._statement_timeout,))
            yield PgRawTx(cur)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.autocommit = True

    def finish_failed(self, handle: RunHandle, outcome: EntityOutcome) -> None:
        with self.transaction() as tx:
            cur = tx.cur
            cur.execute(_UPDATE_ENTITY_RUN, _entity_run_params(outcome, handle.entity_run_id))
            cur.execute(_UPDATE_RUN, (outcome.status, _json(outcome.summary()), outcome.error, handle.run_id))
            cur.execute(
                _STATE_FAILED,
                (
                    outcome.company_id,
                    outcome.resource,
                    outcome.sync_state_scope,
                    handle.started_at,
                    outcome.error,
                    outcome.rows_received,
                    outcome.duration_ms,
                    outcome.status,
                    handle.run_id,
                ),
            )
