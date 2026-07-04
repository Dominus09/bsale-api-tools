"""Persistencia devoluciones Bsale — bsale.returns / return_details."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from psycopg2.extras import Json

logger = logging.getLogger(__name__)

RETURNS = "bsale.returns"
RETURN_DETAILS = "bsale.return_details"
SYNC_STATE = "bsale.returns_sync_state"
SYNC_RUNS = "bsale.returns_sync"

_SCHEMA = "bsale"


def _schema_log(message: str) -> None:
    logger.info("[RETURNS_SCHEMA] %s", message)


def _table_exists(cur, qualified_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (qualified_name,))
    return cur.fetchone()[0] is not None


def _column_exists(cur, *, schema: str, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
          AND column_name = %s
        """,
        (schema, table, column),
    )
    return cur.fetchone() is not None


def _index_exists(cur, *, schema: str, index_name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = %s
          AND indexname = %s
        """,
        (schema, index_name),
    )
    return cur.fetchone() is not None


def _constraint_exists(cur, *, schema: str, table: str, conname: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE n.nspname = %s
          AND t.relname = %s
          AND c.conname = %s
        """,
        (schema, table, conname),
    )
    return cur.fetchone() is not None


def _ensure_schema(cur) -> None:
    cur.execute(
        """
        SELECT 1 FROM information_schema.schemata WHERE schema_name = %s
        """,
        (_SCHEMA,),
    )
    if cur.fetchone():
        _schema_log(f"Esquema existente: {_SCHEMA}")
        return
    cur.execute(f"CREATE SCHEMA {_SCHEMA}")
    _schema_log(f"Esquema creado: {_SCHEMA}")


def _ensure_table(cur, qualified: str, create_sql: str) -> None:
    if _table_exists(cur, qualified):
        _schema_log(f"Tabla existente: {qualified}")
        return
    cur.execute(create_sql)
    _schema_log(f"Tabla creada: {qualified}")


def _ensure_column(
    cur,
    *,
    schema: str,
    table: str,
    column: str,
    add_sql: str,
) -> None:
    if _column_exists(cur, schema=schema, table=table, column=column):
        _schema_log(f"Columna existente: {schema}.{table}.{column}")
        return
    cur.execute(add_sql)
    _schema_log(f"Columna creada: {schema}.{table}.{column}")


def _ensure_index(cur, *, schema: str, index_name: str, create_sql: str) -> None:
    if _index_exists(cur, schema=schema, index_name=index_name):
        _schema_log(f"Índice existente: {schema}.{index_name}")
        return
    cur.execute(create_sql)
    _schema_log(f"Índice creado: {schema}.{index_name}")


def _ensure_constraint(
    cur,
    *,
    schema: str,
    table: str,
    conname: str,
    add_sql: str,
) -> None:
    if _constraint_exists(cur, schema=schema, table=table, conname=conname):
        _schema_log(f"Constraint existente: {schema}.{table}.{conname}")
        return
    cur.execute(add_sql)
    _schema_log(f"Constraint creado: {schema}.{table}.{conname}")


def ensure_returns_schema(cur) -> None:
    """DDL idempotente para tablas de devoluciones Bsale — seguro ejecutar N veces."""
    _ensure_schema(cur)

    _ensure_table(
        cur,
        f"{_SCHEMA}.returns_sync_state",
        f"""
        CREATE TABLE {_SCHEMA}.returns_sync_state (
            company_id     INTEGER NOT NULL,
            office_id      INTEGER NOT NULL DEFAULT 1,
            last_return_ts BIGINT,
            last_sync_at   TIMESTAMPTZ,
            records_total  BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (company_id, office_id)
        )
        """,
    )

    _ensure_table(
        cur,
        f"{_SCHEMA}.returns",
        f"""
        CREATE TABLE {_SCHEMA}.returns (
            company_id              INTEGER NOT NULL,
            office_id               INTEGER NOT NULL DEFAULT 1,
            bsale_id                BIGINT NOT NULL,
            code                    TEXT,
            return_date             TIMESTAMPTZ,
            motive                  TEXT,
            return_type             INTEGER,
            amount                  NUMERIC(18, 4) NOT NULL DEFAULT 0,
            price_adjustment        NUMERIC(18, 4) NOT NULL DEFAULT 0,
            edit_texts              INTEGER NOT NULL DEFAULT 0,
            reference_document_id   BIGINT,
            reference_document_number BIGINT,
            reference_document_type_id INTEGER,
            credit_note_id          BIGINT,
            credit_note_number      BIGINT,
            client_id               BIGINT,
            client_name             TEXT,
            seller_id               INTEGER,
            seller_name             TEXT,
            municipality            TEXT,
            credit_note_emission    TIMESTAMPTZ,
            reference_emission      TIMESTAMPTZ,
            raw_data                JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            synced_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
    )
    _ensure_constraint(
        cur,
        schema=_SCHEMA,
        table="returns",
        conname="returns_company_bsale_unique",
        add_sql=f"""
            ALTER TABLE {_SCHEMA}.returns
            ADD CONSTRAINT returns_company_bsale_unique UNIQUE (company_id, bsale_id)
        """,
    )

    for index_name, index_sql in (
        (
            "idx_bsale_returns_company_office_date",
            f"""
            CREATE INDEX idx_bsale_returns_company_office_date
                ON {_SCHEMA}.returns (company_id, office_id, return_date DESC)
            """,
        ),
        (
            "idx_bsale_returns_motive",
            f"CREATE INDEX idx_bsale_returns_motive ON {_SCHEMA}.returns (company_id, motive)",
        ),
        (
            "idx_bsale_returns_seller",
            f"CREATE INDEX idx_bsale_returns_seller ON {_SCHEMA}.returns (company_id, seller_id)",
        ),
        (
            "idx_bsale_returns_client",
            f"CREATE INDEX idx_bsale_returns_client ON {_SCHEMA}.returns (company_id, client_id)",
        ),
    ):
        _ensure_index(cur, schema=_SCHEMA, index_name=index_name, create_sql=index_sql)

    _ensure_table(
        cur,
        f"{_SCHEMA}.return_details",
        f"""
        CREATE TABLE {_SCHEMA}.return_details (
            company_id           INTEGER NOT NULL,
            return_id            BIGINT NOT NULL,
            bsale_detail_id      BIGINT NOT NULL,
            document_detail_id   BIGINT,
            variant_id           BIGINT,
            product_name         TEXT,
            variant_description  TEXT,
            quantity             NUMERIC(18, 4) NOT NULL DEFAULT 0,
            unit_value           NUMERIC(18, 4) NOT NULL DEFAULT 0,
            total_amount         NUMERIC(18, 4) NOT NULL DEFAULT 0,
            raw_data             JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            synced_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (company_id, return_id, bsale_detail_id)
        )
        """,
    )
    _ensure_index(
        cur,
        schema=_SCHEMA,
        index_name="idx_bsale_return_details_variant",
        create_sql=f"""
            CREATE INDEX idx_bsale_return_details_variant
                ON {_SCHEMA}.return_details (company_id, variant_id)
        """,
    )

    _ensure_table(
        cur,
        f"{_SCHEMA}.returns_sync",
        f"""
        CREATE TABLE {_SCHEMA}.returns_sync (
            id                  SERIAL PRIMARY KEY,
            company_id          INTEGER NOT NULL,
            office_id           INTEGER NOT NULL DEFAULT 1,
            sync_type           TEXT NOT NULL,
            date_from           DATE,
            date_to             DATE,
            last_return_date    TIMESTAMPTZ,
            last_return_id      BIGINT,
            pages_processed     INTEGER NOT NULL DEFAULT 0,
            records_processed   INTEGER NOT NULL DEFAULT 0,
            started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            finished_at         TIMESTAMPTZ,
            duration_ms         BIGINT,
            status              TEXT NOT NULL DEFAULT 'running',
            error_message       TEXT
        )
        """,
    )
    _ensure_constraint(
        cur,
        schema=_SCHEMA,
        table="returns_sync",
        conname="returns_sync_sync_type_check",
        add_sql=f"""
            ALTER TABLE {_SCHEMA}.returns_sync
            ADD CONSTRAINT returns_sync_sync_type_check
            CHECK (sync_type IN ('history', 'incremental'))
        """,
    )
    _ensure_constraint(
        cur,
        schema=_SCHEMA,
        table="returns_sync",
        conname="returns_sync_status_check",
        add_sql=f"""
            ALTER TABLE {_SCHEMA}.returns_sync
            ADD CONSTRAINT returns_sync_status_check
            CHECK (status IN ('running', 'completed', 'failed', 'no_data'))
        """,
    )
    _ensure_index(
        cur,
        schema=_SCHEMA,
        index_name="idx_bsale_returns_sync_lookup",
        create_sql=f"""
            CREATE INDEX idx_bsale_returns_sync_lookup
                ON {_SCHEMA}.returns_sync (company_id, office_id, sync_type, status, started_at DESC)
        """,
    )


def get_sync_state(cur, *, company_id: int, office_id: int) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT company_id, office_id, last_return_ts, last_sync_at, records_total
        FROM {SYNC_STATE}
        WHERE company_id = %s AND office_id = %s
        """,
        (company_id, office_id),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def upsert_sync_state(
    cur,
    *,
    company_id: int,
    office_id: int,
    last_return_ts: int | None,
    records_delta: int,
) -> None:
    cur.execute(
        f"""
        INSERT INTO {SYNC_STATE} (company_id, office_id, last_return_ts, last_sync_at, records_total)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (company_id, office_id) DO UPDATE SET
            last_return_ts = GREATEST({SYNC_STATE}.last_return_ts, EXCLUDED.last_return_ts),
            last_sync_at = NOW(),
            records_total = {SYNC_STATE}.records_total + EXCLUDED.records_total
        """,
        (company_id, office_id, last_return_ts, records_delta),
    )


def _row_dict(cur, row) -> dict[str, Any]:
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def create_sync_run(
    cur,
    *,
    company_id: int,
    office_id: int,
    sync_type: str,
    date_from,
    date_to,
) -> int:
    cur.execute(
        f"""
        INSERT INTO {SYNC_RUNS} (
            company_id, office_id, sync_type, date_from, date_to, status
        ) VALUES (%s, %s, %s, %s, %s, 'running')
        RETURNING id
        """,
        (company_id, office_id, sync_type, date_from, date_to),
    )
    return int(cur.fetchone()[0])


def update_sync_run_progress(
    cur,
    sync_id: int,
    *,
    pages_processed: int,
    records_processed: int,
    last_return_date: datetime | None,
    last_return_id: int | None,
) -> None:
    cur.execute(
        f"""
        UPDATE {SYNC_RUNS}
        SET pages_processed = %s,
            records_processed = %s,
            last_return_date = %s,
            last_return_id = %s
        WHERE id = %s
        """,
        (pages_processed, records_processed, last_return_date, last_return_id, sync_id),
    )


def finish_sync_run(
    cur,
    sync_id: int,
    *,
    status: str,
    duration_ms: int,
    error_message: str | None = None,
) -> None:
    cur.execute(
        f"""
        UPDATE {SYNC_RUNS}
        SET status = %s,
            finished_at = NOW(),
            duration_ms = %s,
            error_message = %s
        WHERE id = %s
        """,
        (status, duration_ms, error_message, sync_id),
    )


def get_completed_history_sync(
    cur,
    *,
    company_id: int,
    office_id: int,
    date_from,
    date_to,
) -> dict[str, Any] | None:
    """Bootstrap histórico exitoso: completed con al menos un registro procesado."""
    cur.execute(
        f"""
        SELECT *
        FROM {SYNC_RUNS}
        WHERE company_id = %s AND office_id = %s
          AND sync_type = 'history'
          AND status = 'completed'
          AND records_processed > 0
          AND date_from = %s AND date_to = %s
        ORDER BY finished_at DESC NULLS LAST
        LIMIT 1
        """,
        (company_id, office_id, date_from, date_to),
    )
    row = cur.fetchone()
    return _row_dict(cur, row) if row else None


def get_resumable_history_sync(
    cur,
    *,
    company_id: int,
    office_id: int,
    date_from,
    date_to,
) -> dict[str, Any] | None:
    cur.execute(
        f"""
        SELECT *
        FROM {SYNC_RUNS}
        WHERE company_id = %s AND office_id = %s
          AND sync_type = 'history'
          AND status IN ('running', 'failed')
          AND date_from = %s AND date_to = %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (company_id, office_id, date_from, date_to),
    )
    row = cur.fetchone()
    return _row_dict(cur, row) if row else None


def list_sync_runs(
    cur,
    *,
    company_id: int,
    office_id: int,
    limit: int = 10,
) -> list[dict[str, Any]]:
    cur.execute(
        f"""
        SELECT *
        FROM {SYNC_RUNS}
        WHERE company_id = %s AND office_id = %s
        ORDER BY started_at DESC
        LIMIT %s
        """,
        (company_id, office_id, limit),
    )
    return [_row_dict(cur, r) for r in cur.fetchall()]


def upsert_return(cur, row: dict[str, Any]) -> None:
    cur.execute(
        f"""
        INSERT INTO {RETURNS} (
            company_id, office_id, bsale_id, code, return_date, motive, return_type,
            amount, price_adjustment, edit_texts,
            reference_document_id, reference_document_number, reference_document_type_id,
            credit_note_id, credit_note_number,
            client_id, client_name, seller_id, seller_name, municipality,
            credit_note_emission, reference_emission, raw_data, synced_at, updated_at
        ) VALUES (
            %(company_id)s, %(office_id)s, %(bsale_id)s, %(code)s, %(return_date)s,
            %(motive)s, %(return_type)s, %(amount)s, %(price_adjustment)s, %(edit_texts)s,
            %(reference_document_id)s, %(reference_document_number)s, %(reference_document_type_id)s,
            %(credit_note_id)s, %(credit_note_number)s,
            %(client_id)s, %(client_name)s, %(seller_id)s, %(seller_name)s, %(municipality)s,
            %(credit_note_emission)s, %(reference_emission)s, %(raw_data)s, NOW(), NOW()
        )
        ON CONFLICT (company_id, bsale_id) DO UPDATE SET
            code = EXCLUDED.code,
            return_date = EXCLUDED.return_date,
            motive = EXCLUDED.motive,
            return_type = EXCLUDED.return_type,
            amount = EXCLUDED.amount,
            price_adjustment = EXCLUDED.price_adjustment,
            edit_texts = EXCLUDED.edit_texts,
            reference_document_id = EXCLUDED.reference_document_id,
            reference_document_number = EXCLUDED.reference_document_number,
            reference_document_type_id = EXCLUDED.reference_document_type_id,
            credit_note_id = EXCLUDED.credit_note_id,
            credit_note_number = EXCLUDED.credit_note_number,
            client_id = EXCLUDED.client_id,
            client_name = EXCLUDED.client_name,
            seller_id = EXCLUDED.seller_id,
            seller_name = EXCLUDED.seller_name,
            municipality = EXCLUDED.municipality,
            credit_note_emission = EXCLUDED.credit_note_emission,
            reference_emission = EXCLUDED.reference_emission,
            raw_data = EXCLUDED.raw_data,
            synced_at = NOW(),
            updated_at = NOW()
        """,
        {**row, "raw_data": Json(row.get("raw_data") or {})},
    )


def replace_return_details(
    cur,
    *,
    company_id: int,
    return_id: int,
    details: list[dict[str, Any]],
) -> None:
    cur.execute(
        f"DELETE FROM {RETURN_DETAILS} WHERE company_id = %s AND return_id = %s",
        (company_id, return_id),
    )
    for d in details:
        cur.execute(
            f"""
            INSERT INTO {RETURN_DETAILS} (
                company_id, return_id, bsale_detail_id, document_detail_id,
                variant_id, product_name, variant_description,
                quantity, unit_value, total_amount, raw_data, synced_at
            ) VALUES (
                %(company_id)s, %(return_id)s, %(bsale_detail_id)s, %(document_detail_id)s,
                %(variant_id)s, %(product_name)s, %(variant_description)s,
                %(quantity)s, %(unit_value)s, %(total_amount)s, %(raw_data)s, NOW()
            )
            ON CONFLICT (company_id, return_id, bsale_detail_id) DO UPDATE SET
                document_detail_id = EXCLUDED.document_detail_id,
                variant_id = EXCLUDED.variant_id,
                product_name = EXCLUDED.product_name,
                variant_description = EXCLUDED.variant_description,
                quantity = EXCLUDED.quantity,
                unit_value = EXCLUDED.unit_value,
                total_amount = EXCLUDED.total_amount,
                raw_data = EXCLUDED.raw_data,
                synced_at = NOW()
            """,
            {**d, "raw_data": Json(d.get("raw_data") or {})},
        )


def _period_filter(
    date_from: datetime | None,
    date_to: datetime | None,
    *,
    alias: str = "r",
) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    if date_from:
        parts.append(f"{alias}.return_date >= %s")
        params.append(date_from)
    if date_to:
        parts.append(f"{alias}.return_date <= %s")
        params.append(date_to)
    sql = (" AND " + " AND ".join(parts)) if parts else ""
    return sql, params
