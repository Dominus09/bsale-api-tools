"""
Sincronización incremental Bsale → distribuidora.documents / distribuidora.document_details.

- company_id = 3 y office_id = 1 (filtrado en API y en consultas).
- Clave lógica de documento: (document_type_id, number); ``document_id`` de Bsale puede cambiar.
- Token: ``BSALE_TOKEN`` o, si no existe, ``BSALE_TOKEN_SPA`` (Coolify).
- Rango de emisión: último sync_state.last_sync − 2 h hasta ahora (timestamps UNIX en Bsale).
- Detalles: documentos sin filas en document_details, más re-sync forzado si cambió ``document_id``.
- Re-sync histórico solo documentos: ``resync_bsale_documents_full()`` (POST ``/erp/resync-distribuidora``).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import requests
from psycopg2.extras import Json, execute_values

from backend.db import get_connection

logger = logging.getLogger(__name__)

# Evita spam en logs cada 30 min si falta el token en el job programado
_token_missing_logged = False

BASE_BSALE = "https://api.bsale.io/v1"


def _bsale_token_distribuidora() -> str:
    """Token Bsale: prioridad BSALE_TOKEN, alternativa BSALE_TOKEN_SPA."""
    return (os.getenv("BSALE_TOKEN") or "").strip() or (os.getenv("BSALE_TOKEN_SPA") or "").strip()


COMPANY_ID = 3
OFFICE_ID = 1
LIMIT_BSALE = 50
MAX_TRANSIENT = 40
_ADVISORY_LOCK_KEY = 5_927_184_003

# Documentos sin detalle: no escanear toda la historia en el primer sync
_FIRST_SYNC_CUTOFF = datetime(2010, 1, 1, tzinfo=timezone.utc)

# Índices fijos en la tupla de _doc_row_from_bsale (no usar row[-1] por columnas nuevas al final).
_ROW_DOCUMENT_TYPE_ID = 2
_ROW_NUMBER = 11

# Un ejemplo de JSON documento por ejecución de sync (logs).
_document_raw_sample_logged = False


def _ensure_distribuidora_tables(cur) -> None:
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS distribuidora.sync_state (
            id SERIAL PRIMARY KEY,
            last_sync TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '2000-01-01 00:00:00+00'
        )
        """
    )
    # Tabla creada antes sin last_sync: migración idempotente
    cur.execute(
        """
        ALTER TABLE distribuidora.sync_state
        ADD COLUMN IF NOT EXISTS last_sync TIMESTAMPTZ NOT NULL
            DEFAULT TIMESTAMPTZ '2000-01-01 00:00:00+00'
        """
    )
    cur.execute(
        """
        UPDATE distribuidora.sync_state
        SET last_sync = TIMESTAMPTZ '2000-01-01 00:00:00+00'
        WHERE last_sync IS NULL
        """
    )
    cur.execute(
        """
        INSERT INTO distribuidora.sync_state (last_sync)
        SELECT TIMESTAMPTZ '2000-01-01 00:00:00+00'
        WHERE NOT EXISTS (SELECT 1 FROM distribuidora.sync_state LIMIT 1)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS distribuidora.documents (
            document_id BIGINT PRIMARY KEY,
            emission_date TIMESTAMPTZ,
            document_type_id INTEGER,
            client_id INTEGER,
            vendedor_id INTEGER,
            total_amount NUMERIC(18, 4),
            state INTEGER,
            url_pdf TEXT,
            token TEXT,
            office_id INTEGER NOT NULL,
            company_id INTEGER NOT NULL DEFAULT 3,
            number BIGINT,
            document_type_name TEXT,
            reference TEXT,
            expiration_date TIMESTAMPTZ,
            raw_data JSONB
        )
        """
    )
    cur.execute(
        """
        ALTER TABLE distribuidora.documents
        ADD COLUMN IF NOT EXISTS number BIGINT
        """
    )
    cur.execute(
        """
        ALTER TABLE distribuidora.documents
        ADD COLUMN IF NOT EXISTS document_type_name TEXT
        """
    )
    cur.execute(
        """
        ALTER TABLE distribuidora.documents
        ADD COLUMN IF NOT EXISTS reference TEXT
        """
    )
    cur.execute(
        """
        ALTER TABLE distribuidora.documents
        ADD COLUMN IF NOT EXISTS expiration_date TIMESTAMPTZ
        """
    )
    cur.execute(
        """
        ALTER TABLE distribuidora.documents
        ADD COLUMN IF NOT EXISTS raw_data JSONB
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_distribuidora_documents_logical
        ON distribuidora.documents (company_id, office_id, document_type_id, number)
        WHERE document_type_id IS NOT NULL AND number IS NOT NULL
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_distribuidora_documents_emission
        ON distribuidora.documents (emission_date)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS distribuidora.document_details (
            detail_id BIGINT PRIMARY KEY,
            document_id BIGINT NOT NULL,
            line_number INTEGER,
            variant_id BIGINT,
            quantity NUMERIC(18, 4),
            net_unit_value NUMERIC(18, 4),
            total_unit_value NUMERIC(18, 4),
            net_amount NUMERIC(18, 4),
            tax_amount NUMERIC(18, 4),
            total_amount NUMERIC(18, 4),
            net_discount NUMERIC(18, 4),
            discount_percentage NUMERIC(10, 4)
        )
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_distribuidora_details_document
        ON distribuidora.document_details (document_id)
        """
    )


def _num(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, Decimal)):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _bsale_get(
    session: requests.Session,
    url: str,
    token: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    transient = 0
    params = params or {}
    while True:
        try:
            r = session.get(url, headers={"access_token": token}, params=params, timeout=45)
        except requests.RequestException as e:
            transient += 1
            if transient >= MAX_TRANSIENT:
                raise RuntimeError(f"Bsale red: {e}") from e
            logger.warning("Bsale red (%s/%s): %s — reintento 3s", transient, MAX_TRANSIENT, e)
            time.sleep(3)
            continue

        if r.status_code == 401:
            raise RuntimeError("Bsale 401 Unauthorized — revisar BSALE_TOKEN o BSALE_TOKEN_SPA")

        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            logger.warning("Bsale 429 — esperando %s s", wait)
            time.sleep(wait)
            continue

        if r.status_code in (500, 502, 503, 504):
            transient += 1
            if transient >= MAX_TRANSIENT:
                raise RuntimeError(f"Bsale HTTP {r.status_code} persistente")
            logger.warning("Bsale HTTP %s — reintento 3s", r.status_code)
            time.sleep(3)
            continue

        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"Bsale HTTP {r.status_code}: {(r.text or '')[:500]}")

        transient = 0
        return r.json()


def _reference_from_bsale(d: dict[str, Any]) -> str | None:
    ref = d.get("reference")
    if ref is not None and ref != "":
        if isinstance(ref, str):
            return ref
        return str(ref)
    refs = d.get("references")
    if refs is None or refs == "":
        return None
    if isinstance(refs, str):
        return refs
    if isinstance(refs, list):
        if not refs:
            return None
        parts: list[str] = []
        for x in refs:
            if isinstance(x, dict):
                parts.append(json.dumps(x, ensure_ascii=False, separators=(",", ":")))
            else:
                parts.append(str(x))
        return "; ".join(parts)
    return str(refs)


def _expiration_from_bsale(d: dict[str, Any]) -> datetime | None:
    raw = d.get("expirationDate")
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _log_sample_document_raw_data(d: dict[str, Any]) -> None:
    """Log de un documento Bsale en JSON (recorte) para inspección manual."""
    try:
        sample = json.dumps(d, ensure_ascii=False, indent=2, default=str)
    except TypeError:
        sample = repr(d)
    max_len = 8000
    if len(sample) > max_len:
        sample = sample[:max_len] + "\n... [truncado]"
    logger.info("ejemplo raw_data documento Bsale (sync distribuidora):\n%s", sample)


def _doc_row_from_bsale(d: dict[str, Any]) -> tuple[Any, ...]:
    emission_raw = d.get("emissionDate")
    emission_date = None
    if emission_raw is not None:
        emission_date = datetime.fromtimestamp(int(emission_raw), tz=timezone.utc)
    doc_type = d.get("document_type") or {}
    return (
        int(d["id"]),
        emission_date,
        doc_type.get("id"),
        (d.get("client") or {}).get("id"),
        (d.get("user") or {}).get("id"),
        _num(d.get("totalAmount")),
        d.get("state"),
        d.get("urlPdf"),
        d.get("token"),
        OFFICE_ID,
        COMPANY_ID,
        d.get("number"),
        doc_type.get("name"),
        _reference_from_bsale(d),
        _expiration_from_bsale(d),
        Json(d),
    )


def _detail_row(document_id: int, item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        int(item["id"]),
        document_id,
        item.get("lineNumber"),
        (item.get("variant") or {}).get("id"),
        _num(item.get("quantity")),
        _num(item.get("netUnitValue")),
        _num(item.get("totalUnitValue")),
        _num(item.get("netAmount")),
        _num(item.get("taxAmount")),
        _num(item.get("totalAmount")),
        _num(item.get("netDiscount")),
        _num(item.get("discountPercentage")),
    )


def _load_existing_documents_by_logical(
    cur,
    logical_keys: list[tuple[int, int]],
    document_ids: list[int],
) -> dict[tuple[int, int], int]:
    """Por (document_type_id, number) → ``document_id`` actual en DB."""
    by_logical: dict[tuple[int, int], int] = {}
    if not logical_keys and not document_ids:
        return by_logical
    t_logical = [k[0] for k in logical_keys]
    n_logical = [k[1] for k in logical_keys]
    doc_ids = list(dict.fromkeys(document_ids))
    cur.execute(
        """
        SELECT d.document_id, d.document_type_id, d.number
        FROM distribuidora.documents d
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND (
              (cardinality(%s::bigint[]) > 0 AND d.document_id = ANY(%s::bigint[]))
              OR (
                  cardinality(%s::int[]) > 0
                  AND (d.document_type_id, d.number) IN (
                      SELECT x.tid, x.num
                      FROM unnest(%s::int[], %s::bigint[]) AS x(tid, num)
                  )
              )
          )
        """,
        (
            COMPANY_ID,
            OFFICE_ID,
            doc_ids,
            doc_ids,
            t_logical,
            t_logical,
            n_logical,
        ),
    )
    for doc_id, dt, num in cur.fetchall():
        if dt is None or num is None:
            continue
        try:
            key = (int(dt), int(num))
            by_logical[key] = int(doc_id)
        except (TypeError, ValueError):
            continue
    return by_logical


def _delete_stale_document_rows_holding_new_ids(
    cur, new_ids: list[int], logical_keys: list[tuple[int, int]]
) -> int:
    """
    Elimina filas que ocupan un ``document_id`` que vamos a asignar por clave lógica,
    pero con otra clave (evita violación de PK antes del UPDATE).
    """
    if not new_ids or not logical_keys:
        return 0
    t_ex = [k[0] for k in logical_keys]
    n_ex = [k[1] for k in logical_keys]
    cur.execute(
        """
        DELETE FROM distribuidora.documents d
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_id = ANY(%s::bigint[])
          AND NOT EXISTS (
              SELECT 1
              FROM unnest(%s::int[], %s::bigint[]) AS ex(tid, num)
              WHERE ex.tid IS NOT DISTINCT FROM d.document_type_id
                AND ex.num IS NOT DISTINCT FROM d.number
          )
        """,
        (COMPANY_ID, OFFICE_ID, new_ids, t_ex, n_ex),
    )
    return cur.rowcount


def _batch_update_documents_by_logical_key(cur, rows: list[tuple[Any, ...]]) -> int:
    """UPDATE ... WHERE (document_type_id, number); sin INSERT."""
    if not rows:
        return 0
    cur.execute(
        """
        UPDATE distribuidora.documents d
        SET document_id = v.document_id,
            emission_date = v.emission_date,
            document_type_id = v.document_type_id,
            client_id = v.client_id,
            vendedor_id = v.vendedor_id,
            total_amount = v.total_amount,
            state = v.state,
            url_pdf = v.url_pdf,
            token = v.token,
            office_id = v.office_id,
            company_id = v.company_id,
            number = v.number,
            document_type_name = v.document_type_name,
            reference = v.reference,
            expiration_date = v.expiration_date,
            raw_data = v.raw_data
        FROM unnest(
            %s::bigint[],
            %s::timestamptz[],
            %s::int[],
            %s::int[],
            %s::int[],
            %s::numeric[],
            %s::int[],
            %s::text[],
            %s::text[],
            %s::int[],
            %s::int[],
            %s::bigint[],
            %s::text[],
            %s::text[],
            %s::timestamptz[],
            %s::jsonb[]
        ) AS v(
            document_id, emission_date, document_type_id, client_id, vendedor_id,
            total_amount, state, url_pdf, token, office_id, company_id, number,
            document_type_name, reference, expiration_date, raw_data
        )
        WHERE d.company_id = %s
          AND d.office_id = %s
          AND d.document_type_id IS NOT DISTINCT FROM v.document_type_id
          AND d.number IS NOT DISTINCT FROM v.number
        """,
        (
            [int(r[0]) for r in rows],
            [r[1] for r in rows],
            [int(r[2]) for r in rows],
            [r[3] for r in rows],
            [r[4] for r in rows],
            [r[5] for r in rows],
            [r[6] for r in rows],
            [r[7] for r in rows],
            [r[8] for r in rows],
            [r[9] for r in rows],
            [r[10] for r in rows],
            [int(r[11]) for r in rows],
            [r[12] for r in rows],
            [r[13] for r in rows],
            [r[14] for r in rows],
            [r[15] for r in rows],
            COMPANY_ID,
            OFFICE_ID,
        ),
    )
    return cur.rowcount


def _upsert_documents_logical_batch(
    cur, rows: list[tuple[Any, ...]]
) -> tuple[int, int, list[tuple[int, int]]]:
    """
    Resuelve clave lógica (document_type_id, number) vs PK (document_id) en Python:

    * Sin fila lógica → INSERT … ON CONFLICT (document_id) DO UPDATE
    * Misma PK → mismo upsert por PK
    * PK distinta → UPDATE por (document_type_id, number) solamente (sin INSERT)

    Devuelve (insertados, actualizados, pares (document_id_antiguo, document_id_nuevo)).
    """
    id_changes: list[tuple[int, int]] = []
    if not rows:
        return 0, 0, id_changes

    keys: list[tuple[int, int]] = []
    nids: list[int] = []
    for row in rows:
        try:
            tid = int(row[_ROW_DOCUMENT_TYPE_ID])
            num = int(row[_ROW_NUMBER])
            nid = int(row[0])
        except (TypeError, ValueError):
            continue
        keys.append((tid, num))
        nids.append(nid)

    if not keys:
        return 0, 0, id_changes

    by_logical = _load_existing_documents_by_logical(cur, keys, nids)

    case_c: list[tuple[Any, ...]] = []
    upsert_pk: list[tuple[Any, ...]] = []

    for row in rows:
        try:
            tid = int(row[_ROW_DOCUMENT_TYPE_ID])
            num = int(row[_ROW_NUMBER])
            nid = int(row[0])
        except (TypeError, ValueError):
            continue
        key = (tid, num)
        existing_id = by_logical.get(key)
        if existing_id is None:
            upsert_pk.append(row)
        elif existing_id == nid:
            upsert_pk.append(row)
        else:
            logger.info(
                "document_id cambiado detectado: old_id=%s new_id=%s document_type_id=%s number=%s",
                existing_id,
                nid,
                tid,
                num,
            )
            id_changes.append((existing_id, nid))
            case_c.append(row)

    upd_c = 0
    if case_c:
        new_ids_c = [int(r[0]) for r in case_c]
        keys_c = [(int(r[_ROW_DOCUMENT_TYPE_ID]), int(r[_ROW_NUMBER])) for r in case_c]
        _delete_stale_document_rows_holding_new_ids(cur, new_ids_c, keys_c)
        upd_c = _batch_update_documents_by_logical_key(cur, case_c)

    ins = upd = 0
    if upsert_pk:
        ins, upd = _upsert_documents_pk_batch(cur, upsert_pk)

    return ins, upd + upd_c, id_changes


def _upsert_documents_pk_batch(cur, rows: list[tuple[Any, ...]]) -> tuple[int, int]:
    sql = """
        INSERT INTO distribuidora.documents (
            document_id, emission_date, document_type_id, client_id, vendedor_id,
            total_amount, state, url_pdf, token, office_id, company_id, number,
            document_type_name, reference, expiration_date, raw_data
        ) VALUES %s
        ON CONFLICT (document_id) DO UPDATE SET
            emission_date = EXCLUDED.emission_date,
            document_type_id = EXCLUDED.document_type_id,
            client_id = EXCLUDED.client_id,
            vendedor_id = EXCLUDED.vendedor_id,
            total_amount = EXCLUDED.total_amount,
            state = EXCLUDED.state,
            url_pdf = EXCLUDED.url_pdf,
            token = EXCLUDED.token,
            office_id = EXCLUDED.office_id,
            company_id = EXCLUDED.company_id,
            number = EXCLUDED.number,
            document_type_name = EXCLUDED.document_type_name,
            reference = EXCLUDED.reference,
            expiration_date = EXCLUDED.expiration_date,
            raw_data = EXCLUDED.raw_data
        RETURNING (xmax = 0) AS inserted
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    execute_values(cur, sql, rows, template=template, page_size=len(rows))
    out = cur.fetchall()
    ins = sum(1 for (x,) in out if x)
    return ins, len(out) - ins


def _upsert_documents_batch(
    cur, rows: list[tuple[Any, ...]]
) -> tuple[int, int, list[tuple[int, int]]]:
    changes: list[tuple[int, int]] = []
    if not rows:
        return 0, 0, changes

    logical_rows = [
        r for r in rows if r[_ROW_DOCUMENT_TYPE_ID] is not None and r[_ROW_NUMBER] is not None
    ]
    pk_rows = [
        r for r in rows if r[_ROW_DOCUMENT_TYPE_ID] is None or r[_ROW_NUMBER] is None
    ]

    ins_total = upd_total = 0
    if logical_rows:
        ins, upd, ch = _upsert_documents_logical_batch(cur, logical_rows)
        changes.extend(ch)
        ins_total += ins
        upd_total += upd
    if pk_rows:
        ins, upd = _upsert_documents_pk_batch(cur, pk_rows)
        ins_total += ins
        upd_total += upd
    return ins_total, upd_total, changes


def _document_ids_needing_raw_backfill(cur, bsale_ids: list[int]) -> set[int]:
    """document_id en DB (distribuidora) con raw_data aún NULL."""
    if not bsale_ids:
        return set()
    cur.execute(
        """
        SELECT document_id
        FROM distribuidora.documents
        WHERE company_id = %s
          AND office_id = %s
          AND document_id = ANY(%s::bigint[])
          AND raw_data IS NULL
        """,
        (COMPANY_ID, OFFICE_ID, bsale_ids),
    )
    return {int(r[0]) for r in cur.fetchall()}


def resync_bsale_documents_full(*, strict_token: bool = False) -> dict[str, Any]:
    """
    Re-sincronización histórica de documentos (Bsale → ``distribuidora.documents``).

    Ventana: ``MIN(emission_date)`` en DB hasta ahora; reutiliza paginación, retry y
    ``_upsert_documents_batch`` (sin duplicar por clave lógica / PK). No toca
    ``sync_state`` ni ``document_details``.

    ``DISTRIBUIDORA_RESYNC_ONLY_RAW_NULL=1`` (defecto): solo considera filas con
    ``raw_data IS NULL`` para la fecha mínima y para decidir qué ítems de cada
    página se vuelven a persistir. ``0`` desactiva el filtro (re-procesa todo el rango).
    """
    t0 = time.perf_counter()
    only_missing_raw = os.getenv("DISTRIBUIDORA_RESYNC_ONLY_RAW_NULL", "1").strip().lower() not in (
        "0",
        "false",
        "no",
    )

    token = _bsale_token_distribuidora()
    if not token:
        if strict_token:
            raise ValueError(
                "Ningún token Bsale en el entorno: defina BSALE_TOKEN o BSALE_TOKEN_SPA (p. ej. en Coolify)."
            )
        return {
            "mode": "resync_documents_full",
            "documents_api_items": 0,
            "documents_processed": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
            "documents_id_changed": 0,
            "only_missing_raw_data": only_missing_raw,
            "emitidos_desde": None,
            "emitidos_hasta": None,
            "raw_data_null_remaining": None,
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "omitido_concurrencia": False,
            "skipped": True,
            "skip_reason": "BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas",
            "errors": None,
        }

    stats: dict[str, Any] = {
        "mode": "resync_documents_full",
        "documents_api_items": 0,
        "documents_processed": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "documents_id_changed": 0,
        "only_missing_raw_data": only_missing_raw,
        "emitidos_desde": None,
        "emitidos_hasta": None,
        "raw_data_null_remaining": None,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "skipped": False,
        "skip_reason": None,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (_ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            logger.info("resync_bsale_documents_full omitido (lock en uso)")
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        _ensure_distribuidora_tables(cur)
        conn.commit()

        if only_missing_raw:
            cur.execute(
                """
                SELECT MIN(emission_date)
                FROM distribuidora.documents
                WHERE company_id = %s
                  AND office_id = %s
                  AND raw_data IS NULL
                """,
                (COMPANY_ID, OFFICE_ID),
            )
        else:
            cur.execute(
                """
                SELECT MIN(emission_date)
                FROM distribuidora.documents
                WHERE company_id = %s AND office_id = %s
                """,
                (COMPANY_ID, OFFICE_ID),
            )
        row = cur.fetchone()
        min_em = row[0] if row else None
        if min_em is None:
            logger.info(
                "resync_bsale_documents_full: sin MIN(emission_date) aplicable "
                "(tabla vacía o nada que coincida con el filtro)"
            )
            stats["skip_reason"] = "no_min_emission_date"
            cur.execute(
                """
                SELECT COUNT(*)
                FROM distribuidora.documents
                WHERE company_id = %s
                  AND office_id = %s
                  AND raw_data IS NULL
                """,
                (COMPANY_ID, OFFICE_ID),
            )
            stats["raw_data_null_remaining"] = int(cur.fetchone()[0])
            conn.commit()
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            logger.info(
                "resync_bsale_documents_full: duration=%ss processed=%s upd=%s ins=%s "
                "raw_null_remaining=%s",
                stats["duration_seconds"],
                stats["documents_processed"],
                stats["documents_updated"],
                stats["documents_inserted"],
                stats["raw_data_null_remaining"],
            )
            return stats

        if min_em.tzinfo is None:
            min_em = min_em.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        desde_ts = int(min_em.timestamp())
        hasta_ts = int(now.timestamp())
        if desde_ts >= hasta_ts:
            desde_ts = hasta_ts - 3600

        stats["emitidos_desde"] = min_em.isoformat()
        stats["emitidos_hasta"] = now.isoformat()

        session = requests.Session()
        offset = 0
        pending_docs: list[tuple[Any, ...]] = []
        id_change_pairs: list[tuple[int, int]] = []

        while True:
            params = {
                "limit": LIMIT_BSALE,
                "offset": offset,
                "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
            }
            data = _bsale_get(session, f"{BASE_BSALE}/documents.json", token, params)
            items = data.get("items") or []
            if not items:
                break

            page_candidates: list[dict[str, Any]] = []
            for d in items:
                oid = (d.get("office") or {}).get("id")
                if oid is None:
                    continue
                if int(oid) != OFFICE_ID:
                    continue
                stats["documents_api_items"] += 1
                page_candidates.append(d)

            if only_missing_raw and page_candidates:
                ids = [int(x["id"]) for x in page_candidates]
                need_ids = _document_ids_needing_raw_backfill(cur, ids)
                for d in page_candidates:
                    if int(d["id"]) not in need_ids:
                        continue
                    pending_docs.append(_doc_row_from_bsale(d))
                    stats["documents_processed"] += 1
            else:
                for d in page_candidates:
                    pending_docs.append(_doc_row_from_bsale(d))
                    stats["documents_processed"] += 1

            if len(pending_docs) >= 200:
                ins, upd, ch = _upsert_documents_batch(cur, pending_docs)
                stats["documents_inserted"] += ins
                stats["documents_updated"] += upd
                id_change_pairs.extend(ch)
                conn.commit()
                pending_docs.clear()

            offset += LIMIT_BSALE

        if pending_docs:
            ins, upd, ch = _upsert_documents_batch(cur, pending_docs)
            stats["documents_inserted"] += ins
            stats["documents_updated"] += upd
            id_change_pairs.extend(ch)
            conn.commit()

        new_to_old: dict[int, int] = {}
        for old_id, new_id in id_change_pairs:
            new_to_old[new_id] = old_id
        stats["documents_id_changed"] = len(new_to_old)

        cur.execute(
            """
            SELECT COUNT(*)
            FROM distribuidora.documents
            WHERE company_id = %s
              AND office_id = %s
              AND raw_data IS NULL
            """,
            (COMPANY_ID, OFFICE_ID),
        )
        stats["raw_data_null_remaining"] = int(cur.fetchone()[0])
        conn.commit()
        cur.close()

        stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
        logger.info(
            "resync_bsale_documents_full OK: api_items=%s processed=%s ins=%s upd=%s "
            "id_changed=%s raw_null_remaining=%s only_missing_raw=%s duration=%ss",
            stats["documents_api_items"],
            stats["documents_processed"],
            stats["documents_inserted"],
            stats["documents_updated"],
            stats["documents_id_changed"],
            stats["raw_data_null_remaining"],
            only_missing_raw,
            stats["duration_seconds"],
        )
    except Exception as e:
        logger.exception("resync_bsale_documents_full: %s", e)
        stats["errors"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_KEY,))
                c2.close()
        except Exception:
            logger.exception("resync_bsale_documents_full: unlock")
        try:
            conn.close()
        except Exception:
            pass

    return stats


def _insert_details_batch(cur, rows: list[tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO distribuidora.document_details (
            detail_id, document_id, line_number, variant_id,
            quantity, net_unit_value, total_unit_value, net_amount, tax_amount,
            total_amount, net_discount, discount_percentage
        ) VALUES %s
        ON CONFLICT (detail_id) DO NOTHING
        RETURNING detail_id
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    execute_values(cur, sql, rows, template=template, page_size=len(rows))
    return len(cur.fetchall())


def sync_bsale_distribuidora(*, strict_token: bool = False) -> dict[str, Any]:
    """
    Sincroniza documentos y luego detalles faltantes (incremental por sync_state + ventana Bsale).

    :param strict_token: si True y no hay token en env, lanza ValueError (p. ej. POST manual).
        Si False (job en background), devuelve ``skipped=True`` sin excepción.
    """
    global _token_missing_logged
    global _document_raw_sample_logged

    t0 = time.perf_counter()
    token = _bsale_token_distribuidora()
    if not token:
        if strict_token:
            raise ValueError(
                "Ningún token Bsale en el entorno: defina BSALE_TOKEN o BSALE_TOKEN_SPA (p. ej. en Coolify)."
            )
        if not _token_missing_logged:
            logger.warning(
                "Sin BSALE_TOKEN ni BSALE_TOKEN_SPA: sync distribuidora omitido. "
                "Configura una de las dos en Coolify (o DISTRIBUIDORA_BSALE_SYNC_DISABLED=1)."
            )
            _token_missing_logged = True
        return {
            "documents_processed": 0,
            "documents_inserted": 0,
            "documents_updated": 0,
            "documents_id_changed": 0,
            "details_inserted": 0,
            "details_deleted": 0,
            "duration_seconds": round(time.perf_counter() - t0, 3),
            "omitido_concurrencia": False,
            "skipped": True,
            "skip_reason": "BSALE_TOKEN / BSALE_TOKEN_SPA no configuradas",
            "errors": None,
        }

    _token_missing_logged = False

    lookback_days = int(os.getenv("DISTRIBUIDORA_DETAILS_LOOKBACK_DAYS", "365"))
    details_chunk = int(os.getenv("DISTRIBUIDORA_DETAILS_DOC_CHUNK", "200"))
    details_batch = int(os.getenv("DISTRIBUIDORA_DETAILS_INSERT_BATCH", "500"))

    stats: dict[str, Any] = {
        "documents_processed": 0,
        "documents_inserted": 0,
        "documents_updated": 0,
        "documents_id_changed": 0,
        "details_inserted": 0,
        "details_deleted": 0,
        "duration_seconds": 0.0,
        "omitido_concurrencia": False,
        "skipped": False,
        "errors": None,
    }

    conn = get_connection()
    got_lock = False
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_try_advisory_lock(%s)", (_ADVISORY_LOCK_KEY,))
        got_lock = bool(cur.fetchone()[0])
        if not got_lock:
            stats["omitido_concurrencia"] = True
            logger.info("sync_bsale_distribuidora omitido (lock en uso)")
            cur.close()
            stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
            return stats

        _ensure_distribuidora_tables(cur)
        conn.commit()

        _document_raw_sample_logged = False

        cur.execute(
            """
            SELECT last_sync
            FROM distribuidora.sync_state
            ORDER BY id DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            raise RuntimeError("sync_state sin last_sync")
        last_sync: datetime = row[0]
        if last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        desde = last_sync - timedelta(hours=2)
        if last_sync < _FIRST_SYNC_CUTOFF:
            desde = now - timedelta(days=30)
            logger.info("sync_bsale_distribuidora: primer sync — ventana documentos últimos 30 días")

        desde_ts = int(desde.timestamp())
        hasta_ts = int(now.timestamp())
        if desde_ts >= hasta_ts:
            desde_ts = hasta_ts - 3600

        session = requests.Session()
        offset = 0
        pending_docs: list[tuple[Any, ...]] = []
        id_change_pairs: list[tuple[int, int]] = []

        while True:
            params = {
                "limit": LIMIT_BSALE,
                "offset": offset,
                "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
            }
            data = _bsale_get(session, f"{BASE_BSALE}/documents.json", token, params)
            items = data.get("items") or []
            if not items:
                break

            for d in items:
                oid = (d.get("office") or {}).get("id")
                if oid is not None and int(oid) != OFFICE_ID:
                    continue
                if oid is None:
                    continue
                if not _document_raw_sample_logged:
                    _log_sample_document_raw_data(d)
                    _document_raw_sample_logged = True
                pending_docs.append(_doc_row_from_bsale(d))
                stats["documents_processed"] += 1

            if len(pending_docs) >= 200:
                ins, upd, ch = _upsert_documents_batch(cur, pending_docs)
                stats["documents_inserted"] += ins
                stats["documents_updated"] += upd
                id_change_pairs.extend(ch)
                conn.commit()
                pending_docs.clear()

            offset += LIMIT_BSALE

        if pending_docs:
            ins, upd, ch = _upsert_documents_batch(cur, pending_docs)
            stats["documents_inserted"] += ins
            stats["documents_updated"] += upd
            id_change_pairs.extend(ch)
            conn.commit()

        new_to_old: dict[int, int] = {}
        for old_id, new_id in id_change_pairs:
            new_to_old[new_id] = old_id
        stats["documents_id_changed"] = len(new_to_old)
        for new_id, old_id in sorted(new_to_old.items(), key=lambda x: x[0]):
            logger.info(
                "document_id Bsale reasignado (misma clave tipo+número): old_id=%s new_id=%s",
                old_id,
                new_id,
            )

        force_resync_new_ids = set(new_to_old.keys())
        since_emission = max(desde, now - timedelta(days=lookback_days))
        pending_details: list[tuple[Any, ...]] = []
        last_doc_id = 0
        empty_force_done: set[int] = set()

        while True:
            fr = list(force_resync_new_ids)
            ef = list(empty_force_done)
            cur.execute(
                """
                SELECT d.document_id
                FROM distribuidora.documents d
                WHERE d.company_id = %s
                  AND d.office_id = %s
                  AND d.emission_date >= %s
                  AND d.document_id > %s
                  AND (
                      NOT EXISTS (
                          SELECT 1
                          FROM distribuidora.document_details dd
                          WHERE dd.document_id = d.document_id
                      )
                      OR (
                          cardinality(%s::bigint[]) > 0
                          AND d.document_id = ANY(%s::bigint[])
                      )
                  )
                  AND (
                      CASE WHEN cardinality(%s::bigint[]) = 0 THEN TRUE
                      ELSE NOT (d.document_id = ANY(%s::bigint[])) END
                  )
                ORDER BY d.document_id
                LIMIT %s
                """,
                (
                    COMPANY_ID,
                    OFFICE_ID,
                    since_emission,
                    last_doc_id,
                    fr,
                    fr,
                    ef,
                    ef,
                    details_chunk,
                ),
            )
            doc_ids = [int(r[0]) for r in cur.fetchall()]
            if not doc_ids:
                break

            for doc_id in doc_ids:
                is_force = doc_id in force_resync_new_ids
                data = _bsale_get(
                    session, f"{BASE_BSALE}/documents/{doc_id}/details.json", token
                )
                lines = data.get("items") or []
                old_id = new_to_old.get(doc_id) if is_force else None

                if is_force:
                    if not lines:
                        empty_force_done.add(doc_id)
                        if old_id is not None:
                            cur.execute(
                                """
                                DELETE FROM distribuidora.document_details
                                WHERE document_id = %s
                                """,
                                (old_id,),
                            )
                            n = cur.rowcount
                            stats["details_deleted"] += n
                            logger.info(
                                "detalles antiguos eliminados (reemplazo ID, 0 líneas Bsale): "
                                "old_id=%s new_id=%s filas=%s",
                                old_id,
                                doc_id,
                                n,
                            )
                        conn.commit()
                        continue
                    detail_rows: list[tuple[Any, ...]] = []
                    for it in lines:
                        try:
                            detail_rows.append(_detail_row(doc_id, it))
                        except Exception as e:
                            logger.warning("Línea inválida doc %s: %s", doc_id, e)
                    if not detail_rows:
                        logger.warning(
                            "doc %s: cambio de document_id pero 0 líneas insertables; "
                            "no se eliminan detalles del document_id antiguo",
                            doc_id,
                        )
                        conn.commit()
                        continue
                    ins = _insert_details_batch(cur, detail_rows)
                    stats["details_inserted"] += ins
                    logger.info(
                        "detalles reinsertados tras cambio de document_id: new_id=%s filas=%s",
                        doc_id,
                        ins,
                    )
                    if old_id is not None:
                        cur.execute(
                            """
                            DELETE FROM distribuidora.document_details
                            WHERE document_id = %s
                            """,
                            (old_id,),
                        )
                        n = cur.rowcount
                        stats["details_deleted"] += n
                        logger.info(
                            "detalles antiguos eliminados tras insertar nuevos: old_id=%s new_id=%s filas=%s",
                            old_id,
                            doc_id,
                            n,
                        )
                    conn.commit()
                    continue

                if not lines:
                    pending_details.append(
                        (
                            -doc_id,
                            doc_id,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    )
                else:
                    for it in lines:
                        try:
                            pending_details.append(_detail_row(doc_id, it))
                        except Exception as e:
                            logger.warning("Línea inválida doc %s: %s", doc_id, e)

                if len(pending_details) >= details_batch:
                    stats["details_inserted"] += _insert_details_batch(cur, pending_details)
                    conn.commit()
                    pending_details.clear()

            last_doc_id = doc_ids[-1]
            if len(doc_ids) < details_chunk:
                break

        if pending_details:
            stats["details_inserted"] += _insert_details_batch(cur, pending_details)
            conn.commit()

        cur.execute(
            """
            SELECT COUNT(*)
            FROM distribuidora.documents d
            WHERE d.company_id = %s
              AND d.office_id = %s
              AND d.emission_date >= %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM distribuidora.document_details dd
                  WHERE dd.document_id = d.document_id
              )
            """,
            (COMPANY_ID, OFFICE_ID, since_emission),
        )
        no_details_count = int(cur.fetchone()[0])
        if no_details_count > 0:
            cur.execute(
                """
                SELECT d.document_id
                FROM distribuidora.documents d
                WHERE d.company_id = %s
                  AND d.office_id = %s
                  AND d.emission_date >= %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM distribuidora.document_details dd
                      WHERE dd.document_id = d.document_id
                  )
                ORDER BY d.document_id
                LIMIT 20
                """,
                (COMPANY_ID, OFFICE_ID, since_emission),
            )
            sample = [int(r[0]) for r in cur.fetchall()]
            logger.warning(
                "documentos sin detalles tras sync (ventana lookback): count=%s sample_ids=%s",
                no_details_count,
                sample,
            )

        cur.execute(
            """
            UPDATE distribuidora.sync_state
            SET last_sync = %s
            WHERE id = (SELECT id FROM distribuidora.sync_state ORDER BY id DESC LIMIT 1)
            """,
            (now,),
        )
        conn.commit()
        cur.close()

        logger.info(
            "sync_bsale_distribuidora OK: processed=%s ins=%s upd=%s id_changed=%s "
            "details_ins=%s details_del=%s s=%.2f",
            stats["documents_processed"],
            stats["documents_inserted"],
            stats["documents_updated"],
            stats["documents_id_changed"],
            stats["details_inserted"],
            stats["details_deleted"],
            time.perf_counter() - t0,
        )
    except Exception as e:
        logger.exception("sync_bsale_distribuidora: %s", e)
        stats["errors"] = str(e)
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            if got_lock:
                c2 = conn.cursor()
                c2.execute("SELECT pg_advisory_unlock(%s)", (_ADVISORY_LOCK_KEY,))
                c2.close()
        except Exception:
            logger.exception("sync_bsale_distribuidora: unlock")
        try:
            conn.close()
        except Exception:
            pass

    stats["duration_seconds"] = round(time.perf_counter() - t0, 3)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    print(sync_bsale_distribuidora(strict_token=True))
