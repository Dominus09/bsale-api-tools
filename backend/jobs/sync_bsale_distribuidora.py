"""
Sincronización incremental Bsale → distribuidora.documents / distribuidora.document_details.

- company_id = 3 y office_id = 1 (filtrado en API y en consultas).
- Clave lógica de documento: (document_type_id, number); ``document_id`` de Bsale puede cambiar.
- Token: ``BSALE_TOKEN`` o, si no existe, ``BSALE_TOKEN_SPA`` (Coolify).
- Rango de emisión: último sync_state.last_sync − 2 h hasta ahora (timestamps UNIX en Bsale).
- Detalles: documentos sin filas en document_details, más re-sync forzado si cambió ``document_id``.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import requests
from psycopg2.extras import execute_values

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
            number BIGINT
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


def _doc_row_from_bsale(d: dict[str, Any]) -> tuple[Any, ...]:
    emission_raw = d.get("emissionDate")
    emission_date = None
    if emission_raw is not None:
        emission_date = datetime.fromtimestamp(int(emission_raw), tz=timezone.utc)
    return (
        int(d["id"]),
        emission_date,
        (d.get("document_type") or {}).get("id"),
        (d.get("client") or {}).get("id"),
        (d.get("user") or {}).get("id"),
        _num(d.get("totalAmount")),
        d.get("state"),
        d.get("urlPdf"),
        d.get("token"),
        OFFICE_ID,
        COMPANY_ID,
        d.get("number"),
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


def _find_document_id_changes(
    cur, rows: list[tuple[Any, ...]]
) -> list[tuple[int, int]]:
    """
    Antes de un upsert por clave lógica, devuelve pares (document_id_antiguo, document_id_nuevo)
    cuando ya existe un documento con el mismo (company_id, office_id, document_type_id, number).
    """
    new_ids: list[int] = []
    type_ids: list[int] = []
    numbers: list[int] = []
    for row in rows:
        new_id = int(row[0])
        dt = row[2]
        num = row[-1]
        if dt is None or num is None:
            continue
        try:
            t_int = int(dt)
            n_int = int(num)
        except (TypeError, ValueError):
            continue
        new_ids.append(new_id)
        type_ids.append(t_int)
        numbers.append(n_int)
    if not new_ids:
        return []
    cur.execute(
        """
        SELECT d.document_id, v.new_id
        FROM distribuidora.documents d
        INNER JOIN (
            SELECT u.new_id, u.document_type_id, u.number
            FROM unnest(%s::bigint[], %s::int[], %s::bigint[]) AS u(new_id, document_type_id, number)
        ) v ON d.company_id = %s
            AND d.office_id = %s
            AND d.document_type_id = v.document_type_id
            AND d.number = v.number
            AND d.document_id IS DISTINCT FROM v.new_id
        """,
        (new_ids, type_ids, numbers, COMPANY_ID, OFFICE_ID),
    )
    return [(int(o), int(n)) for o, n in cur.fetchall()]


def _upsert_documents_logical_batch(cur, rows: list[tuple[Any, ...]]) -> tuple[int, int]:
    sql = """
        INSERT INTO distribuidora.documents (
            document_id, emission_date, document_type_id, client_id, vendedor_id,
            total_amount, state, url_pdf, token, office_id, company_id, number
        ) VALUES %s
        ON CONFLICT (company_id, office_id, document_type_id, number)
        WHERE document_type_id IS NOT NULL AND number IS NOT NULL
        DO UPDATE SET
            document_id = EXCLUDED.document_id,
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
            number = EXCLUDED.number
        RETURNING (xmax = 0) AS inserted
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    execute_values(cur, sql, rows, template=template, page_size=len(rows))
    out = cur.fetchall()
    ins = sum(1 for (x,) in out if x)
    return ins, len(out) - ins


def _upsert_documents_pk_batch(cur, rows: list[tuple[Any, ...]]) -> tuple[int, int]:
    sql = """
        INSERT INTO distribuidora.documents (
            document_id, emission_date, document_type_id, client_id, vendedor_id,
            total_amount, state, url_pdf, token, office_id, company_id, number
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
            number = EXCLUDED.number
        RETURNING (xmax = 0) AS inserted
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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

    logical_rows = [r for r in rows if r[2] is not None and r[-1] is not None]
    pk_rows = [r for r in rows if r[2] is None or r[-1] is None]

    ins_total = upd_total = 0
    if logical_rows:
        changes.extend(_find_document_id_changes(cur, logical_rows))
        ins, upd = _upsert_documents_logical_batch(cur, logical_rows)
        ins_total += ins
        upd_total += upd
    if pk_rows:
        ins, upd = _upsert_documents_pk_batch(cur, pk_rows)
        ins_total += ins
        upd_total += upd
    return ins_total, upd_total, changes


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
