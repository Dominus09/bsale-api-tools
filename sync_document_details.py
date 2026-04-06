"""
Sincroniza detalles de documentos Bsale → PostgreSQL (bsale.document_details).

Patrón equivalente a sync_documents.py:
  - Multiempresa desde bsale.companies + token por env.
  - Rango por SYNC_FROM_DATE/SYNC_TO_DATE (filtro en PostgreSQL sobre bsale.documents).
  - Lectura de documentos por bloques (sin cargar todo en memoria).
  - Llamadas secuenciales a Bsale por documento.
  - Insert batch con execute_values y ON CONFLICT DO NOTHING.

Ejecución del Job:
  python sync_document_details.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, time as dt_time, timezone
from typing import Any

import psycopg2
import requests
from psycopg2 import sql
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("sync_document_details")

BASE_BSALE = "https://api.bsale.io/v1"
MAX_BSALE_TRANSIENT = 40
WARN_RANGE_DAYS = 31
DOCS_CHUNK = 500
DETAILS_BATCH = 1000

# Rango de emisión (inclusive) aplicado sobre bsale.documents
SYNC_FROM_DATE = "2026-04-01"
SYNC_TO_DATE = "2026-04-05"

# ---------------------------------
# POSTGRES (igual que sync_documents.py / sync_prices_costs.py)
# ---------------------------------

conn = psycopg2.connect(
    host=os.getenv("PG_HOST"),
    database=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
)

cur = conn.cursor()


def die(msg: str, code: int = 1) -> None:
    log.error("%s", msg)
    sys.exit(code)


def get_companies():
    cur.execute(
        """
        SELECT company_id, name, bsale_token
        FROM bsale.companies
        WHERE active = true
        """
    )
    rows = cur.fetchall()
    companies = []
    for r in rows:
        token = os.getenv(r[2])
        if not token:
            log.warning("TOKEN NOT FOUND: %s (empresa %s)", r[2], r[1])
            continue
        companies.append({"company_id": r[0], "name": r[1], "token": token})
    return companies


def bsale_get_details(session: requests.Session, doc_id: int, token: str) -> list[dict[str, Any]]:
    transient = 0
    url = f"{BASE_BSALE}/documents/{doc_id}/details.json"
    while True:
        try:
            r = session.get(
                url,
                headers={"access_token": token},
                timeout=30,
            )
        except requests.RequestException as e:
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(f"Bsale: demasiados errores de red ({transient}) en doc {doc_id}: {e}", 1)
            log.warning(
                "Doc %s — error de red (%s/%s): %s — reintento en 3 s",
                doc_id,
                transient,
                MAX_BSALE_TRANSIENT,
                e,
            )
            time.sleep(3)
            continue

        if r.status_code == 401:
            die(f"Bsale 401 Unauthorized — token inválido/expirado (doc {doc_id})", 1)
        if r.status_code == 403:
            die(f"Bsale 403 Forbidden — /documents/{{id}}/details.json (doc {doc_id})", 1)
        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            log.warning("Doc %s — Bsale 429 rate limit, esperando %s s", doc_id, wait)
            time.sleep(wait)
            continue
        if 400 <= r.status_code < 500:
            die(f"Bsale error cliente HTTP {r.status_code} en doc {doc_id}: {(r.text or '')[:800]}", 1)
        if r.status_code in (500, 502, 503, 504):
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(f"Bsale: demasiados 5xx en doc {doc_id} (último {r.status_code})", 1)
            log.warning(
                "Doc %s — Bsale HTTP %s, reintento en 3 s (%s/%s)",
                doc_id,
                r.status_code,
                transient,
                MAX_BSALE_TRANSIENT,
            )
            time.sleep(3)
            continue
        if not (200 <= r.status_code < 300):
            die(f"Bsale respuesta inesperada HTTP {r.status_code} en doc {doc_id}", 1)

        try:
            data = r.json()
        except ValueError as e:
            die(f"Bsale JSON inválido en doc {doc_id}: {e}", 1)
        return data.get("items") or []


def parse_date_const(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError:
        die(f"{label} inválida (YYYY-MM-DD): {s!r}", 1)


def _range_datetimes(start_d: date, end_d: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_d, dt_time.min, tzinfo=timezone.utc)
    end_exclusive_dt = datetime.combine(end_d + timedelta(days=1), dt_time.min, tzinfo=timezone.utc)
    return start_dt, end_exclusive_dt


def fetch_document_ids_chunk(
    company_id: int,
    range_start: datetime,
    range_end_exclusive: datetime,
    last_bsale_id: int,
    chunk_size: int,
    schema: str,
    documents_table: str,
) -> list[int]:
    query = sql.SQL(
        """
        SELECT bsale_id
        FROM {schema}.{documents_table}
        WHERE company_id = %s
          AND emission_date >= %s
          AND emission_date < %s
          AND bsale_id > %s
        ORDER BY bsale_id ASC
        LIMIT %s
        """
    ).format(
        schema=sql.Identifier(schema),
        documents_table=sql.Identifier(documents_table),
    )
    cur.execute(query, (company_id, range_start, range_end_exclusive, last_bsale_id, chunk_size))
    return [int(r[0]) for r in cur.fetchall()]


def detail_row_from_bsale(company_id: int, document_id: int, item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        company_id,
        item["id"],
        document_id,
        item.get("lineNumber"),
        (item.get("variant") or {}).get("id"),
        item.get("quantity"),
        item.get("netUnitValue"),
        item.get("totalUnitValue"),
        item.get("netAmount"),
        item.get("taxAmount"),
        item.get("totalAmount"),
        item.get("netDiscount"),
        item.get("discountPercentage"),
    )


def insert_details_batch(
    schema: str,
    details_table: str,
    rows: list[tuple[Any, ...]],
) -> tuple[int, int]:
    """
    Inserta lote de detalles.
    Retorna (insertados, ignorados_por_conflicto).
    """
    if not rows:
        return 0, 0

    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.{details_table} (
            company_id,
            bsale_detail_id,
            document_id,
            line_number,
            variant_id,
            quantity,
            net_unit_value,
            total_unit_value,
            net_amount,
            tax_amount,
            total_amount,
            net_discount,
            discount_percentage
        )
        VALUES %s
        ON CONFLICT (company_id, bsale_detail_id) DO NOTHING
        RETURNING 1
        """
    ).format(
        schema=sql.Identifier(schema),
        details_table=sql.Identifier(details_table),
    )

    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        execute_values(
            cur,
            insert_sql.as_string(conn),
            rows,
            template=template,
            page_size=len(rows),
        )
        inserted = len(cur.fetchall())
        conn.commit()
    except Exception as e:
        conn.rollback()
        die(f"PostgreSQL error al insertar lote detalles ({len(rows)} filas): {type(e).__name__}: {e}", 1)

    ignored = len(rows) - inserted
    return inserted, ignored


def sync_company_details(
    company_id: int,
    token: str,
    company_name: str,
    range_start: datetime,
    range_end_exclusive: datetime,
    schema: str,
    documents_table: str,
    details_table: str,
    session: requests.Session,
) -> tuple[int, int, int, int]:
    """
    Retorna (docs_procesados, detalles_insertados, detalles_ignorados, errores_doc).
    """
    log.info(
        "Empresa %s (id=%s) — sync detalles desde %s hasta %s",
        company_name,
        company_id,
        range_start,
        range_end_exclusive,
    )

    block = 0
    last_bsale_id = 0
    docs_processed = 0
    details_inserted = 0
    details_ignored = 0
    doc_errors = 0
    pending_rows: list[tuple[Any, ...]] = []

    while True:
        doc_ids = fetch_document_ids_chunk(
            company_id,
            range_start,
            range_end_exclusive,
            last_bsale_id,
            DOCS_CHUNK,
            schema,
            documents_table,
        )
        if not doc_ids:
            break

        block += 1
        log.info(
            "Empresa %s — bloque %s docs=%s (bsale_id %s..%s)",
            company_name,
            block,
            len(doc_ids),
            doc_ids[0],
            doc_ids[-1],
        )

        for doc_id in doc_ids:
            try:
                items = bsale_get_details(session, doc_id, token)
            except Exception as e:
                doc_errors += 1
                log.error("Empresa %s — error en documento %s: %s", company_name, doc_id, e)
                continue

            docs_processed += 1
            for item in items:
                pending_rows.append(detail_row_from_bsale(company_id, doc_id, item))

            if len(pending_rows) >= DETAILS_BATCH:
                ins, ign = insert_details_batch(schema, details_table, pending_rows)
                details_inserted += ins
                details_ignored += ign
                log.info(
                    "Empresa %s — bloque %s commit lote detalles: insertados=%s ignorados=%s docs_procesados=%s",
                    company_name,
                    block,
                    ins,
                    ign,
                    docs_processed,
                )
                pending_rows.clear()

        last_bsale_id = doc_ids[-1]

        if pending_rows:
            ins, ign = insert_details_batch(schema, details_table, pending_rows)
            details_inserted += ins
            details_ignored += ign
            log.info(
                "Empresa %s — bloque %s commit final bloque: insertados=%s ignorados=%s",
                company_name,
                block,
                ins,
                ign,
            )
            pending_rows.clear()

        log.info(
            "Empresa %s — bloque %s COMPLETO docs_procesados=%s insertados=%s ignorados=%s errores=%s",
            company_name,
            block,
            docs_processed,
            details_inserted,
            details_ignored,
            doc_errors,
        )

    return docs_processed, details_inserted, details_ignored, doc_errors


def main() -> None:
    log.info("SYNC DOCUMENT DETAILS START")

    start_d = parse_date_const(SYNC_FROM_DATE, "SYNC_FROM_DATE")
    end_d = parse_date_const(SYNC_TO_DATE, "SYNC_TO_DATE")
    if end_d < start_d:
        die("SYNC_TO_DATE debe ser >= SYNC_FROM_DATE", 1)

    num_days = (end_d - start_d).days + 1
    if num_days > WARN_RANGE_DAYS:
        log.warning(
            "Rango grande: %s días (> %s). Considere acortar SYNC_* en el script o trocear el Job.",
            num_days,
            WARN_RANGE_DAYS,
        )

    range_start, range_end_exclusive = _range_datetimes(start_d, end_d)
    schema = (os.getenv("PG_DOCUMENTS_SCHEMA") or "bsale").strip() or "bsale"
    documents_table = "documents"
    details_table = "document_details"

    companies = get_companies()
    if not companies:
        die("No hay empresas activas con token configurado (bsale.companies + env).", 1)

    session = requests.Session()
    run_docs = 0
    run_inserted = 0
    run_ignored = 0
    run_errors = 0

    for company in companies:
        log.info("")
        docs, ins, ign, errs = sync_company_details(
            company["company_id"],
            company["token"],
            company["name"],
            range_start,
            range_end_exclusive,
            schema,
            documents_table,
            details_table,
            session,
        )
        run_docs += docs
        run_inserted += ins
        run_ignored += ign
        run_errors += errs
        log.info(
            "Empresa %s — subtotal docs=%s insertados=%s ignorados=%s errores=%s",
            company["name"],
            docs,
            ins,
            ign,
            errs,
        )

    conn.close()
    log.info(
        "SYNC DOCUMENT DETAILS COMPLETE — empresas=%s días=%s docs=%s insertados=%s ignorados=%s errores=%s",
        len(companies),
        num_days,
        run_docs,
        run_inserted,
        run_ignored,
        run_errors,
    )


if __name__ == "__main__":
    main()
