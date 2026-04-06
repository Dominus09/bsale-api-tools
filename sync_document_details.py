"""
Sincroniza líneas de documento Bsale → PostgreSQL (bsale.document_details).

Mismo patrón operativo que sync_documents.py:
  - Postgres al inicio (PG_HOST, PG_DB, PG_USER, PG_PASSWORD).
  - Multiempresa desde bsale.companies + token por env.
  - Rango SYNC_FROM_DATE / SYNC_TO_DATE: env en Coolify, o constantes como fallback.
  - Sin NocoDB; sin filtrar Bsale por fecha (solo por documento).

Solo procesa documentos que aún no tienen ninguna fila en document_details
(LEFT JOIN + IS NULL), en bloques LIMIT para no cargar millones en memoria.

Ejecución:
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
# Documentos por bloque leídos de PostgreSQL (entre 200 y 500)
DOCS_CHUNK = 400
# Filas de detalle acumuladas antes de execute_values + commit
DETAILS_BATCH = 800

SYNC_FROM_DATE = "2026-04-01"
SYNC_TO_DATE = "2026-04-05"

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


def parse_date_const(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError:
        die(f"{label} inválida (YYYY-MM-DD): {s!r}", 1)


def range_datetimes_utc(start_d: date, end_d: date) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(start_d, dt_time.min, tzinfo=timezone.utc)
    end_exclusive = datetime.combine(end_d + timedelta(days=1), dt_time.min, tzinfo=timezone.utc)
    return start_dt, end_exclusive


def fetch_docs_missing_details_chunk(
    company_id: int,
    range_start: datetime,
    range_end_exclusive: datetime,
    chunk_size: int,
    schema: str,
) -> list[int]:
    """
    Documentos en el rango de emisión sin ninguna fila en document_details.
    """
    q = sql.SQL(
        """
        SELECT d.bsale_id
        FROM {schema}.documents d
        LEFT JOIN {schema}.document_details dd
          ON dd.company_id = d.company_id
         AND dd.document_id = d.bsale_id
        WHERE d.company_id = %s
          AND d.emission_date >= %s
          AND d.emission_date < %s
          AND dd.document_id IS NULL
        ORDER BY d.bsale_id
        LIMIT %s
        """
    ).format(schema=sql.Identifier(schema))
    cur.execute(q, (company_id, range_start, range_end_exclusive, chunk_size))
    return [int(r[0]) for r in cur.fetchall()]


def bsale_get_details(
    session: requests.Session,
    doc_id: int,
    token: str,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """
    Retorna (items, None) si OK (items puede ser []).
    Retorna (None, motivo) si se omite el documento (p.ej. 404) sin tumbar el job.
    Lanza SystemExit vía die() solo en errores fatales (401/403).
    """
    transient = 0
    url = f"{BASE_BSALE}/documents/{doc_id}/details.json"
    while True:
        try:
            r = session.get(url, headers={"access_token": token}, timeout=30)
        except requests.RequestException as e:
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(f"Bsale: demasiados errores de red ({transient}) en doc {doc_id}: {e}", 1)
            log.warning(
                "Doc %s — red (%s/%s): %s — reintento en 3 s",
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
            die(f"Bsale 403 Forbidden — details doc {doc_id}", 1)

        if r.status_code == 404:
            return None, "HTTP 404"

        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            log.warning("Doc %s — 429, esperando %s s", doc_id, wait)
            time.sleep(wait)
            continue

        if 400 <= r.status_code < 500:
            return None, f"HTTP {r.status_code}: {(r.text or '')[:200]}"

        if r.status_code in (500, 502, 503, 504):
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(f"Bsale: demasiados 5xx en doc {doc_id} (último {r.status_code})", 1)
            log.warning(
                "Doc %s — HTTP %s, reintento en 3 s (%s/%s)",
                doc_id,
                r.status_code,
                transient,
                MAX_BSALE_TRANSIENT,
            )
            time.sleep(3)
            continue

        if not (200 <= r.status_code < 300):
            return None, f"HTTP {r.status_code}"

        try:
            data = r.json()
        except ValueError as e:
            return None, f"JSON inválido: {e}"

        return data.get("items") or [], None


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


def insert_details_batch(schema: str, rows: list[tuple[Any, ...]]) -> tuple[int, int]:
    if not rows:
        return 0, 0
    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.document_details (
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
    ).format(schema=sql.Identifier(schema))
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
        die(f"PostgreSQL error insertando detalles ({len(rows)} filas): {type(e).__name__}: {e}", 1)
    ignored = len(rows) - inserted
    return inserted, ignored


def sync_company_details(
    company_id: int,
    token: str,
    company_name: str,
    range_start: datetime,
    range_end_exclusive: datetime,
    schema: str,
    session: requests.Session,
) -> tuple[int, int, int, int]:
    """
    Retorna (docs_ok, detalles_insertados, detalles_ignorados, errores_doc).
    """
    log.info(
        "Empresa %s (id=%s) — detalles faltantes, rango emisión [%s, %s)",
        company_name,
        company_id,
        range_start,
        range_end_exclusive,
    )

    block = 0
    docs_ok = 0
    details_ins = 0
    details_ign = 0
    doc_errors = 0
    pending: list[tuple[Any, ...]] = []

    while True:
        ids = fetch_docs_missing_details_chunk(
            company_id,
            range_start,
            range_end_exclusive,
            DOCS_CHUNK,
            schema,
        )
        if not ids:
            break

        block += 1
        log.info(
            "Empresa %s — bloque %s: %s documentos sin detalle (bsale_id %s..%s)",
            company_name,
            block,
            len(ids),
            ids[0],
            ids[-1],
        )

        for doc_id in ids:
            items, skip_reason = bsale_get_details(session, doc_id, token)
            if skip_reason is not None:
                doc_errors += 1
                log.warning(
                    "Empresa %s — doc %s omitido: %s",
                    company_name,
                    doc_id,
                    skip_reason,
                )
                continue

            try:
                new_rows = [detail_row_from_bsale(company_id, doc_id, it) for it in items]
            except Exception as e:
                doc_errors += 1
                log.error(
                    "Empresa %s — error mapeando líneas doc %s: %s",
                    company_name,
                    doc_id,
                    e,
                )
                continue

            docs_ok += 1
            pending.extend(new_rows)

            if len(pending) >= DETAILS_BATCH:
                ins, ign = insert_details_batch(schema, pending)
                details_ins += ins
                details_ign += ign
                log.info(
                    "Empresa %s — bloque %s commit lote: insertados=%s ignorados=%s (pendiente docs_ok=%s)",
                    company_name,
                    block,
                    ins,
                    ign,
                    docs_ok,
                )
                pending.clear()

        if pending:
            ins, ign = insert_details_batch(schema, pending)
            details_ins += ins
            details_ign += ign
            log.info(
                "Empresa %s — bloque %s commit final: insertados=%s ignorados=%s",
                company_name,
                block,
                ins,
                ign,
            )
            pending.clear()

        log.info(
            "Empresa %s — bloque %s COMPLETO docs_ok=%s insertados=%s ignorados=%s errores_doc=%s",
            company_name,
            block,
            docs_ok,
            details_ins,
            details_ign,
            doc_errors,
        )

    return docs_ok, details_ins, details_ign, doc_errors


def main() -> None:
    log.info("SYNC DOCUMENT DETAILS START")

    env_from = os.getenv("SYNC_FROM_DATE")
    env_to = os.getenv("SYNC_TO_DATE")

    if env_from and env_to:
        log.info("Usando rango desde variables de entorno: %s → %s", env_from, env_to)
        start_d = parse_date_const(env_from, "SYNC_FROM_DATE")
        end_d = parse_date_const(env_to, "SYNC_TO_DATE")
    else:
        log.info("Usando rango por defecto del script: %s → %s", SYNC_FROM_DATE, SYNC_TO_DATE)
        start_d = parse_date_const(SYNC_FROM_DATE, "SYNC_FROM_DATE")
        end_d = parse_date_const(SYNC_TO_DATE, "SYNC_TO_DATE")

    if end_d < start_d:
        die("SYNC_TO_DATE debe ser >= SYNC_FROM_DATE", 1)

    log.info("Rango final aplicado: %s → %s", start_d, end_d)

    num_days = (end_d - start_d).days + 1
    if num_days > WARN_RANGE_DAYS:
        log.warning(
            "Rango grande: %s días (> %s). Considere acortar SYNC_* en el script o trocear el Job.",
            num_days,
            WARN_RANGE_DAYS,
        )

    range_start, range_end_exclusive = range_datetimes_utc(start_d, end_d)
    schema = (os.getenv("PG_DOCUMENTS_SCHEMA") or "bsale").strip() or "bsale"

    companies = get_companies()
    if not companies:
        die("No hay empresas activas con token configurado (bsale.companies + env).", 1)

    session = requests.Session()
    run_docs = 0
    run_ins = 0
    run_ign = 0
    run_err = 0

    for company in companies:
        log.info("")
        d_ok, ins, ign, errs = sync_company_details(
            company["company_id"],
            company["token"],
            company["name"],
            range_start,
            range_end_exclusive,
            schema,
            session,
        )
        run_docs += d_ok
        run_ins += ins
        run_ign += ign
        run_err += errs
        log.info(
            "Empresa %s — resumen: docs_ok=%s detalles_insertados=%s detalles_ignorados=%s errores_doc=%s",
            company["name"],
            d_ok,
            ins,
            ign,
            errs,
        )

    conn.close()
    log.info(
        "SYNC DOCUMENT DETAILS COMPLETE — empresas=%s días=%s docs_ok=%s insertados=%s ignorados=%s errores_doc=%s",
        len(companies),
        num_days,
        run_docs,
        run_ins,
        run_ign,
        run_err,
    )


if __name__ == "__main__":
    main()
