"""
Sincroniza documentos Bsale → PostgreSQL (bsale.documents).

Mismo patrón que sync_prices_costs.py:
  - Conexión Postgres al inicio con PG_HOST, PG_DB, PG_USER, PG_PASSWORD (Coolify).
  - company_id y token por fila en bsale.companies (bsale_token = nombre de env con el token).

Ejecución del Job (sin argumentos):
  python sync_documents.py

Editar SYNC_FROM_DATE / SYNC_TO_DATE abajo para cambiar el rango (YYYY-MM-DD, inclusive).
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
log = logging.getLogger("sync_documents")

BASE_BSALE = "https://api.bsale.io/v1"
LIMIT_BSALE = 50
BATCH_PG = 200
MAX_BSALE_TRANSIENT = 40
WARN_RANGE_DAYS = 31

# Rango de emisión (inclusive). Ajustar aquí; el Job no recibe fechas por CLI.
SYNC_FROM_DATE = "2026-04-01"
SYNC_TO_DATE = "2026-04-05"

# ---------------------------------
# POSTGRES (igual que sync_prices_costs.py)
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


def bsale_get(session: requests.Session, params: dict[str, Any], token: str) -> dict[str, Any]:
    transient = 0
    while True:
        try:
            r = session.get(
                f"{BASE_BSALE}/documents.json",
                headers={"access_token": token},
                params=params,
                timeout=30,
            )
        except requests.RequestException as e:
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(f"Bsale: demasiados errores de red ({transient}): {e}", 1)
            log.warning("Bsale error de red (%s/%s): %s — reintento en 3 s", transient, MAX_BSALE_TRANSIENT, e)
            time.sleep(3)
            continue

        if r.status_code == 401:
            body = (r.text or "")[:800]
            die(f"Bsale 401 Unauthorized — token inválido o expirado. Respuesta: {body}", 1)

        if r.status_code == 403:
            die(f"Bsale 403 Forbidden — /documents.json. Respuesta: {(r.text or '')[:800]}", 1)

        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            log.warning("Bsale 429 rate limit — esperando %s s", wait)
            time.sleep(wait)
            continue

        if 400 <= r.status_code < 500:
            die(f"Bsale error cliente HTTP {r.status_code}: {(r.text or '')[:800]}", 1)

        if r.status_code in (500, 502, 503, 504):
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(
                    f"Bsale: demasiados errores de servidor HTTP {r.status_code}",
                    1,
                )
            log.warning(
                "Bsale HTTP %s — reintento en 3 s (%s/%s)",
                r.status_code,
                transient,
                MAX_BSALE_TRANSIENT,
            )
            time.sleep(3)
            continue

        if not (200 <= r.status_code < 300):
            die(f"Bsale respuesta inesperada HTTP {r.status_code}: {(r.text or '')[:800]}", 1)

        transient = 0
        try:
            return r.json()
        except ValueError as e:
            die(f"Bsale: JSON inválido en respuesta 200: {e}", 1)


def row_from_bsale(company_id: int, d: dict[str, Any]) -> tuple[Any, ...]:
    bsale_id = d["id"]
    emission_raw = d.get("emissionDate")
    emission_date = None
    if emission_raw is not None:
        emission_date = datetime.fromtimestamp(int(emission_raw), tz=timezone.utc)

    return (
        company_id,
        bsale_id,
        d.get("number"),
        emission_date,
        (d.get("document_type") or {}).get("id"),
        (d.get("client") or {}).get("id"),
        (d.get("office") or {}).get("id"),
        (d.get("user") or {}).get("id"),
        d.get("totalAmount"),
        d.get("state"),
        d.get("urlPdf"),
        d.get("token"),
    )


def upsert_batch(
    schema: str,
    table: str,
    rows: list[tuple[Any, ...]],
) -> tuple[int, int]:
    if not rows:
        return 0, 0

    insert_sql = sql.SQL(
        """
        INSERT INTO {schema}.{table} (
            company_id,
            bsale_id,
            number,
            emission_date,
            document_type_id,
            client_id,
            office_id,
            user_id,
            total_amount,
            state,
            url_pdf,
            token
        )
        VALUES %s
        ON CONFLICT (company_id, bsale_id)
        DO UPDATE SET
            number = EXCLUDED.number,
            emission_date = EXCLUDED.emission_date,
            document_type_id = EXCLUDED.document_type_id,
            client_id = EXCLUDED.client_id,
            office_id = EXCLUDED.office_id,
            user_id = EXCLUDED.user_id,
            total_amount = EXCLUDED.total_amount,
            state = EXCLUDED.state,
            url_pdf = EXCLUDED.url_pdf,
            token = EXCLUDED.token
        RETURNING (xmax = 0) AS inserted
        """
    ).format(schema=sql.Identifier(schema), table=sql.Identifier(table))

    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    try:
        execute_values(
            cur,
            insert_sql.as_string(conn),
            rows,
            template=template,
            page_size=len(rows),
        )
        returned = cur.fetchall()
        conn.commit()
    except Exception as e:
        conn.rollback()
        die(f"PostgreSQL error al upsert de lote ({len(rows)} filas): {type(e).__name__}: {e}", 1)

    n_ins = sum(1 for (ins,) in returned if ins)
    n_upd = len(returned) - n_ins
    return n_ins, n_upd


def parse_date_const(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError:
        die(f"{label} inválida (YYYY-MM-DD): {s!r}", 1)


def iter_days(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def sync_company_documents(
    company_id: int,
    token: str,
    company_name: str,
    start_d: date,
    end_d: date,
    schema: str,
    table: str,
    session: requests.Session,
) -> tuple[int, int, int]:
    """Retorna (insertados, actualizados, páginas_api)."""
    total_ins = 0
    total_upd = 0
    grand_pages = 0

    num_days = (end_d - start_d).days + 1
    log.info(
        "Empresa %s (id=%s) — rango %s..%s (%s días)",
        company_name,
        company_id,
        start_d,
        end_d,
        num_days,
    )

    for day in iter_days(start_d, end_d):
        day_start = datetime.combine(day, dt_time.min)
        start_ts = int(day_start.timestamp())
        end_ts = int((day_start + timedelta(days=1)).timestamp()) - 1

        day_ins = 0
        day_upd = 0
        day_pages = 0
        offset = 0
        batch: list[tuple[Any, ...]] = []

        log.info("Día %s — epoch [%s, %s]", day.isoformat(), start_ts, end_ts)

        while True:
            params = {
                "limit": LIMIT_BSALE,
                "offset": offset,
                "emissiondaterange": f"[{start_ts},{end_ts}]",
            }
            data = bsale_get(session, params, token)
            items = data.get("items") or []
            day_pages += 1
            grand_pages += 1

            if not items:
                log.info("Día %s — sin más documentos (offset=%s)", day.isoformat(), offset)
                break

            for d in items:
                batch.append(row_from_bsale(company_id, d))

            if len(batch) >= BATCH_PG:
                ins, upd = upsert_batch(schema, table, batch)
                day_ins += ins
                day_upd += upd
                total_ins += ins
                total_upd += upd
                log.info(
                    "Día %s — commit lote: insertados=%s actualizados=%s (páginas día=%s)",
                    day.isoformat(),
                    ins,
                    upd,
                    day_pages,
                )
                batch.clear()

            offset += LIMIT_BSALE

        if batch:
            ins, upd = upsert_batch(schema, table, batch)
            day_ins += ins
            day_upd += upd
            total_ins += ins
            total_upd += upd
            log.info(
                "Día %s — commit lote final: insertados=%s actualizados=%s",
                day.isoformat(),
                ins,
                upd,
            )

        log.info(
            "Día %s COMPLETO — páginas=%s insertados=%s actualizados=%s",
            day.isoformat(),
            day_pages,
            day_ins,
            day_upd,
        )

    return total_ins, total_upd, grand_pages


def main() -> None:
    log.info("SYNC DOCUMENTS START")

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

    schema = (os.getenv("PG_DOCUMENTS_SCHEMA") or "bsale").strip() or "bsale"
    table = "documents"

    companies = get_companies()
    if not companies:
        die("No hay empresas activas con token configurado (bsale.companies + env).", 1)

    session = requests.Session()
    run_ins = 0
    run_upd = 0
    run_pages = 0

    for company in companies:
        log.info("")
        ins, upd, pages = sync_company_documents(
            company["company_id"],
            company["token"],
            company["name"],
            start_d,
            end_d,
            schema,
            table,
            session,
        )
        run_ins += ins
        run_upd += upd
        run_pages += pages
        log.info(
            "Empresa %s — subtotal insertados=%s actualizados=%s páginas_api=%s",
            company["name"],
            ins,
            upd,
            pages,
        )

    conn.close()
    log.info(
        "SYNC DOCUMENTS COMPLETE — empresas=%s días=%s páginas=%s insertados=%s actualizados=%s",
        len(companies),
        num_days,
        run_pages,
        run_ins,
        run_upd,
    )


if __name__ == "__main__":
    main()
