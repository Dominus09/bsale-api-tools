"""
Sincroniza documentos Bsale → PostgreSQL (tabla documents). Job no interactivo (p. ej. Coolify).

Uso exacto:
  python sync_documents.py <from_date> <to_date>

Env obligatorias: COMPANY_ID, BSALE_TOKEN_SPA, PG_HOST, PG_DB, PG_USER, PG_PASSWORD
Opcional: PG_PORT (default 5432), PG_DOCUMENTS_SCHEMA (default bsale)

Requiere UNIQUE (company_id, bsale_id) en la tabla destino.
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, time as dt_time, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values

load_dotenv()

# Logs a stdout para que Coolify muestre la salida del Job
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
# Reintentos agregados por errores de red o 5xx Bsale antes de fallar el Job
MAX_BSALE_TRANSIENT = 40
WARN_RANGE_DAYS = 31

REQUIRED_ENV = (
    "COMPANY_ID",
    "BSALE_TOKEN_SPA",
    "PG_HOST",
    "PG_DB",
    "PG_USER",
    "PG_PASSWORD",
)


def die(msg: str, code: int = 1) -> None:
    log.error("%s", msg)
    sys.exit(code)


def parse_argv_dates() -> tuple[str, str]:
    """Exige sys.argv: script, from_date, to_date (solo dos argumentos de fecha)."""
    if len(sys.argv) != 3:
        print(
            "Uso: python sync_documents.py <from_date> <to_date>\n"
            "  Fechas en formato YYYY-MM-DD (inclusive). Ejemplo:\n"
            "  python sync_documents.py 2026-04-01 2026-04-05",
            file=sys.stderr,
        )
        sys.exit(2)
    return sys.argv[1].strip(), sys.argv[2].strip()


def validate_env() -> None:
    missing = [name for name in REQUIRED_ENV if not os.getenv(name) or not str(os.getenv(name)).strip()]
    if missing:
        die(
            "Faltan variables de entorno obligatorias: "
            + ", ".join(missing)
            + ". Defina COMPANY_ID, BSALE_TOKEN_SPA, PG_HOST, PG_DB, PG_USER, PG_PASSWORD.",
            2,
        )


def company_id_from_env() -> int:
    raw = os.environ["COMPANY_ID"].strip()
    try:
        return int(raw)
    except ValueError:
        die(f"COMPANY_ID debe ser entero, recibido: {raw!r}", 2)


def connect_pg():
    import psycopg2

    try:
        return psycopg2.connect(
            host=os.environ["PG_HOST"].strip(),
            database=os.environ["PG_DB"].strip(),
            user=os.environ["PG_USER"].strip(),
            password=os.environ["PG_PASSWORD"].strip(),
            port=int(os.getenv("PG_PORT", "5432")),
        )
    except Exception as e:
        die(f"PostgreSQL: no se pudo conectar ({type(e).__name__}): {e}", 1)


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
                die(
                    f"Bsale: demasiados errores de red ({transient}): {e}",
                    1,
                )
            log.warning("Bsale error de red (%s/%s): %s — reintento en 3 s", transient, MAX_BSALE_TRANSIENT, e)
            time.sleep(3)
            continue

        if r.status_code == 401:
            body = (r.text or "")[:800]
            die(f"Bsale 401 Unauthorized — revise BSALE_TOKEN_SPA. Respuesta: {body}", 1)

        if r.status_code == 403:
            die(f"Bsale 403 Forbidden — sin permiso para /documents.json. Respuesta: {(r.text or '')[:800]}", 1)

        if r.status_code == 429:
            try:
                wait = int(r.json().get("retry_after", 60))
            except Exception:
                wait = 60
            log.warning("Bsale 429 rate limit — esperando %s s", wait)
            time.sleep(wait)
            continue

        if 400 <= r.status_code < 500:
            die(
                f"Bsale error cliente HTTP {r.status_code}: {(r.text or '')[:800]}",
                1,
            )

        if r.status_code in (500, 502, 503, 504):
            transient += 1
            if transient >= MAX_BSALE_TRANSIENT:
                die(
                    f"Bsale: demasiados errores de servidor HTTP {r.status_code} (últimos {transient} intentos)",
                    1,
                )
            log.warning(
                "Bsale HTTP %s — reintento en 3 s (fallos transitorios %s/%s)",
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
    cur,
    conn,
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
            insert_sql.as_string(cur.connection),
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


def parse_date(s: str, label: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        die(f"{label} inválida (use YYYY-MM-DD): {s!r}", 2)


def iter_days(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> None:
    from_s, to_s = parse_argv_dates()
    validate_env()

    start_d = parse_date(from_s, "from_date")
    end_d = parse_date(to_s, "to_date")
    if end_d < start_d:
        die("to_date debe ser >= from_date", 2)

    num_days = (end_d - start_d).days + 1
    if num_days > WARN_RANGE_DAYS:
        log.warning(
            "Rango grande: %s días (> %s). En Coolify puede acercarse al límite de tiempo; "
            "considere trocear el Job.",
            num_days,
            WARN_RANGE_DAYS,
        )

    company_id = company_id_from_env()
    schema = os.getenv("PG_DOCUMENTS_SCHEMA", "bsale").strip() or "bsale"
    table = "documents"
    token = os.environ["BSALE_TOKEN_SPA"].strip()

    log.info(
        "SYNC DOCUMENTS START company_id=%s rango=%s..%s (%s días) schema=%s.%s",
        company_id,
        start_d,
        end_d,
        num_days,
        schema,
        table,
    )

    total_ins = 0
    total_upd = 0
    grand_pages = 0
    session = requests.Session()
    conn = connect_pg()

    try:
        with conn:
            with conn.cursor() as cur:
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
                            ins, upd = upsert_batch(cur, conn, schema, table, batch)
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
                        ins, upd = upsert_batch(cur, conn, schema, table, batch)
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
    finally:
        conn.close()

    log.info(
        "SYNC DOCUMENTS COMPLETE — días=%s páginas=%s insertados=%s actualizados=%s",
        num_days,
        grand_pages,
        total_ins,
        total_upd,
    )


if __name__ == "__main__":
    main()
