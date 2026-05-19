#!/usr/bin/env python3
"""
Flujo de prueba: descarga documentos Bsale (solo office_id = 1) hacia tablas test y exporta Excel.

NO usa FastAPI. NO se ejecuta solo: invocar manualmente.

Requisitos:
  - Haber aplicado backend/sql/documents_bc_test_schema.sql
  - Variables PG_HOST, PG_DB, PG_USER, PG_PASSWORD (y opcional PG_PORT)
  - ``BSALE_TOKEN`` o ``BSALE_TOKEN_SPA`` en el entorno o en ``.env`` (no pegar token en el código).

Salida:
  exports/bsale_documents_office_1_test.xlsx

Ejecución (desde la raíz del repositorio):

  1) Crear solo tablas de prueba (no toca tablas productivas):
       psql "host=... dbname=... user=... password=..." -f backend/sql/documents_bc_test_schema.sql
     O con variables PG_* ya exportadas:
       psql -h "$PG_HOST" -U "$PG_USER" -d "$PG_DB" -f backend/sql/documents_bc_test_schema.sql

  2) Exportar conexión PostgreSQL y token Bsale, por ejemplo:
       set PG_HOST=...
       set PG_DB=...
       set PG_USER=...
       set PG_PASSWORD=...
       set PG_PORT=5432
       set BSALE_TOKEN=tu_token_spa

  3) Ajustar en este archivo DATE_FROM, DATE_TO si aplica.

  4) Ejecutar:
       python -m backend.debug.test_bsale_documents_office_1
"""

from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import requests
from psycopg2.extras import Json

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from backend.services.distribuidora.bsale_params import merge_bsale_office_query
from backend.utils.bsale_token_env import require_bsale_token

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# CONFIGURACIÓN MANUAL (La Quillotana SPA — office 1)
# ---------------------------------------------------------------------------

# Rango de emisión inclusive (UTC, día calendario alineado a sync_documents / Bsale emissiondaterange).
DATE_FROM = "2026-04-01"
DATE_TO = "2026-04-03"

OFFICE_ID = 1
LIMIT_BSALE = 50
REQUEST_TIMEOUT_SEC = 60
SLEEP_BETWEEN_CALLS_SEC = 0.25
BASE_BSALE = "https://api.bsale.io/v1"

EXPORT_REL_PATH = "exports/bsale_documents_office_1_test.xlsx"


def _token() -> str:
    return require_bsale_token(label="test_bsale_documents_office_1")


def _parse_date(s: str, label: str) -> date:
    try:
        return datetime.strptime(s.strip(), "%Y-%m-%d").date()
    except ValueError as e:
        raise SystemExit(f"[test_bsale_documents] {label} inválida (YYYY-MM-DD): {s!r}") from e


def _utc_day_epoch_bounds(d: date) -> tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)
    end_ts = int((start + timedelta(days=1)).timestamp()) - 1
    return int(start.timestamp()), end_ts


def _num(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, Decimal)):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _ts(raw: Any) -> datetime | None:
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(int(raw), tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None


def _number_bigint(d: dict[str, Any]) -> int | None:
    raw = d.get("number")
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        try:
            i = int(raw)
            return i if i == raw else None
        except (TypeError, ValueError, OverflowError):
            return None
    s = str(raw).strip()
    if not s:
        return None
    try:
        return int(s, 10)
    except ValueError:
        return None


def _client_name(client: dict[str, Any]) -> str | None:
    if not client:
        return None
    parts: list[str] = []
    co = client.get("company")
    if co is not None and str(co).strip():
        parts.append(str(co).strip())
    fn = (client.get("firstName") or "").strip()
    ln = (client.get("lastName") or "").strip()
    if fn or ln:
        parts.append(f"{fn} {ln}".strip())
    if not parts:
        return None
    return " | ".join(parts)


def _client_rut(client: dict[str, Any]) -> str | None:
    if not client:
        return None
    for k in ("code", "rut", "identifier"):
        v = client.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def bsale_get(session: requests.Session, token: str, path: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{BASE_BSALE}{path}"
    r = session.get(
        url,
        headers={"access_token": token},
        params=params,
        timeout=REQUEST_TIMEOUT_SEC,
    )
    if r.status_code == 429:
        wait = 60
        try:
            wait = int(r.json().get("retry_after", 60))
        except Exception:
            pass
        time.sleep(wait)
        r = session.get(
            url,
            headers={"access_token": token},
            params=params,
            timeout=REQUEST_TIMEOUT_SEC,
        )
    if not (200 <= r.status_code < 300):
        raise RuntimeError(f"Bsale HTTP {r.status_code}: {(r.text or '')[:400]}")
    return r.json()


def map_document_row(d: dict[str, Any]) -> tuple[Any, ...]:
    doc_type = d.get("document_type") or {}
    client = d.get("client") or {}
    office = d.get("office") or {}
    oid = office.get("id")
    em = _ts(d.get("emissionDate"))
    gen = _ts(d.get("generationDate"))
    st_raw = d.get("state")
    st_val: int | None
    if st_raw is None:
        st_val = None
    else:
        try:
            st_val = int(st_raw)
        except (TypeError, ValueError):
            st_val = None

    return (
        int(d["id"]),
        _number_bigint(d),
        int(doc_type["id"]) if doc_type.get("id") is not None else None,
        int(oid) if oid is not None else None,
        int(client["id"]) if client.get("id") is not None else None,
        _client_name(client),
        _client_rut(client),
        em.date() if em else None,
        gen,
        _num(d.get("totalAmount")),
        _num(d.get("netAmount")),
        _num(d.get("taxAmount")),
        st_val,
        d.get("urlPdf"),
        Json(d),
    )


UPSERT_DOCUMENT_SQL = """
INSERT INTO app.documents_bc_test (
    bsale_id, number, document_type_id, office_id, client_id, client_name, client_rut,
    emission_date, generation_date, total_amount, net_amount, tax_amount, state, url_pdf, raw_json
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (bsale_id) DO UPDATE SET
    number = EXCLUDED.number,
    document_type_id = EXCLUDED.document_type_id,
    office_id = EXCLUDED.office_id,
    client_id = EXCLUDED.client_id,
    client_name = EXCLUDED.client_name,
    client_rut = EXCLUDED.client_rut,
    emission_date = EXCLUDED.emission_date,
    generation_date = EXCLUDED.generation_date,
    total_amount = EXCLUDED.total_amount,
    net_amount = EXCLUDED.net_amount,
    tax_amount = EXCLUDED.tax_amount,
    state = EXCLUDED.state,
    url_pdf = EXCLUDED.url_pdf,
    raw_json = EXCLUDED.raw_json
"""


def map_detail_row(document_bsale_id: int, item: dict[str, Any]) -> tuple[Any, ...]:
    variant = item.get("variant") or {}
    if not isinstance(variant, dict):
        variant = {}
    product = variant.get("product") if isinstance(variant.get("product"), dict) else {}
    if not isinstance(product, dict):
        product = item.get("product") if isinstance(item.get("product"), dict) else {}
    pid = variant.get("id")
    product_name = None
    if isinstance(product, dict) and product.get("name"):
        product_name = str(product["name"])
    if not product_name:
        product_name = item.get("description") if item.get("description") else None
    variant_name = variant.get("description") or variant.get("code")
    did = item.get("id")
    if did is None:
        raise ValueError("detail sin id")
    return (
        document_bsale_id,
        int(did),
        int(pid) if pid is not None else None,
        product_name,
        str(variant_name) if variant_name is not None else None,
        _num(item.get("quantity")),
        _num(item.get("netUnitValue")),
        _num(item.get("totalUnitValue")),
        _num(item.get("netAmount")),
        _num(item.get("totalAmount")),
        Json(item),
    )


UPSERT_DETAIL_SQL = """
INSERT INTO app.document_details_bc_test (
    document_bsale_id, detail_bsale_id, variant_id, product_name, variant_name,
    quantity, net_unit_value, total_unit_value, net_amount, total_amount, raw_json
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (detail_bsale_id) DO UPDATE SET
    document_bsale_id = EXCLUDED.document_bsale_id,
    variant_id = EXCLUDED.variant_id,
    product_name = EXCLUDED.product_name,
    variant_name = EXCLUDED.variant_name,
    quantity = EXCLUDED.quantity,
    net_unit_value = EXCLUDED.net_unit_value,
    total_unit_value = EXCLUDED.total_unit_value,
    net_amount = EXCLUDED.net_amount,
    total_amount = EXCLUDED.total_amount,
    raw_json = EXCLUDED.raw_json
"""


def main() -> None:
    print("[test_bsale_documents] inicio")
    token = _token()
    date_from = _parse_date(DATE_FROM, "DATE_FROM")
    date_to = _parse_date(DATE_TO, "DATE_TO")
    if date_to < date_from:
        raise SystemExit("[test_bsale_documents] DATE_TO debe ser >= DATE_FROM")

    port = os.getenv("PG_PORT", "5432")
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=port,
    )
    cur = conn.cursor()

    session = requests.Session()
    n_docs = 0
    n_dets = 0
    n_err = 0

    d = date_from
    while d <= date_to:
        desde_ts, hasta_ts = _utc_day_epoch_bounds(d)
        offset = 0
        while True:
            params = merge_bsale_office_query(
                {
                    "limit": LIMIT_BSALE,
                    "offset": offset,
                    "emissiondaterange": f"[{desde_ts},{hasta_ts}]",
                },
                OFFICE_ID,
            )
            try:
                data = bsale_get(session, token, "/documents.json", params)
            except Exception as e:
                n_err += 1
                print(f"[test_bsale_documents] error listado documentos día={d} offset={offset}: {e}")
                break
            time.sleep(SLEEP_BETWEEN_CALLS_SEC)
            items = data.get("items") or []
            if not items:
                break

            for doc in items:
                office = (doc.get("office") or {})
                try:
                    oid = int(office["id"]) if office.get("id") is not None else None
                except (TypeError, ValueError):
                    oid = None
                if oid != OFFICE_ID:
                    continue
                try:
                    row = map_document_row(doc)
                    cur.execute(UPSERT_DOCUMENT_SQL, row)
                    conn.commit()
                    n_docs += 1
                    bsale_doc_id = int(doc["id"])
                    try:
                        det = bsale_get(
                            session,
                            token,
                            f"/documents/{bsale_doc_id}/details.json",
                            {"limit": 50, "offset": 0},
                        )
                    except Exception as e:
                        n_err += 1
                        print(f"[test_bsale_documents] error details document_id={bsale_doc_id}: {e}")
                        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
                        continue
                    time.sleep(SLEEP_BETWEEN_CALLS_SEC)
                    det_items = det.get("items") or []
                    off = 0
                    while True:
                        for it in det_items:
                            if not isinstance(it, dict):
                                continue
                            try:
                                dr = map_detail_row(bsale_doc_id, it)
                                cur.execute(UPSERT_DETAIL_SQL, dr)
                                n_dets += 1
                            except Exception as e:
                                n_err += 1
                                print(f"[test_bsale_documents] error fila detalle doc={bsale_doc_id}: {e}")
                        conn.commit()
                        if len(det_items) < 50:
                            break
                        off += len(det_items)
                        try:
                            det = bsale_get(
                                session,
                                token,
                                f"/documents/{bsale_doc_id}/details.json",
                                {"limit": 50, "offset": off},
                            )
                        except Exception as e:
                            n_err += 1
                            print(f"[test_bsale_documents] error details pag doc={bsale_doc_id} off={off}: {e}")
                            break
                        time.sleep(SLEEP_BETWEEN_CALLS_SEC)
                        det_items = det.get("items") or []
                        if not det_items:
                            break
                except Exception as e:
                    n_err += 1
                    conn.rollback()
                    print(f"[test_bsale_documents] error documento id={doc.get('id')}: {e}")

            offset += len(items)

        d += timedelta(days=1)

    cur.close()
    conn.close()

    print(f"[test_bsale_documents] cantidad documentos (filas upsert): {n_docs}")
    print(f"[test_bsale_documents] cantidad detalles (filas upsert): {n_dets}")
    print(f"[test_bsale_documents] errores: {n_err}")

    # --- Excel (pandas) ---
    import pandas as pd

    conn = psycopg2.connect(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT", "5432"),
    )
    df_docs = pd.read_sql_query(
        """
        SELECT id, bsale_id, number, document_type_id, office_id, client_id, client_name, client_rut,
               emission_date, generation_date, total_amount, net_amount, tax_amount, state, url_pdf,
               created_at
        FROM app.documents_bc_test
        WHERE office_id = %s
          AND emission_date >= %s
          AND emission_date <= %s
        ORDER BY emission_date, bsale_id
        """,
        conn,
        params=(OFFICE_ID, date_from, date_to),
    )
    df_dets = pd.read_sql_query(
        """
        SELECT d.id, d.document_bsale_id, d.detail_bsale_id, d.variant_id, d.product_name, d.variant_name,
               d.quantity, d.net_unit_value, d.total_unit_value, d.net_amount, d.total_amount, d.created_at
        FROM app.document_details_bc_test d
        INNER JOIN app.documents_bc_test m ON m.bsale_id = d.document_bsale_id
        WHERE m.office_id = %s
          AND m.emission_date >= %s
          AND m.emission_date <= %s
        ORDER BY d.document_bsale_id, d.detail_bsale_id
        """,
        conn,
        params=(OFFICE_ID, date_from, date_to),
    )
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(total_amount), 0)::numeric
        FROM app.documents_bc_test
        WHERE office_id = %s AND emission_date >= %s AND emission_date <= %s
        """,
        (OFFICE_ID, date_from, date_to),
    )
    (total_ventas,) = cur.fetchone()
    cur.execute(
        """
        SELECT COUNT(*)::bigint
        FROM app.documents_bc_test m
        WHERE m.office_id = %s
          AND m.emission_date >= %s
          AND m.emission_date <= %s
          AND NOT EXISTS (
            SELECT 1 FROM app.document_details_bc_test d WHERE d.document_bsale_id = m.bsale_id
          )
        """,
        (OFFICE_ID, date_from, date_to),
    )
    (sin_detalle,) = cur.fetchone()
    cur.close()
    conn.close()

    resumen = pd.DataFrame(
        [
            {
                "metrica": "cantidad_documentos",
                "valor": int(len(df_docs)),
            },
            {
                "metrica": "total_ventas_suma_total_amount",
                "valor": float(total_ventas) if total_ventas is not None else None,
            },
            {
                "metrica": "cantidad_detalles",
                "valor": int(len(df_dets)),
            },
            {
                "metrica": "documentos_sin_detalle",
                "valor": int(sin_detalle or 0),
            },
            {
                "metrica": "rango_fecha_desde",
                "valor": date_from.isoformat(),
            },
            {
                "metrica": "rango_fecha_hasta",
                "valor": date_to.isoformat(),
            },
            {
                "metrica": "office_id",
                "valor": OFFICE_ID,
            },
        ]
    )

    repo_root = Path(__file__).resolve().parents[2]
    out_path = repo_root / EXPORT_REL_PATH
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_docs.to_excel(writer, sheet_name="documents", index=False)
        df_dets.to_excel(writer, sheet_name="details", index=False)
        resumen.to_excel(writer, sheet_name="resumen", index=False)

    print(f"[test_bsale_documents] excel: {out_path}")
    print("[test_bsale_documents] fin")


if __name__ == "__main__":
    main()
