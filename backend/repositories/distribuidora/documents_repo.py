"""Upsert de ``distribuidora.documents``."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values


def _emission_sort_key(r: dict[str, Any]) -> tuple[float, int]:
    """Mayor = más reciente: ``emission_date`` (timestamp), empate ``document_id``."""
    em = r.get("emission_date")
    did = int(r["document_id"])
    if em is None:
        return (-1.0, did)
    try:
        ts = float(em.timestamp())
    except Exception:
        ts = -1.0
    return (ts, did)


def _dedupe_logical_latest(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Misma clave (company_id, office_id, document_type_id, number): deja una fila
    (la de emisión más reciente; empate por mayor ``document_id``).
    Filas sin ``number`` o sin ``document_type_id`` se conservan todas (clave por PK).
    """
    by_logical: dict[tuple[int, int, int, int], dict[str, Any]] = {}
    rest: list[dict[str, Any]] = []
    for r in rows:
        num, tid = r.get("number"), r.get("document_type_id")
        if num is None or tid is None:
            rest.append(r)
            continue
        k = (int(r["company_id"]), int(r["office_id"]), int(tid), int(num))
        prev = by_logical.get(k)
        if prev is None or _emission_sort_key(r) > _emission_sort_key(prev):
            by_logical[k] = r
    return list(by_logical.values()) + rest


def _delete_stale_logical_versions(cur, rows: list[dict[str, Any]]) -> None:
    """Elimina otras versiones Bsale del mismo folio (CASCADE limpia hijos del ``document_id`` viejo)."""
    seen: set[tuple[int, int, int, int, int]] = set()
    for r in rows:
        num, tid = r.get("number"), r.get("document_type_id")
        if num is None or tid is None:
            continue
        key = (
            int(r["company_id"]),
            int(r["office_id"]),
            int(tid),
            int(num),
            int(r["document_id"]),
        )
        if key in seen:
            continue
        seen.add(key)
        cur.execute(
            """
            DELETE FROM distribuidora.documents
            WHERE company_id = %s
              AND office_id = %s
              AND document_type_id = %s
              AND number = %s
              AND document_id <> %s
            """,
            (
                int(r["company_id"]),
                int(r["office_id"]),
                int(tid),
                int(num),
                int(r["document_id"]),
            ),
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


def document_dict_from_bsale(
    d: dict[str, Any],
    *,
    company_id: int = 3,
    default_office_id: int = 1,
) -> dict[str, Any] | None:
    """Mapea JSON documento Bsale → fila ``documents``. None si no aplica (office distinto)."""
    office = d.get("office") or {}
    oid = office.get("id")
    if oid is None or int(oid) != default_office_id:
        return None
    comp = (d.get("company") or {}).get("id")
    if comp is not None and int(comp) != company_id:
        return None

    doc_type = d.get("document_type") or {}
    client = d.get("client") or {}
    user = d.get("user") or {}
    price_list = d.get("priceList") or d.get("price_list") or {}

    def _ts(raw: Any):
        if raw is None:
            return None
        try:
            return datetime.fromtimestamp(int(raw), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None

    return {
        "document_id": int(d["id"]),
        "number": int(d["number"]) if d.get("number") is not None else None,
        "document_type_id": int(doc_type["id"]) if doc_type.get("id") is not None else None,
        "client_id": int(client["id"]) if client.get("id") is not None else None,
        "office_id": int(oid),
        "company_id": company_id,
        "user_id": int(user["id"]) if user.get("id") is not None else None,
        "emission_date": _ts(d.get("emissionDate")),
        "expiration_date": _ts(d.get("expirationDate")),
        "generation_date": _ts(d.get("generationDate")),
        "total_amount": _num(d.get("totalAmount")),
        "net_amount": _num(d.get("netAmount")),
        "tax_amount": _num(d.get("taxAmount")),
        "state": d.get("state"),
        "commercial_state": d.get("commercialState"),
        "informed_sii": d.get("informedSii"),
        "municipality": d.get("municipality"),
        "city": d.get("city"),
        "address": d.get("address"),
        "token": d.get("token"),
        "url_pdf": d.get("urlPdf"),
        "url_public_view": d.get("urlPublicView"),
        "price_list_id": int(price_list["id"]) if isinstance(price_list, dict) and price_list.get("id") is not None else None,
        "tracking_number": d.get("trackingNumber"),
        "raw_data": Json(d),
    }


def upsert_documents(cur, rows: list[dict[str, Any]]) -> tuple[int, int]:
    """
    Inserta/actualiza por ``document_id``.

    Si Bsale envía un **nuevo** ``document_id`` para el mismo folio lógico
    (``company_id``, ``office_id``, ``document_type_id``, ``number``), elimina primero
    filas anteriores con esa clave (``ON DELETE CASCADE`` en hijos) y luego hace upsert
    por PK para que no choque con ``uq_distribuidora_documents_logical``.
    """
    if not rows:
        return 0, 0
    rows = _dedupe_logical_latest(rows)
    _delete_stale_logical_versions(cur, rows)
    cols = [
        "document_id",
        "number",
        "document_type_id",
        "client_id",
        "office_id",
        "company_id",
        "user_id",
        "emission_date",
        "expiration_date",
        "generation_date",
        "total_amount",
        "net_amount",
        "tax_amount",
        "state",
        "commercial_state",
        "informed_sii",
        "municipality",
        "city",
        "address",
        "token",
        "url_pdf",
        "url_public_view",
        "price_list_id",
        "tracking_number",
        "raw_data",
    ]
    values = [tuple(r[c] for c in cols) for r in rows]
    sql = f"""
        INSERT INTO distribuidora.documents ({", ".join(cols)}, created_at, updated_at)
        VALUES %s
        ON CONFLICT (document_id) DO UPDATE SET
            number = EXCLUDED.number,
            document_type_id = EXCLUDED.document_type_id,
            client_id = EXCLUDED.client_id,
            office_id = EXCLUDED.office_id,
            company_id = EXCLUDED.company_id,
            user_id = EXCLUDED.user_id,
            emission_date = EXCLUDED.emission_date,
            expiration_date = EXCLUDED.expiration_date,
            generation_date = EXCLUDED.generation_date,
            total_amount = EXCLUDED.total_amount,
            net_amount = EXCLUDED.net_amount,
            tax_amount = EXCLUDED.tax_amount,
            state = EXCLUDED.state,
            commercial_state = EXCLUDED.commercial_state,
            informed_sii = EXCLUDED.informed_sii,
            municipality = EXCLUDED.municipality,
            city = EXCLUDED.city,
            address = EXCLUDED.address,
            token = EXCLUDED.token,
            url_pdf = EXCLUDED.url_pdf,
            url_public_view = EXCLUDED.url_public_view,
            price_list_id = EXCLUDED.price_list_id,
            tracking_number = EXCLUDED.tracking_number,
            raw_data = EXCLUDED.raw_data,
            updated_at = NOW()
    """
    template = "(" + ",".join(["%s"] * len(cols)) + ",NOW(),NOW())"
    try:
        execute_values(cur, sql, values, template=template, page_size=len(values))
    except Exception:
        try:
            cur.connection.rollback()
        except Exception:
            pass
        raise
    return len(rows), len(rows)


def parse_document_sellers_response(data: dict[str, Any]) -> tuple[int | None, str | None]:
    """
    Respuesta típica GET ``/v1/documents/{id}/sellers.json`` (Bsale):
    ``{ "items": [ { "id", "firstName", "lastName", ... } ] }``.
    Se usa el primer ítem como vendedor asociado al documento.
    """
    if not isinstance(data, dict):
        return None, None
    items = data.get("items")
    if not isinstance(items, list) or len(items) == 0:
        return None, None
    s = items[0]
    if not isinstance(s, dict):
        return None, None
    raw_id = s.get("id")
    try:
        seller_id = int(raw_id) if raw_id is not None else None
    except (TypeError, ValueError):
        seller_id = None
    fn = str(s.get("firstName") or s.get("firstname") or "").strip()
    ln = str(s.get("lastName") or s.get("lastname") or "").strip()
    name = f"{fn} {ln}".strip() or None
    return seller_id, name


def update_document_seller_if_empty(
    cur,
    document_id: int,
    seller_id: int | None,
    seller_name: str | None,
) -> bool:
    """
    Persiste ``seller_id`` / ``seller_name`` solo si ``seller_name`` está vacío
    (no sobrescribe vendedor ya fijado).
    """
    if not seller_name or not str(seller_name).strip():
        return False
    name = str(seller_name).strip()
    cur.execute(
        """
        UPDATE distribuidora.documents
        SET
            seller_id = %s,
            seller_name = %s,
            updated_at = NOW()
        WHERE document_id = %s
          AND (seller_name IS NULL OR BTRIM(seller_name) = '')
        """,
        (seller_id, name, document_id),
    )
    return cur.rowcount > 0
