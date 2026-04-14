"""Upsert de ``distribuidora.documents``."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values


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
    INSERT ... ON CONFLICT (document_id) DO UPDATE.
    Devuelve (insertados_estimado, actualizados_estimado) — aproximado vía xmax.
    """
    if not rows:
        return 0, 0
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
    execute_values(cur, sql, values, template=template, page_size=len(values))
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
