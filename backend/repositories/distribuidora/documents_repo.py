"""Upsert de ``distribuidora.documents``."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values

logger = logging.getLogger(__name__)


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


def _num(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, Decimal)):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def _folio_number_from_bsale(d: dict[str, Any]) -> int | None:
    """
    Folio numérico para clave lógica (company, office, type, number).
    Si Bsale envía folio no numérico, retorna None y el upsert usa solo ``document_id``.
    """
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


def document_dict_from_bsale(
    d: dict[str, Any],
    *,
    company_id: int = 3,
    default_office_id: int = 1,
    sync_stats: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """
    Mapea JSON documento Bsale → fila ``documents``.

    * Solo ``company_id`` y ``office_id`` configurados (Distribuidora): si el JSON trae otra
      empresa u otra sucursal, no se persiste (defensa adicional al filtro ``officeid`` en API).
    * No filtra por tipo de documento (1/6/9/33/…): el filtrado fino va en vistas.
    """
    doc_id = d.get("id")

    comp = (d.get("company") or {}).get("id")
    if comp is not None:
        try:
            if int(comp) != company_id:
                if sync_stats is not None:
                    sync_stats["skipped_other_company"] = (
                        int(sync_stats.get("skipped_other_company") or 0) + 1
                    )
                logger.info(
                    "Documento omitido por company distinta: id=%s company_id=%r (esperado %s)",
                    doc_id,
                    comp,
                    company_id,
                )
                return None
        except (TypeError, ValueError):
            if sync_stats is not None:
                sync_stats["skipped_other_company"] = int(sync_stats.get("skipped_other_company") or 0) + 1
            logger.info(
                "Documento omitido por company inválida: id=%s company=%r",
                doc_id,
                comp,
            )
            return None

    office = d.get("office") or {}
    oid_raw = office.get("id")
    if oid_raw is None:
        if sync_stats is not None:
            sync_stats["skipped_other_office"] = int(sync_stats.get("skipped_other_office") or 0) + 1
        logger.info(
            "Documento omitido por office distinta: id=%s (sin office en JSON; se requiere office_id=%s)",
            doc_id,
            default_office_id,
        )
        return None
    try:
        oid = int(oid_raw)
    except (TypeError, ValueError):
        if sync_stats is not None:
            sync_stats["skipped_other_office"] = int(sync_stats.get("skipped_other_office") or 0) + 1
        logger.info(
            "Documento omitido por office distinta: id=%s office_id=%r no numérico (esperado %s)",
            doc_id,
            oid_raw,
            default_office_id,
        )
        return None
    if oid != default_office_id:
        if sync_stats is not None:
            sync_stats["skipped_other_office"] = int(sync_stats.get("skipped_other_office") or 0) + 1
        logger.info(
            "Documento omitido por office distinta: id=%s office_id=%s (esperado %s)",
            doc_id,
            oid,
            default_office_id,
        )
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
        "number": _folio_number_from_bsale(d),
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


def _execute_values_batch(
    cur,
    sql: str,
    batch: list[dict[str, Any]],
    cols: list[str],
    template: str,
) -> None:
    if not batch:
        return
    vals = [tuple(r[c] for c in cols) for r in batch]
    execute_values(cur, sql, vals, template=template, page_size=len(vals))


def _apply_persisted_document_ids_for_folio_rows(cur, rows: list[dict[str, Any]]) -> None:
    """
    Tras upsert por folio (clave lógica), alinea ``document_id`` en memoria con el PK en BD.

    En conflicto por folio **no** se actualiza ``document_id`` en la tabla; los hijos
    (``document_details``, etc.) deben seguir usando el id histórico persistido.
    """
    keys: list[tuple[int, int, int, int]] = []
    seen: set[tuple[int, int, int, int]] = set()
    for r in rows:
        if r.get("number") is None or r.get("document_type_id") is None:
            continue
        k = (
            int(r["company_id"]),
            int(r["office_id"]),
            int(r["document_type_id"]),
            int(r["number"]),
        )
        if k in seen:
            continue
        seen.add(k)
        keys.append(k)
    if not keys:
        return
    cur.execute(
        """
        SELECT document_id, company_id, office_id, document_type_id, number
        FROM distribuidora.documents
        WHERE (company_id, office_id, document_type_id, number) IN %s
        """,
        (tuple(keys),),
    )
    by_key: dict[tuple[int, int, int, int], int] = {}
    for row in cur.fetchall() or []:
        did, c, o, tid, num = int(row[0]), int(row[1]), int(row[2]), int(row[3]), int(row[4])
        by_key[(c, o, tid, num)] = did
    for r in rows:
        if r.get("number") is None or r.get("document_type_id") is None:
            continue
        k = (
            int(r["company_id"]),
            int(r["office_id"]),
            int(r["document_type_id"]),
            int(r["number"]),
        )
        stored = by_key.get(k)
        if stored is not None:
            r["document_id"] = stored


def _folio_conflict_key(r: dict[str, Any]) -> tuple[int, int, int, int] | None:
    if r.get("number") is None or r.get("document_type_id") is None:
        return None
    try:
        return (
            int(r["company_id"]),
            int(r["office_id"]),
            int(r["document_type_id"]),
            int(r["number"]),
        )
    except (TypeError, ValueError, KeyError):
        return None


def upsert_documents(
    cur,
    rows: list[dict[str, Any]],
    sync_stats: dict[str, Any] | None = None,
) -> tuple[int, int]:
    """
    Inserta/actualiza documentos sin borrar filas y **sin** cambiar ``document_id`` en updates.

    * Con folio completo (``company_id``, ``office_id``, ``document_type_id``, ``number``):
      ``ON CONFLICT`` en el índice único parcial; en update se refrescan **todos** los campos
      relevantes desde Bsale (incl. ``state``, montos, fechas, ``raw_data``, etc.).
    * Sin folio (``number`` o ``document_type_id`` nulos): upsert por ``document_id`` (PK).

    Retorna ``(total_filas, filas_que_ya_existían_en_bd)``; la segunda sirve para
    ``updated_documents`` en logs de sync (aprox. conflictos / refrescos).
    """
    if not rows:
        return 0, 0
    rows = _dedupe_logical_latest(rows)
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
    template = "(" + ",".join(["%s"] * len(cols)) + ",NOW(),NOW())"

    folio_rows = [
        r
        for r in rows
        if r.get("number") is not None and r.get("document_type_id") is not None
    ]
    pk_rows = [
        r
        for r in rows
        if r.get("number") is None or r.get("document_type_id") is None
    ]

    updated_existing = 0

    if folio_rows:
        folio_keys: list[tuple[int, int, int, int]] = []
        seen_k: set[tuple[int, int, int, int]] = set()
        for r in folio_rows:
            k = _folio_conflict_key(r)
            if k is None or k in seen_k:
                continue
            seen_k.add(k)
            folio_keys.append(k)

        before_folio: set[tuple[int, int, int, int]] = set()
        if folio_keys:
            cur.execute(
                """
                SELECT company_id, office_id, document_type_id, number
                FROM distribuidora.documents
                WHERE (company_id, office_id, document_type_id, number) IN %s
                """,
                (tuple(folio_keys),),
            )
            for row in cur.fetchall() or []:
                before_folio.add(
                    (int(row[0]), int(row[1]), int(row[2]), int(row[3])),
                )

        sql_folio_upsert = f"""
            INSERT INTO distribuidora.documents ({", ".join(cols)}, created_at, updated_at)
            VALUES %s
            ON CONFLICT (company_id, office_id, document_type_id, number)
            WHERE document_type_id IS NOT NULL AND number IS NOT NULL
            DO UPDATE SET
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
        try:
            _execute_values_batch(cur, sql_folio_upsert, folio_rows, cols, template)
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            raise
        _apply_persisted_document_ids_for_folio_rows(cur, rows)

        seen_folio_count: set[tuple[int, int, int, int]] = set()
        for r in folio_rows:
            k = _folio_conflict_key(r)
            if k is None or k not in before_folio or k in seen_folio_count:
                continue
            seen_folio_count.add(k)
            updated_existing += 1

    if pk_rows:
        pk_ids: list[int] = []
        seen_id: set[int] = set()
        for r in pk_rows:
            try:
                did = int(r["document_id"])
            except (TypeError, ValueError, KeyError):
                continue
            if did in seen_id:
                continue
            seen_id.add(did)
            pk_ids.append(did)

        before_pk: set[int] = set()
        if pk_ids:
            cur.execute(
                """
                SELECT document_id
                FROM distribuidora.documents
                WHERE document_id IN %s
                """,
                (tuple(pk_ids),),
            )
            for (did,) in cur.fetchall() or []:
                before_pk.add(int(did))

        sql_pk = f"""
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
        try:
            _execute_values_batch(cur, sql_pk, pk_rows, cols, template)
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            raise

        seen_pk_count: set[int] = set()
        for r in pk_rows:
            try:
                did = int(r["document_id"])
            except (TypeError, ValueError, KeyError):
                continue
            if did not in before_pk or did in seen_pk_count:
                continue
            seen_pk_count.add(did)
            updated_existing += 1

    if sync_stats is not None:
        sync_stats["updated_documents"] = int(
            sync_stats.get("updated_documents", 0),
        ) + int(updated_existing)
        logger.info(
            "distribuidora.documents upsert: batch_rows=%s updated_documents=%s",
            len(rows),
            updated_existing,
        )

    return len(rows), int(updated_existing)


def seller_tuple_from_bsale_item(s: dict[str, Any]) -> tuple[int | None, str | None]:
    """Un vendedor desde ítem ``items[]`` de sellers.json (Bsale)."""
    raw_id = s.get("id")
    try:
        seller_id = int(raw_id) if raw_id is not None else None
    except (TypeError, ValueError):
        seller_id = None
    fn = str(s.get("firstName") or s.get("firstname") or "").strip()
    ln = str(s.get("lastName") or s.get("lastname") or "").strip()
    name = f"{fn} {ln}".strip() or None
    return seller_id, name


def seller_tuples_from_sellers_api_response(data: Any) -> list[tuple[int | None, str | None]]:
    """
    Respuesta típica GET ``/v1/documents/{id}/sellers.json``:
    ``{ "items": [ { "id", "firstName", "lastName", ... }, ... ] }``.
    Devuelve todas las tuplas con nombre no vacío.
    """
    if not isinstance(data, dict):
        return []
    items = data.get("items")
    if not isinstance(items, list) or len(items) == 0:
        return []
    out: list[tuple[int | None, str | None]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        sid, sname = seller_tuple_from_bsale_item(it)
        if sname and str(sname).strip():
            out.append((sid, str(sname).strip()))
    return out


def parse_document_sellers_response(data: dict[str, Any]) -> tuple[int | None, str | None]:
    """
    Primer vendedor de la respuesta sellers.json (compatibilidad con código legado).
    """
    rows = seller_tuples_from_sellers_api_response(data)
    return rows[0] if rows else (None, None)


def replace_document_sellers(
    cur,
    document_id: int,
    rows: list[tuple[int | None, str | None]],
) -> int:
    """
    Reemplaza vendedores del documento: borra filas previas e inserta ``rows`` (0..n).

    Omite filas sin ``seller_name`` útil.
    """
    cur.execute(
        "DELETE FROM distribuidora.document_sellers WHERE document_id = %s",
        (document_id,),
    )
    if not rows:
        return 0
    n = 0
    for sid, sname in rows:
        if not sname or not str(sname).strip():
            continue
        cur.execute(
            """
            INSERT INTO distribuidora.document_sellers (document_id, seller_id, seller_name)
            VALUES (%s, %s, %s)
            """,
            (document_id, sid, str(sname).strip()),
        )
        n += 1
    return n


def set_document_primary_seller(
    cur,
    document_id: int,
    seller_id: int | None,
    seller_name: str | None,
) -> None:
    """Sincroniza ``documents.seller_*`` con el vendedor principal (primera fila de sync)."""
    if not seller_name or not str(seller_name).strip():
        return
    cur.execute(
        """
        UPDATE distribuidora.documents
        SET
            seller_id = %s,
            seller_name = %s,
            updated_at = NOW()
        WHERE document_id = %s
        """,
        (seller_id, str(seller_name).strip(), document_id),
    )


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
