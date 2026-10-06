"""Upsert de ``distribuidora.documents``."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from psycopg2.extras import Json, execute_values

from backend.utils.bsale_document_ids import (
    coerce_folio_number,
    positive_folio_number,
)

logger = logging.getLogger(__name__)

_DOCS_BSALE_MODIFIED_COL: bool | None = None
_DOCS_SOURCE_COLS: bool | None = None

_SOURCE_SYNC_COLUMNS = (
    "source_document_id",
    "source_hash",
    "source_updated_at",
    "last_synced_at",
)

# Revisión vigente del header: (bsale_modified_at, id Bsale del raw_data). Debe coincidir
# con ``backend.utils.bsale_document_ids.is_revision_not_older``.
_CURRENT_REVISION_ID_SQL = (
    "COALESCE(NULLIF(distribuidora.documents.raw_data->>'id', '')::bigint, "
    "distribuidora.documents.source_document_id, distribuidora.documents.document_id)"
)
_CURRENT_REVISION_ID_SQL_NO_SOURCE = (
    "COALESCE(NULLIF(distribuidora.documents.raw_data->>'id', '')::bigint, "
    "distribuidora.documents.document_id)"
)

_DOCUMENT_UPSERT_COLS_BASE = [
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

_DOCUMENT_UPSERT_UPDATE_SET_BASE = [
    "number = EXCLUDED.number",
    "document_type_id = EXCLUDED.document_type_id",
    "client_id = EXCLUDED.client_id",
    "office_id = EXCLUDED.office_id",
    "company_id = EXCLUDED.company_id",
    "user_id = EXCLUDED.user_id",
    "emission_date = EXCLUDED.emission_date",
    "expiration_date = EXCLUDED.expiration_date",
    "generation_date = EXCLUDED.generation_date",
    "total_amount = EXCLUDED.total_amount",
    "net_amount = EXCLUDED.net_amount",
    "tax_amount = EXCLUDED.tax_amount",
    "state = EXCLUDED.state",
    "commercial_state = EXCLUDED.commercial_state",
    "informed_sii = EXCLUDED.informed_sii",
    "municipality = EXCLUDED.municipality",
    "city = EXCLUDED.city",
    "address = EXCLUDED.address",
    "token = EXCLUDED.token",
    "url_pdf = EXCLUDED.url_pdf",
    "url_public_view = EXCLUDED.url_public_view",
    "price_list_id = EXCLUDED.price_list_id",
    "tracking_number = EXCLUDED.tracking_number",
    "raw_data = EXCLUDED.raw_data",
    "updated_at = NOW()",
]


def _documents_has_bsale_modified_at(cur) -> bool:
    global _DOCS_BSALE_MODIFIED_COL
    if _DOCS_BSALE_MODIFIED_COL is not None:
        return _DOCS_BSALE_MODIFIED_COL
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'distribuidora'
              AND table_name = 'documents'
              AND column_name = 'bsale_modified_at'
        )
        """
    )
    _DOCS_BSALE_MODIFIED_COL = bool(cur.fetchone()[0])
    return _DOCS_BSALE_MODIFIED_COL


def _documents_has_source_sync_cols(cur) -> bool:
    """Columnas de 044 (``source_document_id``, ``source_hash``, …) presentes."""
    global _DOCS_SOURCE_COLS
    if _DOCS_SOURCE_COLS is not None:
        return _DOCS_SOURCE_COLS
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'distribuidora'
          AND table_name = 'documents'
          AND column_name = ANY(%s)
        """,
        (list(_SOURCE_SYNC_COLUMNS),),
    )
    _DOCS_SOURCE_COLS = int(cur.fetchone()[0]) == len(_SOURCE_SYNC_COLUMNS)
    return _DOCS_SOURCE_COLS


def _document_upsert_cols(cur) -> list[str]:
    cols = list(_DOCUMENT_UPSERT_COLS_BASE)
    if _documents_has_bsale_modified_at(cur):
        idx = cols.index("generation_date") + 1
        cols.insert(idx, "bsale_modified_at")
    if _documents_has_source_sync_cols(cur):
        cols.extend(["source_document_id", "source_updated_at", "last_synced_at"])
    return cols


def _document_upsert_update_set(cur) -> str:
    parts = list(_DOCUMENT_UPSERT_UPDATE_SET_BASE)
    if _documents_has_bsale_modified_at(cur):
        idx = parts.index("generation_date = EXCLUDED.generation_date") + 1
        parts.insert(idx, "bsale_modified_at = EXCLUDED.bsale_modified_at")
    if _documents_has_source_sync_cols(cur):
        parts.extend(
            [
                "source_document_id = EXCLUDED.source_document_id",
                "source_updated_at = EXCLUDED.source_updated_at",
                # El hash describe header+details de una revisión concreta; otra revisión lo invalida.
                "source_hash = CASE WHEN distribuidora.documents.source_document_id "
                "IS NOT DISTINCT FROM EXCLUDED.source_document_id "
                "THEN distribuidora.documents.source_hash ELSE NULL END",
                "last_synced_at = EXCLUDED.last_synced_at",
            ]
        )
    return ",\n                ".join(parts)


def _document_upsert_freshness_where(cur) -> str:
    """
    ``WHERE`` del ``DO UPDATE``: una revisión Bsale más antigua nunca pisa a la vigente.

    Sin ``bsale_modified_at`` (columna o valor) se compara solo por id de revisión.
    """
    if not _documents_has_bsale_modified_at(cur):
        return ""
    current_id = (
        _CURRENT_REVISION_ID_SQL
        if _documents_has_source_sync_cols(cur)
        else _CURRENT_REVISION_ID_SQL_NO_SOURCE
    )
    return f"""
            WHERE (
                CASE
                    WHEN EXCLUDED.bsale_modified_at IS NOT NULL
                     AND distribuidora.documents.bsale_modified_at IS NOT NULL
                    THEN (EXCLUDED.bsale_modified_at, EXCLUDED.document_id)
                         >= (distribuidora.documents.bsale_modified_at, {current_id})
                    ELSE EXCLUDED.document_id >= {current_id}
                END
            )"""


def _revision_sort_key(r: dict[str, Any]) -> tuple[float, int]:
    """Mayor = revisión Bsale más reciente: ``bsale_modified_at`` (o generación), empate id."""
    ts_raw = r.get("bsale_modified_at") or r.get("generation_date")
    did = int(r["document_id"])
    if ts_raw is None:
        return (-1.0, did)
    try:
        ts = float(ts_raw.timestamp())
    except Exception:
        ts = -1.0
    return (ts, did)


def _dedupe_logical_latest(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Misma clave (company_id, office_id, document_type_id, number): deja una fila
    (la revisión Bsale más reciente; empate por mayor ``document_id``).
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
        if prev is None or _revision_sort_key(r) > _revision_sort_key(prev):
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

    Solo enteros > 0: ``number <= 0`` (p. ej. revisión técnica reemplazada) no es folio
    comercial. Folio no numérico → None y el upsert usa solo ``document_id``.
    """
    return positive_folio_number(d.get("number"))


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

    folio_raw = coerce_folio_number(d.get("number"))
    if folio_raw is not None and folio_raw <= 0:
        # Revisión sin folio comercial: insertarla por PK crearía "OC 0" o pisaría el
        # documento comercial que comparte su id técnico.
        if sync_stats is not None:
            sync_stats["skipped_non_positive_folio"] = (
                int(sync_stats.get("skipped_non_positive_folio") or 0) + 1
            )
        logger.info(
            "Documento omitido por folio no comercial: id=%s number=%s state=%s",
            doc_id,
            folio_raw,
            d.get("state"),
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

    gen_ts = _ts(d.get("generationDate"))
    mod_ts = _ts(d.get("modificationDate"))

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
        "generation_date": gen_ts,
        "bsale_modified_at": mod_ts or gen_ts,
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
) -> set[int] | None:
    """Ejecuta el upsert; retorna los ids Bsale efectivamente escritos (``RETURNING``)."""
    if not batch:
        return set()
    vals = [tuple(r[c] for c in cols) for r in batch]
    returned = execute_values(
        cur, sql, vals, template=template, page_size=len(vals), fetch=True
    )
    if returned is None:
        return None
    applied: set[int] = set()
    for row in returned:
        try:
            applied.add(int(row[0]))
        except (TypeError, ValueError, IndexError):
            continue
    return applied


def _raw_revision_id(r: dict[str, Any]) -> int | None:
    """Id Bsale que ``RETURNING raw_data->>'id'`` devolverá para esta fila (si se escribe)."""
    raw = r.get("raw_data")
    payload = getattr(raw, "adapted", raw)
    if not isinstance(payload, dict):
        return None
    try:
        rid = int(payload.get("id"))
    except (TypeError, ValueError):
        return None
    return rid if rid == int(r["document_id"]) else None


def _mark_stale_revisions(
    batch: list[dict[str, Any]],
    applied: set[int] | None,
    sync_stats: dict[str, Any] | None,
) -> None:
    """Marca ``stale_revision_skipped`` en filas cuyo ``DO UPDATE`` fue bloqueado por frescura."""
    for r in batch:
        rev = r.get("_bsale_revision_id")
        skipped = applied is not None and rev is not None and int(rev) not in applied
        r["stale_revision_skipped"] = skipped
        if not skipped:
            continue
        if sync_stats is not None:
            sync_stats["stale_revisions_skipped"] = (
                int(sync_stats.get("stale_revisions_skipped") or 0) + 1
            )
        logger.warning(
            "documents_upsert stale_revision_skipped bsale_document_id=%s folio=%s "
            "document_type_id=%s bsale_modified_at=%s",
            rev,
            r.get("number"),
            r.get("document_type_id"),
            r.get("bsale_modified_at"),
        )


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

    * ``number <= 0`` nunca se persiste (no es folio comercial).
    * Una revisión Bsale más antigua que la vigente no actualiza la fila
      (``row["stale_revision_skipped"] = True``); sus hijos no deben persistirse.
    * Con columnas de 044, ``source_document_id`` / ``source_updated_at`` /
      ``last_synced_at`` se escriben junto al header y ``source_hash`` se invalida
      si cambia la revisión.

    Retorna ``(total_filas, filas_que_ya_existían_en_bd)``; la segunda sirve para
    ``updated_documents`` en logs de sync (aprox. conflictos / refrescos).
    """
    if not rows:
        return 0, 0
    kept: list[dict[str, Any]] = []
    for r in rows:
        n = coerce_folio_number(r.get("number"))
        if n is not None and n <= 0:
            if sync_stats is not None:
                sync_stats["skipped_non_positive_folio"] = (
                    int(sync_stats.get("skipped_non_positive_folio") or 0) + 1
                )
            logger.warning(
                "documents_upsert skipped_non_positive_folio document_id=%s number=%s",
                r.get("document_id"),
                r.get("number"),
            )
            continue
        kept.append(r)
    if not kept:
        return 0, 0
    rows = _dedupe_logical_latest(kept)
    kept_ids = {id(r) for r in rows}
    for r in kept:
        if id(r) not in kept_ids:
            # Misma clave lógica en el lote con una revisión más nueva.
            r["stale_revision_skipped"] = True
    cols = _document_upsert_cols(cur)
    update_set = _document_upsert_update_set(cur)
    freshness_where = _document_upsert_freshness_where(cur)
    template = "(" + ",".join(["%s"] * len(cols)) + ",NOW(),NOW())"
    has_source_cols = "source_document_id" in cols
    synced_at = datetime.now(timezone.utc)
    for r in rows:
        r["_bsale_revision_id"] = _raw_revision_id(r)
        if has_source_cols:
            r["source_document_id"] = int(r["document_id"])
            r["source_updated_at"] = r.get("bsale_modified_at") or r.get("generation_date")
            r["last_synced_at"] = synced_at

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
            WHERE document_type_id IS NOT NULL AND number > 0
            DO UPDATE SET
                {update_set}{freshness_where}
            RETURNING raw_data->>'id'
        """
        try:
            applied = _execute_values_batch(cur, sql_folio_upsert, folio_rows, cols, template)
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            raise
        _mark_stale_revisions(folio_rows, applied, sync_stats)
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
                {update_set}{freshness_where}
            RETURNING raw_data->>'id'
        """
        try:
            applied_pk = _execute_values_batch(cur, sql_pk, pk_rows, cols, template)
        except Exception:
            try:
                cur.connection.rollback()
            except Exception:
                pass
            raise
        _mark_stale_revisions(pk_rows, applied_pk, sync_stats)

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
