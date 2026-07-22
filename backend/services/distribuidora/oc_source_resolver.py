"""Descubrimiento del source vigente de una OC Bsale por folio."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from backend.services.distribuidora.bsale_client import BsaleClient
from backend.services.distribuidora.bsale_params import merge_bsale_office_query

COMPANY_ID = 3
OFFICE_ID = 1
OC_DOCUMENT_TYPE_ID = 33
PAGE_LIMIT = 50
# Convención Bsale observada en reemisiones/anulaciones de OC.
BSALE_CANCELLED_STATE = 8888


def _int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _nested_id(document: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        raw = document.get(key)
        if isinstance(raw, dict):
            value = _int(raw.get("id"))
        else:
            value = _int(raw)
        if value is not None:
            return value
    return None


def summarize_bsale_document(
    document: dict[str, Any],
    *,
    expected_company_id: int = COMPANY_ID,
) -> dict[str, Any]:
    """Resumen seguro y uniforme de un documento devuelto por Bsale."""
    company_id = _nested_id(document, "company", "company_id", "companyId")
    company_inferred = company_id is None
    if company_id is None:
        # El token Bsale es tenant-scoped y muchos payloads omiten ``company``.
        company_id = expected_company_id
    return {
        "id": _int(document.get("id")),
        "number": _int(document.get("number")),
        "state": _int(document.get("state")),
        "commercialState": _int(document.get("commercialState")),
        "emissionDate": _int(document.get("emissionDate")),
        "generationDate": _int(document.get("generationDate")),
        "modificationDate": _int(document.get("modificationDate")),
        "totalAmount": document.get("totalAmount"),
        "company_id": company_id,
        "company_inferred_from_token_scope": company_inferred,
        "office_id": _nested_id(document, "office", "office_id", "officeId"),
        "document_type_id": _nested_id(
            document,
            "document_type",
            "documentType",
            "document_type_id",
            "documentTypeId",
        ),
    }


def is_active_oc_candidate(
    document: dict[str, Any],
    *,
    folio: int,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
) -> tuple[bool, list[str]]:
    """Valida el candidato y explica cada descarte."""
    row = summarize_bsale_document(document, expected_company_id=company_id)
    reasons: list[str] = []
    if row["id"] is None:
        reasons.append("missing_id")
    if row["number"] != int(folio) or row["number"] == 0:
        reasons.append("folio_mismatch_or_zero")
    if row["state"] != 0:
        reasons.append("state_not_active")
    commercial = row["commercialState"]
    if commercial not in (None, 0):
        reasons.append("commercial_state_not_active")
    if row["company_id"] != int(company_id):
        reasons.append("company_mismatch")
    if row["office_id"] != int(office_id):
        reasons.append("office_mismatch")
    if row["document_type_id"] != int(document_type_id):
        reasons.append("document_type_mismatch")
    return not reasons, reasons


def is_cancelled_bsale_document(document: dict[str, Any]) -> bool:
    """True si Bsale representa el documento como anulado/inactivo."""
    row = summarize_bsale_document(document)
    state = row.get("state")
    number = row.get("number")
    if state is not None and int(state) != 0:
        return True
    if number == 0:
        return True
    return False


def find_cancelled_source_evidence(
    discovery: dict[str, Any],
    *,
    known_source_ids: Iterable[int] = (),
) -> dict[str, Any] | None:
    """
    Sin versión activa del folio, elige evidencia de anulación entre sources conocidos.

    Requiere que el source conocido exista en el discovery (GET directo o búsqueda)
    y esté descartado por estado inactivo y/o number=0.
    """
    if discovery.get("active_document") is not None:
        return None
    known: set[int] = set()
    for raw_id in known_source_ids:
        source_id = _int(raw_id)
        if source_id is not None and source_id > 0:
            known.add(source_id)
    if not known:
        return None

    candidates: list[dict[str, Any]] = []
    for item in discovery.get("documents") or []:
        if not isinstance(item, dict) or item.get("eligible"):
            continue
        source_id = _int(item.get("id"))
        if source_id is None or source_id not in known:
            continue
        reasons = set(item.get("discard_reasons") or [])
        if not reasons.intersection({"state_not_active", "folio_mismatch_or_zero"}):
            continue
        state = _int(item.get("state"))
        number = _int(item.get("number"))
        if (state is not None and state != 0) or number == 0:
            candidates.append(item)
    if not candidates:
        return None

    def sort_key(row: dict[str, Any]) -> tuple[int, int]:
        return (_int(row.get("generationDate")) or -1, _int(row.get("id")) or -1)

    return max(candidates, key=sort_key)


def select_active_oc_source(
    documents: Iterable[dict[str, Any]],
    *,
    folio: int,
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Selecciona activo por generationDate más reciente; desempata por mayor id."""
    evaluated: list[dict[str, Any]] = []
    valid: list[dict[str, Any]] = []
    for raw in documents:
        if not isinstance(raw, dict):
            continue
        ok, reasons = is_active_oc_candidate(
            raw,
            folio=folio,
            company_id=company_id,
            office_id=office_id,
            document_type_id=document_type_id,
        )
        summary = summarize_bsale_document(raw, expected_company_id=company_id)
        evaluated.append({**summary, "eligible": ok, "discard_reasons": reasons})
        if ok:
            valid.append(raw)

    if not valid:
        return None, evaluated

    def key(document: dict[str, Any]) -> tuple[int, int]:
        generation = _int(document.get("generationDate")) or -1
        source_id = _int(document.get("id")) or -1
        return generation, source_id

    return max(valid, key=key), evaluated


def search_documents_by_folio(
    client: BsaleClient,
    *,
    folio: int,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
) -> list[dict[str, Any]]:
    """Usa el filtro real Bsale ``GET /documents.json?number=…`` paginado."""
    output: list[dict[str, Any]] = []
    offset = 0
    while True:
        params = merge_bsale_office_query(
            {
                "number": int(folio),
                "documenttypeid": int(document_type_id),
                "limit": PAGE_LIMIT,
                "offset": offset,
            },
            int(office_id),
            context="resolve_oc_source_by_folio",
        )
        payload = client.get("/documents.json", params)
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            break
        output.extend(item for item in items if isinstance(item, dict))
        if len(items) < PAGE_LIMIT:
            break
        offset += len(items)
    return output


def discover_oc_sources(
    client: BsaleClient,
    *,
    folio: int,
    known_source_ids: Iterable[int] = (),
    company_id: int = COMPANY_ID,
    office_id: int = OFFICE_ID,
    document_type_id: int = OC_DOCUMENT_TYPE_ID,
) -> dict[str, Any]:
    """
    Busca por folio y agrega GET directos de sources históricos conocidos.

    Los GET directos permiten exhibir documentos que ya devuelven ``number=0`` y,
    por ello, dejaron de aparecer en la búsqueda por folio.
    """
    by_id: dict[int, dict[str, Any]] = {}
    search_items = search_documents_by_folio(
        client,
        folio=folio,
        office_id=office_id,
        document_type_id=document_type_id,
    )
    anonymous: list[dict[str, Any]] = []
    for item in search_items:
        source_id = _int(item.get("id"))
        if source_id is None:
            anonymous.append(item)
        else:
            by_id[source_id] = item

    direct_status: dict[int, str] = {}
    normalized_known: list[int] = []
    for raw_id in known_source_ids:
        source_id = _int(raw_id)
        if source_id is not None and source_id > 0 and source_id not in normalized_known:
            normalized_known.append(source_id)
    for source_id in normalized_known:
        try:
            raw = client.get(f"/documents/{source_id}.json")
        except Exception as exc:
            direct_status[source_id] = f"error:{type(exc).__name__}"
            continue
        if isinstance(raw, dict):
            by_id[source_id] = raw
            direct_status[source_id] = "ok"

    all_documents = list(by_id.values()) + anonymous
    active, evaluated = select_active_oc_source(
        all_documents,
        folio=folio,
        company_id=company_id,
        office_id=office_id,
        document_type_id=document_type_id,
    )
    return {
        "folio": int(folio),
        "search_path": "/documents.json",
        "search_params": {
            "number": int(folio),
            "officeid": int(office_id),
            "documenttypeid": int(document_type_id),
        },
        "documents": evaluated,
        "active_document": active,
        "active_source_document_id": _int(active.get("id")) if active else None,
        "known_source_direct_status": direct_status,
    }


def fetch_all_document_details(
    client: BsaleClient,
    source_document_id: int,
) -> list[dict[str, Any]]:
    items_out: list[dict[str, Any]] = []
    offset = 0
    while True:
        payload = client.get(
            f"/documents/{int(source_document_id)}/details.json",
            {"limit": PAGE_LIMIT, "offset": offset},
        )
        items = payload.get("items") if isinstance(payload, dict) else None
        if not isinstance(items, list):
            raise ValueError(
                "Respuesta Bsale details inválida: falta una lista items; "
                "se preservan los detalles locales"
            )
        items_out.extend(item for item in items if isinstance(item, dict))
        if len(items) < PAGE_LIMIT:
            break
        offset += len(items)
    return items_out


def source_updated_at(document: dict[str, Any]) -> datetime | None:
    raw = _int(document.get("modificationDate"))
    if raw is None:
        raw = _int(document.get("generationDate"))
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    except (ValueError, OSError, OverflowError):
        return None


_HEADER_HASH_FIELDS = (
    "id",
    "number",
    "state",
    "commercialState",
    "emissionDate",
    "expirationDate",
    "generationDate",
    "modificationDate",
    "totalAmount",
    "netAmount",
    "taxAmount",
    "informedSii",
)
_DETAIL_HASH_FIELDS = (
    "id",
    "lineNumber",
    "quantity",
    "netUnitValue",
    "totalUnitValue",
    "netAmount",
    "taxAmount",
    "totalAmount",
    "netDiscount",
    "totalDiscount",
    "discountPercentage",
    "relatedDetailId",
)


def _canonical_scalar(value: Any) -> Any:
    if value is None or isinstance(value, bool):
        return value
    numeric = isinstance(value, (int, float, Decimal)) or (
        isinstance(value, str)
        and re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value.strip()) is not None
    )
    if numeric:
        try:
            decimal = Decimal(str(value))
        except InvalidOperation:
            return str(value)
        if not decimal.is_finite():
            return str(value)
        normalized = decimal.normalize()
        return format(normalized, "f")
    return value


def compute_oc_source_hash(
    document: dict[str, Any],
    details: list[dict[str, Any]],
) -> str:
    """Hash estable de campos operacionales del encabezado y líneas."""
    header = {
        key: _canonical_scalar(document.get(key)) for key in _HEADER_HASH_FIELDS
    }
    lines: list[dict[str, Any]] = []
    for item in details:
        row = {
            key: _canonical_scalar(item.get(key)) for key in _DETAIL_HASH_FIELDS
        }
        variant = item.get("variant") if isinstance(item.get("variant"), dict) else {}
        row["variant_id"] = _canonical_scalar(variant.get("id"))
        row["variant_code"] = variant.get("code")
        lines.append(row)
    lines.sort(
        key=lambda row: (
            row.get("lineNumber") is None,
            row.get("lineNumber") or 0,
            row.get("id") or 0,
        )
    )
    canonical = json.dumps(
        {"document": header, "details": lines},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
