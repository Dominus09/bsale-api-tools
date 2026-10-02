"""Inbox de webhooks Bsale (diseño). Sólo parseo y ruteo puros; no hay endpoint HTTP ni acciones productivas.

Payload oficial (CL): ``cpnId``, ``resource``, ``resourceId``, ``topic``, ``action``, ``send`` y
extras por topic (``officeId`` en stock/document, ``priceListId`` en price). ``cpnId`` es el id de
instancia Bsale; se traduce a ``company_id`` con un mapa que vive en ``bsale_raw.sources``
(id de instancia obtenido de ``credential.bsale.io/v1/instances/basic/{token}.json``).
Nunca se asume entrega exactly-once: el procesamiento debe ser idempotente.

``resource`` se consulta tal como lo entrega Bsale (sin reescribir su versión): los ejemplos
oficiales usan ``/v2/...`` para product/variant/price/stock y ``/documents/{id}.json`` para
document. Sólo se aceptan rutas relativas que calcen con los patrones documentados del topic,
coherentes con ``resourceId``/``officeId``/``priceListId``, y siempre sobre ``https://api.bsale.io``.

Observado en fase 2: las respuestas ``/v2`` usan envelope ``code`` + ``data`` (no la forma V1) y
pueden devolver 503 transitorios. Por eso la respuesta exacta sólo se guarda como evidencia y la
entidad se refresca luego con su endpoint canónico V1, que es la única forma que alimenta las
tablas operativas RAW.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping

from backend.services.bsale_raw.core.models import ResponseEnvelope
from backend.services.bsale_raw.core.rate_limit import RequestPriority

KNOWN_TOPICS = frozenset({"product", "variant", "price", "stock", "document"})
KNOWN_ACTIONS = frozenset({"post", "put", "delete"})

BSALE_API_ORIGIN = "https://api.bsale.io"

RESOURCE_PATTERNS: dict[str, re.Pattern[str]] = {
    "product": re.compile(r"^/v2/products/(?P<id>\d+)\.json$"),
    "variant": re.compile(r"^/v2/variants/(?P<id>\d+)\.json$"),
    "price": re.compile(r"^/v2/price_lists/(?P<price_list>\d+)/details\.json\?variant=(?P<id>\d+)$"),
    "stock": re.compile(r"^/v2/stocks\.json\?variant=(?P<id>\d+)&office=(?P<office>\d+)$"),
    "document": re.compile(r"^/documents/(?P<id>\d+)\.json$"),
}


class WebhookValidationError(ValueError):
    pass


@dataclass(frozen=True)
class WebhookEvent:
    company_id: int
    cpn_id: int
    topic: str
    action: str
    resource: str
    resource_id: int
    office_id: int | None
    price_list_id: int | None
    sent_at: int | None
    raw: Mapping[str, Any]

    @property
    def dedupe_key(self) -> tuple[Any, ...]:
        """Reenvío exacto del mismo evento (Bsale no documenta event_id). No es único en BD."""
        return (
            self.company_id,
            self.topic,
            self.action,
            self.resource_id,
            self.office_id,
            self.price_list_id,
            self.sent_at,
        )

    @property
    def refresh_key(self) -> str:
        """Trabajo de refresh del recurso (sin action/send): base del coalescing en webhook_events."""
        return _key_text((self.company_id, self.topic, self.resource_id, self.office_id, self.price_list_id))

    @property
    def dedupe_key_text(self) -> str:
        return _key_text(self.dedupe_key)

    @property
    def resource_url(self) -> str:
        return BSALE_API_ORIGIN + self.resource


def _key_text(parts: tuple[Any, ...]) -> str:
    return "|".join("" if p is None else str(p) for p in parts)


def _opt_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def validate_resource(
    topic: str,
    resource: str,
    resource_id: int,
    office_id: int | None,
    price_list_id: int | None,
) -> str:
    """Devuelve ``resource`` si es una ruta relativa esperada para el topic; si no, error (anti-SSRF)."""
    pattern = RESOURCE_PATTERNS[topic]
    match = pattern.fullmatch(resource)
    if match is None:
        raise WebhookValidationError(f"resource no esperado para topic {topic}")
    groups = match.groupdict()
    if int(groups["id"]) != resource_id:
        raise WebhookValidationError("resource no coincide con resourceId")
    if "office" in groups and office_id is not None and int(groups["office"]) != office_id:
        raise WebhookValidationError("resource no coincide con officeId")
    if "price_list" in groups and price_list_id is not None and int(groups["price_list"]) != price_list_id:
        raise WebhookValidationError("resource no coincide con priceListId")
    return resource


def parse_webhook(payload: Mapping[str, Any], cpn_to_company: Mapping[int, int]) -> WebhookEvent:
    if not isinstance(payload, Mapping):
        raise WebhookValidationError("payload no es objeto JSON")
    for key in ("cpnId", "topic", "action", "resourceId", "resource"):
        if payload.get(key) in (None, ""):
            raise WebhookValidationError(f"falta {key}")
    topic = str(payload["topic"]).lower()
    action = str(payload["action"]).lower()
    if topic not in KNOWN_TOPICS:
        raise WebhookValidationError(f"topic desconocido: {topic}")
    if action not in KNOWN_ACTIONS:
        raise WebhookValidationError(f"action desconocida: {action}")
    try:
        cpn_id = int(payload["cpnId"])
        resource_id = int(payload["resourceId"])
        office_id = _opt_int(payload.get("officeId"))
        price_list_id = _opt_int(payload.get("priceListId"))
    except (TypeError, ValueError) as exc:
        raise WebhookValidationError("ids no numéricos") from exc
    company_id = cpn_to_company.get(cpn_id)
    if company_id is None:
        raise WebhookValidationError(f"cpnId {cpn_id} no corresponde a una empresa configurada")
    resource = validate_resource(topic, str(payload["resource"]), resource_id, office_id, price_list_id)
    return WebhookEvent(
        company_id=company_id,
        cpn_id=cpn_id,
        topic=topic,
        action=action,
        resource=resource,
        resource_id=resource_id,
        office_id=office_id,
        price_list_id=price_list_id,
        sent_at=_opt_int(payload.get("send")),
        raw=dict(payload),
    )


@dataclass(frozen=True)
class ExactResourceResponse:
    """Respuesta del GET exacto, guardada tal cual (evidencia); no alimenta tablas operativas."""

    http_status: int | None
    envelope: ResponseEnvelope
    code: Any
    body: Any


def classify_exact_response(http_status: int | None, body: Any) -> ExactResourceResponse:
    if http_status is None:
        return ExactResourceResponse(None, ResponseEnvelope.NETWORK_ERROR, None, body)
    if not isinstance(body, Mapping):
        envelope = ResponseEnvelope.NO_JSON if body is None else ResponseEnvelope.OTHER
        return ExactResourceResponse(http_status, envelope, None, body)
    if "code" in body and "data" in body:
        return ExactResourceResponse(http_status, ResponseEnvelope.V2_CODE_DATA, body.get("code"), body)
    return ExactResourceResponse(http_status, ResponseEnvelope.OTHER, None, body)


class TaskKind(str, Enum):
    RESOURCE_EXACT = "RESOURCE_EXACT"  # GET exacto de `resource`; se guarda la respuesta original
    CANONICAL_V1 = "CANONICAL_V1"  # refresh puntual V1: única fuente de las tablas operativas RAW
    DERIVED = "DERIVED"  # efectos derivados (costos de variante nueva, stock de variantes del documento)


@dataclass(frozen=True)
class RefreshTask:
    kind: TaskKind
    resource: str
    company_id: int
    priority: RequestPriority
    params: Mapping[str, int]
    url: str | None = None


_PRIMARY_RESOURCE = {
    "product": "products",
    "variant": "variants",
    "price": "variant_prices",
    "stock": "stocks",
    "document": "documents",
}


def _canonical_v1(event: WebhookEvent) -> tuple[str, dict[str, int]]:
    rid = event.resource_id
    if event.topic == "product":
        return f"/v1/products/{rid}.json", {"id": rid}
    if event.topic == "variant":
        return f"/v1/variants/{rid}.json", {"id": rid}
    if event.topic == "price":
        if event.price_list_id is None:
            raise WebhookValidationError("webhook price sin priceListId")
        return (
            f"/v1/price_lists/{event.price_list_id}/details.json?variantid={rid}",
            {"price_list_id": event.price_list_id, "variantid": rid},
        )
    if event.topic == "stock":
        if event.office_id is None:
            return f"/v1/stocks.json?variantid={rid}", {"variantid": rid}
        return (
            f"/v1/stocks.json?variantid={rid}&officeid={event.office_id}",
            {"variantid": rid, "officeid": event.office_id},
        )
    return f"/v1/documents/{rid}.json", {"id": rid}


def route(event: WebhookEvent) -> list[RefreshTask]:
    """Tareas de un evento: exacto (respuesta original) → canónico V1 → derivados. Todas P0."""
    cid = event.company_id
    p0 = RequestPriority.P0_TARGETED
    primary = _PRIMARY_RESOURCE[event.topic]
    canonical_path, canonical_params = _canonical_v1(event)
    tasks = [
        RefreshTask(TaskKind.RESOURCE_EXACT, primary, cid, p0, {"id": event.resource_id}, url=event.resource_url),
        RefreshTask(TaskKind.CANONICAL_V1, primary, cid, p0, canonical_params, url=BSALE_API_ORIGIN + canonical_path),
    ]
    if event.topic == "variant" and event.action == "post":
        tasks.append(RefreshTask(TaskKind.DERIVED, "variant_costs", cid, p0, {"variant_id": event.resource_id}))
    if event.topic == "document":
        tasks.append(RefreshTask(TaskKind.DERIVED, "document_details", cid, p0, {"document_id": event.resource_id}))
        tasks.append(RefreshTask(TaskKind.DERIVED, "stocks_for_document", cid, p0, {"document_id": event.resource_id}))
    return tasks
