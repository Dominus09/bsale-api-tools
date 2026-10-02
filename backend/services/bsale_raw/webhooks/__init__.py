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
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping

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
        """Clave de idempotencia: el mismo evento reenviado no se procesa dos veces."""
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
    def resource_url(self) -> str:
        return BSALE_API_ORIGIN + self.resource


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
class RefreshTask:
    resource: str
    company_id: int
    params: Mapping[str, int]
    url: str | None = None


def route(event: WebhookEvent) -> list[RefreshTask]:
    """Refrescos que dispara un evento. La primera tarea consulta exactamente ``event.resource_url``."""
    cid = event.company_id
    primary = {
        "product": "products",
        "variant": "variants",
        "price": "variant_prices",
        "stock": "stocks",
        "document": "documents",
    }[event.topic]
    params: dict[str, int] = {"id": event.resource_id}
    if event.office_id is not None:
        params["office_id"] = event.office_id
    if event.price_list_id is not None:
        params["price_list_id"] = event.price_list_id
    tasks = [RefreshTask(primary, cid, params, url=event.resource_url)]
    if event.topic == "variant":
        tasks.append(RefreshTask("variant_costs", cid, {"variant_id": event.resource_id}))
    if event.topic == "document":
        tasks.append(RefreshTask("stocks_for_document", cid, {"document_id": event.resource_id}))
    return tasks
