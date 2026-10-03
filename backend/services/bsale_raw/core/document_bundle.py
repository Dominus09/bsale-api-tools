"""Bundle de UNA versión observada de un documento Bsale, en memoria (sin BD ni red).

Bundle = header + details (paginados completos) + references + sellers + attributes. Todas las
partes se validan antes de abrir cualquier transacción; un bundle inválido nunca se escribe.

Identidad de hijos SIEMPRE acotada al padre: details / references ``(company_id, document_id,
bsale_id)``, sellers ``(company_id, document_id, user_id)``. El id de un hijo nunca se trata como
identidad global.

Hashes (``core/document_version.DocumentVersion``):
- ``payload_hash``  = hash canónico del header RAW;
- ``children_hash`` = hash de los hashes de details / references / sellers / attributes;
- ``version_hash``  = hash de los hashes de las 5 partes (header + children).
Las colecciones se ordenan por id antes de hashear: el orden de la API no cambia la versión, y el
contenido completo de cada ítem sí entra al hash (cambiar una cantidad cambia children/version).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urlsplit

from backend.services.bsale_raw.core.document_version import DOCUMENT_REFRESH_PARTS, DocumentVersion
from backend.services.bsale_raw.core.models import DocumentChangeKind, payload_hash
from backend.services.bsale_raw.core.registry import ResourceSpec, optional_relation_id
from backend.services.bsale_raw.core.snapshot import Snapshot, SnapshotValidationError, _bsale_id

BSALE_API_HOST = "api.bsale.io"
CHILD_KINDS: tuple[str, ...] = ("details", "references", "sellers")
CHILD_SPEC_NAMES = {"details": "document_details", "references": "document_references", "sellers": "document_sellers"}
CHILD_KEY_COLUMN = {"details": "bsale_id", "references": "bsale_id", "sellers": "user_id"}
# Ítems con id propio de relación bajo el documento (sellers son usuarios: href a /users/{id}).
_CHILD_ITEM_PATH = {"details": "details", "references": "references"}


class DocumentTypeNotAllowedError(SnapshotValidationError):
    """El documento existe pero su document_type_id no está habilitado para este refresh."""


@dataclass(frozen=True)
class ChildRow:
    kind: str
    key: int
    typed: dict[str, Any]
    payload: dict[str, Any] = field(repr=False)
    payload_hash: str

    @property
    def variant_id(self) -> int | None:
        return self.typed.get("variant_id")


@dataclass(frozen=True)
class DocumentBundle:
    company_id: int
    document_id: int
    header: dict[str, Any] = field(repr=False)
    typed: dict[str, Any] = field(repr=False)
    details: list[ChildRow] = field(repr=False)
    references: list[ChildRow] = field(repr=False)
    sellers: list[ChildRow] = field(repr=False)
    # Instante de la respuesta del header: frescura del bundle (el más viejo de sus componentes).
    api_fetched_at: datetime
    children_fetched_at: datetime
    version: DocumentVersion = field(repr=False)

    @property
    def attributes(self) -> Any:
        return self.version.attributes

    @property
    def payload_hash(self) -> str:
        return self.version.part_hashes()["header"]

    @property
    def children_hash(self) -> str:
        return self.version.children_hash

    @property
    def version_hash(self) -> str:
        return self.version.version_hash

    @property
    def document_type_id(self) -> int | None:
        return self.typed.get("document_type_id")

    @property
    def office_id(self) -> int | None:
        return self.typed.get("office_id")

    def children(self, kind: str) -> list[ChildRow]:
        return {"details": self.details, "references": self.references, "sellers": self.sellers}[kind]

    def child_hashes(self, kind: str) -> dict[int, str]:
        return {row.key: row.payload_hash for row in self.children(kind)}

    def current_variant_ids(self) -> list[int]:
        return sorted({row.variant_id for row in self.details if row.variant_id is not None})


def child_path(spec: ResourceSpec, document_id: int) -> str:
    """Ruta documentada en la matriz (``/v1/documents/{id}/<hijo>.json``), relativa a la base ``/v1``."""
    path = spec.list_endpoint.format(parent_id=int(document_id)).lstrip("/")
    return path[len("v1/"):] if path.startswith("v1/") else path


def _check_href(href: Any, expected_path: str, what: str) -> None:
    if not isinstance(href, str):
        raise SnapshotValidationError(f"{what}: href no es texto")
    parts = urlsplit(href)
    if parts.scheme != "https" or parts.netloc != BSALE_API_HOST:
        raise SnapshotValidationError(f"{what}: href fuera de https://{BSALE_API_HOST} (rechazado)")
    if parts.path != expected_path or parts.query or parts.fragment:
        raise SnapshotValidationError(f"{what}: href inesperado {parts.path!r} (esperado {expected_path!r})")


def check_child_link(header: dict[str, Any], name: str, document_id: int) -> None:
    """El link que entrega Bsale debe coincidir EXACTO con la ruta documentada; nunca se sigue otro host."""
    node = header.get(name)
    if node is None:
        return
    if not isinstance(node, dict):
        raise SnapshotValidationError(f"documento {document_id}: relación {name} no es objeto")
    if "href" in node:
        _check_href(node["href"], f"/v1/documents/{int(document_id)}/{name}.json", f"documento {document_id} {name}")


def build_header(
    spec: ResourceSpec, document_id: int, header: Any, allowed_type_ids: frozenset[int]
) -> dict[str, Any]:
    if not isinstance(header, dict):
        raise SnapshotValidationError(f"documento {document_id}: respuesta no es objeto")
    if _bsale_id(header) != document_id:
        raise SnapshotValidationError(f"documento {document_id}: la respuesta trae otro id")
    try:
        typed = {col.column: col.extract(header) for col in spec.typed_columns}
    except ValueError as exc:
        raise SnapshotValidationError(f"documento {document_id}: {exc}") from exc
    type_id = typed.get("document_type_id")
    if type_id is None:
        raise SnapshotValidationError(f"documento {document_id}: sin document_type.id")
    if type_id not in allowed_type_ids:
        raise DocumentTypeNotAllowedError(
            f"documento {document_id}: document_type_id={type_id} no habilitado (permitidos {sorted(allowed_type_ids)})"
        )
    office_id = typed.get("office_id")
    if office_id is not None and office_id <= 0:
        raise SnapshotValidationError(f"documento {document_id}: office.id inválido")
    return typed


def build_child_rows(kind: str, spec: ResourceSpec, document_id: int, snapshot: Snapshot) -> list[ChildRow]:
    """Una fila por ítem; cualquier ítem inválido, de otro documento o con clave repetida invalida el bundle."""
    rows: list[ChildRow] = []
    seen: set[int] = set()
    duplicates: set[int] = set()
    item_path = _CHILD_ITEM_PATH.get(kind)
    for item in snapshot.items:
        payload = item.payload
        try:
            key = _bsale_id(payload)
        except SnapshotValidationError as exc:
            raise SnapshotValidationError(f"documento {document_id} {kind}: {exc}") from exc
        if key in seen:
            duplicates.add(key)
            continue
        seen.add(key)
        if item_path is not None and "href" in payload:
            _check_href(
                payload["href"], f"/v1/documents/{int(document_id)}/{item_path}/{key}.json",
                f"documento {document_id} {kind} id={key}",
            )
        if "document" in payload:
            try:
                parent = optional_relation_id(payload["document"])
            except ValueError as exc:
                raise SnapshotValidationError(f"documento {document_id} {kind} id={key}: {exc}") from exc
            if parent is not None and parent != document_id:
                raise SnapshotValidationError(f"documento {document_id} {kind} id={key}: pertenece al documento {parent}")
        try:
            typed = {col.column: col.extract(payload) for col in spec.typed_columns}
        except ValueError as exc:
            raise SnapshotValidationError(f"documento {document_id} {kind} id={key}: {exc}") from exc
        variant = typed.get("variant_id")
        if variant is not None and variant <= 0:
            raise SnapshotValidationError(f"documento {document_id} {kind} id={key}: variant.id inválido")
        rows.append(ChildRow(kind=kind, key=key, typed=typed, payload=payload, payload_hash=payload_hash(payload)))
    if duplicates:
        raise SnapshotValidationError(
            f"documento {document_id} {kind}: ids repetidos {sorted(duplicates)[:5]} (posible paginación inconsistente)"
        )
    return rows


def make_bundle(
    *,
    company_id: int,
    document_id: int,
    header: dict[str, Any],
    typed: dict[str, Any],
    children: dict[str, list[ChildRow]],
    api_fetched_at: datetime,
    children_fetched_at: datetime,
) -> DocumentBundle:
    version = DocumentVersion(
        header=header,
        details=[r.payload for r in children["details"]],
        references=[r.payload for r in children["references"]],
        sellers=[r.payload for r in children["sellers"]],
        attributes=header.get("attributes"),
    )
    return DocumentBundle(
        company_id=company_id,
        document_id=document_id,
        header=header,
        typed=typed,
        details=children["details"],
        references=children["references"],
        sellers=children["sellers"],
        api_fetched_at=api_fetched_at,
        children_fetched_at=children_fetched_at,
        version=version,
    )


@dataclass(frozen=True)
class StoredDocument:
    """Versión vigente en RAW (lo necesario para frescura, cambios, variantes y pendientes)."""

    header_exists: bool
    api_fetched_at: datetime | None = None
    payload_hash: str | None = None
    version_hash: str | None = None
    state: int | None = None
    commercial_state: str | None = None
    office_id: int | None = None
    attributes_payload: Any = field(default=None, repr=False)
    details: dict[int, tuple[str, int | None]] = field(default_factory=dict)
    references: dict[int, str] = field(default_factory=dict)
    sellers: dict[int, str] = field(default_factory=dict)
    # Filas de document_change_log con stock pedido y no confirmado: (id, affected_variant_ids).
    pending: list[tuple[int, list[int]]] = field(default_factory=list)

    def child_hashes(self, kind: str) -> dict[int, str]:
        if kind == "details":
            return {k: h for k, (h, _) in self.details.items()}
        return dict(self.references if kind == "references" else self.sellers)

    def variant_ids(self) -> list[int]:
        return sorted({v for _, v in self.details.values() if v is not None})


EMPTY_STORED = StoredDocument(header_exists=False)


@dataclass
class DocumentPlan:
    stale: bool
    change_kind: str | None
    version_changed: bool
    changed: dict[str, bool]
    previous_variant_ids: list[int]
    current_variant_ids: list[int]
    affected_variant_ids: list[int]
    pending_change_ids: list[int]
    stock_variant_ids: list[int]
    stock_office_id: int | None


def plan_document(stored: StoredDocument, bundle: DocumentBundle) -> DocumentPlan:
    """
    Decide qué hacer con un bundle frente a la versión vigente (puro; mismo cálculo en dry-run y escritura).

    - stale: la fila guardada tiene ``api_fetched_at`` posterior → no se toca nada ni se pide stock.
    - CREATED: no había header. MODIFIED: cambió ``version_hash``. Sin cambio: sin fila de log.
    - affected = previous ∪ current (una línea quitada libera reserva y también se refresca).
    - stock = affected (si hubo cambio) ∪ variantes de filas de log pendientes (reintento sin
      fabricar un MODIFIED nuevo).
    - sucursal del stock = la del documento; si la OC cambió de sucursal o no trae sucursal, todas.
    """
    previous = stored.variant_ids()
    current = bundle.current_variant_ids()
    if stored.header_exists and stored.api_fetched_at is not None and stored.api_fetched_at > bundle.api_fetched_at:
        return DocumentPlan(
            stale=True, change_kind=None, version_changed=False, changed=dict.fromkeys(DOCUMENT_REFRESH_PARTS, False),
            previous_variant_ids=previous, current_variant_ids=current, affected_variant_ids=[],
            pending_change_ids=[], stock_variant_ids=[], stock_office_id=bundle.office_id,
        )

    if not stored.header_exists:
        kind: str | None = DocumentChangeKind.CREATED.value
        changed = dict.fromkeys(DOCUMENT_REFRESH_PARTS, True)
    else:
        changed = {
            "header": stored.payload_hash != bundle.payload_hash,
            "details": stored.child_hashes("details") != bundle.child_hashes("details"),
            "references": stored.child_hashes("references") != bundle.child_hashes("references"),
            "sellers": stored.child_hashes("sellers") != bundle.child_hashes("sellers"),
            "attributes": payload_hash(stored.attributes_payload) != payload_hash(bundle.attributes),
        }
        kind = DocumentChangeKind.MODIFIED.value if stored.version_hash != bundle.version_hash else None

    affected = sorted(set(previous) | set(current)) if kind else []
    pending_ids = [change_id for change_id, _ in stored.pending]
    pending_variants = {int(v) for _, variants in stored.pending for v in variants}
    office = bundle.office_id
    if stored.header_exists and stored.office_id is not None and office is not None and stored.office_id != office:
        office = None
    return DocumentPlan(
        stale=False,
        change_kind=kind,
        version_changed=kind is not None,
        changed=changed,
        previous_variant_ids=previous,
        current_variant_ids=current,
        affected_variant_ids=affected,
        pending_change_ids=pending_ids,
        stock_variant_ids=sorted(set(affected) | pending_variants),
        stock_office_id=office,
    )
