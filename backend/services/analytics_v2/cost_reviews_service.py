"""Casos de revisión humana de costos (no re-calcula alertas Costos V2).

API append-only sobre historial: solo INSERT de eventos vía operaciones autorizadas.
No hay DELETE de revisiones ni UPDATE/DELETE de eventos.
Inmutabilidad absoluta del historial no está garantizada si el rol PostgreSQL es privilegiado.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, TypeVar

from fastapi import HTTPException
from psycopg2.extras import Json

from backend.auth.hierarchy import is_owner
from backend.auth.permissions import has_management_access
from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    open_readonly_connection,
)
from backend.services.analytics_v2.access import resolve_company_scope
from backend.services.analytics_v2.cost_control_repo import CostControlV2Repository
from backend.services.analytics_v2.cost_control_service import (
    CostControlNotFound,
    CostControlV2Service,
)
from backend.services.analytics_v2.cost_reviews_repo import SORT_SQL, CostReviewsRepo
from backend.services.analytics_v2.cost_signals import to_decimal

T = TypeVar("T")

logger = logging.getLogger(__name__)

STATUSES = frozenset({"pendiente", "en_revision", "resuelto", "descartado"})
PRIORITIES = frozenset({"alta", "media", "baja"})
SOURCES = frozenset(
    {"dashboard_alert", "reception_analysis", "reviews_grid", "product_sheet"}
)
SnapshotFn = Callable[[int], dict[str, Any]]


def can_mutate_cost_reviews(user: dict[str, Any] | None) -> bool:
    return bool(is_owner(user) or has_management_access(user))


def _actor_id(user: dict[str, Any]) -> int | None:
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    try:
        return int(raw) if raw is not None else None
    except (TypeError, ValueError):
        return None


def _actor_email(user: dict[str, Any]) -> str:
    return str(user.get("email") or "").strip().lower()


def _dec(v: Any) -> Decimal | None:
    return to_decimal(v)


def snapshot_from_cost_product(item: dict[str, Any]) -> dict[str, Any]:
    """Fotografía desde respuesta pública de Costos V2 (no valores del cliente)."""
    product = (item.get("product_name") or "").strip()
    variant = (item.get("variant_name") or "").strip()
    if product and variant:
        name = f"{product} — {variant}"
    else:
        name = product or variant or None
    last = item.get("last_comparable") or {}
    prev = item.get("previous_comparable") or {}
    change = item.get("change") or {}
    return {
        "product_name": name,
        "sku_code": item.get("code"),
        "barcode": item.get("barcode"),
        "current_cost_ref": _dec(last.get("cost")),
        "previous_cost_ref": _dec(prev.get("cost")),
        "change_pct_ref": _dec(change.get("pct")),
    }


def _public_row(row: dict[str, Any], *, company_name_fallback: str | None = None) -> dict[str, Any]:
    def ts(v: Any) -> str | None:
        if v is None:
            return None
        return v.isoformat() if hasattr(v, "isoformat") else str(v)

    def num(v: Any) -> float | None:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    company_name = row.get("company_name") or company_name_fallback or ""
    return {
        "id": int(row["id"]),
        "company_id": int(row["company_id"]),
        "company_name": company_name,
        "variant_id": int(row["variant_id"]),
        "anomaly_code": row["anomaly_code"],
        "reason_label": row["reason_label"],
        "priority": row["priority"],
        "status": row["status"],
        "product_name": row.get("product_name"),
        "sku_code": row.get("sku_code"),
        "barcode": row.get("barcode"),
        "current_cost_ref": num(row.get("current_cost_ref")),
        "previous_cost_ref": num(row.get("previous_cost_ref")),
        "change_pct_ref": num(row.get("change_pct_ref")),
        "assignee_user_id": int(row["assignee_user_id"]) if row.get("assignee_user_id") is not None else None,
        "assignee_email": row.get("assignee_email"),
        "started_at": ts(row.get("started_at")),
        "resolved_at": ts(row.get("resolved_at")),
        "observations": row.get("observations"),
        "conclusion": row.get("conclusion"),
        "source": row.get("source"),
        "source_ref": row.get("source_ref") or {},
        "created_by_user_id": int(row["created_by_user_id"]) if row.get("created_by_user_id") is not None else None,
        "created_at": ts(row.get("created_at")),
        "updated_at": ts(row.get("updated_at")),
        "is_human_review": True,
        "is_auto_detection": False,
        "costs_are_snapshot": True,
        "bsale_costs_editable": False,
    }


def _public_event(row: dict[str, Any]) -> dict[str, Any]:
    def ts(v: Any) -> str | None:
        if v is None:
            return None
        return v.isoformat() if hasattr(v, "isoformat") else str(v)

    return {
        "id": int(row["id"]),
        "review_id": int(row["review_id"]),
        "actor_user_id": int(row["actor_user_id"]) if row.get("actor_user_id") is not None else None,
        "actor_email": row.get("actor_email"),
        "action": row["action"],
        "status_from": row.get("status_from"),
        "status_to": row.get("status_to"),
        "note": row.get("note"),
        "detail": row.get("detail") or {},
        "created_at": ts(row.get("created_at")),
    }


class CostReviewsService:
    def __init__(
        self,
        *,
        company_id: int,
        company_name: str,
        actor: dict[str, Any],
        snapshot_fn: SnapshotFn | None = None,
        repo: CostReviewsRepo | None = None,
    ):
        self.company_id = int(company_id)
        self.company_name = (company_name or "").strip() or f"Empresa {company_id}"
        self.actor = actor
        self._snapshot_fn = snapshot_fn
        self._repo = repo or CostReviewsRepo()

    def _ensure_ready(self) -> None:
        if not self._repo.table_ready():
            raise HTTPException(
                status_code=503,
                detail="Revisiones de costos no disponibles: aplique la migración 067 (app.cost_reviews)",
            )

    def _require_mutate(self) -> None:
        if not can_mutate_cost_reviews(self.actor):
            raise HTTPException(
                status_code=403,
                detail="Acceso restringido: requiere gerencia, administración u OWNER",
            )

    def _row(self, row: dict[str, Any]) -> dict[str, Any]:
        return _public_row(row, company_name_fallback=self.company_name)

    def _load_snapshot(self, variant_id: int) -> dict[str, Any]:
        if self._snapshot_fn is None:
            raise HTTPException(status_code=503, detail="Snapshot Costos V2 no disponible")
        try:
            return self._snapshot_fn(int(variant_id))
        except HTTPException:
            raise
        except CostControlNotFound as exc:
            raise HTTPException(status_code=404, detail="Variante inexistente en la empresa") from exc
        except Exception as exc:
            logger.exception("cost review snapshot failed variant_id=%s", variant_id)
            raise HTTPException(
                status_code=503, detail="No se pudo obtener la instantánea de Costos V2"
            ) from exc

    def list(
        self,
        *,
        status: str | None = None,
        q: str | None = None,
        assignee_user_id: int | None = None,
        priority: str | None = None,
        sort: str = "status",
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        self._ensure_ready()
        if status and status not in STATUSES:
            raise HTTPException(status_code=422, detail="status inválido")
        if priority and priority not in PRIORITIES:
            raise HTTPException(status_code=422, detail="priority inválida")
        if sort not in SORT_SQL:
            raise HTTPException(status_code=422, detail="sort inválido")
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 100))
        rows, total = self._repo.list_reviews(
            company_id=self.company_id,
            status=status,
            q=q,
            assignee_user_id=assignee_user_id,
            priority=priority,
            sort=sort,
            page=page,
            page_size=page_size,
        )
        return {
            "items": [self._row(r) for r in rows],
            "total": total,
            "page": page,
            "page_size": page_size,
            "sort": sort,
            "company": {"id": self.company_id, "name": self.company_name},
            "meta": {
                "note": "Casos de revisión humana. Las alertas automáticas de Costos V2 no crean casos por sí solas.",
                "statuses": sorted(STATUSES),
                "open_statuses": ["pendiente", "en_revision"],
                "sorts": sorted(SORT_SQL.keys()),
                "costs_are_snapshot": True,
            },
        }

    def get(self, review_id: int) -> dict[str, Any]:
        self._ensure_ready()
        row = self._repo.get(company_id=self.company_id, review_id=review_id)
        if not row:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        events = self._repo.list_events(review_id=int(row["id"]), company_id=self.company_id)
        out = self._row(row)
        out["events"] = [_public_event(e) for e in events]
        out["company"] = {"id": self.company_id, "name": out["company_name"]}
        return out

    def create_or_get(
        self,
        *,
        variant_id: int,
        anomaly_code: str,
        reason_label: str | None = None,
        priority: str = "media",
        source: str = "reviews_grid",
        source_ref: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Crea caso PENDIENTE o reutiliza el abierto. Instantánea desde Costos V2. Sin assignee."""
        self._ensure_ready()
        self._require_mutate()
        if int(variant_id) <= 0:
            raise HTTPException(status_code=422, detail="variant_id inválido")
        code = (anomaly_code or "").strip().lower()
        if not code:
            raise HTTPException(status_code=422, detail="anomaly_code requerido")
        if priority not in PRIORITIES:
            raise HTTPException(status_code=422, detail="priority inválida")
        if source not in SOURCES:
            raise HTTPException(status_code=422, detail="source inválido")

        # Solo metadatos de origen; nunca costos/nombre desde el cliente.
        safe_ref = {
            k: source_ref[k]
            for k in ("reception_id", "detail_id", "severity", "status", "reliability")
            if isinstance(source_ref, dict) and k in source_ref
        }

        existing = self._repo.find_open(
            company_id=self.company_id, variant_id=int(variant_id), anomaly_code=code
        )
        if existing:
            return {"item": self._row(existing), "created": False}

        snap = self._load_snapshot(int(variant_id))
        label = (reason_label or code).strip() or code
        try:
            row = self._repo.insert_review_with_event(
                {
                    "company_id": self.company_id,
                    "company_name": self.company_name,
                    "variant_id": int(variant_id),
                    "anomaly_code": code,
                    "reason_label": label,
                    "priority": priority,
                    "status": "pendiente",
                    "product_name": snap.get("product_name"),
                    "sku_code": snap.get("sku_code"),
                    "barcode": snap.get("barcode"),
                    "current_cost_ref": snap.get("current_cost_ref"),
                    "previous_cost_ref": snap.get("previous_cost_ref"),
                    "change_pct_ref": snap.get("change_pct_ref"),
                    "source": source,
                    "source_ref": Json(safe_ref),
                    "created_by_user_id": _actor_id(self.actor),
                },
                {
                    "actor_user_id": _actor_id(self.actor),
                    "actor_email": _actor_email(self.actor),
                    "action": "created",
                    "status_from": None,
                    "status_to": "pendiente",
                    "note": "Caso de revisión humana creado (aún sin responsable).",
                    "detail": Json({"source": source, "snapshot_from": "costos_v2"}),
                },
            )
        except Exception as exc:
            existing = self._repo.find_open(
                company_id=self.company_id, variant_id=int(variant_id), anomaly_code=code
            )
            if existing:
                return {"item": self._row(existing), "created": False}
            logger.exception("create cost review failed")
            raise HTTPException(status_code=500, detail="No se pudo crear la revisión") from exc

        return {"item": self._row(row), "created": True}

    def start(self, review_id: int) -> dict[str, Any]:
        """Asigna al usuario autenticado y pasa a EN_REVISION. Concurrencia segura."""
        self._ensure_ready()
        self._require_mutate()
        actor_id = _actor_id(self.actor)
        if actor_id is None:
            raise HTTPException(status_code=403, detail="Sesión sin identificador de usuario")

        current = self._repo.get(company_id=self.company_id, review_id=review_id)
        if not current:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        if current["status"] == "en_revision":
            if current.get("assignee_user_id") == actor_id:
                return self._row(current)
            raise HTTPException(
                status_code=409,
                detail=f"La revisión ya está a cargo de {current.get('assignee_email') or 'otro usuario'}",
            )
        if current["status"] in ("resuelto", "descartado"):
            raise HTTPException(status_code=409, detail="La revisión ya está cerrada")

        updated = self._repo.update_review_with_event(
            company_id=self.company_id,
            review_id=review_id,
            sets={
                "status": "en_revision",
                "assignee_user_id": actor_id,
                "assignee_email": _actor_email(self.actor),
                "started_at": datetime.now(timezone.utc),
            },
            event={
                "actor_user_id": actor_id,
                "actor_email": _actor_email(self.actor),
                "action": "started",
                "status_from": "pendiente",
                "status_to": "en_revision",
                "note": "Revisión iniciada; responsable asignado automáticamente.",
                "detail": Json({}),
            },
            expected_status="pendiente",
            expected_assignee_null=True,
        )
        if not updated:
            again = self._repo.get(company_id=self.company_id, review_id=review_id)
            if again and again["status"] == "en_revision":
                raise HTTPException(
                    status_code=409,
                    detail=f"Otro usuario inició la revisión ({again.get('assignee_email')})",
                )
            raise HTTPException(status_code=409, detail="No se pudo iniciar la revisión")
        return self._row(updated)

    def update_observations(self, review_id: int, observations: str) -> dict[str, Any]:
        self._ensure_ready()
        self._require_mutate()
        current = self._repo.get(company_id=self.company_id, review_id=review_id)
        if not current:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        if current["status"] not in ("pendiente", "en_revision"):
            raise HTTPException(status_code=409, detail="No se pueden editar observaciones de un caso cerrado")
        note = (observations or "").strip()
        updated = self._repo.update_review_with_event(
            company_id=self.company_id,
            review_id=review_id,
            sets={"observations": note or None},
            event={
                "actor_user_id": _actor_id(self.actor),
                "actor_email": _actor_email(self.actor),
                "action": "observations_updated",
                "status_from": current["status"],
                "status_to": current["status"],
                "note": note[:500] if note else None,
                "detail": Json({}),
            },
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        return self._row(updated)

    def update_priority(self, review_id: int, priority: str) -> dict[str, Any]:
        self._ensure_ready()
        self._require_mutate()
        if priority not in PRIORITIES:
            raise HTTPException(status_code=422, detail="priority inválida")
        current = self._repo.get(company_id=self.company_id, review_id=review_id)
        if not current:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        if current["status"] not in ("pendiente", "en_revision"):
            raise HTTPException(status_code=409, detail="No se puede cambiar prioridad de un caso cerrado")
        if current["priority"] == priority:
            return self._row(current)
        updated = self._repo.update_review_with_event(
            company_id=self.company_id,
            review_id=review_id,
            sets={"priority": priority},
            event={
                "actor_user_id": _actor_id(self.actor),
                "actor_email": _actor_email(self.actor),
                "action": "priority_updated",
                "status_from": current["status"],
                "status_to": current["status"],
                "note": f"{current['priority']} → {priority}",
                "detail": Json({"from": current["priority"], "to": priority}),
            },
        )
        if not updated:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        return self._row(updated)

    def resolve(self, review_id: int, *, conclusion: str) -> dict[str, Any]:
        """Solo desde EN_REVISION (flujo: Pendiente → En revisión → Resuelto)."""
        return self._close(
            review_id,
            status_to="resuelto",
            conclusion=conclusion,
            action="resolved",
            allowed_from=("en_revision",),
        )

    def discard(self, review_id: int, *, conclusion: str) -> dict[str, Any]:
        """Desde PENDIENTE o EN_REVISION (alerta justificada sin corrección)."""
        return self._close(
            review_id,
            status_to="descartado",
            conclusion=conclusion,
            action="discarded",
            allowed_from=("pendiente", "en_revision"),
        )

    def _close(
        self,
        review_id: int,
        *,
        status_to: str,
        conclusion: str,
        action: str,
        allowed_from: tuple[str, ...],
    ) -> dict[str, Any]:
        self._ensure_ready()
        self._require_mutate()
        text = (conclusion or "").strip()
        if len(text) < 5:
            raise HTTPException(
                status_code=422,
                detail="Debe indicar una conclusión (mín. 5 caracteres no blancos). Cerrar no corrige el costo en Bsale.",
            )
        current = self._repo.get(company_id=self.company_id, review_id=review_id)
        if not current:
            raise HTTPException(status_code=404, detail="Revisión no encontrada")
        if current["status"] in ("resuelto", "descartado"):
            raise HTTPException(status_code=409, detail="La revisión ya está cerrada")
        if current["status"] not in allowed_from:
            raise HTTPException(
                status_code=409,
                detail=(
                    "Transición no permitida: para resolver debe estar En revisión; "
                    "para descartar puede estar Pendiente o En revisión."
                ),
            )
        updated = self._repo.update_review_with_event(
            company_id=self.company_id,
            review_id=review_id,
            sets={
                "status": status_to,
                "conclusion": text,
                "resolved_at": datetime.now(timezone.utc),
            },
            event={
                "actor_user_id": _actor_id(self.actor),
                "actor_email": _actor_email(self.actor),
                "action": action,
                "status_from": current["status"],
                "status_to": status_to,
                "note": text[:1000],
                "detail": Json(
                    {
                        "cost_origin_unchanged": True,
                        "message": "Cerrar la revisión no modifica costos ni documentos Bsale.",
                        "snapshot_preserved": True,
                    }
                ),
            },
            expected_status=current["status"],
        )
        if not updated:
            raise HTTPException(status_code=409, detail="No se pudo cerrar la revisión (concurrencia)")
        return self._row(updated)


def run_cost_reviews(
    user: dict[str, Any],
    company_id: int,
    fn: Callable[[CostReviewsService], T],
    *,
    get_conn: Callable[[], Any] = get_connection,
) -> T:
    """Valida empresa, prepara snapshot Costos V2 y ejecuta la acción."""
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(
            conn,
            statement_timeout_seconds=25,
            lock_timeout="3s",
        )
        scope = resolve_company_scope(user, company_id, executor)
        cost_svc = CostControlV2Service(
            CostControlV2Repository(executor),
            scope,
            use_cache=False,
        )

        def snapshot_fn(variant_id: int) -> dict[str, Any]:
            item = cost_svc.get_product(variant_id)
            return snapshot_from_cost_product(item)

        return fn(
            CostReviewsService(
                company_id=scope.company_id,
                company_name=scope.company_name,
                actor=user,
                snapshot_fn=snapshot_fn,
            )
        )
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
