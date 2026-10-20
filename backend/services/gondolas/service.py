"""Control de Góndolas: catálogo por sucursal, recorridos persistentes y reposición.

Reglas de negocio:
- el universo del recorrido son SKU distintos (variantes) con stock positivo al iniciar;
- el stock de referencia se congela al crear el recorrido y se contrasta al cerrarlo;
- un SKU se marca encontrado una sola vez por recorrido;
- un código con varias variantes coincidentes nunca se resuelve arbitrariamente;
- la última entrada es la última recepción/transferencia registrada en RAW para esa
  oficina; si no existe registro, se informa como dato no disponible;
- nada de este módulo modifica stock, documentos ni precios de Bsale.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping

from backend.db import get_connection
from backend.services.analytics.validate_distribuidora_source import (
    make_psycopg_executor,
    make_psycopg_rw_executor,
    open_readonly_connection,
    open_readwrite_connection,
)
from backend.services.analytics_v2.access import CompanyScope, resolve_company_scope
from backend.services.gondolas.access import gondola_capabilities
from backend.services.gondolas.repository import GondolasRepository

SCAN_RESULTS = (
    "encontrado", "duplicado", "ambiguo", "sin_coincidencia",
    "fuera_de_tipo", "stock_cero", "fuera_de_universo",
)
REPLENISHMENT_STATUSES = (
    "pendiente_buscar", "encontrado_bodega", "repuesto_gondola", "no_encontrado",
)
STOCK_FILTERS = ("todos", "con_stock", "sin_stock")
CATALOG_SORTS = ("producto", "stock_desc", "stock_asc", "ultima_entrada_desc")


class GondolasValidationError(ValueError):
    pass


class GondolasNotFound(LookupError):
    pass


class GondolasNotReady(RuntimeError):
    pass


class GondolasForbidden(PermissionError):
    pass


def _actor(user: Mapping[str, Any] | None) -> tuple[str, str | None]:
    email = str((user or {}).get("email") or "").strip().lower()
    role = (user or {}).get("role")
    return email or "desconocido", (str(role).strip().lower() if role else None)


def _num(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _row_base(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "variant_id": row.get("variant_id"),
        "product_id": row.get("product_id"),
        "sku_code": row.get("sku_code"),
        "bar_code": row.get("bar_code"),
        "product_name": row.get("product_name"),
        "variant_name": row.get("variant_name"),
    }


def classify_scan(
    candidates: list[Mapping[str, Any]],
    session_item: Mapping[str, Any] | None,
    *,
    product_type_id: int,
) -> tuple[str, Mapping[str, Any] | None]:
    """Clasifica un código leído sin efectos secundarios (probable en tests)."""
    if not candidates:
        return "sin_coincidencia", None
    distinct = {c.get("variant_id") for c in candidates}
    if len(distinct) > 1:
        return "ambiguo", None
    candidate = candidates[0]
    if session_item is not None:
        if session_item.get("exhibition_status") == "encontrado":
            return "duplicado", candidate
        return "encontrado", candidate
    if candidate.get("product_type_id") != product_type_id:
        return "fuera_de_tipo", candidate
    stock = candidate.get("stock")
    if stock is None or Decimal(str(stock)) <= 0:
        return "stock_cero", candidate
    return "fuera_de_universo", candidate


class GondolasService:
    def __init__(self, repo: GondolasRepository, scope: CompanyScope,
                 user: Mapping[str, Any] | None = None) -> None:
        self._repo = repo
        self._scope = scope
        self._actor, self._role = _actor(user)
        self._caps = gondola_capabilities(dict(user) if user else None)
        self._ready: bool | None = None

    @property
    def company_id(self) -> int:
        return self._scope.company_id

    def _tables_ready(self) -> bool:
        if self._ready is None:
            self._ready = self._repo.tables_ready()
        return self._ready

    def _require_ready(self) -> None:
        if not self._tables_ready():
            raise GondolasNotReady(
                "Control de Góndolas aún no tiene sus tablas migradas en esta base "
                "(pendiente: backend/sql/gondolas/001_gondola_control.sql)"
            )

    def _require_operate(self) -> None:
        if not self._caps.get("operate"):
            raise GondolasForbidden("Su rol no permite trabajar recorridos de góndolas")

    def _session_or_404(self, session_id: int) -> dict[str, Any]:
        session = self._repo.session(self.company_id, session_id)
        if session is None:
            raise GondolasNotFound("Recorrido no encontrado en la empresa seleccionada")
        return session

    # ------------------------------------------------------------- catálogo
    def options(self) -> dict[str, Any]:
        return {
            "company_id": self.company_id,
            "company_name": self._scope.company_name,
            "offices": self._repo.offices(self.company_id),
            "product_types": [
                {
                    "product_type_id": r.get("product_type_id"),
                    "name": r.get("product_type_name"),
                    "variant_count": r.get("variant_count"),
                }
                for r in self._repo.product_types(self.company_id)
            ],
            "capabilities": self._caps,
            "tables_ready": self._tables_ready(),
        }

    def catalog(self, office_id: int, product_type_id: int, *, q: str = "",
                stock_filter: str = "todos", exhibition_filter: str = "",
                session_id: int | None = None, sort: str = "producto",
                page: int = 1, page_size: int = 100) -> dict[str, Any]:
        if stock_filter not in STOCK_FILTERS:
            raise GondolasValidationError("Filtro de stock inválido")
        if sort not in CATALOG_SORTS:
            raise GondolasValidationError("Ordenamiento inválido")
        if exhibition_filter not in ("", "pendiente", "encontrado"):
            raise GondolasValidationError("Filtro de exhibición inválido")
        ready = self._tables_ready()
        if session_id and not ready:
            raise GondolasNotReady("Los recorridos aún no están migrados en esta base")
        rows = self._repo.catalog(
            self.company_id, office_id, product_type_id, session_id, ready
        )
        needle = q.strip().lower()
        items: list[dict[str, Any]] = []
        for row in rows:
            stock = _num(row.get("stock")) or 0
            if stock_filter == "con_stock" and stock <= 0:
                continue
            if stock_filter == "sin_stock" and stock > 0:
                continue
            status = row.get("exhibition_status")
            if exhibition_filter and status != exhibition_filter:
                continue
            if needle:
                haystack = " ".join(
                    str(row.get(k) or "") for k in ("product_name", "variant_name", "sku_code", "bar_code")
                ).lower()
                if needle not in haystack:
                    continue
            items.append({
                **_row_base(row),
                "product_type_id": row.get("product_type_id"),
                "product_type_name": row.get("product_type_name"),
                "stock": stock,
                "stock_available": _num(row.get("stock_available")),
                "stock_reserved": _num(row.get("stock_reserved")),
                "stock_fetched_at": _iso(row.get("stock_fetched_at")),
                "last_entry_at": _iso(row.get("last_entry_at")),
                "last_entry_movement": row.get("last_entry_movement"),
                "last_entry_reception_id": row.get("last_entry_reception_id"),
                "exhibition_status": status,
                "found_at": _iso(row.get("found_at")),
                "found_by": row.get("found_by"),
            })
        if sort == "producto":
            items.sort(key=lambda i: (str(i["product_name"] or ""), str(i["variant_name"] or "")))
        elif sort == "stock_desc":
            items.sort(key=lambda i: (-(i["stock"] or 0), str(i["product_name"] or "")))
        elif sort == "stock_asc":
            items.sort(key=lambda i: (i["stock"] or 0, str(i["product_name"] or "")))
        else:
            items.sort(key=lambda i: (i["last_entry_at"] is None, -(date.fromisoformat(i["last_entry_at"]).toordinal() if i["last_entry_at"] else 0)))
        total = len(items)
        start = (page - 1) * page_size
        return {
            "company_id": self.company_id,
            "office_id": office_id,
            "product_type_id": product_type_id,
            "session_id": session_id,
            "total": total,
            "page": page,
            "page_size": page_size,
            "items": items[start:start + page_size],
        }

    # ------------------------------------------------------------ recorridos
    def _session_out(self, row: Mapping[str, Any]) -> dict[str, Any]:
        total = int(row.get("item_count") or row.get("total_sku_snapshot") or 0)
        found = int(row.get("found_count") or 0)
        pending = int(row.get("pending_count") or max(total - found, 0))
        return {
            "id": row.get("id"),
            "company_id": row.get("company_id"),
            "office_id": row.get("office_id"),
            "office_name": row.get("office_name"),
            "product_type_id": row.get("product_type_id"),
            "product_type_name": row.get("product_type_name"),
            "status": row.get("status"),
            "created_by": row.get("created_by"),
            "started_at": _iso(row.get("started_at")),
            "finished_at": _iso(row.get("finished_at")),
            "finished_by": row.get("finished_by"),
            "cancelled_at": _iso(row.get("cancelled_at")),
            "cancelled_by": row.get("cancelled_by"),
            "stock_snapshot_at": _iso(row.get("stock_snapshot_at")),
            "total_sku": total,
            "found_count": found,
            "pending_count": pending,
            "coverage_pct": round(found * 100.0 / total, 2) if total else 0.0,
            "stock_changed_count": int(row.get("stock_changed_count") or 0),
        }

    def list_sessions(self, limit: int = 30) -> dict[str, Any]:
        self._require_ready()
        return {"items": [self._session_out(r) for r in self._repo.sessions(self.company_id, limit)]}

    def create_session(self, office_id: int, product_type_id: int) -> dict[str, Any]:
        self._require_ready()
        self._require_operate()
        offices = {r.get("office_id") for r in self._repo.offices(self.company_id)}
        if office_id not in offices:
            raise GondolasValidationError("Oficina no disponible para la empresa seleccionada")
        types = {r.get("product_type_id") for r in self._repo.product_types(self.company_id)}
        if product_type_id not in types:
            raise GondolasValidationError("Tipo de producto no disponible para la empresa seleccionada")
        session = self._repo.create_session(
            self.company_id, office_id, product_type_id, self._actor, self._role
        )
        total = self._repo.freeze_items(
            int(session["id"]), self.company_id, office_id, product_type_id
        )
        self._repo.set_session_total(int(session["id"]), self.company_id, total)
        return self.get_session(int(session["id"]))

    def get_session(self, session_id: int, scans_limit: int = 20) -> dict[str, Any]:
        self._require_ready()
        session = self._session_or_404(session_id)
        out = self._session_out(session)
        items = []
        for row in self._repo.session_items(self.company_id, session_id, int(session["office_id"])):
            stock_now = _num(row.get("stock_current")) or 0
            snapshot = _num(row.get("stock_snapshot")) or 0
            items.append({
                **_row_base(row),
                "stock_snapshot": snapshot,
                "stock_available_snapshot": _num(row.get("stock_available_snapshot")),
                "stock_at_close": _num(row.get("stock_at_close")),
                "stock_current": stock_now,
                "stock_changed": stock_now != snapshot,
                "last_entry_at": _iso(row.get("last_entry_at")),
                "last_entry_movement": row.get("last_entry_movement"),
                "exhibition_status": row.get("exhibition_status"),
                "found_at": _iso(row.get("found_at")),
                "found_by": row.get("found_by"),
                "found_device": row.get("found_device"),
            })
        out["items"] = items
        out["recent_scans"] = [
            {**r, "scanned_at": _iso(r.get("scanned_at"))}
            for r in self._repo.recent_scans(self.company_id, session_id, scans_limit)
        ]
        if session.get("status") == "cerrado":
            out["replenishment"] = self.list_replenishment(session_id)
        return out

    def register_scan(self, session_id: int, bar_code: str,
                      device: str | None = None, variant_id: int | None = None) -> dict[str, Any]:
        self._require_ready()
        self._require_operate()
        code = (bar_code or "").strip()
        if not code:
            raise GondolasValidationError("Código de barras vacío")
        session = self._session_or_404(session_id)
        if session.get("status") != "en_curso":
            raise GondolasValidationError("El recorrido no está en curso")
        # Candado transaccional: si un cierre/cancelación ganó la fila de la
        # sesión, esta actualización no devuelve fila y el escaneo se rechaza.
        if not self._repo.touch_active_session(self.company_id, session_id):
            raise GondolasValidationError("El recorrido ya no está en curso")
        office_id = int(session["office_id"])
        product_type_id = int(session["product_type_id"])
        candidates = self._repo.scan_candidates(self.company_id, office_id, code)
        if variant_id is not None:
            chosen = [c for c in candidates if c.get("variant_id") == variant_id]
            if not chosen:
                raise GondolasValidationError("La variante elegida no coincide con el código leído")
            candidates = chosen
        distinct = {c.get("variant_id") for c in candidates}
        item = None
        if len(distinct) == 1:
            item = self._repo.session_item(
                self.company_id, session_id, int(candidates[0]["variant_id"])
            )
        result, candidate = classify_scan(candidates, item, product_type_id=product_type_id)
        found_at = None
        if result == "encontrado":
            marked = self._repo.mark_found(
                self.company_id, session_id, int(candidate["variant_id"]), self._actor, device
            )
            if marked is None:
                # La actualización condicional no marcó: otro escaneo simultáneo
                # ya registró el hallazgo (duplicado) o el recorrido dejó de
                # estar en curso. Se distingue antes de responder.
                current = self._repo.session_item(
                    self.company_id, session_id, int(candidate["variant_id"])
                )
                if current is not None and current.get("exhibition_status") == "encontrado":
                    result = "duplicado"
                else:
                    raise GondolasValidationError("El recorrido ya no está en curso")
            else:
                found_at = _iso(marked.get("found_at"))
        self._repo.insert_scan(
            self.company_id, session_id, code,
            int(candidate["variant_id"]) if candidate is not None else None,
            result, device, self._actor,
        )
        progress = self._session_or_404(session_id)
        return {
            "result": result,
            "bar_code": code,
            "found_at": found_at,
            "product": (
                {**_row_base(candidate), "product_type_id": candidate.get("product_type_id"),
                 "stock": _num(candidate.get("stock"))}
                if candidate is not None else None
            ),
            "candidates": [
                {**_row_base(c), "stock": _num(c.get("stock"))} for c in candidates
            ] if result == "ambiguo" else [],
            "progress": {
                "total_sku": int(progress.get("item_count") or 0),
                "found_count": int(progress.get("found_count") or 0),
                "pending_count": int(progress.get("pending_count") or 0),
                "coverage_pct": self._session_out(progress)["coverage_pct"],
            },
        }

    def close_session(self, session_id: int) -> dict[str, Any]:
        self._require_ready()
        self._require_operate()
        session = self._session_or_404(session_id)
        if session.get("status") != "en_curso":
            raise GondolasValidationError("Solo se puede cerrar un recorrido en curso")
        # Primero se reclama el cierre (bloquea escaneos concurrentes); recién
        # después se congela el stock actual y se genera la reposición, viendo
        # todos los hallazgos confirmados antes del cierre.
        closed = self._repo.claim_close(self.company_id, session_id, self._actor)
        if closed is None:
            raise GondolasValidationError("El recorrido ya no está en curso")
        self._repo.close_items(self.company_id, session_id)
        created = self._repo.create_replenishment(self.company_id, session_id)
        out = self.get_session(session_id)
        out["replenishment_created"] = created
        return out

    def cancel_session(self, session_id: int, reason: str | None = None) -> dict[str, Any]:
        self._require_ready()
        if not self._caps.get("cancel"):
            raise GondolasForbidden("Solo gerencia y administración pueden cancelar recorridos")
        session = self._session_or_404(session_id)
        if session.get("status") != "en_curso":
            raise GondolasValidationError("Solo se puede cancelar un recorrido en curso")
        cancelled = self._repo.cancel_session(self.company_id, session_id, self._actor, reason)
        if cancelled is None:
            raise GondolasValidationError("El recorrido ya no está en curso")
        return self._session_out(self._session_or_404(session_id))

    # ------------------------------------------------------------- reposición
    def list_replenishment(self, session_id: int) -> list[dict[str, Any]]:
        self._require_ready()
        session = self._session_or_404(session_id)
        out = []
        for row in self._repo.replenishment(self.company_id, session_id, int(session["office_id"])):
            snapshot = _num(row.get("stock_snapshot")) or 0
            current = _num(row.get("stock_current")) or 0
            out.append({
                **_row_base(row),
                "status": row.get("status"),
                "note": row.get("note"),
                "stock_snapshot": snapshot,
                "stock_at_close": _num(row.get("stock_at_close")),
                "stock_current": current,
                "stock_changed": current != snapshot,
                "updated_by": row.get("updated_by"),
                "updated_at": _iso(row.get("updated_at")),
            })
        return out

    def update_replenishment(self, session_id: int, variant_id: int,
                             status: str, note: str | None = None) -> dict[str, Any]:
        self._require_ready()
        self._require_operate()
        if status not in REPLENISHMENT_STATUSES:
            raise GondolasValidationError("Estado de reposición inválido")
        session = self._session_or_404(session_id)
        if session.get("status") != "cerrado":
            raise GondolasValidationError("La lista de reposición se trabaja con el recorrido cerrado")
        updated = self._repo.update_replenishment(
            self.company_id, session_id, variant_id, status, note, self._actor
        )
        if updated is None:
            raise GondolasNotFound("SKU no encontrado en la lista de reposición")
        for row in self.list_replenishment(session_id):
            if row["variant_id"] == variant_id:
                return row
        raise GondolasNotFound("SKU no encontrado en la lista de reposición")


# ---------------------------------------------------------------- runners


def _close_quietly(conn: Any, *, commit: bool = False) -> None:
    try:
        if commit:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


def run_gondola_companies(user: dict[str, Any],
                          *, get_conn: Callable[[], Any] = get_connection) -> dict[str, Any]:
    """Empresas con RAW disponible, filtradas por el alcance del token si lo declara."""
    from backend.services.analytics_v2.access import token_company_ids

    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=15, lock_timeout="3s")
        rows = GondolasRepository(executor).companies()
        allowed = token_company_ids(user)
        if allowed is not None:
            rows = [r for r in rows if r.get("company_id") in allowed]
        return {"companies": rows}
    finally:
        _close_quietly(conn)


def run_gondola_read(user: dict[str, Any], company_id: int,
                     fn: Callable[[GondolasService], Any],
                     *, get_conn: Callable[[], Any] = get_connection) -> Any:
    conn = open_readonly_connection(get_conn)
    try:
        executor = make_psycopg_executor(conn, statement_timeout_seconds=25, lock_timeout="3s")
        scope = resolve_company_scope(user, company_id, executor)
        return fn(GondolasService(GondolasRepository(executor), scope, user=user))
    finally:
        _close_quietly(conn)


def run_gondola_write(user: dict[str, Any], company_id: int,
                      fn: Callable[[GondolasService], Any],
                      *, get_conn: Callable[[], Any] = get_connection) -> Any:
    conn = open_readwrite_connection(get_conn)
    try:
        executor = make_psycopg_rw_executor(conn, statement_timeout_seconds=20, lock_timeout="5s")
        scope = resolve_company_scope(user, company_id, executor)
        result = fn(GondolasService(GondolasRepository(executor), scope, user=user))
        conn.commit()
        return result
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
