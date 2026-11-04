"""Asignación de chofer (usuario multiapp) a dispatch_plan.

Independiente de confirm_dispatch_plan. No modifica OC/facturas/pesos/picking.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from backend.auth.multiapp import APP_PWA, load_multiapp_scope
from backend.auth.permissions import has_management_access, normalized_role
from backend.db import get_connection
from backend.repositories.distribuidora import dispatch_plan_load_batch_repo as events_repo
from backend.repositories.distribuidora import dispatch_plan_repo as plan_repo

logger = logging.getLogger(__name__)

ACTION_ASSIGN_DRIVER = "assign_driver"

ASSIGNABLE_PLAN_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
    }
)

# Despachos que aún cuentan como “en curso” para aviso mismo día.
SAME_DAY_ACTIVE_STATUSES = frozenset(
    {
        "planned",
        "invoicing",
        "ready_for_picking",
        "picking_generated",
        "dispatched",
    }
)

DRIVER_ASSIGN_ROLES = frozenset({"gerencia", "operativo", "adm", "admin"})


class DriverAssignError(ValueError):
    def __init__(self, message: str, *, code: str = "driver_assign_rejected", http_status: int = 400):
        super().__init__(message)
        self.code = code
        self.http_status = http_status


def can_assign_dispatch_driver(user: dict[str, Any] | None) -> bool:
    """Administrador/supervisor ERP: gerencia/adm/operativo/owner."""
    from backend.auth.hierarchy import is_owner

    if not user:
        return False
    if is_owner(user):
        return True
    if has_management_access(user):
        return True
    return normalized_role(user) in DRIVER_ASSIGN_ROLES


def _display_name(row: dict[str, Any]) -> str:
    return (
        (row.get("nombre_usuario") or "").strip()
        or (row.get("email") or "").strip()
        or f"user:{row.get('id')}"
    )


def _load_user_row(cur, user_id: int) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT id, email, role, active, nombre_usuario
        FROM bsale.users
        WHERE id = %s
        """,
        (int(user_id),),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


DRIVER_REQUIREMENT_LABELS_ES = {
    "user_inactive": "Usuario desactivado",
    "pwa_required": "Falta acceso PWA explícito (app.user_apps)",
    "chofer_profile_required": "Falta perfil operacional chofer activo",
    "company_not_allowed": "Sin empresa autorizada para el despacho",
    "office_not_allowed": "Sin oficina autorizada para el despacho",
    "scope_unavailable": "No se pudo verificar alcance multiapp",
    "user_not_found": "Usuario inexistente",
}


def _eligibility_from_scope(
    row: dict[str, Any],
    scope: Any,
    *,
    company_id: int | None = None,
    office_id: int | None = None,
) -> tuple[bool, list[str]]:
    missing: list[str] = []
    if not bool(row.get("active")):
        missing.append("user_inactive")
    if scope.load_failed:
        missing.append("scope_unavailable")
        return False, missing
    if APP_PWA not in scope.apps:
        missing.append("pwa_required")
    if "chofer" not in scope.profiles:
        missing.append("chofer_profile_required")
    if company_id is not None and scope.company_ids is not None:
        if int(company_id) not in scope.company_ids:
            missing.append("company_not_allowed")
    if company_id is not None and office_id is not None and scope.offices:
        if (int(company_id), int(office_id)) not in scope.offices:
            # Evitar ruido si ya falló la empresa.
            if "company_not_allowed" not in missing:
                missing.append("office_not_allowed")
    return (len(missing) == 0), missing


def _driver_scope(_cur, user_id: int):
    """Alcance multiapp para elegibilidad de chofer.

    Usa conexión propia (no el cursor del listado): un SELECT fallido sobre
    ``user_companies``/tablas multiapp no debe abortar la TX del caller ni
    convertir el listado en HTTP 503 genérico.
    """
    return load_multiapp_scope(int(user_id), always_load_companies=True)


def diagnose_driver_eligibility(
    user_id: int,
    *,
    company_id: int | None = None,
    office_id: int | None = None,
) -> dict[str, Any]:
    """Diagnóstico de elegibilidad para administración (sin ampliar permisos)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        row = _load_user_row(cur, int(user_id))
        if not row:
            cur.close()
            return {
                "user_id": int(user_id),
                "eligible": False,
                "missing": ["user_not_found"],
                "missing_labels": [DRIVER_REQUIREMENT_LABELS_ES["user_not_found"]],
                "requirements_summary_es": "Activo + PWA explícita + perfil chofer",
            }
        scope = _driver_scope(cur, int(user_id))
        ok, missing = _eligibility_from_scope(
            row, scope, company_id=company_id, office_id=office_id
        )
        cur.close()
        return {
            "user_id": int(user_id),
            "display_name": _display_name(row),
            "active": bool(row.get("active")),
            "eligible": ok,
            "has_pwa": APP_PWA in scope.apps and not scope.load_failed,
            "has_chofer_profile": "chofer" in scope.profiles and not scope.load_failed,
            "apps": sorted(scope.apps) if not scope.load_failed else [],
            "profiles": sorted(scope.profiles) if not scope.load_failed else [],
            "company_ids": (
                list(scope.company_ids)
                if scope.company_ids is not None and not scope.load_failed
                else None
            ),
            "offices": (
                [
                    {"company_id": c, "office_id": o}
                    for c, o in sorted(scope.offices)
                ]
                if not scope.load_failed
                else []
            ),
            "filter_company_id": company_id,
            "filter_office_id": office_id,
            "missing": missing,
            "missing_labels": [DRIVER_REQUIREMENT_LABELS_ES.get(m, m) for m in missing],
            "requirements_summary_es": (
                "Activo + PWA explícita + perfil chofer"
                + (
                    " + empresa/oficina del despacho"
                    if company_id is not None or office_id is not None
                    else ""
                )
            ),
        }
    finally:
        conn.close()


def assert_user_is_eligible_driver(
    cur,
    user_id: int,
    *,
    company_id: int | None = None,
    office_id: int | None = None,
) -> dict[str, Any]:
    """Valida usuario activo + PWA + perfil chofer (+ alcance opcional)."""
    row = _load_user_row(cur, user_id)
    if not row:
        raise DriverAssignError("Usuario inexistente", code="user_not_found", http_status=404)
    scope = _driver_scope(cur, int(user_id))
    ok, missing = _eligibility_from_scope(
        row, scope, company_id=company_id, office_id=office_id
    )
    if not ok:
        code = missing[0]
        if code == "scope_unavailable":
            status = 503
        elif code in ("company_not_allowed", "office_not_allowed"):
            status = 403
        else:
            status = 409
        raise DriverAssignError(
            DRIVER_REQUIREMENT_LABELS_ES.get(code, "Chofer no elegible"),
            code=code,
            http_status=status,
        )
    return {
        **row,
        "display_name": _display_name(row),
        "apps": sorted(scope.apps),
        "profiles": sorted(scope.profiles),
    }


def list_available_drivers(
    *,
    company_id: int | None = None,
    office_id: int | None = None,
    include_diagnostics: bool = False,
) -> dict[str, Any]:
    """Choferes PWA activos (user_apps + user_operational_profiles).

    Causa frecuente de lista vacía: usuario con rol ERP operativo pero sin
    fila app.user_apps(pwa) y/o sin perfil chofer. El rol ERP no basta.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT u.id, u.email, u.nombre_usuario, u.active
            FROM bsale.users u
            INNER JOIN app.user_apps ua
                ON ua.user_id = u.id AND ua.app_code = 'pwa' AND ua.active IS TRUE
            INNER JOIN app.user_operational_profiles up
                ON up.user_id = u.id AND up.profile_code = 'chofer' AND up.active IS TRUE
            WHERE u.active IS TRUE
            ORDER BY lower(COALESCE(u.nombre_usuario, u.email))
            """
        )
        cols = [d[0] for d in cur.description]
        candidates = [dict(zip(cols, r)) for r in cur.fetchall()]
        out: list[dict[str, Any]] = []
        for row in candidates:
            uid = int(row["id"])
            try:
                eligible = assert_user_is_eligible_driver(
                    cur, uid, company_id=company_id, office_id=office_id
                )
            except DriverAssignError:
                continue
            out.append(
                {
                    "user_id": uid,
                    "email": eligible.get("email"),
                    "nombre_usuario": eligible.get("nombre_usuario"),
                    "display_name": eligible.get("display_name"),
                }
            )

        diagnostics: dict[str, Any] = {
            "requirements_summary_es": (
                "Usuario activo + app PWA explícita + perfil operacional chofer. "
                "El rol ERP (p. ej. operativo) no habilita el selector por sí solo."
            ),
            "eligible_count": len(out),
            "filter_company_id": company_id,
            "filter_office_id": office_id,
        }
        if include_diagnostics:
            try:
                cur.execute(
                    """
                    SELECT DISTINCT u.id, u.email, u.nombre_usuario, u.active
                    FROM bsale.users u
                    WHERE u.active IS TRUE
                      AND (
                        EXISTS (
                          SELECT 1 FROM app.user_apps ua
                          WHERE ua.user_id = u.id AND ua.app_code = 'pwa'
                        )
                        OR EXISTS (
                          SELECT 1 FROM app.user_operational_profiles up
                          WHERE up.user_id = u.id AND up.profile_code = 'chofer'
                        )
                      )
                    ORDER BY lower(COALESCE(u.nombre_usuario, u.email))
                    LIMIT 100
                    """
                )
                dcols = [d[0] for d in cur.description]
                near: list[dict[str, Any]] = []
                eligible_ids = {int(x["user_id"]) for x in out}
                for r in cur.fetchall():
                    drow = dict(zip(dcols, r))
                    uid = int(drow["id"])
                    if uid in eligible_ids:
                        continue
                    scope = _driver_scope(cur, uid)
                    _ok, missing = _eligibility_from_scope(
                        drow, scope, company_id=company_id, office_id=office_id
                    )
                    if not missing:
                        continue
                    near.append(
                        {
                            "user_id": uid,
                            "display_name": _display_name(drow),
                            "missing": missing,
                            "missing_labels": [
                                DRIVER_REQUIREMENT_LABELS_ES.get(m, m) for m in missing
                            ],
                        }
                    )
                diagnostics["ineligible_near_misses"] = near
                diagnostics["note_es"] = (
                    "Si creó un chofer y no aparece, revise en Admin → Usuarios que tenga "
                    "PWA + perfil chofer (no solo rol operativo ERP)."
                )
            except Exception:
                logger.exception("available-drivers diagnostics failed")
                try:
                    conn.rollback()
                except Exception:
                    pass
                diagnostics["diagnostics_error"] = True
                diagnostics["note_es"] = (
                    "Listado OK; el diagnóstico auxiliar falló y se omitió."
                )
        cur.close()
        return {"items": out, "diagnostics": diagnostics}
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()


def find_same_day_driver_conflicts(
    cur,
    *,
    driver_user_id: int,
    planning_date: date,
    exclude_plan_id: int,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT id, planning_code, status, truck_id, route_name, planning_name
        FROM distribuidora.dispatch_plan
        WHERE driver_user_id = %s
          AND planning_date = %s
          AND id <> %s
          AND status = ANY(%s)
        ORDER BY id DESC
        """,
        (
            int(driver_user_id),
            planning_date,
            int(exclude_plan_id),
            list(SAME_DAY_ACTIVE_STATUSES),
        ),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def get_plan_driver(plan_id: int) -> dict[str, Any]:
    conn = get_connection()
    try:
        cur = conn.cursor()
        plan = plan_repo.get_plan_by_id(cur, int(plan_id))
        if not plan:
            raise DriverAssignError("Plan no encontrado", code="plan_not_found", http_status=404)
        driver_user_id = plan.get("driver_user_id")
        driver = None
        if driver_user_id is not None:
            row = _load_user_row(cur, int(driver_user_id))
            if row:
                driver = {
                    "user_id": int(row["id"]),
                    "email": row.get("email"),
                    "nombre_usuario": row.get("nombre_usuario"),
                    "display_name": _display_name(row),
                    "active": bool(row.get("active")),
                }
        history = _list_driver_events(cur, int(plan_id))
        cur.close()
        return {
            "plan_id": int(plan_id),
            "planning_date": plan.get("planning_date"),
            "status": plan.get("status"),
            "driver_user_id": int(driver_user_id) if driver_user_id is not None else None,
            "driver_name": plan.get("driver_name"),
            "assistant_count": plan.get("assistant_count"),
            "driver_pending": driver_user_id is None,
            "driver": driver,
            "history": history,
        }
    finally:
        conn.close()


def _list_driver_events(cur, plan_id: int) -> list[dict[str, Any]]:
    rows = events_repo.list_order_events(cur, int(plan_id), limit=100)
    out = []
    for row in rows:
        if str(row.get("action") or "") != ACTION_ASSIGN_DRIVER:
            continue
        payload = row.get("payload") or {}
        if isinstance(payload, str):
            import json

            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        out.append(
            {
                "id": row.get("id"),
                "created_at": row.get("created_at"),
                "actor_name": row.get("user_name"),
                "reason": row.get("reason"),
                "previous_driver_user_id": payload.get("previous_driver_user_id"),
                "previous_driver_name": payload.get("previous_driver_name"),
                "new_driver_user_id": payload.get("new_driver_user_id"),
                "new_driver_name": payload.get("new_driver_name"),
            }
        )
    return out


def assign_driver_to_plan(
    *,
    plan_id: int,
    driver_user_id: int,
    reason: str,
    actor: dict[str, Any],
    company_id: int | None = None,
    office_id: int | None = None,
) -> dict[str, Any]:
    reason_n = (reason or "").strip()
    if len(reason_n) < 3:
        raise DriverAssignError(
            "Motivo obligatorio (mínimo 3 caracteres)",
            code="reason_required",
        )
    from backend.services.distribuidora.dispatch_plan_access import (
        user_can_assign_driver_on_plan,
    )

    if not user_can_assign_driver_on_plan(actor, int(plan_id)):
        raise DriverAssignError(
            "Sin permiso para asignar choferes",
            code="forbidden",
            http_status=403,
        )

    actor_name = (
        str(actor.get("email") or actor.get("nombre_usuario") or actor.get("id") or "staff")
    )
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, status, planning_date, driver_user_id, driver_name, truck_id
            FROM distribuidora.dispatch_plan
            WHERE id = %s
            FOR UPDATE
            """,
            (int(plan_id),),
        )
        row = cur.fetchone()
        if not row:
            raise DriverAssignError("Plan no encontrado", code="plan_not_found", http_status=404)
        cols = [d[0] for d in cur.description]
        plan = dict(zip(cols, row))
        status = str(plan.get("status") or "")
        if status not in ASSIGNABLE_PLAN_STATUSES:
            raise DriverAssignError(
                f"Plan en estado '{status}' no admite asignación de chofer",
                code="plan_not_assignable",
                http_status=409,
            )

        eligible = assert_user_is_eligible_driver(
            cur,
            int(driver_user_id),
            company_id=company_id,
            office_id=office_id,
        )
        prev_id = plan.get("driver_user_id")
        prev_name = plan.get("driver_name")
        if prev_id is not None and int(prev_id) == int(driver_user_id):
            conn.commit()
            return {
                "status": "unchanged",
                "plan_id": int(plan_id),
                "driver_user_id": int(driver_user_id),
                "driver_name": prev_name,
                "same_day_conflicts": [],
            }

        pdate = plan.get("planning_date")
        conflicts = []
        if isinstance(pdate, date):
            conflicts = find_same_day_driver_conflicts(
                cur,
                driver_user_id=int(driver_user_id),
                planning_date=pdate,
                exclude_plan_id=int(plan_id),
            )

        new_name = eligible["display_name"]
        cur.execute(
            """
            UPDATE distribuidora.dispatch_plan
            SET driver_user_id = %s,
                driver_name = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (int(driver_user_id), new_name, int(plan_id)),
        )
        events_repo.insert_order_event(
            cur,
            plan_id=int(plan_id),
            action=ACTION_ASSIGN_DRIVER,
            user_name=actor_name,
            reason=reason_n,
            payload={
                "previous_driver_user_id": int(prev_id) if prev_id is not None else None,
                "previous_driver_name": prev_name,
                "new_driver_user_id": int(driver_user_id),
                "new_driver_name": new_name,
                "actor_user_id": actor.get("id") or actor.get("uid"),
            },
        )
        conn.commit()
        cur.close()
        return {
            "status": "assigned" if prev_id is None else "reassigned",
            "plan_id": int(plan_id),
            "driver_user_id": int(driver_user_id),
            "driver_name": new_name,
            "previous_driver_user_id": int(prev_id) if prev_id is not None else None,
            "previous_driver_name": prev_name,
            "same_day_conflicts": conflicts,
            "same_day_conflict_policy": "warning_only_no_schedules",
            "notes": {
                "picking_unchanged": True,
                "orders_unchanged": True,
                "legacy_without_driver_allowed": True,
            },
        }
    except DriverAssignError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_plans_for_driver_user(driver_user_id: int, *, limit: int = 50) -> list[dict[str, Any]]:
    """Despachos operativos visibles para el chofer autenticado (prep PWA)."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                id, planning_code, planning_name, planning_date, truck_id,
                truck_name, route_name, status, driver_user_id, driver_name,
                confirmed_at
            FROM distribuidora.dispatch_plan
            WHERE driver_user_id = %s
              AND status = ANY(%s)
            ORDER BY planning_date DESC, id DESC
            LIMIT %s
            """,
            (
                int(driver_user_id),
                list(SAME_DAY_ACTIVE_STATUSES),
                max(1, min(int(limit), 200)),
            ),
        )
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
        return rows
    finally:
        conn.close()


def driver_has_access_to_plan(driver_user_id: int, plan_id: int) -> bool:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 1
            FROM distribuidora.dispatch_plan
            WHERE id = %s
              AND driver_user_id = %s
              AND status = ANY(%s)
            LIMIT 1
            """,
            (int(plan_id), int(driver_user_id), list(SAME_DAY_ACTIVE_STATUSES)),
        )
        ok = cur.fetchone() is not None
        cur.close()
        return ok
    finally:
        conn.close()
