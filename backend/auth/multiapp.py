"""Fundación multiaplicación (ERP + PWA) sobre ``bsale.users``.

Reglas de transición (default deny para PWA; ERP legacy preservado):

- Sin filas en ``app.user_apps`` → se considera acceso **ERP** (cuentas actuales).
- Con filas → solo ``app_code`` con ``active=true``.
- PWA **nunca** se concede implícitamente.
- ``AUTH_REQUIRE_USER_APPS=1``: sin fila erp activa = denegar ERP (cutover futuro).
- Perfiles / oficinas: default deny si se exige perfil u oficina.
- No migra ni toca ``bsale.vendedores_app``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Annotated, Any, Collection, Iterable

from fastapi import Header, HTTPException, Request

from backend.auth.jwt import decode_staff_token
from backend.auth.staff_session import (
    _table_exists,
    enrich_staff_payload,
    load_user_company_ids,
)
from backend.db import get_connection

logger = logging.getLogger(__name__)

BearerDep = Annotated[str | None, Header(alias="Authorization")]

APP_ERP = "erp"
APP_PWA = "pwa"
KNOWN_APPS = frozenset({APP_ERP, APP_PWA})

OPERATIONAL_PROFILES = frozenset(
    {
        "vendedor",
        "chofer",
        "bodega",
        "cajero",
        "reponedor",
        "jefe_sucursal",
        "supervisor",
    }
)

# Capacidades ERP acotadas (069). Independientes de perfiles PWA.
ERP_CAPABILITIES = frozenset({"simple_planning"})
CAP_SIMPLE_PLANNING = "simple_planning"


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def require_user_apps_cutover() -> bool:
    """Cutover: exige fila activa en user_apps para la app pedida (default off)."""
    return _env_flag("AUTH_REQUIRE_USER_APPS", default=False)


@dataclass(frozen=True, slots=True)
class MultiAppScope:
    """Alcance cargado desde BD (o vacío si tablas ausentes)."""

    apps: frozenset[str] = field(default_factory=frozenset)
    profiles: frozenset[str] = field(default_factory=frozenset)
    erp_capabilities: frozenset[str] = field(default_factory=frozenset)
    company_ids: tuple[int, ...] | None = None  # None = no restringir empresas
    offices: frozenset[tuple[int, int]] = field(default_factory=frozenset)
    # True si existía tabla user_apps y se leyeron filas (aunque active=false)
    apps_rows_present: bool = False
    tables_ready: bool = False
    # True si falló la lectura de alcance (BD caída / error). Nunca autorizar en positivo.
    load_failed: bool = False
    # Lectura de empresas/oficinas falló: fail-closed si el caller filtra por ellas.
    companies_unverified: bool = False
    offices_unverified: bool = False


def _user_id(user: dict[str, Any] | None) -> int | None:
    if not user:
        return None
    raw = user.get("id") if user.get("id") is not None else user.get("uid")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _load_user_company_ids_on_cur(cur, user_id: int) -> list[int] | None:
    """Lee ``app.user_companies`` en el cursor dado.

    None = sin filas (no restringir). Lista = empresas explícitas.
    """
    if not _table_exists(cur, "app", "user_companies"):
        return None
    cur.execute(
        """
        SELECT company_id
        FROM app.user_companies
        WHERE user_id = %s
        ORDER BY company_id
        """,
        (int(user_id),),
    )
    ids = [int(r[0]) for r in cur.fetchall()]
    return ids or None


def load_multiapp_scope(
    user_id: int | None,
    *,
    cur: Any | None = None,
    always_load_companies: bool = False,
) -> MultiAppScope:
    """Carga apps/perfiles/oficinas. Seguro si faltan tablas o no hay BD local.

    ``cur``: reutilizar cursor abierto (evita conexión anidada en listados).
    Si se reutiliza, se abre ``SAVEPOINT`` para no dejar la TX padre abortada
    cuando falle un SELECT (p. ej. permiso denegado en ``user_companies``).

    ``always_load_companies``: leer ``user_companies`` aunque AUTH_EMIT_* esté off
    (elegibilidad de choferes). Si la lectura falla: ``companies_unverified``
    (fail-closed al filtrar por empresa); no se omite el control.
    """
    if user_id is None:
        return MultiAppScope()
    owns_conn = cur is None
    conn = None
    local_cur = cur
    savepoint = "multiapp_scope_load" if not owns_conn else None
    try:
        if local_cur is None:
            conn = get_connection()
            local_cur = conn.cursor()
        assert local_cur is not None
        if savepoint:
            local_cur.execute(f"SAVEPOINT {savepoint}")
        has_apps = _table_exists(local_cur, "app", "user_apps")
        has_profiles = _table_exists(local_cur, "app", "user_operational_profiles")
        has_offices = _table_exists(local_cur, "app", "user_offices")
        has_caps = _table_exists(local_cur, "app", "user_erp_capabilities")
        tables_ready = has_apps  # mínimo para multiapp
        apps: set[str] = set()
        apps_rows_present = False
        if has_apps:
            local_cur.execute(
                """
                SELECT app_code, active
                FROM app.user_apps
                WHERE user_id = %s
                """,
                (int(user_id),),
            )
            rows = local_cur.fetchall()
            apps_rows_present = bool(rows)
            for code, active in rows:
                if active and str(code) in KNOWN_APPS:
                    apps.add(str(code))
        profiles: set[str] = set()
        if has_profiles:
            local_cur.execute(
                """
                SELECT profile_code
                FROM app.user_operational_profiles
                WHERE user_id = %s AND active IS TRUE
                """,
                (int(user_id),),
            )
            for (code,) in local_cur.fetchall():
                c = str(code)
                if c in OPERATIONAL_PROFILES:
                    profiles.add(c)
        erp_capabilities: set[str] = set()
        if has_caps:
            local_cur.execute(
                """
                SELECT capability_code
                FROM app.user_erp_capabilities
                WHERE user_id = %s AND active IS TRUE
                """,
                (int(user_id),),
            )
            for (code,) in local_cur.fetchall():
                c = str(code).strip().lower()
                if c in ERP_CAPABILITIES:
                    erp_capabilities.add(c)
        offices: set[tuple[int, int]] = set()
        offices_unverified = False
        if has_offices:
            try:
                local_cur.execute(
                    """
                    SELECT company_id, office_id
                    FROM app.user_offices
                    WHERE user_id = %s
                    """,
                    (int(user_id),),
                )
                for company_id, office_id in local_cur.fetchall():
                    offices.add((int(company_id), int(office_id)))
            except Exception:
                logger.warning(
                    "load_multiapp_scope offices unverified user_id=%s",
                    user_id,
                    exc_info=True,
                )
                if savepoint:
                    local_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                    local_cur.execute(f"SAVEPOINT {savepoint}")
                offices = set()
                offices_unverified = True
        company_ids: list[int] | None
        companies_unverified = False
        try:
            if always_load_companies:
                company_ids = _load_user_company_ids_on_cur(local_cur, int(user_id))
            else:
                company_ids = load_user_company_ids(int(user_id))
        except Exception:
            # No tumbar TX padre; fail-closed si el caller exige empresa.
            logger.warning(
                "load_multiapp_scope companies unverified user_id=%s",
                user_id,
                exc_info=True,
            )
            if savepoint:
                local_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                local_cur.execute(f"SAVEPOINT {savepoint}")
            if always_load_companies:
                company_ids = []
                companies_unverified = True
            else:
                company_ids = None
        if savepoint:
            local_cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            savepoint = None
        if owns_conn:
            local_cur.close()
        return MultiAppScope(
            apps=frozenset(apps),
            profiles=frozenset(profiles),
            erp_capabilities=frozenset(erp_capabilities),
            company_ids=tuple(company_ids) if company_ids is not None else None,
            offices=frozenset(offices),
            apps_rows_present=apps_rows_present,
            tables_ready=tables_ready,
            companies_unverified=companies_unverified,
            offices_unverified=offices_unverified,
        )
    except Exception:
        logger.exception("load_multiapp_scope failed user_id=%s", user_id)
        if savepoint and local_cur is not None:
            try:
                local_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                local_cur.execute(f"RELEASE SAVEPOINT {savepoint}")
            except Exception:
                logger.exception(
                    "load_multiapp_scope savepoint rollback failed user_id=%s",
                    user_id,
                )
        # Fail-closed: un error de BD no concede ERP ni PWA.
        return MultiAppScope(load_failed=True)
    finally:
        if conn is not None:
            conn.close()


def has_app_access(scope: MultiAppScope, app_code: str) -> bool:
    app = str(app_code or "").strip().lower()
    if app not in KNOWN_APPS:
        return False
    if scope.load_failed:
        return False
    if app == APP_PWA:
        # Default deny: solo fila explícita activa.
        return APP_PWA in scope.apps
    # ERP
    if not scope.tables_ready:
        # Pre-migración 064: comportamiento legacy (ERP ok).
        return True
    if not scope.apps_rows_present:
        # Sin filas: legacy ERP, salvo cutover.
        return not require_user_apps_cutover()
    return APP_ERP in scope.apps


def has_profile(scope: MultiAppScope, profile_code: str) -> bool:
    code = str(profile_code or "").strip().lower()
    return code in scope.profiles


def has_any_profile(scope: MultiAppScope, allowed: Collection[str]) -> bool:
    wanted = {str(p).strip().lower() for p in allowed}
    return bool(scope.profiles & wanted)


def has_erp_capability(scope: MultiAppScope, capability_code: str) -> bool:
    code = str(capability_code or "").strip().lower()
    return code in scope.erp_capabilities


def company_allowed(scope: MultiAppScope, company_id: int | None) -> bool:
    if company_id is None:
        return False
    if scope.company_ids is None:
        # Sin restricción de empresas (flag emit/require off o sin filas).
        return True
    return int(company_id) in scope.company_ids


def office_allowed(
    scope: MultiAppScope,
    *,
    company_id: int | None,
    office_id: int | None,
    require_assignment: bool = True,
) -> bool:
    if company_id is None or office_id is None:
        return False
    if not company_allowed(scope, company_id):
        return False
    key = (int(company_id), int(office_id))
    if scope.offices:
        return key in scope.offices
    # Sin oficinas asignadas: denegar si se exige asignación (PWA default).
    return not require_assignment


def assert_app_access(scope: MultiAppScope, app_code: str) -> None:
    if not has_app_access(scope, app_code):
        raise HTTPException(
            status_code=403,
            detail=f"Aplicación '{app_code}' no autorizada para este usuario",
        )


def assert_profiles(
    scope: MultiAppScope,
    allowed: Iterable[str],
    *,
    detail: str | None = None,
) -> None:
    if not has_any_profile(scope, list(allowed)):
        raise HTTPException(
            status_code=403,
            detail=detail or "Perfil operativo insuficiente",
        )


def assert_company(scope: MultiAppScope, company_id: int | None) -> None:
    if not company_allowed(scope, company_id):
        raise HTTPException(status_code=403, detail="Empresa no autorizada")


def assert_office(
    scope: MultiAppScope,
    *,
    company_id: int | None,
    office_id: int | None,
    require_assignment: bool = True,
) -> None:
    if not office_allowed(
        scope,
        company_id=company_id,
        office_id=office_id,
        require_assignment=require_assignment,
    ):
        raise HTTPException(status_code=403, detail="Oficina no autorizada")


def attach_multiapp_scope(user: dict[str, Any]) -> dict[str, Any]:
    """Enriquece el dict de usuario con alcance multiapp (idempotente)."""
    out = dict(user)
    if out.get("_multiapp_scope") is not None and isinstance(
        out.get("_multiapp_scope"), MultiAppScope
    ):
        return out
    scope = load_multiapp_scope(_user_id(out))
    out["_multiapp_scope"] = scope
    out["apps"] = sorted(scope.apps) if scope.apps_rows_present else (
        [APP_ERP] if has_app_access(scope, APP_ERP) else []
    )
    out["profiles"] = sorted(scope.profiles)
    out["erp_capabilities"] = sorted(scope.erp_capabilities)
    out["company_ids"] = list(scope.company_ids) if scope.company_ids is not None else None
    out["office_scopes"] = [
        {"company_id": c, "office_id": o} for c, o in sorted(scope.offices)
    ]
    out["has_erp_access"] = has_app_access(scope, APP_ERP)
    out["has_pwa_access"] = has_app_access(scope, APP_PWA)
    return out


def get_scope(user: dict[str, Any]) -> MultiAppScope:
    attached = user.get("_multiapp_scope")
    if isinstance(attached, MultiAppScope):
        return attached
    return attach_multiapp_scope(user)["_multiapp_scope"]


def require_authenticated_staff(authorization: BearerDep = None) -> dict[str, Any]:
    """JWT válido + enrich + alcance multiapp (sin exigir app concreta)."""
    user = enrich_staff_payload(decode_staff_token(authorization))
    return attach_multiapp_scope(user)


def require_erp_app(
    authorization: BearerDep = None,
    request: Request = None,
) -> dict[str, Any]:
    """Staff con acceso ERP. Rechaza cuentas PWA-only (con filas apps sin erp)."""
    user = require_authenticated_staff(authorization)
    scope = get_scope(user)
    if scope.load_failed:
        raise HTTPException(
            status_code=503,
            detail="No se pudo verificar autorización multiaplicación",
        )
    if not has_app_access(scope, APP_ERP):
        path = request.url.path if request is not None else "-"
        logger.warning(
            "[AUTH_MULTIAPP] deny erp user_id=%s path=%s apps=%s",
            user.get("id"),
            path,
            sorted(scope.apps),
        )
        raise HTTPException(
            status_code=403,
            detail="Acceso ERP no autorizado para esta cuenta",
        )
    return user


def require_pwa_app(
    authorization: BearerDep = None,
    request: Request = None,
) -> dict[str, Any]:
    """Staff con acceso PWA explícito (default deny)."""
    user = require_authenticated_staff(authorization)
    scope = get_scope(user)
    if scope.load_failed:
        raise HTTPException(
            status_code=503,
            detail="No se pudo verificar autorización multiaplicación",
        )
    if not has_app_access(scope, APP_PWA):
        path = request.url.path if request is not None else "-"
        logger.warning(
            "[AUTH_MULTIAPP] deny pwa user_id=%s path=%s",
            user.get("id"),
            path,
        )
        raise HTTPException(
            status_code=403,
            detail="Acceso PWA no autorizado para esta cuenta",
        )
    return user


def require_pwa_profiles(*allowed_profiles: str):
    """Factory Depends: PWA + al menos uno de los perfiles."""

    allowed = tuple(str(p).strip().lower() for p in allowed_profiles)

    def _dep(authorization: BearerDep = None, request: Request = None) -> dict[str, Any]:
        user = require_pwa_app(authorization, request)
        assert_profiles(get_scope(user), allowed)
        return user

    return _dep


def require_erp_or_pwa_app(
    authorization: BearerDep = None,
    request: Request = None,
) -> dict[str, Any]:
    """JWT válido con acceso ERP o PWA (p. ej. ``GET /auth/me``).

    No otorga ERP a cuentas PWA-only: solo permite leer el perfil.
    """
    user = require_authenticated_staff(authorization)
    scope = get_scope(user)
    if scope.load_failed:
        raise HTTPException(
            status_code=503,
            detail="No se pudo verificar autorización multiaplicación",
        )
    if has_app_access(scope, APP_ERP) or has_app_access(scope, APP_PWA):
        return user
    path = request.url.path if request is not None else "-"
    logger.warning(
        "[AUTH_MULTIAPP] deny erp_or_pwa user_id=%s path=%s apps=%s",
        user.get("id"),
        path,
        sorted(scope.apps),
    )
    raise HTTPException(
        status_code=403,
        detail="Sin acceso ERP ni PWA para esta cuenta",
    )


def scope_public_dict(user: dict[str, Any]) -> dict[str, Any]:
    """Fragmento seguro para ``/auth/me`` (sin objetos internos)."""
    enriched = attach_multiapp_scope(user)
    return {
        "apps": list(enriched.get("apps") or []),
        "profiles": list(enriched.get("profiles") or []),
        "erp_capabilities": list(enriched.get("erp_capabilities") or []),
        "company_ids": enriched.get("company_ids"),
        "office_scopes": list(enriched.get("office_scopes") or []),
        "has_erp_access": bool(enriched.get("has_erp_access")),
        "has_pwa_access": bool(enriched.get("has_pwa_access")),
    }
