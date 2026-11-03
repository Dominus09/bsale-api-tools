"""Asignaciones multiapp (apps / perfiles / oficinas) para administración de usuarios.

Escrituras atómicas vía el cursor de la transacción del caller.
Sin tablas 064–066: lecturas vacías; escrituras → 503 (nunca permisos inventados).
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException

from backend.auth.multiapp import APP_ERP, APP_PWA, KNOWN_APPS, OPERATIONAL_PROFILES

PROFILE_LABELS: dict[str, str] = {
    "vendedor": "Vendedor",
    "chofer": "Chofer",
    "bodega": "Bodega",
    "cajero": "Cajero",
    "reponedor": "Reponedor",
    "jefe_sucursal": "Jefe de sucursal",
    "supervisor": "Supervisor",
}

APP_LABELS: dict[str, str] = {
    APP_ERP: "ERP",
    APP_PWA: "PWA operativa",
}


def multiapp_tables_ready(cur) -> dict[str, bool]:
    from backend.services.user_admin.service import _table_exists

    return {
        "user_apps": _table_exists(cur, "app", "user_apps"),
        "user_operational_profiles": _table_exists(cur, "app", "user_operational_profiles"),
        "user_offices": _table_exists(cur, "app", "user_offices"),
    }


def require_multiapp_tables(cur, *, need_apps: bool, need_profiles: bool, need_offices: bool) -> None:
    ready = multiapp_tables_ready(cur)
    missing: list[str] = []
    if need_apps and not ready["user_apps"]:
        missing.append("app.user_apps (064)")
    if need_profiles and not ready["user_operational_profiles"]:
        missing.append("app.user_operational_profiles (065)")
    if need_offices and not ready["user_offices"]:
        missing.append("app.user_offices (066)")
    if missing:
        raise HTTPException(
            status_code=503,
            detail=(
                "Tablas multiapp no disponibles: "
                + ", ".join(missing)
                + ". Aplique las migraciones 064–066 antes de asignar apps, perfiles u oficinas."
            ),
        )


def normalize_apps(raw: list[str] | None) -> list[str] | None:
    if raw is None:
        return None
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        code = str(item or "").strip().lower()
        if not code:
            continue
        if code not in KNOWN_APPS:
            raise HTTPException(
                status_code=400,
                detail=f"Aplicación no válida: {item!r}. Use erp y/o pwa.",
            )
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def normalize_profiles(raw: list[str] | None) -> list[str] | None:
    if raw is None:
        return None
    out: list[str] = []
    seen: set[str] = set()
    for item in raw:
        code = str(item or "").strip().lower()
        if not code:
            continue
        if code not in OPERATIONAL_PROFILES:
            raise HTTPException(
                status_code=400,
                detail=f"Perfil operativo no válido: {item!r}",
            )
        if code not in seen:
            seen.add(code)
            out.append(code)
    return out


def normalize_office_scopes(
    raw: list[dict[str, Any]] | list[Any] | None,
) -> list[tuple[int, int]] | None:
    if raw is None:
        return None
    out: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for item in raw:
        if isinstance(item, dict):
            try:
                company_id = int(item.get("company_id"))
                office_id = int(item.get("office_id"))
            except (TypeError, ValueError):
                raise HTTPException(
                    status_code=400,
                    detail="Cada oficina requiere company_id y office_id numéricos",
                ) from None
        else:
            raise HTTPException(
                status_code=400,
                detail="office_scopes debe ser lista de {company_id, office_id}",
            )
        key = (company_id, office_id)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def validate_apps_profiles_consistency(apps: list[str], profiles: list[str]) -> None:
    if profiles and APP_PWA not in apps:
        raise HTTPException(
            status_code=400,
            detail="Los perfiles operativos requieren acceso PWA",
        )


def apps_catalog() -> dict[str, Any]:
    return {
        "items": [
            {"code": APP_ERP, "label": APP_LABELS[APP_ERP]},
            {"code": APP_PWA, "label": APP_LABELS[APP_PWA]},
        ]
    }


def profiles_catalog() -> dict[str, Any]:
    return {
        "items": [
            {"code": code, "label": PROFILE_LABELS[code]}
            for code in sorted(OPERATIONAL_PROFILES)
        ]
    }


def list_offices_catalog(*, company_id: int | None = None, company_ids: list[int] | None = None) -> list[dict[str, Any]]:
    from backend.services.user_admin.service import _column_exists, get_connection

    ids: list[int] = []
    if company_id is not None:
        ids.append(int(company_id))
    if company_ids:
        ids.extend(int(c) for c in company_ids)
    ids = sorted({c for c in ids if c})
    if not ids:
        raise HTTPException(
            status_code=400,
            detail="Indique company_id o company_ids para listar oficinas",
        )

    conn = get_connection()
    try:
        cur = conn.cursor()
        has_state = _column_exists(cur, "bsale", "offices", "state")
        state_sql = " AND o.state = 0" if has_state else ""
        cur.execute(
            f"""
            SELECT o.company_id, o.bsale_id, o.name
            FROM bsale.offices o
            WHERE o.company_id = ANY(%s)
            {state_sql}
            ORDER BY o.company_id, o.bsale_id
            """,
            (ids,),
        )
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "company_id": int(r[0]),
                "office_id": int(r[1]),
                "name": (r[2] or "").strip() or f"Oficina {r[1]}",
            }
            for r in rows
        ]
    finally:
        conn.close()


def _load_apps_map(cur, user_ids: list[int]) -> tuple[bool, dict[int, list[str]]]:
    from backend.services.user_admin.service import _table_exists

    out: dict[int, list[str]] = {uid: [] for uid in user_ids}
    if not user_ids or not _table_exists(cur, "app", "user_apps"):
        return False, out
    cur.execute(
        """
        SELECT user_id, app_code
        FROM app.user_apps
        WHERE user_id = ANY(%s) AND active IS TRUE
        ORDER BY app_code
        """,
        (user_ids,),
    )
    for uid, code in cur.fetchall():
        c = str(code)
        if c in KNOWN_APPS:
            out.setdefault(int(uid), []).append(c)
    return True, out


def _load_profiles_map(cur, user_ids: list[int]) -> tuple[bool, dict[int, list[str]]]:
    from backend.services.user_admin.service import _table_exists

    out: dict[int, list[str]] = {uid: [] for uid in user_ids}
    if not user_ids or not _table_exists(cur, "app", "user_operational_profiles"):
        return False, out
    cur.execute(
        """
        SELECT user_id, profile_code
        FROM app.user_operational_profiles
        WHERE user_id = ANY(%s) AND active IS TRUE
        ORDER BY profile_code
        """,
        (user_ids,),
    )
    for uid, code in cur.fetchall():
        c = str(code)
        if c in OPERATIONAL_PROFILES:
            out.setdefault(int(uid), []).append(c)
    return True, out


def _load_offices_map(cur, user_ids: list[int]) -> tuple[bool, dict[int, list[dict[str, Any]]]]:
    from backend.services.user_admin.service import _table_exists

    out: dict[int, list[dict[str, Any]]] = {uid: [] for uid in user_ids}
    if not user_ids or not _table_exists(cur, "app", "user_offices"):
        return False, out
    cur.execute(
        """
        SELECT uo.user_id, uo.company_id, uo.office_id, o.name
        FROM app.user_offices uo
        LEFT JOIN bsale.offices o
          ON o.company_id = uo.company_id AND o.bsale_id = uo.office_id
        WHERE uo.user_id = ANY(%s)
        ORDER BY uo.company_id, uo.office_id
        """,
        (user_ids,),
    )
    for uid, company_id, office_id, name in cur.fetchall():
        out.setdefault(int(uid), []).append(
            {
                "company_id": int(company_id),
                "office_id": int(office_id),
                "name": (name or "").strip() or None,
            }
        )
    return True, out


def enrich_user_row(
    row: dict[str, Any],
    *,
    apps: list[str],
    profiles: list[str],
    offices: list[dict[str, Any]],
    tables_ready: bool,
) -> dict[str, Any]:
    """Añade campos multiapp + flags de lectura para la UI."""
    out = dict(row)
    out["apps"] = list(apps)
    out["profiles"] = list(profiles)
    out["offices"] = list(offices)
    out["multiapp_ready"] = tables_ready
    # Sin filas en user_apps con tablas listas = ERP legacy en runtime.
    out["apps_legacy_erp"] = bool(tables_ready and not apps)
    out["has_erp_access"] = (not tables_ready) or (not apps) or (APP_ERP in apps)
    out["has_pwa_access"] = APP_PWA in apps
    return out


def set_user_apps(cur, user_id: int, apps: list[str]) -> None:
    require_multiapp_tables(cur, need_apps=True, need_profiles=False, need_offices=False)
    unique = sorted(set(apps))
    cur.execute("DELETE FROM app.user_apps WHERE user_id = %s", (user_id,))
    for code in unique:
        cur.execute(
            """
            INSERT INTO app.user_apps (user_id, app_code, active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (user_id, app_code) DO UPDATE SET active = TRUE
            """,
            (user_id, code),
        )


def set_user_profiles(cur, user_id: int, profiles: list[str]) -> None:
    require_multiapp_tables(cur, need_apps=False, need_profiles=True, need_offices=False)
    unique = sorted(set(profiles))
    cur.execute(
        "DELETE FROM app.user_operational_profiles WHERE user_id = %s",
        (user_id,),
    )
    for code in unique:
        cur.execute(
            """
            INSERT INTO app.user_operational_profiles (user_id, profile_code, active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (user_id, profile_code) DO UPDATE SET active = TRUE
            """,
            (user_id, code),
        )


def set_user_offices(
    cur,
    user_id: int,
    offices: list[tuple[int, int]],
    *,
    allowed_company_ids: list[int] | None,
) -> None:
    """Persiste oficinas. ``allowed_company_ids`` = empresas del usuario (None = sin filtro de lista)."""
    require_multiapp_tables(cur, need_apps=False, need_profiles=False, need_offices=True)
    unique = sorted(set(offices))
    if allowed_company_ids is not None:
        allowed = {int(c) for c in allowed_company_ids}
        bad = [(c, o) for c, o in unique if c not in allowed]
        if bad:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Oficinas fuera de las empresas autorizadas del usuario: "
                    + ", ".join(f"company={c}/office={o}" for c, o in bad)
                ),
            )
    missing: list[tuple[int, int]] = []
    for company_id, office_id in unique:
        cur.execute(
            """
            SELECT 1 FROM bsale.offices
            WHERE company_id = %s AND bsale_id = %s
            LIMIT 1
            """,
            (company_id, office_id),
        )
        if not cur.fetchone():
            missing.append((company_id, office_id))
    if missing:
        raise HTTPException(
            status_code=400,
            detail=(
                "Oficina inexistente o no pertenece a la empresa indicada: "
                + ", ".join(f"company={c}/office={o}" for c, o in missing)
            ),
        )
    cur.execute("DELETE FROM app.user_offices WHERE user_id = %s", (user_id,))
    for company_id, office_id in unique:
        cur.execute(
            """
            INSERT INTO app.user_offices (user_id, company_id, office_id)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (user_id, company_id, office_id),
        )


def apply_multiapp_writes(
    cur,
    *,
    user_id: int,
    apps: list[str] | None,
    profiles: list[str] | None,
    office_scopes: list[tuple[int, int]] | None,
    company_ids_for_offices: list[int] | None,
) -> dict[str, Any]:
    """Aplica solo los campos no-None. Devuelve resumen de lo escrito para auditoría."""
    written: dict[str, Any] = {}
    effective_apps = apps
    if apps is not None and profiles is not None:
        validate_apps_profiles_consistency(apps, profiles)
    elif profiles is not None and apps is None:
        # Al editar solo perfiles, exigir PWA ya asignada o fallar.
        ready, apps_map = _load_apps_map(cur, [user_id])
        current = apps_map.get(user_id, []) if ready else []
        if profiles and APP_PWA not in current:
            raise HTTPException(
                status_code=400,
                detail="Asigne acceso PWA antes de perfiles operativos",
            )
    if apps is not None:
        set_user_apps(cur, user_id, apps)
        written["apps"] = apps
        effective_apps = apps
        # Si se quita PWA, limpiar perfiles automáticamente para no dejar huérfanos.
        if APP_PWA not in apps and profiles is None:
            ready, _ = _load_profiles_map(cur, [user_id])
            if ready:
                set_user_profiles(cur, user_id, [])
                written["profiles"] = []
    if profiles is not None:
        if effective_apps is not None:
            validate_apps_profiles_consistency(effective_apps, profiles)
        set_user_profiles(cur, user_id, profiles)
        written["profiles"] = profiles
    if office_scopes is not None:
        set_user_offices(
            cur,
            user_id,
            office_scopes,
            allowed_company_ids=company_ids_for_offices,
        )
        written["offices"] = [
            {"company_id": c, "office_id": o} for c, o in office_scopes
        ]
    return written
