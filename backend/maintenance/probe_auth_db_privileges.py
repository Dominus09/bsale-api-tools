"""Precheck de solo lectura: rol y privilegios del backend sobre tablas auth.

Uso (en el contenedor Coolify del API, o local con las mismas PG_*):
  python -m backend.maintenance.probe_auth_db_privileges

No imprime contraseñas ni cadenas de conexión.
"""

from __future__ import annotations

import sys

from backend.utils.bsale_token_env import load_dotenv_if_available


def main() -> int:
    load_dotenv_if_available()
    try:
        from backend.db import get_connection
    except Exception as exc:
        print(f"ERROR: no se pudo importar backend.db ({type(exc).__name__})")
        return 2

    # Solo has_*_privilege / catálogo: sin DML ni lectura de filas de negocio.
    checks = [
        ("schema.app", "USAGE", "SELECT has_schema_privilege(current_user, 'app', 'USAGE')"),
        ("bsale.users", "SELECT", "SELECT has_table_privilege(current_user, 'bsale.users', 'SELECT')"),
        ("bsale.users", "INSERT", "SELECT has_table_privilege(current_user, 'bsale.users', 'INSERT')"),
        ("bsale.users", "UPDATE", "SELECT has_table_privilege(current_user, 'bsale.users', 'UPDATE')"),
        ("app.user_companies", "SELECT", "SELECT has_table_privilege(current_user, 'app.user_companies', 'SELECT')"),
        ("app.user_companies", "INSERT", "SELECT has_table_privilege(current_user, 'app.user_companies', 'INSERT')"),
        ("app.user_companies", "UPDATE", "SELECT has_table_privilege(current_user, 'app.user_companies', 'UPDATE')"),
        ("app.user_companies", "DELETE", "SELECT has_table_privilege(current_user, 'app.user_companies', 'DELETE')"),
        ("app.auth_events", "SELECT", "SELECT has_table_privilege(current_user, 'app.auth_events', 'SELECT')"),
        ("app.auth_events", "INSERT", "SELECT has_table_privilege(current_user, 'app.auth_events', 'INSERT')"),
        (
            "app.auth_password_tokens",
            "SELECT",
            "SELECT has_table_privilege(current_user, 'app.auth_password_tokens', 'SELECT')",
        ),
        (
            "app.auth_password_tokens",
            "INSERT",
            "SELECT has_table_privilege(current_user, 'app.auth_password_tokens', 'INSERT')",
        ),
        (
            "app.auth_password_tokens",
            "UPDATE",
            "SELECT has_table_privilege(current_user, 'app.auth_password_tokens', 'UPDATE')",
        ),
        (
            "app.auth_events_id_seq",
            "USAGE",
            "SELECT has_sequence_privilege(current_user, 'app.auth_events_id_seq', 'USAGE')",
        ),
        (
            "app.auth_password_tokens_id_seq",
            "USAGE",
            "SELECT has_sequence_privilege(current_user, 'app.auth_password_tokens_id_seq', 'USAGE')",
        ),
    ]

    try:
        conn = get_connection()
    except Exception as exc:
        print(f"ERROR: no hay conexión backend ({type(exc).__name__}). Defina PG_* / ejecute en Coolify.")
        return 2

    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT current_database(), current_user, session_user, "
            "COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false)"
        )
        db, user, session_user, is_super = cur.fetchone()
        print(f"database={db}")
        print(f"current_user={user}")
        print(f"session_user={session_user}")
        print(f"is_superuser={bool(is_super)}")
        print("--- privileges (individual) ---")
        all_ok = True
        for obj, priv, sql in checks:
            cur.execute(sql)
            ok = bool(cur.fetchone()[0])
            print(f"{obj}\t{priv}\t{'OK' if ok else 'MISSING'}")
            if not ok:
                all_ok = False
        cur.close()
        if bool(is_super):
            print(
                "WARNING: el rol del API es superusuario (privilegios excesivos). "
                "Planificar rol de aplicación con mínimos privilegios."
            )
        return 0 if all_ok else 1
    except Exception as exc:
        print(f"ERROR: consulta falló ({type(exc).__name__})")
        return 2
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
