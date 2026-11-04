"""Reproduce el 503 de available-drivers por TX abortada en cursor compartido.

Causa eed68b0: ``load_multiapp_scope(cur=...)`` + fallo en ``user_companies``
dejaba ``InFailedSqlTransaction`` en el cursor del listado; el siguiente SQL
escapaba como Exception genérica → HTTP 503 «No se pudieron listar choferes».
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

from backend.auth.multiapp import APP_PWA, MultiAppScope, load_multiapp_scope
from backend.services.distribuidora import dispatch_plan_driver_service as svc

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()


class _AbortingCompaniesCursor:
    """Simula cursor PG: SELECT user_companies → permission denied / abort."""

    def __init__(self):
        self.aborted = False
        self.queries: list[str] = []
        self._savepoints: list[str] = []
        self.description = [("app_code",), ("active",)]

    def execute(self, sql, params=None):
        text = " ".join(str(sql).split())
        self.queries.append(text)
        if self.aborted and "ROLLBACK TO SAVEPOINT" not in text.upper():
            raise RuntimeError(
                "current transaction is aborted, commands ignored until end of "
                "transaction block"
            )
        upper = text.upper()
        if upper.startswith("SAVEPOINT "):
            self._savepoints.append(text.split()[-1])
            return
        if "ROLLBACK TO SAVEPOINT" in upper:
            self.aborted = False
            return
        if upper.startswith("RELEASE SAVEPOINT"):
            return
        if "FROM APP.USER_COMPANIES" in upper or "FROM app.user_companies" in text:
            self.aborted = True
            raise PermissionError("permission denied for table user_companies")
        # information_schema / otras lecturas OK
        return

    def fetchall(self):
        if self.aborted:
            raise RuntimeError("current transaction is aborted")
        last = (self.queries[-1] if self.queries else "").upper()
        if "INFORMATION_SCHEMA.TABLES" in last:
            # fingir que existen tablas multiapp
            return [("x",)]
        if "USER_APPS" in last and "APP_CODE" in last:
            return [("pwa", True), ("erp", True)]
        if "USER_OPERATIONAL_PROFILES" in last:
            return [("chofer",)]
        if "USER_OFFICES" in last:
            return [(3, 1)]
        if "USER_ERP_CAPABILITIES" in last:
            return []
        return []

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None

    def close(self):
        return None


def test_shared_cursor_survives_user_companies_permission_denied(monkeypatch):
    """Tras fallo de empresas, el cursor padre debe seguir usable (SAVEPOINT)."""
    monkeypatch.setattr(
        "backend.auth.multiapp._table_exists",
        lambda cur, schema, table: True,
    )
    cur = _AbortingCompaniesCursor()
    scope = load_multiapp_scope(16, cur=cur, always_load_companies=True)
    assert scope.load_failed is False
    assert APP_PWA in scope.apps
    assert "chofer" in scope.profiles
    assert (3, 1) in scope.offices
    assert scope.company_ids is None  # degradado, no fatal

    # Cursor no abortado: otra sentencia debe funcionar.
    cur.execute("SELECT 1 FROM information_schema.tables WHERE table_schema = %s", ("app",))
    assert cur.fetchone() is not None


def test_list_available_drivers_does_not_503_when_scope_uses_own_connection(monkeypatch):
    """Listado captura DriverAssignError; scope en conexión propia no envenena cur."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur
    cur.description = [("id",), ("email",), ("nombre_usuario",), ("active",)]
    cur.fetchall.side_effect = [
        [(16, "chofer2@quillotana.cl", "Pablo chofer", True)],  # candidates
    ]
    monkeypatch.setattr(svc, "get_connection", lambda: conn)
    monkeypatch.setattr(
        svc,
        "_driver_scope",
        lambda _cur, _uid: MultiAppScope(
            tables_ready=True,
            apps_rows_present=True,
            apps=frozenset({APP_PWA}),
            profiles=frozenset({"chofer"}),
            company_ids=(3,),
            offices=frozenset({(3, 1)}),
        ),
    )
    # _load_user_row inside assert
    cur.fetchone.return_value = (16, "chofer2@quillotana.cl", "operativo", True, "Pablo chofer")

    payload = svc.list_available_drivers(company_id=3, office_id=1, include_diagnostics=False)
    assert payload["items"]
    assert payload["items"][0]["user_id"] == 16


@pytest.mark.skipif(
    not DSN,
    reason="Define SIMPLE_PLANNING_TEST_DSN a Postgres aislado (no analytics)",
)
def test_pg_isolated_scope_savepoint_and_list_sql():
    """Esquema mínimo app.* + bsale.users en Postgres de prueba."""
    import psycopg2

    conn = psycopg2.connect(DSN)
    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS app")
        cur.execute("CREATE SCHEMA IF NOT EXISTS bsale")
        cur.execute("DROP TABLE IF EXISTS app.user_offices CASCADE")
        cur.execute("DROP TABLE IF EXISTS app.user_companies CASCADE")
        cur.execute("DROP TABLE IF EXISTS app.user_operational_profiles CASCADE")
        cur.execute("DROP TABLE IF EXISTS app.user_apps CASCADE")
        cur.execute("DROP TABLE IF EXISTS app.user_erp_capabilities CASCADE")
        cur.execute("DROP TABLE IF EXISTS bsale.users CASCADE")
        cur.execute(
            """
            CREATE TABLE bsale.users (
              id int PRIMARY KEY,
              email text,
              role text,
              active boolean,
              nombre_usuario text
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE app.user_apps (
              user_id int, app_code text, active boolean,
              PRIMARY KEY (user_id, app_code)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE app.user_operational_profiles (
              user_id int, profile_code text, active boolean,
              PRIMARY KEY (user_id, profile_code)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE app.user_offices (
              user_id int, company_id int, office_id int,
              PRIMARY KEY (user_id, company_id, office_id)
            )
            """
        )
        # Sin GRANT a user_companies: no creamos la tabla → degrade limpio
        cur.execute(
            """
            INSERT INTO bsale.users (id, email, role, active, nombre_usuario)
            VALUES (16, 'chofer2@test.cl', 'operativo', true, 'Pablo')
            """
        )
        cur.execute(
            "INSERT INTO app.user_apps VALUES (16, 'pwa', true), (16, 'erp', true)"
        )
        cur.execute(
            "INSERT INTO app.user_operational_profiles VALUES (16, 'chofer', true)"
        )
        cur.execute("INSERT INTO app.user_offices VALUES (16, 3, 1)")
        conn.commit()

        scope = load_multiapp_scope(16, cur=cur, always_load_companies=True)
        assert scope.load_failed is False
        assert "chofer" in scope.profiles
        # Otra query en el mismo cursor debe vivir
        cur.execute("SELECT id FROM bsale.users WHERE id = 16")
        assert cur.fetchone()[0] == 16
    finally:
        conn.close()
