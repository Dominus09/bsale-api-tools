"""Control de Góndolas: prueba integrada del circuito completo (opt-in).

Ejecuta, en UNA SOLA TRANSACCIÓN que termina en ROLLBACK:
crear recorrido → escanear un SKU → detectar el duplicado → cerrar → reposición.

No deja recorridos persistentes ni modifica stock, documentos o sincronizaciones:
todas las lecturas RAW son SELECT y las escrituras van a `app.gondola_*` dentro de
la transacción revertida.

Para ejecutarla se requiere un entorno con las variables PG_* del backend y la
migración `001_gondola_control.sql` ya aplicada:

    GONDOLAS_INTEGRATION=1 python -m pytest backend/tests/test_gondolas_integration.py -q

Piloto: empresa 3 (La Quillotana SPA), oficina 4 (Quillotana I), tipo VINO (35).
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    os.getenv("GONDOLAS_INTEGRATION") != "1",
    reason="Prueba integrada opt-in: defina GONDOLAS_INTEGRATION=1 con PG_* configuradas",
)

COMPANY_ID = 3
OFFICE_ID = 4
PRODUCT_TYPE_ID = 35


def test_circuito_completo_con_rollback():
    from backend.db import get_connection
    from backend.services.analytics.validate_distribuidora_source import (
        make_psycopg_rw_executor,
        open_readwrite_connection,
    )
    from backend.services.analytics_v2.access import CompanyScope
    from backend.services.gondolas.repository import GondolasRepository
    from backend.services.gondolas.service import GondolasService

    # GARANTÍA DE AISLAMIENTO: se usa UNA sola conexión y UNA sola transacción.
    # El servicio se construye directamente con el repositorio ligado a esta
    # conexión (no se usan los runners `run_gondola_*`, que abrirían conexiones
    # propias y confirmarían). Ningún método del servicio hace commit: el único
    # cierre posible es el ROLLBACK del bloque finally. Las secuencias pueden
    # avanzar (efecto normal de PostgreSQL), pero ningún registro persiste.
    conn = open_readwrite_connection(get_connection)
    try:
        executor = make_psycopg_rw_executor(conn, statement_timeout_seconds=30, lock_timeout="5s")
        repo = GondolasRepository(executor)
        assert repo.tables_ready(), "Falta aplicar backend/sql/gondolas/001_gondola_control.sql"

        # La FK de integridad de reposición debe existir y estar validada.
        fks = executor(
            "SELECT conname, convalidated FROM pg_constraint "
            "WHERE conrelid = 'app.gondola_replenishment'::regclass AND contype = 'f'",
            (),
        )
        by_name = {r["conname"]: r["convalidated"] for r in fks}
        assert by_name.get("fk_gondola_replenishment_item") is True

        service = GondolasService(
            repo,
            CompanyScope(company_id=COMPANY_ID, company_name="La Quillotana SPA"),
            user={"email": "integracion@gondolas.local", "role": "operativo"},
        )

        # 1) Crear recorrido: congela el universo con stock positivo del piloto.
        session = service.create_session(OFFICE_ID, PRODUCT_TYPE_ID)
        session_id = int(session["id"])
        total = int(session["total_sku"])
        assert total > 0, "El piloto debe tener SKU con stock positivo"
        assert session["stock_snapshot_at"], "La sesión debe entregar stock_snapshot_at"
        assert session["found_count"] == 0
        assert session["pending_count"] == total

        items = session["items"]
        assert len(items) == total
        target = next((i for i in items if i["bar_code"]), None)
        assert target is not None, "El SKU a escanear debe tener código de barras"

        # 2) Escanear el SKU: encontrado una sola vez.
        first = service.register_scan(session_id, target["bar_code"], device="integracion")
        assert first["result"] == "encontrado"
        assert first["progress"]["found_count"] == 1

        # 3) Reescanear: duplicado, sin nuevo hallazgo.
        second = service.register_scan(session_id, target["bar_code"], device="integracion")
        assert second["result"] == "duplicado"
        assert second["progress"]["found_count"] == 1

        # 4) Cerrar: genera la reposición con el resto del universo.
        closed = service.close_session(session_id)
        assert closed["status"] == "cerrado"
        assert closed["replenishment_created"] == total - 1
        replenishment = closed["replenishment"]
        assert len(replenishment) == total - 1
        assert target["variant_id"] not in {r["variant_id"] for r in replenishment}
        assert all(r["status"] == "pendiente_buscar" for r in replenishment)
    finally:
        # Nada de esta prueba persiste: la transacción completa se revierte y se
        # comprueba, ya fuera de ella, que no quedó ningún recorrido del actor
        # de integración.
        try:
            conn.rollback()
            executor = make_psycopg_rw_executor(conn, statement_timeout_seconds=10, lock_timeout="3s")
            leftovers = executor(
                "SELECT count(*) AS n FROM app.gondola_sessions WHERE created_by = %s",
                ("integracion@gondolas.local",),
            )
            assert int(leftovers[0]["n"]) == 0, "El ROLLBACK dejó recorridos persistentes"
            conn.rollback()
        finally:
            conn.close()
