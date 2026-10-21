"""Regresión del cierre de recorridos de Góndolas contra PostgreSQL real.

Reproduce el incidente de producción (21-oct-2026): cerrar la sesión con
cobertura parcial devolvía HTTP 500. La causa fue `CLOSE_ITEMS_SQL`, cuyo
`LEFT JOIN ... ON` referenciaba la tabla destino del `UPDATE` (`i`), algo
que PostgreSQL rechaza ("invalid reference to FROM-clause entry for table
i"): todo cierre fallaba y la transacción revertía, dejando la sesión en
curso con sus escaneos intactos.

Estas pruebas corren el servicio y el repositorio REALES sobre un
PostgreSQL embebido y desechable (pgserver), sembrado como el caso real:
empresa 3, oficina 4, tipo 35, 281 SKU, 218 encontrados, 63 pendientes.
Nunca tocan producción ni la sesión real. Sin pgserver, se omiten.
"""

from __future__ import annotations

import pathlib
import tempfile

import pytest

pgserver = pytest.importorskip("pgserver")
psycopg2 = pytest.importorskip("psycopg2")

TOTAL = 281
FOUND = 218


def _seed(cur) -> None:
    cur.execute(
        """
        CREATE SCHEMA bsale;
        CREATE SCHEMA bsale_raw;
        CREATE TABLE bsale.companies (company_id BIGINT PRIMARY KEY, name TEXT, active BOOLEAN);
        CREATE TABLE bsale.product_types (company_id BIGINT, bsale_id BIGINT, name TEXT);
        CREATE TABLE bsale_raw.offices (company_id BIGINT, bsale_id BIGINT, name TEXT, missing_since TIMESTAMPTZ);
        CREATE TABLE bsale_raw.stocks (
            company_id BIGINT, office_id BIGINT, variant_id BIGINT,
            quantity NUMERIC(14,3), quantity_available NUMERIC(14,3)
        );
        INSERT INTO bsale.companies VALUES (3, 'La Quillotana SPA', true);
        INSERT INTO bsale.product_types VALUES (3, 35, 'Vinos');
        INSERT INTO bsale_raw.offices VALUES (3, 4, 'Quillotana I', NULL);
        """
    )
    for name in ("001_gondola_control.sql", "002_gondola_integrity.sql"):
        sql = pathlib.Path(
            f"backend/sql/gondolas/{name}"
        ).read_text(encoding="utf-8")
        cur.execute(sql)
    cur.execute(
        """
        INSERT INTO app.gondola_sessions
            (id, company_id, office_id, product_type_id, status, created_by,
             stock_snapshot_at, total_sku_snapshot)
        VALUES (2, 3, 4, 35, 'en_curso', 'operario@quillotana.cl', now(), %s);
        SELECT setval('app.gondola_sessions_id_seq', 2);
        """,
        (TOTAL,),
    )
    for i in range(TOTAL):
        variant = 1000 + i
        found = i < FOUND
        cur.execute(
            """
            INSERT INTO app.gondola_session_items
                (session_id, company_id, variant_id, product_id, sku_code, bar_code,
                 product_name, variant_name, stock_snapshot, exhibition_status, found_at, found_by)
            VALUES (2, 3, %s, 10, %s, %s, %s, '750cc', 5, %s, %s, %s)
            """,
            (
                variant, f"SKU{i:04d}", f"780000{i:06d}", f"VINO {i:04d}",
                "encontrado" if found else "pendiente",
                "now()" if found else None,
                "operario@quillotana.cl" if found else None,
            ),
        )
        if i < 250:  # 31 variantes sin fila de stock: al cierre deben quedar en 0
            cur.execute("INSERT INTO bsale_raw.stocks VALUES (3, 4, %s, 7, 7)", (variant,))
        if found:
            cur.execute(
                """
                INSERT INTO app.gondola_scans (session_id, company_id, bar_code_raw, variant_id, result, scanned_by)
                VALUES (2, 3, %s, %s, 'encontrado', 'operario@quillotana.cl')
                """,
                (f"780000{i:06d}", variant),
            )


@pytest.fixture()
def lab():
    from backend.services.analytics.validate_distribuidora_source import (
        make_psycopg_rw_executor,
    )
    from backend.services.analytics_v2.access import CompanyScope
    from backend.services.gondolas.repository import GondolasRepository
    from backend.services.gondolas.service import GondolasService

    srv = pgserver.get_server(tempfile.mkdtemp(prefix="lq-gondola-test-"))
    conn = psycopg2.connect(srv.get_uri())
    cur = conn.cursor()
    _seed(cur)
    conn.commit()

    def make_service():
        executor = make_psycopg_rw_executor(
            conn, statement_timeout_seconds=20, lock_timeout="5s"
        )
        return GondolasService(
            GondolasRepository(executor),
            CompanyScope(company_id=3, company_name="La Quillotana SPA"),
            user={"email": "operario@quillotana.cl", "role": "operativo"},
        )

    yield conn, cur, make_service
    conn.rollback()
    conn.close()
    srv.cleanup()


def test_cierre_cobertura_parcial_218_de_281(lab):
    conn, _cur, make_service = lab
    service = make_service()

    before = service.get_session(2)
    assert before["status"] == "en_curso"
    assert (before["found_count"], before["pending_count"]) == (218, 63)

    closed = service.close_session(2)
    conn.commit()

    assert closed["status"] == "cerrado"
    assert closed["finished_by"] == "operario@quillotana.cl"
    assert closed["finished_at"]
    assert (closed["found_count"], closed["pending_count"]) == (218, 63)
    assert closed["total_sku"] == 281
    assert closed["replenishment_created"] == 63
    assert len(closed["replenishment"]) == 63


def test_cierre_congela_stock_y_preserva_escaneos(lab):
    conn, cur, make_service = lab
    make_service().close_session(2)
    conn.commit()

    cur.execute(
        "SELECT count(*) FROM app.gondola_session_items WHERE session_id=2 AND stock_at_close = 7"
    )
    assert cur.fetchone()[0] == 250  # con fila en stocks: cantidad actual
    cur.execute(
        "SELECT count(*) FROM app.gondola_session_items WHERE session_id=2 AND stock_at_close = 0"
    )
    assert cur.fetchone()[0] == 31  # sin fila: 0 explícito, nunca NULL tras cerrar
    cur.execute(
        "SELECT count(*) FROM app.gondola_session_items WHERE session_id=2 AND stock_at_close IS NULL"
    )
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT count(*) FROM app.gondola_scans WHERE session_id=2")
    assert cur.fetchone()[0] == 218  # bitácora histórica intacta
    cur.execute(
        "SELECT count(*) FROM app.gondola_session_items WHERE session_id=2 AND stock_snapshot = 5"
    )
    assert cur.fetchone()[0] == 281  # snapshot de inicio inalterado


def test_segundo_cierre_es_error_controlado_sin_duplicar(lab):
    from backend.services.gondolas.service import GondolasValidationError

    conn, cur, make_service = lab
    service = make_service()
    service.close_session(2)
    conn.commit()

    with pytest.raises(GondolasValidationError):
        service.close_session(2)
    conn.rollback()

    cur.execute("SELECT count(*) FROM app.gondola_replenishment WHERE session_id=2")
    assert cur.fetchone()[0] == 63  # sin duplicación de resultados


def test_error_intermedio_revierte_todo_el_cierre(lab, monkeypatch):
    from backend.services.gondolas.repository import GondolasRepository

    conn, cur, make_service = lab
    service = make_service()

    def boom(self, company_id, session_id):  # noqa: ARG001
        raise RuntimeError("falla simulada al generar la reposición")

    monkeypatch.setattr(GondolasRepository, "create_replenishment", boom)
    with pytest.raises(RuntimeError):
        service.close_session(2)
    conn.rollback()  # mismo rollback que ejecuta run_gondola_write

    cur.execute("SELECT status, finished_at FROM app.gondola_sessions WHERE id=2")
    assert cur.fetchone() == ("en_curso", None)
    cur.execute(
        "SELECT count(*) FROM app.gondola_session_items WHERE session_id=2 AND stock_at_close IS NOT NULL"
    )
    assert cur.fetchone()[0] == 0
    cur.execute("SELECT count(*) FROM app.gondola_replenishment WHERE session_id=2")
    assert cur.fetchone()[0] == 0


def test_recarga_desde_otro_servicio_tras_cerrar(lab):
    conn, _cur, make_service = lab
    make_service().close_session(2)
    conn.commit()

    # Equivalente a abrir el recorrido desde un segundo dispositivo.
    reloaded = make_service().get_session(2)
    assert reloaded["status"] == "cerrado"
    assert (reloaded["found_count"], reloaded["pending_count"]) == (218, 63)
    assert len(reloaded["replenishment"]) == 63
