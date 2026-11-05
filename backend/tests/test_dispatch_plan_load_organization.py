"""Organización anticipada OC→grupo: reglas puras + Postgres aislado."""

from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    LoadOrganizationConflictError,
    organization_completeness,
    validate_client_group_coherence,
)

DSN = (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()

pytestmark_pg = pytest.mark.skipif(
    not DSN,
    reason=(
        "Define SIMPLE_PLANNING_TEST_DSN a un Postgres de prueba aislado "
        "(nunca analytics) para validar persistencia/concurrencia."
    ),
)


def test_client_coherence_same_group_ok():
    orders = [
        {"oc_document_id": 1, "client_id": 10},
        {"oc_document_id": 2, "client_id": 10},
        {"oc_document_id": 3, "client_id": 11},
    ]
    assign = {1: 100, 2: 100, 3: 200}
    assert validate_client_group_coherence(orders, assign) == []


def test_client_coherence_split_rejected():
    orders = [
        {"oc_document_id": 1, "client_id": 10},
        {"oc_document_id": 2, "client_id": 10},
    ]
    assign = {1: 100, 2: 200}
    errs = validate_client_group_coherence(orders, assign)
    assert len(errs) == 1
    assert "Cliente 10" in errs[0]


def test_completeness_incomplete_not_organized():
    orders = [{"oc_document_id": i, "client_id": i} for i in range(1, 19)]
    assign = {i: 1 for i in range(1, 10)}
    c = organization_completeness(orders, assign)
    assert c["total_orders"] == 18
    assert c["assigned_orders"] == 9
    assert c["is_complete"] is False
    assert len(c["missing"]) == 9


def test_completeness_full():
    orders = [{"oc_document_id": i} for i in range(1, 4)]
    assign = {1: 10, 2: 10, 3: 11}
    assert organization_completeness(orders, assign)["is_complete"] is True


def _connect():
    import psycopg2

    return psycopg2.connect(DSN)


def _apply_schema(conn):
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_order_events CASCADE")
    cur.execute(
        "DROP TABLE IF EXISTS distribuidora.dispatch_plan_order_load_assignments CASCADE"
    )
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_orders CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_load_batches CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan CASCADE")
    cur.execute("DROP TABLE IF EXISTS distribuidora.trucks CASCADE")
    # Stub mínimo: get_plan_by_id hace LEFT JOIN a trucks.
    cur.execute(
        """
        CREATE TABLE distribuidora.trucks (
            id BIGSERIAL PRIMARY KEY,
            name TEXT,
            plate TEXT,
            max_weight_kg NUMERIC
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan (
            id BIGSERIAL PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'planned',
            planning_code TEXT,
            truck_id BIGINT REFERENCES distribuidora.trucks(id),
            load_organization_status TEXT NOT NULL DEFAULT 'legacy'
                CHECK (load_organization_status IN ('legacy', 'draft', 'organized')),
            load_organization_version INTEGER NOT NULL DEFAULT 0,
            load_organization_updated_at TIMESTAMPTZ
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_load_batches (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL
                REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            sort_order INTEGER NOT NULL DEFAULT 0,
            name TEXT NOT NULL,
            description TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX uq_dispatch_plan_load_batches_plan_id
            ON distribuidora.dispatch_plan_load_batches (dispatch_plan_id, id)
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_orders (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL
                REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            oc_document_id BIGINT NOT NULL,
            oc_number BIGINT,
            client_id INTEGER,
            client_name TEXT,
            oc_total_amount NUMERIC(18,2),
            route_order INTEGER,
            peso_total_kg NUMERIC(18,3),
            cantidad_productos INTEGER,
            cantidad_unidades NUMERIC(18,2),
            cantidad_cajas NUMERIC(18,2),
            productos_sin_peso INTEGER,
            cobertura_logistica NUMERIC(8,2),
            UNIQUE (dispatch_plan_id, oc_document_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_load_assignments (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT NOT NULL
                REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
            oc_document_id BIGINT NOT NULL,
            load_batch_id BIGINT NOT NULL,
            client_id INTEGER,
            assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            assigned_by TEXT,
            UNIQUE (dispatch_plan_id, oc_document_id),
            FOREIGN KEY (dispatch_plan_id, load_batch_id)
                REFERENCES distribuidora.dispatch_plan_load_batches (dispatch_plan_id, id)
                ON DELETE RESTRICT,
            FOREIGN KEY (dispatch_plan_id, oc_document_id)
                REFERENCES distribuidora.dispatch_plan_orders (dispatch_plan_id, oc_document_id)
                ON DELETE CASCADE
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE distribuidora.dispatch_plan_order_events (
            id BIGSERIAL PRIMARY KEY,
            dispatch_plan_id BIGINT,
            action TEXT,
            user_name TEXT,
            payload JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )
    conn.commit()
    cur.close()


def _seed_plan_18_clients(conn) -> tuple[int, list[int]]:
    """Plan sintético: 18 clientes, 20 OC (2 clientes con 2 OC), 3 grupos."""
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan(status, planning_code) "
        "VALUES ('planned', 'PLAN-SYN-F') RETURNING id"
    )
    plan_id = int(cur.fetchone()[0])
    batch_ids: list[int] = []
    for i, name in enumerate(["Grupo 1", "Grupo 2", "Grupo 3"], start=1):
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_load_batches
                (dispatch_plan_id, sort_order, name)
            VALUES (%s, %s, %s) RETURNING id
            """,
            (plan_id, i, name),
        )
        batch_ids.append(int(cur.fetchone()[0]))

    # 16 clientes con 1 OC + 2 clientes con 2 OC = 20 OC / 18 clientes
    oc = 1000
    for client_id in range(1, 17):
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders
                (dispatch_plan_id, oc_document_id, oc_number, client_id, client_name, route_order)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (plan_id, oc, oc, client_id, f"Cliente {client_id}", client_id),
        )
        oc += 1
    for client_id in (17, 18):
        for _ in range(2):
            cur.execute(
                """
                INSERT INTO distribuidora.dispatch_plan_orders
                    (dispatch_plan_id, oc_document_id, oc_number, client_id, client_name, route_order)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (plan_id, oc, oc, client_id, f"Cliente {client_id}", client_id),
            )
            oc += 1
    # Segundo plan para probar rechazo cross-plan
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan(status, planning_code) "
        "VALUES ('planned', 'PLAN-OTHER') RETURNING id"
    )
    other_id = int(cur.fetchone()[0])
    cur.execute(
        """
        INSERT INTO distribuidora.dispatch_plan_load_batches
            (dispatch_plan_id, sort_order, name)
        VALUES (%s, 1, 'Ajeno') RETURNING id
        """,
        (other_id,),
    )
    foreign_batch = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    return plan_id, batch_ids + [foreign_batch]


@pytestmark_pg
def test_pg_save_reload_and_client_default(monkeypatch):
    conn = _connect()
    try:
        _apply_schema(conn)
        plan_id, batch_ids = _seed_plan_18_clients(conn)
        g1, g2, g3, foreign = batch_ids

        # A–C: lotes creados en plan sintético sin facturas ni picking.
        cur = conn.cursor()
        cur.execute(
            "SELECT to_regclass('distribuidora.dispatch_plan_pickings') IS NOT NULL"
        )
        # En este esquema aislado no existe picking: los 3 grupos bastan.
        assert cur.fetchone()[0] is False
        cur.execute(
            "SELECT COUNT(*) FROM distribuidora.dispatch_plan_load_batches "
            "WHERE dispatch_plan_id=%s",
            (plan_id,),
        )
        assert int(cur.fetchone()[0]) == 3
        cur.close()

        from backend.services.distribuidora import (
            dispatch_plan_load_organization_service as svc,
        )

        monkeypatch.setattr(svc, "get_connection", _connect)

        # Agrupar: clientes 1-6 → G1, 7-12 → G2, 13-18 → G3 (manteniendo multi-OC juntos)
        cur = conn.cursor()
        cur.execute(
            "SELECT oc_document_id, client_id FROM distribuidora.dispatch_plan_orders "
            "WHERE dispatch_plan_id=%s ORDER BY client_id, oc_document_id",
            (plan_id,),
        )
        rows = cur.fetchall()
        cur.close()

        def group_for_client(cid: int) -> int:
            if cid <= 6:
                return g1
            if cid <= 12:
                return g2
            return g3

        assignments = [
            {"oc_document_id": int(oc), "load_batch_id": group_for_client(int(cid))}
            for oc, cid in rows
        ]

        out = svc.save_load_organization(
            plan_id,
            assignments=assignments,
            organization_version=0,
            mark_organized=True,
            user={"email": "ops@test.local"},
        )
        assert out["load_organization_status"] == "organized"
        assert out["completeness"]["is_complete"] is True
        assert out["load_organization_version"] == 1

        again = svc.get_load_organization(plan_id)
        assert again["load_organization_version"] == 1
        assert len(again["assignments"]) == 20

        # OC no puede ir a grupo de otro plan
        with pytest.raises(ValueError, match="no pertenece a este despacho"):
            svc.save_load_organization(
                plan_id,
                assignments=[
                    {"oc_document_id": int(rows[0][0]), "load_batch_id": foreign},
                ],
                organization_version=1,
            )

        # Incompleto no se marca organized
        with pytest.raises(ValueError, match="No se puede marcar como organized"):
            svc.save_load_organization(
                plan_id,
                assignments=assignments[:5],
                organization_version=1,
                mark_organized=True,
            )

        # Split de cliente rechazado por defecto
        oc_a = [int(oc) for oc, cid in rows if int(cid) == 17]
        assert len(oc_a) == 2
        bad = [
            {"oc_document_id": oc_a[0], "load_batch_id": g1},
            {"oc_document_id": oc_a[1], "load_batch_id": g2},
        ] + [
            {"oc_document_id": int(oc), "load_batch_id": group_for_client(int(cid))}
            for oc, cid in rows
            if int(cid) != 17
        ]
        with pytest.raises(ValueError, match="Cliente 17"):
            svc.save_load_organization(
                plan_id,
                assignments=bad,
                organization_version=1,
            )
    finally:
        conn.close()


@pytestmark_pg
def test_pg_unique_oc_and_concurrency(monkeypatch):
    conn = _connect()
    try:
        _apply_schema(conn)
        plan_id, batch_ids = _seed_plan_18_clients(conn)
        g1 = batch_ids[0]

        from backend.services.distribuidora import (
            dispatch_plan_load_organization_service as svc,
        )

        monkeypatch.setattr(svc, "get_connection", _connect)

        cur = conn.cursor()
        cur.execute(
            "SELECT oc_document_id FROM distribuidora.dispatch_plan_orders "
            "WHERE dispatch_plan_id=%s ORDER BY oc_document_id",
            (plan_id,),
        )
        ocs = [int(r[0]) for r in cur.fetchall()]
        cur.close()
        base = [{"oc_document_id": oc, "load_batch_id": g1} for oc in ocs]

        svc.save_load_organization(
            plan_id, assignments=base, organization_version=0, mark_organized=True
        )

        # Duplicado en payload
        with pytest.raises(ValueError, match="duplicada"):
            svc.save_load_organization(
                plan_id,
                assignments=base + [base[0]],
                organization_version=1,
            )

        results: list[str] = []

        def worker(ver: int, tag: str):
            try:
                svc.save_load_organization(
                    plan_id,
                    assignments=base,
                    organization_version=ver,
                    mark_organized=True,
                    user={"email": tag},
                )
                results.append("ok")
            except LoadOrganizationConflictError:
                results.append("conflict")
            except Exception as e:  # noqa: BLE001
                results.append(f"err:{e}")

        # Ambos intentan version=1; solo uno debe ganar
        with ThreadPoolExecutor(max_workers=2) as ex:
            futs = [ex.submit(worker, 1, "a"), ex.submit(worker, 1, "b")]
            for f in as_completed(futs):
                f.result()
        assert results.count("ok") == 1
        assert results.count("conflict") == 1

        final = svc.get_load_organization(plan_id)
        assert final["load_organization_version"] == 2
        assert final["load_organization_status"] == "organized"
    finally:
        conn.close()


@pytestmark_pg
def test_pg_legacy_plan_untouched_until_opt_in(monkeypatch):
    conn = _connect()
    try:
        _apply_schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO distribuidora.dispatch_plan(status, planning_code) "
            "VALUES ('picking_generated', 'PLAN-LEGACY') RETURNING id"
        )
        plan_id = int(cur.fetchone()[0])
        # Sin grupos creados automáticamente
        cur.execute(
            "SELECT COUNT(*) FROM distribuidora.dispatch_plan_load_batches "
            "WHERE dispatch_plan_id=%s",
            (plan_id,),
        )
        assert int(cur.fetchone()[0]) == 0
        conn.commit()
        cur.close()

        from backend.services.distribuidora import (
            dispatch_plan_load_organization_service as svc,
        )

        monkeypatch.setattr(svc, "get_connection", _connect)

        meta = svc.get_load_organization(plan_id)
        assert meta["load_organization_status"] == "legacy"
        assert meta["completeness"]["is_complete"] is False
        assert meta["batches"] == []
        assert meta.get("clients") == []
    finally:
        conn.close()


def _seed_melinka_like_25_oc(conn) -> tuple[int, list[int]]:
    """25 OC / 18 clientes / 3 grupos (prioritario, pareja, consolidado).

    Distribución exacta: 1×3 + 2×2 + 3×2 + 12×1 = 25 OC; clientes 1+2+3+12 = 18.
    """
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO distribuidora.dispatch_plan(status, planning_code) "
        "VALUES ('planned', 'PLAN-MELINKA-SYN') RETURNING id"
    )
    plan_id = int(cur.fetchone()[0])
    roles = [
        ("Grupo 1 Hardy", "role=priority\nGran variedad"),
        ("Grupo 2 descarga", "role=priority"),
        ("Grupo 3 resto", "role=consolidated\nRuta restante"),
    ]
    batch_ids: list[int] = []
    for i, (name, desc) in enumerate(roles, start=1):
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_load_batches
                (dispatch_plan_id, sort_order, name, description)
            VALUES (%s, %s, %s, %s) RETURNING id
            """,
            (plan_id, i, name, desc),
        )
        batch_ids.append(int(cur.fetchone()[0]))

    oc = 70000

    def _ins(
        cid: int,
        name: str,
        *,
        amount: float,
        units: float,
        peso: float,
        cov: float | None,
    ) -> None:
        nonlocal oc
        cur.execute(
            """
            INSERT INTO distribuidora.dispatch_plan_orders
                (dispatch_plan_id, oc_document_id, oc_number, client_id, client_name,
                 route_order, oc_total_amount, cantidad_unidades, peso_total_kg,
                 cobertura_logistica)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (plan_id, oc, oc, cid, name, cid, amount, units, peso, cov),
        )
        oc += 1

    for i in range(3):
        _ins(
            1,
            "Hardy Prioritario",
            amount=500000 + i * 1000,
            units=200 + i,
            peso=100 + i,
            cov=80.0 if i else 100.0,
        )
    for cid, name in ((2, "Cliente Compatible A"), (3, "Cliente Compatible B")):
        for _ in range(2):
            _ins(cid, name, amount=120000, units=40, peso=30, cov=100.0)
    for cid in range(4, 7):
        for _ in range(2):
            _ins(
                cid,
                f"Cliente mediano {cid}",
                amount=50000,
                units=20,
                peso=15,
                cov=60.0,
            )
    for cid in range(7, 19):
        _ins(
            cid,
            f"Cliente pequeño con nombre muy largo para UI {cid}",
            amount=25000,
            units=10,
            peso=5,
            cov=100.0 if cid % 2 == 0 else None,
        )

    cur.execute(
        "SELECT COUNT(*), COUNT(DISTINCT client_id) FROM distribuidora.dispatch_plan_orders "
        "WHERE dispatch_plan_id=%s",
        (plan_id,),
    )
    n_oc, n_cli = cur.fetchone()
    conn.commit()
    cur.close()
    assert int(n_oc) == 25
    assert int(n_cli) == 18
    return plan_id, batch_ids


@pytestmark_pg
def test_pg_melinka_manual_organization_flow(monkeypatch):
    conn = _connect()
    try:
        _apply_schema(conn)
        plan_id, batch_ids = _seed_melinka_like_25_oc(conn)
        g1, g2, g3 = batch_ids

        from backend.services.distribuidora import (
            dispatch_plan_load_batch_service as batch_svc,
            dispatch_plan_load_organization_service as svc,
        )

        monkeypatch.setattr(svc, "get_connection", _connect)
        monkeypatch.setattr(batch_svc, "get_connection", _connect)

        cur = conn.cursor()
        cur.execute(
            "SELECT oc_document_id, client_id FROM distribuidora.dispatch_plan_orders "
            "WHERE dispatch_plan_id=%s ORDER BY client_id, oc_document_id",
            (plan_id,),
        )
        rows = [(int(o), int(c)) for o, c in cur.fetchall()]
        cur.close()

        by_client: dict[int, list[int]] = {}
        for oc, cid in rows:
            by_client.setdefault(cid, []).append(oc)

        # Manual: cliente 1 → G1; clientes 2-3 → G2; resto → G3 (sin auto por venta)
        assignments = []
        for cid, ocs in by_client.items():
            if cid == 1:
                batch = g1
            elif cid in (2, 3):
                batch = g2
            else:
                batch = g3
            for oc in ocs:
                assignments.append({"oc_document_id": oc, "load_batch_id": batch})

        assert len(assignments) == 25

        draft = svc.save_load_organization(
            plan_id,
            assignments=assignments,
            organization_version=0,
            mark_organized=False,
            user={"email": "ops@test.local"},
        )
        assert draft["load_organization_status"] == "draft"
        assert draft["completeness"]["is_complete"] is True
        assert draft["load_organization_version"] == 1
        assert len(draft["clients"]) == 18
        assert len(draft["group_summaries"]) == 3

        # Roles sin filtrar cadenas técnicas al consumidor estructurado
        roles = {b["id"]: b.get("presentation_role") for b in draft["batches"]}
        assert roles[g1] == "priority"
        assert roles[g2] == "priority"
        assert roles[g3] == "consolidated"
        for b in draft["batches"]:
            assert "role=" not in (b.get("display_description") or "")

        again = svc.get_load_organization(plan_id)
        assert again["load_organization_version"] == 1
        assert {a["oc_document_id"] for a in again["assignments"]} == {
            a["oc_document_id"] for a in assignments
        }

        # Renombrar preserva rol
        renamed = batch_svc.update_load_batch(
            plan_id, g1, name="Grupo 1 exclusiv", description="Nota humana"
        )
        assert "role=priority" in (renamed.get("description") or "")
        meta2 = svc.get_load_organization(plan_id)
        b1 = next(b for b in meta2["batches"] if int(b["id"]) == g1)
        assert b1["presentation_role"] == "priority"
        assert b1["display_description"] == "Nota humana"

        # Completar OK
        done = svc.save_load_organization(
            plan_id,
            assignments=assignments,
            organization_version=meta2["load_organization_version"],
            mark_organized=True,
        )
        assert done["load_organization_status"] == "organized"

        # Incompleto no marca organized
        with pytest.raises(ValueError, match="No se puede marcar como organized"):
            svc.save_load_organization(
                plan_id,
                assignments=assignments[:10],
                organization_version=done["load_organization_version"],
                mark_organized=True,
            )

        # Eliminar grupo con OC bloqueado
        with pytest.raises(ValueError, match="OC asignadas"):
            batch_svc.delete_load_batch(plan_id, g3)

        # 409 versión
        with pytest.raises(svc.LoadOrganizationConflictError):
            svc.save_load_organization(
                plan_id,
                assignments=assignments,
                organization_version=0,
                mark_organized=True,
            )

        # Totales de venta del plan = suma clientes
        venta_clients = sum(float(c["venta_oc_clp"]) for c in done["clients"])
        venta_groups = sum(float(g["venta_oc_clp"]) for g in done["group_summaries"])
        assert abs(venta_clients - venta_groups) < 0.01
        assert sum(int(c["oc_count"]) for c in done["clients"]) == 25
    finally:
        conn.close()


@pytestmark_pg
def test_pg_schema_ready_false_without_053_columns(monkeypatch):
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS distribuidora")
        cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan_load_batches CASCADE")
        cur.execute("DROP TABLE IF EXISTS distribuidora.dispatch_plan CASCADE")
        cur.execute("DROP TABLE IF EXISTS distribuidora.trucks CASCADE")
        cur.execute(
            "CREATE TABLE distribuidora.trucks (id BIGSERIAL PRIMARY KEY, name TEXT)"
        )
        cur.execute(
            """
            CREATE TABLE distribuidora.dispatch_plan (
                id BIGSERIAL PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'planned',
                planning_code TEXT,
                truck_id BIGINT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE distribuidora.dispatch_plan_load_batches (
                id BIGSERIAL PRIMARY KEY,
                dispatch_plan_id BIGINT NOT NULL
                    REFERENCES distribuidora.dispatch_plan(id) ON DELETE CASCADE,
                sort_order INTEGER NOT NULL DEFAULT 0,
                name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            "INSERT INTO distribuidora.dispatch_plan(status, planning_code) "
            "VALUES ('picking_generated', 'LEGACY-NO-053') RETURNING id"
        )
        plan_id = int(cur.fetchone()[0])
        conn.commit()
        cur.close()

        from backend.services.distribuidora import (
            dispatch_plan_load_organization_service as svc,
        )

        monkeypatch.setattr(svc, "get_connection", _connect)
        out = svc.get_load_organization(plan_id)
        assert out["schema_ready"] is False
        assert out["load_organization_status"] == "legacy"
        assert "053" in (out.get("message") or "")
        with pytest.raises(ValueError, match="053"):
            svc.save_load_organization(
                plan_id, assignments=[], organization_version=0, mark_organized=True
            )
    finally:
        conn.close()
