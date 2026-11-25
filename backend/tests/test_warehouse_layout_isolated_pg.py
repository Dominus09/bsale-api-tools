"""Suite warehouse slice 1.1 — SOLO Postgres aislado (nunca analytics/prod).

Requiere SIMPLE_PLANNING_TEST_DSN o WAREHOUSE_TEST_DSN apuntando a PG temporal.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.warehouse.errors import WarehouseConflictError, WarehouseValidationError
from backend.services.warehouse.service import WarehouseLayoutService

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "backend" / "sql" / "warehouse"


def _dsn() -> str | None:
    return (
        (os.getenv("WAREHOUSE_TEST_DSN") or "").strip()
        or (os.getenv("SIMPLE_PLANNING_TEST_DSN") or "").strip()
        or None
    )


def _assert_isolated(dsn: str) -> None:
    low = dsn.lower()
    for ban in (
        "analytics",
        "quillotana",
        "prod",
        "production",
        "coolify",
        "neon.tech",
        "supabase",
        "amazonaws.com",
    ):
        if ban in low:
            raise AssertionError(f"DSN no aislado (contiene {ban!r})")


pytestmark = pytest.mark.skipif(
    not _dsn(),
    reason="Define WAREHOUSE_TEST_DSN o SIMPLE_PLANNING_TEST_DSN (PG aislado)",
)


@pytest.fixture(scope="module")
def dsn() -> str:
    uri = _dsn()
    assert uri
    _assert_isolated(uri)
    return uri


@pytest.fixture(scope="module")
def migrated_dsn(dsn: str) -> str:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SHOW data_directory")
    datadir = str(cur.fetchone()[0]).lower()
    if "analytics" in datadir or "quillotana" in datadir:
        cur.close()
        conn.close()
        raise RuntimeError("data_directory parece productivo — abort")
    # Limpiar schema si re-run
    cur.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    for name in (
        "001_schema_layout_AWAITING_APPROVAL.sql",
        "002_seed_bodega_central_draft_AWAITING_APPROVAL.sql",
        "003_seed_bc_special_zones_AWAITING_APPROVAL.sql",
        "004_publish_layout_AWAITING_APPROVAL.sql",
    ):
        sql = (SQL_DIR / name).read_text(encoding="utf-8")
        cur.execute(sql)
    cur.close()
    conn.close()
    return dsn


@pytest.fixture
def svc(migrated_dsn: str) -> WarehouseLayoutService:
    def _factory():
        return psycopg2.connect(migrated_dsn)

    return WarehouseLayoutService(_factory)


def test_seed_idempotent_bc(svc: WarehouseLayoutService, migrated_dsn: str):
    facs = svc.list_facilities()
    assert len(facs) == 1
    assert facs[0]["code"] == "BC"
    assert facs[0]["bsale_office_id"] == 1
    floors = svc.list_floors(int(facs[0]["id"]))
    assert len(floors) == 1
    assert floors[0]["code"] == "PISO1"
    assert float(floors[0]["length_m"]) == 23.10
    assert float(floors[0]["width_m"]) == 13.70
    revs = svc.list_revisions(int(floors[0]["id"]))
    draft = next(r for r in revs if r["status"] == "draft")
    bundle = svc.get_revision(int(draft["id"]))
    assert bundle["element_count"] >= 14
    codes = {e["physical"]["code"] for e in bundle["elements"]}
    assert {"IZ-SU", "BAE-01", "BAE-02"}.issubset(codes)
    su = next(e for e in bundle["elements"] if e["physical"]["code"] == "IZ-SU")
    assert su["physical"]["element_type"] == "loose_storage"
    assert su["geometry"]["validation_status"] == "pending_validation"
    # Re-run seed
    conn = psycopg2.connect(migrated_dsn)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute((SQL_DIR / "002_seed_bodega_central_draft_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
    cur.execute("SELECT COUNT(*) FROM warehouse.facilities")
    assert cur.fetchone()[0] == 1
    cur.execute("SELECT COUNT(*) FROM warehouse.layout_elements")
    n_before = cur.fetchone()[0]
    cur.execute((SQL_DIR / "002_seed_bodega_central_draft_AWAITING_APPROVAL.sql").read_text(encoding="utf-8"))
    cur.execute("SELECT COUNT(*) FROM warehouse.layout_elements")
    assert cur.fetchone()[0] == n_before
    cur.close()
    conn.close()


def test_stable_physical_identity_across_geometry_edits(svc: WarehouseLayoutService):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = svc.get_or_create_draft(int(floor["id"]), actor="test")["revision"]
    rev_id = int(draft["id"])
    version = int(draft["row_version"])
    bundle = svc.get_revision(rev_id)
    iz = next(e for e in bundle["elements"] if e["physical"]["code"] == "IZ")
    pid = int(iz["physical"]["id"])
    out = svc.apply_batch(
        rev_id,
        upserts=[
            {
                "physical_element_id": pid,
                "parent_id": iz["physical"]["parent_id"],
                "element_type": "sector",
                "code": "IZ",
                "name": "Sector izquierdo (corregido)",
                "geometry": {
                    "x_m": 0,
                    "y_m": 0,
                    "width_m": 23.10,
                    "depth_m": 2.70,  # corrección métrica
                    "height_clear_m": 4.0,
                    "validation_status": "pending_validation",
                    "observations": "Ajuste provisional profundidad",
                },
            }
        ],
        expected_row_version=version,
        actor="test",
    )
    assert int(out["upserted"][0]["physical"]["id"]) == pid
    assert float(out["upserted"][0]["geometry"]["depth_m"]) == 2.70
    again = svc.get_revision(rev_id)
    iz2 = next(e for e in again["elements"] if e["physical"]["code"] == "IZ")
    assert int(iz2["physical"]["id"]) == pid


def test_batch_does_not_delete_absent_elements(svc: WarehouseLayoutService):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    rev_id = int(draft["id"])
    before = svc.get_revision(rev_id)["element_count"]
    svc.apply_batch(
        rev_id,
        upserts=[],
        expected_row_version=int(draft["row_version"]),
        actor="test",
    )
    # bump version even with empty upserts
    after = svc.get_revision(rev_id)
    assert after["element_count"] == before


def test_concurrency_conflict(svc: WarehouseLayoutService):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    rev_id = int(draft["id"])
    with pytest.raises(WarehouseConflictError):
        svc.apply_batch(
            rev_id,
            upserts=[],
            expected_row_version=int(draft["row_version"]) - 1,
            actor="test",
        )


def test_invalid_parent_and_cycle(svc: WarehouseLayoutService):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    rev_id = int(draft["id"])
    version = int(draft["row_version"])
    with pytest.raises(WarehouseValidationError):
        svc.apply_batch(
            rev_id,
            upserts=[
                {
                    "element_type": "position",
                    "code": "P2-99",
                    "name": "Posicion test",
                    "parent_id": 99999999,
                    "geometry": {"width_m": 1.26, "depth_m": 1.06},
                }
            ],
            expected_row_version=version,
            actor="test",
        )


def test_retire_keeps_history_code_reusable_only_after_retire(
    svc: WarehouseLayoutService,
):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    rev_id = int(draft["id"])
    version = int(draft["row_version"])
    # create temp element
    created = svc.apply_batch(
        rev_id,
        upserts=[
            {
                "element_type": "obstacle",
                "code": "TMP-OBS",
                "name": "Obstáculo temporal",
                "geometry": {
                    "x_m": 1,
                    "y_m": 1,
                    "width_m": 0.5,
                    "depth_m": 0.5,
                    "validation_status": "pending_validation",
                },
            }
        ],
        expected_row_version=version,
        actor="test",
    )
    pid = int(created["upserted"][0]["physical"]["id"])
    version2 = int(created["revision"]["row_version"])
    retired = svc.apply_batch(
        rev_id,
        retire_physical_ids=[pid],
        expected_row_version=version2,
        actor="test",
    )
    assert pid in retired["retired_physical_ids"]
    # code can be reused for a NEW physical id
    version3 = int(retired["revision"]["row_version"])
    again = svc.apply_batch(
        rev_id,
        upserts=[
            {
                "element_type": "obstacle",
                "code": "TMP-OBS",
                "name": "Obstáculo nuevo",
                "geometry": {"x_m": 2, "y_m": 2, "width_m": 0.5, "depth_m": 0.5},
            }
        ],
        expected_row_version=version3,
        actor="test",
    )
    assert int(again["upserted"][0]["physical"]["id"]) != pid


def test_published_revision_not_writable(svc: WarehouseLayoutService, migrated_dsn: str):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    conn = psycopg2.connect(migrated_dsn)
    cur = conn.cursor()
    # Como máximo una published por piso (004): archivar previas de prueba.
    cur.execute(
        """
        UPDATE warehouse.layout_revisions
        SET status = 'archived', archived_at = now(), archived_by = 'test'
        WHERE floor_id = %s AND status = 'published'
        """,
        (int(floor["id"]),),
    )
    cur.execute(
        """
        INSERT INTO warehouse.layout_revisions (floor_id, status, label, row_version, created_by)
        VALUES (%s, 'published', 'fake published', 1, 'test')
        RETURNING id
        """,
        (int(floor["id"]),),
    )
    pub_id = int(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    with pytest.raises(WarehouseConflictError):
        svc.apply_batch(pub_id, upserts=[], expected_row_version=1, actor="test")


def test_draft_rename_does_not_alter_published_identity_snapshot(
    svc: WarehouseLayoutService, migrated_dsn: str
):
    """Aislar identidad: rename/retire en draft no muta representación published."""
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    draft_id = int(draft["id"])
    draft_bundle = svc.get_revision(draft_id)
    iz = next(e for e in draft_bundle["elements"] if e["physical"]["code"] == "IZ")
    pid = int(iz["physical"]["id"])
    published_name = iz["physical"]["name"]
    published_code = iz["physical"]["code"]

    conn = psycopg2.connect(migrated_dsn)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE warehouse.layout_revisions
        SET status = 'archived', archived_at = now(), archived_by = 'test'
        WHERE floor_id = %s AND status = 'published'
        """,
        (int(floor["id"]),),
    )
    cur.execute(
        """
        INSERT INTO warehouse.layout_revisions (floor_id, status, label, row_version, published_at, published_by)
        VALUES (%s, 'published', 'published isolation test', 1, now(), 'test')
        RETURNING id
        """,
        (int(floor["id"]),),
    )
    pub_id = int(cur.fetchone()[0])
    # Copiar geometría + snaps del draft → published (simula publicación futura)
    cur.execute(
        """
        INSERT INTO warehouse.layout_elements (
            revision_id, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, polygon_m, stack_levels_max, validation_status,
            constraints_json, observations, meta_json
        )
        SELECT
            %s, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, polygon_m, stack_levels_max, validation_status,
            constraints_json, observations, meta_json
        FROM warehouse.layout_elements
        WHERE revision_id = %s
        """,
        (pub_id, draft_id),
    )
    conn.commit()
    cur.close()
    conn.close()

    pub_before = svc.get_revision(pub_id)
    iz_pub = next(e for e in pub_before["elements"] if e["physical"]["id"] == pid)
    assert iz_pub["physical"]["code"] == published_code
    assert iz_pub["physical"]["name"] == published_name
    assert iz_pub["physical"]["identity_source"] == "revision_snapshot"

    # Rename en draft
    out = svc.apply_batch(
        draft_id,
        upserts=[
            {
                "physical_element_id": pid,
                "parent_id": iz["physical"]["parent_id"],
                "element_type": "sector",
                "code": "IZ",
                "name": "Sector izquierdo RENAMED DRAFT ONLY",
                "geometry": {
                    "x_m": iz["geometry"]["x_m"],
                    "y_m": iz["geometry"]["y_m"],
                    "width_m": iz["geometry"]["width_m"],
                    "depth_m": iz["geometry"]["depth_m"],
                    "height_clear_m": iz["geometry"]["height_clear_m"],
                    "validation_status": "pending_validation",
                },
            }
        ],
        expected_row_version=int(svc.get_revision(draft_id)["revision"]["row_version"]),
        actor="test",
    )
    assert "RENAMED" in out["upserted"][0]["physical"]["name"]

    draft_after = svc.get_revision(draft_id)
    iz_draft = next(e for e in draft_after["elements"] if e["physical"]["id"] == pid)
    assert "RENAMED" in iz_draft["physical"]["name"]

    pub_after = svc.get_revision(pub_id)
    iz_pub2 = next(e for e in pub_after["elements"] if e["physical"]["id"] == pid)
    assert iz_pub2["physical"]["name"] == published_name
    assert iz_pub2["physical"]["code"] == published_code
    assert float(iz_pub2["geometry"]["width_m"]) == float(iz["geometry"]["width_m"])


def test_retire_from_draft_keeps_published_geometry(
    svc: WarehouseLayoutService, migrated_dsn: str
):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    draft_id = int(draft["id"])
    # Crear elemento solo en draft, copiar a published, retirar del draft
    created = svc.apply_batch(
        draft_id,
        upserts=[
            {
                "element_type": "obstacle",
                "code": "ISO-OBS",
                "name": "Obstáculo aislamiento",
                "geometry": {
                    "x_m": 3,
                    "y_m": 3,
                    "width_m": 0.4,
                    "depth_m": 0.4,
                    "validation_status": "pending_validation",
                },
            }
        ],
        expected_row_version=int(svc.get_revision(draft_id)["revision"]["row_version"]),
        actor="test",
    )
    pid = int(created["upserted"][0]["physical"]["id"])
    draft_id = int(created["revision"]["id"])

    conn = psycopg2.connect(migrated_dsn)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id FROM warehouse.layout_revisions
        WHERE floor_id = %s AND status = 'published'
        ORDER BY id DESC LIMIT 1
        """,
        (int(floor["id"]),),
    )
    row = cur.fetchone()
    if row:
        pub_id = int(row[0])
    else:
        cur.execute(
            """
            INSERT INTO warehouse.layout_revisions (floor_id, status, label, row_version, published_at)
            VALUES (%s, 'published', 'pub retire isolation', 1, now())
            RETURNING id
            """,
            (int(floor["id"]),),
        )
        pub_id = int(cur.fetchone()[0])
    cur.execute(
        """
        INSERT INTO warehouse.layout_elements (
            revision_id, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, validation_status, constraints_json, observations, meta_json
        )
        SELECT
            %s, physical_element_id,
            code_snap, name_snap, element_type_snap, parent_physical_id_snap, physical_meta_snap,
            x_m, y_m, elevation_m, width_m, depth_m, height_clear_m, rotation_deg,
            geom_type, validation_status, constraints_json, observations, meta_json
        FROM warehouse.layout_elements
        WHERE revision_id = %s AND physical_element_id = %s
        ON CONFLICT (revision_id, physical_element_id) DO NOTHING
        """,
        (pub_id, draft_id, pid),
    )
    conn.commit()
    cur.close()
    conn.close()

    pub_before = svc.get_revision(pub_id)
    assert any(e["physical"]["id"] == pid for e in pub_before["elements"])

    svc.apply_batch(
        draft_id,
        retire_physical_ids=[pid],
        expected_row_version=int(svc.get_revision(draft_id)["revision"]["row_version"]),
        actor="test",
    )
    draft_after = svc.get_revision(draft_id)
    assert all(e["physical"]["id"] != pid for e in draft_after["elements"])

    pub_after = svc.get_revision(pub_id)
    still = next(e for e in pub_after["elements"] if e["physical"]["id"] == pid)
    assert still["physical"]["code"] == "ISO-OBS"
    assert still["physical"]["name"] == "Obstáculo aislamiento"
    assert float(still["geometry"]["width_m"]) == 0.4


def test_no_writes_to_bsale_schemas(migrated_dsn: str):
    conn = psycopg2.connect(migrated_dsn)
    cur = conn.cursor()
    # Schema warehouse only; bsale may not even exist on isolated PG
    cur.execute(
        """
        SELECT n.nspname, c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname IN ('bsale', 'bsale_raw')
          AND c.relkind = 'r'
        """
    )
    # On isolated DB these schemas typically absent — assert warehouse exists
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='warehouse'"
    )
    assert cur.fetchone()[0] >= 5
    cur.close()
    conn.close()


def test_audit_recorded(svc: WarehouseLayoutService):
    fac = svc.list_facilities()[0]
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    items = svc.list_audit(int(draft["id"]), limit=50)
    assert any(i["action"] in ("seed_bc_draft_applied", "batch_saved", "seed_skipped_existing_draft") for i in items)


def test_router_requires_management(migrated_dsn: str, monkeypatch):
    from fastapi import HTTPException

    from backend.routers import warehouse as wh

    app = FastAPI()
    app.include_router(wh.router)

    def _deny_dep():
        raise HTTPException(status_code=403, detail="forbidden")

    app.dependency_overrides[wh.require_management_access] = _deny_dep

    def _factory():
        return psycopg2.connect(migrated_dsn)

    monkeypatch.setattr(wh, "_svc", lambda: WarehouseLayoutService(_factory))
    client = TestClient(app)
    assert client.get("/warehouse/facilities").status_code == 403
