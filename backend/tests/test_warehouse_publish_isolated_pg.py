"""Publicación formal, inmutabilidad histórica y restauración (slice 1.4)."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import psycopg2
import pytest

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


pytestmark = pytest.mark.skipif(not _dsn(), reason="WAREHOUSE_TEST_DSN requerido")


@pytest.fixture(scope="module")
def migrated_dsn() -> str:
    uri = _dsn()
    assert uri
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
    for name in (
        "001_schema_layout_AWAITING_APPROVAL.sql",
        "002_seed_bodega_central_draft_AWAITING_APPROVAL.sql",
        "003_seed_bc_special_zones_AWAITING_APPROVAL.sql",
        "004_publish_layout_AWAITING_APPROVAL.sql",
    ):
        cur.execute((SQL_DIR / name).read_text(encoding="utf-8"))
    cur.close()
    conn.close()
    return uri


@pytest.fixture
def svc(migrated_dsn: str) -> WarehouseLayoutService:
    return WarehouseLayoutService(lambda: psycopg2.connect(migrated_dsn))


def _make_publishable_draft(svc: WarehouseLayoutService) -> tuple[int, int]:
    """Crea piso LAB con dos elementos no solapados y draft limpio."""
    token = uuid.uuid4().hex[:12]
    conn = svc._conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO warehouse.facilities (code, name, company_id, status)
            VALUES (%s, 'Lab publicación', 3, 'active')
            RETURNING id
            """,
            (f"LAB-{token}",),
        )
        fac_id = int(cur.fetchone()[0])
        cur.execute(
            """
            INSERT INTO warehouse.floors (facility_id, code, name, level_index, length_m, width_m, status)
            VALUES (%s, 'P1', 'Piso lab', 1, 10, 8, 'active')
            RETURNING id
            """,
            (fac_id,),
        )
        floor_id = int(cur.fetchone()[0])
        conn.commit()
    finally:
        conn.close()

    created = svc.get_or_create_draft(floor_id, label="Lab publish draft", actor="test")
    draft_id = int(created["revision"]["id"])
    rv = int(created["revision"]["row_version"])
    batch = svc.apply_batch(
        draft_id,
        upserts=[
            {
                "element_type": "facility_boundary",
                "code": "LAB-BND",
                "name": "Límite lab",
                "geometry": {
                    "x_m": 0,
                    "y_m": 0,
                    "width_m": 10,
                    "depth_m": 8,
                    "validation_status": "validated",
                },
            },
            {
                "element_type": "position",
                "code": "LAB-01",
                "name": "Pos 1",
                "geometry": {
                    "x_m": 1,
                    "y_m": 1,
                    "width_m": 1.26,
                    "depth_m": 1.06,
                    "validation_status": "validated",
                },
            },
            {
                "element_type": "position",
                "code": "LAB-02",
                "name": "Pos 2",
                "geometry": {
                    "x_m": 3,
                    "y_m": 1,
                    "width_m": 1.26,
                    "depth_m": 1.06,
                    "validation_status": "pending_validation",
                },
            },
        ],
        expected_row_version=rv,
        actor="test",
    )
    return floor_id, int(batch["revision"]["id"])


def test_seed_bc_not_auto_publishable(svc: WarehouseLayoutService):
    fac = next(f for f in svc.list_facilities() if f["code"] == "BC")
    floor = svc.list_floors(int(fac["id"]))[0]
    draft = next(r for r in svc.list_revisions(int(floor["id"])) if r["status"] == "draft")
    ev = svc.evaluate_publish(int(draft["id"]))
    assert ev["can_publish"] is False
    assert ev["blockers"]
    assert ev["safety_certification"] is False


def test_publish_atomic_and_history(svc: WarehouseLayoutService):
    floor_id, draft_id = _make_publishable_draft(svc)
    ev = svc.evaluate_publish(draft_id)
    assert ev["can_publish"] is True
    # pending_validation no bloquea
    assert (ev["counts"].get("pending_validation") or 0) >= 1

    rev = svc.get_revision(draft_id)["revision"]
    published = svc.publish_revision(
        draft_id,
        expected_row_version=int(rev["row_version"]),
        actor="publisher@test",
        label="Lab v1",
    )
    assert published["revision"]["status"] == "published"
    assert published["revision"]["published_by"] == "publisher@test"
    assert published["archived_previous"] is None

    # No escrituras sobre published
    with pytest.raises(WarehouseConflictError):
        svc.apply_batch(
            draft_id,
            upserts=[
                {
                    "physical_element_id": published["revision"]["id"],  # wrong but blocked first
                    "element_type": "position",
                    "code": "X",
                    "name": "X",
                    "geometry": {"width_m": 1, "depth_m": 1, "x_m": 0, "y_m": 0},
                }
            ],
            expected_row_version=int(published["revision"]["row_version"]),
            actor="test",
        )

    # Nuevo draft desde published
    created = svc.create_draft_from_revision(
        draft_id, label="Lab v2 draft", actor="editor@test"
    )
    assert created["created"] is True
    assert created["copied_elements"] >= 3
    new_draft_id = int(created["revision"]["id"])
    assert new_draft_id != draft_id

    # Identidades físicas preservadas
    pub_ids = {
        e["physical"]["id"] for e in svc.get_revision(draft_id)["elements"]
    }
    draft_ids = {
        e["physical"]["id"] for e in svc.get_revision(new_draft_id)["elements"]
    }
    assert pub_ids == draft_ids

    # Segunda publicación archiva la previa
    rv = int(svc.get_revision(new_draft_id)["revision"]["row_version"])
    # Ajuste menor no conflictivo
    lab01 = next(
        e for e in svc.get_revision(new_draft_id)["elements"] if e["physical"]["code"] == "LAB-01"
    )
    svc.apply_batch(
        new_draft_id,
        upserts=[
            {
                "physical_element_id": lab01["physical"]["id"],
                "parent_id": lab01["physical"]["parent_id"],
                "element_type": "position",
                "code": "LAB-01",
                "name": "Pos 1 moved",
                "geometry": {
                    **lab01["geometry"],
                    "x_m": 1.1,
                    "validation_status": "validated",
                },
            }
        ],
        expected_row_version=rv,
        actor="test",
    )
    rv2 = int(svc.get_revision(new_draft_id)["revision"]["row_version"])
    second = svc.publish_revision(
        new_draft_id, expected_row_version=rv2, actor="publisher@test"
    )
    assert second["archived_previous"]["id"] == draft_id
    assert second["archived_previous"]["status"] == "archived"
    assert svc.get_revision(draft_id)["revision"]["status"] == "archived"

    # Histórico inmutable: rename live no cambia snap archived
    conn = svc._conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE warehouse.physical_elements SET name='MUTATED' WHERE code='LAB-01'"
        )
        conn.commit()
    finally:
        conn.close()
    archived = svc.get_revision(draft_id)
    a01 = next(e for e in archived["elements"] if e["physical"]["code"] == "LAB-01")
    assert a01["physical"]["name"] == "Pos 1"
    assert float(a01["geometry"]["x_m"]) == 1.0


def test_publish_concurrency_conflict(svc: WarehouseLayoutService):
    floor_id, draft_id = _make_publishable_draft(svc)
    with pytest.raises(WarehouseConflictError) as ei:
        svc.publish_revision(draft_id, expected_row_version=999999, actor="test")
    assert ei.value.code == "row_version_conflict"


def test_publish_blocked_on_overlap(svc: WarehouseLayoutService):
    floor_id, draft_id = _make_publishable_draft(svc)
    rev = svc.get_revision(draft_id)["revision"]
    # Crear solape
    svc.apply_batch(
        draft_id,
        upserts=[
            {
                "element_type": "position",
                "code": "LAB-OVER",
                "name": "Overlap",
                "geometry": {
                    "x_m": 1.0,
                    "y_m": 1.0,
                    "width_m": 1.26,
                    "depth_m": 1.06,
                    "validation_status": "validated",
                },
            }
        ],
        expected_row_version=int(rev["row_version"]),
        actor="test",
    )
    with pytest.raises(WarehouseValidationError) as ei:
        svc.publish_revision(
            draft_id,
            expected_row_version=int(svc.get_revision(draft_id)["revision"]["row_version"]),
            actor="test",
        )
    assert ei.value.code == "publish_blocked"


def test_retire_preserves_published_identity(svc: WarehouseLayoutService, migrated_dsn: str):
    floor_id, draft_id = _make_publishable_draft(svc)
    rv = int(svc.get_revision(draft_id)["revision"]["row_version"])
    pub = svc.publish_revision(draft_id, expected_row_version=rv, actor="test")
    assert pub["revision"]["status"] == "published"

    created = svc.create_draft_from_revision(int(pub["revision"]["id"]), actor="test")
    new_draft = int(created["revision"]["id"])
    lab02 = next(
        e for e in svc.get_revision(new_draft)["elements"] if e["physical"]["code"] == "LAB-02"
    )
    pid = int(lab02["physical"]["id"])
    result = svc.apply_batch(
        new_draft,
        retire_physical_ids=[pid],
        expected_row_version=int(svc.get_revision(new_draft)["revision"]["row_version"]),
        actor="test",
    )
    assert pid in result["preserved_in_published_physical_ids"]
    assert pid not in result["retired_physical_ids"]

    # Physical sigue active
    conn = psycopg2.connect(migrated_dsn)
    cur = conn.cursor()
    cur.execute("SELECT status FROM warehouse.physical_elements WHERE id=%s", (pid,))
    assert cur.fetchone()[0] == "active"
    cur.close()
    conn.close()

    # Published aún tiene el elemento (snaps)
    pub_bundle = svc.get_revision(int(pub["revision"]["id"]))
    assert any(e["physical"]["id"] == pid for e in pub_bundle["elements"])


def test_draft_from_requires_no_existing_draft(svc: WarehouseLayoutService):
    floor_id, draft_id = _make_publishable_draft(svc)
    rv = int(svc.get_revision(draft_id)["revision"]["row_version"])
    pub = svc.publish_revision(draft_id, expected_row_version=rv, actor="test")
    svc.create_draft_from_revision(int(pub["revision"]["id"]), actor="test")
    with pytest.raises(WarehouseConflictError) as ei:
        svc.create_draft_from_revision(int(pub["revision"]["id"]), actor="test")
    assert ei.value.code == "draft_already_exists"
