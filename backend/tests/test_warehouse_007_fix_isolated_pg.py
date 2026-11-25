"""Prueba SQL 007 en PG aislado: ESC→CIRC, BAE⊂ESC, P7 intacto, parent/snap, guards."""

from __future__ import annotations

import os
from pathlib import Path

import psycopg2
import pytest

SQL_DIR = Path(__file__).resolve().parents[1] / "sql" / "warehouse"


def _dsn() -> str | None:
    return os.environ.get("WAREHOUSE_TEST_DSN")


pytestmark = pytest.mark.skipif(not _dsn(), reason="WAREHOUSE_TEST_DSN requerido")


def _read(name: str) -> str:
    return (SQL_DIR / name).read_text(encoding="utf-8")


def _exec_script(cur, sql: str) -> None:
    cur.execute(sql)


@pytest.fixture
def migrated_conn():
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
        _exec_script(cur, _read(name))
    cur.execute("SET warehouse.fix_006_lab = 'on'")
    _exec_script(cur, _read("006_fix_bc_draft_layout_orientation_AWAITING_APPROVAL.sql"))
    cur.execute("RESET warehouse.fix_006_lab")
    cur.execute("SELECT id, row_version, status FROM warehouse.layout_revisions WHERE id=1")
    rid, rv, status = cur.fetchone()
    assert int(rid) == 1 and int(rv) == 2 and status == "draft"
    cur.close()
    yield conn
    conn.close()


def _run_007(conn, *, expect_ok: bool) -> str | None:
    cur = conn.cursor()
    cur.execute("SET warehouse.fix_007_lab = 'on'")
    sql = _read("007_fix_bc_draft_esc_izsu_bae_AWAITING_APPROVAL.sql")
    err = None
    try:
        cur.execute(sql)
    except Exception as exc:  # noqa: BLE001
        err = str(exc)
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
    finally:
        try:
            conn.rollback()
        except Exception:  # noqa: BLE001
            pass
        try:
            cur.execute("SET warehouse.fix_007_lab = 'on'")
            cur.execute("RESET warehouse.fix_007_lab")
        except Exception:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
        cur.close()
    if expect_ok and err:
        raise AssertionError(f"007 debió aplicar: {err}")
    if not expect_ok and not err:
        raise AssertionError("007 debió fallar y no falló")
    return err


def test_007_applies_esc_in_circ_and_preserves_p7(migrated_conn):
    _run_007(migrated_conn, expect_ok=True)
    cur = migrated_conn.cursor()
    cur.execute("SELECT status, row_version FROM warehouse.layout_revisions WHERE id=1")
    status, rv = cur.fetchone()
    assert status == "draft"
    assert int(rv) == 3

    cur.execute(
        """
        SELECT code_snap, x_m, y_m, width_m, depth_m
        FROM warehouse.layout_elements WHERE revision_id=1
        ORDER BY code_snap
        """
    )
    by = {r[0]: tuple(float(v) for v in r[1:]) for r in cur.fetchall()}
    assert by["P7"] == pytest.approx((19.60, 7.35, 2.80, 6.00))
    assert by["P1"] == pytest.approx((2.80, 7.35, 2.80, 6.00))
    assert by["IZ-SU"] == pytest.approx((21.84, 0.10, 1.26, 2.12))
    assert by["ESC"] == pytest.approx((21.58, 4.05, 1.52, 3.20))
    assert by["BAE-01"] == pytest.approx((21.68, 4.20, 1.26, 1.06))
    assert by["BAE-02"] == pytest.approx((21.68, 5.40, 1.26, 1.06))

    esc = by["ESC"]
    assert esc[1] >= 2.65 - 1e-9
    assert esc[1] + esc[3] <= 7.35 + 1e-9
    for code in ("BAE-01", "BAE-02"):
        x, y, w, d = by[code]
        assert x >= esc[0] - 1e-9
        assert y >= esc[1] - 1e-9
        assert x + w <= esc[0] + esc[2] + 1e-9
        assert y + d <= esc[1] + esc[3] + 1e-9

    cur.execute(
        """
        SELECT pe.code, pe.parent_id, le.parent_physical_id_snap, parent.code
        FROM warehouse.layout_elements le
        JOIN warehouse.physical_elements pe ON pe.id = le.physical_element_id
        JOIN warehouse.physical_elements parent ON parent.id = pe.parent_id
        WHERE le.revision_id=1 AND le.code_snap='ESC'
        """
    )
    code, parent_id, snap, parent_code = cur.fetchone()
    assert code == "ESC"
    assert parent_code == "CIRC"
    assert parent_id == snap

    cur.execute(
        """
        SELECT code_snap,
               validation_status,
               (meta_json->>'provisional')::boolean,
               (constraints_json->>'safe_storage_authorized')
        FROM warehouse.layout_elements
        WHERE revision_id=1 AND code_snap IN ('ESC','BAE-01','BAE-02','IZ-SU')
        ORDER BY 1
        """
    )
    for row in cur.fetchall():
        assert row[1] == "pending_validation"
        assert row[2] is True
        if row[0].startswith("BAE"):
            assert row[3] == "false"

    cur.execute("SELECT COUNT(*) FROM warehouse.layout_revisions WHERE status='published'")
    assert cur.fetchone()[0] == 0
    cur.close()


def test_007_rejects_wrong_row_version(migrated_conn):
    cur = migrated_conn.cursor()
    cur.execute("UPDATE warehouse.layout_revisions SET row_version = 99 WHERE id = 1")
    cur.close()
    err = _run_007(migrated_conn, expect_ok=False)
    assert err and "row_version" in err.lower()
    uri = _dsn()
    assert uri
    migrated_conn.close()
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT row_version FROM warehouse.layout_revisions WHERE id=1")
    assert int(cur.fetchone()[0]) == 99
    cur.execute(
        "SELECT y_m, depth_m FROM warehouse.layout_elements WHERE revision_id=1 AND code_snap='ESC'"
    )
    y, d = cur.fetchone()
    # Post-006 ESC aún en franja DER; 007 no aplicó
    assert float(y) == pytest.approx(7.35)
    assert float(d) == pytest.approx(6.00)
    cur.close()
    conn.close()


def test_007_rejects_second_apply(migrated_conn):
    _run_007(migrated_conn, expect_ok=True)
    err = _run_007(migrated_conn, expect_ok=False)
    assert err and ("row_version" in err.lower() or "corrected_by" in err.lower())


def test_007_sql_has_no_r_record_ambiguity():
    sql = _read("007_fix_bc_draft_esc_izsu_bae_AWAITING_APPROVAL.sql")
    assert "r RECORD" not in sql
    assert "FOR UPDATE OF rev" in sql
    assert "FOR UPDATE OF r;" not in sql
