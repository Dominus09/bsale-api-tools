"""Prueba SQL 006 en PG aislado: aplica OK + rechazo row_version / reentrada."""

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
    cur.execute("SELECT id, row_version, status FROM warehouse.layout_revisions")
    rows = cur.fetchall()
    assert len(rows) == 1
    assert int(rows[0][0]) == 1
    assert int(rows[0][1]) == 1
    assert rows[0][2] == "draft"
    cur.execute("SELECT COUNT(*) FROM warehouse.layout_elements WHERE revision_id=1")
    assert cur.fetchone()[0] == 17
    cur.close()
    yield conn
    conn.close()


def _run_006(conn, *, expect_ok: bool) -> str | None:
    cur = conn.cursor()
    cur.execute("SET warehouse.fix_006_lab = 'on'")
    sql = _read("006_fix_bc_draft_layout_orientation_AWAITING_APPROVAL.sql")
    err = None
    try:
        # El archivo trae BEGIN/COMMIT propios; en autocommit psycopg ejecuta el script entero
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
            cur.execute("SET warehouse.fix_006_lab = 'on'")
            cur.execute("RESET warehouse.fix_006_lab")
        except Exception:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
        cur.close()
    if expect_ok and err:
        raise AssertionError(f"006 debió aplicar: {err}")
    if not expect_ok and not err:
        raise AssertionError("006 debió fallar y no falló")
    return err


def test_006_applies_and_bumps_row_version(migrated_conn):
    _run_006(migrated_conn, expect_ok=True)
    cur = migrated_conn.cursor()
    cur.execute("SELECT status, row_version FROM warehouse.layout_revisions WHERE id=1")
    status, rv = cur.fetchone()
    assert status == "draft"
    assert rv == 2
    cur.execute(
        """
        SELECT code_snap, x_m, y_m, width_m, depth_m
        FROM warehouse.layout_elements WHERE revision_id=1
        ORDER BY code_snap
        """
    )
    by = {r[0]: tuple(float(v) for v in r[1:]) for r in cur.fetchall()}
    assert by["OFC"] == pytest.approx((0.0, 7.35, 2.80, 6.00))
    assert by["P1"] == pytest.approx((2.80, 7.35, 2.80, 6.00))
    assert by["P7"] == pytest.approx((19.60, 7.35, 2.80, 6.00))
    assert by["PORTON"] == pytest.approx((0.0, 4.675, 0.40, 4.35))
    assert by["IZ-SU"][2] * by["IZ-SU"][3] == pytest.approx(2.52 * 1.06)
    # BAE ⊂ ESC
    esc = by["ESC"]
    for code in ("BAE-01", "BAE-02"):
        x, y, w, d = by[code]
        assert x >= esc[0] - 1e-9
        assert y >= esc[1] - 1e-9
        assert x + w <= esc[0] + esc[2] + 1e-9
        assert y + d <= esc[1] + esc[3] + 1e-9
    cur.execute("SELECT COUNT(*) FROM warehouse.physical_elements WHERE status='active'")
    assert cur.fetchone()[0] == 17
    cur.close()


def test_006_rejects_wrong_row_version(migrated_conn):
    cur = migrated_conn.cursor()
    cur.execute("UPDATE warehouse.layout_revisions SET row_version = 99 WHERE id = 1")
    cur.close()
    err = _run_006(migrated_conn, expect_ok=False)
    assert err and "row_version" in err.lower()
    # Reabrir sesión limpia tras abort del script 006
    uri = _dsn()
    assert uri
    migrated_conn.close()
    conn = psycopg2.connect(uri)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT row_version FROM warehouse.layout_revisions WHERE id=1")
    assert int(cur.fetchone()[0]) == 99
    cur.execute("SELECT x_m, width_m FROM warehouse.layout_elements WHERE code_snap='PORTON'")
    x, w = cur.fetchone()
    assert float(x) == 9.375 and float(w) == 4.35
    cur.close()
    conn.close()


def test_006_rejects_second_apply(migrated_conn):
    _run_006(migrated_conn, expect_ok=True)
    err = _run_006(migrated_conn, expect_ok=False)
    assert err and ("row_version" in err.lower() or "corrected_by" in err.lower())
