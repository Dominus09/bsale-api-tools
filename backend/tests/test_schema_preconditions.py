"""Fail-fast de schema F1A/F1B (048): helper, jobs, runner y arranque de la API."""

from __future__ import annotations

import importlib

import pytest

from backend.repositories.distribuidora import schema_preconditions as sp

MISSING = ["distribuidora.document_detail_history"]


class _Cur:
    def __init__(self, rows):
        self.rows = rows
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        self.sql.append(sql)

    def fetchall(self):
        return [(r,) for r in self.rows]

    def close(self):
        pass


@pytest.fixture(autouse=True)
def _reset_cache(monkeypatch):
    monkeypatch.setattr(sp, "_REISSUE_SCHEMA_OK", False)


def test_check_is_catalog_only_select():
    cur = _Cur([])
    assert sp.missing_reissue_schema_objects(cur) == []
    assert len(cur.sql) == 1
    assert cur.sql[0].lstrip().upper().startswith("SELECT")
    assert "to_regclass" in cur.sql[0]


def test_require_raises_and_does_not_cache_negative():
    with pytest.raises(sp.SchemaPreconditionError, match="048_document_reissue_lineage"):
        sp.require_reissue_schema(_Cur(MISSING))
    assert sp._REISSUE_SCHEMA_OK is False
    sp.require_reissue_schema(_Cur([]))
    assert sp._REISSUE_SCHEMA_OK is True


@pytest.mark.parametrize("missing, expected", [(MISSING, 3), ([], 0), (None, 0)])
def test_exit_code(monkeypatch, missing, expected):
    monkeypatch.setattr(sp, "missing_reissue_schema_objects_new_connection", lambda: missing)
    assert sp.reissue_schema_exit_code("job_x") == expected


@pytest.mark.parametrize(
    "module",
    [
        "backend.jobs.live_sync_documents",
        "backend.jobs.live_sync_details",
        "backend.jobs.live_sync_related",
        "backend.jobs.live_sync_probable_matches",
    ],
)
def test_live_sync_jobs_abort_before_work_when_schema_missing(monkeypatch, module):
    mod = importlib.import_module(module)
    monkeypatch.setattr(mod, "reissue_schema_exit_code", lambda job: sp.SCHEMA_MISSING_EXIT_CODE)
    monkeypatch.setattr(mod, "require_bsale_token", _forbidden, raising=False)
    monkeypatch.setattr(mod, "get_connection", _forbidden, raising=False)
    assert mod.main() == sp.SCHEMA_MISSING_EXIT_CODE


def _forbidden(*a, **k):
    raise AssertionError("no debe llegar a Bsale/BD sin schema 048")


def test_reconcile_checks_schema_only_in_execute(monkeypatch):
    mod = importlib.import_module("backend.jobs.reconcile_open_purchase_orders")
    monkeypatch.setattr(mod, "reissue_schema_exit_code", lambda job: sp.SCHEMA_MISSING_EXIT_CODE)
    monkeypatch.setattr(mod, "require_bsale_token", _forbidden)
    assert mod.main(["--execute"]) == sp.SCHEMA_MISSING_EXIT_CODE

    calls = []
    monkeypatch.setattr(mod, "reissue_schema_exit_code", lambda job: calls.append(job) or 3)
    monkeypatch.setattr(mod, "require_bsale_token", lambda **k: "t")
    monkeypatch.setattr(mod, "BsaleClient", lambda token: object())
    monkeypatch.setattr(mod, "reconcile_open_purchase_orders_batch", lambda *a, **k: {"errors": 0})
    assert mod.main([]) == 0
    assert calls == []


class _Conn:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return _Cur([])

    def close(self):
        pass


def _runner(monkeypatch, missing):
    mod = importlib.import_module("backend.jobs.apply_distribuidora_schema")
    conn = _Conn()
    monkeypatch.setattr(mod, "load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(mod, "_configure_logging", lambda: None)
    monkeypatch.setattr(mod, "get_connection", lambda: conn)
    monkeypatch.setattr(mod, "pg_backend_pid", lambda c: 1)
    monkeypatch.setattr(mod, "apply_distribuidora_migrations", lambda cur: ["x.sql"])
    seen = []
    monkeypatch.setattr(
        mod, "missing_schema_objects", lambda cur, names: seen.append(names) or missing
    )
    monkeypatch.setattr(mod, "safe_commit", lambda c, job: setattr(c, "committed", True))
    monkeypatch.setattr(mod, "safe_rollback", lambda c, job: setattr(c, "rolled_back", True))
    rc = mod.main()
    assert seen == [sp.RUNNER_REQUIRED_OBJECTS]
    assert set(sp.REISSUE_SCHEMA_OBJECTS) < set(sp.RUNNER_REQUIRED_OBJECTS)
    assert "distribuidora.document_type_roles" in sp.RUNNER_REQUIRED_OBJECTS
    return rc, conn


def test_runner_rolls_back_if_048_objects_missing(monkeypatch):
    rc, conn = _runner(monkeypatch, MISSING)
    assert rc == 1 and not conn.committed and conn.rolled_back


def test_runner_commits_when_schema_complete(monkeypatch):
    rc, conn = _runner(monkeypatch, [])
    assert rc == 0 and conn.committed and not conn.rolled_back


def test_api_startup_fails_without_048(monkeypatch):
    main = importlib.import_module("backend.main")
    monkeypatch.setattr(sp, "missing_reissue_schema_objects_new_connection", lambda: MISSING)
    monkeypatch.delenv("DISTRIBUIDORA_SCHEMA_CHECK", raising=False)
    with pytest.raises(RuntimeError, match="falta 048"):
        main._startup_require_distribuidora_reissue_schema()
    monkeypatch.setenv("DISTRIBUIDORA_SCHEMA_CHECK", "warn")
    main._startup_require_distribuidora_reissue_schema()


@pytest.mark.parametrize("missing", [[], None])
def test_api_startup_ok_or_db_unavailable(monkeypatch, missing):
    main = importlib.import_module("backend.main")
    monkeypatch.setattr(sp, "missing_reissue_schema_objects_new_connection", lambda: missing)
    monkeypatch.delenv("DISTRIBUIDORA_SCHEMA_CHECK", raising=False)
    main._startup_require_distribuidora_reissue_schema()
