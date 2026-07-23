"""Tests Etapa 2B — job diagnóstico read-only (sin PostgreSQL real)."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

import pytest

from backend.jobs import validate_analytics_distribuidora_source as job
from backend.services.analytics.distribuidora_source import (
    DistribuidoraDocumentSource,
    build_lines_for_document_ids_query,
)
from backend.services.analytics.validate_distribuidora_source import (
    AnalyticsValidationError,
    assert_sql_is_read_only,
    clamp_validate_args,
    commercial_date_window,
    make_psycopg_executor,
    open_readonly_connection,
    run_validation,
)


class FakeConn:
    def __init__(self) -> None:
        self.readonly = False
        self.autocommit = True
        self.rolled_back = False
        self.closed = False
        self.executed: list[str] = []
        self._fail_timeout = False
        self._rows: list[tuple] = []
        self._description: list[tuple] | None = None

    def set_session(self, *, readonly: bool = False, autocommit: bool = False) -> None:
        self.readonly = readonly
        self.autocommit = autocommit

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, conn: FakeConn) -> None:
        self.conn = conn
        self.description = conn._description

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.conn.executed.append(sql)
        if self.conn._fail_timeout and "FROM distribuidora.documents" in sql:
            raise Exception("canceling statement due to statement timeout")

    def fetchall(self) -> list[tuple]:
        return list(self.conn._rows)

    def close(self) -> None:
        return None


class RecordingExecutor:
    def __init__(self, headers: list[dict], lines: list[dict]) -> None:
        self.headers = headers
        self.lines = lines
        self.sqls: list[str] = []
        self.params: list[tuple] = []

    def __call__(self, sql: str, params: tuple) -> list[dict]:
        assert_sql_is_read_only(sql)
        self.sqls.append(sql)
        self.params.append(params)
        if "document_details" in sql:
            if "ANY(%s)" in sql or "= ANY(%s)" in sql:
                ids = set(params[2] if len(params) >= 3 else [])
                return [r for r in self.lines if r["document_id"] in ids]
            return list(self.lines)
        return list(self.headers)


def _header(
    document_id: int = 1,
    *,
    dtype: int = 6,
    total: str = "119",
    net: str = "100",
    state: int = 0,
) -> dict[str, Any]:
    return {
        "document_id": document_id,
        "source_document_id": document_id + 1000,
        "document_type_id": dtype,
        "number": 50000 + document_id,
        "company_id": 3,
        "office_id": 1,
        "emission_date": date(2026, 7, 22),
        "generation_date": date(2026, 7, 22),
        "client_id": 1,
        "seller_id": 80,
        "seller_name": "Test",
        "net_amount": Decimal(net),
        "tax_amount": Decimal("19"),
        "total_amount": Decimal(total),
        "state": state,
        "commercial_state": 0,
    }


def _line(
    document_id: int,
    detail_id: int,
    *,
    total: str,
    net: str | None = None,
) -> dict[str, Any]:
    return {
        "detail_id": detail_id,
        "document_id": document_id,
        "variant_id": 1,
        "variant_code": "X",
        "quantity": Decimal("1"),
        "net_amount": Decimal(net) if net is not None else None,
        "tax_amount": None,
        "total_amount": Decimal(total),
        "net_unit_value": None,
        "total_unit_value": None,
        "net_discount": None,
        "total_discount": None,
    }


def test_clamp_args_and_max_limits():
    args = clamp_validate_args(
        company_id=3,
        office_id=1,
        days=99,
        limit=999,
        page_size=999,
        statement_timeout_seconds=99,
    )
    assert args.days == 7
    assert args.limit == 50
    assert args.page_size == 50
    assert args.statement_timeout_seconds == 30

    with pytest.raises(AnalyticsValidationError):
        clamp_validate_args(company_id=0, office_id=1)


def test_commercial_date_window():
    d0, d1 = commercial_date_window(2, today=date(2026, 7, 23))
    assert d1 == date(2026, 7, 23)
    assert d0 == date(2026, 7, 22)


def test_open_readonly_connection():
    fake = FakeConn()
    conn = open_readonly_connection(lambda: fake)
    assert conn is fake
    assert fake.readonly is True
    assert fake.autocommit is False


def test_make_executor_sets_timeouts_and_blocks_dml():
    fake = FakeConn()
    fake._description = [("document_id",)]
    fake._rows = [(1,)]
    exe = make_psycopg_executor(fake, statement_timeout_seconds=15, sql_log=[])
    rows = exe(
        "SELECT d.document_id FROM distribuidora.documents d WHERE d.company_id = %s",
        (3,),
    )
    assert rows == [{"document_id": 1}]
    joined = " | ".join(fake.executed)
    assert "statement_timeout" in joined
    assert "lock_timeout" in joined

    with pytest.raises(AnalyticsValidationError) as exc:
        exe("UPDATE distribuidora.documents SET state = 1", ())
    assert exc.value.error_type == "forbidden_sql"


def test_run_validation_max_docs_and_lines_only_for_ids():
    headers = [_header(1), _header(2, dtype=1), _header(9, dtype=9)]
    lines = [
        _line(1, 1, total="119", net="100"),
        _line(2, 2, total="50", net="42"),
        _line(999, 3, total="999", net="999"),  # fuera del set → filtrado
    ]
    executor = RecordingExecutor(headers, lines)
    source = DistribuidoraDocumentSource(executor)
    args = clamp_validate_args(company_id=3, office_id=1, days=2, limit=20)
    report = run_validation(args=args, source=source, today=date(2026, 7, 23))

    assert report["ok"] is True
    assert report["read_only"] is True
    assert report["documents_loaded"] == 3
    assert report["lines_loaded"] == 2
    assert report["document_types"]["invoice"] == 1
    assert report["document_types"]["receipt"] == 1
    assert report["document_types"]["credit_note"] == 1
    assert any("ANY(%s)" in sql or "= ANY(%s)" in sql for sql in executor.sqls)
    # No debe incluir línea 999
    assert all("999" not in str(s.get("document_id")) for s in report["samples"])


def test_matched_mismatch_missing_in_report():
    headers = [
        _header(1, total="100", net="84"),
        _header(2, total="100", net="84"),
        _header(3, total="100", net="84"),
    ]
    lines = [
        _line(1, 1, total="60", net="50"),
        _line(1, 2, total="40", net="34"),
        _line(2, 3, total="60", net="50"),
        _line(2, 4, total="10", net="8"),  # mismatch
        # doc 3 sin líneas → missing_lines
    ]
    source = DistribuidoraDocumentSource(RecordingExecutor(headers, lines))
    args = clamp_validate_args(company_id=3, office_id=1, limit=20)
    report = run_validation(args=args, source=source, today=date(2026, 7, 23))
    assert report["reconciliation"]["matched"] == 1
    assert report["reconciliation"]["mismatch"] == 1
    assert report["reconciliation"]["missing_lines"] == 1
    assert isinstance(report["samples"], list)
    assert len(report["samples"]) <= 10


def test_empty_documents_ok_exit_via_job(monkeypatch):
    class EmptyExec:
        def __call__(self, sql: str, params: tuple) -> list[dict]:
            return []

    class EmptyConn(FakeConn):
        pass

    monkeypatch.setattr(job, "get_connection", lambda: EmptyConn())
    # Patch open path to use empty executor via run_validation path
    # Simpler: call run_validation directly for empty case exit semantics
    source = DistribuidoraDocumentSource(EmptyExec())
    args = clamp_validate_args(company_id=3, office_id=1)
    report = run_validation(args=args, source=source, today=date(2026, 7, 23))
    assert report["ok"] is True
    assert report["documents_loaded"] == 0

    # Job wrapper with empty headers from fake connection returning no rows
    monkeypatch.setattr(
        job,
        "open_readonly_connection",
        lambda get_connection: EmptyConn(),
    )

    def fake_executor(conn, **kwargs):
        return EmptyExec()

    monkeypatch.setattr(job, "make_psycopg_executor", fake_executor)
    code, payload = job.run_job(
        ["--company-id", "3", "--office-id", "1", "--days", "2", "--limit", "20"]
    )
    assert code == 0
    assert payload["ok"] is True
    assert payload["documents_loaded"] == 0
    # rollback en finally
    # (EmptyConn from open_readonly_connection)


def test_timeout_rollback_and_exit_1(monkeypatch):
    conn = FakeConn()
    conn._fail_timeout = True
    conn._description = [("document_id",)]
    conn._rows = []

    monkeypatch.setattr(job, "open_readonly_connection", lambda get_connection: conn)

    def boom_executor(c, **kwargs):
        def _exec(sql: str, params: tuple) -> list[dict]:
            raise AnalyticsValidationError(
                "statement_timeout",
                error_type="statement_timeout",
            )

        return _exec

    monkeypatch.setattr(job, "make_psycopg_executor", boom_executor)
    code, payload = job.run_job(
        ["--company-id", "3", "--office-id", "1", "--days", "2", "--limit", "20"]
    )
    assert code == 1
    assert payload["ok"] is False
    assert payload["error_type"] == "statement_timeout"
    assert conn.rolled_back is True
    assert conn.closed is True


def test_lines_query_for_ids_contains_filters():
    sql, params = build_lines_for_document_ids_query(
        company_id=3,
        office_id=1,
        document_ids=[10, 20],
    )
    assert "d.company_id = %s" in sql
    assert "d.office_id = %s" in sql
    assert "dd.document_id = ANY(%s)" in sql
    assert "LIMIT %s" in sql
    assert "SELECT *" not in sql
    assert params[0] == 3
    assert params[1] == 1
    assert params[2] == [10, 20]


def test_never_allows_ddl_dml_tokens():
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("DELETE FROM distribuidora.documents")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("ALTER TABLE distribuidora.documents ADD COLUMN x INT")
    with pytest.raises(AnalyticsValidationError):
        assert_sql_is_read_only("SELECT * FROM t FOR UPDATE")
    assert_sql_is_read_only(
        "SELECT d.document_id FROM distribuidora.documents d WHERE d.company_id = %s"
    )


def test_job_clamps_limit_via_cli(monkeypatch):
    captured: dict[str, Any] = {}

    def fake_run_validation(*, args, source, today=None):
        captured["limit"] = args.limit
        captured["days"] = args.days
        return {
            "ok": True,
            "read_only": True,
            "documents_loaded": 0,
            "requested_limit": args.limit,
        }

    monkeypatch.setattr(job, "open_readonly_connection", lambda g: FakeConn())
    monkeypatch.setattr(
        job,
        "make_psycopg_executor",
        lambda conn, **kw: (lambda sql, params: []),
    )
    monkeypatch.setattr(job, "run_validation", fake_run_validation)
    code, payload = job.run_job(
        [
            "--company-id",
            "3",
            "--office-id",
            "1",
            "--days",
            "100",
            "--limit",
            "999",
        ]
    )
    assert code == 0
    assert captured["limit"] == 50
    assert captured["days"] == 7
