"""Tests: DDL fuera del hot path + helpers de transacción."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.repositories.distribuidora import sync_repo
from backend.utils import db_tx


def test_ensure_distribuidora_schema_is_noop_does_not_run_sql():
    cur = MagicMock()
    # Reset warning flag so we exercise the path.
    sync_repo._ENSURE_SCHEMA_NOOP_WARNED = False
    with patch.object(sync_repo, "_run_sql_file") as run_sql:
        sync_repo.ensure_distribuidora_schema(cur)
        run_sql.assert_not_called()
    cur.execute.assert_not_called()


def test_apply_distribuidora_migrations_runs_all_files_in_order():
    cur = MagicMock()
    seen: list[str] = []

    def _capture(_cur, name: str) -> None:
        seen.append(name)

    with patch.object(sync_repo, "_run_sql_file", side_effect=_capture):
        applied = sync_repo.apply_distribuidora_migrations(cur)

    assert applied == list(sync_repo.DISTRIBUIDORA_SCHEMA_FILES)
    assert seen[0] == "001_schema.sql"
    assert "001_schema.sql" in seen
    assert len(seen) == len(sync_repo.DISTRIBUIDORA_SCHEMA_FILES)


def test_release_transaction_commits():
    conn = MagicMock()
    db_tx.release_transaction(conn, job="unit_test")
    conn.commit.assert_called_once()


def test_safe_rollback_swallows_errors():
    conn = MagicMock()
    conn.rollback.side_effect = RuntimeError("already closed")
    db_tx.safe_rollback(conn, job="unit_test")  # no raise


def test_sync_services_do_not_import_apply_in_hot_path():
    """Los módulos de sync no deben invocar apply_distribuidora_migrations."""
    import ast
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "services" / "distribuidora"
    offenders: list[str] = []
    for path in root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                name = ""
                if isinstance(func, ast.Name):
                    name = func.id
                elif isinstance(func, ast.Attribute):
                    name = func.attr
                if name in ("apply_distribuidora_migrations", "ensure_distribuidora_schema"):
                    # ensure_* puede seguir como no-op legado; apply_* no.
                    if name == "apply_distribuidora_migrations":
                        offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, f"apply_distribuidora_migrations en hot path: {offenders}"


def test_ensure_calls_removed_from_sync_modules():
    """Tras el fix, sync/live/related/probable no deben llamar ensure_*."""
    from pathlib import Path

    files = [
        "sync_service.py",
        "live_sync_service.py",
        "sync_related_service.py",
        "probable_invoice_service.py",
    ]
    root = Path(__file__).resolve().parents[1] / "services" / "distribuidora"
    leftovers: list[str] = []
    for name in files:
        text = (root / name).read_text(encoding="utf-8")
        if "ensure_distribuidora_schema(" in text:
            leftovers.append(name)
    assert not leftovers, f"ensure_distribuidora_schema aún llamado en: {leftovers}"
