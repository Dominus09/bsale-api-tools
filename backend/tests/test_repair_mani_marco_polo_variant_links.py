"""Pruebas del job puntual repair_mani_marco_polo_variant_links."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backend.jobs import repair_mani_marco_polo_variant_links as job


NOW = datetime(2026, 7, 22, 12, 0, 0, tzinfo=timezone.utc)


def _base_rows() -> dict[int, dict]:
    return {
        3924: {
            "id": 3924,
            "barcode": "7802337801014",
            "product_id": 4063,
            "variant_id": 8943,
            "product_name": "JUGO LIMON TRAVERSO",
            "variant_name": "250 CC (SEC 48)",
            "units_per_box": 48,
            "weight_box_kg": 13.29,
            "sale_type": "PARCIAL",
            "is_active": True,
            "created_at": NOW,
            "updated_at": NOW,
        },
        3925: {
            "id": 3925,
            "barcode": "7802337801038",
            "product_id": 4063,
            "variant_id": 8942,
            "product_name": "JUGO LIMON TRAVERSO",
            "variant_name": "500 CC (SEC 30)",
            "units_per_box": 30,
            "weight_box_kg": 16.42,
            "sale_type": "PARCIAL",
            "is_active": True,
            "created_at": NOW,
            "updated_at": NOW,
        },
        4152: {
            "id": 4152,
            "barcode": "7802420009518",
            "product_id": 2928,
            "variant_id": 8942,
            "product_name": "MANI MARCO POLO",
            "variant_name": "SALADO 150 GR (SEC 16)",
            "units_per_box": 16,
            "weight_box_kg": None,
            "sale_type": None,
            "is_active": True,
            "created_at": NOW,
            "updated_at": NOW,
        },
        4177: {
            "id": 4177,
            "barcode": "7802420125430",
            "product_id": 2928,
            "variant_id": 8943,
            "product_name": "MANI MARCO POLO",
            "variant_name": "CON PASAS 180 GR (SEC 16)",
            "units_per_box": 16,
            "weight_box_kg": None,
            "sale_type": None,
            "is_active": True,
            "created_at": NOW,
            "updated_at": NOW,
        },
    }


def _base_variants() -> dict[int, dict]:
    return {
        28941: {
            "variant_id": 28941,
            "product_id": 5594,
            "barcode": "7802420009518",
            "description": "SALADO 150 GR (SEC 16)",
            "units_per_box": 16,
        },
        23054: {
            "variant_id": 23054,
            "product_id": 5594,
            "barcode": "7802420125430",
            "description": "CON PASAS 180 GR (SEC 16)",
            "units_per_box": 16,
        },
    }


class FakeCursor:
    def __init__(
        self,
        *,
        rows: dict[int, dict],
        variants: dict[int, dict],
        extra_active: list[dict] | None = None,
        fail_update_id: int | None = None,
    ) -> None:
        self.rows = rows
        self.variants = variants
        self.extra_active = extra_active or []
        self.fail_update_id = fail_update_id
        self.sql: list[str] = []
        self.params: list[object] = []
        self.description: list[tuple[str]] = []
        self.rowcount = 0
        self._result: list[tuple] = []
        self.updates: list[tuple] = []

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.sql.append(normalized)
        self.params.append(params)
        self.rowcount = 0
        self._result = []

        if "FOR UPDATE" in normalized and "FROM bsale.products_master" in normalized:
            ids = sorted(int(x) for x in params[0])
            selected = [self.rows[i] for i in ids if i in self.rows]
            self.description = [(k,) for k in selected[0].keys()] if selected else []
            self._result = [tuple(row[col[0]] for col in self.description) for row in selected]
            return

        if "FROM bsale.variants" in normalized:
            ids = [int(x) for x in params[1]]
            selected = [self.variants[i] for i in ids if i in self.variants]
            selected.sort(key=lambda row: row["variant_id"])
            self.description = (
                [(k,) for k in selected[0].keys()] if selected else
                [("variant_id",), ("product_id",), ("barcode",), ("description",), ("units_per_box",)]
            )
            self._result = [tuple(row[col[0]] for col in self.description) for row in selected]
            return

        if (
            "FROM bsale.products_master" in normalized
            and "id <> ALL" in normalized
            and "is_active = TRUE" in normalized
        ):
            repair_ids = {int(x) for x in params[0]}
            target_variants = {int(x) for x in params[1]}
            target_barcodes = {str(x) for x in params[2]}
            conflicts = []
            for row in list(self.rows.values()) + self.extra_active:
                if int(row["id"]) in repair_ids:
                    continue
                if not row.get("is_active"):
                    continue
                barcode = str(row.get("barcode") or "").strip()
                if int(row["variant_id"]) in target_variants or barcode in target_barcodes:
                    conflicts.append(row)
            self.description = [("id",), ("variant_id",), ("barcode",), ("product_id",), ("is_active",)]
            self._result = [
                (row["id"], row["variant_id"], row["barcode"], row["product_id"], row["is_active"])
                for row in conflicts
            ]
            return

        if normalized.startswith("UPDATE bsale.products_master"):
            target_variant_id, target_product_id, row_id, barcode, old_variant, old_product = params
            row = self.rows.get(int(row_id))
            if self.fail_update_id == int(row_id):
                self.rowcount = 0
                self.description = []
                self._result = []
                return
            if (
                row
                and str(row["barcode"]).strip() == barcode
                and int(row["variant_id"]) == int(old_variant)
                and int(row["product_id"]) == int(old_product)
                and row.get("is_active") is True
            ):
                row["variant_id"] = int(target_variant_id)
                row["product_id"] = int(target_product_id)
                row["updated_at"] = NOW
                self.rowcount = 1
                self.description = [(k,) for k in row.keys()]
                self._result = [tuple(row[col[0]] for col in self.description)]
                self.updates.append(params)
            else:
                self.rowcount = 0
                self.description = []
                self._result = []
            return

        if (
            "FROM bsale.products_master" in normalized
            and "WHERE id = %s" in normalized
            and "FOR UPDATE" not in normalized
            and "variant_id = %s" not in normalized
        ):
            row = self.rows[int(params[0])]
            self.description = [(k,) for k in row.keys()]
            self._result = [tuple(row[col[0]] for col in self.description)]
            return

        if (
            "FROM bsale.products_master" in normalized
            and "variant_id = %s" in normalized
            and "is_active = TRUE" in normalized
        ):
            variant_id = int(params[0])
            selected = [
                row
                for row in self.rows.values()
                if row.get("is_active") and int(row["variant_id"]) == variant_id
            ]
            selected.sort(key=lambda row: row["id"])
            self.description = [("id",), ("barcode",), ("product_id",), ("variant_id",), ("is_active",)]
            self._result = [
                (row["id"], row["barcode"], row["product_id"], row["variant_id"], row["is_active"])
                for row in selected
            ]
            return

        if "FROM bsale.v_product_logistics" in normalized:
            ids = [int(x) for x in params[0]]
            counts: dict[int, list[int]] = {vid: [] for vid in ids}
            for row in self.rows.values():
                if row.get("is_active") and int(row["variant_id"]) in counts:
                    counts[int(row["variant_id"])].append(int(row["id"]))
            self.description = [("variant_id",), ("active_rows",), ("products_master_ids",)]
            self._result = [
                (vid, len(pm_ids), sorted(pm_ids))
                for vid, pm_ids in sorted(counts.items())
                if pm_ids
            ]
            return

        raise AssertionError(f"SQL no soportado en FakeCursor: {normalized}")

    def fetchall(self):
        return list(self._result)

    def fetchone(self):
        return self._result[0] if self._result else None

    def close(self):
        return None


class FakeConnection:
    def __init__(self, cursor: FakeCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def _factory(conn: FakeConnection):
    return lambda: conn


def test_dry_run_does_not_update_or_commit():
    cursor = FakeCursor(rows=_base_rows(), variants=_base_variants())
    conn = FakeConnection(cursor)

    report = job.run_repair(execute=False, connection_factory=_factory(conn))

    assert report["mode"] == "dry-run"
    assert report["wrote"] is False
    assert report["committed"] is False
    assert report["before"]["4152"]["variant_id"] == 8942
    assert report["projected"]["4152"]["variant_id"] == 28941
    assert report["projected"]["4177"]["variant_id"] == 23054
    assert report["validations"]["no_active_conflicts"] is True
    assert cursor.updates == []
    assert not any(sql.startswith("UPDATE") for sql in cursor.sql)
    assert conn.commits == 0
    assert conn.rollbacks >= 1
    assert conn.closed is True


def test_execute_updates_both_rows_and_commits():
    rows = _base_rows()
    cursor = FakeCursor(rows=rows, variants=_base_variants())
    conn = FakeConnection(cursor)

    report = job.run_repair(execute=True, connection_factory=_factory(conn))

    assert report["mode"] == "execute"
    assert report["wrote"] is True
    assert report["committed"] is True
    assert report["after"]["4152"]["variant_id"] == 28941
    assert report["after"]["4152"]["product_id"] == 5594
    assert report["after"]["4152"]["barcode"] == "7802420009518"
    assert report["after"]["4177"]["variant_id"] == 23054
    assert report["after"]["4177"]["product_id"] == 5594
    assert rows[4152]["variant_id"] == 28941
    assert rows[4177]["variant_id"] == 23054
    assert rows[3925]["variant_id"] == 8942
    assert rows[3924]["variant_id"] == 8943
    assert len(cursor.updates) == 2
    assert conn.commits == 1
    assert conn.closed is True


def test_guard_fails_when_current_state_changed():
    rows = _base_rows()
    rows[4152]["variant_id"] = 9999
    cursor = FakeCursor(rows=rows, variants=_base_variants())
    conn = FakeConnection(cursor)

    with pytest.raises(job.RepairGuardError, match="Estado inicial inesperado"):
        job.run_repair(execute=True, connection_factory=_factory(conn))

    assert cursor.updates == []
    assert conn.commits == 0
    assert conn.rollbacks >= 1


def test_active_conflict_aborts_before_write():
    rows = _base_rows()
    cursor = FakeCursor(
        rows=rows,
        variants=_base_variants(),
        extra_active=[
            {
                "id": 9999,
                "barcode": "7802420009518",
                "product_id": 5594,
                "variant_id": 28941,
                "is_active": True,
            }
        ],
    )
    conn = FakeConnection(cursor)

    with pytest.raises(job.RepairGuardError, match="fila activa conflictiva"):
        job.run_repair(execute=True, connection_factory=_factory(conn))

    assert cursor.updates == []
    assert conn.commits == 0
    assert conn.rollbacks >= 1


def test_update_rowcount_mismatch_rolls_back():
    rows = _base_rows()
    cursor = FakeCursor(
        rows=rows,
        variants=_base_variants(),
        fail_update_id=4152,
    )
    conn = FakeConnection(cursor)

    with pytest.raises(job.RepairGuardError, match="afectó 0 filas"):
        job.run_repair(execute=True, connection_factory=_factory(conn))

    assert rows[4152]["variant_id"] == 8942
    assert rows[4177]["variant_id"] == 8943
    assert conn.commits == 0
    assert conn.rollbacks >= 1


def test_cli_defaults_to_dry_run(monkeypatch, capsys):
    captured = {}

    def fake_run_repair(*, execute: bool = False, connection_factory=None):
        captured["execute"] = execute
        return {
            "mode": "dry-run" if not execute else "execute",
            "before": {},
            "after": None,
            "validations": {},
            "committed": False,
            "wrote": False,
        }

    monkeypatch.setattr(job, "load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(job, "run_repair", fake_run_repair)

    assert job.main([]) == 0
    assert captured["execute"] is False
    assert '"committed": false' in capsys.readouterr().out


def test_cli_execute_can_be_forced_back_to_dry_run(monkeypatch):
    captured = {}

    def fake_run_repair(*, execute: bool = False, connection_factory=None):
        captured["execute"] = execute
        return {
            "mode": "dry-run",
            "before": {},
            "after": None,
            "validations": {},
            "committed": False,
            "wrote": False,
        }

    monkeypatch.setattr(job, "load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(job, "run_repair", fake_run_repair)

    assert job.main(["--execute", "--dry-run"]) == 0
    assert captured["execute"] is False


def test_cli_guard_failure_exits_nonzero(monkeypatch, capsys):
    def boom(*, execute: bool = False, connection_factory=None):
        raise job.RepairGuardError("bloqueado")

    monkeypatch.setattr(job, "load_dotenv_if_available", lambda: None)
    monkeypatch.setattr(job, "run_repair", boom)

    assert job.main(["--execute"]) == 1
    err = capsys.readouterr().err
    assert '"ok": false' in err
    assert "bloqueado" in err
