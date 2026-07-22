"""Persistencia idempotente y deduplicación de líneas del snapshot de peso."""

from __future__ import annotations

import pytest

from backend.services import order_weight_service as service
from backend.utils.order_weight_calc import aggregate_order_summary


def _line(*, quantity: float = 1, weight: float = 15) -> dict:
    return {
        "detail_id": 7001,
        "line_number": 0,
        "codigo": "SKU-1",
        "producto": "Producto",
        "variante": "Variante",
        "cantidad_unitaria": quantity,
        "cantidad_cajas": quantity,
        "units_per_box": 1,
        "peso_unitario_kg": 15,
        "peso_caja_kg": 15,
        "peso_linea_kg": weight,
        "fuente_peso": "manual",
        "estado_linea": "ok",
        "products_master_id": 10,
        "variant_id": 27383,
        "join_debug": {"match": "variant_id"},
    }


def _header() -> dict:
    return {
        "document_id": 3832233,
        "company_id": 3,
        "office_id": 1,
        "oc": 68199,
    }


class SnapshotCursor:
    def __init__(self) -> None:
        self.snapshot_id = 99
        self.snapshot_weight = 15.0
        self.lines: dict[int, tuple] = {}
        self._fetchone = None
        self.sql: list[str] = []

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.sql.append(normalized)
        if normalized.startswith("SELECT id, document_id, peso_total_kg"):
            self._fetchone = (
                self.snapshot_id,
                3832233,
                self.snapshot_weight,
                1,
                1,
                0,
                1,
                0,
                100,
                None,
                None,
            )
        elif normalized.startswith("INSERT INTO distribuidora.order_weight_snapshots"):
            self.snapshot_weight = float(params[4])
            self._fetchone = (self.snapshot_id,)
        elif normalized.startswith(
            "DELETE FROM distribuidora.order_weight_snapshot_lines"
        ):
            self.lines.clear()
        elif normalized.startswith(
            "INSERT INTO distribuidora.order_weight_snapshot_lines"
        ):
            detail_id = int(params[1])
            if detail_id in self.lines:
                raise AssertionError("simulated unique violation")
            self.lines[detail_id] = tuple(params)

    def fetchone(self):
        return self._fetchone


def test_recalculate_same_snapshot_twice_then_quantity_change_updates_line():
    cur = SnapshotCursor()
    first_lines = [_line()]
    first_summary = aggregate_order_summary(first_lines)

    assert service._persist_snapshot(
        cur,
        header=_header(),
        lines=first_lines,
        summary=first_summary,
        user_email=None,
    ) == 99
    assert service._persist_snapshot(
        cur,
        header=_header(),
        lines=[_line()],
        summary=aggregate_order_summary([_line()]),
        user_email=None,
    ) == 99
    assert len(cur.lines) == 1
    assert cur.lines[7001][6] == 1

    changed = _line(quantity=20, weight=300)
    service._persist_snapshot(
        cur,
        header=_header(),
        lines=[changed],
        summary=aggregate_order_summary([changed]),
        user_email=None,
    )
    assert len(cur.lines) == 1
    assert cur.lines[7001][6] == 20
    assert cur.lines[7001][11] == 300

    delete_positions = [
        index
        for index, sql in enumerate(cur.sql)
        if sql.startswith("DELETE FROM distribuidora.order_weight_snapshot_lines")
    ]
    insert_positions = [
        index
        for index, sql in enumerate(cur.sql)
        if sql.startswith("INSERT INTO distribuidora.order_weight_snapshot_lines")
    ]
    assert len(delete_positions) == 3
    assert all(delete < insert for delete, insert in zip(delete_positions, insert_positions))


def test_identical_duplicate_detail_is_deduplicated(caplog):
    cur = SnapshotCursor()
    lines = [_line(), dict(_line())]
    summary = aggregate_order_summary(lines)
    caplog.set_level("WARNING")

    service._persist_snapshot(
        cur,
        header=_header(),
        lines=lines,
        summary=summary,
        user_email=None,
    )

    assert len(lines) == 1
    assert len(cur.lines) == 1
    assert summary["peso_total_kg"] == 15
    assert (
        "snapshot_line_deduplicated snapshot_id=99 "
        "detail_id=7001 occurrences=2"
    ) in caplog.text


def test_conflicting_duplicate_detail_aborts_before_header_write(caplog):
    cur = SnapshotCursor()
    caplog.set_level("ERROR")
    conflicting = [_line(quantity=1, weight=15), _line(quantity=20, weight=300)]

    with pytest.raises(service.SnapshotLineConflictError, match="detail_id=7001"):
        service._persist_snapshot(
            cur,
            header=_header(),
            lines=conflicting,
            summary=aggregate_order_summary(conflicting),
            user_email=None,
        )

    assert not any(
        sql.startswith("INSERT INTO distribuidora.order_weight_snapshots")
        for sql in cur.sql
    )
    assert (
        "snapshot_line_conflict snapshot_id=99 "
        "detail_id=7001 occurrences=2"
    ) in caplog.text
