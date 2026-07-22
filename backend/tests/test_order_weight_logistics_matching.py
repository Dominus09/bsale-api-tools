"""Resolución cardinalidad-segura de logística para líneas de OC."""

from __future__ import annotations

import pytest

from backend.services import order_weight_service as service


def _query_row(**overrides):
    row = {
        "detail_id": 7001,
        "line_number": 1,
        "variant_id": 28941,
        "codigo": "SKU-1",
        "producto": "MANI",
        "cantidad_unitaria": 16,
        "units_per_box": 16,
        "peso_unitario_kg": 0.15,
        "peso_caja_kg": 2.4,
        "products_master_id": 4152,
        "product_name": "MANI MARCO POLO",
        "variante": "SALADO 150 GR",
        "bsale_product_name": "MANI MARCO POLO",
        "logistics_completed": True,
        "pm_updated_at": None,
        "last_bsale_sync_at": None,
        "height_cm": None,
        "width_cm": None,
        "length_cm": None,
        "product_id": 5594,
        "barcode": "7802420009518",
        "codigo_interno": "SKU-1",
        "join_variant_ok": True,
        "join_barcode_ok": False,
        "exists_in_pm": True,
        "logistics_match_status": "matched_variant",
        "conflicting_products_master_ids": [],
    }
    row.update(overrides)
    return row


class RowsCursor:
    def __init__(self, rows):
        self._rows = rows
        keys = list(rows[0]) if rows else []
        self.description = [(key,) for key in keys]
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        keys = [column[0] for column in self.description]
        return [tuple(row[key] for key in keys) for row in self._rows]


def test_unique_variant_match_produces_one_line():
    cur = RowsCursor([_query_row()])

    lines = service.compute_order_lines(cur, document_id=1, company_id=3)

    assert len(lines) == 1
    assert lines[0]["detail_id"] == 7001
    assert lines[0]["products_master_id"] == 4152
    assert lines[0]["peso_linea_kg"] == 2.4
    assert lines[0]["join_debug"]["join_correcto"] is True


def test_unique_barcode_fallback_produces_one_line():
    cur = RowsCursor(
        [
            _query_row(
                logistics_match_status="matched_barcode",
                join_variant_ok=False,
                join_barcode_ok=True,
            )
        ]
    )

    lines = service.compute_order_lines(cur, document_id=1, company_id=3)

    assert len(lines) == 1
    assert lines[0]["join_debug"]["join_correcto"] is False
    assert lines[0]["join_debug"]["join_por_barcode"] is True


def test_no_match_is_one_pending_weight_line():
    cur = RowsCursor(
        [
            _query_row(
                logistics_match_status="pending",
                units_per_box=None,
                peso_unitario_kg=None,
                peso_caja_kg=None,
                products_master_id=None,
                product_name=None,
                variante=None,
                logistics_completed=None,
                join_variant_ok=False,
                join_barcode_ok=False,
                exists_in_pm=False,
            )
        ]
    )

    lines = service.compute_order_lines(cur, document_id=1, company_id=3)

    assert len(lines) == 1
    assert lines[0]["estado_linea"] == "sin_peso"
    assert lines[0]["fuente_peso"] == "sin_datos"
    assert lines[0]["has_logistics_record"] is False
    assert lines[0]["join_debug"]["logistics_match_status"] == "pending"


def test_multiple_variant_matches_raise_domain_error_with_ids(caplog):
    cur = RowsCursor(
        [
            _query_row(
                logistics_match_status="conflict_variant",
                products_master_id=None,
                conflicting_products_master_ids=[3925, 4152],
            )
        ]
    )
    caplog.set_level("ERROR")

    with pytest.raises(service.LogisticsMatchConflictError) as exc_info:
        service.compute_order_lines(cur, document_id=1, company_id=3)

    error = exc_info.value
    assert error.variant_id == 28941
    assert error.detail_id == 7001
    assert error.match_type == "variant_id"
    assert error.products_master_ids == [3925, 4152]
    assert "products_master_ids=[3925, 4152]" in caplog.text


def test_ambiguous_match_aborts_before_snapshot_persistence(monkeypatch):
    class HeaderCursor:
        description = [
            ("document_id",),
            ("oc",),
            ("company_id",),
            ("office_id",),
        ]

        def execute(self, sql, params=None):
            pass

        def fetchone(self):
            return (1, 100, 3, 1)

    def raise_conflict(*args, **kwargs):
        raise service.LogisticsMatchConflictError(
            variant_id=28941,
            detail_id=7001,
            match_type="variant_id",
            products_master_ids=[10, 11],
        )

    persisted = False

    def record_persist(*args, **kwargs):
        nonlocal persisted
        persisted = True

    monkeypatch.setattr(service, "compute_order_lines", raise_conflict)
    monkeypatch.setattr(service, "_persist_snapshot", record_persist)

    with pytest.raises(service.LogisticsMatchConflictError):
        service.recalculate_order_weight_in_transaction(
            HeaderCursor(),
            document_id=1,
            company_id=3,
            persist=True,
        )

    assert persisted is False


def test_same_detail_id_never_produces_more_than_one_line(caplog):
    row = _query_row()
    cur = RowsCursor([row, dict(row)])
    caplog.set_level("WARNING")

    lines = service.compute_order_lines(cur, document_id=1, company_id=3)

    assert len(lines) == 1
    assert lines[0]["detail_id"] == 7001
    assert "order_weight_line_deduplicated detail_id=7001 occurrences=2" in caplog.text


def test_order_lines_sql_uses_only_cardinality_safe_canonical_source():
    normalized = " ".join(service._ORDER_LINES_SQL.split())

    assert "JOIN bsale.products_master" not in normalized
    assert normalized.count("FROM bsale.v_product_logistics pl") == 2
    assert normalized.count("COUNT(*)::integer AS match_count") == 2
    assert "pl_v.match_count = 0" in normalized
