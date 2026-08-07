"""planning-rows / batch de pesos: snapshots masivos, sin N+1 ni Bsale."""

from __future__ import annotations

from backend.services import order_weight_service as service
from backend.services.distribuidora import orders_service


def test_batch_reads_snapshots_not_recalculate(monkeypatch):
    calls = {"calc": 0, "fetch": 0}

    def boom(*_a, **_k):
        calls["calc"] += 1
        raise AssertionError("calculate_order_weight no debe llamarse desde planning batch")

    def fake_fetch(ids):
        calls["fetch"] += 1
        return {
            int(ids[0]): {
                "peso_total_kg": 100.5,
                "productos_sin_peso": 0,
                "porcentaje_cobertura": 100.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 10,
                "productos_con_peso": 10,
                "calculated_at": "2026-08-06T12:00:00",
                "cantidad_unidades": 10,
                "cantidad_cajas": 1,
            }
        }

    monkeypatch.setattr(service, "calculate_order_weight", boom)
    monkeypatch.setattr(service, "fetch_weights_by_document_ids", fake_fetch)

    out = service.get_order_weight_summaries_batch(
        [111, 111],
        persist_cache=True,
        log_planning=False,
    )
    assert calls["calc"] == 0
    assert calls["fetch"] == 1
    assert out[111]["weight"]["status"] == "calculated"
    assert out[111]["weight"]["value_kg"] == 100.5
    assert out[111]["weight"]["source"] == "order_weight_snapshot"
    assert out[111]["peso_total_kg"] == 100.5


def test_batch_missing_snapshot_is_unavailable_not_zero(monkeypatch):
    monkeypatch.setattr(service, "fetch_weights_by_document_ids", lambda _ids: {})
    out = service.get_order_weight_summaries_batch([222], log_planning=False)
    assert out[222]["peso_total_kg"] is None
    assert out[222]["weight"]["value_kg"] is None
    assert out[222]["weight"]["status"] == "unavailable"
    assert out[222]["weight"]["reason"] == "snapshot_missing"


def test_batch_partial_snapshot(monkeypatch):
    monkeypatch.setattr(
        service,
        "fetch_weights_by_document_ids",
        lambda _ids: {
            333: {
                "peso_total_kg": 300.2,
                "productos_sin_peso": 23,
                "porcentaje_cobertura": 45.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 42,
                "productos_con_peso": 19,
                "calculated_at": "2026-08-06T12:00:00",
                "cantidad_unidades": 100,
                "cantidad_cajas": 5,
            }
        },
    )
    out = service.get_order_weight_summaries_batch([333], log_planning=False)
    assert out[333]["weight"]["status"] == "partial"
    assert out[333]["weight"]["value_kg"] == 300.2
    assert out[333]["weight"]["missing_lines"] == 23


def test_batch_query_count_stable_for_20_and_500(monkeypatch):
    counts: list[int] = []

    def fake_fetch(ids):
        counts.append(len(ids))
        return {
            i: {
                "peso_total_kg": 1.0,
                "productos_sin_peso": 0,
                "porcentaje_cobertura": 100.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 1,
                "productos_con_peso": 1,
                "calculated_at": None,
                "cantidad_unidades": 1,
                "cantidad_cajas": 0,
            }
            for i in ids
        }

    monkeypatch.setattr(service, "fetch_weights_by_document_ids", fake_fetch)
    service.get_order_weight_summaries_batch(list(range(20)), log_planning=False)
    service.get_order_weight_summaries_batch(list(range(500)), log_planning=False)
    # Una sola llamada fetch por batch (no N+1), independiente del tamaño.
    assert len(counts) == 2
    assert counts == [20, 500]


def test_overlay_uses_snapshot_batch(monkeypatch):
    calc_calls = {"n": 0}

    def boom(*_a, **_k):
        calc_calls["n"] += 1
        raise AssertionError("no recalc")

    monkeypatch.setattr(service, "calculate_order_weight", boom)
    monkeypatch.setattr(
        service,
        "fetch_weights_by_document_ids",
        lambda ids: {
            1: {
                "peso_total_kg": 10.0,
                "productos_sin_peso": 0,
                "porcentaje_cobertura": 100.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 2,
                "productos_con_peso": 2,
                "calculated_at": None,
                "cantidad_unidades": 2,
                "cantidad_cajas": 0,
            }
        },
    )
    rows = [{"document_id": 1, "oc": 100}]
    orders_service._overlay_order_weights_to_rows(rows)
    assert calc_calls["n"] == 0
    assert rows[0]["peso_total_kg"] == 10.0
    assert rows[0]["weight"]["status"] == "calculated"
    assert rows[0]["peso_fuente"] == "order_weight_snapshot"


def test_weights_by_docs_sql_is_bulk_any():
    assert "ANY(%s::bigint[])" in service._WEIGHTS_BY_DOCS_SQL
    assert "document_id = %s" not in service._WEIGHTS_BY_DOCS_SQL.replace(
        "document_id = ANY(%s::bigint[])", ""
    )
    # Listado: solo header de snapshot (sin JOIN a líneas).
    assert "order_weight_snapshot_lines" not in service._WEIGHTS_BY_DOCS_SQL
    assert "GROUP BY" not in service._WEIGHTS_BY_DOCS_SQL


def test_recalc_batch_still_calculates(monkeypatch):
    """ensure/recalculate sí puede recalcular; planning-rows no lo usa."""

    def fake_calc(doc_id, **_k):
        return {
            "productos_totales": 1,
            "productos_con_peso": 1,
            "productos_sin_peso": 0,
            "productos_manuales": 0,
            "productos_estimados": 0,
            "peso_total_kg": 5.0,
            "porcentaje_cobertura": 100.0,
            "lines": [
                {
                    "cantidad_unitaria": 1,
                    "cantidad_cajas": 0,
                    "peso_linea_kg": 5.0,
                    "estado_linea": "completo",
                }
            ],
            "weight": {
                "value_kg": 5.0,
                "status": "calculated",
                "source": "product_lines",
                "reason": None,
            },
        }

    monkeypatch.setattr(service, "calculate_order_weight", fake_calc)
    out = service.calculate_order_weights_batch([9], persist_cache=False)
    assert out[9]["peso_total_kg"] == 5.0


def test_snapshot_with_peso_survives_zero_productos_totales():
    """Regresión canario: peso persistido no debe volverse null/unavailable."""
    payload = service.weight_payload_from_snapshot(
        {
            "peso_total_kg": 37.353,
            "productos_totales": 0,
            "productos_sin_peso": 0,
            "porcentaje_cobertura": 100.0,
            "calculated_at": "2026-08-07T20:00:00",
        }
    )
    assert payload["value_kg"] == 37.353
    assert payload["status"] == "calculated"


def test_canary_local_ne_source_overlay_keeps_37_353(monkeypatch):
    """68701: local 3853320 ≠ source 3853408; batch key = document_id local."""
    local_id = 3853320

    def fake_fetch(ids):
        assert local_id in [int(x) for x in ids]
        return {
            local_id: {
                "peso_total_kg": 37.353,
                "productos_sin_peso": 0,
                "porcentaje_cobertura": 100.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 6,
                "productos_con_peso": 6,
                "calculated_at": "2026-08-07T20:00:00",
                "cantidad_unidades": 0,
                "cantidad_cajas": 0,
            }
        }

    monkeypatch.setattr(service, "fetch_weights_by_document_ids", fake_fetch)
    rows = [{"document_id": local_id, "oc": 68701, "peso_total_kg": None}]
    orders_service._overlay_order_weights_to_rows(rows)
    assert rows[0]["peso_total_kg"] == 37.353
    assert rows[0]["weight"]["status"] == "calculated"
    assert rows[0]["weight"]["value_kg"] == 37.353


def test_canary_local_eq_source_partial_13_52(monkeypatch):
    """68700: local == source; coverage 75 % → partial, no unavailable."""
    doc_id = 3853317
    monkeypatch.setattr(
        service,
        "fetch_weights_by_document_ids",
        lambda _ids: {
            doc_id: {
                "peso_total_kg": 13.52,
                "productos_sin_peso": 1,
                "porcentaje_cobertura": 75.0,
                "productos_manuales": 0,
                "productos_estimados": 0,
                "productos_totales": 4,
                "productos_con_peso": 3,
                "calculated_at": "2026-08-07T20:00:00",
                "cantidad_unidades": 0,
                "cantidad_cajas": 0,
            }
        },
    )
    rows = [{"document_id": doc_id, "oc": 68700, "peso_total_kg": None}]
    orders_service._overlay_order_weights_to_rows(rows)
    assert rows[0]["peso_total_kg"] == 13.52
    assert rows[0]["weight"]["status"] == "partial"
    assert rows[0]["weight"]["value_kg"] == 13.52


def test_weights_sql_uses_local_document_id_not_source():
    sql = service._WEIGHTS_BY_DOCS_SQL
    assert "ows.document_id = ANY(%s::bigint[])" in sql
    assert "source_document_id" not in sql
