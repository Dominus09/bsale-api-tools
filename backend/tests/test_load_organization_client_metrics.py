"""Métricas y presentación de organización anticipada (sin auto-asignar)."""

from __future__ import annotations

from backend.services.distribuidora.dispatch_plan_load_organization_service import (
    build_client_rows,
    build_group_summaries,
    encode_batch_description,
    parse_batch_presentation_role,
)


def test_parse_and_encode_batch_role():
    assert parse_batch_presentation_role("role=priority") == "priority"
    assert parse_batch_presentation_role("role=consolidated\nnota") == "consolidated"
    assert parse_batch_presentation_role("solo nota") is None
    assert encode_batch_description("priority", "A") == "role=priority\nA"
    assert encode_batch_description("consolidated", None) == "role=consolidated"


def test_client_rows_keep_multi_oc_together_and_distinct_sku():
    orders = [
        {
            "oc_document_id": 1,
            "oc_number": 100,
            "client_id": 10,
            "client_name": "A",
            "oc_total_amount": 1000,
            "cantidad_unidades": 10,
            "peso_total_kg": 5,
            "cobertura_logistica": 100,
            "load_batch_id": 1,
            "load_batch_name": "G1",
        },
        {
            "oc_document_id": 2,
            "oc_number": 101,
            "client_id": 10,
            "client_name": "A",
            "oc_total_amount": 500,
            "cantidad_unidades": 4,
            "peso_total_kg": 2,
            "cobertura_logistica": 80,
            "load_batch_id": 1,
            "load_batch_name": "G1",
        },
        {
            "oc_document_id": 3,
            "oc_number": 200,
            "client_id": 20,
            "client_name": "B",
            "oc_total_amount": 200,
            "cantidad_unidades": 1,
            "peso_total_kg": 1,
            "cobertura_logistica": 100,
            "load_batch_id": None,
        },
    ]
    variants = {1: [1, 2, 3], 2: [3, 4], 3: [9]}
    clients = build_client_rows(orders, variants_by_oc=variants, boxes_by_oc={})
    assert len(clients) == 2
    a = next(c for c in clients if c["client_id"] == 10)
    assert a["oc_count"] == 2
    assert a["venta_oc_clp"] == 1500
    assert a["sku_distinct"] == 4  # 1,2,3,4 sin doble contar 3
    assert a["units"] == 14
    assert a["load_batch_id"] == 1
    assert "peso_cobertura_incompleta" in a["limitations"]


def test_group_summary_distinct_sku_not_sum_of_clients():
    clients = [
        {
            "client_id": 1,
            "client_name": "A",
            "oc_count": 1,
            "oc_document_ids": [10],
            "venta_oc_clp": 100,
            "sku_distinct": 45,
            "units": 10,
            "cajas_equivalentes": 5,
            "peso_conocido_kg": 20,
            "load_batch_id": 7,
            "limitations": [],
        },
        {
            "client_id": 2,
            "client_name": "B",
            "oc_count": 1,
            "oc_document_ids": [11],
            "venta_oc_clp": 80,
            "sku_distinct": 50,
            "units": 8,
            "cajas_equivalentes": 4,
            "peso_conocido_kg": 15,
            "load_batch_id": 7,
            "limitations": [],
        },
    ]
    # Variantes compartidas: total consolidado 60, no 95
    variants = {10: list(range(1, 46)), 11: list(range(40, 90))}
    batches = [{"id": 7, "name": "G", "description": "role=priority", "sort_order": 1}]
    summaries = build_group_summaries(clients, batches, variants_by_oc=variants)
    assert len(summaries) == 1
    s = summaries[0]
    assert s["sku_distinct"] == 89  # 1..89
    assert s["client_count"] == 2
    assert s["priority_review_warning"] is False  # exactamente 2 OK
    batches2 = [{"id": 7, "name": "G", "description": "role=priority", "sort_order": 1}]
    clients3 = clients + [
        {
            "client_id": 3,
            "client_name": "C",
            "oc_count": 1,
            "oc_document_ids": [12],
            "venta_oc_clp": 10,
            "sku_distinct": 1,
            "units": 1,
            "cajas_equivalentes": None,
            "peso_conocido_kg": 1,
            "load_batch_id": 7,
            "limitations": [],
        }
    ]
    s3 = build_group_summaries(clients3, batches2, variants_by_oc=variants)[0]
    assert s3["priority_review_warning"] is True
    # Consolidado no advierte
    batches_c = [
        {"id": 7, "name": "Resto", "description": "role=consolidated", "sort_order": 1}
    ]
    sc = build_group_summaries(clients3, batches_c, variants_by_oc={})[0]
    assert sc["priority_review_warning"] is False
