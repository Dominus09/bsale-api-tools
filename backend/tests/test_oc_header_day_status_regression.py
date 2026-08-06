"""Regresión: reconciliación OC actualiza día (OBSERVACIONES) y estado/factura."""

from __future__ import annotations

from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

from backend.services.distribuidora.oc_reconciliation_service import (
    compare_oc_state,
    observaciones_from_attributes_payload,
    reconcile_one_oc,
)
from backend.services.distribuidora import oc_reconciliation_service as reconciliation
from backend.services.distribuidora.orders_service import (
    _apply_status_fields_to_row,
    _enrich_row_delivery_day,
)
from backend.utils.delivery_day_detect import (
    resolve_delivery_day,
    sql_resolve_delivery_day,
)


def test_observaciones_from_attributes_payload():
    payload = {
        "items": [
            {"name": "FORMA DE PAGO", "value": "Efectivo"},
            {"name": "OBSERVACIONES", "value": "Jueves"},
        ]
    }
    assert observaciones_from_attributes_payload(payload) == "Jueves"
    assert observaciones_from_attributes_payload({"items": []}) is None


def test_compare_oc_detects_delivery_day_change_miercoles_to_jueves():
    doc = {
        "id": 1,
        "number": 100,
        "state": 0,
        "commercialState": 0,
        "totalAmount": 1000,
        "netAmount": 840,
        "taxAmount": 160,
        "office": {"id": 1},
        "document_type": {"id": 33},
    }
    pg = {
        "number": 100,
        "total_amount": 1000,
        "net_amount": 840,
        "tax_amount": 160,
        "state": 0,
        "commercial_state": 0,
    }
    details = [{"id": 10, "quantity": 1, "totalAmount": 1000, "variant": {"id": 1}}]
    pg_details = [{"detail_id": 10, "variant_id": 1, "quantity": 1, "total_amount": 1000}]
    diff = compare_oc_state(
        pg_document=pg,
        pg_details=pg_details,
        bsale_document=doc,
        bsale_details=details,
        pg_observaciones="Miercoles",
        bsale_observaciones="Jueves",
    )
    assert diff["matches"] is False
    assert diff["attributes"]["postgresql_delivery_day"] == "miercoles"
    assert diff["attributes"]["bsale_delivery_day"] == "jueves"
    assert diff["attributes"]["matches"] is False


def test_compare_matches_when_attributes_aligned():
    doc = {
        "id": 1,
        "number": 100,
        "state": 0,
        "commercialState": 0,
        "totalAmount": 1000,
        "netAmount": 840,
        "taxAmount": 160,
        "office": {"id": 1},
        "document_type": {"id": 33},
    }
    pg = {
        "number": 100,
        "total_amount": 1000,
        "net_amount": 840,
        "tax_amount": 160,
        "state": 0,
        "commercial_state": 0,
    }
    details = [{"id": 10, "quantity": 1, "totalAmount": 1000, "variant": {"id": 1}}]
    pg_details = [{"detail_id": 10, "variant_id": 1, "quantity": 1, "total_amount": 1000}]
    diff = compare_oc_state(
        pg_document=pg,
        pg_details=pg_details,
        bsale_document=doc,
        bsale_details=details,
        pg_observaciones="Jueves",
        bsale_observaciones="Jueves",
    )
    assert diff["matches"] is True
    assert diff["attributes"]["delivery_day_matches"] is True


def test_delivery_date_explicit_in_obs_beats_route_day():
    day, source = resolve_delivery_day("Jueves", None, "Miercoles")
    assert day == "jueves"
    assert source == "observacion"


def test_route_day_is_fallback_only():
    day, source = resolve_delivery_day(None, None, "Miercoles")
    assert day == "miercoles"
    assert source == "ruta"


def test_sql_resolve_uses_comments_even_if_obs_text_without_day():
    sql = sql_resolve_delivery_day("obs", "comments", "ruta")
    assert "COALESCE" in sql
    # Ya no bloquea comments cuando obs tiene texto
    assert "WHEN NULLIF(BTRIM(obs)" not in sql


def test_invoiced_priority_over_probable():
    row: dict = {"state": 0}
    conf = {
        "is_invoiced": True,
        "invoicing_document_id": 99,
        "invoicing_number": 123,
        "invoicing_document_type_id": 1,
    }
    prob = {"score": 95, "candidate_document_id": 88, "candidate_number": 1}
    _apply_status_fields_to_row(row, conf, prob)
    assert row["estado_real"] == "Facturada"
    assert row["status"]["code"] == "invoiced"
    assert row["status"]["source"] == "linked_invoice"
    assert row["invoice"]["number"] == 123


def test_cancelled_invoice_not_facturada_via_state():
    row: dict = {"state": 1}
    _apply_status_fields_to_row(row, None, None)
    assert row["estado_real"] == "Anulada"
    assert row["status"]["code"] == "cancelled"


def test_endpoint_fields_delivery_and_status_contract():
    row = {
        "observaciones": "Jueves",
        "comments": None,
        "dia_atencion": "Lunes",
        "state": 0,
    }
    _enrich_row_delivery_day(row)
    _apply_status_fields_to_row(
        row,
        {
            "is_invoiced": True,
            "invoicing_document_id": 1,
            "invoicing_number": 10,
            "invoicing_document_type_id": 1,
        },
        None,
    )
    assert row["delivery"]["day"] == "jueves"
    assert row["delivery"]["label"] == "Jueves"
    assert row["status"]["label"] == "Facturada"
    assert row["dia_entrega_detectado"] == "jueves"


def test_reconcile_apply_updates_attributes_and_related():
    folio = 70001
    local_id = 900001
    source_id = 900002
    bsale_doc = {
        "id": source_id,
        "number": folio,
        "state": 0,
        "commercialState": 0,
        "emissionDate": 1784505600,
        "generationDate": 1784653800,
        "modificationDate": 1784653900,
        "totalAmount": 5000,
        "netAmount": 4201,
        "taxAmount": 799,
        "office": {"id": 1},
        "document_type": {"id": 33},
        "client": {"id": 10},
        "user": {"id": 1},
    }
    details = [
        {
            "id": 1,
            "lineNumber": 0,
            "variant": {"id": 1, "code": "X"},
            "quantity": 1.0,
            "totalAmount": 5000,
            "netAmount": 4201,
            "taxAmount": 799,
        }
    ]
    attrs = {"items": [{"name": "OBSERVACIONES", "value": "Jueves", "id": 47}]}

    class Client:
        def get(self, path: str, params=None, **kwargs):
            if path == "/documents.json":
                return {"items": [bsale_doc], "count": 1}
            if path == f"/documents/{source_id}.json":
                return bsale_doc
            if path == f"/documents/{source_id}/details.json":
                return {"items": details, "count": 1}
            if path == f"/documents/{source_id}/attributes.json":
                return attrs
            if path == f"/documents/{source_id}/references.json":
                return {"items": []}
            if path.endswith(".json") and "/documents/" in path:
                return {"id": 0, "number": 0, "state": 8888, "office": {"id": 1}}
            raise AssertionError(path)

    pg_doc = {
        "document_id": local_id,
        "number": folio,
        "company_id": 3,
        "office_id": 1,
        "document_type_id": 33,
        "total_amount": 4000,
        "net_amount": 3361,
        "tax_amount": 639,
        "state": 0,
        "commercial_state": 0,
        "raw_data": {"id": source_id, "number": folio},
        "source_document_id": source_id,
        "source_hash": "old",
        "observaciones": "Miercoles",
        "invoice_link": {
            "has_confirmed_invoice_link": False,
            "has_probable_match": False,
        },
    }
    pg_details = [
        {"detail_id": 1, "variant_id": 1, "quantity": 1, "total_amount": 4000}
    ]

    fake_conn = MagicMock()
    fake_cur = MagicMock()
    fake_conn.cursor.return_value = fake_cur

    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(pg_doc, pg_details),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._assert_source_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service."
            "_assert_plan_invalidation_schema",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection",
            return_value=fake_conn,
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.upsert_documents",
        ) as upsert,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_details",
            return_value=1,
        ) as repl_det,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_attributes",
            return_value=1,
        ) as repl_attr,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_references",
            return_value=0,
        ) as repl_ref,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service."
            "recalculate_order_weight_in_transaction",
            return_value={"peso_total_kg": 12.5, "porcentaje_cobertura": 100},
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service."
            "_invalidate_affected_dispatch_plans",
            return_value=[],
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service."
            "document_dict_from_bsale",
            return_value={
                "document_id": local_id,
                "number": folio,
                "company_id": 3,
                "office_id": 1,
                "document_type_id": 33,
            },
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service."
            "load_local_observaciones",
            return_value="Jueves",
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service."
            "load_local_invoice_link_flags",
            return_value={"has_confirmed_invoice_link": True},
        ),
        patch(
            "backend.services.distribuidora.sync_related_service.sync_related_for_single_oc",
            return_value={"rows_inserted": 2, "items_api": 2, "http_calls": 3},
        ) as related,
        patch.object(reconciliation, "_oc_writer_locks", return_value=nullcontext()),
    ):
        report = reconcile_one_oc(
            Client(),
            folio=folio,
            dry_run=False,
        )

    assert report["wrote"] is True
    assert report["status"] == "synced"
    assert report["attributes_replaced"] == 1
    assert report["details_replaced"] == 1
    assert report["related_rows_inserted"] == 2
    assert report["delivery"]["bsale_observaciones"] == "Jueves"
    assert report["delivery"]["postgresql_observaciones"] == "Miercoles"
    upsert.assert_called_once()
    repl_det.assert_called_once()
    repl_attr.assert_called_once()
    repl_ref.assert_called_once()
    related.assert_called_once()


def test_reconcile_dry_run_does_not_write():
    folio = 70002
    local_id = 900010
    source_id = 900011
    bsale_doc = {
        "id": source_id,
        "number": folio,
        "state": 0,
        "commercialState": 0,
        "emissionDate": 1784505600,
        "generationDate": 1784653800,
        "totalAmount": 100,
        "netAmount": 84,
        "taxAmount": 16,
        "office": {"id": 1},
        "document_type": {"id": 33},
    }

    class Client:
        def get(self, path: str, params=None, **kwargs):
            if path == "/documents.json":
                return {"items": [bsale_doc], "count": 1}
            if path.endswith("/details.json"):
                return {"items": [], "count": 0}
            if path.endswith("/attributes.json"):
                return {"items": [{"name": "OBSERVACIONES", "value": "Jueves"}]}
            if path.endswith("/references.json"):
                return {"items": []}
            raise AssertionError(path)

    with (
        patch(
            "backend.services.distribuidora.oc_reconciliation_service._load_local_oc",
            return_value=(
                {
                    "document_id": local_id,
                    "number": folio,
                    "company_id": 3,
                    "office_id": 1,
                    "document_type_id": 33,
                    "total_amount": 100,
                    "net_amount": 84,
                    "tax_amount": 16,
                    "state": 0,
                    "commercial_state": 0,
                    "raw_data": {"id": source_id},
                    "source_document_id": source_id,
                    "source_hash": None,
                    "observaciones": "Miercoles",
                    "invoice_link": {"has_confirmed_invoice_link": False},
                },
                [],
            ),
        ),
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.replace_document_attributes"
        ) as repl_attr,
        patch(
            "backend.services.distribuidora.oc_reconciliation_service.get_connection"
        ) as get_conn,
    ):
        report = reconcile_one_oc(Client(), folio=folio, dry_run=True)

    assert report["dry_run"] is True
    assert report["wrote"] is False
    assert "dry_run" in report["status"]
    repl_attr.assert_not_called()
    get_conn.assert_not_called()


def test_repair_job_requires_confirm_for_apply():
    from backend.jobs import repair_oc_header_from_bsale as job

    with pytest.raises(SystemExit):
        job.main(["--order-number", "68513", "--apply"])
