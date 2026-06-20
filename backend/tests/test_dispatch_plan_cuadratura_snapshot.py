"""Tests snapshot cuadratura v2."""

from backend.utils.dispatch_plan_cuadratura_snapshot import (
    build_documents_from_picking_clients,
    build_product_catalog_from_picking,
    enrich_not_loaded_rows,
    estimate_not_loaded_monto,
)


def test_build_documents_from_picking():
    clients = [
        {
            "related_document_id": 1,
            "document_number": 49681,
            "client_name": "Manila",
            "document_total": 42907,
            "payment_method": "Transferencia",
            "route_order": 1,
        }
    ]
    docs = build_documents_from_picking_clients(clients)
    assert len(docs) == 1
    assert docs[0]["monto_clp"] == 42907
    assert docs[0]["medio_pago"] == "transferencia"


def test_estimate_not_loaded_from_catalog():
    catalog = build_product_catalog_from_picking(
        [{"producto": "Coca Cola 3L", "unidades": 10, "total_monto": 50000}]
    )
    monto = estimate_not_loaded_monto(
        producto="Coca Cola 3L",
        cantidad=2,
        catalog=catalog,
    )
    assert monto == 10000
    rows = enrich_not_loaded_rows(
        [{"cliente": "Manila", "producto": "Coca Cola 3L", "cantidad": 2, "motivo": "Sin stock"}],
        catalog,
    )
    assert rows[0]["monto_clp"] == 10000
