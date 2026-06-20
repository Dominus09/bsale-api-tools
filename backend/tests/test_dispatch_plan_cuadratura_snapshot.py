"""Tests snapshot cuadratura v2."""

from backend.utils.dispatch_plan_cuadratura_snapshot import (
    build_documents_from_picking_clients,
    build_product_catalog_from_picking,
    normalize_credit_notes,
    normalize_not_loaded_rows,
    resolve_product_from_catalog,
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


def test_normalize_credit_notes_legacy_fields():
    rows = normalize_credit_notes(
        [
            {
                "documento_venta": "49681",
                "numero_nc": "1234",
                "monto": 8500,
                "motivo": "Devolución",
            }
        ]
    )
    assert rows[0]["documento"] == "49681"
    assert rows[0]["nota_credito"] == "1234"
    assert rows[0]["observacion"] == "Devolución"


def test_resolve_product_by_barcode():
    catalog = build_product_catalog_from_picking(
        [
            {
                "producto": "Coca Cola",
                "variante": "3L",
                "codigo_barras": "7800001",
                "product_id": 10,
                "variant_id": 20,
            }
        ]
    )
    match = resolve_product_from_catalog("7800001", catalog)
    assert match is not None
    assert match["product_id"] == 10
    rows = normalize_not_loaded_rows(
        [{"producto": "7800001", "cantidad": 2, "motivo": "Sin stock"}],
        catalog,
    )
    assert rows[0]["codigo_barras"] == "7800001"
    assert rows[0]["product_id"] == 10
    assert "monto_clp" not in rows[0]
