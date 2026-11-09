"""Agregación P2: grupos adicionales, docs por cliente, sin inventar estados."""

from __future__ import annotations

from backend.services.distribuidora import dispatch_plan_pwa_driver_read_service as svc


def test_build_client_index_merges_groups_and_docs(monkeypatch):
    monkeypatch.setattr(
        svc.map_svc,
        "get_delivery_map",
        lambda plan_id: {
            "summary": {"orders": 3},
            "stops": [
                {
                    "customer_key": "id:10",
                    "customer_id": 10,
                    "customer_name": "Cliente Dual",
                    "fantasy_name": "",
                    "address": "Calle 1",
                    "city": "Ancud",
                    "latitude": -41.8,
                    "longitude": -73.8,
                    "has_coordinates": True,
                    "coordinates_source": "dispatch_plan_orders.lat/lng",
                    "orders": [
                        {"oc_document_id": 100, "oc_number": 70001, "route_order": 1},
                        {"oc_document_id": 101, "oc_number": 70566, "route_order": 2},
                    ],
                    "units_count": 5,
                },
                {
                    "customer_key": "name:sin coords",
                    "customer_id": None,
                    "customer_name": "Sin Coords",
                    "fantasy_name": "",
                    "address": "X",
                    "city": "Achao",
                    "latitude": None,
                    "longitude": None,
                    "has_coordinates": False,
                    "coordinates_source": None,
                    "orders": [
                        {"oc_document_id": 200, "oc_number": 70002, "route_order": 3},
                    ],
                    "units_count": 1,
                },
            ],
        },
    )
    monkeypatch.setattr(
        svc.plan_svc,
        "get_picking_by_client",
        lambda plan_id: {
            "ready": True,
            "picking_id": 1,
            "version": 1,
            "clients": [
                {
                    "client_id": 10,
                    "client_name": "Cliente Dual",
                    "address": "Calle 1",
                    "city": "Ancud",
                    "phone": "911",
                    "observations": "",
                    "oc_document_id": 100,
                    "related_document_id": 500,
                    "document_number": 111,
                    "document_type": "Factura",
                    "document_total": 1000,
                    "payment_method": "Contado",
                    "route_order": 1,
                },
                {
                    "client_id": 10,
                    "client_name": "Cliente Dual",
                    "address": "Calle 1",
                    "city": "Ancud",
                    "phone": "911",
                    "observations": "",
                    "oc_document_id": 101,
                    "related_document_id": 501,
                    "document_number": 2792373,
                    "document_type": "Boleta",
                    "document_total": 2000,
                    "payment_method": "Crédito",
                    "route_order": 2,
                },
                {
                    "client_id": None,
                    "client_name": "Sin Coords",
                    "address": "X",
                    "city": "Achao",
                    "phone": "",
                    "observations": "Obs",
                    "oc_document_id": 200,
                    "related_document_id": 502,
                    "document_number": 222,
                    "document_type": "Boleta",
                    "document_total": 500,
                    "payment_method": "",
                    "route_order": 3,
                },
            ],
        },
    )
    monkeypatch.setattr(
        svc,
        "_list_document_assignments",
        lambda plan_id: [
            {
                "related_document_id": 500,
                "load_batch_id": 1,
                "load_batch_name": "Grupo 1",
                "load_batch_sort_order": 1,
                "oc_document_id": 100,
            },
            {
                "related_document_id": 501,
                "load_batch_id": 9,
                "load_batch_name": "Grupo adicional",
                "load_batch_sort_order": 99,
                "oc_document_id": 101,
            },
            {
                "related_document_id": 502,
                "load_batch_id": 2,
                "load_batch_name": "consolidado",
                "load_batch_sort_order": 3,
                "oc_document_id": 200,
            },
        ],
    )
    monkeypatch.setattr(
        svc,
        "_units_by_related_document",
        lambda ids: {500: 3.0, 501: 4.0, 502: 1.0},
    )
    monkeypatch.setattr(
        svc,
        "_oc_number_by_oc_id",
        lambda plan_id: {100: 70001, 101: 70566, 200: 70002},
    )

    bundle = svc._build_client_index(79)
    assert bundle["summary"]["clients"] == 2
    dual = next(c for c in bundle["clients"] if c["customer_key"] == "id:10")
    assert dual["document_count"] == 2
    assert dual["units_total"] == 7.0
    assert {g["name"] for g in dual["load_groups"]} == {"Grupo 1", "Grupo adicional"}
    assert "status" not in dual
    assert dual["has_coordinates"] is True
    assert dual["phone"] == "911"

    no_gps = next(c for c in bundle["clients"] if "sin coords" in c["customer_key"])
    assert no_gps["has_coordinates"] is False
    assert no_gps["phone"] == ""
    assert no_gps["document_count"] == 1

    listed = svc.list_clients_for_driver(79, q="2792373")
    assert listed["count"] == 1
    assert listed["items"][0]["client_name"] == "Cliente Dual"

    monkeypatch.setattr(
        svc.plan_svc,
        "compute_picking_products_for_related_documents",
        lambda plan_id, rels: [
            {
                "producto": "Prod A",
                "variante": "Prod A",
                "producto_variante": "Prod A",
                "product_id": 1,
                "variant_id": 2,
                "codigo_barras": "780",
                "unidades": 4,
                "cajas": 1,
                "units_per_box": 4,
                "sin_unidad_caja": False,
            }
        ],
    )
    ficha = svc.get_client_ficha_for_driver(79, "id:10")
    assert ficha["document_count"] == 2
    assert ficha["documents"][1]["document_number"] == 2792373
    assert ficha["documents"][1]["oc_number"] == 70566
    assert ficha["documents"][1]["products"][0]["cajas_equivalentes"] == 1
    assert ficha["client"]["facade_photo_placeholder"] is True
    assert ficha["navigation"]["available"] is True

    mapa = svc.get_map_for_driver(79, origin_lat=-41.8, origin_lng=-73.8)
    assert mapa["count"] == 1
    assert mapa["markers"][0]["distance_m"] is not None
    assert mapa["device_location"]["tracking_persistent"] is False
