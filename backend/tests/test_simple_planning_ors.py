"""ORS acotado a simple_planning: fingerprint, consolidación y validación."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.services.distribuidora.simple_planning_ors_service import (
    SimplePlanningOrsError,
    calculate_simple_planning_route,
    route_input_fingerprint,
    validate_ors_snapshot_for_orders,
)


def test_route_input_fingerprint_stable():
    stops = [
        {"document_id": 1, "lat": -43.123456, "lng": -73.654321},
        {"document_id": 2, "lat": -43.1, "lng": -73.6},
    ]
    a = route_input_fingerprint(truck_id=5, stops=stops)
    b = route_input_fingerprint(truck_id=5, stops=stops)
    assert a == b
    assert (
        route_input_fingerprint(truck_id=6, stops=stops) != a
    )


def test_calculate_route_rejects_out_of_scope_docs():
    with patch(
        "backend.services.distribuidora.simple_planning_ors_service._load_stops_from_db",
        side_effect=SimplePlanningOrsError(
            "OC no autorizadas",
            code="documents_out_of_scope",
            http_status=403,
            details={"missing_document_ids": [99]},
        ),
    ):
        with pytest.raises(SimplePlanningOrsError) as ei:
            calculate_simple_planning_route(
                user_id=1,
                company_id=3,
                office_id=1,
                truck_id=2,
                document_ids=[99],
            )
        assert ei.value.code == "documents_out_of_scope"


def test_calculate_route_ok_preserve_order_and_snapshot():
    stops_geo = [
        {
            "document_id": 10,
            "folio": 100,
            "client_id": 1,
            "client_name": "A",
            "address": "Calle 1",
            "municipality": "X",
            "has_georef": True,
            "lat": -43.12,
            "lng": -73.64,
            "oc_document_ids": [10],
            "folios": [100],
        },
        {
            "document_id": 20,
            "folio": 200,
            "client_id": 2,
            "client_name": "B",
            "address": "Calle 2",
            "municipality": "Y",
            "has_georef": True,
            "lat": -43.15,
            "lng": -73.60,
            "oc_document_ids": [20],
            "folios": [200],
        },
    ]
    ors_row = {
        "distance_km": 22.5,
        "duration_min": 55.0,
        "geometry": {"type": "LineString", "coordinates": [[1, 2], [3, 4]]},
        "stops_ordered": [
            {"document_id": 10, "stop_index": 1},
            {"document_id": 20, "stop_index": 2},
        ],
        "liters_estimated": 3.1,
        "fuel_cost_clp": 4000,
        "crew_cost_clp": 10000,
        "cost_breakdown": {"total_clp": 14000},
        "depot": {"lat": -43.13, "lng": -73.63},
    }

    with (
        patch(
            "backend.services.distribuidora.simple_planning_ors_service._check_rate_limit"
        ),
        patch(
            "backend.services.distribuidora.simple_planning_ors_service._load_stops_from_db",
            return_value=(stops_geo, [], stops_geo),
        ),
        patch(
            "backend.services.distribuidora.simple_planning_ors_service.get_connection",
            return_value=MagicMock(),
        ),
        patch(
            "backend.services.distribuidora.simple_planning_ors_service.get_diesel_price_per_liter",
            return_value=900.0,
        ),
        patch(
            "backend.services.distribuidora.simple_planning_ors_service.load_active_trucks",
            return_value=({2: {"id": 2, "name": "Hyundai"}}, {"Hyundai": {"id": 2}}),
        ),
        patch(
            "backend.services.distribuidora.simple_planning_ors_service.get_logistics_cost_settings",
            return_value=object(),
        ),
        patch(
            "backend.services.distribuidora.simple_planning_ors_service.compute_ors_route_for_truck",
            return_value=ors_row,
        ) as compute,
    ):
        out = calculate_simple_planning_route(
            user_id=9,
            company_id=3,
            office_id=1,
            truck_id=2,
            document_ids=[10, 20],
            camion="Hyundai",
        )
        assert out["ok"] is True
        assert out["ors_metrics_calculated"] is True
        assert out["distance_km"] == 22.5
        assert out["ors_snapshot"]["route_geometry"]["type"] == "LineString"
        assert compute.call_args.kwargs["preserve_order"] is True
        assert [s["document_id"] for s in compute.call_args.kwargs["stops"]] == [10, 20]


def test_validate_ors_snapshot_rejects_truck_change():
    snap = {
        "truck_id": 2,
        "document_ids": [10],
        "route_fingerprint": "abc",
        "distance_km": 1,
        "route_geometry": {"type": "LineString", "coordinates": []},
    }
    with patch(
        "backend.services.distribuidora.simple_planning_ors_service._load_stops_from_db",
        return_value=(
            [{"document_id": 10, "lat": 1.0, "lng": 2.0}],
            [],
            [],
        ),
    ):
        assert (
            validate_ors_snapshot_for_orders(
                snap,
                truck_id=99,
                ordered_document_ids=[10],
                company_id=3,
                office_id=1,
            )
            is None
        )
