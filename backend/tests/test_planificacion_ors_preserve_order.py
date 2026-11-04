"""preserve_order no debe reordenar; default sigue optimizando."""

from __future__ import annotations

from unittest.mock import patch

from backend.services.distribuidora.planificacion_ors_service import (
    compute_ors_route_for_truck,
)


def _stops():
    return [
        {"document_id": 10, "lat": -43.12, "lon": -73.64},
        {"document_id": 20, "lat": -43.15, "lon": -73.60},
        {"document_id": 30, "lat": -43.10, "lon": -73.62},
    ]


def test_preserve_order_keeps_input_sequence():
    fake_geom = {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

    def fake_merge(coords, _):
        return fake_geom, 12.5, 40.0

    with (
        patch(
            "backend.services.distribuidora.planificacion_ors_service.optimizar_secuencia_cerrado"
        ) as opt,
        patch(
            "backend.services.distribuidora.planificacion_ors_service._ors_route_merge_chunks",
            side_effect=fake_merge,
        ),
        patch(
            "backend.services.distribuidora.planificacion_ors_service.compute_route_costs"
        ) as costs,
    ):
        costs.return_value.as_dict.return_value = {
            "liters_estimated": 1.2,
            "fuel_cost_clp": 1000,
            "crew_cost_clp": 0,
            "driver_cost_clp": 0,
            "assistant_cost_clp": 0,
            "cost_breakdown": {"total_clp": 1000},
            "truck_id": 7,
            "truck_name": "T",
        }
        costs.return_value.truck_id = 7
        costs.return_value.truck_name = "T"

        row = compute_ors_route_for_truck(
            camion="T",
            stops=_stops(),
            truck_id=7,
            trucks_by_id={},
            trucks_by_name={},
            diesel_clp=1000.0,
            logistics_settings=object(),
            preserve_order=True,
        )
        opt.assert_not_called()
        assert row is not None
        assert [s["document_id"] for s in row["stops_ordered"]] == [10, 20, 30]
        assert row["preserve_order"] is True


def test_default_calls_optimizer():
    fake_geom = {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}

    def fake_opt(_depot, clientes):
        # reverse order to prove optimizer ran
        return 0.0, list(reversed(clientes))

    def fake_merge(coords, _):
        return fake_geom, 5.0, 10.0

    with (
        patch(
            "backend.services.distribuidora.planificacion_ors_service.optimizar_secuencia_cerrado",
            side_effect=fake_opt,
        ) as opt,
        patch(
            "backend.services.distribuidora.planificacion_ors_service._ors_route_merge_chunks",
            side_effect=fake_merge,
        ),
        patch(
            "backend.services.distribuidora.planificacion_ors_service.compute_route_costs"
        ) as costs,
    ):
        costs.return_value.as_dict.return_value = {
            "liters_estimated": 0.5,
            "fuel_cost_clp": 500,
            "crew_cost_clp": 0,
            "driver_cost_clp": 0,
            "assistant_cost_clp": 0,
            "cost_breakdown": {"total_clp": 500},
            "truck_id": 7,
            "truck_name": "T",
        }
        costs.return_value.truck_id = 7
        costs.return_value.truck_name = "T"

        row = compute_ors_route_for_truck(
            camion="T",
            stops=_stops(),
            truck_id=7,
            trucks_by_id={},
            trucks_by_name={},
            diesel_clp=1000.0,
            logistics_settings=object(),
            preserve_order=False,
        )
        opt.assert_called_once()
        assert [s["document_id"] for s in row["stops_ordered"]] == [30, 20, 10]
        assert row["preserve_order"] is False
