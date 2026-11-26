"""Regresiones de ORS: timeouts, errores remotos y respuestas inválidas."""
from unittest.mock import patch

import pytest
import requests
from fastapi import HTTPException

from backend.utils.ors_client import (
    ORS_CONNECT_TIMEOUT_S,
    ORS_READ_TIMEOUT_S,
    OrsServiceError,
    get_route,
)

COORDS = [[-73.639, -43.131], [-73.640, -43.132], [-73.639, -43.131]]


def test_ors_request_has_bounded_timeout():
    response = requests.Response()
    response.status_code = 200
    response._content = b'{"routes":[{"summary":{"distance":1000,"duration":120}}]}'
    with patch("backend.utils.ors_client.requests.post", return_value=response) as post:
        out = get_route(COORDS)
    assert out["routes"]
    assert post.call_args.kwargs["timeout"] == (
        ORS_CONNECT_TIMEOUT_S,
        ORS_READ_TIMEOUT_S,
    )


def test_ors_timeout_has_safe_error_message():
    with patch(
        "backend.utils.ors_client.requests.post",
        side_effect=requests.exceptions.ReadTimeout("secret upstream details"),
    ):
        with pytest.raises(OrsServiceError, match="tiempo máximo") as exc:
            get_route(COORDS)
    assert "secret upstream details" not in str(exc.value)


def test_ors_html_502_never_leaks_to_user():
    response = requests.Response()
    response.status_code = 502
    response._content = b'<html><body>Cloudflare 502 <script>alert(1)</script></body></html>'
    with patch("backend.utils.ors_client.requests.post", return_value=response):
        with pytest.raises(OrsServiceError) as exc:
            get_route(COORDS)
    assert "HTTP 502" in str(exc.value)
    assert "<html>" not in str(exc.value)


def test_ors_http_503_is_unavailable_without_body_leak():
    response = requests.Response()
    response.status_code = 503
    response._content = b"<html>Service Unavailable token=abc</html>"
    with patch("backend.utils.ors_client.requests.post", return_value=response):
        with pytest.raises(OrsServiceError) as exc:
            get_route(COORDS)
    assert "HTTP 503" in str(exc.value)
    assert "no está disponible temporalmente" in str(exc.value)
    assert "<html>" not in str(exc.value)
    assert "token=abc" not in str(exc.value)


def test_ors_connection_error_is_safe():
    with patch(
        "backend.utils.ors_client.requests.post",
        side_effect=requests.exceptions.ConnectionError("proxy leaked Authorization: secret"),
    ):
        with pytest.raises(OrsServiceError, match="No fue posible conectar") as exc:
            get_route(COORDS)
    assert "Authorization" not in str(exc.value)
    assert "secret" not in str(exc.value)


def test_ors_retry_after_error_can_succeed():
    fail = requests.Response()
    fail.status_code = 502
    fail._content = b"<html>Cloudflare</html>"
    ok = requests.Response()
    ok.status_code = 200
    ok._content = b'{"routes":[{"summary":{"distance":2500,"duration":300}}]}'
    with patch("backend.utils.ors_client.requests.post", side_effect=[fail, ok]):
        with pytest.raises(OrsServiceError):
            get_route(COORDS)
        out = get_route(COORDS)
    assert out["routes"][0]["summary"]["distance"] == 2500


@pytest.mark.parametrize("payload", [b"<html>bad gateway</html>", b"{}",
                                     b'{"routes":[]}', b'{"routes":"bad"}'])
def test_ors_rejects_invalid_success_response(payload):
    response = requests.Response()
    response.status_code = 200
    response._content = payload
    with patch("backend.utils.ors_client.requests.post", return_value=response):
        with pytest.raises(OrsServiceError):
            get_route(COORDS)


def test_planificacion_ors_routes_http_hides_upstream_exception_detail():
    from backend.routers.distribuidora_planificacion import (
        OrsRoutesRequestBody,
        post_planificacion_ors_routes,
    )

    body = OrsRoutesRequestBody(
        routes=[
            {
                "camion": "T1",
                "truck_id": 1,
                "stops": [{"document_id": 1, "lat": -43.13, "lng": -73.64}],
            }
        ]
    )
    with patch(
        "backend.routers.distribuidora_planificacion.compute_planificacion_ors_routes",
        side_effect=OrsServiceError("ORS no está disponible temporalmente. (HTTP 502)."),
    ):
        with pytest.raises(HTTPException) as exc:
            post_planificacion_ors_routes(body)
    assert exc.value.status_code == 502
    assert exc.value.detail == "No fue posible calcular las rutas ORS. Intente nuevamente."
    assert "HTTP 502" not in str(exc.value.detail)
    assert "<html>" not in str(exc.value.detail)


def test_ors_logs_only_safe_provider_diagnostics(caplog):
    response = requests.Response()
    response.status_code = 404
    response._content = b'{"error":{"code":2010,"message":"SECRET_COORDS_OR_TOKEN"}}'
    with patch("backend.utils.ors_client.requests.post", return_value=response):
        with pytest.raises(OrsServiceError):
            get_route(COORDS)
    assert "http_status=404" in caplog.text
    assert "ors_code=2010" in caplog.text
    assert "SECRET_COORDS_OR_TOKEN" not in caplog.text


def test_ors_contingency_http_explicit_null_metrics():
    from backend.routers.distribuidora_dispatch_plan import ConfirmDispatchPlanBody, confirm_plan
    body = ConfirmDispatchPlanBody(
        plan_session_id="test-session", truck_id=2, route_name="T2",
        diesel_price_per_liter=None, km_total=None, duration_min=None,
        liters_estimated=None, confirm_without_ors=True,
        orders=[{"oc_document_id": 1, "route_order": 1}],
    )
    with patch("backend.routers.distribuidora_dispatch_plan.svc.confirm_dispatch_plan", return_value={"plan": {"id": 1}}) as confirm:
        confirm_plan(body, _ops={})
    kwargs = confirm.call_args.kwargs
    assert kwargs["planning_origin"] == "ors"
    assert kwargs["ors_metrics_calculated"] is False
    assert kwargs["allow_ors_without_metrics"] is True
    assert kwargs["km_total"] is None
    assert kwargs["route_geometry"] is None


def test_ors_contingency_rejects_fabricated_metrics():
    from backend.routers.distribuidora_dispatch_plan import ConfirmDispatchPlanBody, confirm_plan
    body = ConfirmDispatchPlanBody(
        plan_session_id="test-session", truck_id=2, route_name="T2",
        diesel_price_per_liter=1600, km_total=12, duration_min=None,
        liters_estimated=None, confirm_without_ors=True,
        orders=[{"oc_document_id": 1}],
    )
    with patch("backend.routers.distribuidora_dispatch_plan.svc.confirm_dispatch_plan") as confirm:
        with pytest.raises(HTTPException) as exc:
            confirm_plan(body, _ops={})
        assert exc.value.status_code == 400
        confirm.assert_not_called()


@pytest.mark.parametrize("capacity,weights,expected", [
    (5600, [{"peso_total_kg": 4413.5, "cobertura_logistica": 100, "productos_sin_peso": 0}], None),
    (5600, [{"peso_total_kg": 6257.2, "cobertura_logistica": 100, "productos_sin_peso": 0}], "sobrecargado"),
    (5600, [{"peso_total_kg": 4413.5, "cobertura_logistica": 50, "productos_sin_peso": 1}], "Peso incompleto"),
    (None, [{"peso_total_kg": 100, "cobertura_logistica": 100}], "Capacidad"),
])
def test_ors_contingency_capacity(capacity, weights, expected):
    from backend.services.distribuidora.dispatch_plan_service import _validate_ors_contingency_capacity
    if expected:
        with pytest.raises(ValueError, match=expected):
            _validate_ors_contingency_capacity(weights, capacity)
    else:
        _validate_ors_contingency_capacity(weights, capacity)
