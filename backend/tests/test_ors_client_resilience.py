"""Regresiones de ORS: timeouts, errores remotos y respuestas inválidas."""
from unittest.mock import patch

import pytest
import requests

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


@pytest.mark.parametrize("payload", [b"<html>bad gateway</html>", b"{}",
                                     b'{"routes":[]}', b'{"routes":"bad"}'])
def test_ors_rejects_invalid_success_response(payload):
    response = requests.Response()
    response.status_code = 200
    response._content = payload
    with patch("backend.utils.ors_client.requests.post", return_value=response):
        with pytest.raises(OrsServiceError):
            get_route(COORDS)
