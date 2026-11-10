"""Semáforo operativo + radio GPS 300 m (sin DB)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException

from backend.services.distribuidora import delivery_semaphore as sem


def test_haversine_same_point():
    assert sem.haversine_m(-33.45, -70.66, -33.45, -70.66) == 0.0


def test_haversine_approx_111m_per_0_001_lat():
    d = sem.haversine_m(-33.4500, -70.6500, -33.4510, -70.6500)
    assert 100 < d < 130


def test_within_300m_ok():
    out = sem.assert_within_incident_radius(
        driver_lat=-33.4500,
        driver_lng=-70.6500,
        client_lat=-33.4505,
        client_lng=-70.6500,
    )
    assert out["within_radius"] is True
    assert out["distance_m"] <= 300


def test_exactly_300m_accepted():
    # ~300 m al norte: 300/111320 ≈ 0.002695°
    delta = 300.0 / 111320.0
    out = sem.assert_within_incident_radius(
        driver_lat=-33.4500,
        driver_lng=-70.6500,
        client_lat=-33.4500 + delta,
        client_lng=-70.6500,
        max_m=300.0,
    )
    assert out["within_radius"] is True
    assert out["distance_m"] <= 300.5


def test_just_over_300m_rejected():
    delta = 310.0 / 111320.0
    with pytest.raises(HTTPException) as ei:
        sem.assert_within_incident_radius(
            driver_lat=-33.4500,
            driver_lng=-70.6500,
            client_lat=-33.4500 + delta,
            client_lng=-70.6500,
            max_m=300.0,
        )
    assert ei.value.detail["code"] == "GPS_OUT_OF_RANGE"


def test_outside_300m_message():
    with pytest.raises(HTTPException) as ei:
        sem.assert_within_incident_radius(
            driver_lat=-33.45,
            driver_lng=-70.66,
            client_lat=-33.46,
            client_lng=-70.70,
        )
    assert ei.value.status_code == 422
    assert ei.value.detail["code"] == "GPS_OUT_OF_RANGE"
    assert "300 metros" in ei.value.detail["message_es"]


def test_client_gps_missing():
    with pytest.raises(HTTPException) as ei:
        sem.assert_within_incident_radius(
            driver_lat=-33.45,
            driver_lng=-70.66,
            client_lat=None,
            client_lng=None,
        )
    assert ei.value.detail["code"] == "CLIENT_GPS_MISSING"


def test_fresh_gps_stale_rejected():
    # Antigüedad máx. 120 s: un fix de 3+ minutos no sirve como presencia actual.
    old = (datetime.now(timezone.utc) - timedelta(seconds=180)).isoformat()
    with pytest.raises(HTTPException) as ei:
        sem.assert_fresh_gps(
            latitude=-33.45,
            longitude=-70.66,
            gps_captured_at=old,
            accuracy_m=20,
        )
    assert ei.value.detail["code"] == "GPS_STALE"


def test_fresh_gps_ok():
    now = datetime.now(timezone.utc)
    out = sem.assert_fresh_gps(
        latitude=-33.45,
        longitude=-70.66,
        gps_captured_at=now.isoformat(),
        accuracy_m=25,
        now=now,
    )
    assert out["latitude"] == -33.45
    assert out["accuracy_warn"] is False


def test_semaphore_colors():
    assert sem.resolve_semaphore(status="completed")["color"] == "green"
    assert sem.resolve_semaphore(status="partial")["color"] == "yellow"
    red = sem.resolve_semaphore(status="not_delivered", result_reason="closed_store")
    assert red["color"] == "red"
    assert "LOCAL CERRADO" in red["whatsapp_header"]
    assert sem.resolve_semaphore(status=None)["color"] == "neutral"
    assert sem.resolve_semaphore(status="in_progress")["color"] == "active"


def test_whatsapp_headers_specific():
    r = sem.resolve_semaphore(status="not_delivered", result_reason="rejected_order")
    assert r["whatsapp_header"] == "🔴 QUILLOTANA | PEDIDO RECHAZADO"
    o = sem.resolve_semaphore(status="not_delivered", result_reason="other")
    assert o["whatsapp_header"] == "🔴 QUILLOTANA | NO ENTREGADO"
