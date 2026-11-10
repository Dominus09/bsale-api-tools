"""Semáforo operativo de entregas + radio GPS de incidencia (PWA 2.0).

No muta Bsale/picking. Validación de proximidad siempre en servidor.
"""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

# Radio máximo chofer↔cliente para local cerrado / rechazo.
INCIDENT_MAX_DISTANCE_M = 300.0
# Edad máxima de la posición GPS aceptada como "actual".
# 120 s: evita fijar un fix obsoleto; el navegador no prueba presencia incorruptible.
GPS_MAX_AGE_SECONDS = 120
# Precisión reportada por encima de esto → revisión (no inventa OK).
GPS_WARN_ACCURACY_M = 80.0

PROXIMITY_REQUIRED_REASONS = frozenset({"closed_store", "rejected_order"})
PHOTO_REQUIRED_REASONS = frozenset(
    {"closed_store", "rejected_order", "absent_customer", "not_found"}
)

SEMAPHORE_GREEN = "green"
SEMAPHORE_YELLOW = "yellow"
SEMAPHORE_RED = "red"
SEMAPHORE_NEUTRAL = "neutral"
SEMAPHORE_ACTIVE = "active"

REASON_LABEL_ES = {
    "closed_store": "Local cerrado",
    "not_found": "Local no encontrado",
    "rejected_order": "Pedido rechazado",
    "absent_customer": "Cliente ausente",
    "other": "No entregado",
    "physical_shortage": "Faltante físico",
    "damaged": "Producto dañado",
    "customer_rejection": "Rechazo del cliente",
    "prep_error": "Error de preparación",
}

WHATSAPP_HEADERS = {
    "completed": "🟢 QUILLOTANA | ENTREGA COMPLETA",
    "partial": "🟡 QUILLOTANA | ENTREGA PARCIAL",
    "closed_store": "🔴 QUILLOTANA | LOCAL CERRADO",
    "not_found": "🔴 QUILLOTANA | LOCAL NO ENCONTRADO",
    "rejected_order": "🔴 QUILLOTANA | PEDIDO RECHAZADO",
    "absent_customer": "🔴 QUILLOTANA | CLIENTE AUSENTE",
    "other_incident": "🔴 QUILLOTANA | NO ENTREGADO",
}


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlmb / 2) ** 2
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def parse_iso_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def resolve_semaphore(
    *,
    status: str | None,
    result_reason: str | None = None,
) -> dict[str, Any]:
    """Semáforo de visita (no de documento). Ignora anulaciones: el caller filtra."""
    st = (status or "").strip()
    reason = (result_reason or "").strip() or None
    if st == "completed":
        return {
            "color": SEMAPHORE_GREEN,
            "code": "completed",
            "label_es": "Entrega completa",
            "whatsapp_header": WHATSAPP_HEADERS["completed"],
            "result_reason": None,
        }
    if st == "partial":
        return {
            "color": SEMAPHORE_YELLOW,
            "code": "partial",
            "label_es": "Entrega parcial",
            "whatsapp_header": WHATSAPP_HEADERS["partial"],
            "result_reason": reason,
        }
    if st == "in_progress":
        return {
            "color": SEMAPHORE_ACTIVE,
            "code": "in_progress",
            "label_es": "En atención",
            "whatsapp_header": None,
            "result_reason": None,
        }
    if st == "not_delivered":
        key = reason if reason in WHATSAPP_HEADERS else "other_incident"
        if reason == "other":
            key = "other_incident"
        label = REASON_LABEL_ES.get(reason or "", "No entregado")
        return {
            "color": SEMAPHORE_RED,
            "code": reason or "not_delivered",
            "label_es": label,
            "whatsapp_header": WHATSAPP_HEADERS.get(key, WHATSAPP_HEADERS["other_incident"]),
            "result_reason": reason,
        }
    return {
        "color": SEMAPHORE_NEUTRAL,
        "code": "pending",
        "label_es": "Pendiente",
        "whatsapp_header": None,
        "result_reason": None,
    }


def assert_fresh_gps(
    *,
    latitude: float | None,
    longitude: float | None,
    gps_captured_at: str | None,
    accuracy_m: float | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Valida que haya posición usable y reciente. No inventa coordenadas."""
    if latitude is None or longitude is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "GPS_REQUIRED",
                "message": "Se requiere ubicación GPS actual del dispositivo",
                "message_es": "Se requiere ubicación GPS actual del dispositivo",
            },
        )
    lat = float(latitude)
    lng = float(longitude)
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lng <= 180.0):
        raise HTTPException(status_code=400, detail="Coordenadas GPS fuera de rango")
    captured = parse_iso_ts(gps_captured_at)
    if captured is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "GPS_STALE",
                "message": "Indique el momento de captura GPS (gps_captured_at)",
                "message_es": "Indique el momento de captura GPS",
            },
        )
    ref = now or datetime.now(timezone.utc)
    age = (ref - captured).total_seconds()
    if age < -60:
        raise HTTPException(status_code=422, detail="gps_captured_at en el futuro")
    if age > GPS_MAX_AGE_SECONDS:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "GPS_STALE",
                "message": "La posición GPS es demasiado antigua; vuelva a capturarla",
                "message_es": "La posición GPS es demasiado antigua; vuelva a capturarla",
            },
        )
    acc = float(accuracy_m) if accuracy_m is not None else None
    return {
        "latitude": lat,
        "longitude": lng,
        "accuracy_m": acc,
        "captured_at": captured,
        "age_seconds": age,
        "accuracy_warn": acc is not None and acc > GPS_WARN_ACCURACY_M,
    }


def assert_within_incident_radius(
    *,
    driver_lat: float,
    driver_lng: float,
    client_lat: float | None,
    client_lng: float | None,
    max_m: float = INCIDENT_MAX_DISTANCE_M,
) -> dict[str, Any]:
    if client_lat is None or client_lng is None:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "CLIENT_GPS_MISSING",
                "message": "El cliente no tiene coordenadas referenciales válidas",
                "message_es": (
                    "El cliente no tiene coordenadas referenciales. "
                    "Registre incidencia como pendiente de validación GPS."
                ),
            },
        )
    dist = haversine_m(
        float(driver_lat), float(driver_lng), float(client_lat), float(client_lng)
    )
    if dist > float(max_m):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "GPS_OUT_OF_RANGE",
                "message": (
                    "Debes estar a menos de 300 metros del cliente "
                    "para registrar esta incidencia"
                ),
                "message_es": (
                    "Debes estar a menos de 300 metros del cliente "
                    "para registrar esta incidencia"
                ),
                "distance_m": round(dist, 1),
                "max_distance_m": float(max_m),
            },
        )
    return {
        "distance_m": round(dist, 1),
        "max_distance_m": float(max_m),
        "within_radius": True,
    }
