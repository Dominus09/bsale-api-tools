"""Cálculo de costos logísticos por ruta (combustible, personal, extensiones futuras)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)
from backend.services.distribuidora.system_config_service import (
    DEFAULT_DIESEL_CLP_PER_LITER,
    get_diesel_price_per_liter,
)

LOGISTICS_SETTINGS_KEY = "logistics_cost_settings"
DEFAULT_KM_PER_LITER = 8.0
DEFAULT_DRIVER_COST_CLP_PER_TRIP = 50895
DEFAULT_ASSISTANT_COST_CLP_PER_TRIP = 38102


@dataclass
class LogisticsCostSettings:
    consumption_tolerance_pct: float = 0.0
    ferry_cost_clp: float = 0.0
    toll_cost_clp_per_km: float = 0.0
    driver_cost_clp_per_hour: float = 0.0
    driver_cost_clp_per_trip: float = DEFAULT_DRIVER_COST_CLP_PER_TRIP
    assistant_cost_clp_per_trip: float = DEFAULT_ASSISTANT_COST_CLP_PER_TRIP
    bonus_clp_per_route: float = 0.0
    per_diem_clp_per_day: float = 0.0
    lodging_clp_per_night: float = 0.0
    enabled_modules: list[str] = field(default_factory=lambda: ["fuel", "crew"])

    def module_enabled(self, name: str) -> bool:
        return name in (self.enabled_modules or ["fuel"])

    def crew_rates(self) -> tuple[int, int]:
        return (
            int(round(self.driver_cost_clp_per_trip)),
            int(round(self.assistant_cost_clp_per_trip)),
        )


@dataclass
class RouteCostBreakdown:
    distance_km: float
    duration_min: float
    km_per_liter: float
    truck_name: str | None
    truck_id: int | None
    fuel_type: str
    liters_base: float
    liters_estimated: float
    fuel_cost_clp: int
    driver_count: int = 1
    assistant_count: int = 0
    driver_cost_clp: int = 0
    assistant_cost_clp: int = 0
    crew_cost_clp: int = 0
    ferry_cost_clp: int = 0
    toll_cost_clp: int = 0
    driver_hourly_cost_clp: int = 0
    bonus_cost_clp: int = 0
    per_diem_cost_clp: int = 0
    lodging_cost_clp: int = 0
    total_cost_clp: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "distance_km": self.distance_km,
            "duration_min": self.duration_min,
            "km_per_liter_used": self.km_per_liter,
            "truck_name": self.truck_name,
            "truck_id": self.truck_id,
            "fuel_type": self.fuel_type,
            "liters_base": round(self.liters_base, 2),
            "liters_estimated": round(self.liters_estimated, 2),
            "fuel_cost_clp": self.fuel_cost_clp,
            "driver_count": self.driver_count,
            "assistant_count": self.assistant_count,
            "driver_cost_clp": self.driver_cost_clp,
            "assistant_cost_clp": self.assistant_cost_clp,
            "crew_cost_clp": self.crew_cost_clp,
            "cost_breakdown": {
                "fuel_clp": self.fuel_cost_clp,
                "ferry_clp": self.ferry_cost_clp,
                "toll_clp": self.toll_cost_clp,
                "driver_clp": self.driver_hourly_cost_clp,
                "crew_clp": self.crew_cost_clp,
                "bonus_clp": self.bonus_cost_clp,
                "per_diem_clp": self.per_diem_cost_clp,
                "lodging_clp": self.lodging_cost_clp,
                "total_clp": self.total_cost_clp,
            },
        }


def _parse_logistics_settings(raw: Any) -> LogisticsCostSettings:
    if raw is None:
        return LogisticsCostSettings()
    data = raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return LogisticsCostSettings()
    if not isinstance(data, dict):
        return LogisticsCostSettings()
    mods = data.get("enabled_modules")
    if not isinstance(mods, list):
        mods = ["fuel", "crew"]
    return LogisticsCostSettings(
        consumption_tolerance_pct=float(data.get("consumption_tolerance_pct") or 0),
        ferry_cost_clp=float(data.get("ferry_cost_clp") or 0),
        toll_cost_clp_per_km=float(data.get("toll_cost_clp_per_km") or 0),
        driver_cost_clp_per_hour=float(data.get("driver_cost_clp_per_hour") or 0),
        driver_cost_clp_per_trip=float(
            data.get("driver_cost_clp_per_trip") or DEFAULT_DRIVER_COST_CLP_PER_TRIP
        ),
        assistant_cost_clp_per_trip=float(
            data.get("assistant_cost_clp_per_trip") or DEFAULT_ASSISTANT_COST_CLP_PER_TRIP
        ),
        bonus_clp_per_route=float(data.get("bonus_clp_per_route") or 0),
        per_diem_clp_per_day=float(data.get("per_diem_clp_per_day") or 0),
        lodging_clp_per_night=float(data.get("lodging_clp_per_night") or 0),
        enabled_modules=[str(m) for m in mods],
    )


def get_logistics_cost_settings(conn=None) -> LogisticsCostSettings:
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT value_json
                FROM distribuidora.system_config
                WHERE key = %s
                """,
                (LOGISTICS_SETTINGS_KEY,),
            )
            row = cur.fetchone()
        except Exception as exc:
            logger.warning(
                "[ORS_STABILITY_DEBUG] get_logistics_cost_settings fallback: %s",
                exc,
            )
            row = None
        cur.close()
        return _parse_logistics_settings(row[0] if row else None)
    finally:
        if own and conn is not None:
            conn.close()


def update_logistics_cost_settings(
    patch: dict[str, Any],
    conn=None,
) -> LogisticsCostSettings:
    """Fusiona parches en ``logistics_cost_settings`` (tarifas crew, módulos, etc.)."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        current = get_logistics_cost_settings(conn)
        data = {
            "consumption_tolerance_pct": current.consumption_tolerance_pct,
            "ferry_cost_clp": current.ferry_cost_clp,
            "toll_cost_clp_per_km": current.toll_cost_clp_per_km,
            "driver_cost_clp_per_hour": current.driver_cost_clp_per_hour,
            "driver_cost_clp_per_trip": current.driver_cost_clp_per_trip,
            "assistant_cost_clp_per_trip": current.assistant_cost_clp_per_trip,
            "bonus_clp_per_route": current.bonus_clp_per_route,
            "per_diem_clp_per_day": current.per_diem_clp_per_day,
            "lodging_clp_per_night": current.lodging_clp_per_night,
            "enabled_modules": current.enabled_modules,
        }
        for key, val in patch.items():
            if val is not None:
                data[key] = val
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO distribuidora.system_config (key, value_json, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value_json = EXCLUDED.value_json,
                updated_at = NOW()
            """,
            (LOGISTICS_SETTINGS_KEY, json.dumps(data)),
        )
        conn.commit()
        cur.close()
        return _parse_logistics_settings(data)
    finally:
        if own and conn is not None:
            conn.close()


def crew_config_as_dict(settings: LogisticsCostSettings | None = None) -> dict[str, Any]:
    st = settings or get_logistics_cost_settings()
    d_rate, a_rate = st.crew_rates()
    return {
        "driver_cost_clp_per_trip": d_rate,
        "assistant_cost_clp_per_trip": a_rate,
        "bonus_clp_per_route": int(round(st.bonus_clp_per_route)),
        "per_diem_clp_per_day": int(round(st.per_diem_clp_per_day)),
        "lodging_clp_per_night": int(round(st.lodging_clp_per_night)),
        "enabled_modules": list(st.enabled_modules or []),
    }


def load_active_trucks(conn=None) -> tuple[dict[int, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Por id y por nombre normalizado (trim, case-insensitive key)."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, name, plate, max_weight_kg,
                       COALESCE(km_per_liter, %s) AS km_per_liter,
                       COALESCE(fuel_type, 'diesel') AS fuel_type
                FROM distribuidora.trucks
                WHERE active = TRUE
                ORDER BY name
                """,
                (DEFAULT_KM_PER_LITER,),
            )
            cols = [d[0] for d in cur.description]
            by_id: dict[int, dict[str, Any]] = {}
            by_name: dict[str, dict[str, Any]] = {}
            for r in cur.fetchall():
                row = dict(zip(cols, r))
                rid = int(row["id"])
                kpl = float(row.get("km_per_liter") or DEFAULT_KM_PER_LITER)
                if kpl <= 0:
                    kpl = DEFAULT_KM_PER_LITER
                row["id"] = rid
                row["km_per_liter"] = kpl
                row["fuel_type"] = str(row.get("fuel_type") or "diesel")
                by_id[rid] = row
                by_name[str(row["name"]).strip().lower()] = row
            cur.close()
            return by_id, by_name
        except Exception as exc:
            logger.warning(
                "[ORS_STABILITY_DEBUG] load_active_trucks fallback empty: %s",
                exc,
            )
            cur.close()
            return {}, {}
    finally:
        if own and conn is not None:
            conn.close()


def resolve_truck(
    truck_id: int | None,
    camion_label: str,
    trucks_by_id: dict[int, dict[str, Any]],
    trucks_by_name: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    if truck_id is not None and truck_id in trucks_by_id:
        return trucks_by_id[truck_id]
    label = (camion_label or "").strip().lower()
    if not label:
        return None
    for name_key, truck in trucks_by_name.items():
        if name_key in label or label.startswith(name_key):
            return truck
    for name_key, truck in trucks_by_name.items():
        if label in name_key:
            return truck
    return None


def compute_route_costs(
    *,
    distance_km: float,
    duration_min: float,
    truck: dict[str, Any] | None,
    truck_id: int | None,
    diesel_clp: float | None = None,
    settings: LogisticsCostSettings | None = None,
    driver_count: int = 1,
    assistant_count: int = 0,
    driver_cost_clp_per_trip: int | None = None,
    assistant_cost_clp_per_trip: int | None = None,
) -> RouteCostBreakdown:
    """
    Combustible según ``trucks.km_per_liter`` y diesel en system_config.
    Personal: (driver_count × tarifa chofer) + (assistant_count × tarifa peoneta) por vuelta.
    Ferry / peajes / chofer horario / bonos / viáticos: preparados (0 si no habilitado).
    """
    st = settings or LogisticsCostSettings()
    diesel = float(diesel_clp if diesel_clp is not None else DEFAULT_DIESEL_CLP_PER_LITER)
    kpl = DEFAULT_KM_PER_LITER
    truck_name = None
    fuel_type = "diesel"
    tid = truck_id
    if truck is not None:
        kpl = float(truck.get("km_per_liter") or DEFAULT_KM_PER_LITER)
        truck_name = str(truck.get("name") or "")
        fuel_type = str(truck.get("fuel_type") or "diesel")
        tid = int(truck["id"]) if truck.get("id") is not None else truck_id
    if kpl <= 0:
        kpl = DEFAULT_KM_PER_LITER

    km = max(0.0, float(distance_km))
    mins = max(0.0, float(duration_min))

    liters_base = km / kpl
    tol_mult = 1.0 + (max(0.0, st.consumption_tolerance_pct) / 100.0)
    liters = liters_base * tol_mult
    fuel_clp = int(round(liters * diesel)) if st.module_enabled("fuel") else 0

    ferry_clp = int(round(st.ferry_cost_clp)) if st.module_enabled("ferry") else 0
    toll_clp = (
        int(round(km * st.toll_cost_clp_per_km))
        if st.module_enabled("tolls")
        else 0
    )
    driver_hourly_clp = (
        int(round((mins / 60.0) * st.driver_cost_clp_per_hour))
        if st.module_enabled("driver")
        else 0
    )

    d_count = max(0, int(driver_count))
    a_count = max(0, int(assistant_count))
    default_d, default_a = st.crew_rates()
    d_unit = int(driver_cost_clp_per_trip if driver_cost_clp_per_trip is not None else default_d)
    a_unit = int(
        assistant_cost_clp_per_trip if assistant_cost_clp_per_trip is not None else default_a
    )
    crew_clp = 0
    if st.module_enabled("crew"):
        crew_clp = d_count * d_unit + a_count * a_unit

    bonus_clp = int(round(st.bonus_clp_per_route)) if st.module_enabled("bonus") else 0
    per_diem_clp = 0
    lodging_clp = 0

    total = fuel_clp + ferry_clp + toll_clp + driver_hourly_clp + crew_clp + bonus_clp
    total += per_diem_clp + lodging_clp

    return RouteCostBreakdown(
        distance_km=km,
        duration_min=mins,
        km_per_liter=round(kpl, 2),
        truck_name=truck_name,
        truck_id=tid,
        fuel_type=fuel_type,
        liters_base=liters_base,
        liters_estimated=liters,
        fuel_cost_clp=fuel_clp,
        driver_count=d_count,
        assistant_count=a_count,
        driver_cost_clp=d_unit,
        assistant_cost_clp=a_unit,
        crew_cost_clp=crew_clp,
        ferry_cost_clp=ferry_clp,
        toll_cost_clp=toll_clp,
        driver_hourly_cost_clp=driver_hourly_clp,
        bonus_cost_clp=bonus_clp,
        per_diem_cost_clp=per_diem_clp,
        lodging_cost_clp=lodging_clp,
        total_cost_clp=total,
    )
