"""Lectura/escritura de ``distribuidora.system_config``."""

from __future__ import annotations

import json
import logging
from typing import Any

from backend.db import get_connection

logger = logging.getLogger(__name__)

DIESEL_PRICE_KEY = "diesel_price_per_liter"
DEFAULT_DIESEL_CLP_PER_LITER = 1500.0


def _parse_diesel_clp(value_json: Any) -> float:
    if value_json is None:
        return DEFAULT_DIESEL_CLP_PER_LITER
    if isinstance(value_json, dict):
        clp = value_json.get("clp")
        if clp is not None:
            return float(clp)
    if isinstance(value_json, (int, float)):
        return float(value_json)
    if isinstance(value_json, str):
        try:
            data = json.loads(value_json)
            if isinstance(data, dict) and data.get("clp") is not None:
                return float(data["clp"])
            return float(data)
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return DEFAULT_DIESEL_CLP_PER_LITER


def get_diesel_price_per_liter(conn=None) -> float:
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
                (DIESEL_PRICE_KEY,),
            )
            row = cur.fetchone()
        except Exception as exc:
            logger.warning(
                "[ORS_STABILITY_DEBUG] get_diesel_price_per_liter fallback: %s",
                exc,
            )
            row = None
        cur.close()
        if not row:
            return DEFAULT_DIESEL_CLP_PER_LITER
        return _parse_diesel_clp(row[0])
    finally:
        if own and conn is not None:
            conn.close()


def set_diesel_price_per_liter(clp: float, conn=None) -> float:
    if clp <= 0:
        raise ValueError("diesel_price_per_liter debe ser > 0")
    own = conn is None
    if own:
        conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO distribuidora.system_config (key, value_json, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value_json = EXCLUDED.value_json,
                updated_at = NOW()
            """,
            (DIESEL_PRICE_KEY, json.dumps({"clp": round(float(clp), 2)})),
        )
        conn.commit()
        cur.close()
        return float(clp)
    finally:
        if own and conn is not None:
            conn.close()
