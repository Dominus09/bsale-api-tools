"""Fragmentos SQL reutilizables para peso y observaciones en planificación."""

from __future__ import annotations

PLANNING_LATEST_OBS_LATERAL = """
LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(da.attribute_value), '') AS observaciones
    FROM distribuidora.document_attributes da
    WHERE da.document_id = d.document_id
      AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
    ORDER BY da.id DESC NULLS LAST
    LIMIT 1
) latest_obs ON TRUE
"""

PLANNING_WEIGHT_PLACEHOLDER = """
NULL::numeric AS peso_total_kg,
NULL::numeric AS weight_kg,
NULL::int AS productos_sin_peso,
NULL::numeric AS porcentaje_cobertura_peso
"""

# Alias legacy (auditoría logística); planificación usa OrderWeightSummary.
PLANNING_WEIGHT_SELECT = PLANNING_WEIGHT_PLACEHOLDER

# Deprecated: planificación no usa join lateral de peso.
PLANNING_WEIGHT_LATERAL = ""

ORDER_WEIGHT_METRICS_SQL = """
SELECT
    dd.document_id,
    ROUND(
        COALESCE(SUM(dd.quantity * COALESCE(pl.weight_unit_kg, 0)), 0)::numeric,
        3
    ) AS peso_total_kg,
    COUNT(*) FILTER (WHERE COALESCE(dd.quantity, 0) > 0)::int AS productos_con_cantidad,
    COUNT(*) FILTER (
        WHERE COALESCE(dd.quantity, 0) > 0
          AND (pl.weight_unit_kg IS NULL OR pl.weight_unit_kg <= 0)
    )::int AS productos_sin_peso,
    CASE
        WHEN COUNT(*) FILTER (WHERE COALESCE(dd.quantity, 0) > 0) > 0
        THEN ROUND(
            100.0 * (
                COUNT(*) FILTER (
                    WHERE COALESCE(dd.quantity, 0) > 0
                      AND pl.weight_unit_kg IS NOT NULL
                      AND pl.weight_unit_kg > 0
                )::numeric
                / COUNT(*) FILTER (WHERE COALESCE(dd.quantity, 0) > 0)::numeric
            ),
            1
        )
        ELSE 0::numeric
    END AS porcentaje_cobertura_peso
FROM distribuidora.document_details dd
LEFT JOIN bsale.v_product_logistics pl ON pl.variant_id = dd.variant_id
WHERE dd.document_id = ANY(%s::bigint[])
GROUP BY dd.document_id
"""

# No referencia d.bsale_modified_at: la migración 041 puede no estar aplicada aún.
PLANNING_LAST_BS_UPDATE_EXPR = """COALESCE(
    CASE
        WHEN d.raw_data->>'modificationDate' ~ '^[0-9]+$'
        THEN to_timestamp((d.raw_data->>'modificationDate')::bigint)
        ELSE NULL
    END,
    d.generation_date
)"""

PLANNING_OBSERVACIONES_EXPR = """
NULLIF(
    BTRIM(
        CONCAT_WS(
            E'\\n',
            latest_obs.observaciones,
            NULLIF(BTRIM(d.raw_data->>'comments'), '')
        )
    ),
    ''
)
"""

LATEST_OBS_LATERAL_LIVE = """
LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(da.attribute_value), '') AS observaciones
    FROM distribuidora.document_attributes da
    WHERE da.document_id = d.document_id
      AND UPPER(BTRIM(da.attribute_name)) = 'OBSERVACIONES'
    ORDER BY da.id DESC NULLS LAST
    LIMIT 1
) latest_obs ON TRUE
"""
