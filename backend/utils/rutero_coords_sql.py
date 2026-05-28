"""
Fragmentos SQL: coordenadas efectivas en bsale.rutero.

- ``lat`` / ``lon``: réplica desde bsale.clients (sync_rutero).
- ``lat_operacional`` / ``lon_operacional``: capturas app (no las toca sync_rutero).
"""

# Tabla aliased como r
R_LAT = "COALESCE(r.lat_operacional, r.lat)"
R_LON = "COALESCE(r.lon_operacional, r.lon)"
R_LAT_AS = f"{R_LAT} AS lat"
R_LON_AS = f"{R_LON} AS lon"

# Tabla sin alias (FROM bsale.rutero)
B_LAT = "COALESCE(lat_operacional, lat)"
B_LON = "COALESCE(lon_operacional, lon)"
B_LAT_AS = f"{B_LAT} AS lat"
B_LON_AS = f"{B_LON} AS lon"

WHERE_HAS_GEOREF_R = (
    f"({R_LAT}) IS NOT NULL AND ({R_LON}) IS NOT NULL "
    f"AND NOT (({R_LAT})::double precision = 0 AND ({R_LON})::double precision = 0)"
)

WHERE_SIN_GEOREF_R = (
    f"(({R_LAT}) IS NULL OR ({R_LON}) IS NULL "
    f"OR (({R_LAT})::double precision = 0 AND ({R_LON})::double precision = 0))"
)

WHERE_HAS_GEOREF_BARE = (
    f"({B_LAT}) IS NOT NULL AND ({B_LON}) IS NOT NULL "
    f"AND NOT (({B_LAT})::double precision = 0 AND ({B_LON})::double precision = 0)"
)

WHERE_SIN_GEOREF_BARE = (
    f"(({B_LAT}) IS NULL OR ({B_LON}) IS NULL "
    f"OR (({B_LAT})::double precision = 0 AND ({B_LON})::double precision = 0))"
)

# Pendiente operacional ERP/app: sin coords efectivas o estado pendiente
WHERE_SOLO_PENDIENTE_GEOREF_R = (
    f"(({R_LAT}) IS NULL OR ({R_LON}) IS NULL OR r.georef_estado = 'pendiente')"
)
