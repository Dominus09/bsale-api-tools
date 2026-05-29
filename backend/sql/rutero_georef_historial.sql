-- Auditoría de cambios de georef operacional en bsale.rutero

CREATE TABLE IF NOT EXISTS bsale.rutero_georef_historial (
    id BIGSERIAL PRIMARY KEY,
    ruta_id INTEGER NOT NULL,
    estado_anterior VARCHAR(30),
    estado_nuevo VARCHAR(30) NOT NULL,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    usuario VARCHAR(50),
    fecha TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    motivo TEXT
);

CREATE INDEX IF NOT EXISTS idx_rutero_georef_hist_ruta
    ON bsale.rutero_georef_historial (ruta_id, fecha DESC);

COMMENT ON TABLE bsale.rutero_georef_historial IS
    'Trazabilidad cambios georef_estado y coordenadas efectivas en rutero.';
