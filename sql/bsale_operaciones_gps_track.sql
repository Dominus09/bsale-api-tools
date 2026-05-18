-- Puntos GPS de tracking (app móvil → POST /operaciones/gps_track)
CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.operaciones_gps_track (
  id BIGSERIAL PRIMARY KEY,
  vendedor_id VARCHAR(64) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  lng DOUBLE PRECISION NOT NULL,
  accuracy DOUBLE PRECISION NULL,
  speed DOUBLE PRECISION NULL,
  battery SMALLINT NULL,
  app_version VARCHAR(64) NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT chk_operaciones_gps_track_battery CHECK (
    battery IS NULL OR (battery >= 0 AND battery <= 100)
  )
);

COMMENT ON TABLE bsale.operaciones_gps_track IS
  'Trazas GPS de vendedores (cola móvil gps_track).';

CREATE INDEX IF NOT EXISTS idx_operaciones_gps_track_vendedor_ts
  ON bsale.operaciones_gps_track (vendedor_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_operaciones_gps_track_fecha
  ON bsale.operaciones_gps_track ((timestamp::date), vendedor_id);
