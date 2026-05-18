-- Telemetría app móvil → panel operaciones (heartbeat GPS / batería / sync)
-- Ejecutar una vez en PostgreSQL (schema bsale).

CREATE SCHEMA IF NOT EXISTS bsale;

CREATE TABLE IF NOT EXISTS bsale.operaciones_heartbeat (
  id BIGSERIAL PRIMARY KEY,
  vendedor_id VARCHAR(64) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  lat DOUBLE PRECISION NULL,
  lng DOUBLE PRECISION NULL,
  bateria SMALLINT NULL,
  conexion VARCHAR(32) NULL,
  pendientes INTEGER NULL,
  app_version VARCHAR(64) NULL,
  dispositivo VARCHAR(128) NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT chk_operaciones_heartbeat_bateria CHECK (
    bateria IS NULL OR (bateria >= 0 AND bateria <= 100)
  ),
  CONSTRAINT chk_operaciones_heartbeat_pendientes CHECK (
    pendientes IS NULL OR pendientes >= 0
  )
);

COMMENT ON TABLE bsale.operaciones_heartbeat IS
  'Pulsos de telemetría desde app_distribuidora (ubicación, batería, pendientes sync).';

CREATE INDEX IF NOT EXISTS idx_operaciones_heartbeat_vendedor_ts
  ON bsale.operaciones_heartbeat (vendedor_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_operaciones_heartbeat_fecha_vendedor
  ON bsale.operaciones_heartbeat ((timestamp::date), vendedor_id);
