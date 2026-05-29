-- Georreferencia operacional en bsale.rutero (no toca bsale.clients ni sync Bsale API).
-- lat/lon = réplica BSALE (sync_rutero). lat_operacional/lon_operacional = capturas app.

ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS lat_operacional DOUBLE PRECISION;

ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS lon_operacional DOUBLE PRECISION;

ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS georef_estado VARCHAR(30) NOT NULL DEFAULT 'pendiente';

ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS georef_actualizada_at TIMESTAMPTZ;

ALTER TABLE bsale.rutero
    ADD COLUMN IF NOT EXISTS georef_actualizada_por VARCHAR(50);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_rutero_georef_estado'
          AND conrelid = 'bsale.rutero'::regclass
    ) THEN
        ALTER TABLE bsale.rutero
            ADD CONSTRAINT chk_rutero_georef_estado
            CHECK (georef_estado IN ('pendiente', 'capturada', 'aplicada'));
    END IF;
END $$;

COMMENT ON COLUMN bsale.rutero.lat IS 'Coordenadas réplica desde bsale.clients (sync_rutero).';
COMMENT ON COLUMN bsale.rutero.lon IS 'Coordenadas réplica desde bsale.clients (sync_rutero).';
COMMENT ON COLUMN bsale.rutero.lat_operacional IS
    'Georef capturada en terreno (app); no la sobrescribe sync_rutero.';
COMMENT ON COLUMN bsale.rutero.lon_operacional IS
    'Georef capturada en terreno (app); no la sobrescribe sync_rutero.';
COMMENT ON COLUMN bsale.rutero.georef_estado IS
    'Estado operacional: pendiente | capturada (app) | aplicada (BSALE manual).';
COMMENT ON COLUMN bsale.rutero.georef_actualizada_at IS 'Última actualización de georef en rutero.';
COMMENT ON COLUMN bsale.rutero.georef_actualizada_por IS 'Código vendedor o usuario staff que actualizó.';

CREATE INDEX IF NOT EXISTS idx_rutero_georef_estado
    ON bsale.rutero (georef_estado)
    WHERE activo = TRUE;

-- Migración: capturas previas guardadas por error en lat/lon
UPDATE bsale.rutero
SET
    lat_operacional = lat,
    lon_operacional = lon
WHERE georef_estado = 'capturada'
  AND lat_operacional IS NULL
  AND lon_operacional IS NULL
  AND lat IS NOT NULL
  AND lon IS NOT NULL;

CREATE OR REPLACE VIEW bsale.v_clientes_sin_georef AS
SELECT
    r.bsale_id::text AS cliente_codigo,
    COALESCE(
        NULLIF(TRIM(r.nombre_fantasia), ''),
        NULLIF(
            TRIM(
                CONCAT_WS(
                    ' ',
                    NULLIF(TRIM(r.first_name), ''),
                    NULLIF(TRIM(r.last_name), '')
                )
            ),
            ''
        ),
        'Cliente #' || r.bsale_id::text
    ) AS cliente_nombre,
    LOWER(TRIM(COALESCE(r.vendedor::text, r.company::text, ''))) AS vendedor_codigo,
    r.id AS ruta_id,
    NULLIF(TRIM(r.address), '') AS direccion,
    COALESCE(r.lat_operacional, r.lat) AS lat,
    COALESCE(r.lon_operacional, r.lon) AS lon,
    r.georef_estado
FROM bsale.rutero r
WHERE r.company_id = 3
  AND r.activo = TRUE
  AND (
        COALESCE(r.lat_operacional, r.lat) IS NULL
     OR COALESCE(r.lon_operacional, r.lon) IS NULL
     OR (
            COALESCE(r.lat_operacional, r.lat)::double precision = 0
        AND COALESCE(r.lon_operacional, r.lon)::double precision = 0
        )
  );

COMMENT ON VIEW bsale.v_clientes_sin_georef IS
    'Sin georef efectiva (NULL o 0,0); no usa georef_estado. Validar listados móvil contra esta vista.';
