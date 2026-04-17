-- Snapshot de cliente en visitas + tipo incidencia atención telefónica (app móvil).
-- Idempotente: seguro ejecutar más de una vez.

ALTER TABLE bsale.visitas
  ADD COLUMN IF NOT EXISTS nombre_fantasia text NULL;

ALTER TABLE bsale.visitas
  ADD COLUMN IF NOT EXISTS direccion text NULL;

ALTER TABLE bsale.visitas
  ADD COLUMN IF NOT EXISTS comuna text NULL;

ALTER TABLE bsale.visitas
  ADD COLUMN IF NOT EXISTS rut_clean varchar(64) NULL;

ALTER TABLE bsale.visitas DROP CONSTRAINT IF EXISTS chk_visitas_tipo_incidencia;

ALTER TABLE bsale.visitas ADD CONSTRAINT chk_visitas_tipo_incidencia CHECK (
  tipo_incidencia IS NULL
  OR tipo_incidencia IN (
    'local cerrado',
    'sin stock',
    'no compra',
    'fuera de ruta',
    'otros',
    'atencion telefonica'
  )
);

COMMENT ON COLUMN bsale.visitas.nombre_fantasia IS 'Copia desde rutero al generar la visita (snapshot).';
COMMENT ON COLUMN bsale.visitas.direccion IS 'Copia desde rutero.address al generar la visita (snapshot).';
COMMENT ON COLUMN bsale.visitas.comuna IS 'Copia desde rutero.municipality al generar la visita (snapshot).';
COMMENT ON COLUMN bsale.visitas.rut_clean IS 'Copia desde rutero al generar la visita (snapshot).';
