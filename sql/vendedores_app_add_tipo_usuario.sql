-- tipo_usuario: vendedor | chofer | bodega (app móvil)
-- Ejecutar en BD existente si la tabla ya fue creada sin esta columna.

ALTER TABLE bsale.vendedores_app
  ADD COLUMN IF NOT EXISTS tipo_usuario VARCHAR(20);

UPDATE bsale.vendedores_app
SET tipo_usuario = 'vendedor'
WHERE tipo_usuario IS NULL OR TRIM(tipo_usuario) = '';

ALTER TABLE bsale.vendedores_app
  ALTER COLUMN tipo_usuario SET DEFAULT 'vendedor';

ALTER TABLE bsale.vendedores_app
  ALTER COLUMN tipo_usuario SET NOT NULL;

COMMENT ON COLUMN bsale.vendedores_app.tipo_usuario IS
  'Rol en la app: vendedor, chofer o bodega. Obligatorio para login.';
