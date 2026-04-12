-- =============================================================================
-- Vendedores de la app móvil (separados de usuarios ERP / bsale.users)
-- Esquema: bsale
-- Contraseñas: solo password_hash (bcrypt), nunca texto plano
-- =============================================================================

CREATE TABLE IF NOT EXISTS bsale.vendedores_app (
  id SERIAL PRIMARY KEY,
  codigo VARCHAR(50) NOT NULL,
  nombre VARCHAR(100) NOT NULL,
  password_hash TEXT NOT NULL,
  activo BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_vendedores_app_codigo UNIQUE (codigo)
);

COMMENT ON TABLE bsale.vendedores_app IS
  'Credenciales de vendedores solo para la app móvil (no mezclar con ERP).';

COMMENT ON COLUMN bsale.vendedores_app.codigo IS
  'Identificador de login (ej. vendedor_1); único en toda la tabla.';

COMMENT ON COLUMN bsale.vendedores_app.password_hash IS
  'Hash bcrypt de la contraseña; comparar con bcrypt.checkpw en login.';

-- El UNIQUE en codigo crea un índice único implícito; no se duplican índices innecesarios.

-- -----------------------------------------------------------------------------
-- Datos iniciales (contraseña en claro para todos: Laquillotana123)
-- Hash generado con bcrypt (cost 12). Para regenerar: backend/scripts/gen_vendedores_app_password_hash.py
-- -----------------------------------------------------------------------------
INSERT INTO bsale.vendedores_app (codigo, nombre, password_hash)
VALUES
  (
    'vendedor_1',
    'Alvaro Vargas',
    '$2b$12$9JpLR/Hsfw661WQ3WEMKAeQbgAIf9b3IdFeO5xns7bLVMkRbIMBLq'
  ),
  (
    'vendedor_2',
    'Cristofer Saldivia',
    '$2b$12$9JpLR/Hsfw661WQ3WEMKAeQbgAIf9b3IdFeO5xns7bLVMkRbIMBLq'
  ),
  (
    'vendedor_4',
    'Marcelo Yañez',
    '$2b$12$9JpLR/Hsfw661WQ3WEMKAeQbgAIf9b3IdFeO5xns7bLVMkRbIMBLq'
  ),
  (
    'vendedor_3',
    'Erick Paredes',
    '$2b$12$9JpLR/Hsfw661WQ3WEMKAeQbgAIf9b3IdFeO5xns7bLVMkRbIMBLq'
  )
ON CONFLICT (codigo) DO NOTHING;
