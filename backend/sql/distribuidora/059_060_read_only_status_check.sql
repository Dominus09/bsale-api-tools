-- Consultas SOLO LECTURA: estado de 059 / 060 (CHECK de métodos de cobro).
-- NO ejecuta DDL. NO modifica datos.
-- Usar antes de cualquier autorización de migración productiva.

-- ¿Existe constraint de method?
SELECT
    c.conname,
    pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'distribuidora'
  AND t.relname = 'delivery_collections'
  AND c.conname = 'delivery_collections_method_check';

-- Flags de contenido 059 / 060
SELECT
    pg_get_constraintdef(c.oid) LIKE '%cash_and_transfer%' AS has_059_cash_and_transfer,
    pg_get_constraintdef(c.oid) LIKE '%check_declared%' AS has_060_check_declared
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'distribuidora'
  AND t.relname = 'delivery_collections'
  AND c.conname = 'delivery_collections_method_check';

-- Dependencias previas Offline V2 (056+)
SELECT
    to_regclass('distribuidora.delivery_visits') IS NOT NULL AS visits_ok,
    to_regclass('distribuidora.delivery_evidence') IS NOT NULL AS evidence_ok,
    to_regclass('distribuidora.delivery_collections') IS NOT NULL AS collections_ok,
    to_regclass('distribuidora.delivery_offline_ops') IS NOT NULL AS offline_ops_061_applied;
