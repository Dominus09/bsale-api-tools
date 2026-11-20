-- 061 — Consultas de SOLO LECTURA para auditar estado previo a aplicar 061.
-- NO ejecuta DDL. NO modifica datos.
-- ESTADO: AWAITING APPROVAL del DDL 061; este archivo es diagnóstico.
--
-- Dependencias:
--   - 056/057/058: tablas delivery_visits / evidence / closures (base Offline V2).
--   - 059 / 060: AMPLÍAN CHECK de delivery_collections.method (cash_and_transfer,
--     check_declared). 061 NO depende de 059/060 para crear delivery_offline_ops.
--   - 061: crea delivery_offline_ops (UNIQUE driver+idempotency_key).
--
-- No asumir que 059 o 060 están pendientes en producción: verificar con estas
-- consultas antes de cualquier autorización de DDL.

-- 1) ¿Existe delivery_offline_ops?
SELECT
    to_regclass('distribuidora.delivery_offline_ops') IS NOT NULL AS offline_ops_table_exists;

-- 2) ¿Estado del CHECK de métodos de cobro (059/060)?
SELECT
    c.conname,
    pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'distribuidora'
  AND t.relname = 'delivery_collections'
  AND c.contype = 'c'
  AND c.conname = 'delivery_collections_method_check';

-- 3) ¿cash_and_transfer / check_declared ya están en el CHECK?
SELECT
    pg_get_constraintdef(c.oid) LIKE '%cash_and_transfer%' AS has_cash_and_transfer,
    pg_get_constraintdef(c.oid) LIKE '%check_declared%' AS has_check_declared
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'distribuidora'
  AND t.relname = 'delivery_collections'
  AND c.conname = 'delivery_collections_method_check';

-- 4) ¿Base visitas (056+) presente?
SELECT
    to_regclass('distribuidora.delivery_visits') IS NOT NULL AS visits_exists,
    to_regclass('distribuidora.delivery_evidence') IS NOT NULL AS evidence_exists,
    to_regclass('distribuidora.delivery_collections') IS NOT NULL AS collections_exists;
