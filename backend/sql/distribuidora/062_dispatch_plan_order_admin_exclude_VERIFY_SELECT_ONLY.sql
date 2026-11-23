-- =============================================================================
-- 062 VERIFY — SELECT ONLY (no DDL / no DML)
-- Verificar si la migración de exclusión admin está aplicada y el estado
-- de una OC candidata (ej. 71041) SIN modificar datos.
-- =============================================================================

-- 1) Columnas 062 presentes
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'distribuidora'
  AND table_name = 'dispatch_plan_orders'
  AND column_name IN (
      'excluded_at',
      'excluded_by_user_id',
      'excluded_by_user_name',
      'exclude_reason',
      'exclusion_kind'
  )
ORDER BY column_name;

-- 2) Constraint e índice
SELECT c.conname, pg_get_constraintdef(c.oid) AS definition
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = 'distribuidora'
  AND t.relname = 'dispatch_plan_orders'
  AND c.conname = 'ck_dispatch_plan_orders_admin_exclude_integrity';

SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'distribuidora'
  AND tablename = 'dispatch_plan_orders'
  AND indexname = 'idx_dispatch_plan_orders_active';

-- 3) OC 71041: pertenencia y exclusión (ajustar plan_id si se conoce)
SELECT
    dpo.dispatch_plan_id,
    dp.planning_code,
    dp.status AS plan_status,
    dpo.oc_document_id,
    dpo.oc_number,
    dpo.oc_total_amount,
    dpo.excluded_at,
    dpo.excluded_by_user_id,
    dpo.excluded_by_user_name,
    dpo.exclude_reason,
    dpo.exclusion_kind
FROM distribuidora.dispatch_plan_orders dpo
JOIN distribuidora.dispatch_plan dp ON dp.id = dpo.dispatch_plan_id
WHERE dpo.oc_number = 71041
   OR dpo.oc_document_id = 71041
ORDER BY dpo.dispatch_plan_id DESC;

-- 4) Conteos activos vs excluidos por plan de la OC 71041
SELECT
    dpo.dispatch_plan_id,
    COUNT(*) FILTER (WHERE dpo.excluded_at IS NULL) AS active_ocs,
    COUNT(*) FILTER (WHERE dpo.excluded_at IS NOT NULL) AS excluded_ocs,
    COALESCE(SUM(dpo.oc_total_amount) FILTER (WHERE dpo.excluded_at IS NULL), 0) AS active_amount
FROM distribuidora.dispatch_plan_orders dpo
WHERE dpo.dispatch_plan_id IN (
    SELECT dispatch_plan_id
    FROM distribuidora.dispatch_plan_orders
    WHERE oc_number = 71041 OR oc_document_id = 71041
)
GROUP BY dpo.dispatch_plan_id;

-- 5) Eventos de exclusión admin
SELECT id, dispatch_plan_id, action, user_name, reason,
       oc_document_id, oc_number, created_at
FROM distribuidora.dispatch_plan_order_events
WHERE action = 'exclude_order_admin'
  AND (oc_number = 71041 OR oc_document_id = 71041)
ORDER BY created_at DESC
LIMIT 20;
