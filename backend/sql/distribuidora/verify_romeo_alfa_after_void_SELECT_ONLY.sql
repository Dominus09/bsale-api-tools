-- =============================================================================
-- VERIFICACIÓN POST-ANULACIÓN — Solo SELECT (tras autorizar y ejecutar void)
-- =============================================================================

SELECT id, status, admin_classification, voided_at, void_ticket_ref, ended_at
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND id IN (1, 3)
ORDER BY id;
-- Esperado: #1 status=partial, voided_admin, voided_at NOT NULL
--           #3 status=cancelled, voided_admin, voided_at NOT NULL

SELECT count(*) AS in_progress_left
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND status = 'in_progress';
-- Esperado: 0

SELECT count(*) AS blocking_valid_closes
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82
  AND customer_key = 'id:3575'
  AND status IN ('completed', 'partial', 'not_delivered')
  AND ended_at IS NOT NULL
  AND NOT (
      COALESCE(admin_classification, 'operational') = 'voided_admin'
      AND voided_at IS NOT NULL
      AND voided_by_user_id IS NOT NULL
      AND voided_by_user_id > 0
      AND void_reason IS NOT NULL
      AND length(btrim(void_reason)) > 0
  );
-- Esperado: 0  → chofer puede iniciar visita operativa nueva
-- Nota: pilot_test / duplicate_pending_review SIN void completo siguen bloqueando.

-- Integridad CHECK: voided_admin incompleto no debería existir tras 058
SELECT id, admin_classification, voided_at, voided_by_user_id, void_reason
FROM distribuidora.delivery_visits
WHERE id IN (1, 3)
  AND admin_classification = 'voided_admin'
  AND (
      voided_at IS NULL
      OR voided_by_user_id IS NULL OR voided_by_user_id <= 0
      OR void_reason IS NULL OR length(btrim(void_reason)) = 0
  );
-- Esperado: 0 filas

SELECT visit_id, action, payload->>'ticket' AS ticket, created_at
FROM distribuidora.delivery_visit_events
WHERE visit_id IN (1, 3) AND action = 'admin_voided'
ORDER BY visit_id;

-- Hijos intactos (sin DELETE)
SELECT visit_id, count(*) AS lines
FROM distribuidora.delivery_line_results
WHERE visit_id IN (1, 3)
GROUP BY visit_id
ORDER BY visit_id;

-- Otras visitas del plan intactas
SELECT id, customer_key, status, admin_classification
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82 AND id NOT IN (1, 3)
ORDER BY id;
