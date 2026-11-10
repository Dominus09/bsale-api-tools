-- =============================================================================
-- ANULACIÓN ADMINISTRATIVA — Romeo Alfa visitas #1 y #3
-- ESTADO: ESPERANDO AUTORIZACIÓN — NO EJECUTAR TODAVÍA
-- =============================================================================
-- Prerrequisito: aplicar 058_delivery_visit_admin_void.sql (también con autorización).
--
-- Reglas:
--   - Sin DELETE de visitas ni hijos.
--   - #1 (finalizada partial): NO cambiar status; marcar voided_admin + auditoría.
--     Cambiar a cancelled rompería el sentido histórico del cierre de prueba.
--   - #3 (in_progress): status → cancelled + voided_admin para liberar
--     uq_delivery_visit_active y permitir una visita operativa nueva.
--   - Solo ids 1 y 3 del plan 82 / customer_key id:3575.
--   - No crea visita nueva.
--   - No toca Bsale / picking / factura 51819.
--
-- Sustituir el literal ACTOR_USER_ID (abajo) por el id entero del administrador.
-- =============================================================================

BEGIN;

-- >>> EDITAR ANTES DE EJECUTAR <<<
-- Ejemplo: 42
-- Usar el mismo valor en los cuatro lugares marcados ACTOR_USER_ID.

-- 0) Respaldo lógico (conteos pre)
CREATE TEMP TABLE IF NOT EXISTS _void_romeo_pre AS
SELECT
    v.id,
    v.status,
    v.admin_classification,
    v.voided_at,
    (SELECT count(*) FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS lines_n,
    (SELECT coalesce(sum(l.delivered_units), 0)
       FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS delivered_sum
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82
  AND v.customer_key = 'id:3575'
  AND v.id IN (1, 3);

SELECT * FROM _void_romeo_pre ORDER BY id;

-- Guardas: abortar si no coinciden estados esperados / actor sin editar
DO $$
DECLARE
    s1 text;
    s3 text;
    n int;
    actor int := /* ACTOR_USER_ID */ 0;
BEGIN
    IF actor IS NULL OR actor <= 0 THEN
        RAISE EXCEPTION 'Reemplazar ACTOR_USER_ID por el id del administrador (>0)';
    END IF;
    SELECT status INTO s1 FROM distribuidora.delivery_visits
     WHERE id = 1 AND dispatch_plan_id = 82 AND customer_key = 'id:3575';
    SELECT status INTO s3 FROM distribuidora.delivery_visits
     WHERE id = 3 AND dispatch_plan_id = 82 AND customer_key = 'id:3575';
    IF s1 IS DISTINCT FROM 'partial' THEN
        RAISE EXCEPTION 'Visita #1 status inesperado: % (se esperaba partial)', s1;
    END IF;
    IF s3 IS DISTINCT FROM 'in_progress' THEN
        RAISE EXCEPTION 'Visita #3 status inesperado: % (se esperaba in_progress)', s3;
    END IF;
    SELECT count(*) INTO n FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND id IN (1, 3);
    IF n <> 2 THEN
        RAISE EXCEPTION 'Se esperaban exactamente 2 filas alvo; encontradas %', n;
    END IF;
END $$;

-- 1) Anular #1 (cierre de prueba) — conservar status=partial / ended_at
UPDATE distribuidora.delivery_visits
SET
    admin_classification = 'voided_admin',
    voided_at = NOW(),
    voided_by_user_id = /* ACTOR_USER_ID */ 0,
    void_reason = 'Prueba de desarrollo Quillotana Operativa — no entrega física (Romeo Alfa / PLAN-00082)',
    void_ticket_ref = 'PILOT-ROMEO-ALFA-VOID',
    updated_at = NOW()
WHERE id = 1
  AND dispatch_plan_id = 82
  AND customer_key = 'id:3575'
  AND status = 'partial'
  AND ended_at IS NOT NULL
  AND voided_at IS NULL;

INSERT INTO distribuidora.delivery_visit_events (visit_id, action, actor_user_id, payload)
VALUES (
    1,
    'admin_voided',
    /* ACTOR_USER_ID */ 0,
    jsonb_build_object(
        'reason', 'pilot_test_cleanup',
        'previous_status', 'partial',
        'kept_status', 'partial',
        'admin_classification', 'voided_admin',
        'ticket', 'PILOT-ROMEO-ALFA-VOID',
        'note_es', 'Cierre de prueba anulado; no cuenta como entrega operativa ni bloquea nueva visita.'
    )
);

-- 2) Anular #3 (abierta de prueba) — cancelled para liberar índice único activo
UPDATE distribuidora.delivery_visits
SET
    status = 'cancelled',
    admin_classification = 'voided_admin',
    voided_at = NOW(),
    voided_by_user_id = /* ACTOR_USER_ID */ 0,
    void_reason = 'Prueba de desarrollo Quillotana Operativa — visita abierta residual (Romeo Alfa / PLAN-00082)',
    void_ticket_ref = 'PILOT-ROMEO-ALFA-VOID',
    updated_at = NOW()
WHERE id = 3
  AND dispatch_plan_id = 82
  AND customer_key = 'id:3575'
  AND status = 'in_progress'
  AND voided_at IS NULL;

INSERT INTO distribuidora.delivery_visit_events (visit_id, action, actor_user_id, payload)
VALUES (
    3,
    'admin_voided',
    /* ACTOR_USER_ID */ 0,
    jsonb_build_object(
        'reason', 'pilot_test_cleanup',
        'previous_status', 'in_progress',
        'new_status', 'cancelled',
        'admin_classification', 'voided_admin',
        'ticket', 'PILOT-ROMEO-ALFA-VOID',
        'note_es', 'Visita abierta de prueba anulada; libera uq_delivery_visit_active.'
    )
);

-- 3) Verificación post (debe devolver 2 filas voided; 0 in_progress para el cliente)
SELECT
    id, status, admin_classification, voided_at, voided_by_user_id, void_ticket_ref
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82
  AND customer_key = 'id:3575'
  AND id IN (1, 3)
ORDER BY id;

SELECT count(*) AS still_in_progress
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82
  AND customer_key = 'id:3575'
  AND status = 'in_progress';

-- Cierre válido operativo restante (debe ser 0)
SELECT count(*) AS valid_closed_blocking
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82
  AND customer_key = 'id:3575'
  AND status IN ('completed', 'partial', 'not_delivered')
  AND ended_at IS NOT NULL
  AND COALESCE(admin_classification, 'operational') NOT IN ('voided_admin', 'pilot_test')
  AND voided_at IS NULL;

-- Hijos intactos (mismo conteo que pre)
SELECT
    v.id,
    (SELECT count(*) FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS lines_n,
    (SELECT coalesce(sum(l.delivered_units), 0)
       FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS delivered_sum
FROM distribuidora.delivery_visits v
WHERE v.id IN (1, 3)
ORDER BY v.id;

-- Si todo OK: COMMIT; si no: ROLLBACK;
-- COMMIT;
-- ROLLBACK;
