-- =============================================================================
-- ANULACIÓN ADMINISTRATIVA — Romeo Alfa visitas #1 y #3
-- ESTADO: ESPERANDO AUTORIZACIÓN — NO EJECUTAR TODAVÍA
-- =============================================================================
-- Prerrequisito: 058_delivery_visit_admin_void.sql aplicada.
-- Índice activo (056):
--   CREATE UNIQUE INDEX uq_delivery_visit_active
--     ON distribuidora.delivery_visits (dispatch_plan_id, customer_key)
--     WHERE status = 'in_progress';
-- Por eso #3 debe pasar a cancelled en la MISMA transacción que el void.
--
-- #1: conserva status=partial + ended_at; voided_admin + auditoría.
-- #3: in_progress → cancelled + voided_admin + auditoría; hijos intactos.
-- Solo plan 82 / customer_key id:3575 / ids 1 y 3.
-- Una transacción; cualquier falla → ROLLBACK total.
-- Reemplazar /* ACTOR_USER_ID */ 0 por id admin (>0) en TODOS los marcadores.
-- =============================================================================

BEGIN;

CREATE TEMP TABLE _void_romeo_pre ON COMMIT DROP AS
SELECT
    v.id, v.status, v.ended_at, v.admin_classification, v.voided_at,
    (SELECT count(*) FROM distribuidora.delivery_document_results d WHERE d.visit_id = v.id) AS docs_n,
    (SELECT count(*) FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS lines_n,
    (SELECT coalesce(sum(l.delivered_units), 0)
       FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS delivered_sum,
    (SELECT count(*) FROM distribuidora.delivery_collections c WHERE c.visit_id = v.id) AS collections_n,
    (SELECT count(*) FROM distribuidora.delivery_evidence e WHERE e.visit_id = v.id) AS evidence_n,
    (SELECT count(*) FROM distribuidora.delivery_conformity f WHERE f.visit_id = v.id) AS conformity_n,
    (SELECT count(*) FROM distribuidora.delivery_visit_events ev WHERE ev.visit_id = v.id) AS events_n
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82 AND v.customer_key = 'id:3575' AND v.id IN (1, 3);

SELECT * FROM _void_romeo_pre ORDER BY id;

DO $$
DECLARE
    s1 text; s3 text; ended1 timestamptz; void1 timestamptz; void3 timestamptz;
    n int; other_ip int;
    actor int := /* ACTOR_USER_ID */ 0;
BEGIN
    IF actor IS NULL OR actor <= 0 THEN
        RAISE EXCEPTION 'Reemplazar ACTOR_USER_ID por id administrador (>0)';
    END IF;

    SELECT status, ended_at, voided_at INTO s1, ended1, void1
      FROM distribuidora.delivery_visits
     WHERE id = 1 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
     FOR UPDATE;
    SELECT status, voided_at INTO s3, void3
      FROM distribuidora.delivery_visits
     WHERE id = 3 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
     FOR UPDATE;

    IF s1 IS NULL OR s3 IS NULL THEN
        RAISE EXCEPTION 'No se encontraron #1 y #3 para plan 82 / id:3575';
    END IF;
    IF s1 IS DISTINCT FROM 'partial' OR ended1 IS NULL THEN
        RAISE EXCEPTION 'Visita #1 inválida: status=% ended_at=%', s1, ended1;
    END IF;
    IF s3 IS DISTINCT FROM 'in_progress' THEN
        RAISE EXCEPTION 'Visita #3 status inesperado: %', s3;
    END IF;
    IF void1 IS NOT NULL OR void3 IS NOT NULL THEN
        RAISE EXCEPTION 'Alguna visita ya anulada; abortando';
    END IF;
    SELECT count(*) INTO n FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND id IN (1, 3);
    IF n <> 2 THEN
        RAISE EXCEPTION 'Se esperaban 2 filas alvo; encontradas %', n;
    END IF;
    SELECT count(*) INTO other_ip FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575'
       AND id NOT IN (1, 3) AND status = 'in_progress';
    IF other_ip > 0 THEN
        RAISE EXCEPTION 'Otra in_progress del mismo cliente; abortando';
    END IF;
END $$;

UPDATE distribuidora.delivery_visits
SET
    admin_classification = 'voided_admin',
    voided_at = NOW(),
    voided_by_user_id = /* ACTOR_USER_ID */ 0,
    void_reason = 'Prueba de desarrollo Quillotana Operativa — no entrega física (Romeo Alfa / PLAN-00082)',
    void_ticket_ref = 'PILOT-ROMEO-ALFA-VOID',
    updated_at = NOW()
WHERE id = 1 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
  AND status = 'partial' AND ended_at IS NOT NULL AND voided_at IS NULL;

DO $$
DECLARE n int;
BEGIN
    SELECT count(*) INTO n FROM distribuidora.delivery_visits
     WHERE id = 1 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
       AND status = 'partial' AND ended_at IS NOT NULL
       AND admin_classification = 'voided_admin'
       AND voided_at IS NOT NULL AND voided_by_user_id > 0
       AND length(btrim(void_reason)) > 0;
    IF n <> 1 THEN RAISE EXCEPTION 'Void #1 incompleto (n=%); ROLLBACK', n; END IF;
END $$;

INSERT INTO distribuidora.delivery_visit_events (visit_id, action, actor_user_id, payload)
VALUES (
    1, 'admin_voided', /* ACTOR_USER_ID */ 0,
    jsonb_build_object(
        'reason', 'pilot_test_cleanup',
        'previous_status', 'partial',
        'kept_status', 'partial',
        'admin_classification', 'voided_admin',
        'ticket', 'PILOT-ROMEO-ALFA-VOID',
        'note_es', 'Cierre de prueba anulado; historial conservado.'
    )
);

UPDATE distribuidora.delivery_visits
SET
    status = 'cancelled',
    admin_classification = 'voided_admin',
    voided_at = NOW(),
    voided_by_user_id = /* ACTOR_USER_ID */ 0,
    void_reason = 'Prueba de desarrollo Quillotana Operativa — visita abierta residual (Romeo Alfa / PLAN-00082)',
    void_ticket_ref = 'PILOT-ROMEO-ALFA-VOID',
    updated_at = NOW()
WHERE id = 3 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
  AND status = 'in_progress' AND voided_at IS NULL;

DO $$
DECLARE n int; ip int;
BEGIN
    SELECT count(*) INTO n FROM distribuidora.delivery_visits
     WHERE id = 3 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
       AND status = 'cancelled'
       AND admin_classification = 'voided_admin'
       AND voided_at IS NOT NULL AND voided_by_user_id > 0
       AND length(btrim(void_reason)) > 0;
    IF n <> 1 THEN RAISE EXCEPTION 'Void #3 incompleto (n=%); ROLLBACK de toda la TX', n; END IF;

    -- Índice único activo liberado solo tras cancelled (misma TX)
    SELECT count(*) INTO ip FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND status = 'in_progress';
    IF ip <> 0 THEN RAISE EXCEPTION 'Índice activo aún ocupado; abortando'; END IF;
END $$;

INSERT INTO distribuidora.delivery_visit_events (visit_id, action, actor_user_id, payload)
VALUES (
    3, 'admin_voided', /* ACTOR_USER_ID */ 0,
    jsonb_build_object(
        'reason', 'pilot_test_cleanup',
        'previous_status', 'in_progress',
        'new_status', 'cancelled',
        'admin_classification', 'voided_admin',
        'ticket', 'PILOT-ROMEO-ALFA-VOID',
        'note_es', 'Aberta de prueba anulada; libera uq_delivery_visit_active.'
    )
);

DO $$
DECLARE
    blocking int; lines1 int; lines3 int; pre1 int; pre3 int;
BEGIN
    SELECT count(*) INTO blocking FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575'
       AND status IN ('completed', 'partial', 'not_delivered')
       AND ended_at IS NOT NULL
       AND NOT (
           admin_classification = 'voided_admin'
           AND voided_at IS NOT NULL
           AND voided_by_user_id IS NOT NULL AND voided_by_user_id > 0
           AND void_reason IS NOT NULL AND length(btrim(void_reason)) > 0
       );
    IF blocking <> 0 THEN RAISE EXCEPTION 'Aún hay cierre operativo bloqueante'; END IF;

    SELECT lines_n INTO pre1 FROM _void_romeo_pre WHERE id = 1;
    SELECT lines_n INTO pre3 FROM _void_romeo_pre WHERE id = 3;
    SELECT count(*) INTO lines1 FROM distribuidora.delivery_line_results WHERE visit_id = 1;
    SELECT count(*) INTO lines3 FROM distribuidora.delivery_line_results WHERE visit_id = 3;
    IF lines1 IS DISTINCT FROM pre1 OR lines3 IS DISTINCT FROM pre3 THEN
        RAISE EXCEPTION 'Conteo de líneas cambió; abortando';
    END IF;
END $$;

SELECT id, status, admin_classification, voided_at, voided_by_user_id,
       left(void_reason, 80) AS void_reason, void_ticket_ref, ended_at
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND id IN (1, 3)
ORDER BY id;

-- COMMIT;  -- solo si todo OK
-- ROLLBACK;
