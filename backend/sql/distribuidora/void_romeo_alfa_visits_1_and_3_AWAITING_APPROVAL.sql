-- =============================================================================
-- ANULACIÓN ADMINISTRATIVA — Romeo Alfa visitas #1 y #3
-- ESTADO: ESPERANDO AUTORIZACIÓN — NO EJECUTAR TODAVÍA
-- =============================================================================
-- Prerrequisito: aplicar 058_delivery_visit_admin_void.sql (también con autorización).
--
-- Reglas:
--   - Sin DELETE de visitas ni hijos (docs/líneas/cobranza/evidencia/conformidad/eventos).
--   - #1 (finalizada partial): CONSERVA status=partial y ended_at; solo marca void.
--   - #3 (in_progress): status → cancelled + void (libera uq_delivery_visit_active).
--   - Solo ids 1 y 3, plan_id 82, customer_key id:3575.
--   - Una sola transacción; si falla cualquier guarda → ROLLBACK total.
--   - No crea visita nueva. No toca Bsale / picking / factura 51819.
--
-- Antes de ejecutar: reemplazar TODOS los /* ACTOR_USER_ID */ 0 por el id admin (>0).
-- =============================================================================

BEGIN;

-- 0) Respaldo lógico pre
CREATE TEMP TABLE _void_romeo_pre ON COMMIT DROP AS
SELECT
    v.id,
    v.status,
    v.ended_at,
    v.admin_classification,
    v.voided_at,
    (SELECT count(*) FROM distribuidora.delivery_document_results d WHERE d.visit_id = v.id) AS docs_n,
    (SELECT count(*) FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS lines_n,
    (SELECT coalesce(sum(l.delivered_units), 0)
       FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS delivered_sum,
    (SELECT count(*) FROM distribuidora.delivery_collections c WHERE c.visit_id = v.id) AS collections_n,
    (SELECT count(*) FROM distribuidora.delivery_evidence e WHERE e.visit_id = v.id) AS evidence_n,
    (SELECT count(*) FROM distribuidora.delivery_conformity f WHERE f.visit_id = v.id) AS conformity_n,
    (SELECT count(*) FROM distribuidora.delivery_visit_events ev WHERE ev.visit_id = v.id) AS events_n
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82
  AND v.customer_key = 'id:3575'
  AND v.id IN (1, 3);

SELECT * FROM _void_romeo_pre ORDER BY id;

-- Guardas previas (abortan la transacción completa)
DO $$
DECLARE
    s1 text;
    s3 text;
    ended1 timestamptz;
    void1 timestamptz;
    void3 timestamptz;
    n int;
    other_touched int;
    actor int := /* ACTOR_USER_ID */ 0;
BEGIN
    IF actor IS NULL OR actor <= 0 THEN
        RAISE EXCEPTION 'Reemplazar ACTOR_USER_ID por el id del administrador (>0)';
    END IF;

    SELECT status, ended_at, voided_at
      INTO s1, ended1, void1
      FROM distribuidora.delivery_visits
     WHERE id = 1 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
     FOR UPDATE;
    SELECT status, voided_at
      INTO s3, void3
      FROM distribuidora.delivery_visits
     WHERE id = 3 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
     FOR UPDATE;

    IF s1 IS NULL OR s3 IS NULL THEN
        RAISE EXCEPTION 'No se encontraron ambas visitas #1 y #3 para plan 82 / id:3575';
    END IF;
    IF s1 IS DISTINCT FROM 'partial' THEN
        RAISE EXCEPTION 'Visita #1 status inesperado: % (se esperaba partial)', s1;
    END IF;
    IF ended1 IS NULL THEN
        RAISE EXCEPTION 'Visita #1 sin ended_at; abortando';
    END IF;
    IF s3 IS DISTINCT FROM 'in_progress' THEN
        RAISE EXCEPTION 'Visita #3 status inesperado: % (se esperaba in_progress)', s3;
    END IF;
    IF void1 IS NOT NULL OR void3 IS NOT NULL THEN
        RAISE EXCEPTION 'Alguna visita ya está anulada; abortando para evitar doble void';
    END IF;

    SELECT count(*) INTO n
      FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND id IN (1, 3);
    IF n <> 2 THEN
        RAISE EXCEPTION 'Se esperaban exactamente 2 filas alvo; encontradas %', n;
    END IF;

    -- Asegurar que no hay otras filas del cliente con ids distintos en el lote alvo
    SELECT count(*) INTO other_touched
      FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82
       AND customer_key = 'id:3575'
       AND id NOT IN (1, 3)
       AND status = 'in_progress';
    -- Informativo: otras activas del mismo cliente serían anomalía (índice único)
    IF other_touched > 0 THEN
        RAISE EXCEPTION 'Hay otra visita in_progress del mismo cliente; abortando';
    END IF;
END $$;

-- 1) Anular #1 — conserva status/ended_at/hijos
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

DO $$
BEGIN
    IF NOT FOUND THEN
        -- NOT FOUND tras UPDATE en DO no aplica al UPDATE externo; se valida por rowcount abajo
        NULL;
    END IF;
END $$;

-- Validar exactamente 1 fila #1
DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n
      FROM distribuidora.delivery_visits
     WHERE id = 1 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
       AND admin_classification = 'voided_admin'
       AND voided_at IS NOT NULL
       AND status = 'partial';
    IF n <> 1 THEN
        RAISE EXCEPTION 'Void #1 falló (filas voided=%). ROLLBACK.', n;
    END IF;
END $$;

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
        'note_es', 'Cierre de prueba anulado; historial conservado; no bloquea nueva visita.'
    )
);

-- 2) Anular #3 — cancelled + void (libera índice único activo)
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

DO $$
DECLARE
    n int;
BEGIN
    SELECT count(*) INTO n
      FROM distribuidora.delivery_visits
     WHERE id = 3 AND dispatch_plan_id = 82 AND customer_key = 'id:3575'
       AND admin_classification = 'voided_admin'
       AND voided_at IS NOT NULL
       AND status = 'cancelled';
    IF n <> 1 THEN
        RAISE EXCEPTION 'Void #3 falló (filas voided=%). ROLLBACK de #1 y #3.', n;
    END IF;
END $$;

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
        'note_es', 'Visita abierta de prueba anulada; libera uq_delivery_visit_active; historial conservado.'
    )
);

-- 3) Verificación final dentro de la misma transacción
DO $$
DECLARE
    ip int;
    blocking int;
    lines1 int;
    lines3 int;
    pre1 int;
    pre3 int;
BEGIN
    SELECT count(*) INTO ip
      FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND status = 'in_progress';
    IF ip <> 0 THEN
        RAISE EXCEPTION 'Aún hay in_progress para Romeo Alfa; abortando';
    END IF;

    SELECT count(*) INTO blocking
      FROM distribuidora.delivery_visits
     WHERE dispatch_plan_id = 82
       AND customer_key = 'id:3575'
       AND status IN ('completed', 'partial', 'not_delivered')
       AND ended_at IS NOT NULL
       AND NOT (
           voided_at IS NOT NULL
           AND COALESCE(admin_classification, 'operational') = 'voided_admin'
       );
    IF blocking <> 0 THEN
        RAISE EXCEPTION 'Aún hay cierre operativo bloqueante; abortando';
    END IF;

    SELECT lines_n INTO pre1 FROM _void_romeo_pre WHERE id = 1;
    SELECT lines_n INTO pre3 FROM _void_romeo_pre WHERE id = 3;
    SELECT count(*) INTO lines1 FROM distribuidora.delivery_line_results WHERE visit_id = 1;
    SELECT count(*) INTO lines3 FROM distribuidora.delivery_line_results WHERE visit_id = 3;
    IF lines1 IS DISTINCT FROM pre1 OR lines3 IS DISTINCT FROM pre3 THEN
        RAISE EXCEPTION 'Conteo de líneas cambió tras void; abortando (no debería ocurrir)';
    END IF;
END $$;

SELECT id, status, admin_classification, voided_at, voided_by_user_id, void_ticket_ref, ended_at
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82 AND customer_key = 'id:3575' AND id IN (1, 3)
ORDER BY id;

SELECT id, lines_n AS lines_pre, delivered_sum AS delivered_pre
FROM _void_romeo_pre
ORDER BY id;

-- Si todo OK en pgAdmin: ejecutar COMMIT;
-- Si algo falló (exception): la transacción queda abortada → ROLLBACK;
-- COMMIT;
-- ROLLBACK;
