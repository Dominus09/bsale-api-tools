-- =============================================================================
-- AUDITORÍA SOLO LECTURA — Romeo Alfa visitas #1 y #3 (PLAN-00082 / plan_id 82)
-- =============================================================================
-- pgAdmin / administrador. NO ejecutar escritura.
-- customer_key = id:3575 · client_id = 3575 · factura 51819 · related_document_id 3975263
-- =============================================================================

-- A) Cabeceras
SELECT
    v.id,
    v.dispatch_plan_id,
    v.customer_key,
    v.client_id,
    v.driver_user_id,
    v.status,
    v.result_reason,
    v.result_note,
    v.closure_review_status,
    v.started_at,
    v.ended_at,
    v.version,
    v.created_at,
    v.updated_at
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82
  AND v.id IN (1, 3)
ORDER BY v.id;

-- B) Conteo de relaciones por visita
SELECT
    v.id AS visit_id,
    v.status,
    (SELECT count(*) FROM distribuidora.delivery_document_results d WHERE d.visit_id = v.id) AS docs,
    (SELECT count(*) FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS lines,
    (SELECT coalesce(sum(l.delivered_units), 0)
       FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS delivered_units_sum,
    (SELECT count(*) FROM distribuidora.delivery_collections c WHERE c.visit_id = v.id) AS collections,
    (SELECT count(*) FROM distribuidora.delivery_evidence e WHERE e.visit_id = v.id) AS evidence,
    (SELECT count(*) FROM distribuidora.delivery_conformity f WHERE f.visit_id = v.id) AS conformity,
    (SELECT count(*) FROM distribuidora.delivery_visit_events ev WHERE ev.visit_id = v.id) AS events,
    (SELECT count(*) FROM distribuidora.delivery_report_snapshots s WHERE s.visit_id = v.id) AS reports
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82
  AND v.id IN (1, 3)
ORDER BY v.id;

-- C) Documentos
SELECT d.*
FROM distribuidora.delivery_document_results d
WHERE d.visit_id IN (1, 3)
ORDER BY d.visit_id, d.id;

-- D) Líneas (cantidades registradas en software; no = entrega física)
SELECT
    l.visit_id,
    l.document_result_id,
    l.product_name,
    l.sku_code,
    l.planned_units,
    l.delivered_units,
    l.undelivered_units,
    l.undelivered_reason
FROM distribuidora.delivery_line_results l
WHERE l.visit_id IN (1, 3)
ORDER BY l.visit_id, l.id;

-- E) Cobranza / evidencia / conformidad / eventos / reportes
SELECT * FROM distribuidora.delivery_collections WHERE visit_id IN (1, 3) ORDER BY visit_id, id;
SELECT id, visit_id, evidence_kind, validation_status, upload_confirmed, size_bytes, captured_at
FROM distribuidora.delivery_evidence WHERE visit_id IN (1, 3) ORDER BY visit_id, id;
SELECT * FROM distribuidora.delivery_conformity WHERE visit_id IN (1, 3);
SELECT id, visit_id, action, actor_user_id, payload, created_at
FROM distribuidora.delivery_visit_events
WHERE visit_id IN (1, 3)
ORDER BY visit_id, created_at, id;
SELECT id, visit_id, length(report_text) AS report_len, generated_at, generated_by_user_id
FROM distribuidora.delivery_report_snapshots
WHERE visit_id IN (1, 3);

-- F) Aislamiento: otras visitas del mismo plan NO deben confundirse
SELECT id, customer_key, client_id, status, ended_at
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82
  AND id NOT IN (1, 3)
ORDER BY id;

-- G) Factura comercial intacta (solo lectura; Bsale no se toca)
-- Si existe vista/tabla local de documentos relacionados, verificar related_document_id:
-- SELECT id, number, total_amount FROM ... WHERE id = 3975263;  -- ajustar al esquema real
