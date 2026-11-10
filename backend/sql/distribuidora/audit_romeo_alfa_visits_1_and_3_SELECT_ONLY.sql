-- =============================================================================
-- AUDITORÍA SOLO LECTURA — Romeo Alfa visitas #1 y #3 (incluye invalidados)
-- =============================================================================
-- pgAdmin. NO escritura. No asumir cero cobranzas/firmas: listar TODAS las filas.
-- plan_id=82 · customer_key=id:3575 · client_id=3575 · factura 51819 · rel 3975263
-- =============================================================================

-- A) Cabeceras
SELECT
    v.id, v.dispatch_plan_id, v.customer_key, v.client_id, v.driver_user_id,
    v.status, v.result_reason, v.result_note, v.closure_review_status,
    v.started_at, v.ended_at, v.version, v.created_at, v.updated_at,
    v.admin_classification, v.voided_at, v.voided_by_user_id, v.void_reason, v.void_ticket_ref
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82 AND v.id IN (1, 3)
ORDER BY v.id;

-- B) Conteos (todas las relaciones; no filtrar “relevantes”)
SELECT
    v.id AS visit_id,
    v.status,
    (SELECT count(*) FROM distribuidora.delivery_document_results d WHERE d.visit_id = v.id) AS docs,
    (SELECT count(*) FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS lines,
    (SELECT coalesce(sum(l.delivered_units), 0)
       FROM distribuidora.delivery_line_results l WHERE l.visit_id = v.id) AS delivered_units_sum,
    (SELECT count(*) FROM distribuidora.delivery_collections c WHERE c.visit_id = v.id) AS collections_all,
    (SELECT count(*) FROM distribuidora.delivery_evidence e WHERE e.visit_id = v.id) AS evidence_all,
    (SELECT count(*) FROM distribuidora.delivery_conformity f WHERE f.visit_id = v.id) AS conformity_rows,
    (SELECT count(*) FROM distribuidora.delivery_conformity f
      WHERE f.visit_id = v.id AND f.invalidated_at IS NULL) AS conformity_valid,
    (SELECT count(*) FROM distribuidora.delivery_conformity f
      WHERE f.visit_id = v.id AND f.invalidated_at IS NOT NULL) AS conformity_invalidated,
    (SELECT count(*) FROM distribuidora.delivery_visit_events ev WHERE ev.visit_id = v.id) AS events,
    (SELECT count(*) FROM distribuidora.delivery_report_snapshots s WHERE s.visit_id = v.id) AS reports
FROM distribuidora.delivery_visits v
WHERE v.dispatch_plan_id = 82 AND v.id IN (1, 3)
ORDER BY v.id;

-- C) Documentos
SELECT * FROM distribuidora.delivery_document_results
WHERE visit_id IN (1, 3) ORDER BY visit_id, id;

-- D) Líneas
SELECT
    visit_id, id, document_result_id, product_name, sku_code,
    planned_units, delivered_units, undelivered_units, undelivered_reason, created_at, updated_at
FROM distribuidora.delivery_line_results
WHERE visit_id IN (1, 3)
ORDER BY visit_id, id;

-- E) Cobranza — TODAS las filas (incluye cualquier estado/método)
SELECT
    c.id, c.visit_id, c.document_result_id, c.method,
    c.document_amount, c.declared_amount, c.verified_amount, c.pending_balance,
    c.note, c.created_by_user_id, c.created_at, c.updated_at
FROM distribuidora.delivery_collections c
WHERE c.visit_id IN (1, 3)
ORDER BY c.visit_id, c.id;

-- F) Evidencias — TODAS
SELECT
    e.id, e.visit_id, e.evidence_kind, e.object_key, e.content_type, e.size_bytes,
    e.upload_confirmed, e.validation_status, e.exception_note,
    e.captured_at, e.captured_by_user_id, e.visit_event_id, e.created_at
FROM distribuidora.delivery_evidence e
WHERE e.visit_id IN (1, 3)
ORDER BY e.visit_id, e.id;

-- G) Conformidad / firma — vigentes e invalidadas
SELECT
    f.id, f.visit_id, f.receptor_name,
    f.signature_object_key IS NOT NULL AS has_signature_object,
    f.signature_content_type, f.accepted_summary_sha256, f.signed_at,
    f.refused, f.refuse_reason, f.visit_version_at_sign,
    f.invalidated_at, f.invalidate_reason, f.created_at, f.updated_at
FROM distribuidora.delivery_conformity f
WHERE f.visit_id IN (1, 3)
ORDER BY f.visit_id, f.id;

-- H) Eventos
SELECT id, visit_id, action, actor_user_id, payload, created_at
FROM distribuidora.delivery_visit_events
WHERE visit_id IN (1, 3)
ORDER BY visit_id, created_at, id;

-- I) Reportes / snapshots
SELECT id, visit_id, length(report_text) AS report_len, generated_at, generated_by_user_id
FROM distribuidora.delivery_report_snapshots
WHERE visit_id IN (1, 3)
ORDER BY visit_id, generated_at;

-- J) Aislamiento: otras visitas del plan
SELECT id, customer_key, client_id, status, ended_at, admin_classification, voided_at
FROM distribuidora.delivery_visits
WHERE dispatch_plan_id = 82 AND id NOT IN (1, 3)
ORDER BY id;
