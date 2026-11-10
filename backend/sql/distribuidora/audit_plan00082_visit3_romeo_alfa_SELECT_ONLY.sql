-- =============================================================================
-- AUDITORÍA SOLO LECTURA — PLAN-00082 / Romeo Alfa / visita #3
-- =============================================================================
-- Destinatario: administrador (pgAdmin). Usuario ai_reader NO tiene permisos.
-- SOLO SELECT. No UPDATE / DELETE / INSERT / TRUNCATE / ALTER.
-- No anular ni cerrar la visita #3.
--
-- Tablas verificadas contra migraciones 056 + 057:
--   distribuidora.delivery_visits
--   distribuidora.delivery_visit_events
--   distribuidora.delivery_document_results
--   distribuidora.delivery_line_results
--   distribuidora.delivery_collections
--   distribuidora.delivery_evidence
--   distribuidora.delivery_conformity
--   distribuidora.delivery_report_snapshots
--
-- Ajuste opcional: si el id de visita #3 no es 3, reemplace :visit_id.
-- =============================================================================

-- 0) Resolver plan + visitas del cliente (Romeo Alfa)
SELECT
    p.id AS dispatch_plan_id,
    p.planning_code,
    v.id AS visit_id,
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
    v.gps_exception_code,
    v.created_at,
    v.updated_at
FROM distribuidora.dispatch_plan p
JOIN distribuidora.delivery_visits v
  ON v.dispatch_plan_id = p.id
WHERE p.planning_code = 'PLAN-00082'
ORDER BY v.id;

-- 1) Cabecera visita #3 (ajuste id si corresponde)
SELECT
    v.*
FROM distribuidora.delivery_visits v
WHERE v.id = 3
  AND v.dispatch_plan_id = (
      SELECT id FROM distribuidora.dispatch_plan WHERE planning_code = 'PLAN-00082'
  );

-- 2) Resultados de documentos
SELECT
    d.id,
    d.visit_id,
    d.related_document_id,
    d.oc_document_id,
    d.document_number,
    d.document_type,
    d.document_total,
    d.result_status,
    d.notes,
    d.created_at,
    d.updated_at
FROM distribuidora.delivery_document_results d
WHERE d.visit_id = 3
ORDER BY d.id;

-- 3) Resultados de líneas
SELECT
    l.id,
    l.visit_id,
    l.document_result_id,
    l.source_detail_id,
    l.line_number,
    l.variant_id,
    l.sku_code,
    l.product_name,
    l.planned_units,
    l.delivered_units,
    l.undelivered_units,
    l.undelivered_reason,
    l.undelivered_note,
    l.created_at,
    l.updated_at
FROM distribuidora.delivery_line_results l
WHERE l.visit_id = 3
ORDER BY l.document_result_id, l.line_number NULLS LAST, l.id;

-- 4) Declaraciones de cobranza
SELECT
    c.id,
    c.visit_id,
    c.document_result_id,
    c.method,
    c.document_amount,
    c.declared_amount,
    c.verified_amount,
    c.pending_balance,
    c.note,
    c.created_by_user_id,
    c.created_at,
    c.updated_at
FROM distribuidora.delivery_collections c
WHERE c.visit_id = 3
ORDER BY c.id;

-- 5) Evidencias (metadatos; bytes en R2 vía object_key)
SELECT
    e.id,
    e.visit_id,
    e.company_id,
    e.evidence_kind,
    e.object_key,
    e.content_type,
    e.size_bytes,
    e.upload_confirmed,
    e.validation_status,
    e.exception_note,
    e.captured_at,
    e.captured_by_user_id,
    e.visit_event_id,
    e.created_at
FROM distribuidora.delivery_evidence e
WHERE e.visit_id = 3
ORDER BY e.id;

-- 6) Conformidad / firma
SELECT
    f.id,
    f.visit_id,
    f.receptor_name,
    f.signature_object_key IS NOT NULL AS has_signature_object,
    f.signature_content_type,
    f.accepted_summary_sha256,
    f.signed_at,
    f.refused,
    f.refuse_reason,
    f.visit_version_at_sign,
    f.invalidated_at,
    f.invalidate_reason,
    f.created_at,
    f.updated_at
FROM distribuidora.delivery_conformity f
WHERE f.visit_id = 3;

-- 7) Eventos de negocio (apertura ≠ registro de entrega)
SELECT
    ev.id,
    ev.visit_id,
    ev.action,
    ev.actor_user_id,
    ev.payload,
    ev.created_at
FROM distribuidora.delivery_visit_events ev
WHERE ev.visit_id = 3
ORDER BY ev.created_at, ev.id;

-- 8) Snapshots de reporte WhatsApp (si existen)
SELECT
    s.id,
    s.visit_id,
    length(s.report_text) AS report_text_len,
    s.generated_at,
    s.generated_by_user_id
FROM distribuidora.delivery_report_snapshots s
WHERE s.visit_id = 3
ORDER BY s.generated_at DESC;

-- 9) Resumen conteos (interpretación rápida)
SELECT
    3 AS visit_id,
    (SELECT count(*) FROM distribuidora.delivery_document_results WHERE visit_id = 3) AS docs,
    (SELECT count(*) FROM distribuidora.delivery_line_results WHERE visit_id = 3) AS lines,
    (SELECT count(*) FROM distribuidora.delivery_collections WHERE visit_id = 3) AS collections,
    (SELECT count(*) FROM distribuidora.delivery_evidence WHERE visit_id = 3) AS evidence,
    (SELECT count(*) FROM distribuidora.delivery_conformity WHERE visit_id = 3) AS conformity,
    (SELECT count(*) FROM distribuidora.delivery_visit_events WHERE visit_id = 3) AS events,
    (SELECT count(*) FROM distribuidora.delivery_report_snapshots WHERE visit_id = 3) AS report_snapshots,
    (
        SELECT string_agg(DISTINCT action, ', ' ORDER BY action)
        FROM distribuidora.delivery_visit_events
        WHERE visit_id = 3
    ) AS event_actions;

-- Nota interpretativa:
-- - Si docs/lines/collections/evidence/conformity = 0 y solo hay visit_started /
--   visit_resumed en events → apertura sin registro comercial de entrega.
-- - No usar este script para cerrar ni anular la visita.
