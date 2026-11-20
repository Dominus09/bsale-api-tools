-- 061 / Offline V2 — dry-run de objetos R2 potencialmente huérfanos.
-- SOLO LECTURA. No borra. No aplica DDL.
--
-- Criterio de candidato (futuro job):
--   - evidence_object_key presente
--   - evidence_upload_confirmed = false  O  status in (failed, awaiting_evidence)
--   - updated_at más antiguo que TTL (p.ej. 30 días) — filtrar en app
--   - NO hay delivery_evidence.upload_confirmed=true con la misma object_key
--
-- Nunca ejecutar DELETE desde PWA logout / SW update.

SELECT
    o.id AS offline_op_id,
    o.driver_user_id,
    o.dispatch_plan_id,
    o.idempotency_key,
    o.status,
    o.processing_stage,
    o.evidence_object_key,
    o.evidence_sha256,
    o.evidence_upload_confirmed,
    o.visit_id,
    o.updated_at,
    EXISTS (
        SELECT 1
        FROM distribuidora.delivery_evidence e
        WHERE e.object_key = o.evidence_object_key
          AND e.upload_confirmed IS TRUE
    ) AS linked_confirmed_evidence
FROM distribuidora.delivery_offline_ops o
WHERE o.evidence_object_key IS NOT NULL
  AND (
        o.evidence_upload_confirmed = FALSE
     OR o.status IN ('failed', 'awaiting_evidence')
  )
ORDER BY o.updated_at ASC
LIMIT 500;
