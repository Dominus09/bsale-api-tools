-- =============================================================================
-- PROPUESTA — NO APLICAR SIN APROBACIÓN EXPLÍCITA
-- Clasificación administrativa / anulación auditada de visitas de entrega
-- =============================================================================
-- Contexto piloto (Romeo Alfa / PLAN-00082): coexisten visitas finalizadas y
-- abiertas del mismo plan+cliente. El software NO prueba entrega física.
--
-- Objetivo:
--   Distinguir visitas de prueba, operativas, duplicadas en revisión y anuladas.
--   Permitir anulación administrativa auditada (abiertas o finalizadas).
--   Si una finalizada era solo prueba, habilitar luego una entrega real
--   únicamente vía acción administrativa autorizada.
--
-- Sin DELETE de filas comerciales hijas. Sin tocar Bsale / picking / R2 bytes.
-- =============================================================================

-- 1) Clasificación (nuevo campo propuesto; default operativo)
-- ALTER TABLE distribuidora.delivery_visits
--   ADD COLUMN IF NOT EXISTS admin_classification TEXT NOT NULL DEFAULT 'operational'
--   CHECK (admin_classification IN (
--     'operational',              -- visita operativa válida
--     'pilot_test',               -- visita de prueba / piloto
--     'duplicate_pending_review', -- abierta o residual tras cierre válido
--     'voided_admin'              -- anulada administrativamente
--   ));

-- 2) Auditoría de anulación
-- ALTER TABLE distribuidora.delivery_visits
--   ADD COLUMN IF NOT EXISTS voided_at TIMESTAMPTZ,
--   ADD COLUMN IF NOT EXISTS voided_by_user_id INTEGER,
--   ADD COLUMN IF NOT EXISTS void_reason TEXT,
--   ADD COLUMN IF NOT EXISTS void_ticket_ref TEXT,
--   ADD COLUMN IF NOT EXISTS replaces_voided_visit_id BIGINT
--     REFERENCES distribuidora.delivery_visits (id);

-- 3) Evento de negocio obligatorio en cada anulación / reclasificación
-- insert into delivery_visit_events(action=...)
--   'admin_classified' | 'admin_voided' | 'admin_reopen_authorized'

-- 4) Reglas de runtime (servicio, no SQL):
--   - get_valid_closed_visit EXCLUYE admin_classification IN ('voided_admin','pilot_test')
--     salvo que se defina que pilot_test también bloquea hasta anulación.
--   - Recomendación piloto: pilot_test y voided_admin NO bloquean nueva visita.
--   - duplicate_pending_review: no operable por chofer; solo lectura + cola admin.
--   - Anular abierta: classification=voided_admin, status puede permanecer in_progress
--     o pasar a cancelled; preferir cancelled + voided_admin para liberar índice único.
--   - Anular finalizada de prueba: voided_admin + evento; entonces start_visit permitido.
--   - Segunda entrega excepcional: admin_reopen_authorized con actor+motivo+ticket;
--     crea visita nueva (no reabre la anulada).

-- 5) Procedimiento operativo propuesto (humano + API admin futura)
--   a) SELECT auditoría (sin escritura) de ambas visitas y conteos.
--   b) Decisión: ¿#1 prueba o operativa? ¿#3 solo residual?
--   c) Endpoint admin POST /admin/visitas/{id}/classify {classification, reason}
--   d) Endpoint admin POST /admin/visitas/{id}/void {reason, ticket_ref}
--      - Requiere rol oficina/admin; escribe evento; no borra líneas/evidencias.
--   e) Si se anula el cierre que bloqueaba: chofer puede iniciar entrega real.
--   f) Si se confirma #1 operativa: marcar #3 duplicate_pending_review o voided_admin.

-- 6) Criterio explícito
--   closure_review_status='verified' ≠ entrega física confirmada en terreno.
--   Solo indica que el software cerró con las reglas de evidencia/conformidad.

-- FIN PROPUESTA — esperar autorización antes de crear migración real.
