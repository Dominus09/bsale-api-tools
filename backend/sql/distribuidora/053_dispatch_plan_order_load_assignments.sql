-- Fase F — Organización anticipada de carga (pre-factura).
-- Aplicar SOLO vía apply_distribuidora_schema o psql manual revisado.
-- No ejecutar contra analytics/productivo desde el agente.
--
-- Reutiliza dispatch_plan_load_batches como contenedor de grupos.
-- Nueva relación canónica: OC (document_id) → grupo del MISMO plan.
-- Planes existentes quedan en load_organization_status='legacy' (sin bloqueo de picking).

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS load_organization_status TEXT NOT NULL DEFAULT 'legacy';

ALTER TABLE distribuidora.dispatch_plan
    DROP CONSTRAINT IF EXISTS dispatch_plan_load_organization_status_check;

ALTER TABLE distribuidora.dispatch_plan
    ADD CONSTRAINT dispatch_plan_load_organization_status_check
    CHECK (load_organization_status IN ('legacy', 'draft', 'organized'));

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS load_organization_version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS load_organization_updated_at TIMESTAMPTZ;

COMMENT ON COLUMN distribuidora.dispatch_plan.load_organization_status IS
  'legacy=sin agrupación anticipada (histórico); draft=organización en curso; organized=todas las OC del plan asignadas a un grupo.';

COMMENT ON COLUMN distribuidora.dispatch_plan.load_organization_version IS
  'Versión optimista de la organización de carga; incrementa en cada guardado atómico.';

-- Permite FK compuesta (plan, batch) para impedir grupos de otro despacho.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_plan_load_batches_plan_id
    ON distribuidora.dispatch_plan_load_batches (dispatch_plan_id, id);

-- Permite FK compuesta (plan, oc) hacia órdenes del plan.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dispatch_plan_orders_plan_oc
    ON distribuidora.dispatch_plan_orders (dispatch_plan_id, oc_document_id);

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_order_load_assignments (
    id BIGSERIAL PRIMARY KEY,
    dispatch_plan_id BIGINT NOT NULL
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    oc_document_id BIGINT NOT NULL,
    load_batch_id BIGINT NOT NULL,
    client_id INTEGER,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    assigned_by TEXT,
    CONSTRAINT uq_dispatch_plan_order_load_assignment
        UNIQUE (dispatch_plan_id, oc_document_id),
    CONSTRAINT fk_order_load_assign_batch_same_plan
        FOREIGN KEY (dispatch_plan_id, load_batch_id)
        REFERENCES distribuidora.dispatch_plan_load_batches (dispatch_plan_id, id)
        ON DELETE RESTRICT,
    CONSTRAINT fk_order_load_assign_oc_in_plan
        FOREIGN KEY (dispatch_plan_id, oc_document_id)
        REFERENCES distribuidora.dispatch_plan_orders (dispatch_plan_id, oc_document_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_order_load_assign_plan_batch
    ON distribuidora.dispatch_plan_order_load_assignments (dispatch_plan_id, load_batch_id);

CREATE INDEX IF NOT EXISTS idx_order_load_assign_client
    ON distribuidora.dispatch_plan_order_load_assignments (dispatch_plan_id, client_id)
    WHERE client_id IS NOT NULL;

COMMENT ON TABLE distribuidora.dispatch_plan_order_load_assignments IS
  'Agrupación anticipada pre-factura: OC canónica (document_id) → load_batch del mismo plan. No modifica documentos tributarios.';
