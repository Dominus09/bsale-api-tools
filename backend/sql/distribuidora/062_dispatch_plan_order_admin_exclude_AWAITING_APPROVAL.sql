-- =============================================================================
-- 062 — Exclusión administrativa de OC en dispatch_plan_orders
-- ESTADO: LISTA PARA REVISIÓN — NO APLICAR EN PRODUCCIÓN SIN AUTORIZACIÓN
-- =============================================================================
-- Namespace: backend/sql/distribuidora/062_* (NO confundir con
-- backend/sql/062_auth_audit_events.sql del esquema app/analytics).
-- Soft-exclude: no DELETE de la fila; no muta Bsale / stock / visitas.
-- Aditiva: ADD COLUMN IF NOT EXISTS + CHECK + índice parcial.
-- Impide que la OC siga contando en KPIs del plan y libera el guard de OC
-- para otras planificaciones (ver filtros excluded_at IS NULL en código).
-- Auditoría operativa: columnas + evento en dispatch_plan_order_events.
-- Verificación read-only: ver 062_dispatch_plan_order_admin_exclude_VERIFY_SELECT_ONLY.sql
-- =============================================================================

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS excluded_at TIMESTAMPTZ;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS excluded_by_user_id INTEGER;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS excluded_by_user_name TEXT;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS exclude_reason TEXT;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD COLUMN IF NOT EXISTS exclusion_kind TEXT;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    DROP CONSTRAINT IF EXISTS ck_dispatch_plan_orders_admin_exclude_integrity;
-- +go

ALTER TABLE distribuidora.dispatch_plan_orders
    ADD CONSTRAINT ck_dispatch_plan_orders_admin_exclude_integrity
    CHECK (
        (
            excluded_at IS NOT NULL
            AND excluded_by_user_id IS NOT NULL
            AND excluded_by_user_id > 0
            AND exclude_reason IS NOT NULL
            AND length(btrim(exclude_reason)) > 0
            AND exclusion_kind IS NOT NULL
            AND exclusion_kind IN ('admin')
        )
        OR
        (
            excluded_at IS NULL
            AND excluded_by_user_id IS NULL
            AND (exclude_reason IS NULL OR length(btrim(exclude_reason)) = 0)
            AND exclusion_kind IS NULL
        )
    );
-- +go

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_orders_active
    ON distribuidora.dispatch_plan_orders (dispatch_plan_id, route_order)
    WHERE excluded_at IS NULL;
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan_orders.excluded_at IS
    'Soft-exclude administrativo; la OC permanece en Bsale y en historial del plan.';
-- +go
