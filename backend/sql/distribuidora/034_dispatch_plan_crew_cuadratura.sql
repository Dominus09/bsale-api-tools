-- Dotación congelada en plan + cuadratura operacional por ruta.

ALTER TABLE distribuidora.dispatch_plan
    ADD COLUMN IF NOT EXISTS driver_name TEXT,
    ADD COLUMN IF NOT EXISTS assistant_names TEXT;

COMMENT ON COLUMN distribuidora.dispatch_plan.driver_name IS
    'Nombre chofer al confirmar planificación (congelado).';
COMMENT ON COLUMN distribuidora.dispatch_plan.assistant_names IS
    'Nombres peoneta(s) separados por coma al confirmar.';

CREATE TABLE IF NOT EXISTS distribuidora.dispatch_plan_cuadratura (
    dispatch_plan_id   BIGINT PRIMARY KEY
        REFERENCES distribuidora.dispatch_plan (id) ON DELETE CASCADE,
    transferencia_clp  INTEGER NOT NULL DEFAULT 0,
    efectivo_clp       INTEGER NOT NULL DEFAULT 0,
    cheque_clp         INTEGER NOT NULL DEFAULT 0,
    debito_clp         INTEGER NOT NULL DEFAULT 0,
    observacion        TEXT,
    credit_notes       JSONB NOT NULL DEFAULT '[]'::jsonb,
    not_loaded         JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dispatch_plan_cuadratura_updated
    ON distribuidora.dispatch_plan_cuadratura (updated_at DESC);

COMMENT ON TABLE distribuidora.dispatch_plan_cuadratura IS
    'Cuadratura operacional: medios de pago, NC y no cargados por plan de despacho.';
