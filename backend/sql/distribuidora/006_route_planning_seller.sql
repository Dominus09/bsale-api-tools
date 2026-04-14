-- Vendedor en filas de planificación (denormalizado al confirmar).

ALTER TABLE distribuidora.route_planning
    ADD COLUMN IF NOT EXISTS seller TEXT;
-- +go
