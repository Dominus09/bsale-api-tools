-- Alias de vistas para App Choferes y columnas adicionales en picking persistido.

ALTER TABLE distribuidora.dispatch_plan_picking_clients
    ADD COLUMN IF NOT EXISTS delivery_notes TEXT;
-- +go

ALTER TABLE distribuidora.dispatch_plan_picking_clients
    ADD COLUMN IF NOT EXISTS stop_status TEXT NOT NULL DEFAULT 'pending';
-- +go

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_dispatch_plan_picking_clients_stop_status'
    ) THEN
        ALTER TABLE distribuidora.dispatch_plan_picking_clients
            ADD CONSTRAINT chk_dispatch_plan_picking_clients_stop_status
            CHECK (stop_status IN ('pending', 'loaded', 'dispatched', 'delivered'));
    END IF;
END $$;
-- +go

ALTER TABLE distribuidora.dispatch_plan_picking_products
    ADD COLUMN IF NOT EXISTS product_id BIGINT;
-- +go

ALTER TABLE distribuidora.dispatch_plan_picking_products
    ADD COLUMN IF NOT EXISTS variant_id BIGINT;
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan_picking_clients.delivery_notes IS
    'Notas de entrega (tracking / instrucciones chofer).';
-- +go

COMMENT ON COLUMN distribuidora.dispatch_plan_picking_clients.stop_status IS
    'Estado parada: pending, loaded, dispatched, delivered.';
-- +go

-- Snapshots versionados (alias legibles para integraciones).
CREATE OR REPLACE VIEW distribuidora.dispatch_plan_snapshots AS
SELECT
    p.id AS snapshot_id,
    p.dispatch_plan_id,
    p.version,
    p.is_current,
    p.include_probable,
    p.generated_at,
    p.superseded_at,
    p.header,
    p.warnings,
    p.stops_count,
    p.product_lines_count,
    p.document_total_clp,
    p.product_total_monto_clp
FROM distribuidora.dispatch_plan_pickings p;
-- +go

CREATE OR REPLACE VIEW distribuidora.dispatch_plan_snapshot_clients AS
SELECT
    c.id,
    c.picking_id AS snapshot_id,
    c.dispatch_plan_id,
    p.version AS snapshot_version,
    p.is_current AS snapshot_is_current,
    c.route_order,
    c.client_id,
    c.client_name AS cliente,
    c.fantasy_name AS nombre_fantasia,
    c.city AS ciudad,
    c.address AS direccion,
    c.phone AS telefono,
    c.lat,
    c.lng,
    c.related_document_id AS document_id,
    c.document_number,
    c.document_type_label AS document_type,
    c.payment_method,
    c.seller_name,
    c.document_total AS total_amount,
    c.observations,
    c.delivery_notes,
    c.stop_status,
    c.relation_source,
    c.inclusion,
    c.is_probable_included,
    c.probable_score
FROM distribuidora.dispatch_plan_picking_clients c
INNER JOIN distribuidora.dispatch_plan_pickings p ON p.id = c.picking_id;
-- +go

CREATE OR REPLACE VIEW distribuidora.dispatch_plan_snapshot_products AS
SELECT
    pr.id,
    pr.picking_id AS snapshot_id,
    pr.dispatch_plan_id,
    p.version AS snapshot_version,
    p.is_current AS snapshot_is_current,
    pr.sort_order,
    pr.product_id,
    pr.variant_id,
    pr.codigo_barras AS barcode,
    pr.producto AS product_name,
    pr.variante AS variant_name,
    pr.tipo_producto AS product_type,
    pr.unidades AS units,
    pr.cajas AS boxes,
    pr.units_per_box,
    pr.total_monto AS total_amount
FROM distribuidora.dispatch_plan_picking_products pr
INNER JOIN distribuidora.dispatch_plan_pickings p ON p.id = pr.picking_id;
-- +go

COMMENT ON VIEW distribuidora.dispatch_plan_snapshot_clients IS
    'Paradas del snapshot actual/histórico para App Choferes (sin recalcular desde OC).';
-- +go
