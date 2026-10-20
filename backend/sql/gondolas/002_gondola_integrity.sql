-- =============================================================================
-- Control de Góndolas — 002: integridad de la lista de reposición.
--
-- Corrección incremental e IDEMPOTENTE sobre `001_gondola_control.sql`:
-- agrega la clave foránea compuesta que impide que un SKU ajeno al universo
-- congelado de un recorrido aparezca en su lista de reposición.
--
-- Ya aplicada manualmente en la base principal (FK `fk_gondola_replenishment_item`
-- confirmada con convalidated = true). Este archivo documenta la corrección y
-- permite aplicarla en instalaciones donde todavía falte.
--
-- NO modifica bsale_raw, stock, documentos ni precios de Bsale.
-- Seguro de re-ejecutar: si la FK ya existe, no hace nada.
-- =============================================================================

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_gondola_replenishment_item'
          AND conrelid = 'app.gondola_replenishment'::regclass
    ) THEN
        ALTER TABLE app.gondola_replenishment
            ADD CONSTRAINT fk_gondola_replenishment_item
            FOREIGN KEY (session_id, variant_id)
            REFERENCES app.gondola_session_items (session_id, variant_id)
            ON DELETE RESTRICT;
    END IF;
END;
$$;

COMMENT ON CONSTRAINT fk_gondola_replenishment_item ON app.gondola_replenishment IS
    'Solo puede aparecer en reposición un SKU del universo congelado del recorrido (gondola_session_items).';

COMMIT;
