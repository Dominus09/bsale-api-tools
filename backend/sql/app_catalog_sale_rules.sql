-- Reglas comerciales catálogo web: tipo de venta y paso de cantidad por producto.
-- Ejecutar una vez en PostgreSQL. No modifica app.orders ni pedidos existentes.

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS sale_type TEXT;

ALTER TABLE bsale.products_master
    ADD COLUMN IF NOT EXISTS quantity_step INTEGER;

-- Valores permitidos cuando sale_type está informado.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'products_master_sale_type_check'
          AND conrelid = 'bsale.products_master'::regclass
    ) THEN
        ALTER TABLE bsale.products_master
            ADD CONSTRAINT products_master_sale_type_check
            CHECK (
                sale_type IS NULL
                OR sale_type IN ('ENTERA', 'PARCIAL', 'UNITARIO')
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'products_master_quantity_step_check'
          AND conrelid = 'bsale.products_master'::regclass
    ) THEN
        ALTER TABLE bsale.products_master
            ADD CONSTRAINT products_master_quantity_step_check
            CHECK (quantity_step IS NULL OR quantity_step >= 1);
    END IF;
END $$;

COMMENT ON COLUMN bsale.products_master.sale_type IS
    'Regla comercial catálogo: ENTERA (caja), PARCIAL (step configurable), UNITARIO (libre).';

COMMENT ON COLUMN bsale.products_master.quantity_step IS
    'Paso mínimo de cantidad para pedidos web. ENTERA→SEC, PARCIAL→config manual, UNITARIO→1.';
