-- Permite tipo 'promocion' además de oferta/remate en app.promotions
ALTER TABLE app.promotions DROP CONSTRAINT IF EXISTS promotions_tipo_chk;
ALTER TABLE app.promotions
    ADD CONSTRAINT promotions_tipo_chk CHECK (tipo IN ('oferta', 'remate', 'promocion'));
