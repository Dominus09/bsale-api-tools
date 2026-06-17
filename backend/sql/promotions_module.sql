-- =============================================================================
-- Módulo promotions (app.*): reemplazo del modelo de ofertas legacy en bsale.
-- Ejecutar manualmente / migración. Ajustar search_path si hace falta.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS app;

-- Legacy ofertas (nombres sin esquema por compatibilidad + bsale del repo)
DROP VIEW IF EXISTS active_offers_view CASCADE;
DROP TABLE IF EXISTS product_offers CASCADE;
DROP VIEW IF EXISTS bsale.active_offers_view CASCADE;
DROP TABLE IF EXISTS bsale.product_offers CASCADE;

-- Cabecera
CREATE TABLE IF NOT EXISTS app.promotions (
    id SERIAL PRIMARY KEY,
    tipo VARCHAR(20) NOT NULL,
    canal VARCHAR(20) NOT NULL,
    fecha_inicio DATE NOT NULL,
    fecha_fin DATE NOT NULL,
    activa BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT promotions_tipo_chk CHECK (tipo IN ('oferta', 'remate', 'promocion')),
    CONSTRAINT promotions_canal_chk CHECK (canal IN ('ruta', 'detalle'))
);

CREATE TABLE IF NOT EXISTS app.promotion_items (
    id SERIAL PRIMARY KEY,
    promotion_id INTEGER NOT NULL REFERENCES app.promotions (id) ON DELETE CASCADE,
    barcode VARCHAR(50) NOT NULL,
    tipo_descuento VARCHAR(20) NOT NULL,
    valor NUMERIC(12, 2) NOT NULL,
    observacion TEXT,
    CONSTRAINT promotion_items_desc_chk CHECK (tipo_descuento IN ('porcentaje', 'precio_fijo'))
);

CREATE TABLE IF NOT EXISTS app.promotion_companies (
    id SERIAL PRIMARY KEY,
    promotion_id INTEGER NOT NULL REFERENCES app.promotions (id) ON DELETE CASCADE,
    company_id INTEGER NOT NULL,
    price_list VARCHAR(50) NULL
);

CREATE TABLE IF NOT EXISTS app.promotion_price_snapshot (
    id SERIAL PRIMARY KEY,
    promotion_id INTEGER NOT NULL REFERENCES app.promotions (id) ON DELETE CASCADE,
    barcode VARCHAR(50) NOT NULL,
    company_id INTEGER NOT NULL,
    price_list VARCHAR(50) NULL,
    precio_normal NUMERIC(12, 2) NOT NULL,
    precio_oferta NUMERIC(12, 2) NOT NULL,
    regular_price NUMERIC(12, 2) NOT NULL,
    sale_price NUMERIC(12, 2) NOT NULL,
    canal VARCHAR(20) NOT NULL,
    fecha_generado TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_promotions_fecha ON app.promotions (fecha_inicio, fecha_fin);
CREATE INDEX IF NOT EXISTS idx_promotion_items_promotion_id ON app.promotion_items (promotion_id);
CREATE INDEX IF NOT EXISTS idx_promotion_companies_promotion_id ON app.promotion_companies (promotion_id);
CREATE INDEX IF NOT EXISTS idx_promotion_snapshot_promotion_id ON app.promotion_price_snapshot (promotion_id);
