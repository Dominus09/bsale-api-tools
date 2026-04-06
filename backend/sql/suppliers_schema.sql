-- Tabla de proveedores para asociar productos en bsale.products_master.supplier_id.
-- Ejecutar en PostgreSQL.

CREATE TABLE IF NOT EXISTS bsale.suppliers (
    id           SERIAL PRIMARY KEY,
    name         TEXT NOT NULL,
    contact_name TEXT,
    phone        TEXT,
    email        TEXT,
    notes        TEXT,
    is_active    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_suppliers_name
    ON bsale.suppliers (name);
