-- Ejecutar una vez en PostgreSQL (no toca schema bsale).
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.orders (
    id SERIAL PRIMARY KEY,
    client_id BIGINT,
    client_name TEXT,
    client_rut TEXT,
    price_list TEXT,
    price_list_id INTEGER,
    payment_method TEXT,
    document_type TEXT,
    contact_name TEXT,
    contact_phone TEXT,
    delivery_date DATE,
    notes TEXT,
    total NUMERIC,
    status TEXT DEFAULT 'pendiente',
    seller_id INTEGER,
    seller_name TEXT,
    client_city TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Bases ya creadas: ejecutar solo si faltan las columnas.
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS client_rut TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'pendiente';
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS payment_method TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS price_list TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS price_list_id INTEGER;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS delivery_date DATE;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS contact_name TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS contact_phone TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS document_type TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS seller_id INTEGER;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS seller_name TEXT;
ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS client_city TEXT;

CREATE TABLE IF NOT EXISTS app.order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES app.orders(id) ON DELETE CASCADE,
    product_id INTEGER,
    product_name TEXT,
    barcode TEXT,
    quantity INTEGER,
    price NUMERIC,
    subtotal NUMERIC
);
