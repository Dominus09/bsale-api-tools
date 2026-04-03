-- Tabla de reglas de margen (no modifica variants/products/variant_prices/variant_cost).
-- Ejecutar solo si aún no existe en PostgreSQL.
-- Claves alineadas con Bsale: product_type_id y price_list_id son los mismos ids que en sync.

CREATE TABLE IF NOT EXISTS bsale.margin_rules (
    id              SERIAL PRIMARY KEY,
    company_id      INTEGER NOT NULL,
    product_type_id INTEGER,
    price_list_id   INTEGER NOT NULL,
    min_margin      NUMERIC NOT NULL,
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Una regla por combinación (empresa, tipo, lista). Varios NULL en product_type_id pueden coexistir en PG;
-- conviene acordar si NULL significa “todos los tipos” y mantener una sola fila por (company_id, price_list_id).
CREATE UNIQUE INDEX IF NOT EXISTS margin_rules_company_type_list_uidx
    ON bsale.margin_rules (company_id, price_list_id, COALESCE(product_type_id, -1));
