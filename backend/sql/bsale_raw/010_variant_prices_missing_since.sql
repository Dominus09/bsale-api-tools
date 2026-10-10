-- =============================================================================
-- bsale_raw 010 — variant_prices.missing_since (NO destructiva)
--
-- Un precio que un snapshot completo y válido de su lista deja de entregar se marca
-- missing_since = now() en vez de borrarse; si Bsale lo vuelve a entregar, vuelve a NULL.
-- Lo escribe sólo el motor de precios (backend/services/bsale_raw/core/price_engine.py),
-- con fusible por lista y re-chequeo de frescura (api_fetched_at <= snapshot_started_at).
--
-- Columna nullable sin DEFAULT: cambio sólo de catálogo (sin reescritura de la tabla).
-- Explícita (sin IF NOT EXISTS): aplicarla dos veces falla en vez de ocultar drift.
-- Después de aplicarla, correr verify_bsale_raw.sql.
-- =============================================================================

BEGIN;

ALTER TABLE bsale_raw.variant_prices ADD COLUMN missing_since TIMESTAMPTZ;

COMMENT ON COLUMN bsale_raw.variant_prices.missing_since IS
    'Primer snapshot completo de la lista que no trajo este precio; NULL = Bsale lo entrega. Nunca se borra.';

COMMIT;
