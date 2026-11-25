-- =============================================================================
-- warehouse / 000 — PRECHECK SELECT ONLY (antes de 001–004)
-- No modifica datos. Ejecutar en la BD destino (analytics) tras confirmar backup.
-- =============================================================================

-- 1) Identidad del destino
SELECT
    current_database() AS db,
    current_user AS db_user,
    inet_server_addr()::text AS server_addr,
    current_setting('server_version') AS pg_version,
    now() AS checked_at;

-- Esperado: db = 'analytics'
-- Si db NO es la BD operativa de api.quillotana.cl, DETENER.

-- 2) Schema warehouse aún no debe existir (primera puesta en marcha)
SELECT EXISTS (
    SELECT 1 FROM pg_namespace WHERE nspname = 'warehouse'
) AS warehouse_exists;

-- Esperado primera vez: warehouse_exists = false
-- Si true: revisar tablas existentes antes de re-aplicar (001–004 son idempotentes
-- pero no sobrescriben drafts con geometría).

-- 3) Schemas existentes (solo lectura de catálogo)
SELECT nspname
FROM pg_namespace
WHERE nspname IN ('warehouse', 'bsale', 'bsale_raw', 'distribuidora', 'app', 'public')
ORDER BY 1;

-- Esperado: bsale, bsale_raw, distribuidora, app, public presentes; warehouse ausente

-- 4) ¿Puede este rol crear schema? (necesario para 001)
SELECT
    has_database_privilege(current_user, current_database(), 'CREATE') AS can_create_db_objects,
    current_setting('is_superuser') AS is_superuser;

-- Esperado: can_create_db_objects = true  (o usar un rol que sí pueda para 001)

-- 5) Oficina Bsale office_id=1 (referencia lógica del seed; NO hay FK desde warehouse)
SELECT c.column_name, c.data_type
FROM information_schema.columns c
WHERE c.table_schema = 'bsale' AND c.table_name = 'offices'
ORDER BY c.ordinal_position;

-- Luego (ajustar nombres de columna según el resultado anterior; típico bsale_id):
-- SELECT * FROM bsale.offices WHERE company_id = 3 AND bsale_id = 1 LIMIT 5;
-- Esperado: existe la oficina 1 de company 3 (Bodega Central).

-- 6) Baseline opcional de schemas ajenos (anotar conteos; no deben bajar por warehouse)
-- Ejecutar solo si las tablas existen:
-- SELECT COUNT(*) AS n_products FROM bsale.products;
-- SELECT COUNT(*) AS n_documents FROM distribuidora.documents;
