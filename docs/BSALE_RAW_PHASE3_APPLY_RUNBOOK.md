# BSALE_RAW — Runbook de aplicación fase 3

Aplicación **manual**. Primero en entorno de prueba, luego en producción.
Sólo crea el schema nuevo `bsale_raw`. No toca `bsale`, `distribuidora` ni los syncs actuales.

Ejecutar desde la raíz del repo. `$DB` = cadena de conexión (no pegarla en archivos del repo).
Todas las ejecuciones usan `ON_ERROR_STOP=1`: si algo falla, psql se detiene y la transacción del archivo hace ROLLBACK.

---

## PASO 0 — Prechecks (sólo lectura)

```sql
-- a) bsale_raw todavía no existe → debe devolver NULL
SELECT to_regnamespace('bsale_raw') AS bsale_raw;

-- b) bsale.companies.company_id es BIGINT → debe devolver 'bigint'
SELECT data_type
FROM information_schema.columns
WHERE table_schema = 'bsale' AND table_name = 'companies' AND column_name = 'company_id';

-- c) existen las empresas 1, 2, 3 → deben salir 3 filas (requerido por el seed del PASO 10)
SELECT company_id FROM bsale.companies WHERE company_id IN (1, 2, 3) ORDER BY company_id;
```

Además:

- Respaldo / snapshot reciente de la base.
- Las variables `BSALE_TOKEN_Mini`, `BSALE_TOKEN_Romero` y `BSALE_TOKEN_SPA` existen en el entorno del backend (verificar que existan, **sin imprimir su valor**).

Si a), b) o c) no dan lo esperado: **detenerse**.

---

## PASO 1–8 — Migraciones (en este orden, una por vez)

```bash
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/001_schema_sources.sql    # PASO 1
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/002_sync_control.sql      # PASO 2
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/003_configuration.sql     # PASO 3
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/004_catalog.sql           # PASO 4
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/005_inventory_pricing.sql # PASO 5
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/006_documents.sql         # PASO 6
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/007_stock_movements.sql   # PASO 7
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/008_webhooks.sql          # PASO 8
```

Cada archivo debe terminar en `COMMIT` sin errores. Si uno falla, no seguir con el siguiente.

---

## PASO 9 — Verificación (sólo lectura)

```bash
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/verify_bsale_raw.sql
```

Esperado: `NOTICE:  bsale_raw OK: 27 tablas, … columnas, … índices`.
Si sale `bsale_raw drift detectado`: **detenerse** y no aplicar el seed.

---

## PASO 10 — Seed de fuentes

```bash
psql "$DB" -v ON_ERROR_STOP=1 -f backend/sql/bsale_raw/009_seed_sources.sql
```

Idempotente (`ON CONFLICT (company_id) DO NOTHING`). Sólo guarda el **nombre** de la variable de entorno, nunca el token.

---

## PASO 11 — Revisar fuentes

```sql
SELECT company_id, cpn_id, name, token_env, active
FROM bsale_raw.sources
ORDER BY company_id;
```

Esperado exactamente:

| company_id | cpn_id | token_env |
|---|---|---|
| 1 | 96674 | BSALE_TOKEN_Mini |
| 2 | 5807 | BSALE_TOKEN_Romero |
| 3 | 21884 | BSALE_TOKEN_SPA |

---

## PASO 12 — Conteo de tablas

```sql
SELECT count(*) AS tablas
FROM information_schema.tables
WHERE table_schema = 'bsale_raw' AND table_type = 'BASE TABLE';
```

Esperado: **27**.

---

## Consultas READ ONLY de control

```sql
-- schema
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'bsale_raw';

-- cantidad de tablas (27)
SELECT count(*) FROM information_schema.tables
WHERE table_schema = 'bsale_raw' AND table_type = 'BASE TABLE';

-- sources (3 filas)
SELECT company_id, cpn_id, name, token_env, active FROM bsale_raw.sources ORDER BY company_id;

-- tablas de sync vacías (todo en 0)
SELECT 'sync_runs' AS tabla, count(*) FROM bsale_raw.sync_runs
UNION ALL SELECT 'sync_entity_runs', count(*) FROM bsale_raw.sync_entity_runs
UNION ALL SELECT 'sync_state', count(*) FROM bsale_raw.sync_state
UNION ALL SELECT 'sync_cursors', count(*) FROM bsale_raw.sync_cursors;

-- tablas raw vacías (todo en 0)
SELECT 'offices' AS tabla, count(*) FROM bsale_raw.offices
UNION ALL SELECT 'taxes', count(*) FROM bsale_raw.taxes
UNION ALL SELECT 'document_types', count(*) FROM bsale_raw.document_types
UNION ALL SELECT 'product_types', count(*) FROM bsale_raw.product_types
UNION ALL SELECT 'price_lists', count(*) FROM bsale_raw.price_lists
UNION ALL SELECT 'products', count(*) FROM bsale_raw.products
UNION ALL SELECT 'variants', count(*) FROM bsale_raw.variants
UNION ALL SELECT 'clients', count(*) FROM bsale_raw.clients
UNION ALL SELECT 'stocks', count(*) FROM bsale_raw.stocks
UNION ALL SELECT 'variant_prices', count(*) FROM bsale_raw.variant_prices
UNION ALL SELECT 'variant_costs', count(*) FROM bsale_raw.variant_costs
UNION ALL SELECT 'documents', count(*) FROM bsale_raw.documents
UNION ALL SELECT 'document_details', count(*) FROM bsale_raw.document_details
UNION ALL SELECT 'document_references', count(*) FROM bsale_raw.document_references
UNION ALL SELECT 'document_sellers', count(*) FROM bsale_raw.document_sellers
UNION ALL SELECT 'document_change_log', count(*) FROM bsale_raw.document_change_log
UNION ALL SELECT 'stock_receptions', count(*) FROM bsale_raw.stock_receptions
UNION ALL SELECT 'stock_reception_details', count(*) FROM bsale_raw.stock_reception_details
UNION ALL SELECT 'stock_consumptions', count(*) FROM bsale_raw.stock_consumptions
UNION ALL SELECT 'stock_consumption_details', count(*) FROM bsale_raw.stock_consumption_details
UNION ALL SELECT 'webhook_events', count(*) FROM bsale_raw.webhook_events
UNION ALL SELECT 'webhook_resource_responses', count(*) FROM bsale_raw.webhook_resource_responses;
```

---

## Si algo falla

- Cada archivo es una transacción: un error deja ese archivo sin efecto.
- Antes del cutover (nadie lee `bsale_raw` todavía) se puede volver a cero con `DROP SCHEMA bsale_raw CASCADE;` y repetir desde el PASO 1.
- Después del cutover **no** se borra el schema: desactivar jobs/webhooks, volver los consumidores a la ruta actual, corregir y retomar.
