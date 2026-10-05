# Sincronización `products_master` + maestro logístico

## Arquitectura

| Capa | Tablas / vista | Rol |
|------|----------------|-----|
| Fuente Bsale | `bsale.products`, `bsale.variants` | Verdad operativa del catálogo API |
| Maestro ERP | `bsale.products_master` | Consolidación por barcode + datos logísticos manuales |

**Reglas críticas**

- Nunca `DELETE` ni `TRUNCATE` de `products_master`.
- UPSERT incremental por `barcode` (`ON CONFLICT DO UPDATE`).
- No se tocan en sync: `supplier_id`, `weight_box_kg`, `height_cm`, `width_cm`, `length_cm`, `logistics_completed`, `sale_type`, `quantity_step`, `is_active`.
- En UPDATE: `sku`, `product_name`, `variant_name`, `product_type`, `companies` (desde `variants`), `units_per_box` (sin vaciar), `last_bsale_sync_at`.
- `product_id` / `variant_id` de `products_master` son **LEGACY**: sólo se rellenan si están vacíos y nunca son clave global (un `variant_id` no es único entre empresas).
- Identidad Bsale autoritativa: `bsale.product_master_variants` (`product_master_id`, `company_id`) → `variant_id`. Estados `AUTO_EXACT` / `MISSING` / `AMBIGUOUS` (escritos con `mapping_source='BARCODE'`); `MANUAL` nunca se modifica. Esquema autoritativo: `backend/sql/051_product_master_variants_sync_columns.sql`.

## Migración DDL

Aplicar en PostgreSQL (una vez por entorno):

```bash
psql "$DATABASE_URL" -f backend/sql/032_products_master_logistics.sql
psql "$DATABASE_URL" -f backend/sql/033_products_master_logistics_canonical.sql
```

Añade columnas logísticas, `units_per_box` en `variants` y `products_master`, índices y comentarios.

## Job Coolify: `sync_bsale_catalog`

```bash
python -m backend.jobs.sync_bsale_catalog
```

Secuencia (en proceso, bajo advisory lock de sesión `5927184030`; una segunda ejecución sale con código 3):

1. Catálogo por empresa (`sync_catalog.py`). Las listas de `bsale.price_lists` que dejan de aparecer en `price_lists.json` se marcan `state=1` (inactiva); nunca se borran.
2. Precios y luego costos por empresa (`sync_prices_costs.py`), en transacciones separadas. Sólo se sincronizan las listas administradas por la ERP (`backend/services/bsale/managed_price_lists.py`); las demás se ignoran (no se descargan, no cuentan como stale ni error, nunca se borran). La reconciliación de precios obsoletos se limita a las listas administradas confirmadas completas (`count` informado == descargado); una lista administrada ausente o inactiva en Bsale, con error, inconsistente o vacía con precios existentes queda degradada y conserva sus precios. Un fallo de costos no revierte precios ya confirmados.
3. Stock por empresa (`sync_stock.py`), con reconciliación de stocks obsoletos
4. `backfill_units_per_box_from_sec()` — patrón `(SEC N)` en `variants.description`
5. `refresh_products_master()` — UPSERT seguro
6. `refresh_product_master_variants()` — mappings por (company_id, barcode)

Cada empresa: descarga HTTP completa primero, luego una sola transacción (ROLLBACK ante error).
Empresas: `bsale.companies WHERE active` + variable de entorno indicada en `bsale_token`; si falta
alguna (o no están 1, 2 y 3; configurable con `BSALE_REQUIRED_COMPANY_IDS`) el job falla.

Fusible de reconciliación (stock y `variant_prices`): snapshot vacío con filas existentes o
`stale_percentage` mayor al umbral → la empresa falla con ROLLBACK y sin DELETE. Umbral (0–100, default 20):
`BSALE_RECONCILE_MAX_STALE_PCT_STOCKS`, `BSALE_RECONCILE_MAX_STALE_PCT_VARIANT_PRICES` o
`BSALE_RECONCILE_MAX_STALE_PCT` (global).

Exit codes: `0` success · `1` failed · `2` partial · `3` lock ocupado. Historial en `bsale.sync_runs`
(migración `050_bsale_sync_runs.sql`).

Logs con prefijo `[CATALOG_SYNC]`:

- `productos_nuevos_estimados_antes`
- `units_per_box_actualizados`
- `products_master_insertados`
- `products_master_actualizados`
- `errores`

Variables: mismas que el backend (`PG_*`, tokens Bsale de los scripts raíz).

Frecuencia sugerida: **1–2 veces al día** (o tras cambios masivos de catálogo en Bsale). Timeout: **45–90 min** según volumen.

HTTP Bsale (`backend/services/bsale/http_client.py`): timeouts (10 s conexión / 60 s lectura), máx. 5
intentos sólo para 408/425/429/5xx transitorios, timeouts y errores de conexión (`Retry-After` o
backoff exponencial con jitter). Un fallo de `product_taxes` tras los reintentos hace fallar la
sincronización de esa empresa: nunca se guarda un producto con `tax_factor` inventado.

## Job SEC independiente

```bash
# Preview (sin escribir)
python -m backend.jobs.backfill_units_per_box --dry-run

# Ejecutar
python -m backend.jobs.backfill_units_per_box
```

Validación SQL: `backend/sql/diagnostics/sec_backfill_validate.sql`

Log ejemplo:

```text
[SEC_BACKFILL] dry_run=false variants_total=45210 variants_con_sec=12840 variants_actualizadas=3200 products_master_actualizados=2850 duration_ms=1240
```

## UI

**Distribuidora → Maestro logístico productos** (`/distribuidora/maestro-logistico`)

Edición inline de CxC, peso/dimensiones de caja y proveedor. `weight_unit_kg = weight_box_kg / units_per_box` (calculado en API, no almacenado).

Vista SQL: `bsale.v_product_logistics` (App Choferes / carga camiones).

KPIs: `GET /products-master/logistics-stats`.

## Código relacionado

- `backend/services/bsale/catalog_sync_service.py`
- `backend/jobs/sync_bsale_catalog.py`
- `backend/routers/products_master.py`
- `backend/sql/032_products_master_logistics.sql`
