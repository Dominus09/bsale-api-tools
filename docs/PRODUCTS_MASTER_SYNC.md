# Sincronización `products_master` + maestro logístico

## Arquitectura

| Capa | Tablas / vista | Rol |
|------|----------------|-----|
| Fuente Bsale | `bsale.products`, `bsale.variants` | Verdad operativa del catálogo API |
| Maestro ERP | `bsale.products_master` | Consolidación por barcode + datos logísticos manuales |

**Reglas críticas**

- Nunca `DELETE` ni `TRUNCATE` de `products_master`.
- UPSERT incremental por `barcode` (`ON CONFLICT DO UPDATE`).
- En UPDATE desde Bsale: solo nombre, variante, ids, `units_per_box`, `sku`, `product_type`, `companies`, `last_bsale_sync_at`.
- No se tocan en sync: `supplier_id`, `peso_caja_kg`, dimensiones, `logistics_completed`.

## Migración DDL

Aplicar en PostgreSQL (una vez por entorno):

```bash
psql "$DATABASE_URL" -f backend/sql/032_products_master_logistics.sql
```

Añade columnas logísticas, `units_per_box` en `variants` y `products_master`, índices y comentarios.

## Job Coolify: `sync_bsale_catalog`

```bash
python -m backend.jobs.sync_bsale_catalog
```

Secuencia:

1. `sync_catalog.py` (raíz del repo)
2. `sync_prices_costs.py`
3. `sync_stock.py`
4. `backfill_units_per_box_from_sec()` — patrón `(SEC N)` en `variants.description`
5. `refresh_products_master()` — UPSERT seguro

Logs con prefijo `[CATALOG_SYNC]`:

- `productos_nuevos_estimados_antes`
- `units_per_box_actualizados`
- `products_master_insertados`
- `products_master_actualizados`
- `errores`

Variables: mismas que el backend (`PG_*`, tokens Bsale de los scripts raíz).

Frecuencia sugerida: **1–2 veces al día** (o tras cambios masivos de catálogo en Bsale). Timeout: **45–90 min** según volumen.

## UI

**Distribuidora → Maestro logístico productos** (`/distribuidora/maestro-logistico`)

Edición inline de CxC, peso/dimensiones de caja y proveedor. Peso unitario = `peso_caja_kg / units_per_box` (solo lectura en API).

## Código relacionado

- `backend/services/bsale/catalog_sync_service.py`
- `backend/jobs/sync_bsale_catalog.py`
- `backend/routers/products_master.py`
- `backend/sql/032_products_master_logistics.sql`
