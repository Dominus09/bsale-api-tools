# Auditoría maestro logístico

Documento vivo. Regenerar contra la base de datos de producción o staging tras aplicar la migración `033`.

## Cómo regenerar

```bash
# 1. Migración canónica (nombres en inglés + vista)
psql "$DATABASE_URL" -f backend/sql/033_products_master_logistics_canonical.sql

# 2. Auditoría SQL (detalle por tabla)
psql "$DATABASE_URL" -f backend/sql/diagnostics/products_master_logistics_audit.sql

# 3. Informe markdown (KPIs agregados)
python -m backend.audits.audit_products_master_logistics --write-doc
```

## Alcance

| Tabla | Qué mide |
|-------|----------|
| `bsale.products_master` | Maestro logístico oficial (barcode, CxC, proveedor, peso, dimensiones) |
| `bsale.variants` | Fuente Bsale sincronizada |
| `bsale.products` | Catálogo padre Bsale |

## Métricas esperadas (products_master)

| Métrica | Descripción |
|---------|-------------|
| Total productos | Filas activas en `products_master` |
| Con barcode | `barcode` no vacío |
| Con `units_per_box` | CxC > 0 |
| Con proveedor | `supplier_id` asignado (Compras) |
| Con peso | `weight_box_kg` > 0 |
| Con dimensiones | `height_cm`, `width_cm`, `length_cm` > 0 |
| Completitud % | Promedio de 4 ejes: proveedor, CxC, peso, dimensiones |

## Brechas típicas

- **Variantes sin PM:** barcode en `variants` sin fila en `products_master` → ejecutar `python -m backend.jobs.sync_bsale_catalog`.
- **CxC faltante:** ejecutar backfill SEC en el mismo job.
- **Peso/dimensiones:** solo ingreso manual en Maestro Logístico o import futuro.

## Vista para consumo downstream

`bsale.v_product_logistics` expone `weight_unit_kg` y `volume_m3` calculados (no almacenados).

## Última corrida

_Pendiente: ejecutar el script de auditoría en el entorno con credenciales PG._
