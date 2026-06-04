# Planificación — Picking V1

## Fuente de verdad

Los pickings **no** se arman desde órdenes de compra crudas. Se calculan solo con:

- Documentos facturados confirmados en Bsale (`document_related`, tipos 1/6)
- Auto-confirmados operacionales (score ≥ 75, `invoicing_auto_confirm.py`)

Los probables (60–74) solo entran si se llama `POST /picking/generate?include_probable=true`.

## Persistencia SQL (schema `distribuidora`)

| Tabla | Rol |
|-------|-----|
| `dispatch_plan_pickings` | Cabecera versionada (`version`, `is_current`, `header` JSONB) |
| `dispatch_plan_picking_clients` | Paradas por documento (ruta, cliente, coords, totales) |
| `dispatch_plan_picking_products` | Consolidado bodega por tipo / barcode |

Vistas alias para integraciones (migración **031**):

- `dispatch_plan_snapshots`
- `dispatch_plan_snapshot_clients` — consumo directo App Choferes
- `dispatch_plan_snapshot_products`

## API

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/dispatch-plans/{id}/dashboard` | Incluye `load_summary` (resumen de carga) |
| POST | `/dispatch-plans/{id}/picking/generate` | Nueva versión persistida |
| GET | `/dispatch-plans/{id}/picking-cliente` | Picking cliente (snapshot actual) |
| GET | `/dispatch-plans/{id}/picking-producto` | Picking producto |
| GET | `.../picking-cliente/export` | Excel |
| GET | `.../picking-producto/export` | Excel |

Al **confirmar** un plan, si la facturación ya está completa (confirmados + auto, sin probables pendientes), se genera automáticamente la versión 1 del picking.

## Estados operacionales (`load_summary.operational_status`)

- `FACTURACION_PENDIENTE` — faltan documentos o hay probables sin resolver
- `LISTO_PARA_CARGA` — facturación lista, sin snapshot
- `PICKING_GENERADO` — existe picking `is_current`
- `DESPACHADO` — plan en `dispatched` / `delivered`

## Migraciones requeridas

Ejecutar en orden: **026**, **027**, **031** (y **028–030** si aún no están).

```bash
# vía sync_repo o runner SQL del proyecto
```

## Frontend

- Bloque **Resumen de carga** en detalle de planificación (`DispatchPlanLoadSummaryBlock`)
- Tabs picking cliente / producto con Excel y PDF (logo Quillotana + KPIs en PDF)

## App Choferes (futuro)

Leer `dispatch_plan_snapshot_clients` filtrando `snapshot_is_current = TRUE` y `dispatch_plan_id`. No recalcular desde OC.
