# Auditoría `GET /distribuidora/orders/dispatch-prep/planning-rows`

## Síntoma (producción)

| Request | Tiempo |
|---------|--------|
| `planning-rows` | 20–26 s |
| `observaciones` | 2–3 s |

## Causa raíz (código anterior)

La consulta hacía:

```sql
LEFT JOIN distribuidora.v_purchase_document_status ps ON ps.document_id = d.document_id
```

Esa vista envuelve `v_purchase_document_status_full`, que por **cada OC**:

- Lee `v_orders_purchase` + cliente Bsale
- `LATERAL` sobre **todas** las filas de `document_probable_matches` candidatas
- Calcula status / score / etiquetas

Con 300–400 OCs en rango de fechas, el costo se multiplica y domina el tiempo total.

Además, por fila:

- `NOT EXISTS (document_related …)` para “solo no facturadas”
- Subquery correlacionada a `document_attributes` para filtro de observaciones (`translate(lower(...))`)

## Cambios aplicados

### 1. SQL optimizado (`orders_service.py`)

- **Sin** `v_purchase_document_status` / `_full`
- `v_orders_purchase_status` (confirmación Bsale, indexable por `document_id`)
- `LATERAL` probable solo si no está facturada (`score >= 60`, `LIMIT 1`)
- `v_oc_attributes_flat` para observaciones (sin subquery por fila)
- Filtro no facturadas: `NOT COALESCE(conf.is_invoiced, FALSE)` (misma semántica que `document_related`)

### 2. Índices (`028_planning_rows_indexes.sql`)

- `documents (company_id, office_id, document_type_id, emission_date DESC)` parcial OC
- `document_attributes (document_id)` filtro OBSERVACIONES

### 3. Instrumentación `[PLANNING_ROWS_DEBUG]`

Logs en servicio y router:

- `total_ms`, `sql_ms`, `row_count`, `payload_bytes`, `phases`
- Con `PLANNING_ROWS_EXPLAIN=true`: ranking EXPLAIN en `_debug.explain_top`

### 4. Script auditoría

```bash
PLANNING_ROWS_EXPLAIN=true python audit_planning_rows.py --from 2026-05-20 --to 2026-05-22
```

### 5. Frontend

- Pendientes / Probables / Facturadas: **solo** `filterPlanningRowsByStatus` (memoria)
- Recarga API solo: fechas, “solo no facturadas”, chip día en observaciones, sync, recargar
- Overlay “Cargando órdenes…” con logo `/placeholder-logo.png`

## Ranking esperado (antes del fix)

| Consulta / nodo | Tiempo estimado | Filas | Índice / scan |
|-----------------|-----------------|-------|----------------|
| `v_purchase_document_status` (materializada por join) | **15–22 s** | N × OC | Seq Scan + LATERAL prob |
| `NOT EXISTS document_related` | 2–4 s | por OC | Index Scan `document_details` |
| Subquery `document_attributes` OBSERVACIONES | 1–2 s | por OC | Index Scan attributes |
| `v_documents_latest` + filtro fecha | 0.5–1 s | 300–400 | Bitmap/Index `emission` |
| `bsale.clients` | < 0.2 s | 300–400 | Index `bsale_id` |

## Ranking esperado (después del fix + índice 028)

| Consulta / nodo | Tiempo objetivo | Índice |
|-----------------|-----------------|--------|
| `documents` filtro fecha+tipo | **< 500 ms** | `idx_distribuidora_documents_planning_rows` Index Scan |
| `v_orders_purchase_status` | < 300 ms | `document_id` |
| `LATERAL document_probable_matches` | < 400 ms | `idx_document_probable_matches_oc_score` |
| `bsale.clients` | < 200 ms | PK / bsale_id |
| **Total HTTP** | **< 2 s** | — |

## Verificación en producción

1. Aplicar migración **028** (o `ensure_distribuidora_schema`)
2. Desplegar backend con SQL nuevo
3. Logs: buscar `[PLANNING_ROWS_DEBUG] done total_ms=...`
4. Chrome Network: un solo `planning-rows` al cambiar fechas; **ninguno** al cambiar KPI Pendientes/Probables/Facturadas

## Variables de entorno

| Variable | Default | Efecto |
|----------|---------|--------|
| `PLANNING_ROWS_DEBUG` | true | Logs `[PLANNING_ROWS_DEBUG]` |
| `PLANNING_ROWS_EXPLAIN` | false | EXPLAIN ANALYZE + `_debug` en JSON (solo auditoría) |
