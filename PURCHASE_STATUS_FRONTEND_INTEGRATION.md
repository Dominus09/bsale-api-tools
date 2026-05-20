# Integración frontend — estado de facturación OC (FASE 7.17)

## SQL final

### Vista principal (operacional)

`distribuidora.v_purchase_document_status_full`

Expone folios y cliente legibles + match probable + flags de score.

| Bloque | Campos |
|--------|--------|
| OC | `oc_document_id`, `oc_number`, `oc_emission_date`, `oc_total_amount`, `oc_client_id`, `oc_client_name` |
| Confirmada | `invoicing_document_id`, `invoicing_document_type_id`, `invoicing_number`, `invoicing_emission_date` |
| Probable | `candidate_document_id`, `candidate_number`, `candidate_document_type`, `candidate_document_type_label`, `candidate_emission_date`, `candidate_total_amount` |
| Heurística | `score`, `match_products_pct`, `same_client`, `same_seller`, `same_day`, `same_amount`, `tracking_match` |
| UI | `status`, `estado_real`, `associated_document_label`, `display_score` |

### Valores `status` (prioridad)

1. `FACTURADA_CONFIRMADA` — `document_related` → boleta/factura 1/6  
2. `PROBABLE_FACTURADA_HIGH` — score ≥ 90  
3. `PROBABLE_FACTURADA_MEDIUM` — score ≥ 75  
4. `PROBABLE_FACTURADA_LOW` — score ≥ 60  
5. `PENDIENTE` — sin match

### Compatibilidad API

`distribuidora.v_purchase_document_status` — alias sobre `_full` (columnas históricas `document_id`, `probable_*`).  
`distribuidora.v_orders_purchase_enriched` — incluye todos los campos de `_full` para `GET /distribuidora/orders/purchase`.

Migración: `backend/sql/distribuidora/015_v_purchase_document_status_full.sql`

### Consulta operacional (sin IDs en pantalla)

```sql
SELECT
    oc_number,
    oc_client_name,
    status,
    associated_document_label,
    display_score,
    match_products_pct,
    same_client,
    same_day
FROM distribuidora.v_purchase_document_status_full
WHERE oc_emission_date >= '2026-05-01'
  AND oc_emission_date < '2026-06-01'
ORDER BY oc_emission_date DESC;
```

Validación OC 66697:

```sql
SELECT oc_number, status, associated_document_label, display_score, score
FROM distribuidora.v_purchase_document_status_full
WHERE oc_number = 66697;
-- Esperado: PROBABLE_FACTURADA_HIGH, Boleta 2616098, score ~92+
```

## Mapping colores (Tailwind)

| `status` | Badge | Clases |
|----------|-------|--------|
| `FACTURADA_CONFIRMADA` | ✔ Facturada | `bg-green-100 text-green-800` |
| `PROBABLE_FACTURADA_HIGH` | ⚠ Probable Facturada | `bg-yellow-200 text-yellow-900` |
| `PROBABLE_FACTURADA_MEDIUM` | ⚠ Probable Facturada | `bg-yellow-100 text-yellow-800` |
| `PROBABLE_FACTURADA_LOW` | ⚠ Probable Facturada | `bg-orange-100 text-orange-800` |
| `PENDIENTE` | ○ Pendiente | `bg-gray-100 text-gray-700` |

Implementación: `frontend/lib/purchase-invoice-status.ts` → `purchaseStatusBadgeClass()`.

## Tooltips

| Estado | Texto |
|--------|-------|
| Confirmada | Relación confirmada vía relateddetailid |
| Probable | Coincidencia operacional detectada automáticamente (Bsale API no expone relación oficial) |
| Pendiente | No se detectaron relaciones operacionales |

## Componentes frontend

| Archivo | Rol |
|---------|-----|
| `frontend/lib/purchase-invoice-status.ts` | Resolución de código, labels, colores, documento asociado, score |
| `frontend/components/distribuidora/orders/PurchaseInvoiceStatusBadge.tsx` | Badge + tooltip |
| `frontend/components/distribuidora/orders/PurchaseInvoiceTableCells.tsx` | Celdas Estado / Documento / Score |
| `frontend/components/distribuidora/orders/OrdersTable.tsx` | Tabla OC con 3 columnas nuevas |
| `frontend/components/distribuidora/orders/OrdersFilters.tsx` | Filtro estado + solo sin confirmada |
| `frontend/app/(dashboard)/distribuidora/ordenes-compra/page.tsx` | Pantalla dedicada órdenes de compra |

## Tabla — ejemplos visuales

| Estado | Documento asociado | Score |
|--------|-------------------|-------|
| ✔ Facturada | Boleta 2615000 | 100 |
| ⚠ Probable Facturada | Boleta 2616098 | 92 |
| ○ Pendiente | — | — |

**Regla UX:** nunca mostrar `oc_document_id` ni `candidate_document_id` en UI; usar `associated_document_label` o `Boleta {number}`.

## Filtros

### API (`GET /distribuidora/orders/purchase`)

| Query | Valores |
|-------|---------|
| `invoice_status` | `confirmed` \| `probable` \| `pending` |
| `only_not_invoiced` | `true` — excluye solo **confirmadas** (probables siguen visibles) |

### Frontend

- Select en `OrdersFilters`: Todos / Solo confirmadas / Solo probables / Solo pendientes  
- Ruta: `/distribuidora/ordenes-compra`

## Pantallas integradas

- `/distribuidora/ordenes-compra` — listado principal  
- `/distribuidora/pre-planificacion` — columnas Estado, Documento, Score  
- `/distribuidora/orders` (pre-despacho) — mismas columnas en tabla de planificación  

## Edge cases

| Caso | Comportamiento UI |
|------|-------------------|
| Confirmada + probable en BD | Muestra confirmada (prioridad SQL) |
| Probable sin job ejecutado | Pendiente, documento — |
| Múltiples candidatos | Solo el de mayor `score` en vista |
| Boleta consolidada (monto distinto) | Probable con score alto; label `Boleta NNN` |
| `associated_document_label` null | Fallback desde `invoicing_number` / `candidate_number` en TS |

## Despliegue

1. Aplicar migración 015 (sync o `ensure_distribuidora_schema`).  
2. Job probable mayo (si aún no): `python -m backend.jobs.build_probable_invoice_matches_may_2026`  
3. Reiniciar API + frontend.
