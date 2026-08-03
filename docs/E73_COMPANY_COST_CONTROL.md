# E.7.3 — Control de costos consolidado por empresa

## Auditoría de endpoints (estado previo)

| Endpoint | Scope | Estado |
|----------|-------|--------|
| `/cost-analytics/v2/receptions` | office | Intactos |
| `/cost-analytics/v2/products` | office | Intactos |
| `/cost-analytics/v2/products-summary` | office | Intactos |
| `/cost-analytics/v2/*` legacy office | office | Intactos |
| `/costos` frontend legacy | — | Intactos |

## Nuevos endpoints (paralelos)

| Método | Path | Auth |
|--------|------|------|
| GET | `/cost-analytics/v2/company-products` | staff JWT |
| GET | `/cost-analytics/v2/company-summary` | staff JWT |
| GET | `/cost-analytics/v2/company-products/{variant_id}` | staff JWT |
| GET | `/cost-analytics/v2/company-products/{variant_id}/history` | staff JWT |

- **No** exigen `office_id`.
- `company_id` validado (whitelist inicial: `3`).
- Solo SELECT read-only; sin `variant_cost`; sin promedios/ponderación.

## Modelo consolidado

Jerarquía: Empresa → Producto consolidado → Oficinas → Recepciones.

- **Costo vigente**: última recepción con `corrected_gross_cost IS NOT NULL` hasta `date_to` (todas las oficinas con V2).
- **Último cambio**: último costo calculable **distinto** (no `rn = 2`).
- **`date_from`**: no elimina el vigente; limita cambios del periodo, historial mostrado y conteos.
- **Cobertura**: oficinas operativas de control (empresa 3: Bodega Central, Supermercado, Q1, Q2) vs oficinas con filas V2.

## Archivos backend

- `backend/schemas/cost_v2_company_read.py`
- `backend/repositories/cost_v2_company_read_repo.py`
- `backend/services/cost_v2_company_read_service.py`
- `backend/routers/cost_analytics.py` (rutas nuevas)
- `backend/tests/test_cost_v2_company_products_api.py`

## Archivos frontend

- `frontend/app/(dashboard)/costos-v2/page.tsx`
- `frontend/components/costos-v2/cost-v2-company-filters.tsx`
- `frontend/components/costos-v2/cost-v2-control-kpis.tsx`
- `frontend/components/costos-v2/cost-v2-products-table.tsx`
- `frontend/components/costos-v2/cost-v2-product-detail-drawer.tsx`
- `frontend/components/costos-v2/cost-v2-symbology-panel.tsx`
- `frontend/lib/costos-v2/{api,types,format,labels}.ts`

## Confirmaciones

- Cursor **no** desplegó producción.
- Cursor **no** aplicó migraciones.
- Cursor **no** ejecutó índices automáticamente.
- Jobs / tablas históricas / cálculos tributarios V2 / `variant_cost` **no** modificados.

Ver también:

- `docs/e73_company_cost_examples.json`
- `docs/e73_explain_and_indexes.md`
- `docs/e73_office_coverage_commands.md`
