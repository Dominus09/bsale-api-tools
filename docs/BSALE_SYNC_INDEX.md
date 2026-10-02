# Índice de sincronizaciones Bsale existentes

Inventario estático del repositorio al 2026-10-01 (lectura de código y docs; **no** se consultó Coolify ni la BD). La columna "Estado" se basa en la documentación del repo (`backend/SCRIPTS_REGISTRY.md`, `COOLIFY_JOB_SETUP.md`, docstrings "Cron Coolify"). **Confirmar en Coolify qué cron está realmente programado** antes de retirar nada.

Leyenda de estado:

- **ACTIVO**: documentado como cron/Coolify productivo.
- **MANUAL**: job con dry-run/apply ejecutado a pedido.
- **LEGACY**: obsoleto, redirige a otro o escribe en NocoDB.
- **DEBUG**: herramienta de diagnóstico, no productivo.

La capa nueva `bsale_raw` **no reemplaza ninguno de estos** en fase 1.

---

## 1. Catálogo, precios, costos, stock (schema `bsale`)

| Script / módulo | Estado | Empresas / token | Endpoints Bsale | Tablas que escribe |
|---|---|---|---|---|
| `python -m backend.jobs.sync_bsale_catalog` → `backend/services/bsale/catalog_job.py` | ACTIVO (1–2/día) | 1, 2, 3 vía `bsale.companies.bsale_token` (nombre de env) | (orquesta los 3 siguientes) | `bsale.sync_runs`, `bsale.products_master`, `bsale.product_master_variants`, `bsale.variants.units_per_box` |
| `sync_catalog.py` (raíz, wrapper) → `backend/services/bsale/catalog_company_sync.py` | ACTIVO (vía job) | 1, 2, 3 | `offices`, `price_lists`, `product_types`, `taxes`, `products`, `variants`, `products/{id}` | `bsale.offices`, `bsale.price_lists`, `bsale.product_types`, `bsale.taxes`, `bsale.products`, `bsale.variants` |
| `sync_prices_costs.py` (raíz, wrapper) → `backend/services/bsale/prices_costs_sync.py` | ACTIVO (vía job) | 1, 2, 3 | `price_lists/{id}/details`, `variants/{id}/costs` | `bsale.variant_prices` (snapshot + fusible), `bsale.variant_cost` |
| `sync_stock.py` (raíz, wrapper) → `backend/services/bsale/stock_sync.py` | ACTIVO (vía job) | 1, 2, 3 | `stocks` | `bsale.stocks` (snapshot + fusible) |
| `sync_meta_bs.py` | ACTIVO? (confirmar) | `bsale.companies` | `users`, `document_types` | `bsale.bsale_users`, `bsale.document_types` |
| `sync_clients.py` | ACTIVO? (confirmar) | `bsale.companies` | `clients` | `bsale.clients` (coordenadas parseadas desde campo `facebook`) |
| `backend/jobs/sync_rutero.py` | ACTIVO | sin API (PG) | — | `bsale.rutero` |
| `generate_rutero.py` | MANUAL | sin API | — | `bsale.clients` (UPDATE) |
| `backend/jobs/backfill_units_per_box.py` | MANUAL | sin API | — | `bsale.variants`, `bsale.products_master` |
| `backend/jobs/repair_mani_marco_polo_variant_links.py` | MANUAL (one-off) | sin API | — | `bsale.products_master` |
| `sync_cost_history.py` | LEGACY (NocoDB) | `bsale.companies` vía NocoDB | `variants/{id}/costs` | NocoDB (no PG) |
| `sync_analytics_margin.py` | LEGACY (NocoDB) | — | — (lee NocoDB) | NocoDB |

## 2. Documentos (schemas `bsale` y `distribuidora`)

| Script / módulo | Estado | Empresas / token | Endpoints Bsale | Tablas que escribe |
|---|---|---|---|---|
| `python -m backend.jobs.live_sync_documents` → `distribuidora/live_sync_service.py` | ACTIVO (`*/5`) | 3 (`BSALE_TOKEN` / `BSALE_TOKEN_SPA`) | `documents` (`emissiondaterange`) | `distribuidora.documents`, `document_sellers`, `document_attributes`, `document_references`, `sync_*` (vía funciones de `sync_service.py`) |
| `python -m backend.jobs.live_sync_details` | ACTIVO (`*/15`) | 3 | `documents/{id}/details` | `distribuidora.document_details` |
| `python -m backend.jobs.live_sync_related` | ACTIVO (`*/20`) | 3 | `documents?relateddetailid=` | `distribuidora.document_related` |
| `python -m backend.jobs.live_sync_probable_matches` | ACTIVO (horario) | sin API (PG) | — | `distribuidora.document_probable_matches` |
| `python -m backend.jobs.sync_bsale_distribuidora` → `distribuidora/sync_service.py` | ACTIVO (Coolify / API) | 3 | `documents` (`emissiondaterange` **y `generationdaterange`**), details, references, attributes | `distribuidora.documents`, `document_details`, `document_references`, `document_sellers`, `document_attributes`, `sync_logs`, `sync_status`, `sync_process_cursor` |
| `python -m backend.jobs.sync_distribuidora_related` | ACTIVO (primer job oficial Coolify) | 3 | `documents?relateddetailid=` | `distribuidora.document_related` |
| `python -m backend.jobs.reconcile_open_purchase_orders` | ACTIVO? (Scheduled Task) | 3 | `documents/{id}`, details, references, attributes | `distribuidora.documents`, `document_details`, `document_references`, `document_attributes`, `distribuidora.dispatch_plan` |
| `backend/jobs/reconcile_bsale_ocs.py`, `diagnose_oc_bsale_vs_pg.py`, `diagnose_oc_header_drift.py`, `diagnose_oc_operational_status_45d.py` | DEBUG / MANUAL | 3 | `documents` | lectura (o vía servicios) |
| `backend/jobs/sync_document_relations.py`, `catchup_oc_invoice_relations.py`, `sync_missing_related_documents.py` | MANUAL (dry-run por defecto) | 3 | `documents` | `distribuidora.document_related`, `distribuidora.documents` |
| `backend/jobs/repair_oc_header_from_bsale.py`, `repair_oc_missing_details_folios.py`, `repair_oc_68199_details_from_bsale_source.py` | MANUAL (one-off) | 3 | `documents`, details | `distribuidora.*` |
| `backend/jobs/backfill_documents_may_2026.py`, `backfill_details_may_2026.py`, `backfill_related_may_2026.py`, `build_probable_invoice_matches_may_2026.py` | MANUAL (one-off mayo 2026) | 3 | `documents`, details | `distribuidora.*` |
| `sync_documents.py` (raíz) | ACTIVO? / histórico (rango por env) | `bsale.companies` | `documents` | `{PG_DOCUMENTS_SCHEMA:-bsale}.documents` |
| `sync_document_details.py` (raíz) | ACTIVO? / histórico | `bsale.companies` | `documents/{id}/details` | `{PG_DOCUMENTS_SCHEMA:-bsale}.document_details` |

## 3. Costos por recepción y devoluciones (schemas `analytics` y `bsale`)

| Script / módulo | Estado | Endpoints Bsale | Tablas que escribe |
|---|---|---|---|
| `python -m backend.jobs.sync_cost_receptions` → `backend/services/sync_cost_receptions.py` | MANUAL (piloto dry-run/apply) | `stocks/receptions`, details, `variants/{id}/costs` | `analytics.cost_reception_history`, `analytics.cost_sync_state`, `analytics.cost_watchlist`, `bsale.variant_cost` (modo legacy) |
| `backend/jobs/sync_cost_analytics.py` | LEGACY (redirige a `sync_cost_receptions`) | — | — |
| `python -m backend.jobs.sync_cost_reception_calculated_v2` | MANUAL / incremental programable | sin API (PG) | `analytics.cost_reception_calculated` |
| `backend/jobs/backfill_cost_reception_calculated.py` | MANUAL | sin API | `analytics.cost_reception_calculated` |
| `python -m backend.jobs.sync_bsale_returns_incremental` → `backend/services/sync_bsale_returns.py` | ACTIVO (cron) | `returns`, details | `bsale.returns`, `bsale.return_details`, `bsale.returns_sync`, `bsale.returns_sync_state` |
| `python -m backend.jobs.sync_bsale_returns_history` | MANUAL (bootstrap) | `returns` | ídem |
| `backend/jobs/sync_bsale_returns.py` | LEGACY (redirige al incremental) | — | — |

## 4. Debug / exploración (no productivos)

`ultimaoc.py`, `pruebadatos.py`, `docs_prueba_bsale.py`, `explore_bsale.py` (vacío), `backend/debug/*` (`debug_single_document`, `debug_document_types`, `export_bsale_documents_test`, `test_bsale_documents_office_1` → `app.*_bc_test`, `analyze_purchase_orders_relationships`, `export_oc_bs_only`, …), shims `backend/jobs/debug_*`, `export_*`, `analyze_related_patterns`.

## 5. Clientes HTTP Bsale existentes

| Cliente | Usado por | Características |
|---|---|---|
| `backend/services/bsale/http_client.py` `BsaleHttpClient` | catálogo, precios, stock (endurecido) | Session, timeouts, retry 408/425/429/5xx, Retry-After, validación de host, paginación estricta. **Reutilizado por `bsale_raw`.** |
| `backend/services/distribuidora/bsale_client.py` `BsaleClient` | Distribuidora (documentos) | Throttle entre requests, manejo 429 con Retry-After. Sólo empresa 3. |
| `requests` directo | scripts raíz legacy (`sync_clients`, `sync_meta_bs`, `sync_documents`, `sync_document_details`, `sync_cost_history`, debug) | Sin rate limit centralizado. |

## 6. Observaciones relevantes para `bsale_raw`

1. **Tokens:** la convención correcta (`bsale.companies.bsale_token` = nombre de env → `BSALE_TOKEN_Mini` / `BSALE_TOKEN_Romero` / `BSALE_TOKEN_SPA`) sólo la usan los syncs de catálogo y los scripts raíz. Distribuidora usa `BSALE_TOKEN` o `BSALE_TOKEN_SPA` (empresa 3 implícita).
2. **Presupuesto compartido:** varios crons de empresa 3 (cada 5, 15 y 20 min) comparten token y límite (3.000 req / 300 s). `bsale_raw` arranca con 50 % del límite por empresa.
3. **`generationdaterange`** en `/documents.json` lo usa `sync_service.py`, pero la documentación oficial sólo lo describe para `/documents/summary.json` → verificar en vivo.
4. **Duplicidad de documentos:** existen `bsale.documents` (scripts raíz) y `distribuidora.documents` (live sync). `bsale_raw.documents` será la fuente única futura; la migración de consumidores es una fase posterior.
5. **Ningún sync actual guarda el payload JSON completo**; todos proyectan columnas. Es la principal brecha que cubre `bsale_raw`.

---

### Nota: `state` y variantes referenciadas que faltan localmente

Los syncs actuales (commit `ac6ec44`) **no** guardan `state` de productos/variantes ni hidratan variantes referenciadas por stock/precios que no vinieron en el listado (caso 10203 emp. 1; 31300/31301 emp. 3). `bsale_raw` lo resuelve por diseño: barridos explícitos `state=0` y `state=1` + refresco puntual `/v1/variants/{id}.json`. Documentación oficial: `state` **0 = activo, 1 = inactivo**.
