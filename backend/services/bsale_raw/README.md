# backend/services/bsale_raw

Capa espejo fiel de la API Bsale: **Bsale API → `bsale_raw` → `bsale` → products_master / ERP**.

Documentación:

- Arquitectura: [`docs/BSALE_RAW_ARCHITECTURE.md`](../../../docs/BSALE_RAW_ARCHITECTURE.md)
- Matriz de endpoints (oficial): [`docs/BSALE_RAW_ENDPOINT_MATRIX.md`](../../../docs/BSALE_RAW_ENDPOINT_MATRIX.md)
- Inventario de syncs existentes: [`docs/BSALE_SYNC_INDEX.md`](../../../docs/BSALE_SYNC_INDEX.md)

## Estado: fase 4D1 (configuración + catálogo + stock por sucursal, manual)

Motor genérico en `core/` + entrypoint `python -m backend.jobs.bsale_raw`. Sin jobs programados ni endpoint de webhooks.
Un recurso se habilita sólo con su `ResourceSpec` (`typed_columns` + `pipeline_enabled=True` + `pipeline_modes`); no hay código por recurso.

| Recurso | Estado |
|---|---|
| `offices` | IMPLEMENTED + LIVE VALIDATED C3 |
| `taxes` | IMPLEMENTED + LIVE VALIDATED C3 |
| `document_types` | IMPLEMENTED + LIVE VALIDATED C3 (sólo metadata, sin lógica OC 33) |
| `product_types` | IMPLEMENTED + LIVE VALIDATED C3 |
| `price_lists` | IMPLEMENTED + LIVE VALIDATED C3 (sólo metadata, sin `variant_prices`) |
| `products` | IMPLEMENTED + LIVE VALIDATED C3 |
| `variants` | IMPLEMENTED + LIVE VALIDATED C3 (sin stock, precios ni costos) |
| `stocks` | IMPLEMENTED / NOT YET LIVE VALIDATED (por sucursal: `--office`, modos `scanner` / `full-reconcile`) |

Entidades: un barrido sin `state` ni `expand` (sólo `limit`/`offset`); devuelve activos e inactivos.
En `variants`, SKU (`code`) y barcode (`bar_code`) se guardan tal cual (sin unicidad ni deduplicación) y
`product_id` es la relación que entrega Bsale, sin resolverla ni repararla. Orden operativo: `products` antes que `variants`.

### Stock (`core/stock_engine.py`)

- Identidad `(company_id, variant_id, office_id)`; `id` Bsale → `bsale_stock_id` (sin unicidad). Scope `office:<id>`;
  advisory lock `(company, stocks, office:<id>)`: sucursales independientes; un refresh dirigido no toma ese lock.
- `quantity`, `quantityReserved` y `quantityAvailable` se guardan **tal cual** (las OC 33 reservan antes de la salida física):
  nunca se recalcula `available`, un `0` explícito queda 0 y un valor ausente queda NULL. **Nunca se fabrica stock 0.**
- Frescura por fila: `api_fetched_at` = respuesta de la página; el UPSERT sólo aplica si
  `t.api_fetched_at <= EXCLUDED.api_fetched_at` (un scanner lento nunca pisa un webhook / refresh dirigido → `skipped_newer`).
- `SCANNER` (frecuente, NO destructivo): sólo UPSERT; lo que no vino no se toca. Tolera que `count` varíe hasta
  `max(10, 1 %)` durante el barrido (endpoint mutable); falla con drift mayor, truncado evidente, clave repetida,
  sucursal distinta a la pedida, `variant`/`office` inválidos o cantidades no numéricas.
- `FULL_RECONCILE` (menos frecuente): snapshot **estricto** (count estable, total exacto) + fusible 20 %; borra sólo filas de
  ESA sucursal ausentes y con `api_fetched_at <= snapshot_started_at`. Es el único modo que borra.
- Lectura de existentes sin `FOR UPDATE`; UPSERT por lotes de 500; una sola transacción corta después del HTTP.
- Idempotencia: a diferencia del catálogo, `updated > 0` en RUN 2 es correcto si hubo ventas/reservas entre corridas;
  lo que no debe aparecer es `inserted` para claves ya existentes, ni borrados en scanner.
- Futuro (no implementado): refresh dirigido `variantid` / `variantid+officeid` (P0) con el mismo UPSERT, disparado por
  webhook `stock` o por OC 33 tras el COMMIT del documento.

| Módulo | Contenido |
|---|---|
| `core/engine.py` | `run_entity_sync`: fuente → lock → run RUNNING → fetch completo → transacción corta (fusible, UPSERT con frescura, `missing_since`, run/state) → unlock. |
| `core/stock_engine.py` | `run_stock_sync`: mismo flujo por `office:<id>`; scanner no destructivo / reconcile estricto con DELETE stale acotado. |
| `core/snapshot.py` | `fetch_snapshot` (paginación contra `count`, estricta o con `CountDrift`), `build_rows`, `build_stock_rows`. |
| `core/reconcile.py` | `plan_reconcile`: conteos, faltantes elegibles (`api_fetched_at <= snapshot_started_at`), fusible 20 %. |
| `core/store.py` | `PgRawStore` (toda la SQL), advisory lock `(int, int)`, `sync_runs` / `sync_entity_runs` / `sync_state`. |
| `core/rate_limit.py` | `TokenBucket` por empresa, `CompanyRateLimiters`, `RateLimitedSession` (cuenta requests, 429 y 5xx, reintentos incluidos). |
| `core/client.py` | `build_company_client`: reutiliza `backend.services.bsale.http_client.BsaleHttpClient` (timeouts, retry 408/425/429/5xx, Retry-After, validación de host) con la sesión limitada. |
| `core/models.py` | `RawRecord`, `StockRecord`, `payload_hash`, enums de modo, estado de corrida y estado de webhook. |
| `core/registry.py` | `ResourceSpec` + `REGISTRY` (un motor, N recursos declarativos). |
| `core/freshness.py` | `FreshnessState` y evaluación contra SLA. |
| `resources/*.py` | Declaración de recursos según la matriz. |
| `webhooks/__init__.py` | `parse_webhook` (cpnId → company_id) y `route` (refrescos puntuales). |

## Reglas

- Identidad siempre `(company_id, bsale_id)`; ningún id Bsale es global.
- Sin reglas de negocio: si Bsale dice variante X → producto Y, se guarda así.
- SKU y código de barras se guardan como columnas de búsqueda, nunca como PK.
- Tokens sólo en variables de entorno; nunca en PostgreSQL ni en logs.
- No modifica `backend/services/bsale/*` ni los syncs legacy.

Tests: `python -m pytest backend/tests/bsale_raw -q`.
