# backend/services/bsale_raw

Capa espejo fiel de la API Bsale: **Bsale API → `bsale_raw` → `bsale` → products_master / ERP**.

Documentación:

- Arquitectura: [`docs/BSALE_RAW_ARCHITECTURE.md`](../../../docs/BSALE_RAW_ARCHITECTURE.md)
- Matriz de endpoints (oficial): [`docs/BSALE_RAW_ENDPOINT_MATRIX.md`](../../../docs/BSALE_RAW_ENDPOINT_MATRIX.md)
- Inventario de syncs existentes: [`docs/BSALE_SYNC_INDEX.md`](../../../docs/BSALE_SYNC_INDEX.md)

## Estado: fase 4E1 (configuración + catálogo + stock + OC 33 POINT, manual)

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
| `stocks` SCANNER | IMPLEMENTED + LIVE VALIDATED C3 office 1 / office 4 (5.860 filas, 118 requests, ~41 s) |
| `stocks` FULL_RECONCILE | IMPLEMENTED / NOT YET LIVE VALIDATED |
| `stocks` POINT | IMPLEMENTED + LIVE VALIDATED C3 (`--variant` [+ `--office`]; 10888 + office 1 y 10888 sin office) |
| `documents` OC 33 POINT | IMPLEMENTED / NOT YET LIVE VALIDATED (`--document <id técnico>`; stock post-COMMIT) |

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
- `POINT` (refresh dirigido, `refresh_stock_point` / `refresh_stock_variants`): un request
  `stocks.json?variantid=V[&officeid=O]` por variante (Bsale no documenta listas de ids), prioridad **P0** en el mismo
  limitador de la empresa, mismo UPSERT con frescura sobre las mismas filas (`last_source = 'POINT'`).
  - `variant + office`: 1 fila → UPSERT; 0 filas → `NO_ROWS` sin escritura (no se fabrica 0 ni se borra); >1 fila → FAILED (ambigua).
  - `variant` sola: UPSERT de todas las sucursales devueltas; las que no vinieron no se tocan.
  - Sin advisory lock: un scanner de ~41 s nunca bloquea un POINT; la frescura por fila decide (un scanner con datos
    anteriores al POINT termina en `skipped_newer`). Un POINT con datos viejos tampoco pisa una fila más nueva.
  - Un `sync_runs` + `sync_entity_runs` por llamada (scope `variant:<v>[:office:<o>]` o `variants:<n>[:office:<o>]`),
    detalle por variante en `summary.point`. `sync_state` usa UNA fila agregada `(company, stocks, point)`, no una por variante.
  - Lote: PARTIAL si fallan algunas variantes, FAILED si fallan todas; máximo 500 variantes por llamada.
- Consumidor: OC 33 POINT (abajo) → `affected_variants` → COMMIT → `refresh_stock_variants(...)`. Webhook `stock`: no implementado.

### Documentos OC 33 POINT (`core/document_engine.py`, fase 4E1)

- `refresh_document_point(company_id, document_id)`: refresca UNA OC por su **id técnico Bsale** (no folio / `number`;
  no hay búsqueda por folio). Sólo `--mode point`; sin scanner, sin full scan, sin `generationdaterange`, sin watcher.
- Endpoints: `/v1/documents/{id}.json` (header, sin `expand`), `/v1/documents/{id}/details.json` paginado completo
  (fuente de integridad), `/references.json`, `/sellers.json` y `/attributes.json` (paginados). `attributes.json` está
  LIVE VERIFIED (C3, documento 3925780, `count=4`); sin tabla propia: `attributes_payload` = `{"count", "items"}` con los
  ítems tal cual (`value` sin normalizar). Los links hijos del header deben apuntar a
  `https://api.bsale.io` con el path exacto (sin query); si no, FAILED.
- Guard de tipo: `document_type.id` debe ser 33 (por id, nunca por nombre); si no, FAILED sin escritura.
- Fetch completo antes de escribir (nunca HTTP dentro de una transacción). Si falla cualquier hijo → FAILED y la versión
  anterior queda intacta. `details_complete = true` sólo con details completo y validado; `variant_id` NULL se conserva
  como línea pero no entra en `affected`.
- **Versión:** `payload_hash` = header; hash por parte (details / references / sellers / attributes completos, ordenados por id);
  `version_hash` = hash de los 5 hashes de parte; `children_hash` = hash de las partes sin header. Cada hijo lleva
  `document_version_hash = documents.version_hash`. Determinista e independiente del orden.
- **Transacción atómica corta:** `pg_advisory_xact_lock(company, documents, document:<id>)` + `SELECT … FOR UPDATE` →
  lectura de la versión previa (hashes, variantes, pendientes) → frescura → reemplazo COMPLETO de cada conjunto hijo
  (DELETE de los que ya no vienen + UPSERT) → UPSERT del header → `document_change_log` → COMMIT. ROLLBACK total ante error.
- **Frescura:** si la fila guardada tiene `api_fetched_at` más nuevo que el bundle → sin escrituras, `skipped_newer = 1`, sin stock.
- **Change log:** `CREATED` (primera vez) / `MODIFIED` (cambia `version_hash`) con booleanos reales por componente,
  `previous/current/affected_variant_ids` y `stock_refresh_requested_at`; versión igual → sin fila.
- **Stock post-COMMIT:** `affected = previous ∪ current` → `refresh_stock_variants(company, office_id de la OC, affected)`
  (P0, mismo limitador). Sin `office_id` en la OC, o si la sucursal cambió, se refrescan todas las sucursales (nunca se
  inventa la office 1). Un fallo de stock **no** revierte la OC: la fila queda con `requested` y sin `done`, run PARTIAL.
- **Pendientes:** filas del log con `stock_refresh_requested_at` NOT NULL y `done` NULL se reintentan en el siguiente
  refresh aunque la versión no cambie, sin fila MODIFIED falsa.
- Sin reglas de estados terminales: se guardan `state`, `commercial_state`, references y payload; nunca DELETE de la OC;
  `watch_*` quedan NULL.
- Tracking: modo POINT, scope `document:<id>`, `sync_state` agregado `(company, documents, point)`, summary sin PII,
  token ni URLs.

| Módulo | Contenido |
|---|---|
| `core/engine.py` | `run_entity_sync`: fuente → lock → run RUNNING → fetch completo → transacción corta (fusible, UPSERT con frescura, `missing_since`, run/state) → unlock. |
| `core/stock_engine.py` | `run_stock_sync`: mismo flujo por `office:<id>`; scanner no destructivo / reconcile estricto con DELETE stale acotado. `refresh_stock_point` / `refresh_stock_variants`: POINT P0 por variante, sin lock, sin borrado. |
| `core/document_engine.py` | `refresh_document_point`: OC 33 POINT (fetch bundle → tx atómica → COMMIT → stock POINT). |
| `core/document_bundle.py` | Validación del bundle (header, hijos, hrefs), `StoredDocument`, `plan_document` (frescura, change kind, affected, pendientes). |
| `core/document_version.py` | `DocumentVersion` (hashes por parte, `version_hash`), `changed_parts`, `affected_variants`. |
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
