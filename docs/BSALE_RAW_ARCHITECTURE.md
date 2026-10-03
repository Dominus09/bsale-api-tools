# BSALE_RAW — Arquitectura

**Estado:**
- fase 1 (arquitectura, inventario, scaffold) y fase 2 (verificación en vivo READ-ONLY) **aprobadas**;
- fase 3 en propuesta (SQL **no ejecutado**);
- sin migraciones aplicadas, sin jobs ni deploy; el sistema actual sigue intacto.

Documentos hermanos:
- [`BSALE_RAW_ENDPOINT_MATRIX.md`](./BSALE_RAW_ENDPOINT_MATRIX.md): fuente oficial por endpoint;
- [`BSALE_RAW_LIVE_VERIFICATION.md`](./BSALE_RAW_LIVE_VERIFICATION.md): resultados de la fase 2;
- [`BSALE_RAW_PHASE3_SQL_PROPOSAL.md`](./BSALE_RAW_PHASE3_SQL_PROPOSAL.md): DDL propuesto;
- [`BSALE_SYNC_INDEX.md`](./BSALE_SYNC_INDEX.md): syncs existentes.

---

## 1. Objetivo y capas

```
Bsale API (3 empresas) ──► bsale_raw (espejo fiel, JSON completo) ──► bsale (modelo ERP) ──► products_master / ERP
                 ▲
       webhooks ─┘ (inbox desacoplado)
```

| Capa | Responsabilidad | NO hace |
|---|---|---|
| `bsale_raw` | Guardar exactamente lo que Bsale entrega, por empresa, con frescura y trazabilidad de corridas. | Reglas de negocio, correcciones, deduplicación por SKU/barcode, joins entre empresas. |
| `bsale` | Proyección tipada para el ERP (columnas, estados, mappings). Se alimentará desde `bsale_raw` en una fase posterior. | Llamar a la API. |
| ERP | products_master, planificación, despacho, analytics. | — |

### Empresas

| company_id | Empresa | Variable de entorno del token | cpnId (OBSERVED fase 2) |
|---|---|---|---|
| 1 | Minimarkets La Quillotana | `BSALE_TOKEN_Mini` | 96674 |
| 2 | Carlos Romero | `BSALE_TOKEN_Romero` | 5807 |
| 3 | La Quillotana SPA | `BSALE_TOKEN_SPA` | 21884 |

`bsale.companies.bsale_token` guarda sólo el **nombre** de la variable. Los secretos nunca se guardan en PostgreSQL ni se loguean (se reutiliza `backend/services/bsale/companies.py`, que ya valida esto de forma estricta).

### Principios

1. **Identidad por empresa:** `(company_id, bsale_id)` en todas las entidades. Ningún `variant_id`, `product_id`, `document_id` u `office_id` es global.
2. **Espejo fiel:** `payload JSONB` completo + `payload_hash`. Si Bsale dice variante X → producto Y, se guarda así aunque parezca inconsistente.
3. **SKU / barcode:** columnas de búsqueda, nunca PK ni clave única.
4. **Sin CHECKs rígidos** sobre valores externos (estados, tipos, códigos SII): Bsale puede introducir valores nuevos.
5. **Sin FKs entre tablas raw:** un hijo (detalle, stock) puede llegar antes que su padre. La integridad se mide, no se impone.
6. **Fail-safe:** ante cualquier error se conserva el último snapshot válido.
7. **Un motor + un registry:** recursos declarados como datos (`ResourceSpec`), nunca un script por endpoint.

---

## 2. Componentes (scaffold creado)

```
backend/services/bsale_raw/
  core/
    client.py       build_company_client(): BsaleHttpClient existente + RateLimitedSession por empresa
    rate_limit.py   RequestPriority (P0–P6), TokenBucket, PriorityRateLimiter, CompanyRateLimiters, RateLimitedSession
    models.py       RawRecord, StockRecord, payload_hash, SyncMode, RunStatus, WebhookStatus
    registry.py     ResourceSpec (request_priority, partition_by_office, full_scan_global_allowed, …), REGISTRY
    freshness.py    FreshnessState(scope).evaluate(sla); scopes canónicos en registry.py
  resources/        configuration, catalog, pricing, inventory, documents (declaraciones)
  webhooks/         parse_webhook (cpnId → company_id), classify_exact_response (envelope V2),
                    route → RESOURCE_EXACT → CANONICAL_V1 → DERIVED
backend/jobs/bsale_raw/   (vacío; entrypoints futuros por modo)
backend/sql/bsale_raw/    (vacío; reglas para migraciones futuras)
backend/tests/bsale_raw/  tests puros del scaffold
```

### Cliente y rate limiting

- Se reutiliza `BsaleHttpClient` sin modificarlo: `requests.Session`, timeouts connect/read, retry **sólo** 408/425/429/5xx y errores de red, backoff exponencial con jitter, `Retry-After` (header o body), validación de host (no envía el token a otro host), paginación estricta, token fuera de `repr` y logs.
- `RateLimitedSession` consume un token del limitador de la empresa **en cada request, incluidos los reintentos**, y captura headers de rate limit si Bsale los envía. En la fase 2 no se observó ninguno: el limiter local conservador y el manejo de 429 / `Retry-After` son la única protección.
- **Prioridad central por empresa / token.** Hay **un** `PriorityRateLimiter` por empresa, compartido por todos los consumidores (webhooks, escáneres, reconciles, refresh desde el ERP). Cuando hay un token disponible, se entrega al waiter de mayor prioridad (FIFO dentro del mismo nivel):

  | Prioridad | Consumidor |
  |---|---|
  | P0 | webhook / refresh puntual |
  | P1 | OC 33 |
  | P2 | stock |
  | P3 | precios |
  | P4 | catálogo |
  | P5 | costos / recepciones / consumos |
  | P6 | clientes / configuración |

  Los procesos de distintas máquinas no comparten el limiter en memoria. Por eso en la fase 4 corre **un único worker por empresa** (lock en `sync_state` / advisory lock), y el budget se reparte con los syncs legacy usando RPS conservador.
- Presupuesto por defecto: **5 req/s por empresa** (50 % del límite oficial de 3.000 req / 300 s), burst 10. Configurable con `BSALE_RAW_RPS_<company_id>`, `BSALE_RAW_RPS` y `BSALE_RAW_BURST`; se rechaza cualquier valor por encima del límite documentado.
- Concurrencia: hasta `BSALE_RAW_MAX_CONCURRENT_COMPANIES` (default 3) empresas en paralelo, cada una con su limitador. Dentro de una empresa las requests son **secuenciales**, sin paralelismo descontrolado.
- `credential.bsale.io` (necesario para obtener el `cpnId`) es otro host: se usará con un cliente aparte y allow-list explícita, nunca relajando la validación de host de `api.bsale.io`.

---

## 3. Tablas propuestas (conceptual)

El DDL completo (PK, FK, índices, staging) está en [`BSALE_RAW_PHASE3_SQL_PROPOSAL.md`](./BSALE_RAW_PHASE3_SQL_PROPOSAL.md) y no se ha ejecutado. Si hay diferencias con esta sección, manda la propuesta SQL.

### 3.1 Columnas comunes de entidades "normales"

| Columna | Tipo | Nota |
|---|---|---|
| `company_id` | smallint | parte de la PK; FK sólo a `bsale.companies` |
| `bsale_id` | bigint | `id` de Bsale; parte de la PK |
| columnas de búsqueda | según recurso | derivadas del payload, sin transformación de negocio |
| `state` | smallint NULL | donde aplique (0 activo / 1 inactivo según docs) |
| `payload` | jsonb | respuesta completa del ítem |
| `payload_hash` | text | SHA-256 del JSON canónico |
| `first_seen_at` | timestamptz | primera vez visto |
| `last_seen_at` | timestamptz | última vez que Bsale lo devolvió |
| `last_changed_at` | timestamptz | última vez que cambió `payload_hash` |
| `api_fetched_at` | timestamptz | momento de la respuesta HTTP |
| `sync_run_id` | bigint | corrida que lo escribió por última vez |

PK: `(company_id, bsale_id)`. Índices de búsqueda por empresa: `(company_id, <col>)`.

### 3.2 Tablas por recurso

| Tabla | Clave | Columnas de búsqueda propuestas | Endpoint origen |
|---|---|---|---|
| `bsale_raw.sources` | `company_id` | `name`, `token_env` (nombre, no secreto), `bsale_cpn_id`, `rut`, `active` | `bsale.companies` + `credential.bsale.io` |
| `bsale_raw.products` | (company_id, bsale_id) | `name`, `product_type_id`, `classification`, `state` | `/products` |
| `bsale_raw.variants` | (company_id, bsale_id) | `product_id`, `code` (SKU), `bar_code`, `description`, `state` | `/variants` |
| `bsale_raw.product_types` | (company_id, bsale_id) | `name`, `state` | `/product_types` |
| `bsale_raw.offices` | (company_id, bsale_id) | `name`, `state` | `/offices` |
| `bsale_raw.taxes` | (company_id, bsale_id) | `name`, `code`, `percentage`, `state` | `/taxes` |
| `bsale_raw.document_types` | (company_id, bsale_id) | `name`, `code_sii`, `state` | `/document_types` |
| `bsale_raw.price_lists` | (company_id, bsale_id) | `name`, `coin_id`, `state` | `/price_lists` |
| `bsale_raw.variant_prices` | (company_id, price_list_id, variant_id) | `detail_id` (id Bsale del detalle), `variant_value`, `variant_value_with_taxes` | `/price_lists/{id}/details` |
| `bsale_raw.variant_costs` | (company_id, variant_id) | `average_cost`, `total_cost`, `history_count`, `last_admission_date` (payload con `history` tal cual; **no** se declara histórico completo) | `/variants/{id}/costs` |
| `bsale_raw.stocks` | ver 3.3 | | `/stocks` |
| `bsale_raw.clients` | (company_id, bsale_id) | `code` (RUT), `first_name`, `last_name`, `company`, `email`, `state` | `/clients` |
| `bsale_raw.documents` | (company_id, bsale_id) | `document_type_id`, `office_id`, `client_id`, `user_id`, `number`, `emission_date`, `generation_date`, `total_amount`, `state`, `informed_sii` | `/documents` |
| `bsale_raw.document_details` | (company_id, document_id, bsale_id) | `document_id`, `variant_id`, `line_number`, `quantity`, `related_detail_id` | `/documents/{id}/details` |
| `bsale_raw.document_references` | (company_id, document_id, bsale_id) | `document_id`, `number`, `dte_code_id`, `reference_date` | `/documents/{id}/references` |
| `bsale_raw.document_sellers` | (company_id, document_id, user_id) | — | `/documents/{id}/sellers` |
| `bsale_raw.stock_receptions` | (company_id, bsale_id) | `office_id`, `admission_date`, `document_number` | `/stocks/receptions` |
| `bsale_raw.stock_reception_details` | (company_id, reception_id, bsale_id) | `reception_id`, `variant_id`, `quantity`, `cost` | `/stocks/receptions/{id}/details` |
| `bsale_raw.stock_consumptions` | (company_id, bsale_id) | `office_id`, `consumption_date` | `/stocks/consumptions` |
| `bsale_raw.stock_consumption_details` | (company_id, consumption_id, bsale_id) | `consumption_id`, `variant_id`, `quantity` | `/stocks/consumptions/{id}/details` |
| `bsale_raw.document_change_log` | `id` bigserial | `company_id`, `document_id`, `detected_by`, `sync_run_id`, `webhook_event_id`, hashes / estados previo y actual, partes cambiadas, `affected_variant_ids` (auditoría de versiones OC, sin payload) | refresh de documentos |
| `bsale_raw.webhook_events` | `id` bigserial | ver sección 5 | webhooks |
| `bsale_raw.webhook_resource_responses` | `id` bigserial | `webhook_event_id`, `http_status`, `envelope`, `body` (evidencia del GET exacto V2 / sin versión; nunca alimenta tablas operativas) | `resource` del webhook |
| `bsale_raw.sync_runs` | `id` | `mode`, `trigger`, `status`, `started_at`, `finished_at`, `summary jsonb` | — |
| `bsale_raw.sync_entity_runs` | `id` | `sync_run_id`, `company_id`, `resource`, `status`, `rows_received`, `rows_upserted`, `rows_unchanged`, `rows_stale`, `requests`, `duration_ms`, `error` | — |
| `bsale_raw.sync_state` | (company_id, resource, scope) | frescura, ver sección 6; `scope` = `global` / `office:<id>` / `price_list:<id>` / `document_type:<id>` (TEXT sin CHECK; convención en `registry.py`) | — |
| `bsale_raw.sync_cursors` | (company_id, resource, scope, cursor_name) | `cursor_value jsonb`, `updated_at` (ventanas incrementales, checkpoint del escáner por empresa + sucursal) | — |

**Tabla extra justificada por endpoint oficial (opcional):** `bsale_raw.stock_consumption_types` (`/v1/stock_consumption_types.json`), sólo si un consumidor necesita interpretar consumos.

Los hijos (`document_details`, `stock_reception_details`, …) usan su propio `id` Bsale como `bsale_id`. Que sea único por empresa sigue siendo **NLV**, porque no se probó en la fase 2. Por eso la PK de los hijos es `(company_id, parent_id, bsale_id)`: un id de hijo nunca identifica solo a su padre, y un UPSERT no puede mover silenciosamente una línea de un documento a otro. El índice no único `(company_id, bsale_id)` permite detectar si un mismo id aparece bajo dos padres (anomalía que se reporta, no se corrige sola).

### 3.3 `bsale_raw.stocks` (estado actual)

| Columna | Nota |
|---|---|
| `company_id`, `variant_id`, `office_id` | **clave única operativa** `(company_id, variant_id, office_id)` |
| `bsale_stock_id` | `id` Bsale del registro, si viene (índice, sin UNIQUE) |
| `quantity`, `quantity_reserved`, `quantity_available` | numéricos tal como llegan |
| `payload`, `payload_hash` | |
| `api_fetched_at`, `last_seen_at`, `last_changed_at`, `sync_run_id` | |
| `last_source` | `WEBHOOK` / `POINT` / `SCANNER` / `FULL_RECONCILE` (trazabilidad de la última confirmación) |

Sin historia por poll. `stock_daily_snapshot` queda para una fase posterior.

---

## 4. Flujo fail-safe (por empresa y recurso)

```
fetch completo (memoria/staging) ─► validar ─► deduplicar ─► fusible/reconcile checks
      │ error                                         │ falla
      ▼                                               ▼
  FAILED (sin escribir)                    FAILED (sin escribir)
                                                      │ ok
                                                      ▼
                      BEGIN ─► UPSERT por payload_hash ─► reconcile (marcar no vistos) ─► COMMIT
                                     │ error ─► ROLLBACK (queda el snapshot anterior)
```

- **Validación:** respuesta con `items` lista; ids numéricos; para stock, `variant.id` y `office.id` presentes; `count` coherente con los ítems recibidos.
- **Deduplicación:** por clave; si dos ítems con la misma clave traen payload distinto en la misma corrida, la corrida falla (no se elige uno arbitrariamente).
- **UPSERT:** sólo cambia `payload`, `last_changed_at` y las columnas de búsqueda si cambió `payload_hash`; siempre actualiza `last_seen_at`, `api_fetched_at` y `sync_run_id`.
- **Frescura / concurrencia:** un snapshot viejo **nunca** sobrescribe una fila obtenida después: `ON CONFLICT … DO UPDATE … WHERE target.api_fetched_at <= EXCLUDED.api_fetched_at`.
  - Un DELETE stale exige tres condiciones: que la fila no esté en staging, que pertenezca al mismo company / resource / scope y que `target.api_fetched_at <= snapshot_started_at`, capturado antes del primer GET.
  - Así, una fila refrescada por webhook o targeted refresh durante el snapshot no se sobrescribe ni se borra.
  - Detalle en `BSALE_RAW_PHASE3_SQL_PROPOSAL.md` §3.
- **Reconcile (full):**
  - En entidades, los registros no vistos **no se borran** (Bsale no borra productos ni variantes, los desactiva): se marca `missing_since` y se reporta.
  - En `stocks` (por empresa + **sucursal**) y `variant_prices` (por empresa + lista), que son estado actual, el **reconcile destructivo** elimina los no vistos sólo si pasa el fusible de % de stale (patrón `snapshot_reconcile.py`): un snapshot vacío con filas existentes es fallo, y un % stale sobre el umbral es fallo, antes de escribir.
  - El escáner frecuente **nunca** borra.
- **Catálogo y clientes:** el full scan es **un** barrido sin `state` (OBSERVED: devuelve activos e inactivos) y se guarda el `state` de cada ítem. Los barridos `state=0/1` son sólo de auditoría.
- **Empresas independientes:** cada empresa tiene su propia transacción. Resultado de la corrida: `SUCCESS` / `PARTIAL` / `FAILED` / `SKIPPED` (lock).
- **Hijos de documentos:** documento + details + references + sellers se escriben en la **misma transacción**; si falla un hijo, no se escribe el documento.

---

## 5. Inbox de webhooks `bsale_raw.webhook_events`

**Recepción** (endpoint futuro, debe responder rápido):

1. Validar el JSON y los campos obligatorios (`cpnId`, `topic`, `action`, `resourceId`); el topic debe ser uno de los documentados.
2. Resolver `company_id` desde `cpnId` con `bsale_raw.sources.cpn_id`. Si el `cpnId` es desconocido, se persiste como `FAILED_FINAL` (evidencia).
3. Persistir el payload original y responder 2xx. **No** llamar a Bsale dentro del request.

Bsale no documenta firma ni secreto para los webhooks. Por eso el contenido del webhook nunca se toma como dato: sólo dispara una reconsulta a la API con el token de la empresa.

**Columnas:**
- `id`, `received_at`, `company_id`, `cpn_id`, `topic`, `action`, `resource`, `resource_id`, `office_id`, `price_list_id`, `sent_at`, `payload jsonb`;
- `dedupe_key`: reenvío exacto, **sin unicidad** (todo POST se conserva como evidencia);
- `refresh_key` (recurso, sin action ni send): **único sólo entre eventos activos**, con un índice parcial para `PENDING`/`RETRY` y otro para `PROCESSING`;
- `status`: `PENDING` / `PROCESSING` / `DONE` / `RETRY` / `FAILED_FINAL` / `COALESCED`; `coalesced_into_id`;
- `attempts`, `next_attempt_at`, `locked_at`, `locked_by`, `validation_error`, `last_error`, `processed_at`.

**Procesamiento** (worker desacoplado):
- Toma eventos con `FOR UPDATE SKIP LOCKED`.
- **Coalescing:** un evento que llega con otro ya en cola para el mismo `refresh_key` se guarda como `COALESCED`. Muchos webhooks de stock de una misma variante y sucursal se resuelven con un solo GET.
  - Si el anterior está `PROCESSING`, el nuevo entra `PENDING`, porque el refresh en curso pudo ser previo al cambio.
  - Tras `DONE`, un evento idéntico vuelve a entrar.
- Respeta el rate limiter de la empresa.
- Usa backoff entre reintentos: tras N intentos el evento pasa a `FAILED_FINAL`.

**Idempotencia:** el procesamiento de un evento es una reconsulta más un UPSERT, por lo que repetirlo no causa daño. No se asume exactly-once ni orden de entrega.

**Ruteo** (`webhooks.route`; diseño, sin acciones productivas).

Flujo por evento (todo a **P0**):

1. **RESOURCE_EXACT:** GET de `https://api.bsale.io` + `resource`, exactamente como lo entrega Bsale.
   - La respuesta original (status, envelope, body) se guarda en `bsale_raw.webhook_resource_responses` como evidencia.
   - OBSERVED: V2 responde con el envelope `code` + `data`, distinto de la forma V1; además se vio un 503 transitorio.
   - `classify_exact_response` clasifica la respuesta como `V2_CODE_DATA` / `OTHER` / `NO_JSON`.
   - Un error aquí **no** bloquea el paso 2.
2. **CANONICAL_V1:** refresh del mismo recurso por su endpoint V1 canónico (`/v1/products/{id}.json`, `/v1/variants/{id}.json`, `/v1/price_lists/{pl}/details.json?variantid=`, `/v1/stocks.json?variantid=&officeid=`, `/v1/documents/{id}.json`). **Sólo esta respuesta** alimenta las tablas operativas RAW, que conservan así una forma consistente V1.
3. **DERIVED:**
   - costos de la variante cuando llega `variant` post;
   - detalles paginados y luego stock puntual de sus variantes cuando llega `document`.

`parse_webhook` valida el paso 1 con `validate_resource`:
- solo acepta rutas relativas con el patrón documentado del topic;
- exige coherencia con `resourceId`, `officeId` y `priceListId`;
- rechaza URLs absolutas, otros hosts, `..`, query extra y versiones no documentadas.

| topic | `resource` aceptado | Tareas adicionales |
|---|---|---|
| `product` | `/v2/products/{id}.json` | — |
| `variant` | `/v2/variants/{id}.json` | costos de la variante (`/v1/variants/{id}/costs.json`) |
| `price` | `/v2/price_lists/{pl}/details.json?variant={id}` | — |
| `stock` | `/v2/stocks.json?variant={id}&office={office}` | — |
| `document` | `/documents/{id}.json` | details paginados (+ references / sellers); luego stock puntual de cada variante del documento (crítico para OC 33 de empresa 3) |

Nunca se reescribe la ruta exacta. El V1 canónico es una tarea **separada y explícita**, no una sustitución silenciosa.

---

## 6. Frescura (`bsale_raw.sync_state`)

Una fila por `(company_id, resource, scope)`:
- `scope = 'global'` para recursos globales de la empresa;
- `scope = 'office:<id>'` para stock, que se particiona por sucursal.

Cada fila tiene:
- `last_attempt_at`, `last_success_at`, `last_webhook_at`, `last_incremental_at`, `last_full_reconcile_at`;
- `last_error_at`, `last_error`;
- `rows_received`, `duration_ms`, `status`.

**"¿Cuándo fue confirmado por última vez este dato contra Bsale?"**
- **Por recurso:** `max(last_success_at, last_webhook_at, last_incremental_at, last_full_reconcile_at)` (`FreshnessState.last_confirmed_at`), comparado con el SLA del registry: `FRESH` / `STALE` / `NEVER`.
- **Por fila:** `api_fetched_at` / `last_seen_at` de la propia fila.

**Uso desde el ERP:**
- La navegación normal lee PostgreSQL; el ERP nunca llama a Bsale directamente.
- Toda pantalla que muestre stock o precio debe exponer la frescura. Si el dato está `STALE`, no se presenta como actual.
- En operaciones críticas (p. ej. preparar o cargar una OC), el ERP puede pedir un **refresh puntual**. Ese refresh encola un `RefreshTask` (el mismo mecanismo que los webhooks) con prioridad alta y respeta el rate limiter de la empresa. Si el refresh falla, se mantiene el último valor con su frescura real.

---

## 7. Estrategias por criticidad

### Stock (crítico)

Volumen observado: C1 12.587, C2 1.264 y C3 35.160 filas, unas 704 páginas en C3 (≈ 2,5 min a 5 rps). Los filtros `variantid`, `officeid` y su combinación funcionan.

1. **Webhook `stock` / refresh puntual (P0):** `stocks.json?variantid=&officeid=`, con prioridad sobre el escáner.
2. **Documentos nuevos (P0 derivada):** refresco de las variantes de cada documento a partir de sus detalles paginados. Prioridad: OC 33 de la empresa 3.
3. **Escáner frecuente NO destructivo (P2):**
   - particionado por **empresa + sucursal**; pagina `stocks.json?officeid=X` con checkpoint en `sync_cursors` (scope `office:X`, offset) para retomar tras un corte;
   - cada página es un **UPSERT** pequeño que actualiza `last_seen_at` y los cambios; **nunca borra**;
   - al terminar una sucursal, actualiza `sync_state` (scope `office:X`) con cobertura y duración;
   - como usa el limiter con P2, cede automáticamente ante P0 y P1.
4. **Reconcile destructivo separado** (menos frecuente, p. ej. cada 6 h por sucursal):
   - snapshot completo de la sucursal en staging;
   - validación y fusible;
   - borrado de las filas de esa sucursal no vistas, en una sola transacción.

   Es el **único** modo que borra. Las filas que fueron refrescadas por P0 durante el snapshot (`api_fetched_at` ≥ inicio del snapshot) **no** se borran.
5. **Frescura por empresa + sucursal:** el SLA de 15 min se evalúa por `office:<id>`, y una sucursal atrasada no oculta el estado de las demás.

### Documentos (crítico; empresa 3 / tipo 33 primero)

- **Full scan global PROHIBIDO:** C3 tiene 3.880.542 documentos (`full_scan_global_allowed=False`; el registry exige `reconcile_window_days`).
- **`generationdaterange` PROHIBIDO:** `/v1/documents.json` responde 403. Riesgo legacy registrado en `BSALE_RAW_LIVE_VERIFICATION.md`.
- **Webhook (P0):** descarga inmediata del documento y sus hijos, UPSERT y refresco del stock de sus variantes.
- **Incremental acotado (P1):**
  - `emissiondaterange` sobre los últimos 2 días con solape (`emissionDate` no tiene zona horaria), filtrado por `documenttypeid`;
  - ventanas y checkpoint en `sync_cursors`;
  - OBSERVED: `documenttypeid=33` + `emissiondaterange` funciona (32 documentos en la ventana de prueba).
- **Reconcile por ventana:**
  - re-barrido de 45 días (OC 33) o 7 días (resto), comparando contra `count.json` por ventana;
  - detecta anulaciones (`state=1`) y modificaciones sin webhook. Bsale sólo documenta `post` para el webhook de documentos.
- **Hijos:** `expand=[details]` es **INCONCLUSIVE**, así que la fuente de integridad es siempre `/documents/{id}/details.json` paginado completo.

**Flujo OC 33:** webhook → documento → detalles paginados → variantes → stock puntual. El documento no trae stock directo (OBSERVED).

**Open document watch:** el webhook sólo garantiza la creación y `generationdaterange` está rechazado. Por eso los documentos de C3 / tipo 33 que siguen abiertos se seleccionan desde `bsale_raw.documents` y se refrescan por ID, sin tabla adicional.
- La selección usa el índice `ix_raw_documents_watch (company_id, document_type_id, api_fetched_at)`, con lookback de 45 días.
- Qué estados son terminales se decide en Python (`OPEN_DOCUMENT_WATCHES`), no en SQL.
- Cada refresh persiste header, details, references, sellers y attributes de forma atómica (ver reglas OC 33).
- El watch no se cierra al primer estado terminal: pasa por una gracia post-cierre (§ siguiente).

### Reglas críticas OC 33 (lecciones del sistema legacy) — OBLIGATORIAS

Implementadas en `006_documents.sql` y `resources/documents.py`. El detalle está en `BSALE_RAW_PHASE3_SQL_PROPOSAL.md` §4.1.

1. **Mutable, no append-only.** Una OC puede ganar o perder líneas y cambiar cantidades, descuentos, montos, vendedor, cliente, atributos o estado. También puede ser facturada o anulada y adquirir references nuevas.
2. **Refresh atómico de una sola versión.** Header, details paginados, references, sellers y attributes se escriben en **una** transacción (`BEGIN → upsert document → replace details → replace references → replace sellers → COMMIT`, con ROLLBACK completo ante cualquier fallo).
   - `documents.version_hash` cubre todas las partes.
   - Cada hijo lleva `document_version_hash NOT NULL`, igual al del documento, así que la mezcla "header nuevo + details antiguos" es detectable.
3. **Replace children.** Las líneas, references o sellers que ya no vienen se **eliminan** de la copia actual en la misma transacción. Requiere details paginado completo.
4. **Identidad de hijos.** Siempre `company_id` + `document_id` (NOT NULL) + id Bsale propio. Nunca se infiere el padre desde el id del hijo.
5. **Stock.** `affected_variants = previous_variants ∪ current_variants`, calculado antes y después del reemplazo. Tras el COMMIT se refresca el stock puntual de **todas**, nunca sólo de la versión nueva. Queda en `document_change_log.affected_variant_ids`, con estado de refresh.
6. **Descuentos, cantidades y montos.** Cualquier cambio de línea cambia `version_hash`. La capa normalizada se reconstruye desde la última versión RAW.
7. **Facturación y anulación.** La OC **nunca se elimina**: se conservan estado, payload, details, sellers y references finales. Se refrescan references y stock (en la anulación, todas las variantes antes comprometidas). Así sigue trazable hasta el documento posterior.
8. **Grace watch post-cierre.** `OpenDocumentWatch.decide()` devuelve `ACTIVE` / `GRACE` / `CLOSE`.
   - Valores por defecto: gracia de 45 min, lectura cada 10 min y al menos 3 lecturas estables antes de cerrar (`watch_closed_at`).
   - Un cambio durante la gracia reinicia el conteo.
   - Los estados terminales sólo se definen en Python.
9. **Hash y frescura.**
   - Sin cambios: `last_seen_at` / `api_fetched_at`, sin tocar `last_changed_at`.
   - Con cambios: `payload_hash` / `version_hash` / `last_changed_at` y refresh completo de hijos.
   - Además se fuerza un refresh de hijos cada 60 min, porque sellers y attributes pueden cambiar sin tocar el header.
10. **Auditoría.** `documents` = estado actual. `document_change_log` + `sync_runs` + `webhook_events` permiten reconstruir: creada → modificaciones → versión vigente → factura o anulación, con cómo y cuándo se detectó cada cambio.

### Seguridad del payload RAW

- `payload` se guarda completo y sin alterar; la protección es de acceso.
- Contenido sensible: `clients` tiene PII, y `documents` puede traer el `token` del documento, URLs PDF / XML / public view y datos del cliente.
- Nunca se expone por endpoints genéricos ni al frontend, ni se imprime completo en logs.
- Los tokens API no se guardan en SQL: `sources.token_env` es sólo el nombre de la variable.

### Costos (P5)

- Costos OBSERVED: `averageCost`, `totalCost`, `history`. `history` llega **sin metadata de paginación**, así que se guarda el JSON completo pero **no** se declara histórico completo.
- **Escáner continuo de baja prioridad:**
  - recorre todas las variantes de la empresa con checkpoint en `sync_cursors`;
  - SLA ≈ 2 h;
  - cede ante todo lo demás (P5).
- Acelerantes:
  - `variant` post → costo inmediato (P0);
  - recepciones nuevas → costos de sus variantes.

### Productos, variantes, precios, clientes, configuración

Se aplican los webhooks documentados más el full reconcile según la matriz. En catálogo y clientes es **un** barrido sin `state`.

---

## 8. Plan de fases (propuesto)

1. **Fase 1 (aprobada):** arquitectura, matriz, inventario, scaffold y tests puros.
2. **Fase 2 (aprobada):** verificación en vivo, de solo lectura, de las dudas NLV (`BSALE_RAW_LIVE_VERIFICATION.md`).
3. **Fase 3 (aplicada y verificada):** `backend/sql/bsale_raw/001…008` → `verify_bsale_raw.sql` → `009_seed_sources.sql` (ver `docs/BSALE_RAW_PHASE3_APPLY_RUNBOOK.md`).
4. **Fase 4A / 4B / 4C:** motor genérico `run_entity_sync` (FULL_RECONCILE, manual, `python -m backend.jobs.bsale_raw`) para configuración y catálogo, sin consumidores. Estado por recurso:

   | Recurso | Estado |
   |---|---|
   | `offices` | IMPLEMENTED + LIVE VALIDATED C3 (dry-run, RUN 1 / RUN 2 idempotente, paridad legacy) |
   | `taxes` | IMPLEMENTED + LIVE VALIDATED C3 |
   | `document_types` | IMPLEMENTED + LIVE VALIDATED C3 (sólo metadata; sin lógica OC 33) |
   | `product_types` | IMPLEMENTED + LIVE VALIDATED C3 |
   | `price_lists` | IMPLEMENTED + LIVE VALIDATED C3 (sólo metadata; sin `variant_prices`) |
   | `products` | IMPLEMENTED / NOT YET LIVE VALIDATED |
   | `variants` | IMPLEMENTED / NOT YET LIVE VALIDATED (sin stock, precios ni costos) |

5. **Fase 4 (siguiente):** stock (escáner y puntual), precios, costos, clientes y documentos (incremental), en paralelo a los syncs actuales para comparar paridad.
6. **Fase 5:** inbox y worker de webhooks; solicitud de activación a Bsale.
7. **Fase 6:** `bsale` pasa a leer desde `bsale_raw`; retiro gradual de los syncs legacy según `BSALE_SYNC_INDEX.md`.

**Rollback:**
- **Antes del cutover** (sin jobs productivos, sin webhooks, sin consumidores y con RAW aún no productivo), `DROP SCHEMA bsale_raw CASCADE` es aceptable.
- **Después del cutover no lo es.** En su lugar:
  1. desactivar los jobs y webhooks nuevos;
  2. devolver los consumidores a la ruta anterior;
  3. conservar `bsale_raw` para diagnóstico;
  4. corregir y reanudar.
