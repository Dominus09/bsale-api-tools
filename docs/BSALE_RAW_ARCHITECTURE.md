# BSALE_RAW — Arquitectura

**Estado:** fase 1 (arquitectura, inventario, scaffold). Sin migraciones, sin jobs, sin deploy. El sistema actual sigue intacto.

Documentos hermanos: [`BSALE_RAW_ENDPOINT_MATRIX.md`](./BSALE_RAW_ENDPOINT_MATRIX.md) (fuente oficial por endpoint) · [`BSALE_SYNC_INDEX.md`](./BSALE_SYNC_INDEX.md) (syncs existentes).

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

| company_id | Empresa | Variable de entorno del token |
|---|---|---|
| 1 | Minimarkets La Quillotana | `BSALE_TOKEN_Mini` |
| 2 | Carlos Romero | `BSALE_TOKEN_Romero` |
| 3 | La Quillotana SPA | `BSALE_TOKEN_SPA` |

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
    rate_limit.py   TokenBucket, CompanyRateLimiters, RateLimitedSession
    models.py       RawRecord, StockRecord, payload_hash, SyncMode, RunStatus, WebhookStatus
    registry.py     ResourceSpec, ResourceRegistry, REGISTRY
    freshness.py    FreshnessState.evaluate(sla)
  resources/        configuration, catalog, pricing, inventory, documents (declaraciones)
  webhooks/         parse_webhook (cpnId → company_id), route (refrescos puntuales)
backend/jobs/bsale_raw/   (vacío; entrypoints futuros por modo)
backend/sql/bsale_raw/    (vacío; reglas para migraciones futuras)
backend/tests/bsale_raw/  tests puros del scaffold
```

### Cliente y rate limiting

- Se reutiliza `BsaleHttpClient` sin modificarlo: `requests.Session`, timeouts connect/read, retry **sólo** 408/425/429/5xx y errores de red, backoff exponencial con jitter, `Retry-After` (header o body), validación de host (no envía el token a otro host), paginación estricta, token fuera de `repr` y logs.
- `RateLimitedSession` consume un token del bucket de la empresa **en cada request, incluidos los reintentos**, y captura headers de rate limit si Bsale los envía (no documentados).
- Presupuesto por defecto: **5 req/s por empresa** (50 % del límite oficial de 3.000 req / 300 s), burst 10. Configurable con `BSALE_RAW_RPS_<company_id>`, `BSALE_RAW_RPS` y `BSALE_RAW_BURST`; se rechaza cualquier valor por encima del límite documentado.
- Concurrencia: hasta `BSALE_RAW_MAX_CONCURRENT_COMPANIES` (default 3) empresas en paralelo, cada una con su limitador. Dentro de una empresa las requests son **secuenciales**, sin paralelismo descontrolado.
- `credential.bsale.io` (necesario para obtener el `cpnId`) es otro host: se usará con un cliente aparte y allow-list explícita, nunca relajando la validación de host de `api.bsale.io`.

---

## 3. Tablas propuestas (conceptual, **sin SQL**)

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
| `bsale_raw.variant_prices` | (company_id, bsale_id) | `price_list_id`, `variant_id`, `variant_value`, `variant_value_with_taxes`; único operativo `(company_id, price_list_id, variant_id)` | `/price_lists/{id}/details` |
| `bsale_raw.variant_costs` | (company_id, variant_id) | `average_cost`, `history_count`, `last_admission_date` (payload con `history` completo) | `/variants/{id}/costs` |
| `bsale_raw.stocks` | ver 3.3 | | `/stocks` |
| `bsale_raw.clients` | (company_id, bsale_id) | `code` (RUT), `first_name`, `last_name`, `company`, `email`, `state` | `/clients` |
| `bsale_raw.documents` | (company_id, bsale_id) | `document_type_id`, `office_id`, `client_id`, `user_id`, `number`, `emission_date`, `generation_date`, `total_amount`, `state`, `informed_sii` | `/documents` |
| `bsale_raw.document_details` | (company_id, bsale_id) | `document_id`, `variant_id`, `line_number`, `quantity`, `related_detail_id` | `/documents/{id}/details` |
| `bsale_raw.document_references` | (company_id, bsale_id) | `document_id`, `number`, `dte_code_id`, `reference_date` | `/documents/{id}/references` |
| `bsale_raw.document_sellers` | (company_id, document_id, user_id) | — | `/documents/{id}/sellers` |
| `bsale_raw.stock_receptions` | (company_id, bsale_id) | `office_id`, `admission_date`, `document_number` | `/stocks/receptions` |
| `bsale_raw.stock_reception_details` | (company_id, bsale_id) | `reception_id`, `variant_id`, `quantity`, `cost` | `/stocks/receptions/{id}/details` |
| `bsale_raw.stock_consumptions` | (company_id, bsale_id) | `office_id`, `consumption_date` | `/stocks/consumptions` |
| `bsale_raw.stock_consumption_details` | (company_id, bsale_id) | `consumption_id`, `variant_id`, `quantity` | `/stocks/consumptions/{id}/details` |
| `bsale_raw.webhook_events` | `id` bigserial | ver sección 5 | webhooks |
| `bsale_raw.sync_runs` | `id` | `mode`, `trigger`, `status`, `started_at`, `finished_at`, `summary jsonb` | — |
| `bsale_raw.sync_entity_runs` | `id` | `sync_run_id`, `company_id`, `resource`, `status`, `rows_received`, `rows_upserted`, `rows_unchanged`, `rows_stale`, `requests`, `duration_ms`, `error` | — |
| `bsale_raw.sync_state` | (company_id, resource) | frescura, ver sección 6 | — |
| `bsale_raw.sync_cursors` | (company_id, resource, cursor_name) | `cursor_value jsonb`, `updated_at` (ventanas incrementales, checkpoint del escáner) | — |

**Tabla extra justificada por endpoint oficial (opcional):** `bsale_raw.stock_consumption_types` (`/v1/stock_consumption_types.json`), sólo si un consumidor necesita interpretar consumos.

Los hijos (`document_details`, `stock_reception_details`, …) usan su propio `id` Bsale como `bsale_id`. Se asume único por empresa, pero eso es **NLV**: si la verificación en vivo lo desmiente, la PK pasa a `(company_id, parent_id, bsale_id)`.

### 3.3 `bsale_raw.stocks` (estado actual)

| Columna | Nota |
|---|---|
| `company_id`, `variant_id`, `office_id` | **clave única operativa** `(company_id, variant_id, office_id)` |
| `stock_id` | `id` Bsale del registro, si viene |
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
- **Reconcile (full):** los registros no vistos **no se borran** en entidades (Bsale no borra productos/variantes; los desactiva): se registra `last_seen_at` antiguo y se reporta. En `stocks` y `variant_prices` (estado actual) se eliminan los no vistos sólo si pasa el fusible de % de stale (patrón `snapshot_reconcile.py`: snapshot vacío con filas existentes = fallo; % stale > umbral = fallo, antes de escribir).
- **Empresas independientes:** cada empresa tiene su propia transacción. Resultado de la corrida: `SUCCESS` / `PARTIAL` / `FAILED` / `SKIPPED` (lock).
- **Hijos de documentos:** documento + details + references + sellers se escriben en la **misma transacción**; si falla un hijo, no se escribe el documento.

---

## 5. Inbox de webhooks `bsale_raw.webhook_events`

**Recepción** (endpoint futuro, debe responder rápido):

1. Validar el JSON y los campos obligatorios (`cpnId`, `topic`, `action`, `resourceId`); el topic debe ser uno de los documentados.
2. Resolver `company_id` desde `cpnId` con `bsale_raw.sources.bsale_cpn_id`. Si el `cpnId` es desconocido, se rechaza o se persiste como `FAILED_FINAL`.
3. Persistir el payload original y responder 2xx. **No** llamar a Bsale dentro del request.

Bsale no documenta firma ni secreto para los webhooks. Por eso el contenido del webhook nunca se toma como dato: sólo dispara una reconsulta a la API con el token de la empresa.

**Columnas:**
- `id`, `received_at`, `company_id`, `cpn_id`, `topic`, `action`, `resource`, `resource_id`, `office_id`, `price_list_id`, `sent_at`, `payload jsonb`;
- `dedupe_key`, único con ventana;
- `status`: `PENDING` / `PROCESSING` / `DONE` / `RETRY` / `FAILED_FINAL`;
- `attempts`, `next_attempt_at`, `locked_at`, `locked_by`, `last_error`, `processed_at`.

**Procesamiento** (worker desacoplado):
- Toma eventos con `FOR UPDATE SKIP LOCKED`.
- Agrupa duplicados de la misma clave de refresco: muchos webhooks de stock de una misma variante y sucursal se resuelven con un solo GET.
- Respeta el rate limiter de la empresa.
- Usa backoff entre reintentos: tras N intentos el evento pasa a `FAILED_FINAL`.

**Idempotencia:** el procesamiento de un evento es una reconsulta más un UPSERT, por lo que repetirlo no causa daño. No se asume exactly-once ni orden de entrega.

**Ruteo** (`webhooks.route`; diseño, sin acciones productivas).

La primera tarea consulta **exactamente** `https://api.bsale.io` + `resource`, tal como lo entrega Bsale. `parse_webhook` lo valida con `validate_resource`:
- solo acepta rutas relativas con el patrón documentado del topic;
- exige coherencia con `resourceId`, `officeId` y `priceListId`;
- rechaza URLs absolutas, otros hosts, `..`, query extra y versiones no documentadas.

| topic | `resource` aceptado | Tareas adicionales |
|---|---|---|
| `product` | `/v2/products/{id}.json` | — |
| `variant` | `/v2/variants/{id}.json` | costos de la variante (`/v1/variants/{id}/costs.json`) |
| `price` | `/v2/price_lists/{pl}/details.json?variant={id}` | — |
| `stock` | `/v2/stocks.json?variant={id}&office={office}` | — |
| `document` | `/documents/{id}.json` | details/references/sellers; luego stock de cada variante del documento (crítico para OC 33 de empresa 3, que puede reservar stock) |

Si la verificación en vivo muestra que una ruta `/v2` o sin versión no responde igual que `/v1`, el patrón se ajusta en código y documentación; nunca se reescribe la versión en silencio.

---

## 6. Frescura (`bsale_raw.sync_state`)

Una fila por `(company_id, resource)` con:
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

1. **Webhook `stock`:** refresco puntual de company + variante + sucursal.
2. **Documentos nuevos:** refresco de las variantes de cada documento (webhook `document` o incremental). Prioridad: OC 33 de la empresa 3.
3. **Escáner continuo controlado** (diseño, sin intervalos fijos hasta medir volúmenes):
   - recorre las empresas y, dentro de cada una, las sucursales activas; pagina `stocks.json?officeid=X` con checkpoint (`sync_cursors`: empresa, sucursal, offset) para retomar tras un corte;
   - consume como máximo una fracción del presupuesto de requests de la empresa y cede siempre prioridad a la cola de webhooks y refrescos puntuales;
   - cada página es un UPSERT pequeño (actualiza `last_seen_at` y cambios); nunca borra;
   - al completar un ciclo por sucursal, registra cobertura y duración, que sirven para fijar el SLA real.
4. **Full reconcile periódico:** snapshot completo por empresa con fusible, que es el único modo que elimina filas no vistas.

### Documentos (crítico; empresa 3 / tipo 33 primero)

- **Webhook:** descarga inmediata del documento con sus hijos, UPSERT y refresco del stock de sus variantes.
- **Incremental frecuente:** `emissiondaterange` sobre los últimos 2 días con solape (`emissionDate` no tiene zona horaria), filtrado por `documenttypeid`. Las ventanas se guardan en `sync_cursors`.
- **Reconcile posterior:**
  - re-barrido de 45 días con comparación contra `count.json` por ventana;
  - detecta anulaciones (`state=1`) y modificaciones que no generaron webhook. Bsale sólo documenta `post` para el webhook de documentos.

### Productos, variantes, precios, costos, configuración

Se aplican los webhooks documentados más el full reconcile según la matriz. Los costos siguen la estrategia de recepciones descrita en la matriz (sección 1.10): no se hacen llamadas continuas para todas las variantes.

---

## 8. Plan de fases (propuesto)

1. **Fase 1 (esta):** arquitectura, matriz, inventario, scaffold y tests puros.
2. **Fase 2:** verificación en vivo, de solo lectura y autorizada, de las dudas NLV de la matriz, empresa por empresa.
3. **Fase 3:** migraciones `bsale_raw` (revisión y aplicación manual), motor de full reconcile para los recursos de configuración, sin consumidores.
4. **Fase 4:** stock (escáner y puntual) y documentos (incremental), en paralelo a los syncs actuales para comparar paridad.
5. **Fase 5:** inbox y worker de webhooks; solicitud de activación a Bsale.
6. **Fase 6:** `bsale` pasa a leer desde `bsale_raw`; retiro gradual de los syncs legacy según `BSALE_SYNC_INDEX.md`.
