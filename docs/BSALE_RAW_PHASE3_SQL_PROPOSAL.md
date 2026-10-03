# BSALE_RAW — Fase 3: migraciones SQL (generadas, NO EJECUTADAS)

> **Estado:** propuesta conceptual aprobada; las migraciones definitivas están en `backend/sql/bsale_raw/`.
>
> **Nada se ha ejecutado**: no hay jobs, ni deploy, ni commit.
>
> La aplicación será manual con el playbook, primero en un entorno de prueba (ver §9).

El DDL vive **sólo** en los archivos SQL; este documento explica las decisiones. Metadatos verificados por MCP (catálogo, sin leer filas):

- `bsale.companies.company_id` es `BIGINT NOT NULL` (PK).
- El schema `bsale_raw` no existe.

---

## 1. Migraciones

| Archivo | Contenido | Tablas |
|---|---|---|
| `001_schema_sources.sql` | Schema `bsale_raw` + identidad de instancia | `sources` |
| `002_sync_control.sql` | Corridas, frescura, checkpoints | `sync_runs`, `sync_entity_runs`, `sync_state`, `sync_cursors` |
| `003_configuration.sql` | Configuración | `offices`, `taxes`, `document_types`, `product_types`, `price_lists` |
| `004_catalog.sql` | Catálogo | `products`, `variants`, `clients` |
| `005_inventory_pricing.sql` | Estado actual | `stocks`, `variant_prices`, `variant_costs` |
| `006_documents.sql` | Documentos + hijos + auditoría de versiones | `documents`, `document_details`, `document_references`, `document_sellers`, `document_change_log` |
| `007_stock_movements.sql` | Movimientos | `stock_receptions`, `stock_reception_details`, `stock_consumptions`, `stock_consumption_details` |
| `008_webhooks.sql` | Inbox + evidencia | `webhook_events`, `webhook_resource_responses` |
| `009_seed_sources.sql` | Seed idempotente (DML separado) | — |
| `verify_bsale_raw.sql` | Verificación post-DDL **sólo lectura** (no es migración) | — |

Son 27 tablas: las 26 de la propuesta más `document_change_log`, que exigen las reglas OC 33 (§4.1) para auditar los cambios y calcular `affected_variants`. Ajustes respecto de la división sugerida:

- **Sin `009_indexes.sql`:** cada índice va junto a su tabla, para que cada migración sea revisable y autosuficiente.
- **El seed pasa a `009`.**
- **La verificación queda sin número**, para que no se confunda con una migración.

Cada migración:

- va envuelta en `BEGIN; … COMMIT;`;
- no usa `CONCURRENTLY`;
- no llama a la API;
- no contiene `DROP` / `TRUNCATE` / `DELETE` / `UPDATE` / `ALTER`;
- no escribe tablas legacy;
- fuera de `bsale_raw`, sólo referencia `bsale.companies (company_id)` en las FK.

## 2. Claves, FK e índices relevantes

**Claves primarias:**

- Entidades: `(company_id, bsale_id)`.
- `stocks`: `(company_id, variant_id, office_id)`.
- `variant_prices`: `(company_id, price_list_id, variant_id)`.
- `variant_costs`: `(company_id, variant_id)`.
- `document_sellers`: `(company_id, document_id, user_id)`.
- Hijos con id propio: `document_details` / `document_references` `(company_id, document_id, bsale_id)`, `stock_reception_details` `(company_id, reception_id, bsale_id)`, `stock_consumption_details` `(company_id, consumption_id, bsale_id)`.
- `sync_state`: `(company_id, resource, scope)`.
- `sync_cursors`: `(company_id, resource, scope, cursor_name)`.
- `sources`: `company_id`, con `UNIQUE (cpn_id)`.
- Control (`sync_runs`, `sync_entity_runs`, `webhook_events`, `webhook_resource_responses`, `document_change_log`): `id BIGSERIAL`.

**FK permitidas** (el test falla ante cualquier otra):

- `company_id → bsale.companies (company_id) ON DELETE RESTRICT`, en todas las tablas excepto `sync_runs`;
- `sync_entity_runs.sync_run_id → sync_runs(id) ON DELETE CASCADE`;
- `webhook_resource_responses.webhook_event_id → webhook_events(id) ON DELETE CASCADE`;
- `webhook_events.coalesced_into_id → webhook_events(id) ON DELETE SET NULL`.

No hay FK entre tablas de datos raw: un hijo puede llegar antes que su padre.

**IDs propios de Bsale (sin UNIQUE):** la unicidad dentro de la instancia no está demostrada, así que sólo llevan índice.

- `stocks.bsale_stock_id`, con índice `(company_id, bsale_stock_id)`.
- `variant_prices.bsale_detail_id`, con índice `(company_id, bsale_detail_id)`.

**Índices de frescura y reconcile:**

- `stocks (company_id, office_id, api_fetched_at)`;
- `variant_prices (company_id, price_list_id, api_fetched_at)`;
- `(company_id, api_fetched_at)` en `products`, `variants`, `clients` y `variant_costs`.

**CHECK:** sólo sobre valores internos. Los tests verifican que coincidan con los enums Python:

| Columna | Enum Python |
|---|---|
| `last_source`, `sync_runs.mode` | `SyncMode` |
| `status` de corridas / `sync_state` | `RunStatus` |
| `webhook_events.status` | `WebhookStatus` (incluye `COALESCED`) |
| `envelope` | `ResponseEnvelope` |
| `document_change_log.detected_by` | `SyncMode` |
| `document_change_log.change_kind` | `DocumentChangeKind` (`CREATED` / `MODIFIED`; facturación y anulación se leen de `state`, no se codifican) |
| `sources.token_env` | `^BSALE_TOKEN_…` (formato del nombre de variable) |

No hay CHECK sobre `state`, tipos, códigos SII ni `scope`.

## 3. Protección de frescura / concurrencia (snapshot vs webhook)

Sobre la misma fila de estado actual pueden escribir cuatro mecanismos: webhook targeted, targeted manual o de sistema, scanner y full reconcile. Por eso todas las tablas de datos raw tienen `api_fetched_at TIMESTAMPTZ NOT NULL`, que es el instante de la respuesta HTTP que originó la fila.

**Regla 1 — un snapshot viejo NUNCA sobrescribe una fila obtenida después.** Todo UPSERT futuro debe usar:

```sql
INSERT INTO bsale_raw.<tabla> (...) VALUES (...)
ON CONFLICT (<pk>) DO UPDATE
SET ... , api_fetched_at = EXCLUDED.api_fetched_at
WHERE bsale_raw.<tabla>.api_fetched_at <= EXCLUDED.api_fetched_at;
```

Las filas omitidas por ser más viejas se cuentan en `sync_entity_runs.rows_skipped_newer`.

**Regla 2 — DELETE stale (sólo reconcile destructivo de stock por company + office y de precios por company + price_list).**

1. `snapshot_started_at` se captura **antes** del primer GET y se guarda en `sync_entity_runs.snapshot_started_at`.
2. Una fila sólo puede eliminarse si se cumplen las tres condiciones:
   - no existe en staging;
   - pertenece exactamente al company / resource / scope reconciliado;
   - `target.api_fetched_at <= snapshot_started_at`.

Una fila refrescada por webhook o targeted refresh durante el snapshot tiene `api_fetched_at > snapshot_started_at`, así que **no se sobrescribe** (regla 1) **ni se borra** (regla 2). El fusible de % stale (20 % por defecto, patrón de `snapshot_reconcile.py`) se evalúa antes de escribir.

En entidades no hay borrado: el full reconcile marca `missing_since` bajo las mismas condiciones de la regla 2.

## 4. Open document watch (OC 33, company 3)

El webhook documentado sólo garantiza la creación, y `generationdaterange` fue REJECTED (403). Por eso los documentos no terminales se identifican desde `bsale_raw.documents` y se refrescan **por ID**, sin tabla adicional.

- **Columnas indexables de `documents`:** `company_id`, `document_type_id`, `office_id`, `state`, `commercial_state` (TEXT tal cual), `emission_date` (DATE), `generation_date` (TIMESTAMPTZ), `api_fetched_at`, `payload_hash`, `details_complete`, `children_fetched_at`.
- **Índices:**
  - `ix_raw_documents_watch (company_id, document_type_id, api_fetched_at) WHERE watch_closed_at IS NULL`: candidatos de empresa + tipo con el watch abierto (incluida la gracia post-cierre), los menos recientemente confirmados primero;
  - `ix_raw_documents_type_emission`: lookback;
  - `ix_raw_documents_type_state (company_id, document_type_id, state, commercial_state)`;
  - `ix_raw_documents_generation`.
- **Interpretación en Python:** `resources/documents.py::OPEN_DOCUMENT_WATCHES` define `OpenDocumentWatch(3, 33, lookback_days=45, min_refresh_interval_seconds=900)`.
  - `terminal_states` y `terminal_commercial_states` están **vacíos** hasta confirmarlos en vivo, así que todo documento dentro del lookback es candidato.
  - El SQL no asigna significado a ningún estado.
- **Consulta futura:**

  ```sql
  SELECT bsale_id FROM bsale_raw.documents
  WHERE company_id = 3 AND document_type_id = 33
    AND emission_date >= current_date - 45
    AND NOT (state = ANY(:terminal_states))          -- desde Python
  ORDER BY api_fetched_at
  LIMIT :n;
  ```

- **Persistencia atómica:** ver §4.1.

### 4.1 Reglas críticas OC 33 (lecciones del sistema legacy)

**Una OC 33 es MUTABLE.** Después de creada puede:
- agregar o quitar productos;
- cambiar cantidades, descuentos, precios o montos;
- cambiar vendedor, cliente, atributos o estado;
- ser facturada o anulada;
- adquirir references nuevas.

Nunca se trata como append-only ni como "creada una vez".

**Refresh atómico, una sola versión.** Un refresh completo obtiene el header, los details paginados, las references, los sellers y los attributes (`DOCUMENT_REFRESH_PARTS`) y los persiste en **una** transacción:

```text
BEGIN
  previous_variants := SELECT variant_id FROM document_details WHERE company_id, document_id
  UPSERT documents (WHERE target.api_fetched_at <= EXCLUDED.api_fetched_at)
      → si no se aplica: ROLLBACK (ya hay una versión más nueva)
  REPLACE document_details     (UPSERT recibidos + eliminar los que ya no vinieron)
  REPLACE document_references  (ídem)
  REPLACE document_sellers     (ídem)
  documents.attributes_payload := respuesta de attributes
  INSERT document_change_log si cambió version_hash (o primera observación)
COMMIT          -- cualquier fallo: ROLLBACK completo
→ stock puntual de TODAS las affected_variants (previous ∪ current)
```

**Columnas que lo soportan:**

- `documents.version_hash`: hash de la versión completa (header + details + references + sellers + attributes), calculado por `DocumentVersion.version_hash`. `children_hash` es lo mismo sin el header. `version_changed_at` registra cuándo cambió.
- **`document_version_hash TEXT NOT NULL`** en `document_details`, `document_references` y `document_sellers`. La invariante es que todo hijo tiene el mismo hash que su documento, así que una mezcla de versiones (header nuevo con details antiguos) es detectable por consulta:

  ```sql
  SELECT d.company_id, d.document_id FROM bsale_raw.document_details d
  JOIN bsale_raw.documents h ON h.company_id = d.company_id AND h.bsale_id = d.document_id
  WHERE d.document_version_hash IS DISTINCT FROM h.version_hash;
  ```

**Replace children.** No basta con hacer UPSERT de los hijos recibidos. Una línea que existía y ya no viene se **elimina** de la copia actual, dentro de la misma transacción. Requiere que los details se hayan paginado completos (`details_complete`). Si la paginación queda incompleta, se hace ROLLBACK.

**Identidad de hijos.** Todo hijo conserva `company_id` y `document_id` (NOT NULL) además de su id Bsale. Nunca se infiere el padre desde el id del hijo. La PK de `document_details` y `document_references` es `(company_id, document_id, bsale_id)` (y la de `document_sellers` `(company_id, document_id, user_id)`), así que su prefijo soporta el reemplazo por documento y un UPSERT no puede mover una línea entre documentos. El índice no único `(company_id, bsale_id)` sirve para detectar un mismo id bajo dos padres. Recepciones y consumos siguen el mismo patrón: `(company_id, reception_id, bsale_id)` y `(company_id, consumption_id, bsale_id)`.

**Productos y stock.**
- `affected_variants = previous_variants ∪ current_variants` (`affected_variants()`). Cubre producto agregado o eliminado, cambio de cantidad, cancelación, facturación y liberación o creación de reserva.
- El conjunto queda en `document_change_log.affected_variant_ids`.
- El refresh de stock puntual se lanza **después del COMMIT**: `stock_refresh_requested_at` / `stock_refresh_done_at`, con índice parcial de pendientes.
- Nunca se refrescan sólo las variantes de la versión nueva.

**Descuentos, cantidades y montos.** Cualquier cambio de línea modifica `version_hash`, aunque el header no cambie: los details se hashean por separado. La capa normalizada posterior se reconstruye desde la última versión RAW, sin conservar cálculos de versiones anteriores.

**Facturación.** La OC **no se elimina**. Se conservan:
- estado final, payload final, details, sellers y references finales;
- related documents, cuando la API permita resolverlos (`payload` / `document_references`).

Tras el cambio de estado se refrescan references y stock de las variantes. La OC sigue siendo trazable hasta el documento posterior.

**Anulación.** La OC **no se elimina**. Se persisten el estado y los campos de cancelación (en `payload`), el payload final y las references actuales. Se refresca el stock de **todas** las variantes anteriormente comprometidas (`previous_variants`).

**Grace watch post-cierre.** Al detectar un estado terminal, la OC no sale del watch de inmediato.
- `OpenDocumentWatch.decide()` devuelve `ACTIVE` / `GRACE` / `CLOSE`.
- Valores por defecto: gracia de 45 min (dentro de la propuesta de 30–60), lectura por ID cada 10 min y al menos 3 lecturas estables (sin cambio de `version_hash`).
- En cada lectura se revisan el header y las references; los details, si cambia el hash; y el stock afectado, si corresponde.
- Un cambio durante la gracia reinicia el conteo.
- Columnas: `watch_terminal_seen_at`, `watch_stable_reads`, `watch_closed_at`. Sólo al cerrar sale del índice del watch.
- Los estados terminales se configuran **sólo en Python** (`terminal_states` / `terminal_commercial_states`, vacíos hasta confirmarlos en vivo). El SQL no los codifica.

**Hash y frescura.** `documents` mantiene `payload_hash`, `first_seen_at`, `last_seen_at`, `last_changed_at` y `api_fetched_at`.
- **Sin cambios:** se actualizan `last_seen_at` y `api_fetched_at`, pero **no** `last_changed_at`.
- **Con cambios:** se actualizan `payload_hash` / `version_hash`, `last_changed_at` y `version_changed_at`, y se hace el refresh completo de hijos.
- Como sellers y attributes pueden cambiar sin tocar el header, también se fuerza un refresh completo de hijos cada `children_full_refresh_seconds` (60 min por defecto).

**Auditoría.** `documents` guarda el estado **actual**; no se guarda el historial de cada polling. `document_change_log` registra cada cambio de versión, sin payload:
- `change_kind`, `detected_at`, `api_fetched_at`;
- **cómo** se detectó: `detected_by`, `sync_run_id`, `webhook_event_id`;
- hashes y estados previo y actual;
- partes cambiadas y variantes previas, actuales y afectadas.

Con eso se reconstruye la secuencia: OC creada → modificaciones → versión vigente → facturada (cambio de `state` y references) o anulada. `sync_runs`, `sync_entity_runs` y `webhook_events` completan el cuándo y el cómo.

## 5. Webhooks: dedupe vs coalescing

Bsale **no documenta un event_id**, así que no se asume. **Cada POST recibido se conserva** como fila (evidencia RAW), incluso los reenvíos idénticos.

| Clave | Composición | Uso | Unicidad |
|---|---|---|---|
| `dedupe_key` | company \| topic \| action \| resourceId \| officeId \| priceListId \| send | Identificar el reenvío exacto | **Ninguna** (índice normal, diagnóstico) |
| `refresh_key` | company \| topic \| resourceId \| officeId \| priceListId | Identificar el trabajo de refresh | **Parcial, sólo eventos activos** |

Índices únicos parciales:

- `uq_raw_webhook_events_queued_refresh (refresh_key) WHERE status IN ('PENDING','RETRY')`: a lo sumo un evento en cola por recurso;
- `uq_raw_webhook_events_processing_refresh (refresh_key) WHERE status = 'PROCESSING'`: nunca dos workers refrescando el mismo recurso a la vez.

Flujo futuro:

1. **Recepción:** `INSERT … status='PENDING' ON CONFLICT (refresh_key) WHERE status IN ('PENDING','RETRY') DO NOTHING`. Si hay conflicto, se inserta el evento como `COALESCED` con `coalesced_into_id` apuntando al de la cola. Si ese era `RETRY`, se adelanta su `next_attempt_at`.
2. **Mientras otro está PROCESSING:** el nuevo entra como `PENDING`. El refresh en curso pudo leer Bsale antes del cambio, así que hace falta otro.
3. **Fallo de un PROCESSING** que ya tiene un `PENDING` del mismo recurso: pasa a `COALESCED` en ese pendiente, en vez de `RETRY`.
4. **Una vez DONE / FAILED_FINAL / COALESCED,** un evento idéntico del mismo recurso vuelve a entrar como `PENDING`: no hay UNIQUE permanente.

Python: `WebhookEvent.refresh_key` y `WebhookEvent.dedupe_key_text`.

## 6. Scope canónico

`sync_state.scope`, `sync_cursors.scope` y `sync_entity_runs.scope` son `TEXT`, **sin CHECK**, con default `'global'`.

| Scope | Uso | Helper (`core/registry.py`) |
|---|---|---|
| `global` | recurso completo de la empresa | `GLOBAL_SCOPE` |
| `office:<office_id>` | stock por sucursal | `office_scope()` |
| `price_list:<price_list_id>` | precios por lista | `price_list_scope()` |
| `document_type:<document_type_id>` | documentos por tipo (OC 33) | `document_type_scope()` |

Los scopes nuevos se definen **exclusivamente** en `registry.py` y no requieren migración. Las PK se mantienen: `sync_state (company_id, resource, scope)` y `sync_cursors (company_id, resource, scope, cursor_name)`.

## 7. Seguridad del payload RAW

- Se guarda el `payload JSONB` **completo**, sin "sanitizar": la protección es de **acceso**, no de alteración del dato fuente.
- **PII:** `clients.payload` contiene nombres, RUT, email, dirección y teléfono. Los documentos también traen datos del cliente.
- **Datos sensibles en `documents.payload`:** el `token` del documento, URLs de PDF / XML / public view y otros. Quien tenga esas URLs puede ver el documento.
- Las columnas sensibles llevan `COMMENT ON` con la advertencia.
- **No exponer `payload` RAW** mediante endpoints genéricos ni al frontend. Los consumidores leen columnas proyectadas o el schema `bsale`.
- **No imprimir `payload`** completo en logs normales. Los errores se registran sanitizados (`last_error`, `error`).
- **Los tokens API nunca se almacenan** en payload, config ni SQL.
  - `sources.token_env` guarda sólo el **nombre** de la variable (CHECK `^BSALE_TOKEN_…`).
  - Los tests verifican que no haya columnas de secretos ni valores con forma de token en los SQL.
- `webhook_resource_responses` no guarda headers de request.

## 8. Rollback

| Momento | Rollback válido |
|---|---|
| **Antes del cutover:** sin jobs productivos, sin webhooks habilitados, sin consumidores conectados y RAW aún no considerado fuente productiva | `DROP SCHEMA bsale_raw CASCADE` (nada fuera de `bsale_raw` depende de él; las FK salen hacia `bsale.companies`, nunca al revés) |
| **Después del cutover** | **No** se hace DROP. En su lugar: (1) desactivar los jobs y webhooks nuevos; (2) devolver los consumidores a la ruta anterior (syncs legacy); (3) conservar `bsale_raw` para diagnóstico; (4) corregir y reanudar |

## 9. Bootstrap y drift

- **Instalación inicial:** `bsale_raw` no existe. Se permite `CREATE SCHEMA IF NOT EXISTS` / `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`.
- **Verificación:** `verify_bsale_raw.sql` es un bloque `DO` de sólo lectura sobre `pg_catalog`. Compara contra el catálogo real:
  - tablas;
  - columnas, con tipo y nulabilidad;
  - constraints por nombre;
  - columnas de cada PK;
  - destino de cada FK;
  - índices, con unicidad.

  Ante cualquier faltante o sobrante lanza `RAISE EXCEPTION`. El test `test_verify_block_matches_migrations` garantiza que el bloque esperado coincide con las migraciones.
- **Migraciones futuras:** **no** deben apoyarse en `IF NOT EXISTS` para ocultar drift. Deben ser explícitas (`ALTER TABLE … ADD COLUMN …`) y actualizar `verify_bsale_raw.sql`.
- **Sintaxis no validada:** el entorno local no tiene un parser PostgreSQL. La sintaxis se valida al aplicar en el entorno de prueba.

**Plan de aplicación (cuando se autorice):**

1. Entorno de prueba: `001` → `008`, luego `verify_bsale_raw.sql`.
2. Seed `009` como paso separado y aprobado.
3. Producción con el mismo orden, sin consumidores.
4. Sólo después, el motor de full reconcile de configuración (P6) en modo manual.

## 10. Abierto (no bloquea el DDL)

- Unicidad de los ids de hijos por empresa (la deduplicación de staging la vigila).
- Estados terminales de OC 33 (`state` / `commercial_state`), que se configuran en Python.
- Attributes del documento: `GET /v1/documents/{id}/attributes.json` LIVE VERIFIED (paginado, `count`/`items`); se guarda la colección completa en `attributes_payload` = `{"count", "items"}`. Related documents (NLV): se guardan en `payload` / `document_references` tal como lleguen.
- Retención de `document_change_log` (sin purga en la fase 3).
- Retención de `webhook_events` y `webhook_resource_responses` (propuesta: 90 / 30 días; job futuro).
- Stock con `quantity = 0` y variantes inactivas (no cambia el DDL).
