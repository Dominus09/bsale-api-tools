# Auditoría: Pendiente / Probable / Facturada (OC Distribuidora)

## Resumen ejecutivo

| Pregunta | Respuesta |
|----------|-----------|
| ¿Se calcula en cada consulta? | **Sí**, en la mayoría de pantallas el estado se **deriva al leer** (vistas SQL con `LATERAL` o joins en Python). |
| ¿Existe tabla cache de estado final? | **No**. Solo existe cache **parcial** de heurística: `document_probable_matches`. |
| ¿Cuánto tarda hoy? | En `planning-rows` (~500 OC/día), instrumentación `[PLANNING_ROWS_STAGE]` muestra que dominan `load_purchase_status` + `load_probable_matches` (orden de **segundos a decenas de segundos** por request, según modo monolito vs desglosado). |

---

## 1. Reglas de negocio (una sola semántica, varios nombres)

### Facturada confirmada (Bsale)

- **Fuente de verdad:** `document_related` enlazado desde un **detalle** de la OC (`document_details`) hacia un documento tipo **1 (Boleta)** o **6 (Factura)**.
- **Código:** `purchase_status = 'FACTURADA_CONFIRMADA'`, `estado_real = 'Facturada'`, `is_invoiced = true`.
- **Planificación despacho:** `relation_source = 'relateddetailid'`, `status = 'confirmed'`.

### Probable facturada (heurística)

- **Fuente:** tabla persistida `document_probable_matches` (score, candidato, flags).
- Se toma el **mejor candidato** con `score >= 60` (LATERAL / `DISTINCT ON`, orden `score DESC`).
- **Tiers UI/API:**
  - `score >= 90` → `PROBABLE_FACTURADA_HIGH`
  - `score >= 75` → `PROBABLE_FACTURADA_MEDIUM`
  - `score >= 60` → `PROBABLE_FACTURADA_LOW`
- **Auto-confirmación operacional** (sin modificar `document_related`):
  - `score >= 75` → tratada como **confirmada** en dashboard plan (`v_dispatch_plan_invoiced_documents`, `invoicing_auto_confirm.py`).
  - En Pre-despacho **sigue mostrándose** como probable/confirmada según `purchase_status` del row (badge distingue auto-confirmada en frontend).

### Pendiente

- Sin enlace `document_related` a boleta/factura **y** sin match probable con `score >= 60`.
- `purchase_status = 'PENDIENTE'`, `estado_real = 'Pendiente'`.

### Umbrales alineados frontend/backend

| Score | Pre-despacho (`purchase_status`) | Dashboard plan (`status`) |
|-------|----------------------------------|---------------------------|
| — + related | `FACTURADA_CONFIRMADA` | `confirmed` / `relateddetailid` |
| ≥ 75 | `PROBABLE_FACTURADA_MEDIUM` (+ auto en plan) | `confirmed` / `auto_match` |
| 60–74 | `PROBABLE_FACTURADA_LOW` | `probable` / `probable_match` |
| &lt; 60 | `PENDIENTE` | `missing` |

---

## 2. ¿Dónde se calcula hoy? (dinámico vs persistido)

### Persistido (solo capa probable)

| Tabla | Qué guarda | Cuándo se escribe |
|-------|------------|-------------------|
| `document_probable_matches` | Candidatos OC→boleta/factura + score | Job `live_sync_probable_matches`, `build_probable_invoice_matches_*`, **no** en `POST /sync-orders` estándar |
| `document_related` | Enlace Bsale confirmado | `sync_distribuidora_related_documents`, live `related` |
| `documents`, `document_details` | Datos base OC/ventas | Sync Bsale órdenes/ventas |

**No hay** tabla con `purchase_status` / `PENDIENTE|PROBABLE|FACTURADA` ya resuelto por `document_id`.

### Calculado en cada lectura (dinámico)

| Consumidor | Mecanismo | Tablas/vistas involucradas |
|------------|-----------|----------------------------|
| **Pre-despacho** `planning-rows` | SQL por request: `NOT EXISTS` facturación + joins batch conf/prob + Python merge | `documents`, `document_details`, `document_related`, `document_probable_matches`, `bsale.clients` |
| **Pre-despacho** filtros | Mismo cálculo embebido en WHERE | Igual |
| **Vista global** | `v_purchase_document_status_full` | `v_orders_purchase` + `v_orders_purchase_status` (LATERAL related) + LATERAL `document_probable_matches` |
| **Vista liviana** | `v_purchase_document_status` | Proyección de `_full` |
| **Dashboard plan** | `v_dispatch_plan_invoiced_documents` (026) | `dispatch_plan_orders` + 2× LATERAL (related + probables) |
| **Lista planificación** | `v_purchase_document_status` join | Vista pesada |
| **Picking cliente/producto** | Datos persistidos en `dispatch_plan_picking_*` generados desde plan; facturación del plan vía vista invoiced | Picking tables + vista 026 |
| **Frontend** | `resolvePurchaseStatusCode()` sobre campos del row | Solo interpreta payload API |

---

## 3. Tablas que participan (grafo)

```
documents (OC type 33)
    ├── document_details
    │       └── document_related ──► documents (inv 1/6)  ← FACTURADA confirmada
    └── document_probable_matches ──► documents (candidato)  ← PROBABLE (pre-calculado)

v_documents_latest  (wrapper documents; usado en vistas legacy)
v_orders_purchase / v_oc_attributes_flat  (enriquecimiento OC)
v_orders_purchase_status  (LATERAL related por OC)
v_purchase_document_status_full  (conf + LATERAL prob + CASE status)
v_dispatch_plan_invoiced_documents  (por plan_id; related + prob + auto 75)
```

---

## 4. Tiempo de cálculo (evidencia)

Medición referencia: `GET .../dispatch-prep/planning-rows`, 1 día, `limit=500`, ~**34.8 s** total (modo monolito histórico).

Con `[PLANNING_ROWS_STAGE]` (consultas desglosadas, sin optimizar):

| Etapa | Qué mide |
|-------|----------|
| `load_purchase_status` | Resolver facturación vía `document_related` |
| `load_probable_matches` | Leer mejor match en `document_probable_matches` |
| `load_base_orders` | Paginación + campos base |
| `load_georef` | Clientes (no es estado, pero suma al request) |

**Conclusión:** el costo no es el CASE en Python sino **repetir joins/LATERAL** (o subconsultas equivalentes) en **cada endpoint y cada página**.

Dashboard plan: logs `[DASHBOARD_STAGE]` / `load_invoiced_documents` — misma familia de joins sobre OCs del plan.

---

## 5. Propuesta: `purchase_document_status_cache`

### Objetivo

- Calcular **una vez** por OC (o por lote en sync).
- Leer con **PK lookup** o índice por fecha en todos los consumidores.
- Invalidar/refresh en:
  1. **Sync Bsale órdenes** (+ related)
  2. **Sync live** (`related` + `probable_matches`)
  3. **Actualizar facturación** (job/API dedicado)
  4. Tras cambios manuales en `document_related` (mismo pipeline related)

### Diseño de tabla

Ver migración `backend/sql/distribuidora/030_purchase_document_status_cache.sql`.

Campos clave:

- `oc_document_id` (PK)
- `purchase_status`, `estado_real`, `is_invoiced_confirmed`, `is_auto_confirmed`
- `relation_source` (`relateddetailid` | `auto_match` | `probable_match` | null)
- Folios confirmados y probables, `probable_score`, `probable_tier`, `display_score`
- `emission_date` (denormalizado para filtros Pre-despacho)
- `computed_at`, `compute_source` (sync_orders, sync_related, sync_probable, manual_refresh)

### Vista de compatibilidad (fase 2 código)

`v_purchase_document_status_cached` → lectura simple para reemplazar `_full` en endpoints.

### Refresh (fase 2 código, no en esta entrega)

```text
refresh_purchase_document_status_cache(oc_document_ids[])
refresh_purchase_document_status_cache_date_range(from, to)
  → UPSERT desde document_related + top probable match (misma lógica que 015/026)
```

Hook sugerido:

- Fin de `sync_distribuidora_related_documents` → refresh OCs tocadas
- Fin de `live_sync_probable_matches` → refresh OCs en ventana
- Nuevo botón **Actualizar facturación** → refresh rango fechas
- `POST /sync-orders` → encolar refresh del rango sincronizado

### Consumidores objetivo (lectura cache)

| Módulo | Cambio |
|--------|--------|
| Pre-despacho `planning-rows` | `JOIN purchase_document_status_cache` en lugar de subconsultas conf/prob |
| Dashboard plan invoicing | Vista plan-first leyendo cache por `oc_document_id` |
| Picking | Al generar picking, copiar status desde cache al persistir |
| App choferes | Misma columna operacional `status` / `relation_source` |

### Impacto esperado

- `planning-rows`: de **O(página × (related + lateral))** a **O(página)** index seek en cache.
- Objetivo operativo: **&lt; 2 s** por día/500 filas (tras cache + índices 028/029).

---

## 6. Qué NO hace la cache

- No reemplaza `document_probable_matches` (sigue siendo el input de la heurística).
- No escribe en Bsale ni en `document_related`.
- No cambia umbrales 60/75/90 sin acuerdo de negocio.

---

## 7. Próximos pasos (implementación)

1. Aplicar migración **030** en BD.
2. Implementar `purchase_document_status_cache_repo.py` + refresh job.
3. Cablear refresh post-sync (orders + related + probable).
4. Cambiar `planning-rows` a leer cache (medir `[PLANNING_ROWS_STAGE]` de nuevo).
5. Sustituir `v_dispatch_plan_invoiced_documents` por join a cache (026 bis).
