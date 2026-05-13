# Plan operacional — Mayo 2026 (Distribuidora, Office 1)

**Decisión de alcance:** preparar el ERP estable **solo con datos de mayo de 2026** (2026-05-01 → 2026-05-31, UTC calendario según convención actual del código). **No** histórico 2025→abril 2026 en esta fase. **No** multiempresa ni otros office.

**Empresa / oficina (código actual):** `company_id = 3`, `office_id = 1` (Quillotana SPA / Distribuidora en el modelo Bsale del repo).

Este documento es **diseño y checklist**; no implementa jobs ni cron.

---

## 1. Arquitectura propuesta (mayo + incremental “mayo+”)

| Capa | Rol en mayo 2026 |
|------|-------------------|
| **A — Documents** | `distribuidora.documents` (+ raw) para todos los tipos necesarios en ventana; OC (33), boletas/facturas (1/6), NC (9) que aparezcan en API dentro del alcance elegido para related y márgenes. |
| **B — Details** | `distribuidora.document_details` por `document_id`; prerequisito para `relateddetailid` por línea. |
| **C — Related operacional** | `distribuidora.document_related` solo vía **relateddetailid** (no mezclar con `document_references` tributario). |
| **Estado incremental** | `distribuidora.sync_process_cursor` (cursores `process_name` existentes) + **`distribuidora.sync_state`** (`sync_type` + `mode` + `office_id`) para watermarks y overlap cuando los jobs oficiales existan. |
| **Historial por corrida** | `distribuidora.sync_status` + `sync_logs` (append-only, sin cambiar contrato de dashboards que lean `v_sync_status`). |

**Separación explícita**

- **Backfill manual mayo:** rangos fijos `2026-05-01` … `2026-05-31`, ejecución acotada, checkpoints por día o subventana, sin competir con live salvo política de locks.
- **Live incremental “mayo+”:** ventana corta + **overlap** sobre el último watermark persistido en `sync_state` (y/o cursor legado hasta migrar), misma regla de idempotencia (`ON CONFLICT` ya presente en related).

---

## 2. Análisis del estado actual de mayo 2026 (tarea 1)

Ejecutar en **PostgreSQL** contra la BD operacional (ajustar si usan otro huso; el código de listados usa fechas calendario + intervalo de día).

### 2.1 Conteos base (documents / details / related)

```sql
-- Ventana mayo 2026 (emisión UTC almacenada en timestamptz)
WITH bounds AS (
  SELECT
    TIMESTAMPTZ '2026-05-01 00:00:00+00' AS d0,
    TIMESTAMPTZ '2026-06-01 00:00:00+00' AS d1_excl
)
SELECT
  (SELECT COUNT(*) FROM distribuidora.documents d, bounds b
   WHERE d.company_id = 3 AND d.office_id = 1
     AND d.emission_date >= b.d0 AND d.emission_date < b.d1_excl) AS documents_mayo,
  (SELECT COUNT(*) FROM distribuidora.documents d, bounds b
   WHERE d.company_id = 3 AND d.office_id = 1 AND d.document_type_id = 33
     AND d.emission_date >= b.d0 AND d.emission_date < b.d1_excl) AS oc_mayo,
  (SELECT COUNT(*) FROM distribuidora.document_details dd
   INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
   CROSS JOIN bounds b
   WHERE d.company_id = 3 AND d.office_id = 1
     AND d.emission_date >= b.d0 AND d.emission_date < b.d1_excl) AS details_mayo,
  (SELECT COUNT(*) FROM distribuidora.document_related dr
   INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
   INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
   CROSS JOIN bounds b
   WHERE d.company_id = 3 AND d.office_id = 1
     AND d.emission_date >= b.d0 AND d.emission_date < b.d1_excl) AS related_rows_mayo;
```

### 2.2 Gaps de fechas (días sin OC u sin documentos)

```sql
-- Días calendario (UTC) en mayo sin ningún documento tipo 33
WITH bounds AS (
  SELECT generate_series(
    date '2026-05-01',
    date '2026-05-31',
    interval '1 day'
  )::date AS d
),
oc_days AS (
  SELECT DISTINCT (d.emission_date AT TIME ZONE 'UTC')::date AS d
  FROM distribuidora.documents d
  WHERE d.company_id = 3 AND d.office_id = 1 AND d.document_type_id = 33
    AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
    AND d.emission_date < TIMESTAMPTZ '2026-06-01'
)
SELECT b.d AS day_utc_sin_oc
FROM bounds b
LEFT JOIN oc_days o ON o.d = b.d
WHERE o.d IS NULL
ORDER BY 1;
```

### 2.3 OC mayo sin ningún detalle (anomalía)

```sql
SELECT d.document_id, d.number, d.emission_date
FROM distribuidora.documents d
WHERE d.company_id = 3 AND d.office_id = 1 AND d.document_type_id = 33
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
  AND NOT EXISTS (
    SELECT 1 FROM distribuidora.document_details dd WHERE dd.document_id = d.document_id
  )
ORDER BY d.emission_date, d.number;
```

### 2.4 OC mayo “sin related hacia 1/6” (candidatas pendientes de facturación según regla ERP)

Misma semántica que `OC_PURCHASE_NOT_INVOICED_BY_RELATED_SQL` en `orders_service.py` (existencia de relación desde un **detalle** de la OC hacia documento relacionado tipo **1 o 6** en `v_documents_latest` misma company/office).

```sql
-- Reutilizar la idea: pendiente = NOT EXISTS (related -> inv type 1,6)
SELECT d.document_id, d.number, d.emission_date
FROM distribuidora.documents d
WHERE d.company_id = 3 AND d.office_id = 1 AND d.document_type_id = 33
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
  AND NOT EXISTS (
    SELECT 1
    FROM distribuidora.document_related dr
    INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
    INNER JOIN distribuidora.v_documents_latest inv
      ON inv.document_id = dr.related_document_id
     AND inv.document_type_id IN (1, 6)
     AND inv.company_id = d.company_id
     AND inv.office_id = d.office_id
    WHERE dd.document_id = d.document_id
  )
ORDER BY d.emission_date, d.number;
```

**Interpretación:** no toda OC “debería” tener related (borrador, anulada en Bsale, solo movimientos internos). Las inconsistencias reales requieren **muestra manual** + API Bsale; el SQL solo acota candidatos.

### 2.5 Inconsistencias detectables (automáticas)

| Chequeo | Idea |
|---------|------|
| Related huérfano | `document_related.detail_id` sin fila en `document_details`. |
| Related apuntando a tipo no permitido | Tipos distintos de `{1,6,9}` según política de ingesta (`sync_related_service` usa allowlist para insertar; filas viejas pueden revisarse con auditoría ya existente). |
| `is_invoiced` en vista vs `document_related` | Si `v_orders_purchase_enriched` expone `is_invoiced` distinto del criterio “related 1/6”, documentar drift y unificar criterio en una fase posterior. |

---

## 3. Validación semántica operacional (tarea 2)

**Fuente de verdad operacional:** `document_related` poblada por **relateddetailid** + detalles de la OC.

### 3.1 Estado actual en SQL de negocio (`OC_PURCHASE_ESTADO_REAL_SQL`)

Hoy el ERP clasifica de forma **binaria** en consultas clave:

- **Facturada:** existe al menos un `document_related` desde un detalle de la OC hacia un documento en `v_documents_latest` con `document_type_id IN (1, 6)` (misma company/office).
- **Pendiente:** en caso contrario.

Eso está en `backend/services/distribuidora/orders_service.py` (no distingue aún “parcial”, “NC”, “anulada” en el mismo `CASE`).

### 3.2 Cómo validar semántica ampliada (diseño, no obligatorio implementar ya)

| Concepto | Definición sugerida (para validación manual / futura vista) |
|----------|----------------------------------------------------------------|
| **OC pendiente** | Sin relación 1/6 como arriba. |
| **OC facturada** | Con relación 1/6. |
| **OC parcial** | Subconjunto: algunas líneas con cantidad facturada &lt; cantidad OC vía suma por relaciones y detalles (requiere modelo cantidades desde API o reglas de negocio; **no** está en el `CASE` actual). |
| **OC con nota de crédito** | Existe relación hacia tipo **9** (NC) desde detalles; el sync related ya considera tipo 9 en allowlist de ingesta; el **estado_real** actual no prioriza NC—conviene KPI aparte (“tiene_NC”) para dashboard. |
| **Anulada** | Preferir `state` / `commercial_state` en `documents.raw_data` o columnas si están mapeadas; cruzar con ausencia de 1/6. |

**Validación práctica mayo:** muestreo de N OC “Pendiente” en SQL vs PDF Bsale / pantalla Bsale; muestreo de “Facturada” con trazabilidad `detail_id` → `related_document_id`.

---

## 4. Estrategia final: backfill manual mayo vs live incremental “mayo+” (tarea 3)

### 4.1 Backfill manual mayo (A → B → C)

1. **Ventana fija:** emisión `[2026-05-01, 2026-05-31]` (UTC día a día o rangos de emisión API equivalentes al `sync_service` / `sync_related_documents_range`).
2. **Orden estricto:** documents (tipos necesarios) → details → related.
3. **Checkpoint:** por cada día cerrado con éxito, escribir en `sync_state` (`mode=backfill`, `sync_type` por dominio, `last_window_to`, `items_processed`) + fila en `sync_status` como hoy.
4. **Carga:** throttle API, `batch` por día, límites de página acordes a Bsale; **un** proceso related a la vez (advisory lock existente en related).
5. **Idempotencia:** `ON CONFLICT` en related; upsert habitual en documents/details.

### 4.2 Live incremental “mayo+” (D → E → F)

1. **Watermark:** `last_watermark` = fin de ventana procesada con éxito o `max(emission_date)` de OCs tocadas (definición exacta al implementar).
2. **Overlap:** recomendado **1–2 días** (o 12–24 h) según frecuencia de corrida, para no perder documentos publicados tarde en Bsale.
3. **Límites por corrida:** acotar documentos OC por ejecución (p. ej. 200–500) y throttle entre páginas/llamadas related (`DISTRIBUIDORA_RELATED_API_DELAY_SEC` ya existe).
4. **No cron agresivo:** p. ej. cada 30–120 min en horario laboral hasta estabilizar; ajustar según volumen mayo real medido en §2.

---

## 5. Jobs oficiales propuestos (tarea 4 + 6)

Nombres alineados a la decisión de producto; implementación **futura**.

| Job | Tipo | Función resumida |
|-----|------|------------------|
| **A — `backfill_documents_may_2026`** | Manual | Ingesta/resync documentos con emisión en mayo 2026; company 3, office 1; paginación por día o `emissiondaterange`; idempotente. |
| **B — `backfill_details_may_2026`** | Manual | Detalles para documentos cuya emisión cae en mayo 2026 (o lista derivada del job A). |
| **C — `backfill_related_may_2026`** | Manual | Equivalente operacional a rango día a día sobre OC mayo (`sync_related_documents_range` conceptual); solo relateddetailid. |
| **D — `sync_documents_incremental`** | Automatizable suave | Ventana corta + overlap; mismo office; advisory lock ya usado en sync principal. |
| **E — `sync_details_incremental`** | Automatizable suave | Detalles para documentos “tocados” en D o por watermark. |
| **F — `sync_related_incremental`** | Automatizable suave | Basado en lookback + límite OC (patrón actual del job related); escribe `sync_state` al cerrar. |

### 5.1 Requisitos obligatorios (tarea 5)

| Requisito | Cómo se cumple en diseño |
|------------|-------------------------|
| Idempotente | Upsert documents/details; `ON CONFLICT` en `document_related`; re-ejecutar mismo día mayo no duplica negocio. |
| Observable | Logs stdout estructurados (inicio, ventana, conteos, duración, errores resumidos); filas en `sync_logs` / `sync_status`. |
| Advisory locks | Reutilizar locks existentes (sync documentos vs lock dedicado related); no paralelizar dos related. |
| Retry controlado | Reintentos por deadlock/transitorio (patrón `_with_deadlock_retry` en related); backoff documento-level en related incremental. |
| `ON CONFLICT` | Ya en related; replicar criterio en otros upserts. |
| Paginación segura | Límites/offset o cursor API Bsale acotados; nunca “descargar todo mayo” en una sola llamada. |
| Overlap incremental | `overlap_days` / `overlap_seconds` persistidos en `sync_state` por corrida. |

### 5.2 Orden, frecuencia, límites (recomendaciones iniciales)

| Parámetro | Backfill mayo | Live incremental |
|-----------|----------------|------------------|
| **Orden** | A → B → C (por día o semana) | D → E → F |
| **Frecuencia** | Manual bajo demanda hasta llenar mayo | Cada 30–120 min (no agresivo) |
| **Overlap** | N/A (rango fijo) | 1–2 días |
| **Límite por corrida (OC related)** | Por día completo o cap 300–500 OC/run | Mismo orden de magnitud que env `DISTRIBUIDORA_RELATED_DETAIL_LIMIT` |
| **Batch / página** | Respetar `limit`/`offset` Bsale del `sync_service` / client | Igual |
| **Checkpoint** | `sync_state` backfill + `sync_status` por subventana | `sync_state` incremental tras éxito |
| **Retry** | 3–5 intentos con backoff en error de red; abortar día con marca en `error_summary` | Igual; no avanzar watermark si fallo global |

---

## 6. Frontend ERP — qué necesita mayo “completo” (tarea 7)

Basado en rutas y servicios actuales (`list_purchase_orders`, `v_orders_purchase_enriched`, planificación, sync-status):

| Área | Necesidad mayo 2026 |
|------|---------------------|
| **Pedidos / OC** | Lista con filtros por emisión, comuna, cliente, vendedor; `only_not_invoiced` coherente con `document_related` (validar vs `is_invoiced`). |
| **Pendientes / rutero** | Redirección actual `pendientes` → `rutero`; datos dependen de documents + details + clients georef. |
| **Facturadas / parciales** | Hoy binario en SQL de despacho; para UX “parcial/NC” hace falta extensión de API o campos calculados. |
| **Anuladas** | Filtro por estado Bsale si el front/API lo expone; si no, backlog. |
| **Paginación** | `limit`/`offset` ya en `list_purchase_orders`; mantener límites razonables (≤500 default). |
| **Dashboards / indicadores** | Card de sync (`/distribuidora/sync-status`); métricas por rango mayo en queries dedicadas (opcional endpoint agregado). |
| **Márgenes** | Rutas `margins` / `margin_analysis_view` usan **bsale** y `company_id`; confirmar que mayo tenga costos/listas para no mostrar vacíos engañosos. |
| **Performance** | Índices por `emission_date`, `document_type_id`, company/office; evitar full scan en mayo con `EXPLAIN` en queries de §2. |

---

## 7. Riesgos y prioridades

| Riesgo | Mitigación |
|--------|------------|
| Despliegue DDL vs código (`sync_process_cursor`) | Ya gobernado en FASE 7.3; mantener `ensure_distribuidora_schema` en despliegue mayo. |
| API Bsale rate limit | Throttle + límites por corrida + no paralelizar related. |
| Deriva `is_invoiced` vs related | Checklist QA: comparar muestra OC en listado vs `estado_real` en pre-planificación. |
| “Parcial / NC” no modelado en `estado_real` | Prioridad baja para mayo si negocio acepta binario; alta si operaciones exigen NC explícita. |
| Mayo con volumen inesperado | Ajustar `DETAIL_LIMIT` y timeouts tras primera corrida de conteos §2. |

**Prioridades:** (1) documents+details+related mayo completo y consistente, (2) incremental estable con overlap, (3) alinear KPI OC con reglas de negocio ampliadas, (4) márgenes y dashboards solo si datos mayo lo permiten.

---

## 8. Checklist operacional (mayo 2026)

- [ ] Ejecutar SQL §2.1–2.4 y archivar resultados.
- [ ] Revisar gaps de días §2.2 (¿festivos reales o falta de sync?).
- [ ] Muestreo 10–20 OC pendientes vs Bsale.
- [ ] Muestreo 10–20 OC facturadas con traza `detail_id` / `related_document_id`.
- [ ] Confirmar `v_sync_status` / panel sync tras backfill diario.
- [ ] Definir ventana incremental + overlap en `sync_state` (`incremental`, `office_id=1`).
- [ ] **No** activar histórico 2025 ni multiempresa hasta fase aparte.
- [ ] Documentar decisiones de “OC sin related esperado” (negocio).

---

## 9. Pendientes explícitos (no hacer en esta fase)

- Histórico completo 2025 → abril 2026.
- Multiempresa / otros office.
- Cron definitivos y workers paralelos masivos.
- Implementación de los seis jobs (solo nombres y requisitos aquí).

---

*Documento generado para FASE 7.4 — estabilización operacional mayo 2026.*
