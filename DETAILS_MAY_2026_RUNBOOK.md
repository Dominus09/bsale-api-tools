# Runbook — Backfill `document_details` mayo 2026 (FASE 7.6)

**Alcance:** solo `distribuidora.document_details`, `company_id = 3`, `office_id = 1`, emisión **UTC** **2026-05-01** … **2026-05-31** (inclusive), más días opcionales hacia atrás vía overlap. Los `document_id` se leen **solo** de `distribuidora.documents` ya cargados (p. ej. tras FASE 7.5).

**No** escribe `document_related`, no calcula estado OC, no cambia el frontend.

**Implementación:** `backfill_distribuidora_document_details_may_2026_only` en `backend/services/distribuidora/sync_service.py`; job:

```bash
python -m backend.jobs.backfill_details_may_2026
```

---

## Comando y entorno

Misma base que el API: `PG_*`, `BSALE_TOKEN` o `BSALE_TOKEN_SPA`, `python-dotenv` opcional.

| Variable | Default | Descripción |
|----------|---------|-------------|
| `BACKFILL_MAY_DETAILS_OVERLAP_DAYS` | `0` | Extiende el inicio del filtro de emisión N días antes del 2026-05-01 (0–7). |
| `BACKFILL_MAY_DETAILS_DOCUMENT_BATCH` | `250` | Tamaño de cada `SELECT … LIMIT` (paginación por `document_id`, keyset). |
| `BACKFILL_MAY_DETAILS_DOC_RETRIES` | `4` | Intentos por documento (`GET details.json` + `replace`). |
| `BACKFILL_MAY_DETAILS_EXTRA_SLEEP_SEC` | `0` | Suma al jitter entre documentos (0,15–0,45 s). |
| `BACKFILL_MAY_DETAILS_MAX_DOCUMENTS` | `0` | Si &gt; 0, tope de documentos a procesar (pruebas). |
| `BACKFILL_MAY_DETAILS_RESUME_AFTER_DOCUMENT_ID` | (vacío) | Solo procesa `document_id` mayores a este valor (reanudar). |
| `BACKFILL_MAY_DETAILS_EXIT_CODE_ON_LOCK` | `1` | Código de salida si el advisory lock está ocupado. |
| `BACKFILL_MAY_DETAILS_EXIT1_ON_ERRORS` | (vacío) | `1`/`true`/`yes` → salida `1` si hubo errores por documento. |
| `LOG_LEVEL` | `INFO` | Logging |

---

## Comportamiento técnico

- **Advisory lock:** mismo `ADVISORY_LOCK_KEY` que sync documentos / backfill documents mayo → **no** ejecutar en paralelo con esos jobs.
- **API:** `GET /v1/documents/{id}/details.json` vía `BsaleClient.get` (reintentos 429/5xx/red internos).
- **Persistencia:** `replace_document_details` = `DELETE` líneas del documento + `INSERT` masivo (idempotente al repetir corrida).
- **`sync_status`:** fila `sync_type='details'`, `records_processed` = total de **filas** de detalle escritas.
- **`sync_state`:** `sync_type='details'`, `mode='backfill'`, `office_id=1`, ventana de emisión y `overlap_days`.
- **`sync_logs`:** proceso `backfill_details_may_2026`.

---

## Validaciones SQL

### Conteo filas details en mayo (vía documento padre)

```sql
SELECT COUNT(*)::bigint AS detail_rows
FROM distribuidora.document_details dd
INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
WHERE d.company_id = 3 AND d.office_id = 1
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01';
```

### `detail_id` duplicado (no debería ocurrir)

```sql
SELECT detail_id, COUNT(*) AS c
FROM distribuidora.document_details
GROUP BY detail_id
HAVING COUNT(*) > 1;
```

### Documentos mayo sin ninguna línea de detalle

```sql
SELECT d.document_id, d.number, d.document_type_id, d.emission_date, d.state
FROM distribuidora.documents d
WHERE d.company_id = 3 AND d.office_id = 1
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
  AND NOT EXISTS (
    SELECT 1 FROM distribuidora.document_details dd WHERE dd.document_id = d.document_id
  )
ORDER BY d.emission_date, d.document_id
LIMIT 200;
```

### Company / office incorrectos en detalles (vía join)

```sql
SELECT COUNT(*) FROM distribuidora.document_details dd
INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
WHERE d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
  AND (d.company_id <> 3 OR d.office_id <> 1);
```

### Documentos anulados (referencia; columnas pueden variar)

```sql
SELECT d.document_id, d.number, d.state, d.commercial_state
FROM distribuidora.documents d
WHERE d.company_id = 3 AND d.office_id = 1
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
  AND (d.state IS NOT NULL OR d.commercial_state IS NOT NULL)
ORDER BY d.document_id
LIMIT 50;
```

---

## Métricas esperadas (logs)

- Inicio con rango de emisión y `overlap_days`.
- Resumen: documentos procesados, filas escritas en `document_details`, documentos con API sin líneas, primer llenado vs refresco, proxy de filas “sustituidas” (`sum(min(antes, después))` en refrescos), errores, duración.

**Interpretación:** `replace_document_details` siempre borra e inserta; “insertadas” en sentido SQL = `details_rows_written`. “Actualizadas” a nivel documento ≈ `documents_refreshed`; a nivel filas aproximado = `details_rows_replaced_proxy`.

---

## Volumen esperado

Depende del número de documentos mayo en BD y líneas por documento. Orden de magnitud:

- **API:** 1 llamada `details.json` por documento en rango.
- **Tiempo:** ~(0,15–0,45 s + extra) × N documentos + latencia Bsale; ejemplo 2.000 documentos ≈ varios minutos a decenas de minutos.

Use `BACKFILL_MAY_DETAILS_MAX_DOCUMENTS=50` para smoke test.

---

## Troubleshooting

| Síntoma | Acción |
|---------|--------|
| Lock ocupado | Terminar otro job que use `ADVISORY_LOCK_KEY` o esperar. |
| Muchos `docs_sin_lineas_api` | Normal si Bsale devuelve `items: []`; revisar anulados o tipos sin líneas. |
| Errores persistentes en un `document_id` | Revisar token y permisos; re-ejecutar con `BACKFILL_MAY_DETAILS_RESUME_AFTER_DOCUMENT_ID` por debajo del id problemático para reprocesar. |
| Conteo details bajo | Confirmar que FASE 7.5 cargó documentos mayo; filtro es por `emission_date` en `documents`. |
| `duplicate key` / violación PK | No esperado; ejecutar validación `detail_id` duplicado y revisar ingesta manual. |

---

## Qué no cubre esta fase

- `document_related` / relateddetailid.
- Live incremental ni cron.
- Cambios de frontend.
