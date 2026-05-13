# Runbook — Backfill documents mayo 2026 (FASE 7.5)

**Alcance:** solo `distribuidora.documents`, `company_id = 3`, `office_id = 1`, emisión por día **UTC** del **2026-05-01** al **2026-05-31** (inclusive), con días opcionales previos por overlap. **No** escribe details, attributes, references ni sellers.

**Implementación:** `backfill_distribuidora_documents_may_2026_documents_only` en `backend/services/distribuidora/sync_service.py` y job `python -m backend.jobs.backfill_documents_may_2026`.

---

## Comando de ejecución

Desde la **raíz del repositorio** (con dependencias instaladas):

```bash
python -m backend.jobs.backfill_documents_may_2026
```

Requisitos: misma configuración que el API (`PG_*`, `BSALE_TOKEN` o `BSALE_TOKEN_SPA`).

---

## Variables de entorno

| Variable | Obligatoria | Default | Descripción |
|----------|-------------|---------|-------------|
| `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASSWORD` | Sí | — | PostgreSQL |
| `PG_PORT` | No | `5432` | Puerto |
| `BSALE_TOKEN` o `BSALE_TOKEN_SPA` | Sí | — | API Bsale |
| `LOG_LEVEL` | No | `INFO` | Nivel de logging |
| `BACKFILL_MAY_OVERLAP_DAYS` | No | `0` | Días **antes** del 2026-05-01 a incluir en la API (0–7). Útil para documentos con emisión en el borde; puede traer abril; usar con criterio. |
| `BACKFILL_MAY_EXTRA_PAGE_SLEEP_SEC` | No | `0` | Suma fija al jitter entre páginas `GET /documents.json` (throttle extra). |
| `BACKFILL_MAY_PAGE_LIMIT` | No | (25–50 desde `DISTRIBUIDORA_RESYNC_PAGE_LIMIT` o 50) | Tamaño de página API (25–50). |
| `BACKFILL_MAY_DAY_MAX_RETRIES` | No | `5` | Reintentos por **día** ante fallo (red/HTTP tras agotar reintentos internos de Bsale). |
| `BACKFILL_MAY_EXIT_CODE_ON_LOCK` | No | `1` | Si el advisory lock de documentos está ocupado: código de salida del job. |
| `BACKFILL_MAY_EXIT1_ON_DOC_ERRORS` | No | (vacío) | Si `1`/`true`/`yes`: salida `1` si hubo errores por documento o fallos de upsert. |

---

## Comportamiento técnico

- **Advisory lock:** `ADVISORY_LOCK_KEY` (mismo que sync incremental de documentos). No ejecutar en paralelo con otro sync que use ese lock.
- **HTTP retry:** reutiliza `_documents_get_resync` (502/503/504/500, red, 429).
- **Retry por día:** hasta `BACKFILL_MAY_DAY_MAX_RETRIES` con backoff corto.
- **Upsert / `ON CONFLICT`:** `upsert_documents` en `documents_repo.py` (folio o PK).
- **Solo documents:** flag interno `_documents_only_skip_children` evita `details.json`, `attributes.json`, `references.json`, sellers.
- **`sync_status`:** una fila `sync_type='documents'`, `status` `success` o `error`, `records_processed` = documentos procesados en la corrida.
- **`sync_state`:** `sync_type='documents'`, `mode='backfill'`, `office_id=1`, ventanas y `overlap_days`; éxito con `update_sync_state_success`, fallo con `update_sync_state_error`.
- **`sync_logs`:** proceso `backfill_documents_may_2026` con contadores al finalizar.

---

## Validaciones SQL (post-ejecución)

Conteos en ventana mayo (emisión UTC):

```sql
SELECT COUNT(*) AS n
FROM distribuidora.documents d
WHERE d.company_id = 3 AND d.office_id = 1
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01';
```

Duplicados lógicos (no debería haber más de una fila por folio):

```sql
SELECT company_id, office_id, document_type_id, number, COUNT(*) AS c
FROM distribuidora.documents
WHERE company_id = 3 AND office_id = 1
  AND document_type_id IS NOT NULL AND number IS NOT NULL
  AND emission_date >= TIMESTAMPTZ '2026-05-01'
  AND emission_date < TIMESTAMPTZ '2026-06-01'
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1;
```

Gaps de días sin ningún documento (cualquier tipo) en mayo:

```sql
WITH days AS (
  SELECT generate_series(date '2026-05-01', date '2026-05-31', interval '1 day')::date AS d
),
have AS (
  SELECT DISTINCT (emission_date AT TIME ZONE 'UTC')::date AS d
  FROM distribuidora.documents
  WHERE company_id = 3 AND office_id = 1
    AND emission_date >= TIMESTAMPTZ '2026-05-01'
    AND emission_date < TIMESTAMPTZ '2026-06-01'
)
SELECT d.d FROM days d LEFT JOIN have h ON h.d = d.d WHERE h.d IS NULL ORDER BY 1;
```

Office/company incorrectos en mayo:

```sql
SELECT COUNT(*) FROM distribuidora.documents d
WHERE d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
  AND (d.company_id <> 3 OR d.office_id <> 1);
```

---

## Métricas esperadas en logs

- Inicio con `company_id`, `office_id`, rango calendario y `overlap_days`.
- Por cada día OK: línea con `docs_proc` y acumulado de páginas API.
- Resumen final del job: días procesados, páginas API, documentos procesados, insertados/actualizados aproximados, errores, duración.

Los valores absolutos dependen del volumen Bsale; la segunda corrida debería mostrar **más actualizados que insertados** si los datos ya estaban cargados.

---

## Troubleshooting

| Síntoma | Acción |
|---------|--------|
| `omitido_concurrencia` / exit por lock | Esperar a que termine otro sync con el mismo advisory lock o ajustar ventana de mantenimiento. |
| `401` / `ValueError` sin token | Definir `BSALE_TOKEN` o `BSALE_TOKEN_SPA`. |
| Día repetido fallando | Revisar logs Bsale; subir `BACKFILL_MAY_DAY_MAX_RETRIES` o `BACKFILL_MAY_EXTRA_PAGE_SLEEP_SEC`; re-ejecutar el job (idempotente). |
| `relation "distribuidora.sync_process_cursor" does not exist` | Aplicar migraciones (`ensure_distribuidora_schema` / `013_operational_sync_state.sql`). |
| Conteos mayo bajos vs Bsale | Comprobar que `officeId` en API es 1 y que las fechas de emisión en Bsale son UTC coherentes con el rango. |
| ERP sin líneas OC | Este job **no** carga `document_details`; hasta FASE details, las pantallas que dependan solo de cabeceras pueden verse incompletas. |

---

## Qué no hace este runbook

- No programa cron ni Coolify.
- No backfill de details ni related.
- No histórico 2025 ni otros office.
