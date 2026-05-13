# Runbook — Backfill `document_related` mayo 2026 (FASE 7.7)

**Alcance:** solo relaciones operacionales en `distribuidora.document_related` vía **`GET /v1/documents.json?relateddetailid=`** (y `details.json` por OC). **OC tipo 33**, `company_id = 3`, `office_id = 1`, emisión **UTC** por día calendario **2026-05-01 … 2026-05-31** inclusive.

**Requisitos:** FASE 7.5 (documents mayo) y 7.6 (`document_details` mayo) completos para que existan `detail_id` coherentes.

**No** cubre: estados OC finales en frontend, NC avanzada, cron, histórico fuera de mayo.

**Implementación:** `backfill_distribuidora_related_may_2026_only` en `backend/services/distribuidora/sync_related_service.py` (delega en `sync_related_documents_range`). Job:

```bash
python -m backend.jobs.backfill_related_may_2026
```

---

## Comando y entorno

Misma base que otros jobs: `PG_*`, `BSALE_TOKEN` o `BSALE_TOKEN_SPA`, `LOG_LEVEL`.

| Variable | Default | Descripción |
|----------|---------|-------------|
| `DISTRIBUIDORA_RELATED_API_DELAY_SEC` | `0.12` | Pausa entre llamadas API related/details. |
| `BACKFILL_MAY_RELATED_EXIT_CODE_ON_LOCK` | `1` | Código de salida si el advisory lock `related` está ocupado. |

---

## Comportamiento técnico

- **Advisory lock:** `ADVISORY_LOCK_RELATED` (no ejecutar otro sync related en paralelo).
- **DDL:** `ensure_distribuidora_schema` al inicio del rango (como hoy).
- **`sync_logs`:** proceso `backfill_related_may_2026`.
- **`sync_status`:** el rango inserta fila `sync_type='related'` al terminar OK; en skip/lock/error el backfill puede insertar fila adicional de error para trazabilidad.
- **`sync_state` operacional:** `sync_type='related'`, `mode='backfill'`, `office_id=1`, ventana mayo, `items_processed` = filas insertadas (nuevas).
- **Idempotencia:** `ON CONFLICT (detail_id, related_document_id) DO NOTHING`; contadores `related_insert_conflicts` / `related_insert_attempts` en stats.
- **Retry:** deadlock en insert por fila (`_with_deadlock_retry`); reintentos HTTP en `BsaleClient`.

---

## Validaciones SQL

### Filas related en mayo (vía OC y detalle)

```sql
SELECT COUNT(*)::bigint
FROM distribuidora.document_related dr
INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
WHERE d.company_id = 3 AND d.office_id = 1 AND d.document_type_id = 33
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01';
```

### Tipos relacionados permitidos (1, 6, 9 típico)

```sql
SELECT dr.related_document_type, COUNT(*) AS n
FROM distribuidora.document_related dr
INNER JOIN distribuidora.document_details dd ON dd.detail_id = dr.detail_id
INNER JOIN distribuidora.documents d ON d.document_id = dd.document_id
WHERE d.company_id = 3 AND d.office_id = 1
  AND d.emission_date >= TIMESTAMPTZ '2026-05-01'
  AND d.emission_date < TIMESTAMPTZ '2026-06-01'
GROUP BY 1 ORDER BY 2 DESC;
```

### Huérfanos: `detail_id` sin fila en `document_details`

```sql
SELECT COUNT(*) FROM distribuidora.document_related dr
WHERE NOT EXISTS (
  SELECT 1 FROM distribuidora.document_details dd WHERE dd.detail_id = dr.detail_id
);
```

### Duplicados lógicos (no debería)

```sql
SELECT detail_id, related_document_id, COUNT(*) AS c
FROM distribuidora.document_related
GROUP BY 1, 2 HAVING COUNT(*) > 1;
```

### Related cuyo documento relacionado no es company/office esperado (muestra)

```sql
SELECT dr.detail_id, dr.related_document_id, inv.company_id, inv.office_id
FROM distribuidora.document_related dr
LEFT JOIN distribuidora.v_documents_latest inv
  ON inv.document_id = dr.related_document_id
WHERE inv.document_id IS NOT NULL
  AND (inv.company_id <> 3 OR inv.office_id <> 1)
LIMIT 50;
```

---

## Métricas esperadas (logs / resumen job)

- Días procesados (31 en mayo pleno).
- OC procesadas, `details` recorridos (`relateddetail_details_processed`), ítems API (`relateddetail_items_total`).
- Filas nuevas insertadas (`rows_inserted`), intentos vs conflictos (`related_insert_attempts` / `related_insert_conflicts`).
- Llamadas API, `related_skipped_other_office`, duración.

Segunda corrida: muchos **conflictos** y pocas inserciones nuevas es normal.

---

## Validación ERP (manual)

- Listados / SQL que usan `document_related` + `v_documents_latest` para “facturada” (tipos 1/6) deberían mejorar tras este backfill.
- Sigue sin modelar “parcial” ni NC avanzada en el `CASE` de `orders_service` hasta fases posteriores.

---

## Troubleshooting

| Síntoma | Acción |
|---------|--------|
| Lock ocupado | Parar `sync_related` incremental u otro backfill; reintentar. |
| Pocas inserciones | Confirmar `document_details` mayo poblados; API sin `relateddetailid` para esa línea. |
| `omitido` sin token | Configurar `BSALE_TOKEN` / `BSALE_TOKEN_SPA`. |
| Timeouts | Subir un poco `DISTRIBUIDORA_RELATED_API_DELAY_SEC` o ejecutar por ventanas menores (no implementado en este job; usar API de rango manual si hace falta). |
| Huérfanos > 0 | Auditar ingesta previa o líneas borradas en BD; no borrar en este runbook. |

---

## Volumen esperado

- **API:** por cada `detail_id` de cada OC mayo, una o más páginas `documents.json?relateddetailid=`.
- **Tiempo:** proporcional a número de OC × líneas de detalle × throttle; puede ser largo (horas) con volumen alto.
