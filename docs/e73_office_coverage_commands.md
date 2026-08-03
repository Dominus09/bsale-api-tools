# E.7.3 — Comandos preparados para ampliar cobertura V2 por oficina

**No ejecutar en producción desde Cursor.** Dry-run / staging primero.

Prioridad futura:

1. Bodega Central (`office_id=1`)
2. Supermercado (`office_id=3`) — ya disponible
3. Q1 (`office_id=4`)
4. Q2 (`office_id=5`)

Empresa: `company_id=3`. Versión: `cost-v2.0.0`.

## 1) Conteo de recepciones por oficina (histórico)

```bash
# Ajustar al script/diagnóstico existente del repo
python -m backend.scripts.diagnose_cost_receptions_sync \
  --company-id 3 \
  --office-id 1 \
  --detail-sample-limit 5
```

Repetir para `--office-id 4` y `5`.

## 2) Candidatos V2 / dry-run calculado (preparado)

```bash
# Ejemplo — usar el job/script de sync calculado del proyecto con --dry-run
# (nombre exacto según CLI vigente en backend/jobs o backend/scripts)
python -m backend.jobs.sync_cost_reception_calculated \
  --company-id 3 \
  --office-id 1 \
  --dry-run \
  --max-receptions 200
```

## 3) Piloto catchup Bodega Central

```bash
python -m backend.jobs.sync_cost_reception_calculated \
  --company-id 3 \
  --office-id 1 \
  --date-from 2025-01-01 \
  --date-to 2026-08-01 \
  --dry-run \
  --confirm-reception-count
# Solo tras revisión humana:
# ... mismas flags con --apply
```

## 4) Job incremental futuro

Hoy el incremental puede estar anclado a una oficina. Preparar recorrido de oficinas activas de control:

```text
for office_id in COST_CONTROL_OFFICE_IDS_BY_COMPANY[company_id]:
    sync_calculated(company_id, office_id, since=watermark[office_id])
```

No hardcodear “siempre 4”; leer `COST_CONTROL_OFFICE_IDS_BY_COMPANY` / `bsale.offices` activas.

## 5) Verificación post-cobertura

```sql
SELECT office_id, COUNT(*) AS v2_rows,
       COUNT(*) FILTER (WHERE corrected_gross_cost IS NOT NULL) AS calculable
FROM analytics.cost_reception_calculated
WHERE company_id = 3
  AND calculation_version = 'cost-v2.0.0'
GROUP BY office_id
ORDER BY office_id;
```

Cuando `offices_with_v2_coverage >= 2`, el KPI “Diferencias entre oficinas” deja de mostrar “Sin comparación todavía”.
