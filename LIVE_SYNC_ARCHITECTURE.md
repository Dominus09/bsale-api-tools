# Arquitectura sync live + on-demand (FASE 8.0)

## Estrategia

Dos capas complementarias (sin backfills mensuales automáticos ni truncate):

1. **Crons Coolify** — jobs incrementales con ventanas cortas y overlap.
2. **Botón frontend** — `POST /distribuidora/sync/live-now` ejecuta la misma cadena en una sola petición.

La fuente oficial de facturación sigue siendo `document_related` (relateddetailid). Los probables son capa analítica.

## Ventanas y overlap

| Job | `sync_type` | Ventana | Overlap default | Cron Coolify |
|-----|-------------|---------|-----------------|--------------|
| `live_sync_documents` | `documents_live` | 2 h | 900 s (15 min) | `*/5 * * * *` |
| `live_sync_details` | `details_live` | 24 h | 3600 s (1 h) | `*/15 * * * *` |
| `live_sync_related` | `related_live` | 3 días | 1 día | `*/20 * * * *` |
| `live_sync_probable_matches` | `probable_live` | 5 días | 1 día | `0 * * * *` |

Variables opcionales: `LIVE_SYNC_DOCUMENTS_WINDOW_HOURS`, `LIVE_SYNC_DETAILS_WINDOW_HOURS`, `LIVE_SYNC_RELATED_WINDOW_DAYS`, `LIVE_SYNC_PROBABLE_WINDOW_DAYS`, y `*_OVERLAP_*` homólogos.

## `sync_state` (incremental)

Tabla: `distribuidora.sync_state`  
Clave: `(sync_type, mode='incremental', office_id=1)`

- **Éxito:** `update_sync_state_success` — avanza `last_watermark`, `last_window_from/to`, limpia `error_summary`.
- **Fallo:** `update_sync_state_error` — **no** avanza watermark.
- **Concurrencia:** job retorna `omitido_concurrencia: true` sin tocar watermark.

## Advisory locks

| Lock | Valor | Uso |
|------|-------|-----|
| `ADVISORY_LOCK_DOCUMENTS_LIVE` | 5927184010 | Cron / paso documents |
| `ADVISORY_LOCK_DETAILS_LIVE` | 5927184011 | Cron / paso details |
| `ADVISORY_LOCK_PROBABLE_LIVE` | 5927184012 | Cron / paso probables |
| `ADVISORY_LOCK_RELATED` | 5927184005 | Related (existente) |
| `ADVISORY_LOCK_GLOBAL_LIVE_NOW` | 5927184019 | On-demand (cadena completa) |

## Comandos Coolify

```bash
python -m backend.jobs.live_sync_documents
python -m backend.jobs.live_sync_details
python -m backend.jobs.live_sync_related
python -m backend.jobs.live_sync_probable_matches
```

Raíz del repo, con `BSALE_TOKEN` / `PG_*` en entorno.

## Endpoint manual

`POST /distribuidora/sync/live-now`

Secuencia: documents → details → related → probable_matches.

Respuesta éxito:

```json
{
  "ok": true,
  "status": "completed",
  "started_at": "...",
  "finished_at": "...",
  "duration_seconds": 12.3,
  "documents": { ... },
  "details": { ... },
  "related": { ... },
  "probable_matches": { ... }
}
```

Si lock global ocupado (`409`):

```json
{
  "ok": false,
  "status": "already_running",
  "message": "Ya hay una sincronización en ejecución"
}
```

## Frontend

- Componente: `frontend/components/distribuidora/orders/LiveBsaleSyncPanel.tsx`
- API: `postDistribuidoraSyncLiveNow()`, `getDistribuidoraSyncStatus()` (incluye `live_sync`)
- Pantallas: `/distribuidora/ordenes-compra`, `/distribuidora/pre-planificacion`

UX: botón deshabilitado mientras corre, panel “Última actualización” por capa, refresh de tabla al terminar.

## Logs (stdout)

Cada job imprime bloque `LIVE SYNC * — SUMMARY` con ventana, contadores y duración.  
On-demand imprime `LIVE SYNC ON-DEMAND — SUMMARY`.

## Costos API (estimación)

| Job | Llamadas típicas |
|-----|------------------|
| Documents (2 h) | 1–N páginas `/documents.json` × 2 (OC + ventas) |
| Details (24 h) | 1× `/details.json` por documento en ventana BD |
| Related (3 d) | `details.json` + `relateddetailid` por OC (límite env related) |
| Probables | Solo BD (sin API Bsale) |

No polling agresivo: frecuencias mínimas indicadas en crons.

## Validación

1. Ejecutar una vez: `python -m backend.jobs.live_sync_documents`
2. Ver fila en `sync_state`:

```sql
SELECT sync_type, last_success_at, last_window_from, last_window_to, status
FROM distribuidora.sync_state
WHERE mode = 'incremental' AND office_id = 1
ORDER BY sync_type;
```

3. `POST /distribuidora/sync/live-now` desde UI o curl (con auth del ERP).
4. Confirmar OC reciente con estado actualizado en `/distribuidora/ordenes-compra`.

## Troubleshooting

| Síntoma | Causo probable | Acción |
|---------|----------------|--------|
| `omitido_concurrencia` | Otro cron del mismo tipo | Esperar o revisar lock |
| `already_running` (409) | On-demand en curso | Esperar fin de cadena |
| Watermark no avanza | Job falló | Ver `error_summary` en `sync_state` |
| Probables desactualizados | Cron horario no corrido | `live_sync_probable_matches` manual |
| Sin token | Env Coolify | `BSALE_TOKEN` / `BSALE_TOKEN_SPA` |

## Qué NO hace esta fase

- Backfill mayo 2026 automático
- Histórico 2025
- Multiempresa / otras offices
- Truncate ni reemplazo de jobs backfill existentes (`backfill_*_may_2026` siguen disponibles para uso manual)

## Archivos principales

| Archivo | Rol |
|---------|-----|
| `backend/services/distribuidora/live_sync_service.py` | Lógica live + on-demand |
| `backend/jobs/live_sync_*.py` | Entrypoints cron |
| `backend/routers/distribuidora_orders.py` | `POST /sync/live-now` |
| `frontend/components/distribuidora/orders/LiveBsaleSyncPanel.tsx` | UI |
