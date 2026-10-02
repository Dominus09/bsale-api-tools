# backend/jobs/bsale_raw

**Fase 1: sin jobs.** Esta carpeta reserva los entrypoints que se programarán en Coolify cuando existan las migraciones `bsale_raw` y el motor esté implementado y probado.

Entrypoints previstos (uno por *modo*, nunca uno por endpoint):

| Entrypoint futuro | Modo | Qué hace |
|---|---|---|
| `python -m backend.jobs.bsale_raw.run --mode incremental --resource documents` | INCREMENTAL | Ventana `emissiondaterange` con solape; prioridad empresa 3 / tipo 33. |
| `python -m backend.jobs.bsale_raw.run --mode full --resource <r>` | FULL_RECONCILE | Barrido completo de un recurso por empresa con fusible. |
| `python -m backend.jobs.bsale_raw.scan_stock` | SCANNER | Escáner continuo de stock por empresa/sucursal con checkpoint. |
| `python -m backend.jobs.bsale_raw.process_webhooks` | WEBHOOK | Worker del inbox `bsale_raw.webhook_events`. |

Reglas:

- Empresas independientes: resultado `SUCCESS` / `PARTIAL` / `FAILED` (exit 0 / 2 / 1; 3 = lock ocupado), igual que `backend/services/bsale/catalog_job.py`.
- Un advisory lock por `(modo, recurso)`; nunca dos corridas iguales en paralelo.
- Tokens sólo por entorno (`BSALE_TOKEN_Mini`, `BSALE_TOKEN_Romero`, `BSALE_TOKEN_SPA`).
- No reemplazan a los syncs actuales hasta que se valide paridad (ver `docs/BSALE_SYNC_INDEX.md`).
