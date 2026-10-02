# backend/jobs/bsale_raw

Entrypoint **único** de `bsale_raw` (nunca un script por endpoint):

```bash
python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile
```

Fase 4A: sólo `--resource offices` y `--mode full-reconcile` (habilitación por `ResourceSpec.pipeline_enabled`).
No está programado en Coolify; se ejecuta manualmente con autorización.

- `--dry-run`: consulta la API, valida, pagina y calcula hashes; abre la BD en **sólo lectura** (no crea `sync_runs`, no toca `sync_state`, no toma lock). Imprime los conteos que *se aplicarían*.
- Salida: `key=value` compacta. Nunca imprime token ni payload.
- Exit: 0 SUCCESS, 1 FAILED, 2 PARTIAL, 3 lock ocupado (SKIPPED), 64 uso inválido.
- Un advisory lock por `(company, resource, scope)`; nunca dos corridas iguales en paralelo.
- Tokens sólo por entorno (`bsale_raw.sources.token_env`); requiere `PG_*` y la variable del token.
- No reemplaza a los syncs actuales hasta validar paridad (ver `docs/BSALE_SYNC_INDEX.md`).
