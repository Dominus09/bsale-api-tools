# backend/jobs/bsale_raw

Entrypoint **único** de `bsale_raw` (nunca un script por endpoint):

```bash
python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile
python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --office 1 --mode scanner --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --variant 10888 --office 1 --mode point --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --variant 10888 --mode point --dry-run
```

Fase 4D1: `--resource` ∈ `offices`, `taxes`, `document_types`, `product_types`, `price_lists`, `products`, `variants` (LIVE VALIDATED C3; sólo `--mode full-reconcile`) y `stocks`: `--mode scanner` (LIVE VALIDATED C3 office 1 / 4; no destructivo) o `--mode full-reconcile` (destructivo por sucursal), ambos con `--office` obligatorio; `--mode point` (fase 4D2, IMPLEMENTED / NOT YET LIVE VALIDATED) exige `--variant` y acepta `--office` opcional (sin él: todas las sucursales que devuelva Bsale). `--variant` sólo se acepta con `--mode point`; `--office` se rechaza en recursos que no son por sucursal. La salida de POINT agrega `variants=`, `no_rows=` y `failed_variants=`. Habilitación por `ResourceSpec.pipeline_enabled` / `pipeline_modes`; el resto se rechaza (exit 64). La salida incluye `scope=` (`global` u `office:<id>`).
No está programado en Coolify; se ejecuta manualmente con autorización.

- `--dry-run`: consulta la API, valida, pagina y calcula hashes; abre la BD en **sólo lectura** (no crea `sync_runs`, no toca `sync_state`, no toma lock). Imprime los conteos que *se aplicarían*.
- Salida: `key=value` compacta. Nunca imprime token ni payload.
- Exit: 0 SUCCESS, 1 FAILED, 2 PARTIAL, 3 lock ocupado (SKIPPED), 64 uso inválido.
- Un advisory lock por `(company, resource, scope)`; nunca dos corridas iguales en paralelo.
- Tokens sólo por entorno (`bsale_raw.sources.token_env`); requiere `PG_*` y la variable del token.
- No reemplaza a los syncs actuales hasta validar paridad (ver `docs/BSALE_SYNC_INDEX.md`).
