# backend/jobs/bsale_raw

Entrypoint **único** de `bsale_raw` (nunca un script por endpoint):

```bash
python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource offices --mode full-reconcile
python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --office 1 --mode scanner --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --variant 10888 --office 1 --mode point --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource stocks --variant 10888 --mode point --dry-run
python -m backend.jobs.bsale_raw sync --company 3 --resource documents --document <BSALE_DOCUMENT_ID> --mode point --dry-run
```

Fase 4D1: `--resource` ∈ `offices`, `taxes`, `document_types`, `product_types`, `price_lists`, `products`, `variants` (LIVE VALIDATED C3; sólo `--mode full-reconcile`) y `stocks`: `--mode scanner` (LIVE VALIDATED C3 office 1 / 4; no destructivo) o `--mode full-reconcile` (destructivo por sucursal), ambos con `--office` obligatorio; `--mode point` (fase 4D2, IMPLEMENTED / NOT YET LIVE VALIDATED) exige `--variant` y acepta `--office` opcional (sin él: todas las sucursales que devuelva Bsale). `--variant` sólo se acepta con `--mode point`; `--office` se rechaza en recursos que no son por sucursal. La salida de POINT agrega `variants=`, `no_rows=` y `failed_variants=`. Habilitación por `ResourceSpec.pipeline_enabled` / `pipeline_modes`; el resto se rechaza (exit 64). La salida incluye `scope=` (`global` u `office:<id>`).

Fase 4E1 (IMPLEMENTED / NOT YET LIVE VALIDATED): `--resource documents` sólo con `--mode point` y `--document <id>` obligatorio. `--document` es el **id técnico** Bsale del documento (el de `/v1/documents/{id}.json`), **no** el folio / `number`; no hay búsqueda por folio. Sólo OC tipo 33 (por `document_type.id`); otro tipo → FAILED sin escritura. `--document` se rechaza en otros recursos y `--variant` / `--office` se rechazan con documents (la sucursal del refresh de stock sale de la OC). Salida: `company`, `resource`, `scope=document:<id>`, `mode`, `document_type_id`, `office_id`, `details`, `references`, `sellers`, `attributes`, `change_kind`, `version_changed`, `previous_variants`, `current_variants`, `affected_variants`, `stock_refresh`, `requests`, `duration_ms`, `status` (+ `dry_run=true`). Nunca imprime token, PII, payload ni URLs PDF/XML. Exit: 0 SUCCESS, 1 FAILED, 2 PARTIAL (OC confirmada pero stock pendiente), 64 uso.
No está programado en Coolify; se ejecuta manualmente con autorización.

- `--dry-run`: consulta la API, valida, pagina y calcula hashes; abre la BD en **sólo lectura** (no crea `sync_runs`, no toca `sync_state`, no toma lock). Imprime los conteos que *se aplicarían*.
- Salida: `key=value` compacta. Nunca imprime token ni payload.
- Exit: 0 SUCCESS, 1 FAILED, 2 PARTIAL, 3 lock ocupado (SKIPPED), 64 uso inválido.
- Un advisory lock por `(company, resource, scope)`; nunca dos corridas iguales en paralelo.
- Tokens sólo por entorno (`bsale_raw.sources.token_env`); requiere `PG_*` y la variable del token.
- No reemplaza a los syncs actuales hasta validar paridad (ver `docs/BSALE_SYNC_INDEX.md`).

## scan-stocks (ciclo de stock por empresa)

```bash
python -m backend.jobs.bsale_raw scan-stocks --company 3 --dry-run
python -m backend.jobs.bsale_raw scan-stocks --company 3
```

`backend/services/bsale_raw/stock_cycle.py`: lee `bsale_raw.offices` (`state = 0`, `missing_since IS NULL`, orden por id) y ejecuta `run_stock_sync` en modo **SCANNER** por sucursal, en serie (`trigger='STOCK_CYCLE'`). Nunca FULL_RECONCILE ni DELETE.

- Lock de ciclo `(company, stocks, cycle)` en conexión dedicada: si otro ciclo de la empresa sigue corriendo → `status=SKIPPED`, exit 3, sin escanear. Cada sucursal mantiene su lock `office:<id>` (ocupado → esa sucursal SKIPPED). POINT no toma locks y no queda bloqueado.
- Una sucursal que falla no detiene a las siguientes.
- Exit: 0 SUCCESS (todas), 2 PARTIAL (alguna falló u omitida), 1 FAILED (ninguna completó, sin sucursales activas o `offices` ilegible), 3 SKIPPED (ciclo solapado), 64 uso.
- `--dry-run`: sin escrituras ni locks; conteos previstos.

## sync-nightly (metadata + catálogo)

```bash
python -m backend.jobs.bsale_raw sync-nightly            # todas las empresas activas de bsale_raw.sources
python -m backend.jobs.bsale_raw sync-nightly --dry-run
python -m backend.jobs.bsale_raw sync-nightly --company 3 [--company 1]
```

Orquestador único (`backend/services/bsale_raw/nightly.py`): por empresa, en serie, `taxes → document_types → product_types → offices → price_lists → products → variants`, cada uno con `run_entity_sync` FULL_RECONCILE (`trigger='NIGHTLY'`) y todas sus protecciones (fusible, frescura, `missing_since`, locks, `sync_runs`/`sync_state`). Escribe sólo `bsale_raw.*`.

- `products` no SUCCESS → `variants` = `SKIPPED_DEPENDENCY`. Ningún otro fallo bloquea. Lock ocupado = recurso FAILED.
- Empresa sin token / sin fuente → empresa FAILED, las demás continúan.
- Tras `document_types` SUCCESS lee (sólo lectura) `distribuidora.document_type_roles`: `NEW_UNCLASSIFIED_DOCUMENT_TYPE` (insertado en esta corrida, sin rol activo), `UNCLASSIFIED_DOCUMENT_TYPE` (conocido, informativo), `ROLE_METADATA_MISSING`, `ROLE_CODE_SII_DRIFT`. Nunca asigna ni modifica roles.
- "Nuevo" = `sync_run_id` de la corrida y `first_seen_at >= sync_runs.started_at` (reloj de BD). Reporta `NEW_PRICE_LIST` (id, nombre, estado) y conteos de products/variants nuevos.
- `--dry-run` no escribe y omite detección/validación de roles.
- Exit: 0 SUCCESS, 2 PARTIAL (algún recurso/empresa FAILED, SKIPPED_DEPENDENCY, tipo nuevo sin rol o drift de roles), 1 FAILED (sources ilegible, sin empresas activas o ninguna empresa con algún recurso SUCCESS), 64 uso.
- No incluye stocks, variant_prices, variant_costs, clients ni documentos. No está programado.
