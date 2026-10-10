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

## scan-costs / refresh-costs (costos por variante)

```bash
python -m backend.jobs.bsale_raw scan-costs --company 3 --batch 1000 --dry-run
python -m backend.jobs.bsale_raw scan-costs --company 3 --batch 1000
python -m backend.jobs.bsale_raw refresh-costs --company 3 --variant 29567 --variant 8716 --dry-run
```

`backend/services/bsale_raw/core/cost_engine.py`: `GET /v1/variants/{id}/costs.json` por variante → `bsale_raw.variant_costs` (`(company_id, variant_id)`; el costo no es por sucursal).

- RAW guarda sólo lo que entrega Bsale: payload completo; `average_cost` = `averageCost` (costo NETO) y `total_cost` = `totalCost` como Decimal exacto; `history_count` / `last_admission_date` de `history`; `history_complete` siempre false. Nunca costo bruto ni impuestos: eso es de la capa de negocio.
- `scan-costs` (SCANNER): un lote de `bsale_raw.variants` (`missing_since IS NULL`, activas e inactivas) por `bsale_id`, con cursor en `sync_cursors` (`variant_costs/global/scanner`); al terminar la vuelta reinicia. Lock `(company, variant_costs, global)`: solapado → exit 3. `sync_state` agregado en scope `scanner`.
- `refresh-costs` (POINT): variantes explícitas, prioridad P0, sin lock ni cursor.
- UPSERT con frescura (`api_fetched_at`); nunca DELETE. Una variante con error no se escribe y no detiene el lote (PARTIAL, exit 2) y entra a la cola `retry` del cursor: se reintenta al inicio de las corridas siguientes (hasta 100 por corrida, 3 intentos; luego la retoma la vuelta). 404 / respuesta inválida = falla de la variante; 10 errores de API seguidos (5xx, 429 agotado, red) cortan el lote y el cursor queda en la última intentada. Caída total → FAILED sin avanzar; lote sólo con 404 → FAILED pero el cursor avanza (no se atasca).
- Ritmo propio `BSALE_RAW_COST_RPS` (default 2 rps; máx. 5) para no competir con `scan-stocks`.

## sync-prices / refresh-prices (precios por lista)

```bash
python -m backend.jobs.bsale_raw sync-prices --company 3 --dry-run
python -m backend.jobs.bsale_raw sync-prices --company 3 --price-list 12
python -m backend.jobs.bsale_raw sync-prices --company 3 --lists inactive --dry-run
python -m backend.jobs.bsale_raw refresh-prices --company 3 --variant 2 [--price-list 12] --dry-run
```

`backend/services/bsale_raw/core/price_engine.py`: `GET /v1/price_lists/{id}/details.json` paginado → `bsale_raw.variant_prices` (`(company_id, price_list_id, variant_id)`; `bsale_detail_id` es metadata). Requiere la migración `010_variant_prices_missing_since.sql`.

- RAW guarda sólo lo que entrega Bsale: payload completo; `variant_value` = `variantValue` y `variant_value_with_taxes` = `variantValueWithTaxes` como Decimal exacto. Nunca recalcula IVA, impuestos ni márgenes.
- `sync-prices`: lock `(company, variant_prices, sync-prices)` (solapado → exit 3) → refresca `price_lists` con el motor de entidades → barre las listas de `bsale_raw.price_lists` según `--lists` (`active` = `state 0`, default; `inactive`; `all`), nunca las ausentes en Bsale; `--price-list` limita dentro de esa selección. Si la metadata falla se usan las listas guardadas y la corrida queda como máximo PARTIAL, con el motivo en la salida.
- Por lista: lock `price_list:N`, su propio `sync_runs`, snapshot ESTRICTO (count estable, total exacto) sin transacción abierta, variante repetida o detalle repetido = lista inválida (no se elige uno), luego UNA transacción: UPSERT con frescura (`missing_since = NULL` al reaparecer) + `missing_since` para precios conocidos que el snapshot completo no trajo y no fueron refrescados después de su inicio. Fusible por lista `BSALE_RAW_MAX_MISSING_PCT_VARIANT_PRICES` (default 20 %; snapshot vacío con precios presentes = fusible). Nunca DELETE.
- Una lista con error no escribe nada y no afecta a las demás: SUCCESS (0) todas OK; PARTIAL (2) alguna falló o falló la metadata; FAILED (1) ninguna; SKIPPED (3) lock ocupado. Variantes ausentes de `bsale_raw.variants` se guardan igual y se informan como `uncatalogued`.
- `refresh-prices` (POINT): `details.json?variantid=V` por (lista, variante), prioridad P0, sin lock; sin `--price-list` usa las listas activas guardadas (no refresca metadata). 0 filas = `NO_ROWS`: no escribe precio 0 ni marca ausencia. Máx. 50 variantes.
- Los precios de listas inactivas no son vigentes: la capa de negocio filtra por `bsale_raw.price_lists.state = 0`.
- Ritmo propio `BSALE_RAW_PRICE_RPS` (default 2 rps; máx. 5).
- `--dry-run`: sin escrituras ni locks; la metadata se valida pero no se guarda, así que las listas se toman de lo ya guardado.

## sync-catalog (catálogo diario, sólo company 3)

```bash
python -m backend.jobs.bsale_raw sync-catalog --company 3 --dry-run --product-taxes-limit 50
python -m backend.jobs.bsale_raw sync-catalog --company 3 --skip-product-taxes
python -m backend.jobs.bsale_raw sync-catalog --company 3
python -m backend.jobs.bsale_raw sync-catalog --company 3 --product-taxes-source individual   # respaldo: 1 request por producto
```

`backend/services/bsale_raw/catalog_daily.py` ejecuta, en serie y sin otros recursos: `taxes` → `product_types` → `products` → `variants` (motor de entidades, FULL_RECONCILE, `trigger='CATALOG_DAILY'`) → `product_taxes` (`core/product_tax_engine.py`). Requiere la migración `011_product_taxes.sql`.

- Asociaciones: `variants.product{id}` y `products.product_type{id}` vienen en el mismo ítem del listado y se guardan como `product_id` / `product_type_id` (sin requests extra; payload completo intacto, incluidos `attribute_values{href}` y `costs{href}` de la variante). Sin `expand`, `products.product_taxes` trae sólo `{href}`; el paso `products` sigue así (su payload guardado no cambia).
- Dependencias: `variants` exige `products` SUCCESS; `product_taxes` exige `products` y `taxes` SUCCESS (si no → `SKIPPED_DEPENDENCY`). Una falla de `product_taxes` no afecta a `variants`. Cada recurso conserva su lock, run, fusible y `sync_state`.
- Lock de catálogo `(company, catalog, daily)`: otro `sync-catalog` en curso → exit 3 sin consultar nada.
- `product_taxes`, fuente `expand` (default, LIVE VALIDATED C3 con el probe: `EXPAND_COMPLETE`): snapshot estricto de `products.json?expand=[product_taxes]` (activos e inactivos; ~41 requests en C3 en vez de ~2.000). Productos y relaciones salen del mismo snapshot; cada nodo se valida (`items` lista, `count` == ítems, sin `next`/`offset`, ítems del mismo producto con `tax.id`, sin repetidos). Nodo ausente, sin expandir, incompleto o inválido → `GET /v1/products/{id}/product_taxes.json` para ese producto (tope 100 por corrida; el resto queda "no consultado", PARTIAL). Snapshot fallido (HTTP, truncado, `count` inestable, ids repetidos) → FAILED sin escribir relaciones ni marcar ausencias. Payload = nodo expandido tal cual (`last_source='FULL_RECONCILE'`) o páginas del endpoint individual (`last_source='POINT'`). Fuente `individual`: un request por producto vigente de `bsale_raw.products` (respaldo explícito).
- Ambas fuentes: `tax_ids` en el orden de Bsale; `items_count = 0` = sin impuestos confirmado. 404 / error / paginación incompleta = falla (nunca lista vacía), registrada en `sync_cursors` (`product_taxes/global/failures`) y reintentada en la corrida siguiente; la fila anterior queda intacta. `tax.id` ausente de `bsale_raw.taxes` se guarda igual y deja la corrida PARTIAL. Producto ausente del listado expandido (o de `bsale_raw.products` en modo `individual`) → `missing_since` (fusible `BSALE_RAW_MAX_MISSING_PCT_PRODUCT_TAXES`, default 20 %). Escritura en lotes de 200 (transacciones cortas, nunca durante HTTP): una caída a mitad conserva lo ya guardado. Sin cálculos de IVA, ILA, factor ni montos.
- Cuota: limitador propio `BSALE_RAW_PRODUCT_TAX_RPS` (default 1 rps, máx. 2); se corta ante 5 respuestas 429 o 10 errores de API seguidos (los demás procesos — stock, costos, precios, legacy — tienen limitadores independientes).
- Exit: 0 SUCCESS; 2 PARTIAL (algún recurso falló, quedó PARTIAL u omitido por dependencia); 1 FAILED (ninguno); 3 SKIPPED (catálogo solapado); 64 uso (`--company` ≠ 3, `--product-taxes-limit` sin `--dry-run`, `--product-taxes-source` con `--skip-product-taxes`).
- `--dry-run`: sin escrituras ni locks. `--product-taxes-limit N` (sólo dry-run): con `expand` lee sólo las páginas necesarias para los primeros N productos del listado y NO evalúa ausencias (`missing_evaluated=False`); con `individual`, los primeros N productos de `bsale_raw.products`.

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
