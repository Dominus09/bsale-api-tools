# Catálogo Unificado V2 — Preview local (etapa lectura)

**Estado:** implementación local preview. **Sin push / redeploy / SQL productivo / Coolify / crons.**  
**GET `/api/catalog` productivo:** sin cambios.

## 1. Código

| Pieza | Ruta |
|-------|------|
| Proyección | `backend/services/catalog_v2/projection.py` |
| Router preview | `backend/routers/catalog_v2_preview.py` |
| Registro app | `backend/main.py` → `/api/catalog-v2/*` |
| Tests | `backend/tests/test_catalog_v2_projection.py` |

### Endpoints internos (require_management_access)

- `GET /api/catalog-v2/preview?price_list=factura|melinka&office_id=1&limit=&q=&in_stock=`
- `GET /api/catalog-v2/compare?price_list=factura&q=harina|sidra|sol&variant_ids=`
- `GET /api/catalog-v2/integrity/raw-sync`
- `GET /api/catalog-v2/coverage/upb`

## 2. Contrato de datos (compatible público + metadatos v2)

Campos públicos (paridad con `GET /api/catalog`):

`id`, `name`, `type`, `barcode`, `price`, `stock`, `image`, `units_per_box`, `sale_type`, `quantity_step`

Bloque `v2` (solo preview): sku, price_net, timestamps RAW, stock_all_offices, PM logística (`peso_caja_kg`←`weight_box_kg`, dimensiones), `missing_upb`, `active`.

**No se exponen:** costos, márgenes, secretos.

**Join:** `bsale_raw.variants.bsale_id` (= variant_id). PM por `variant_id` preferente, barcode solo como fallback LATERAL (1 fila). Nunca por nombre.

## 3. Comparación RAW vs legacy (muestra ≥20)

Fuente lectura MCP: `catalog_view` + `bsale_raw.stocks` office 1 + PM.  
(Nota: rol MCP **sin SELECT** a `bsale_raw.products` / `variant_prices`; la proyección V2 completa requiere credencial app.)

| variant_id | Producto | price_13 | stock legacy | stock RAW o1 | UPB | peso kg | stock_cmp |
|------------|----------|----------|--------------|--------------|-----|---------|-----------|
| 28216 | Harina Mariposa 25 kg | 20990 | 227 | 503 | 1 | 25 | **diff** |
| 28258 | Harina TG 25 kg | 20990 | 31 | 27 | 1 | 25 | **diff** |
| 30814 | Harina Rahue 25 kg | 19270 | 22 | 2 | 1 | 25 | **diff** |
| 30293 | Harina Kunstmann Integral 25 kg | 25624 | 0 | 0 | 1 | 25 | ok |
| 29094 | Sidra Real Blanca 750 cc | 4080 | 4827 | 3392 | 6 | 8 | **diff** |
| 23312 | Sidra Real Blanca 910 cc | 4220 | 0 | 0 | 6 | 9.23 | ok |
| 29289 | Sidra Real Citrus 750 | 3990 | 0 | 0 | 6 | 8 | ok |
| 24709 | Sidra Real Etiqueta Negra 750 | 4365 | 0 | 0 | 6 | 9.23 | ok |
| 31199 | Sol Pack 300 cc x18 | 12800 | 324 | 243 | (SEC/PM null) | — | **diff** |
| 19577 | Cerezas Ambrosoli (SEC 18) | 3914 | 0 | 0 | 18 | — | ok |
| 31339 | Halloween Ambrosoli (SEC 18) | 1740 | 90 | 0 | — | — | **diff** |
| 30931 | Exportación Selecto 1.5 L | 3590 | 114 | 114 | null | 12.8 | ok |
| 31366–31373 | stock0 recientes | price_13 OK | 0 | 0 | — | — | ok |

**Causa principal de diffs de stock:** `catalog_view` desfasado vs `bsale_raw.stocks` (fetched 2026-11-24 ~15:07 UTC). No invalidar RAW solo por timestamp; sí preferir RAW office_id=1 para catálogo ruta al cutover.

## 4. Cobertura UPB (PM activo)

| Métrica | Valor |
|---------|------:|
| PM activos | 7602 |
| Con UPB (>0) | 6604 |
| Sin UPB | **998** (~13.1%) |
| Con peso caja | 1238 |
| RAW variants activas | 6253 |

## 5. Errores parciales RAW (`sync_state`)

Patrón dominante: `status=SUCCESS` + `last_error` no vacío + **`last_error_at` < `last_success_at`** → residual histórico (401/timeout previos; ciclo posterior OK).

Ejemplos stale: stocks offices 1–6, price_list 13/16, products/product_types/taxes (401 del 23-nov; success 24-nov).

Atención actual:

- `variant_costs` / `scanner` → **RUNNING** + error 401 residual.
- `variant_prices` price_list:12 tuvo SnapshotValidationError (count cambió); success posterior.

**Control propuesto (no implementado en crons):** marcar `VALIDATED` solo si `last_error IS NULL` **o** `last_error_at < last_success_at`, status≠RUNNING, y `rows_received` estable. Endpoint: `/api/catalog-v2/integrity/raw-sync`.

## 6. Diseño transición Products Master

| Desde RAW | Conservar manual en PM |
|-----------|-------------------------|
| Identidad product/variant, barcode, sku, nombre, tipo, state | `units_per_box` |
| Precios por lista, taxes | `weight_box_kg`, `height_cm`, `width_cm`, `length_cm` |
| Stock por oficina | `sale_type`, `quantity_step`, `logistics_completed`, supplier |

**Dependientes PM hoy:** `GET /api/catalog`, picking/peso (`order_weight_service`), dispatch plan, delivery seguimiento, admin products_master.

**Futuro (sin borrar tabla):** PM = overlay logístico keyed por `variant_id`; dejar de duplicar nombre/precio/stock; SEC como fallback UPB solo lectura.

**Picking/peso:** no cambiar cálculos en esta etapa. Riesgo cutover: stock office1 distinto a catalog_view → cantidades picking visibles cambian si se lee stock V2 prematuramente.

## 7. Clientes / OC / Ventas V2

- `sync_clients`: conservar; **pendiente confirmar tarea Coolify** (no tocar).
- OC / planificaciones / `documents_orders`: **no tocados**.
- `bsale_sales_v2`: arquitectura **aprobada**, sin escritura productiva en esta etapa.

## 8. Riesgos

1. Stock ruta: legacy vs RAW office1 diverge (ej. Sidra 750: 4827→3392).
2. ~998 variantes sin UPB estructurado → SEC/heurística o `missing_upb`.
3. Imágenes: seguir `{barcode}.webp`; variantes sin foto quedan `image=null`.
4. MCP/readonly incompleto sobre `bsale_raw.products|variant_prices` → validar preview completo con credencial app antes de cutover.
5. Activar V2 en público sin feature flag rompería expectativas de stock.

## 9. Plan de activación reversible (pendiente aprobación)

1. Desplegar preview endpoints (solo gerencia).
2. Validar muestra 50–100 SKU con compare + firma gerencial.
3. Feature flag `CATALOG_SOURCE=legacy|v2` detrás de `/api/catalog` (default legacy).
4. Canary ruta/web; rollback = flag→legacy (sin DROP).
5. Luego picking lee stock/precio RAW vía misma proyección; PM solo logística.

## 10. Tests / SHA

- `pytest backend/tests/test_catalog_v2_projection.py` → **5 passed**
- SHA: ver commit local de esta entrega (no push).
