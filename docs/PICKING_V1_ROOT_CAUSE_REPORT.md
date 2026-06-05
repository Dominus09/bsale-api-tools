# Picking V1 — causas raíz y correcciones

## 1. Logo PDF no aparecía

| Campo | Detalle |
|-------|---------|
| **Causa** | `fetch()` + `FileReader` fallaba silenciosamente; `doc.addImage` en `catch {}` vacío. jsPDF no carga URLs remotas directamente (no es ReportLab). |
| **Fix** | `frontend/lib/quillotana-logo-pdf.ts`: carga vía `Image` + `canvas` + `crossOrigin`, logs `[PICKING_PDF_LOGO]` con url, dimensiones, bytes, resultado. |
| **Fallback** | Texto "GRUPO QUILLOTANA" solo si falla la imagen. |
| **Asset** | `QUILLOTANA_LOGO_GRUPO_URL` en `quillotana-brand.ts` (4267×1215 PNG, ~84 KB). |

## 2. Categoría OTROS en HEINEKEN (7802100501196)

| Campo | Detalle |
|-------|---------|
| **Causa raíz** | `VARIANTS_JOIN` unía `v.bar_code = dd.variant_code`, pero `document_details.variant_code` guarda **SKU** (`variant.code`), no EAN. El JOIN a `variants`/`products`/`product_types` fallaba → `pm.product_type` NULL → `OTROS`. |
| **Fix generación** | JOIN por `dd.variant_id = v.bsale_id` OR barcode OR SKU; tipo `COALESCE(pm.product_type, pt.name, 'OTROS')`. |
| **Fix lectura** | `enrich_picking_product_rows` consulta Bsale (`products` + `product_types`) y `resolve_tipo_producto()` prioriza snapshot válido → PM → Bsale. |
| **SQL evidencia** | `backend/sql/diagnostics/picking_trace_barcode.sql` |

**Resultado esperado:** HEINEKEN → **CERVEZAS** (desde `product_types.name` en Bsale).

## 3. Nombre solo variante

| Campo | Detalle |
|-------|---------|
| **Causa** | SQL usaba `dd.variant_description` para **producto** y **variante**. |
| **Fix** | `PRODUCTO_EXPR = p.name`, `VARIANTE_EXPR = v.description`; enrich usa `format_product_display` con `product_name` + `variant_description`. |

**Resultado:** `HEINEKEN LATA 470 CC (SEC 24)`.

## 4. SKU vs EAN

| Campo | Detalle |
|-------|---------|
| **Causa** | `codigo_barras` en picking = `dd.variant_code` = SKU interno Bsale (`details_repo` línea 39). |
| **Fix** | `BARCODE_EXPR = COALESCE(v.bar_code, dd.variant_code)`; enrich fuerza `codigo_barras` desde `variants.bar_code`. |

## 5. Cajas con CEIL incorrecto

| Campo | Detalle |
|-------|---------|
| **Causa** | `effective_cajas()` y `effectiveBoxes()` usaban `Math.ceil` / `CEIL()`. |
| **Fix** | `unidades / units_per_box` redondeado a **2 decimales**. |
| **Pruebas** | `backend/tests/test_picking_display.py` (10/48=0.21, 18/6=3.00, 720/24=30.00). |

## 6. KPIs en página 2 = 0

| Campo | Detalle |
|-------|---------|
| **Causa** | `buildKpiLine` con `items=[]` en PDF cliente ignoraba ventas; en PDF producto `clients=[]` ponía 0 clientes/$. |
| **Fix** | `stablePickingKpiLine()` precalculado una vez; fusiona snapshot + `header.load_kpis` en todas las páginas. |

## Archivos modificados

- `backend/utils/picking_sql.py`
- `backend/services/distribuidora/dispatch_plan_service.py`
- `backend/utils/picking_display.py`
- `backend/tests/test_picking_display.py`
- `frontend/lib/quillotana-logo-pdf.ts`
- `frontend/lib/picking-display.ts`
- `frontend/lib/dispatch-plan-picking-pdf.ts`
- `backend/sql/diagnostics/picking_trace_barcode.sql`

## Validación barcode 7802100501196

```bash
psql "$DATABASE_URL" -f backend/sql/diagnostics/picking_trace_barcode.sql
pytest backend/tests/test_picking_display.py -q
```

Tras deploy:

1. Regenerar picking del plan (nuevas filas con JOIN corregido) **o** confiar en enrich al leer snapshot antiguo.
2. Exportar PDF producto → consola debe mostrar `[PICKING_PDF_LOGO] render_ok`.
3. Verificar CERVEZAS, nombre HEINEKEN completo, EAN 7802100501196, cajas 0.21 para 10 u / SEC 48.
4. Página 2+ debe repetir KPIs del plan (clientes, documentos, monto).

**Formato:** carta horizontal (`format: "letter"`, landscape).
