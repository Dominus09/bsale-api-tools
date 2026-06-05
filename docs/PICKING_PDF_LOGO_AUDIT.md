# Auditoría logo corporativo — Picking PDF

**Fecha:** generado automáticamente desde el repositorio.  
**Alcance:** `frontend/public`, assets, branding, pantallas de carga, menú, PDFs.

## Resumen ejecutivo

El logo corporativo **oficial para documentos** (login, OC, pre-despacho, picking PDF) **no está en `frontend/public`**. Vive en **Vercel Blob Storage** y se centraliza en `frontend/lib/quillotana-brand.ts`.

El menú lateral y la home usan un **emblema auxiliar distinto** (más pesado, orientado a UI).

---

## Ranking de candidatos

### A) Logo Grupo Quillotana PS (recomendado para PDF)

| Campo | Valor |
|-------|--------|
| **Ruta en repo** | `frontend/lib/quillotana-brand.ts` → constante `QUILLOTANA_LOGO_GRUPO_URL` |
| **URL asset** | `https://hebbkx1anhila5yf.public.blob.vercel-storage.com/GRUPO%20QUILLOTANA%20PS-fK4da0sPbUwnmEpeEVmmumWdj977f0.png` |
| **Tipo** | PNG |
| **Dimensiones** | 4267 × 1215 px (ratio ~3.5:1, horizontal) |
| **Tamaño** | ~84 KB |
| **Usos actuales** | Login (`app/login/page.tsx`), OC compras (`components/compras/oc-invoice.tsx`), overlay pre-despacho (`PreDespachoLoadingOverlay.tsx`), **Picking PDF** (`lib/dispatch-plan-picking-pdf.ts`) |
| **PDF** | **Mejor candidato** — ya integrado vía `quillotanaLogoDataUrl()`; proporción ideal para cabecera landscape |

### B) Emblema auxiliar sin sucursal (UI / menú)

| Campo | Valor |
|-------|--------|
| **Ruta** | URL directa (no en `public/`) |
| **URL** | `https://hebbkx1anhila5yf.public.blob.vercel-storage.com/Emblema%20auxiliar%20sin%20sucursal-3muphOJR8q7mpoZPwKQhJb7RbLYvdu.png` |
| **Tipo** | PNG |
| **Dimensiones** | 5484 × 4082 px (casi cuadrado) |
| **Tamaño** | ~260 KB |
| **Usos** | Sidebar (`components/layout/sidebar.tsx`), home (`app/home-client.tsx`), selector empresa (`app/company-selector/page.tsx`) |
| **PDF** | No recomendado — muy pesado y ratio vertical; pensado para avatar/emblema en UI |

### C) `frontend/public/icons/base.png`

| Campo | Valor |
|-------|--------|
| **Ruta** | `frontend/public/icons/base.png` |
| **Tipo** | PNG |
| **Dimensiones** | 1376 × 768 px |
| **Tamaño** | ~862 KB |
| **Usos** | Sin referencias grep en componentes TSX (posible asset legacy / PWA) |
| **PDF** | No recomendado — archivo grande, uso no documentado en ERP |

### D) Placeholders en `frontend/public`

| Archivo | Dimensiones | Tamaño | Uso |
|---------|-------------|--------|-----|
| `placeholder-logo.png` | 256 × 144 | 568 B | Plantilla Next/shadcn, no branding real |
| `placeholder-logo.svg` | — | 3.2 KB | Plantilla |
| `icon-dark/light-32x32.png` | 32 × 32 | ~580 B | Favicon |
| `icon.svg` | — | 21 KB | Favicon |

---

## Búsqueda por palabras clave

| Término | Hallazgos relevantes |
|---------|---------------------|
| `logo` | `quillotana-brand.ts`, `oc-invoice.tsx`, `PreDespachoLoadingOverlay`, `dispatch-plan-picking-pdf.ts` |
| `quillotana` / `grupo-quillotana` | `quillotana-brand.ts`, títulos layout, login |
| `brand` / `branding` | `quillotana-brand.ts` |
| `loading` / `spinner` | `PreDespachoLoadingOverlay` usa logo A |
| `splash` / `header` | Sin splash screen; header usa dropdown usuario sin logo |

---

## Diagnóstico «Sin tipo» en picking producto

### Cadena de datos

1. **Generación en vivo** (`dispatch_plan_service.py`): `PM_TIPO_PRODUCTO_EXPR` en `picking_sql.py`  
   `COALESCE(NULLIF(BTRIM(pm.product_type), ''), 'Sin tipo')`
2. **JOIN** `bsale.products_master pm` por barcode (`variant_code`).
3. Si **no hay fila en PM** o **`product_type` NULL/vacío** → cae en `'Sin tipo'`.
4. El valor se **persiste en snapshot** (`dispatch_plan_picking_products.tipo_producto`).
5. **Enriquecimiento al leer** (`picking_display.enrich_picking_product_rows`): solo rellena `tipo_producto` desde PM si el snapshot viene vacío y PM tiene `product_type`.

### Causas típicas en producción

- Producto facturado con barcode que **no está en `products_master`**.
- Fila en PM **sin `product_type`** (sync Bsale no pobla ese campo en PM; viene del UPSERT de `product_types.name` solo en refresh).
- Snapshot generado **antes** de tener PM actualizado.

### Regla aplicada (presentación + nuevas generaciones)

- Mostrar **`OTROS`** en lugar de `Sin tipo` / vacío.
- SQL default y normalización en lectura/PDF sin alterar estructura de snapshots.

### SQL diagnóstico (ejecutar en PG)

```sql
-- Productos en último picking con «Sin tipo» y causa
SELECT
    pp.codigo_barras,
    pp.producto,
    pp.tipo_producto AS tipo_snapshot,
    pm.product_type AS tipo_pm,
    pt.name AS tipo_bsale_variant
FROM distribuidora.dispatch_plan_picking_products pp
LEFT JOIN bsale.products_master pm ON pm.barcode = BTRIM(pp.codigo_barras)
LEFT JOIN bsale.variants v
    ON v.company_id = 3 AND BTRIM(v.bar_code) = BTRIM(pp.codigo_barras)
LEFT JOIN bsale.products p
    ON p.company_id = v.company_id AND p.bsale_id = v.product_id
LEFT JOIN bsale.product_types pt
    ON pt.company_id = p.company_id AND pt.bsale_id = p.product_type_id
WHERE pp.tipo_producto IS NULL
   OR BTRIM(pp.tipo_producto) = ''
   OR pp.tipo_producto = 'Sin tipo'
LIMIT 50;
```

---

## Recomendación PDF (sin subir logo nuevo)

Seguir usando **candidato A** vía `QUILLOTANA_LOGO_GRUPO_URL` en `quillotana-brand.ts`. No copiar a `public/` salvo requisito de offline; el fetch + cache base64 en PDF ya funciona.
