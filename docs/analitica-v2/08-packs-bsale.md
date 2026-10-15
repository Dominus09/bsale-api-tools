# Analítica V2 — Rentabilidad de packs Bsale

Estado: implementado localmente, **en validación**. No publicado.

## Fuente

- Composición: `bsale_raw.products.payload -> 'pack_details'` de productos con `classification = 3` y `missing_since IS NULL`.
  Es la única lectura de `payload` permitida en Analítica V2 (`PACK_DETAILS_SQL`); el test de SQL lo restringe a esa expresión.
- No se crean sincronizaciones nuevas, no se modifica BSALE RAW, impuestos, precios, costos, reglas comerciales ni crons.

## Cálculo (`services/analytics_v2/pack_engine.py`, puro)

Para cada componente: costo neto del componente (base elegida: promedio Bsale o último comparable, sin sustitución entre
bases) × cantidad, con los **impuestos del propio componente** (IDs Bsale, motor tributario V2). Se suman neto y bruto.

- Nunca se usa el `averageCost` del SKU del pack ni los impuestos asociados al pack (ej. packs con dos ILA).
- Los componentes se identifican solo por ID de variante; un detalle que trae solo el producto queda pendiente
  (`pack_abierto`). Nunca se deducen por nombre o categoría.
- Recargo % = (precio bruto / costo bruto − 1) × 100; margen sobre venta = (precio − costo) / precio × 100.

### Estados del pack (`PackCostStatus`)

| Estado | Significado |
| --- | --- |
| `calculado` | Todos los componentes con costo e impuestos resueltos |
| `sin_composicion` | `pack_details` ausente, vacío o no expandido |
| `composicion_no_interpretable` | Estructura o cantidades inválidas |
| `pack_abierto` | Algún detalle con `multipleVariant` o sin variante determinada |
| `ciclo_detectado` | Un pack se contiene a sí mismo directa o indirectamente |
| `componente_inexistente` | Variante inexistente o retirada en RAW |
| `componente_pack_no_calculable` | Pack anidado no calculable o anidamiento > 4 niveles |
| `componente_requiere_revision_tributaria` | Impuestos del componente sin resolver |
| `componente_sin_costo` | Algún componente sin costo en la base elegida |

Solo `calculado` produce margen; el resto queda como `pack_no_calculable` en Márgenes V2 (sin margen definitivo).

## Integración

- **Costos V2**: `is_pack`, bloque `pack` y `commercial_gross_cost` por componentes; filtro `kind=individual|pack`.
  Los packs se excluyen de la cobertura tributaria estricta de productos individuales (bloque `packs` aparte).
- **Márgenes V2**: el costo de referencia del pack es la suma de componentes; las alertas comerciales se mantienen.
  Solo una promoción o remate aprobado y verificado justifica una excepción.
- **Dashboard V2**: bloque `packs` (permiso de márgenes) y avisos `packs_con_costo_pendiente` / `bloque_packs_no_disponible`.
- **Promociones V2**: pestaña informativa "Packs Bsale" que distingue pack Bsale, descuento Bsale, oferta mensual ERP y remate ERP.

## API (permiso de márgenes)

- `GET /analytics-v2/packs/summary?company_id&cost_basis`
- `GET /analytics-v2/packs/rows?company_id&q&price_list_id&pack_status&margin_status&problem&pack_state&cost_basis&sort&page&page_size`
- `GET /analytics-v2/packs/{variant_id}?company_id&cost_basis`

## Problemas detectados

Precio bajo costo, bajo política, pack no calculable, componentes faltantes o sin costo, costo de componente desactualizado
o en revisión, alza de costo de componente, componente inactivo, detalle inactivo excluido y diferencia extrema entre
listas (precio máximo / mínimo ≥ 2). La diferencia entre listas **se confirma en BSALE RAW antes de clasificarla como
anomalía**; nunca se cambian precios automáticamente.

## Auditoría (pgAdmin, solo SELECT)

`backend/sql/analytics_v2/checks/bsale_pack_composition_audit.sql`, bloques C0–C8: estructura de `pack_details`,
distribución de componentes, validación de detalles, estado estimado por pack, anidamiento/ciclos, packs no calculables,
frescura, ejemplo 5646 y caso 8171 (precios RAW listas 12/13/16).

## Pendiente

- Rentabilidad histórica de ventas de packs (no implementada).
- Confirmar la forma exacta del JSON `pack_details` con el bloque C0 (el parser es tolerante a las variantes conocidas).
- `TAX_CONFIG_VERIFIED = False`: todos los costos brutos son preliminares.
- Packs abiertos (`multipleVariant`) quedan pendientes: requieren la variante vendida en cada documento.
