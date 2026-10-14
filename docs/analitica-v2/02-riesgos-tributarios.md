# Analítica V2 — Motor tributario y costo bruto comercial

Estado: **implementación local en validación**. `TAX_CONFIG_VERIFIED = False`: todo costo bruto y
todo margen calculado con él es **provisional** hasta que contabilidad contraste la configuración.

Motor único: `backend/services/analytics_v2/tax_profile.py`. Lo usan Control de Costos V2,
Márgenes V2 y Dashboard V2; ninguna pantalla calcula impuestos por su cuenta.

## Fórmula

```
COSTO_BRUTO_COMERCIAL = COSTO_NETO × (1 + (IVA + ILA) / 100)
```

- IVA e ILA se aplican sobre la **misma base neta**: 13.950 con IVA 19 % + ILA 20,5 % = 19.460,25
  (no 13.950 × 1,19 × 1,205). 15.050 con IVA 19 % + ILA 31,5 % = 22.650,25.
- Las tasas salen de `bsale_raw.taxes.percentage`; nunca de constantes del código.
- Decimal sin redondeos intermedios; el JSON informa el valor exacto como string.
- Base: `variant_costs.average_cost` (promedio Bsale), que **no se modifica**. Se expone neto, IVA
  aplicado, ILA aplicado, factor, bruto, estado y motivo (`commercial_gross_cost`).
- El costo de recepción (`stock_reception_details.cost`) también se lleva a bruto
  (`gross_cost_preliminary`), pero no puede considerarse validado mientras `NET_COST_VERIFIED = False`.

## Clasificación de cada impuesto

Fuente: `bsale_raw.taxes` de la misma empresa (`code`, `percentage`, `state`, `missing_since`).

| Código SII (`code`) | Tipo | Uso | Tasa legal (solo control) |
|---|---|---|---|
| 14 | IVA | Comparación comercial | 19 |
| 24 | ILA licores/destilados | Comparación comercial | 31,5 |
| 25 | ILA vinos | Comparación comercial | 20,5 |
| 26 | ILA cervezas | Comparación comercial | 20,5 |
| 27 | ILA analcohólicas | Comparación comercial | 10 |
| 271 | ILA analcohólicas alto azúcar | Comparación comercial | 18 |
| 17, 18 | IVA anticipado carne | Anticipado contable | 5 |
| 19 | IVA anticipado harina | Anticipado contable | 12 |

**El tipo de impuesto se identifica solo por el ID Bsale y su metadata** (código SII de
`bsale_raw.taxes`). Nunca por el nombre del impuesto, la categoría, el tipo de producto, palabras
clave (cerveza, bebida, vino, licor) ni porcentajes asignados por categoría: en una misma categoría
conviven ILA 10 % y 18 %, y solo `product_taxes.tax_ids` de cada producto decide cuál aplica.
El nombre del impuesto se muestra únicamente como referencia visual.

- La tasa usada es siempre `taxes.percentage` del ID; el `tax_id` nunca se interpreta como tasa.
- Código desconocido (incluidas retenciones) → no clasificado → `requiere_revision_tributaria`.
- Sin código SII: no se clasifica, salvo que contabilidad confirme el tipo de ese ID en
  `CONFIRMED_TAX_ROLES[empresa][tax_id]` (`contracts.py`, vacío por defecto). Queda la observación
  `clasificado_por_id_confirmado`; si la tasa RAW no es legal para el tipo confirmado
  (`tasa_no_legal_para_tipo`) o un código SII contradice la confirmación
  (`codigo_sii_contradice_id_confirmado`), el impuesto bloquea.
- Tasa distinta a la legal del código, tasa nula, impuesto inactivo (`state ≠ 0`) o retirado de Bsale
  (`missing_since`) → impuesto bloqueante.
- El catálogo se construye por empresa: un mismo ID en otra empresa es otro impuesto.

> Los códigos SII asignados por Bsale a cada impuesto de la empresa 3 **no se han podido leer** desde
> el MCP (sin privilegio SELECT sobre `bsale_raw.taxes`). Deben confirmarse con la consulta 1 de
> `backend/sql/analytics_v2/checks/cost_gross_tax_validation.sql`.

## Perfil tributario por producto

Fuente: `bsale_raw.product_taxes.tax_ids` (y su `missing_since`).

| Estado | Condición | Validación | Bruto |
|---|---|---|---|
| `no_consultado` | Sin fila o `tax_ids` nulo | `sin_configuracion` | No |
| `sin_impuestos` | `tax_ids = []` (**no se asume exento**) | `sin_configuracion` | No |
| `exento_verificado` | `tax_ids = []` y producto en `VERIFIED_EXEMPT_PRODUCT_IDS[empresa]` | `calculable` | = neto |
| `iva` | Un IVA | `calculable` | Sí |
| `iva_ila` | Un IVA + un ILA | `calculable` | Sí |
| `iva_anticipado` | Incluye IVA anticipado | `requiere_revision_tributaria` | No |
| `ila_combinado` | Dos o más ILA | `requiere_revision_tributaria` | No |
| `impuesto_no_reconocido` | Id fuera del catálogo o impuesto no clasificado | `requiere_revision_tributaria` | No |
| `configuracion_inconsistente` | Impuesto bloqueante, IVA duplicado, ILA sin IVA, exento con impuestos o configuración retirada | `requiere_revision_tributaria` | No |

Ids repetidos en `tax_ids` se cuentan una vez y quedan como observación `impuesto_repetido_en_bsale`
(el IVA nunca se suma dos veces). Dos ILA no se suman automáticamente: quedan en revisión.
La lista de exentos verificados está vacía: solo contabilidad puede poblarla.

Trazabilidad en cada resultado (`commercial_gross_cost` y `costs.reference` de Márgenes V2):
`tax_ids` del producto, `taxes_applied` (`tax_id`, código SII, tasa, tipo), `factor` y fuente
(`bsale_raw.product_taxes.tax_ids + bsale_raw.taxes`). En revisión se conservan los `tax_ids` pero
`taxes_applied` queda vacío y no hay bruto.

Ejemplos (tasas ilustrativas, no reglas por producto): 10.000 + IVA 19 + ILA 10 = 12.900;
10.000 + IVA 19 + ILA 18 = 13.700; 10.000 + IVA 19 = 11.900.

## Integración

- **Control de Costos V2:** cada SKU trae `tax_profile` (estado, validación, tasas, observaciones,
  impuestos con código y uso) y `commercial_gross_cost`. `GET /analytics-v2/costs/tax-coverage`
  entrega cobertura, combinaciones, anomalías y catálogo.
- **Márgenes V2:** base por defecto `promedio_bsale`. Recargo = (Precio bruto Bsale / Costo bruto − 1)
  × 100. Si el bruto no es calculable por impuestos, estado `requiere_revision_tributaria`, sin
  recargo ni precio objetivo. Si es calculable pero `TAX_CONFIG_VERIFIED = False`, calificador
  `costo_no_verificado` y validez provisional. Las 361 reglas no se modifican.
- **Dashboard V2:** bloque `taxes` (permiso de márgenes o costos) con impuestos identificados, bruto
  calculable, sin costo, impuestos sospechosos, sin configuración, cobertura, anomalías y catálogo.
  Advertencias `configuracion_tributaria_no_contrastada` y `productos_requieren_revision_tributaria`.

## Rendimiento y automatización

Sin cron nuevo ni tablas nuevas: el catálogo se lee en la misma consulta de contexto y los perfiles
se calculan en memoria sobre los ítems de costo ya cacheados (120 s). Se actualiza solo con las
sincronizaciones RAW existentes. Persistir resultados no se justifica mientras el cálculo en memoria
sea de milisegundos; reevaluar con el `EXPLAIN ANALYZE` de la consulta 10.

## Riesgos pendientes de validación contable

1. **Códigos SII reales** de los 8 impuestos de la empresa 3 (consulta 1 del SQL).
2. **IVA anticipado (harina, carne):** es recuperable; definir si forma parte del costo comercial.
3. **ILA sobre base distinta:** el ILA se calcula sobre el precio de venta del productor/importador;
   sumarlo como porcentaje del costo de compra asume que el proveedor lo traspasa íntegro.
4. **Carácter neto de `average_cost`:** confirmar con facturas de compra que el promedio Bsale es neto.
5. **Productos sin impuestos:** decidir cuáles son exentos reales y registrarlos explícitamente.
6. **Coherencia precio/costo:** la consulta 8 compara el precio bruto de las listas 12, 13 y 16 con
   el costo bruto calculado; recargos negativos masivos indicarían un error de base.

## Validaciones (pgAdmin, solo lectura)

`backend/sql/analytics_v2/checks/cost_gross_tax_validation.sql`: catálogo de impuestos,
combinaciones por producto, SKUs por combinación, IVA/ILA duplicados, IVA anticipado, SKUs sin
configuración, 30 ejemplos neto → bruto, precio vs costo bruto en listas 12/13/16, conteo calculable
vs revisión y `EXPLAIN (ANALYZE, BUFFERS)`.

`backend/sql/analytics_v2/checks/bsale_tax_ids_audit.sql` (**ejecutar antes de dar por definitivo el
cálculo**): A) catálogo por ID real con código SII, tasa, estado, banderas Bsale y productos/SKUs que
lo usan; B) asociaciones por producto en orden Bsale (`id:código:tasa`), IVA+ILA, factor y resultado;
C) combinaciones de IDs por tipo de producto (evidencia de que la categoría no determina el ILA);
D) configuraciones ambiguas agrupadas por motivo con ejemplos.
