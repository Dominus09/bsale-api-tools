# Inteligencia Comercial — Matriz de confiabilidad y revisión de recepciones

Alcance: empresa 3 (La Quillotana SpA), Analítica V2 sobre BSALE RAW (solo lectura).
Este documento registra **qué significa realmente cada estado que muestra la ERP**,
qué está verificado hoy y qué sigue pendiente. Ningún dato se declara validado
contablemente sin evidencia y aprobación registradas.

---

## 1. Cómo se calcula cada estado (código real)

### 1.1 Confiabilidad del costo (`sku_reliability`, `cost_signals.py`)
| Estado | Regla exacta | Qué NO significa |
|---|---|---|
| `sin_costo` | Sin compra comparable desde 2025 y sin costo promedio Bsale positivo | — |
| `solo_promedio_bsale` | Hay promedio Bsale > 0, pero ninguna compra comparable desde 2025 | No hay recepción reciente que lo respalde |
| `requiere_revision` | La última compra comparable tiene anomalías de revisión (alzas/bajas anómalas, cantidad extrema, línea modificada, etc.) | No es un error confirmado: es una señal |
| `desactualizado` | Última compra comparable hace más de `STALE_COST_DAYS` días | El promedio puede seguir siendo el de Bsale |
| `consistente` | Compra comparable reciente y sin anomalías | **No significa validado contablemente** |

### 1.2 Perfil tributario (`tax_profile.py`, desde `product_taxes.tax_ids` + `taxes`)
- La clasificación es por **código SII** (14=IVA, 24=ILA destilados, 25=ILA vinos,
  26=ILA cervezas, 27/271=ILA bebidas, 17/18/19=IVA anticipado) o por id confirmado
  por contabilidad. **Nunca por nombre, categoría o tipo de producto.**
- Sin impuestos en Bsale → `sin_impuestos`: **no se asume exento**.
- IVA anticipado → `iva_anticipado`: requiere revisión tributaria (es recuperable
  contablemente; su tratamiento comercial no está definido).
- Impuesto sin código reconocido, tasa distinta a la legal, inactivo o retirado →
  `requiere_revision_tributaria` / configuración inconsistente.

### 1.3 Costo bruto comercial (`commercial_gross_cost`)
`bruto = neto × (1 + (IVA + ILA)/100)`, solo con IVA + un único ILA identificados.
Su estado es `preliminar_no_verificado` mientras `TAX_CONFIG_VERIFIED` y
`NET_COST_VERIFIED` sigan en falso: **es provisional por diseño**.

### 1.4 Márgenes (`margin_engine.evaluate` + `data_class`)
- `data_class`: `no_disponible` (sin precio/costo/regla comparable), `confirmado`
  (solo si confirmación = confirmado **y** validez = validado), si no `provisional`.
- La validez combina el estado de revisión de la política y que el costo esté
  verificado; hoy la política está pendiente de revisión con el Excel de gerencia,
  por lo que **los márgenes se muestran provisionales**.
- Precio objetivo = `target_price`: costo bruto × (1 + mínimo de política), al peso
  superior, **solo** con regla usable, costo e impuestos válidos; si falta algo, no
  se genera (en Excel la celda queda vacía).

### 1.5 Frescura de sincronización
`bsale_raw.sync_state` por recurso/empresa (último éxito, último error). El
Dashboard V2 lo expone por recurso (precios por lista, costos, catálogo, etc.).

---

## 2. Matriz de validación

### A. Integridad de sincronización
| Criterio | Fuente | Prueba realizada | Resultado | Evidencia | Responsable | Fecha | Estado |
|---|---|---|---|---|---|---|---|
| Catálogo productos/variantes | `bsale_raw.products/variants` | Último éxito en `sync_state` (20-oct-2026) | Sincronizando | Consulta `sync_state` empresa 3 | Sistema | — | Verificado operativo |
| Costos promedio | `bsale_raw.variant_costs` (scanner) | Estado `RUNNING`/éxitos del día | Activo | `sync_state` 20-oct-2026 | Sistema | — | Verificado operativo |
| Precios lista 12 | `sync_state` `variant_prices:price_list:12` | Consulta directa 20-oct-2026 | `SUCCESS`, último éxito 22:03 UTC, 6.140 filas recibidas | `sync_state` empresa 3 | Sistema | 20-oct-2026 | Verificado operativo |
| Precios lista 16 | `sync_state` `variant_prices:price_list:16` | Consulta directa 20-oct-2026 | `SUCCESS`, último éxito 22:05 UTC, 6.140 filas recibidas | `sync_state` empresa 3 | Sistema | 20-oct-2026 | Verificado operativo |
| Precios lista 13 | `sync_state` `variant_prices:price_list:13` | Consulta directa 20-oct-2026 (tras el `FAILED` detectado antes) | **Recuperada**: `SUCCESS`, último éxito 22:04 UTC, 6.140 filas recibidas; último error previo 19:32 UTC del mismo día | `sync_state` empresa 3 | Sistema | 20-oct-2026 | Verificado operativo (recuperación confirmada) |
| Catálogo: conteo de variantes | `bsale_raw.variants` vs `sync_state` | Consulta directa de solo lectura 20-oct-2026 | 6.361 variantes vigentes = 6.361 filas recibidas por la sincronización (`SUCCESS` 06:44 UTC); 6.136 activas y 225 inactivas; 6.361 `bsale_id` distintos, sin variantes sin producto | Consulta PostgreSQL empresa 3 | Sistema | 20-oct-2026 | Verificado (alcance variantes) |
| Catálogo: duplicación por joins, costo promedio, IVA/ILA y precios por lista | `products`, `product_taxes`, `taxes`, `variant_costs`, `variant_prices` | Consulta directa intentada 20-oct-2026 | **No verificable con el rol de consulta actual** (`permission denied` en esas tablas). Los joins provienen de consultas en producción y el campo de precio bruto es `variant_value_with_taxes`, pero **el catálogo NO se declara conciliado** hasta repetir la validación con un rol con acceso o vía el Excel descargado contrastado en Bsale | Registro de la revisión 20-oct-2026 | Administración TI | Pendiente | **Pendiente de validación real** |
| Impuestos | `bsale_raw.taxes/product_taxes` | Éxitos del día | Sincronizando | `sync_state` | Sistema | — | Verificado operativo |
| Recepciones | `bsale_raw.stock_receptions(_details)` | Éxitos del día (admission_date y point) | Sincronizando | `sync_state` | Sistema | — | Verificado operativo |
| Completitud/paginación de jobs | Coolify (Scheduled Tasks) | Sin acceso desde el entorno de desarrollo | No verificado | Solicitar lista/capturas de Coolify | Administración TI | Pendiente | Pendiente |

### Verificación técnica previa a la publicación (20-oct-2026)

- **Precio bruto**: el Catálogo exporta como precio bruto exclusivamente
  `bsale_raw.variant_prices.variant_value_with_taxes`, que la sincronización
  RAW guarda desde el campo `variantValueWithTaxes` de Bsale
  (`price_engine.py`: `variant_value` = `variantValue` neto;
  `variant_value_with_taxes` = `variantValueWithTaxes`). Es el **mismo campo**
  que usan Márgenes V2 y Promociones V2 (`variant_value_with_taxes` →
  `price_gross`). El Catálogo **no** usa el precio neto (`variant_value`) ni
  recalcula brutos desde el neto.
- **Una fila por variante (estructural)**: todos los joins del Catálogo son
  contra claves primarias RAW — `variants` PK `(company_id, bsale_id)`,
  `products` PK `(company_id, bsale_id)`, `variant_costs` PK
  `(company_id, variant_id)`, `variant_prices` PK
  `(company_id, price_list_id, variant_id)` (máximo una fila por variante y
  lista, columnas independientes por lista) y `product_taxes` PK
  `(company_id, product_id)` con impuestos como arreglo resuelto en Python
  (los impuestos **no** multiplican filas; los filtros usan `EXISTS`).
- **Alcance**: solo empresas con RAW completo (`RAW_READY_COMPANY_IDS = {3}`);
  otra empresa responde 409 y una empresa fuera del alcance del usuario, 403.
- **Stock Total (columna del Catálogo)**: suma de `bsale_raw.stocks.quantity`
  (stock **físico** actual de Bsale, `/v1/stocks.json`) por variante en todas
  las sucursales de la empresa. La tabla tiene PK
  `(company_id, variant_id, office_id)` — una fila por variante×sucursal, sin
  saldos globales que dupliquen — y el cálculo usa un subquery agrupado por
  variante (una fila por variante en el Catálogo). **No** es stock disponible
  (`quantity_available`) ni reservado (`quantity_reserved`), y no suma
  recepciones históricas. Sin filas sincronizadas → «s/d» (nunca cero
  inventado); cero real → 0; negativos se conservan. Sincronización por
  sucursal verificada en producción (6 oficinas en `SUCCESS`).
- **Pendiente post-despliegue** (sin inventar resultados): la validación fila
  a fila contra PostgreSQL de producción quedó bloqueada por permisos del rol
  de consulta (`products`, `variant_costs`, `variant_prices`, `taxes` y
  `product_taxes` denegadas); se repetirá tras el despliegue con el Excel
  real contrastado en Bsale.
- **Incidente HTTP 500 de `/analytics-v2/catalog/rows` (20-oct-2026,
  resuelto)**: la estructura real de producción coincide con todas las
  columnas consultadas (verificado vía `pg_catalog`), y la causa raíz fue de
  código, no de datos ni de permisos: `price_list_freshness()` usaba
  `scope LIKE 'price_list:%'` con un `%` literal dentro de una consulta
  parametrizada de psycopg2, que formatea el SQL en el cliente
  (`sql % params`) y levantaba `TypeError: not enough arguments for format
  string` antes de llegar a PostgreSQL. Al ejecutarse dentro de `meta()`,
  rompía `/catalog/rows`, `/catalog/meta` y la exportación del catálogo.
  Corrección: escapar el literal como `%%`. Regresión cubierta por
  `backend/tests/test_analytics_v2_catalog.py`, cuyo ejecutor de prueba
  emula el formateo de psycopg2 (los ejecutores que no formatean no detectan
  esta clase de error). Sin cambios de esquema, permisos ni datos.

### B. Conciliación contra Bsale
| Criterio | Fuente | Prueba | Resultado | Evidencia | Responsable | Fecha | Estado |
|---|---|---|---|---|---|---|---|
| Precios por lista (12/13/16) | Catálogo Maestro (Excel) vs Bsale | Descargar catálogo completo y contrastar con Bsale | **Todavía no ejecutada** | Archivo Excel por generar | Administración | Pendiente | Pendiente |
| Costos promedio por variante | Catálogo Maestro vs Bsale | Idem | No ejecutada | — | Administración | Pendiente | Pendiente |
| Impuestos por producto | Catálogo Maestro vs Bsale | Idem | No ejecutada | — | Administración | Pendiente | Pendiente |
| Cantidad total de registros / estados | Catálogo Maestro vs Bsale | Idem | No ejecutada | — | Administración | Pendiente | Pendiente |

Nada se marca conciliado hasta ejecutar la comparación real con el Excel.

### C. Validación contable
| Criterio | Fuente | Prueba | Resultado | Estado |
|---|---|---|---|---|
| Interpretación del costo neto Bsale | Facturas de compra vs recepciones | Contrastar líneas con documentos | Pendiente | Pendiente |
| Tratamiento IVA/ILA en costo bruto | Contabilidad | Definir criterio y registrar aprobación | Pendiente (fórmula provisional vigente) | Pendiente |
| IVA anticipado (harina/carne) | Contabilidad | Definir tratamiento comercial/contable | Pendiente: hoy bloquea el bruto | Pendiente |
| Impuestos ambiguos/sin identificar | Catálogo Maestro (filtro «Impuestos sin identificar») | Revisión caso a caso | Pendiente | Pendiente |
| Diferencias de unidad/presentación | Recepciones vs facturas | Revisión con el caso concreto (ver §3) | Pendiente de la captura | Pendiente |

### D. Validación comercial
| Criterio | Fuente | Prueba | Resultado | Estado |
|---|---|---|---|---|
| Reglas de márgenes (361) y alcance por lista | Política vigente + Excel de gerencia | Revisión de gerencia | Pendiente de revisión (`rules_pending_review`) | Pendiente |
| Recargo mínimo/máximo por lista×tipo | Motor de márgenes | Contraste con Excel de gerencia | Pendiente | Pendiente |
| Excepciones de promociones aprobadas | Promociones V2 verificadas | Cruce con campañas vigentes | Operativo, con pendientes de aprobación | Parcial |

---

## 3. Revisión de recepciones (Fase 5) — cómo se relacionan los datos

Fuente: `cost_control_repo._LINES_CTE` (única vía de lectura de recepciones V2).

- Cabecera: `bsale_raw.stock_receptions` (empresa, vigentes, desde `HISTORY_START`).
- Detalle: `JOIN bsale_raw.stock_reception_details d
  ON d.company_id = r.company_id AND d.reception_id = r.bsale_id`.
- **Identidad de línea**: `(company_id, reception_id, detail_id = d.bsale_id, variant_id)`.
  El costo exportado/mostrado es `d.cost`, **el original de la línea**: nunca se
  recalcula ni se fusiona con otras líneas.
- La clase de movimiento (compra / transferencia interna / no determinado) se
  deriva de la **cabecera** (`document` + `internal_dispatch_id`), por lo que una
  transferencia nunca se presenta como compra a proveedor.
- Línea comparable = movimiento comparable + costo > 0 + cantidad > 0 +
  `details_complete`. Las líneas modificadas en Bsale se marcan con la bitácora
  `stock_reception_change_log` (costo/cantidad/variante), sin alterar el dato.

### Duplicidad legítima vs duplicación técnica
- **Legítimo**: el mismo `variant_id` en varias líneas de una recepción
  (`detail_id` distintos): presentaciones, descuentos o partidas separadas del
  mismo documento. Deben conservarse por separado.
- **Sospechoso**: mismo `(reception_id, detail_id)` repetido o costos distintos
  para la **misma** línea: indicaría duplicación en la ingesta o un documento
  compartido entre recepciones.

### Caso GLENFIDDICH — resuelto con Bsale (factura N.º 10155967)

El usuario revisó directamente los movimientos en Bsale. La evidencia es
exacta y cierra el diagnóstico:

**Recepción Bsale #151669 — 04-08-2026 (factura N.º 10155967, ingreso inicial
con costos cruzados):**

- Glenfiddich **18 años**: 6 unidades a **$42.981**.
- Glenfiddich **15 años**: 6 unidades a **$63.134**.

Posteriormente Bsale registra un **consumo de existencias** y un nuevo ingreso.

**Recepción Bsale #151734 — 06-08-2026 (reingreso con costos corregidos):**

- Glenfiddich **18 años**: 6 unidades a **$63.134**.
- Glenfiddich **15 años**: 6 unidades a **$42.981**.

Los costos están **literalmente intercambiados** entre ambas recepciones
($42.981 ↔ $63.134), con las mismas cantidades (6 y 6). La evidencia respalda
que administración realizó un ingreso con **costos cruzados**, un consumo y un
reingreso corregido. **La ERP estaba mostrando fielmente las recepciones
registradas en Bsale**: no son duplicados técnicos inventados por la
aplicación, no hay duplicación de la ERP y los costos distintos pertenecen a
ingresos reales de fechas distintas.

Decisiones adoptadas (vigentes):

- No eliminar, fusionar ni modificar las recepciones RAW.
- No tratar las dos recepciones como un duplicado técnico.
- Mantener intactos los registros y costos originales.
- No presentar esta variación como un aumento confirmado del proveedor, sino
  como una **corrección administrativa de costos cruzados** entre las variantes
  de 15 y 18 años.
- Futuras «correcciones administrativas» se clasificarán solo con respaldo en
  movimientos y documentación. **Todavía no se implementan cambios de cálculo
  ni exclusiones automáticas de compras comparables.**
- Para no confundir Glenfiddich 15 y 18 años, las líneas de recepción muestran
  **nombre de producto y variante completos**, SKU, código de barras e
  identificador de variante; el encabezado del detalle de producto hace lo mismo.

### Cobertura de movimientos: qué permite relacionar RAW hoy

- **Ingresos**: sincronizados e históricos (`stock_receptions` +
  `stock_reception_details`, empresa 3, 2023→hoy). Los dos ingresos de la
  factura están y deben permanecer.
- **Consumos**: el esquema (`stock_consumptions` +
  `stock_consumption_details`) y el recurso están registrados, pero **no existe
  pipeline activo ni corrida registrada en `sync_state` para la empresa 3**. El
  consumo intermedio del caso **no está en RAW**.
- **Consecuencia**: hoy **no es posible relacionar automáticamente ingreso →
  consumo → reingreso** con las fuentes sincronizadas. No se inventará esa
  relación. La evidencia del caso proviene de la revisión directa en Bsale
  (tarjeta de existencias) y de la factura.
- **Futuro (separado, con autorización)**: habilitar la sincronización de
  consumos permitiría respaldar la clasificación «corrección administrativa»
  con movimientos; mientras tanto esa clasificación es documental y manual,
  sin efectos en cálculos.

---

## 4. Cómo se mostrará la validación en pantalla (propuesta)

Cuatro estados **independientes**, visibles por módulo (sin eliminar avisos):

1. **Dato sincronizado** — frescura RAW por recurso (ya visible en Dashboard).
2. **Verificado contra Bsale** — solo tras ejecutar la conciliación con el
   Catálogo Maestro y registrar fecha/responsable.
3. **Validado contablemente** — solo con aprobación contable registrada
   (hoy: pendiente; los costos brutos siguen «provisionales»).
4. **Política comercial aprobada** — solo tras la revisión de gerencia del Excel
   de políticas (hoy: pendiente).

Un módulo podrá declararse «alcance validado» únicamente cuando los criterios
que le aplican estén cumplidos, manteniendo siempre los avisos de excepciones
específicas (sin costo, impuestos por revisar, etc.).

### 4.1 Distinción obligatoria de estados (lección del caso Glenfiddich)

Estos cuatro conceptos son **independientes** y nunca deben confundirse:

1. **Dato correctamente sincronizado** — la ERP refleja fielmente lo que Bsale
   registró (Glenfiddich: las recepciones #151669 y #151734 están tal cual
   fueron ingresadas; la sincronización es correcta).
2. **Historial de movimientos explicado** — existe una explicación respaldada
   por movimientos y documentos (ingreso con costos cruzados → consumo →
   reingreso corregido, verificado por el usuario en la tarjeta de
   existencias de Bsale y en la factura N.º 10155967).
3. **Costo contablemente validado** — requiere revisión documental y
   aprobación contable registrada. **Glenfiddich no está validado
   contablemente** y ninguna variación se declara aumento del proveedor sin
   esa revisión.
4. **Anomalía administrativa investigada** — la diferencia tiene causa
   administrativa identificada (costos cruzados 15/18 años), distinta de un
   error de sincronización y de un cambio real de precio del proveedor.

---

## 5. Auditoría Comercial 8.0 — Fase 1: costos y productos sin costo (21-oct-2026)

Auditoría de solo lectura sobre empresa 3 ejecutada vía el rol de consulta
MCP. **Medido** = obtenido de PostgreSQL hoy. **Por código** = verificado en
el repositorio. **Pendiente** = requiere un rol con acceso o conciliación.

### 5.1 Universo y sincronización (MEDIDO)

| Magnitud | Valor | Fuente |
|---|---|---|
| Variantes vigentes empresa 3 | **6.366** (6.140 activas, 226 inactivas, 0 `missing`) | `bsale_raw.variants` |
| Variantes con fila de stock | 5.887 | `bsale_raw.stocks` |
| Stock Total positivo (suma `quantity`, todas las oficinas) | **3.848 variantes** | `bsale_raw.stocks` |
| Stock Total = 0 | 2.039 variantes | `bsale_raw.stocks` |
| Stock Total negativo | 0 | `bsale_raw.stocks` |
| Activas sin fila de stock («s/d» de stock, ≠ cero) | 258 | variantes − stocks |
| Stock físico total | 472.194,63 unidades | `sum(quantity)` |
| Variantes nuevas 30/90 días | No medible: `first_seen_at` se reinició con la reconstrucción de RAW (todo el universo figura «nuevo») | `bsale_raw.variants` |

Sincronización (`sync_state`, 21-oct-2026): variantes SUCCESS 06:44 UTC
(6.366 filas = conteo exacto en tabla); precios listas 12/13/16 SUCCESS
20:31–20:34 UTC (6.144 filas c/u); stocks SUCCESS 20:49–20:53 UTC en las
6 oficinas (5.886 filas c/u; última lectura 21:01 UTC); productos e
impuestos SUCCESS del día. `variant_costs` figura **RUNNING** (scanner,
último éxito 20:44 UTC, 1.000 filas en la corrida en curso): la foto de
costos puede estar en movimiento durante el día.

### 5.2 Clasificación de costos A–H: estado de medición

La fuente de costo promedio (`bsale_raw.variant_costs`) **no es accesible
con el rol de consulta actual** (`permission denied`, confirmado hoy;
tampoco `products`, `variant_prices`, `stock_receptions`). Por tanto las
clases A–D y F **no se declaran con números de producción** en esta fase.
Lo que sí está definido y listo para ejecutar en cuanto exista acceso
(consulta de clasificación en §5.5):

- **A** costo > 0 · **B** sin registro o NULL · **C** costo = 0 ·
  **D** costo < 0 · **E** costo > 0 sin recepción comparable (la ERP ya lo
  etiqueta `solo_promedio_bsale`, **nunca** «sin costo») ·
  **F** costo > 0 con divergencia vs última comparable (anomalía
  `DIFERENCIA_PROMEDIO_BSALE` del motor) · **G** packs (motor propio por
  componentes; sin composición completa = no calculable) ·
  **H** sin confirmación por sincronización (p. ej. costos en scanner).
- **Ventas últimos 90 días**: no existe fuente confiable accesible en RAW
  para este rol (documents solo scope `point`); queda **fuera de la
  priorización automática** hasta habilitarla. No se estima.
- Distribución por tipo de producto: pendiente (requiere `products`).

### 5.3 Motor de márgenes — auditoría por código (sin modificar)

Verificado en `margin_engine.evaluate`, `tax_profile.commercial_gross_cost`
y `cost_signals.sku_reliability`:

1. **Sin margen ficticio**: costo de referencia `None` o `<= 0` ⇒ estado
   `sin_costo`, `markup_pct` y `margin_on_sale_pct` en `None`. No existe
   margen de 100 % inventado.
2. Costo cero y negativo se tratan como «sin costo válido» en Márgenes,
   pero la capa de costos **sí los distingue** (`costo_cero` /
   `costo_negativo` / `costo_nulo`): la clasificación administrativa debe
   usar esa distinción, no el motivo genérico de Márgenes.
3. Recargo (sobre costo bruto comercial) y margen (sobre precio bruto de
   venta) son magnitudes separadas y trazables por fila; precio bruto =
   `variant_value_with_taxes` original de Bsale.
4. Impuestos desconocidos o sin configuración ⇒ sin costo bruto y estado
   `requiere_revision_tributaria` (sin porcentajes). IVA anticipado bloquea
   el bruto. Packs sin composición completa ⇒ `pack_no_calculable`.
5. Venta bajo costo se marca como riesgo siempre (`venta_bajo_costo`),
   también bajo excepciones promocionales verificadas.
6. Todo margen hoy es **provisional por diseño**: `NET_COST_VERIFIED` y
   `TAX_CONFIG_VERIFIED` en falso y políticas pendientes de revisión de
   gerencia. Ningún resultado debe presentarse como validado contablemente.

### 5.4 Priorización administrativa (reglas propuestas, no estados contables)

- **CRÍTICO**: stock total > 0 sin costo válido; costo negativo; cualquier
  caso que impida margen confiable con existencias. (Ventas recientes sin
  costo se sumará cuando exista fuente de ventas.)
- **ALTO**: activo comercializado con anomalías de costo (divergencia vs
  recepciones, `requiere_revision`), problemas tributarios que bloquean el
  bruto.
- **MEDIO**: activo con precio, sin costo válido, sin stock ni movimiento
  conocido; costo solo desde promedio sin respaldo comparable (clase E).
- **BAJO**: inactivo sin stock ni movimientos; packs y casos especiales.
Con lo medible hoy: el universo máximo de exposición crítica es
«3.848 variantes con stock > 0» cruzado con «sin costo válido» — el cruce
exacto es la primera consulta pendiente de §5.5. Las 258 activas sin fila
de stock son prioridad de sincronización (su stock real se desconoce).

### 5.5 Primera consulta pendiente (al habilitar acceso a `variant_costs`)

Clasificación A–D por variante y cruce con Stock Total ya calculado en
Catálogo/Márgenes (`sum(quantity)` por variante): conteos por clase,
conteos «con stock > 0» por clase, y distribución por tipo de producto.
Hasta ejecutarla, **ningún conteo de «productos sin costo» se declara**.

Consulta canónica (solo lectura; empresa 3). Ejecutar cuando el rol pueda
`SELECT` en `bsale_raw.variant_costs` (y opcionalmente `products` para tipo):

```sql
WITH st AS (
  SELECT variant_id, sum(quantity) AS stock_total
  FROM bsale_raw.stocks
  WHERE company_id = 3
  GROUP BY variant_id
),
base AS (
  SELECT
    v.bsale_id AS variant_id,
    COALESCE(v.state, 0) AS variant_state,
    vc.average_cost,
    CASE
      WHEN vc.variant_id IS NULL OR vc.average_cost IS NULL THEN 'B_sin_registro_o_null'
      WHEN vc.average_cost > 0 THEN 'A_positivo'
      WHEN vc.average_cost = 0 THEN 'C_cero'
      WHEN vc.average_cost < 0 THEN 'D_negativo'
    END AS clase_costo,
    st.stock_total
  FROM bsale_raw.variants v
  LEFT JOIN bsale_raw.variant_costs vc
    ON vc.company_id = v.company_id AND vc.variant_id = v.bsale_id
  LEFT JOIN st ON st.variant_id = v.bsale_id
  WHERE v.company_id = 3
    AND v.missing_since IS NULL
)
SELECT
  clase_costo,
  count(*) AS variantes,
  count(*) FILTER (WHERE COALESCE(stock_total, 0) > 0) AS con_stock_positivo,
  count(*) FILTER (WHERE stock_total IS NULL) AS sin_fila_stock,
  count(*) FILTER (WHERE variant_state = 0) AS activas,
  count(*) FILTER (
    WHERE variant_state = 0
      AND COALESCE(stock_total, 0) > 0
      AND clase_costo IN ('B_sin_registro_o_null', 'C_cero', 'D_negativo')
  ) AS critico_activo_stock_sin_costo_valido
FROM base
GROUP BY clase_costo
ORDER BY clase_costo;
```

**No mezclar** A/B/C/D en un solo «sin costo». Márgenes trata C y D como
sin costo válido para porcentajes; la auditoría administrativa los reporta
separados.

### 5.6 Diseño de mejora de Excel (diseño, aún no implementado)

Sobre los exportadores existentes (sin motor paralelo), agregar:

1. **Resumen ejecutivo**: variantes auditadas, clases A–H, con stock sin
   costo, anomalías, pendientes, cobertura y fecha de corte por recurso.
2. **Pendientes prioritarios**: ordenados por prioridad (CRÍTICO→BAJO),
   con razón de alerta, evidencia y acción administrativa propuesta.
3. **Análisis completo**: lo actual (costo promedio separado del costo de
   última recepción comparable, Stock Total, precios 12/13/16, impuestos,
   estados), distinguiendo costo cero de ausente.
4. **Recepciones relevantes**: líneas que respaldan cada anomalía
   (documento, recepción, variante completa), declarando límites
   (historial desde `HISTORY_START`; consumos no sincronizados).
5. **Revisión administrativa**: ID estable por variante, estado de
   revisión, causa, observación, responsable, fecha, respaldo, solución
   propuesta y aprobación.
6. **Metodología y sincronización**: empresa, corte, fuentes, fórmulas
   (recargo vs margen), advertencias y limitaciones explícitas.
Números reales con formato CLP, SKU como texto, filtros y totales que no
valorizan inventario con costo cero desconocido (sin valorización ficticia).

### 5.7 Futura categoría «Posible corrección administrativa» (preparada, no implementada)

> Nota de edición (21-oct-2026): esta subsección se redactó originalmente
> como 4.2 junto al caso Glenfiddich (§3); conserva su contenido íntegro.

Clasificación futura para variaciones de costo explicables por correcciones
de la administración. Criterios **todos** exigibles (sin clasificar
automáticamente cualquier consumo y reingreso como corrección):

- Dos ingresos reales del mismo documento/proveedor con costos cruzados o
  corregidos entre variantes de una misma familia, con cantidades coherentes.
- Un movimiento de consumo/salida intermedio que retire la mercadería
  ingresada con el costo erróneo (cuando la fuente de consumos esté
  sincronizada y lo respalde; hoy no lo está, ver §3).
- Respaldo documental (factura y, cuando exista, documento del consumo y del
  reingreso) revisado por una persona, con responsable y fecha registrados.
- La categoría será **informativa**: no recalculará costos, no excluirá
  recepciones de las compras comparables ni modificará registros de Bsale o
  de RAW sin una decisión posterior expresa.

### 5.8 Actualización de censo (23-oct-2026 UTC) — sin conteos A–D aún

Continuación de Fase 1 sin duplicar la auditoría de motor (§5.3). Solo se
refrescan magnitudes **medibles** con el rol MCP actual.

| Magnitud | 21-oct-2026 | 23-oct-2026 UTC | Fuente |
|---|---|---|---|
| Variantes vigentes | 6.366 | **6.370** | `variants` (= `rows_received` sync 22-oct 06:44) |
| Activas / inactivas | 6.140 / 226 | **6.144 / 226** | `state=0` / `<>0` |
| Con fila de stock | 5.887 | **5.902** | `stocks` agrupado |
| Stock Total > 0 | 3.848 | **3.873** (60,80 % del universo) | `sum(quantity)` |
| Stock Total = 0 | 2.039 | **2.029** | idem |
| Stock Total < 0 | 0 | **0** | idem |
| Activas sin fila stock | 258 | **258** (4,20 % de activas) | left join |
| Unidades físicas | ~472.195 | **472.108,52** | `sum(quantity)` |
| `variant_costs` SELECT | denied | **denied** (`permission denied`) | MCP |
| `products` / `variant_prices` / `stock_receptions` / `taxes` | denied | **denied** | MCP |

Sincronización relevante (corte ~23-oct 02:40 UTC):

- `variant_costs` scope `scanner`: **RUNNING**; último éxito 02:28 UTC;
  corrida en curso con `rows_received=1000` → foto de costos móvil.
- Listas comerciales habituales 12 / 13 / 16: precios SUCCESS recientes
  (13 RUNNING al corte); filas ~6.159 en 12/16.
- Stocks oficinas 1–6: SUCCESS reciente (oficina 3 RUNNING al corte);
  hubo errores 401 intermitentes el 22-oct (autenticación Bsale).
- `stock_receptions`: SUCCESS 22-oct (adm_date / point).
- Ventas 90 días: `documents` scope `point` sin cobertura usable para
  priorización (último éxito 4-oct, 1 fila) → **sigue fuera**.

**Conclusión de bloqueo:** sigue sin poder declararse cuántos productos
tienen costo promedio válido, cero, nulo o negativo. El universo máximo de
exposición crítica medible es «**3.873 variantes con Stock Total > 0**»
cruzado con sin costo válido — el cruce exacto requiere §5.5.

### 5.9 Fase 1.1 — Censo cerrado + script backend (23-oct-2026)

#### Diferencia de 14 variantes (RESUELTA)

En una misma consulta (empresa 3, Stock Total = `sum(quantity)` por
`variant_id`, sin duplicar oficinas):

| Magnitud | Valor |
|---|---|
| Stock Total > 0 (filas de `stocks`) | **3.873** |
| Activas con stock > 0 (join a `variants`) | **3.859** |
| Inactivas con stock > 0 | **0** |
| Stock > 0 **sin fila en `variants`** | **14** |
| Identidad | 3.859 + 0 + 14 = **3.873** ✓ |

Las 14 son `variant_id` presentes en `bsale_raw.stocks` (6 oficinas c/u) y
**ausentes** en `bsale_raw.variants` (no es `missing_since`; no hay fila).
IDs: 31352–31357, 31359–31366. Unidades involucradas: **356**.
Interpretación: desfase de sincronización catálogo↔stock (variantes nuevas
en Bsale con stock ya bajado, o variantes eliminadas del sync de catálogo
pero no del de stocks). **No** son inactivas con existencias.

Censo de catálogo coherente (solo variantes existentes):

- Universo variants: **6.370** (6.144 activas + 226 inactivas)
- Activas sin fila stock: **258**
- Unidades stock > 0 totales (incluye huérfanas): **472.093,52**

#### Rol MCP y permisos

- Rol actual: **`ai_reader`**
- SELECT concedido hoy: `variants`, `stocks`, `sync_state`
- SELECT **denegado**: `variant_costs`, `products`, `variant_prices`,
  `stock_receptions`, `stock_reception_details`, `taxes`, …

Por tanto las clases A/B1/B2/C/D y el top-30 crítico **no se midieron** en
esta sesión. No se inventan.

#### Script administrativo (solo lectura)

Archivo: `backend/scripts/audit_commercial_cost_census.py`

- Conexión: `backend.db.get_connection` (mismas `PG_*` que FastAPI)
- TX: `READ ONLY` + `REPEATABLE READ` + `statement_timeout` (default 120 s)
- Sin INSERT/UPDATE/DELETE; siempre `ROLLBACK`; sin endpoint; no loguea secretos
- Emite censo, identidad de cierre, cobertura de costos, clases, top críticos
- Distingue B1 (sin fila PG) de B2 (`average_cost` NULL) y no declara B1 como
  «Bsale sin costo» sin contrastar `sync_state`

Comando en terminal Coolify del **contenedor backend** (tras desplegar el
script o montar el código):

```bash
python -m backend.scripts.audit_commercial_cost_census \
  --company-id 3 --timeout-sec 120 --top-critical 30 \
  --price-list-id 12 --json
```

En el entorno local del agente **no** hay `PG_*`: el script aborta sin
inventar resultados y muestra el comando anterior.

#### GRANT mínimo sugerido (NO ejecutado; solo instrucción)

Como propietario del esquema / DBA, **sin** superusuario de auditoría y
**sin** GRANT OPTION:

```sql
GRANT SELECT ON TABLE bsale_raw.variant_costs TO ai_reader;
GRANT SELECT ON TABLE bsale_raw.products TO ai_reader;
GRANT SELECT ON TABLE bsale_raw.product_types TO ai_reader;
GRANT SELECT ON TABLE bsale_raw.variant_prices TO ai_reader;
GRANT SELECT ON TABLE bsale_raw.stock_receptions TO ai_reader;
GRANT SELECT ON TABLE bsale_raw.stock_reception_details TO ai_reader;
```

Alternativa operativa: ejecutar el mismo script **dentro del contenedor**
con el rol de la aplicación FastAPI (ya lee costos en producción) — no
requiere ampliar `ai_reader`.

#### Implicaciones Excel (diseño; sin implementar)

1. Separar A / B1 / B2 / C / D en resumen (NULL ≠ cero).
2. Columna/flag «activa + stock>0 + sin costo válido».
3. Hora de corte + estado scanner `variant_costs`.
4. Advertencia de stock huérfano (sin variante en catálogo).
5. No valorizar inventario con costo B1/B2/C/D.
6. No sustituir promedio por última recepción en columnas de margen.

### 5.10 Fase 1.2 — Huérfanas, costo cero y diseño Excel Premium (23-oct-2026)

Resultados del censo real (script en contenedor FastAPI, empresa 3),
aceptados como base:

| Clase | Variantes |
|---|---:|
| Universo | 6.370 |
| A — promedio > 0 | 3.902 |
| C — promedio = 0 | 2.468 |
| B1 / B2 / D | 0 |
| Activas + stock>0 + costo válido (A) | 3.859 |
| Huérfanas stock>0 | 14 (356 uds) |

Cierre A+C = 6.370 ✓. No hay «sin registro» de costo en PG: toda variante
del catálogo tiene fila en `variant_costs`.

#### Huérfanas (medido MCP `ai_reader`)

- Catálogo: `max(variants.bsale_id)=31351`; sync variants SUCCESS 22-oct
  06:44 UTC (6.370 filas).
- Huérfanas stock>0: **31352–31357, 31359–31366** (14). Primera aparición en
  `stocks` entre 22-oct 15:08 y 20:22 UTC — **después** del sync de
  variantes. Cantidad ≠ 0 solo en oficina **1** (5 SKU) u oficina **3**
  (9 SKU); resto de oficinas en 0.
- Huérfana adicional stock=0: **31358** (fila en stocks, total 0).
- No existen en `variants` de **ninguna** empresa en PG.
- Hipótesis respaldada: **SKU nuevos en Bsale el 22-oct**, capturados por
  el sync incremental/scanner de stocks, pendientes del próximo
  FULL_RECONCILE de variantes (nightly). Inventario **vigente** (no
  histórico): `api_fetched_at` reciente ~23-oct 03:07–03:09 UTC.
- Acción propuesta (sin implementar): forzar sync de variantes o esperar
  nightly; contrastar IDs 31352–31366 en Bsale UI; no borrar stocks.

#### Costo cero y divergencias (bloqueado en MCP)

`ai_reader` sigue sin SELECT a `variant_costs` / precios / recepciones.
La clasificación de los **2.277 activos con costo 0** (precio, stock,
recepciones) y la distribución de diferencias promedio vs última
comparable sobre las **3.859** con stock+costo válido **requieren**
re-ejecutar consultas en el contenedor backend (mismo rol `postgres` /
app). No se inventan conteos aquí.

Umbrales **propuestos** para revisión administrativa (no reglas
comerciales definitivas; alineados al motor actual):

| Banda | |Δ%| promedio vs última comparable | Uso |
|---|---|---|
| Sin diferencia relevante | ≤ 15 % | monitoreo |
| Moderada | > 15 % y ≤ 50 % | revisión |
| Importante | > 50 % (`AVERAGE_DIVERGENCE_PCT`) | alerta motor actual |
| Extrema | ≥ 100 % alza o ≤ −60 % baja | severidad ALTA en señales |
| Sin recepción comparable | — | clase E / solo promedio |
| Posible corrección adm. | criterio documental (§5.7, Glenfiddich) | manual |

El promedio Bsale **no** se sustituye por la última compra.

#### Frescura (corte ~23-oct 03:10 UTC)

- `variant_costs` scanner: **RUNNING** (último éxito 02:54; corrida con
  ~370 filas) → foto móvil.
- Stocks oficinas: SUCCESS reciente; oficina 4 RUNNING; historial de 401
  intermitentes.
- Variants: SUCCESS 22-oct 06:44 (desfase vs stocks = causa de huérfanas).
- Precios 12/13/16: SUCCESS ~03:02–03:04.

#### Rol y permisos (sin cambiar)

El script usa la conexión FastAPI (`current_user` típico **`postgres`** /
rol de app). Los `suggested_grants` del JSON que digan `TO postgres` son
**incorrectos como destino de auditoría**: no conceder SELECT extra al
superusuario/app. Destino correcto futuro: rol mínimo tipo `ai_reader` o
`lq_audit_ro`. Hoy la vía operativa es ejecutar el script en el contenedor
(sin GRANT nuevos).

#### Excel Premium — diseño definitivo (sin implementar)

Seis hojas sobre exportadores existentes (Costos / Márgenes / Catálogo),
manteniendo fórmulas:

1. **Resumen ejecutivo** — universo, A/C (y B/D si aparecen), stock>0 con
   A, activos C, huérfanas, frescura sync, % cobertura; **sin valorizar**
   inventario con C.
2. **Críticos y anomalías** — activas stock>0 con C o D; divergencias
   importantes/extremas; motivo + evidencia.
3. **Activos con costo cero** — cruce precio lista 12/13/16, stock,
   recepción comparable sí/no; acción sugerida.
4. **Promedio vs recepciones** — % diff, bandas, doc/fecha comparable;
   no reemplazar promedio.
5. **Huérfanas y sync** — IDs, oficinas, unidades, first_seen vs último
   sync variants; estado sync.
6. **Revisión administrativa** — ID estable, estado, responsable, fecha,
   observación, respaldo, solución propuesta (vacía para llenar).

Prioridad de implementación: (1) hoja huérfanas + sync variants, (2)
activos costo cero con stock/precio, (3) divergencias, (4) resumen.

### 5.11 Fase 1.3 — Procedimiento huérfanas + script extendido (23-oct-2026)

**Sin ejecutar sync automáticamente.** Procedimiento normal (Coolify,
contenedor backend, `WORKDIR=/app`):

1. `python -m backend.jobs.bsale_raw sync-catalog --company 3 --dry-run`
2. Con autorización: `… sync-catalog --company 3` (incluye FULL variants).
3. `refresh-costs --company 3 --variant 31352 … 31366` (tras existir en
   `variants`); el scanner `scan-costs` solo itera el catálogo ya
   materializado.
4. Re-auditar: `stock_pos_sin_variante` debe tender a 0.

Script `audit_commercial_cost_census.py` ampliado (`--extended`, default):

- `zero_cost_actives`: grupos A/B (precio×stock) y C/D (recepción).
- `positive_cost_divergence`: buckets ≤15 / 15–50 / >50 / extrema /
  sin comparable + top 30 anomalías con documento.
- `orphan_resolution_procedure` y `excel_field_map` en el JSON.
- GRANT sugerido siempre hacia `ai_reader` (nunca hacia `postgres`).

Comando análisis ampliado:

```bash
cd /app
python -m backend.scripts.audit_commercial_cost_census \
  --company-id 3 --timeout-sec 180 --top-critical 30 --extended --json
```

Excel Premium: implementar **después** de pegar resultados 1.3
(`zero_cost_actives` + `positive_cost_divergence`).
