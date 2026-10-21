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

### 4.2 Futura categoría «Posible corrección administrativa» (preparada, no implementada)

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
