# Flujo operacional logístico V2 — Distribuidora

Documento de arquitectura para **FASE LOGÍSTICA 1.2**: persistencia operacional, snapshot congelado, facturación real, picking desde documentos, costos logísticos y **resultado operativo neto** (margen comercial real − costo ruta).

---

## 1. Flujo end-to-end

```
Planificar camión (ORS, 1 camión)
    → Confirmar planificación (snapshot + código PLAN-#####)
    → Excel facturación (desde snapshot, no datos vivos)
    → Facturar en Bsale (operación manual)
    → Detectar boletas/facturas (document_related + probable_matches)
    → Dashboard facturación (confirmadas / probables / pendientes)
    → Picking cliente (solo docs confirmados, orden ruta ORS)
    → Picking producto (consolidado document_details reales)
    → Resultado operativo neto (si costos variante completos)
    → Despachar / Entregar
    → Historial auditable permanente
```

---

## 2. Persistencia — `dispatch_plan`

Cada confirmación crea un registro **inmutable en identidad** con metadatos operativos:

| Campo | Descripción |
|--------|-------------|
| `planning_code` | Secuencia `PLAN-00021` |
| `planning_name` | Nombre operativo ej. "Castro Sur" |
| `truck_id` / `truck_name` | Camión congelado al confirmar |
| `route_name` | Nombre ruta ORS |
| `planning_date` | Fecha planificación |
| `created_at` / `confirmed_at` | Auditoría temporal |
| `status` | Ver §8 |
| Costos ORS | `km_total`, combustible, ferry, peajes, tripulación, `total_route_cost_clp` |

**API:** `POST /distribuidora/dispatch-plans/confirm`  
**Historial:** `GET /distribuidora/dispatch-plans` → frontend `/distribuidora/planificaciones`

---

## 3. Snapshot operacional — `dispatch_plan_orders`

Al confirmar se **congela** cada OC de la ruta. La planificación histórica **no** debe cambiar si después cambia cliente, dirección o vendedor en Bsale.

Campos snapshot (migración `021`, extendido `025`):

- `oc_document_id`, `oc_number`
- `client_name`, `fantasy_name`
- `address`, `city`
- `seller_name`, `payment_method`, `document_type_to_generate`
- `route_order`, `oc_total_amount`
- `lat`, `lng`

**Enriquecimiento al confirmar:** `_enrich_orders_snapshot()` une:

- `v_documents_latest` + `v_orders_purchase`
- `v_oc_attributes_flat` (forma pago, tipo documento)
- `document_attributes` (fallback atributos)
- `document_sellers` + `bsale.clients`

**Reparación histórica:** `POST /dispatch-plans/{id}/repair-snapshot` — solo rellena campos **vacíos**, no sobrescribe valores ya congelados.

---

## 4. Facturación vinculada

Vista `v_dispatch_plan_invoiced_documents` (`022`):

| Estado | Criterio | Uso |
|--------|----------|-----|
| **Confirmada** | `document_related` con `relateddetailid` | Excel auditoría, picking, margen |
| **Probable** | `probable_matches` heurístico | Solo advertencia en UI |
| **Pendiente** | Sin documento asociado | Bloquea picking completo |

**Dashboard:** `GET /dispatch-plans/{id}/dashboard`  
Separa montos por estado usando montos **OC del snapshot** (no ventas facturadas para el panel de facturación).

---

## 5. Excel facturación

`GET /dispatch-plans/{id}/billing-export`

Lee **exclusivamente** `dispatch_plan_orders` + metadatos del plan. Columnas:

- número OC, forma pago, tipo documento generar
- cliente, nombre fantasía, dirección, ciudad, vendedor
- total, orden ruta, camión

Si un plan antiguo tiene columnas vacías → ejecutar **Reparar snapshot** una vez.

---

## 6. Picking real (NO desde OC)

### Picking por cliente

`GET /dispatch-plans/{id}/picking-by-client`

- Solo filas con `status = 'confirmed'` en vista facturación.
- Encabezado: parada, cliente, fantasía, dirección, ciudad, celular, **número documento real**, tipo, forma pago, vendedor, total documento.
- Detalle: líneas de `document_details` del documento facturado.

### Picking por producto

`GET /dispatch-plans/{id}/picking-by-product`

- Consolida `document_details` de todas las boletas/facturas confirmadas de la ruta.
- Agrupa por variante: tipo producto, nombre+variante, código barras, unidades, cajas, monto.

**Restricciones:**

- NO usar OC para consolidado de productos.
- NO tratar `probable` como confirmado.
- NO mezclar camiones/rutas en un mismo plan.

Snapshots de picking opcionales: tabla `dispatch_plan_picking_snapshots` (`024`).

---

## 7. Costos logísticos

Persistidos en `dispatch_plan` al confirmar desde panel ORS:

| Concepto | Fuente |
|----------|--------|
| Combustible | km ORS ÷ rendimiento camión × precio diesel |
| Ferry / peajes / extras | Inputs operador |
| Chofer / peonetas | `logistics_cost_settings` + `ors_plan_route_crew` |

Rendimientos por camión (km/L):

- Hyundai: 7
- Hino 2 / Hino 3: 4.2
- Hino 4: 3.5
- Hino 5: 2.8

Tripulación por viaje (CLP):

- Chofer: 50.895
- Peoneta: 38.102

Soporta 1 chofer y hasta 2 peonetas por ruta.

---

## 8. Estados — `dispatch_plan.status`

```
draft → planned → invoicing → ready_for_picking → picking_generated → dispatched → delivered
```

| Estado | Significado |
|--------|-------------|
| `draft` | Borrador (no exporta Excel) |
| `planned` | Confirmado, snapshot guardado |
| `invoicing` | En proceso facturación Bsale |
| `ready_for_picking` | Todas OCs con doc confirmado |
| `picking_generated` | Picking generado |
| `dispatched` | Camión despachado |
| `delivered` | Ruta entregada |

---

## 9. Margen y resultado operativo neto

### Lo que NO se hace

```
❌ margen = ventas_facturadas - costo_transporte
```

Eso mezcla ingreso bruto con costo logístico y **no** es margen comercial.

### Fórmula correcta

```
resultado_operativo_neto = margen_comercial_facturado_real - costo_logístico_ruta_total
```

Donde:

```
margen_comercial_facturado_real = Σ (venta línea documento confirmado)
                                - Σ (costo variante × tax_factor × cantidad)
```

### Auditoría Bsale

Servicio: `dispatch_commercial_margin_service.py`  
Endpoint: `GET /distribuidora/dispatch-plans/margin-audit`

**Conclusión auditoría (muestras API + `raw_data` en BD):**

- Documentos (`documents.json`): `totalAmount`, `netAmount`, `taxAmount` — **sin** `totalCost`, `margin`, `netMargin`, `commercialMargin`.
- Detalle (`details.json`): **sin** `cost` en línea.
- Por tanto **no** existe margen listo en API de documentos para consumo directo.

### Fuente implementada

| Fuente | Cuándo |
|--------|--------|
| `variant_cost` + `products.tax_factor` | Cuando **todas** las líneas de docs confirmados tienen `average_cost_net` |
| `NULL` + mensaje UI | Si falta costo en alguna línea — **no se inventa margen** |

Columnas plan (`025`):

- `commercial_margin_clp`
- `net_operational_clp`
- `margin_computation_source` (`variant_cost`, `variant_cost_partial`, `unavailable`)
- `margin_lines_with_cost` / `margin_lines_total`

### Visibilidad

Roles con margen visible: `admin`, `finanzas`, `gerencia`, `superadmin` (`MARGIN_VIEW_ROLES`).  
Otros roles ven dashboard de facturación sin bloque de margen.

### Arquitectura futura (si Bsale entrega costo)

Si en el futuro `raw_data` o endpoints de analytics exponen margen por documento, cambiar `margin_computation_source` sin recalcular snapshots históricos de OC.

Alternativas preparadas (no activas sin datos):

- Costo promedio producto / compra / inventario en catálogo propio.

---

## 10. Restricciones operativas (NO HACER)

1. **NO** recalcular snapshots históricos de OC al leer (excepto `repair-snapshot` para huecos).
2. **NO** generar picking consolidado desde OC.
3. **NO** asumir `probable` como confirmado.
4. **NO** mezclar rutas/camiones en un plan.
5. **NO** mostrar margen si faltan costos de variante en Bsale.
6. **NO** usar ventas − transporte como “margen”.

---

## 11. Migraciones SQL

| Archivo | Contenido |
|---------|-----------|
| `021_dispatch_plan.sql` | Tablas plan + órdenes |
| `022_dispatch_plan_invoiced_view.sql` | Vista facturación |
| `023_dispatch_plan_identity.sql` | Código, nombre, secuencia |
| `024_dispatch_plan_picking_snapshots.sql` | Snapshots picking |
| `025_dispatch_plan_margin_and_snapshot.sql` | Margen neto, `fantasy_name`, `delivered` |

Registradas en `sync_repo.py` — ejecutar sync/migraciones en entorno antes de usar 1.2.

---

## 12. API resumen

| Método | Ruta | Uso |
|--------|------|-----|
| GET | `/dispatch-plans` | Historial |
| POST | `/dispatch-plans/confirm` | Confirmar + snapshot |
| GET | `/dispatch-plans/{id}/dashboard` | Facturación + margen |
| GET | `/dispatch-plans/{id}/billing-export` | Excel |
| GET | `/dispatch-plans/{id}/invoiced-documents` | Detalle vínculos |
| GET | `/dispatch-plans/{id}/picking-by-client` | Picking cliente |
| GET | `/dispatch-plans/{id}/picking-by-product` | Picking producto |
| POST | `/dispatch-plans/{id}/repair-snapshot` | Rellenar campos vacíos |
| GET | `/dispatch-plans/margin-audit` | Auditoría campos Bsale |

---

## 13. Frontend

| Ruta | Pantalla |
|------|----------|
| `/distribuidora/planificacion` | ORS + confirmar plan |
| `/distribuidora/planificaciones` | Historial con estado facturación |
| `/distribuidora/planificaciones/[id]` | Dashboard, Excel, picking, margen |

Componente principal: `DispatchPlanInvoicingDashboard.tsx` — muestra resultado operativo neto o mensaje si margen no disponible.

---

## 14. Objetivo de negocio

**Resultado operativo neto del camión/ruta** = rentabilidad comercial de lo **realmente facturado** en esa salida, menos el costo logístico real de la ruta — solo cuando los costos de producto existen en catálogo Bsale; en caso contrario el sistema es explícito y no muestra cifras falsas.
