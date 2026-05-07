# APIs consumidas por el frontend ERP

## Resumen

- **Cantidad total de llamadas detectadas (fetch):** 73 en código frontend (`frontend/lib/api.ts`: 69, `frontend/services/orders.ts`: 3, `frontend/lib/resumen-vendedor-pdf-route-canvas.ts`: 1).
- **Métodos HTTP utilizados:** `GET`, `POST`, `PATCH`, `PUT`, `DELETE`.
- **URL base y proxy:**
  - `frontend/lib/api-base.ts` define `DEFAULT_API_URL = https://api.quillotana.cl`.
  - En browser usa prefijo same-origin `/api-upstream` (rewrite en `frontend/next.config.mjs`).
- **Archivos principales de consumo API:** `frontend/lib/api.ts`, `frontend/services/orders.ts`.

## APIs por módulo

| Módulo | Archivo | Función/Hook | Método | Endpoint/URL | Datos enviados | Datos esperados | Pantalla/Componente | Estado |
|---|---|---|---|---|---|---|---|---|
| Auth | `frontend/lib/api.ts` | `login()` | POST | `/login` | `{email,password}` | `{token,email,role}` | `frontend/app/login/page.tsx` | Activa |
| Contexto empresa | `frontend/lib/api.ts` | `getCompanies()` | GET | `/companies` | — | `Company[]` | `company-selector`, `margins`, compras | Activa |
| Analítica margen | `frontend/lib/api.ts` | `getMarginSummary()` | GET | `/margin-summary` | `company_id` query | KPI resumen | `dashboard` | Activa |
| Analítica margen | `frontend/lib/api.ts` | `getMarginAlerts()` | GET | `/margin-alerts` | `company_id` query | alertas | `dashboard`, `alerts` | Activa |
| Analítica margen | `frontend/lib/api.ts` | `getMarginAnalysisView()` | GET | `/margin-analysis-view` | `company_id`, `price_list_id` | rows vista márgenes | `margins` | Activa |
| Productos | `frontend/lib/api.ts` | `getProductsWithoutCost()` | GET | `/products-without-cost` | `company_id` | productos | `products-without-cost` | Activa |
| Proveedores | `frontend/lib/api.ts` | `getSuppliers()` | GET | `/suppliers` | query opcional (`company_id`, `name`) | `Supplier[]` | compras (OC/proveedores) | Activa |
| Proveedores | `frontend/lib/api.ts` | `createSupplier()` | POST | `/suppliers` | payload proveedor | proveedor creado | `compras/proveedores` | Activa |
| Proveedores | `frontend/lib/api.ts` | `updateSupplier()` | PATCH | `/suppliers/{id}` | payload parcial | proveedor actualizado | `compras/proveedores` | Activa |
| Compras | `frontend/lib/api.ts` | `getPurchaseOffices()` | GET | `/purchase-offices` | `company_id` | oficinas | `compras/*` | Activa |
| Compras | `frontend/lib/api.ts` | `getPurchaseAnalysis()` | GET | `/purchase-analysis` | `company_id`,`office_id`,`supplier_id?` | filas análisis | `compras/generar-oc` | Activa |
| Compras | `frontend/lib/api.ts` | `getPurchaseOrders()` | GET | `/purchase-orders` | `company_id`,`office_id?` | headers OC | `compras/registros-oc` | Activa |
| Compras | `frontend/lib/api.ts` | `getPurchaseOrder()` | GET | `/purchase-orders/{oc_id}` | `company_id` | header+details | `compras/registros-oc`, generar OC | Activa |
| Compras | `frontend/lib/api.ts` | `generatePurchaseOrderFromLines()` | POST | `/purchase-orders/generate-from-lines` | líneas OC | `{oc_id}` | `compras/generar-oc` | Activa |
| Compras | `frontend/lib/api.ts` | `patchPurchaseOrderStatus()` | PATCH | `/purchase-orders/{oc_id}` | `{status}` | estado actualizado | `compras/registros-oc` | Activa |
| Product master | `frontend/lib/api.ts` | `getProductsMaster()` | GET | `/products-master` | filtros/paginación | listado | `compras/productos`, promociones | Activa |
| Product master | `frontend/lib/api.ts` | `patchProductMaster()` | PATCH | `/products-master/{barcode}` | `supplier_id` | fila actualizada | `compras/productos` | Activa |
| Product master | `frontend/lib/api.ts` | `getProductsMasterWithoutSupplierCount()` | GET | `/products-master/count-without-supplier` | — | count | compras y proveedores | Activa |
| Promociones | `frontend/lib/api.ts` | `getPromotionsGrid()` | GET | `/promotions/grid` | filtros | grilla | `promotions` | Activa |
| Promociones | `frontend/lib/api.ts` | `createPromotion()` | POST | `/promotions` | payload promoción | ids/procesados | `promotions` | Activa |
| Promociones | `frontend/lib/api.ts` | `togglePromotion()` | PUT | `/promotions/{id}/toggle` | — | estado promoción | `promotions` | Activa |
| Distribuidora sync | `frontend/lib/api.ts` | `getDistribuidoraSyncStatus()` | GET | `/distribuidora/sync-status` | — | estado sync | `distribuidora/dashboard`,`orders`,`pre-planificacion` | Activa |
| Distribuidora sync | `frontend/lib/api.ts` | `postDistribuidoraSyncOrders()` | POST | `/distribuidora/sync-orders` | — | estado/procesados | `distribuidora/orders`,`pre-planificacion` | Activa |
| Distribuidora sync | `frontend/lib/api.ts` | `postDistribuidoraSyncSales()` | POST | `/distribuidora/sync-sales` | — | queued/completed | `distribuidora/dashboard` | Activa |
| Distribuidora clientes | `frontend/lib/api.ts` | `getDistribuidoraClientsDashboard()` | GET | `/distribuidora/clients/dashboard` | filtros query | KPIs + series | `distribuidora/dashboard` | Activa |
| Distribuidora clientes | `frontend/lib/api.ts` | `getDistribuidoraClientsConsolidated()` | GET | `/distribuidora/clients` | fecha/vendedor/comuna | items | `distribuidora/clientes` | Activa |
| Distribuidora clientes | `frontend/lib/api.ts` | `getDistribuidoraClientsFrequency()` | GET | `/distribuidora/clients/frequency` | filtros | frecuencia | `distribuidora/clientes` | Activa |
| Distribuidora clientes | `frontend/lib/api.ts` | `getDistribuidoraClientsInactive()` | GET | `/distribuidora/clients/inactive` | days/limit | inactivos | `distribuidora/clientes/inactivos` | Activa |
| Distribuidora clientes | `frontend/lib/api.ts` | `getDistribuidoraClientsSummarySellers()` | GET | `/distribuidora/clients/summary/sellers` | filtros/seller_ids | resumen vendedor | `distribuidora/vendedores`,`clientes` | Activa |
| Distribuidora análisis | `frontend/lib/api.ts` | `getDistribuidoraClientesAnalisis()` | GET | `/distribuidora/clientes/analisis` | limit | items análisis | `distribuidora/clientes/analisis` | Activa |
| Distribuidora análisis | `frontend/lib/api.ts` | `downloadDistribuidoraClientesAnalisisExcel()` | GET | `/distribuidora/clientes/analisis/export` | limit | blob xlsx | `distribuidora/clientes/analisis` | Activa |
| Distribuidora órdenes | `frontend/lib/api.ts` | `getDistribuidoraOrdersPurchase()` | GET | `/distribuidora/orders/purchase` | rango/filtros | orders | `dispatch-analysis` | Activa |
| Distribuidora pre-despacho | `frontend/lib/api.ts` | `getDistribuidoraDispatchPrepByMunicipality()` | GET | `/distribuidora/orders/dispatch-prep/by-municipality` | rango/filtros | resumen comuna | `distribuidora/orders` | Activa |
| Distribuidora pre-despacho | `frontend/lib/api.ts` | `getDistribuidoraDispatchPrepObservaciones()` | GET | `/distribuidora/orders/dispatch-prep/observaciones` | rango/filtros | observaciones | `distribuidora/orders` | Activa |
| Distribuidora pre-despacho | `frontend/lib/api.ts` | `getDistribuidoraDispatchPrepPlanningRows()` | GET | `/distribuidora/orders/dispatch-prep/planning-rows` | rango/filtros | rows | `distribuidora/orders` | Activa |
| Distribuidora camiones | `frontend/lib/api.ts` | `getDistribuidoraTrucks()` | GET | `/distribuidora/trucks` | — | `{items}` | `distribuidora/orders`,`pre-planificacion` | Activa |
| Distribuidora planificación | `frontend/lib/api.ts` | `getDistribuidoraPlanificacionOrders()` | GET | `/distribuidora/planificacion/orders` | rango/delivery_day | orders | `pre-planificacion` | Activa |
| Distribuidora planificación | `frontend/lib/api.ts` | `postDistribuidoraPlanificacionOrsRoutes()` | POST | `/distribuidora/planificacion/ors-routes` | routes[] | rutas ORS | `planificacion` | Activa |
| Distribuidora planning | `frontend/lib/api.ts` | `getDistribuidoraPurchaseByDocumentIds()` | GET | `/distribuidora/orders/purchase/by-document-ids` | ids csv | preview orders | `planning` | Activa |
| Distribuidora planning | `frontend/lib/api.ts` | `postDistribuidoraRoutePlanningBatch()` | POST | `/distribuidora/route-planning/batch` | assignments | confirmación | `planning` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `getDistribuidoraMapa()` | GET | `/distribuidora/mapa` | — | clientes+bases | `mapa`,`rutero`,`resumen-vendedor` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `getDistribuidoraRutaDetalle()` | GET | `/distribuidora/ruta-detalle` | vendedor,dia | detalle ruta | `mapa` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `getDistribuidoraRutaSugerencias()` | GET | `/distribuidora/ruta-sugerencias` | vendedor,dia,min_delta | sugerencias | `mapa` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `postDistribuidoraOrdenManualBulk()` | POST | `/distribuidora/orden-manual-bulk` | items orden | ack | `mapa` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `postDistribuidoraOrdenManualReset()` | POST | `/distribuidora/orden-manual/reset` | vendedor,dia | ack | `mapa` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `postDistribuidoraOptimizarRuta()` | POST | `/distribuidora/optimizar-ruta` | vendedor,dia,bloque | json ruta | `mapa` | Activa |
| Distribuidora mapa | `frontend/lib/api.ts` | `postDistribuidoraOptimizarRutaDesde()` | POST | `/distribuidora/optimizar-ruta-desde` | vendedor,dia,desde | json ruta | `mapa` | Activa |
| Distribuidora rutero | `frontend/lib/api.ts` | `getDistribuidoraRutero()` | GET | `/distribuidora/rutero` | filtros | filas rutero | `rutero` | Activa |
| Distribuidora rutero | `frontend/lib/api.ts` | `postDistribuidoraObservacionRutero()` | POST | `/distribuidora/observacion` | cliente_id,obs | fila | `rutero` | Activa |
| Distribuidora rutero | `frontend/lib/api.ts` | `patchDistribuidoraRuteroTipoAtencion()` | PATCH | `/distribuidora/rutero/{id}` | tipo_atencion | fila | `rutero` | Activa |
| Distribuidora rutero | `frontend/lib/api.ts` | `patchDistribuidoraRuteroSabado()` | PATCH | `/distribuidora/rutero/sabado` | rut_clean,activo | updated | `rutero` | Activa |
| Distribuidora resumen | `frontend/lib/api.ts` | `getDistribuidoraResumenVendedor()` | GET | `/distribuidora/resumen-vendedor` | vendedor | resumen | `resumen-vendedor` | Activa |
| Legacy distribuidora | `frontend/lib/api.ts` | `getDistribuidoraPendientes()` | GET | `/distribuidora/pendientes` | — | items | legacy | Deprecada |
| Legacy distribuidora | `frontend/lib/api.ts` | `postDistribuidoraPendientesAsignarDia()` | POST | `/distribuidora/pendientes/asignar-dia` | bsale_id,dia | json | legacy | Deprecada |
| Legacy distribuidora | `frontend/lib/api.ts` | `getDistribuidoraSinGeoref()` | GET | `/distribuidora/sin-georef` | — | items | legacy | Deprecada |
| Legacy distribuidora | `frontend/lib/api.ts` | `downloadDistribuidoraSinGeorefExcel()` | GET | `/distribuidora/sin-georef/export` | — | blob xlsx | legacy | Deprecada |
| Pedidos | `frontend/services/orders.ts` | `getOrders()` | GET | `/orders` | page/limit/status | `OrderRow[]` | `orders` | Activa |
| Pedidos | `frontend/services/orders.ts` | `getOrderById()` | GET | `/orders/{id}` | — | detalle pedido | `orders/[id]` | Activa |
| Pedidos | `frontend/services/orders.ts` | `updateOrderStatus()` | PUT | `/orders/{id}/status` | `{status}` | estado | `orders/[id]` | Activa |
| Export/PDF | `frontend/lib/resumen-vendedor-pdf-route-canvas.ts` | fetch de mapa/base tiles | GET | URL externa de tile/imagen | binario | canvas/image | resumen vendedor | Requiere verificación |

## APIs que requieren revisión

- URLs hardcodeadas de assets externos en páginas (`img src` de blob URL en login/sidebar/company-selector/home-client).
- Funciones legacy marcadas `@deprecated` en `frontend/lib/api.ts` (`pendientes`, `sin-georef`) aún disponibles por compatibilidad.
- `frontend/services/orders.ts` no agrega explícitamente header `Authorization`; requiere verificación si endpoint es público o si falta token.
- Varias funciones manejan `error`, pero loading depende de cada pantalla (no estandarizado globalmente).
