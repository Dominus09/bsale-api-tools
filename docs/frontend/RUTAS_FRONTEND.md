# Inventario de rutas frontend

## Resumen

- **Total de rutas/pantallas detectadas:** 33 (`frontend/app/**/page.tsx`).
- **Rutas con redirección/deprecadas:** 2 (`/distribuidora/pendientes`, `/distribuidora/sin-georef`).
- **Layout con guard de autenticación:** `frontend/app/(dashboard)/layout.tsx`.

## Rutas por módulo

| Módulo | Ruta frontend | Archivo | Componente/Página | Descripción | Endpoints consumidos | Auth | Estado |
|---|---|---|---|---|---|---|---|
| Acceso | `/` | `frontend/app/page.tsx`, `frontend/app/home-client.tsx` | `HomePage`, `HomeClient` | Redirección según sesión/empresa | Ninguno directo | Sí (redirect por token) | Activa |
| Acceso | `/login` | `frontend/app/login/page.tsx` | `LoginPage` | Login staff | `POST /login` | Pública | Activa |
| Acceso | `/company-selector` | `frontend/app/company-selector/page.tsx` | `CompanySelectorPage` | Selección de empresa | `GET /companies` | Sí | Activa |
| Dashboard | `/dashboard` | `frontend/app/(dashboard)/dashboard/page.tsx` | `DashboardPage` | KPIs principales | `GET /margin-summary`, `GET /margin-alerts` | Sí | Activa |
| Dashboard | `/alerts` | `frontend/app/(dashboard)/alerts/page.tsx` | `AlertsPage` | Alertas de márgenes | `GET /margin-alerts` | Sí | Activa |
| Dashboard | `/margins` | `frontend/app/(dashboard)/margins/page.tsx` | `MarginsPage` | Análisis de márgenes | `GET /companies`, `GET /price-lists`, `GET /margin-analysis-view` | Sí | Activa |
| Dashboard | `/promotions` | `frontend/app/(dashboard)/promotions/page.tsx` | `PromotionsPage` | Gestión de promociones | `GET /promotions/grid`, `POST /promotions`, `PUT /promotions/{id}/toggle`, `GET /products-master` | Sí | Activa |
| Dashboard | `/products-without-cost` | `frontend/app/(dashboard)/products-without-cost/page.tsx` | `ProductsWithoutCostPage` | Productos sin costo | `GET /products-without-cost` | Sí | Activa |
| Compras | `/compras/generar-oc` | `frontend/app/(dashboard)/compras/generar-oc/page.tsx` | `GenerarOcPage` | Generación de OC | `GET /purchase-analysis`, `POST /purchase-orders/generate-from-lines`, etc. | Sí | Activa |
| Compras | `/compras/registros-oc` | `frontend/app/(dashboard)/compras/registros-oc/page.tsx` | `RegistrosOcPage` | Listado/estado de OCs | `GET /purchase-orders`, `GET /purchase-orders/{id}`, `PATCH /purchase-orders/{id}` | Sí | Activa |
| Compras | `/compras/proveedores` | `frontend/app/(dashboard)/compras/proveedores/page.tsx` | `Page` | CRUD proveedores | `GET /suppliers`, `POST /suppliers`, `PATCH /suppliers/{id}` | Sí | Activa |
| Compras | `/compras/productos` | `frontend/app/(dashboard)/compras/productos/page.tsx` | `Page` | Gestión products master | `GET /products-master`, `PATCH /products-master/{barcode}` | Sí | Activa |
| Distribuidora | `/distribuidora/dashboard` | `frontend/app/(dashboard)/distribuidora/dashboard/page.tsx` | `DistribuidoraCommercialDashboardPage` | Dashboard comercial | `GET /distribuidora/clients/dashboard`, `GET /distribuidora/sync-status`, `POST /distribuidora/sync-sales` | Sí | Activa |
| Distribuidora | `/distribuidora/clientes` | `frontend/app/(dashboard)/distribuidora/clientes/page.tsx` | `DistribuidoraClientesDashboardPage` | Clientes consolidados | `GET /distribuidora/clients`, `GET /distribuidora/clients/frequency` | Sí | Activa |
| Distribuidora | `/distribuidora/clientes/analisis` | `frontend/app/(dashboard)/distribuidora/clientes/analisis/page.tsx` | `DistribuidoraClientesAnalisisPage` | Analítica clientes y export | `GET /distribuidora/clientes/analisis`, `GET /distribuidora/clientes/analisis/export` | Sí | Activa |
| Distribuidora | `/distribuidora/clientes/inactivos` | `frontend/app/(dashboard)/distribuidora/clientes/inactivos/page.tsx` | `DistribuidoraClientesInactivosPage` | Clientes inactivos | `GET /distribuidora/clients/inactive` | Sí | Activa |
| Distribuidora | `/distribuidora/vendedores` | `frontend/app/(dashboard)/distribuidora/vendedores/page.tsx` | `DistribuidoraVendedoresPage` | Resumen por vendedor | `GET /distribuidora/clients/summary/sellers` | Sí | Activa |
| Distribuidora | `/distribuidora/dispatch-analysis` | `frontend/app/(dashboard)/distribuidora/dispatch-analysis/page.tsx` | `DispatchAnalysisPage` | Análisis de despacho | `GET /distribuidora/orders/purchase` | Sí | Activa |
| Distribuidora | `/distribuidora/orders` | `frontend/app/(dashboard)/distribuidora/orders/page.tsx` | `DistribuidoraOrdersPage` | Pre-despacho | `GET /distribuidora/orders/dispatch-prep/*`, `POST /distribuidora/sync-orders` | Sí | Activa |
| Distribuidora | `/distribuidora/pre-planificacion` | `frontend/app/(dashboard)/distribuidora/pre-planificacion/page.tsx` | `PrePlanificacionDespachoPage` | Pre-planificación | `GET /distribuidora/planificacion/orders`, `POST /distribuidora/sync-orders` | Sí | Activa |
| Distribuidora | `/distribuidora/planificacion` | `frontend/app/(dashboard)/distribuidora/planificacion/page.tsx` | `PlanificacionDespachoPage` | ORS routing | `POST /distribuidora/planificacion/ors-routes` | Sí | Activa |
| Distribuidora | `/distribuidora/planning` | `frontend/app/(dashboard)/distribuidora/planning/page.tsx` | `DistribuidoraPlanningPage` | Confirmación de planificación | `GET /distribuidora/orders/purchase/by-document-ids`, `POST /distribuidora/route-planning/batch` | Sí | Activa |
| Distribuidora | `/distribuidora/mapa` | `frontend/app/(dashboard)/distribuidora/mapa/page.tsx` | `MapaPage` | Mapa rutero | `GET /distribuidora/mapa`, `GET /distribuidora/ruta-detalle`, `POST /distribuidora/optimizar-ruta*` | Sí | Activa |
| Distribuidora | `/distribuidora/rutero` | `frontend/app/(dashboard)/distribuidora/rutero/page.tsx` | `RuteroPage` | Gestión rutero | `GET /distribuidora/rutero`, `PATCH /distribuidora/rutero/{id}`, `POST /distribuidora/observacion` | Sí | Activa |
| Distribuidora | `/distribuidora/resumen-vendedor` | `frontend/app/(dashboard)/distribuidora/resumen-vendedor/page.tsx` | `ResumenVendedorPage` | Resumen semanal vendedor | `GET /distribuidora/resumen-vendedor` | Sí | Activa |
| Distribuidora | `/distribuidora/pendientes` | `frontend/app/(dashboard)/distribuidora/pendientes/page.tsx` | `PendientesPage` | Redirección legacy | N/A (redirige) | Sí | Deprecada (redirect) |
| Distribuidora | `/distribuidora/sin-georef` | `frontend/app/(dashboard)/distribuidora/sin-georef/page.tsx` | `SinGeorefPage` | Redirección legacy | N/A (redirige) | Sí | Deprecada (redirect) |
| Operaciones | `/orders` | `frontend/app/(dashboard)/orders/page.tsx` | `OrdersPage` | Lista pedidos | `GET /orders` | Sí | Activa |
| Operaciones | `/orders/[id]` | `frontend/app/(dashboard)/orders/[id]/page.tsx` | `OrderDetailPage` | Detalle pedido | `GET /orders/{id}`, `PUT /orders/{id}/status` | Sí | Activa |
| Sucursales | `/sucursales/recepciones` | `frontend/app/(dashboard)/sucursales/recepciones/page.tsx` | `Page` | Vista sucursal | Requiere verificación | Sí | Activa (sin API clara) |
| Sucursales | `/sucursales/ofertas` | `frontend/app/(dashboard)/sucursales/ofertas/page.tsx` | `Page` | Vista sucursal | Requiere verificación | Sí | Activa (sin API clara) |
| Sucursales | `/sucursales/trazabilidad` | `frontend/app/(dashboard)/sucursales/trazabilidad/page.tsx` | `Page` | Vista sucursal | Requiere verificación | Sí | Activa (sin API clara) |
| Sucursales | `/sucursales/etiquetas` | `frontend/app/(dashboard)/sucursales/etiquetas/page.tsx` | `Page` | Vista sucursal | Requiere verificación | Sí | Activa (sin API clara) |

## Rutas que requieren revisión

- Rutas de sucursales (`/sucursales/*`) por ausencia de consumo API explícito en revisión estática.
- Rutas deprecadas con `permanentRedirect`: `/distribuidora/pendientes`, `/distribuidora/sin-georef`.
- Protección por rol fino: se valida token/empresa en layout, pero no se encontró control por permisos detallados (requiere verificación).
