# Analítica V2 — Plan de limpieza de legacy

**No se ha eliminado nada.** Este plan clasifica módulos según la evidencia de consumidores
(búsqueda en `frontend/` y `backend/`, excluyendo `node_modules`, `.next`, `.md` y logs).
Cada retiro requiere autorización específica, y ninguna tabla se elimina con `DROP` en este plan.

Categorías: **CONSERVAR**, **REEMPLAZAR**, **RETIRAR CUANDO V2 ESTÉ VALIDADA**, **BLOQUEADO POR DEPENDENCIAS**.

## Backend — routers

| Elemento | Evidencia de consumo | Clasificación |
|---|---|---|
| `routers/erp.py` (`/erp/dashboard`, `/erp/alerts`, `/erp/margins`) | Sin consumidores en el frontend | RETIRAR CUANDO V2 ESTÉ VALIDADA (primero en la lista) |
| `routers/margin_problems.py` (`/margin/problems`) | Sin consumidores en el frontend | RETIRAR CUANDO V2 ESTÉ VALIDADA |
| `routers/margins.py` `/margin-analysis`, `/margin-analysis-view` | `getMarginAnalysis` y `getMarginAnalysisView` en `lib/api.ts` sin uso en páginas | RETIRAR CUANDO V2 ESTÉ VALIDADA |
| `routers/margins.py` `/price-list-control` | `/margins` (página) | REEMPLAZAR (Etapa 4) |
| `routers/summary.py` `/margin-summary` | `/dashboard` | REEMPLAZAR (Etapa 5) |
| `routers/alerts.py` `/margin-alerts` | `/dashboard`, `/alerts` | REEMPLAZAR (Etapa 5) |
| `routers/products.py` `/products-without-cost` | `/products-without-cost` | REEMPLAZAR (filtro `reliability=sin_costo` de Costos V2) |
| `routers/margin_rules.py` | `/politica-margenes`; también lo usa `price_list_control_service` | BLOQUEADO POR DEPENDENCIAS → REEMPLAZAR (Etapa 2) |
| `routers/margin_export.py` (`/margin/export`) | Revisar uso desde Excel o exportaciones | BLOQUEADO POR DEPENDENCIAS (confirmar consumidores externos) |
| `routers/cost_analytics.py` `/cost-analytics/v2/*` | `/costos` (control legacy, `lib/costos/control/api.ts`) | REEMPLAZAR (Costos V2) |
| `routers/cost_analytics.py` intelligence, watchlist, variant history, compare offices | Componentes de `/costos` | REEMPLAZAR (Costos V2) |
| `routers/cost_analytics.py` dashboard, offices, history, receptions, products, opportunities, branches, margin-impact, `POST /sync` | Funciones en `lib/api.ts` sin uso en páginas | RETIRAR CUANDO V2 ESTÉ VALIDADA |
| `routers/dashboard.py` (`/dashboard/{company_id}`) | Sin consumidor directo detectado | RETIRAR CUANDO V2 ESTÉ VALIDADA (verificar logs de acceso) |
| `routers/promotions.py` | `/promotions` | CONSERVAR (ya protegido) → REEMPLAZAR en Etapa 3 con migración revisada |
| `routers/offers.py` (`/offers`) | Sin consumidor; `/sucursales/ofertas` usa datos en memoria | BLOQUEADO POR DEPENDENCIAS (decidir junto con la Etapa 3) |
| `routers/analytics_v2.py` | Nuevo | CONSERVAR |

## Backend — servicios y jobs

| Elemento | Clasificación | Nota |
|---|---|---|
| `services/cost_analytics_service.py`, `repositories/cost_analytics_repo.py`, `services/cost_v2_read_service.py`, `services/analytics/cost_*`, `historical_costs.py`, `max_valid_gross_cost.py`, `cost_v2_backfill.py` | RETIRAR CUANDO V2 ESTÉ VALIDADA | El nombre "cost v2" legacy se confunde con Analítica V2; este último vive en `services/analytics_v2/` |
| `jobs/sync_cost_receptions.py`, `services/sync_cost_receptions.py` | BLOQUEADO POR DEPENDENCIAS | Los crons no se tocan; retirar solo con autorización y después de apagar `/costos` legacy |
| `services/price_list_control_service.py` | REEMPLAZAR (Etapa 4) | Lee `margin_rules` y vistas de margen |
| `services/distribuidora/dispatch_commercial_margin_service.py` | CONSERVAR | Distribuidora fuera de alcance |
| `services/bsale_raw/*`, `jobs/bsale_raw/*` | CONSERVAR | Fuente RAW |
| `commercial_analytics*`, `returns_analytics*` | CONSERVAR | Comercial Vendedores y Notas de Crédito no se modifican |

## Tablas y vistas (sin DROP)

| Objeto | Clasificación |
|---|---|
| `bsale_raw.*` | CONSERVAR (solo lectura desde V2) |
| `bsale.margin_rules` (361 reglas) | CONSERVAR; Etapa 2 la reutiliza tal cual |
| `bsale.variant_cost` | CONSERVAR (lo usan Distribuidora y otros servicios) |
| `app.promotions*` | CONSERVAR hasta migrar con revisión (Etapa 3) |
| Tablas de recepciones y costos legacy (`038_cost_analytics_receptions.sql`, `047_cost_reception_calculated.sql`) | BLOQUEADO POR DEPENDENCIAS: dejar de escribir antes de archivar; nunca borrar historia sin autorización |
| Vistas `margin_analysis_*` (`margin_analysis_view.sql`, `046_*`) | RETIRAR CUANDO V2 ESTÉ VALIDADA (después de la Etapa 4) |

## Frontend

| Elemento | Clasificación |
|---|---|
| Modo demo y mocks de `lib/api.ts` | **Retirado en Etapa 0** (falla cerrado) |
| `/costos` y `components/costos/*`, `lib/costos/*` | RETIRAR CUANDO V2 ESTÉ VALIDADA (reemplazo: `/analitica-v2/costos`) |
| `/products-without-cost` | RETIRAR CUANDO V2 ESTÉ VALIDADA |
| `/margins` | REEMPLAZAR (Etapa 4) |
| `/politica-margenes` | REEMPLAZAR (Etapa 2) |
| `/dashboard`, `/alerts` | REEMPLAZAR (Etapa 5) |
| `/promotions` | CONSERVAR → REEMPLAZAR (Etapa 3) |
| `/sucursales/ofertas` (datos en memoria) | BLOQUEADO POR DEPENDENCIAS (definir con Etapa 3) |
| Funciones sin uso en `lib/api.ts` (`getMarginAnalysis`, `getMarginAnalysisView`, `getActivePromotionSnapshot`, `togglePromotion`, `getCostAnalyticsDashboard`, `listCostProducts`, `syncCostAnalytics` y otras) | RETIRAR CUANDO V2 ESTÉ VALIDADA |
| Comercial Vendedores, Notas de Crédito, Distribuidora | CONSERVAR |

## Orden recomendado de retiro (cada paso con autorización)

1. Validar Costos V2 con datos reales (pgAdmin + uso de gerencia) durante al menos un ciclo de compras.
2. Retirar los endpoints sin consumidores (`/erp/*`, `/margin/problems`, funciones muertas de `lib/api.ts`).
3. Ocultar `/costos` y `/products-without-cost` del menú; mantener las rutas un tiempo y revisar logs.
4. Tras las Etapas 2 a 5: reemplazar `/politica-margenes`, `/margins`, `/dashboard` y `/promotions`.
5. Detener los jobs legacy de costos (cambio de cron autorizado) y archivar sus tablas, sin borrar historia.
