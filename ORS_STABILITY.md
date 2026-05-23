# Estabilización ORS y planificación

## Causas típicas de HTTP 500

| Área | Causa | Síntoma |
|------|--------|---------|
| Historial | Columnas `net_operational_clp`, `planning_code` o vista `v_dispatch_plan_invoiced_documents` sin migrar | `GET /distribuidora/dispatch-plans` |
| Dashboard | Vista facturación o margen con costos incompletos | `GET .../dashboard` |
| Mapa ORS | Tabla `ors_plan_route_crew` o `system_config` ausente | `POST .../ors-routes`, `GET .../route-crew` |
| Costos | `trucks.km_per_liter` / `system_config` | Cálculo combustible en 0 o error |
| JSON | `Decimal`, `datetime`, `UUID` sin serializar | 500 al devolver respuesta FastAPI |

## Migraciones requeridas (021–025)

Ejecutar sync/migraciones en orden:

1. `021_dispatch_plan.sql`
2. `022_dispatch_plan_invoiced_view.sql`
3. `023_dispatch_plan_identity.sql`
4. `024_dispatch_plan_picking_snapshots.sql`
5. `025_dispatch_plan_margin_and_snapshot.sql`

Sin ellas el sistema **sigue respondiendo** (listas vacías o fallbacks), pero facturación/margen en historial quedarán en 0.

## Logs de diagnóstico

Buscar en logs del backend:

```
[ORS_STABILITY_DEBUG]
[PLANNING_HISTORY_DEBUG]
```

Incluyen: endpoint, `planning_id`, filas, fase del error.

## Endpoints endurecidos

- `GET /distribuidora/dispatch-plans` → `{"items":[]}` si falla
- `GET /distribuidora/dispatch-plans/by-session/{id}` → `{"items":[]}`
- `GET /distribuidora/dispatch-plans/{id}` → 404 o null seguro
- `GET .../dashboard` → facturación fallback si falta vista
- `GET /distribuidora/planificacion/fuel-config` → diesel default 1200
- `GET /distribuidora/planificacion/crew-config` → tarifas default
- `GET /distribuidora/planificacion/route-crew` → `routes: []`
- `POST /distribuidora/planificacion/ors-routes` → 502 solo si ORS falla; DB parcial no 500

## Serialización

Módulo `backend/utils/json_safe.py` — usado en `dispatch_plan_service` y respuestas ORS.

## Listado liviano vs dashboard

| Endpoint | Payload | Cuándo |
|----------|---------|--------|
| `GET /dispatch-plans?limit=N` | ~8 campos por fila, sin vista facturación | Historial |
| `GET /dispatch-plans/{id}/header` | Cabecera sin `route_geometry` | Detalle (fase 1) |
| `GET /dispatch-plans/{id}/dashboard` | Facturación + margen (sin `invoiced_items` por defecto) | Detalle (fase 2, lazy) |

El listado **ya no** ejecuta `v_dispatch_plan_invoiced_documents` por cada fila (era la causa principal de timeout).

## Frontend (loops / ECONNRESET)

- `frontend/lib/planificacion-fetch.ts` — dedupe `by-session` e historial; logs `[FRONTEND_PLAN_DEBUG]`
- `frontend/lib/fetch-timeout.ts` — timeout 90s (ORS 180s) en fetch críticos
- `planificacion/page.tsx` — bootstrap **una vez** al montar; abort ORS anterior; debounce crew 450ms
- `planificaciones/page.tsx` — una petición concurrente al listado
- Evitar `useEffect` con deps `[fetchRoutes, loadSessionPlans]` (re-disparaba ORS + by-session en loop)

## Frontend

- Historial y detalle: empty states si API vacía o error
- Planificación ORS: captura errores en `fetchRoutes` y `route-crew`
