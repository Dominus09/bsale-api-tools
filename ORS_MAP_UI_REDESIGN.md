# FASE UI 3 — Rediseño Planificación mapa ORS

Documentación del rediseño visual del módulo **Planif. mapa ORS** (`/distribuidora/planificacion`).

**Alcance:** solo frontend visual. Sin cambios en ORS, backend, cálculos de ruta ni generación PDF.

---

## Layout nuevo

Pantalla tipo **dispatch center** a altura completa (`calc(100dvh - 4rem)`), sin scroll de página; scroll solo en panel izquierdo.

```
┌─────────────────────────────────────────────────────────────┐
│ TOP BAR: título · acciones · KPIs (km, clientes, tiempo,    │
│          combustible est.)                                   │
├──────────────┬──────────────────────────────────────────────┤
│ IZQUIERDA    │ CENTRO — mapa protagonista (flex-1, alto     │
│ Panel ops    │ completo, tiles Carto Voyager, rutas gruesas) │
│ scroll propio│                                               │
│ · filtro     │                                               │
│   camión     │                                               │
│ · km / tiempo│                                               │
│ · lista      │                                               │
│   clientes   │                                               │
│ · obs. info  │                                               │
└──────────────┴──────────────────────────────────────────────┘
```

### Izquierda — panel operacional

| Bloque | Contenido |
|--------|-----------|
| Cabecera | Filtro por camión, km y tiempo (total o por ruta seleccionada) |
| Lista | Cards compactas: nombre, camión (contexto logístico), OC, ETA aproximada |
| Pie | Nota sobre observaciones (gestionadas en pre-despacho) |

Scroll independiente en la lista (`overflow-y-auto`).

### Centro — mapa

- Ocupa todo el espacio restante (`flex-1`, `h-full`).
- Basemap **Carto Voyager** (mejor contraste que OSM estándar).
- Polylines: peso 6, opacidad 0.92, bordes redondeados.
- Markers 26px, borde blanco, sombra; resaltado al seleccionar cliente en lista.

### Top bar — métricas compactas

| Card | Fuente |
|------|--------|
| Km total | Suma `distance_km` de respuesta ORS |
| Clientes | `client_id` únicos en cola |
| Tiempo est. | Suma `duration_min` ORS |
| Combustible est. | `km × 180` CLP/km (`ORS_FUEL_CLP_PER_KM`) — **solo UI** |

---

## Mejoras visuales

1. **Jerarquía dispatch** — Encabezado fijo, KPIs siempre visibles, mapa como superficie principal.
2. **Lista operacional** — Cards con orden de visita, hover y estado activo al clic.
3. **ETA aproximada** — Reparto lineal de `duration_min` entre paradas (`buildOrsVisitRows`); no altera ORS.
4. **Sincronización lista ↔ mapa** — Clic en cliente resalta marker (`highlightedStopKey`).
5. **Loading** — `OrsMapSkeleton` en mapa; skeletons en panel y KPIs.
6. **Empty state** — `OrsDispatchEmptyState` con CTA a pre-despacho OC.
7. **Refresh** — Botón «Recalcular rutas» reutiliza `postDistribuidoraPlanificacionOrsRoutes` existente.

---

## Componentes

| Archivo | Rol |
|---------|-----|
| `frontend/app/(dashboard)/distribuidora/planificacion/page.tsx` | Layout dispatch, estado, integración |
| `frontend/components/distribuidora/planificacion/OrsTopBar.tsx` | KPIs compactos |
| `frontend/components/distribuidora/planificacion/OrsClientPanel.tsx` | Panel izquierdo + lista |
| `frontend/components/distribuidora/planificacion/OrsMapSkeleton.tsx` | Skeleton mapa |
| `frontend/components/distribuidora/planificacion/OrsDispatchEmptyState.tsx` | Sin cola |
| `frontend/components/distribuidora/planificacion-despacho-map-client.tsx` | Mapa Leaflet mejorado |
| `frontend/lib/ors-map-ui.ts` | ETA UI, combustible est., filas de visita |

**Eliminado del layout anterior:** grid de cards sueltas debajo del mapa y «Resumen por camión» en cards grandes (sustituido por panel + filtro camión).

---

## Rationale UX

1. **Módulo crítico** — El operador necesita ver ruta y secuencia sin desplazarse; el mapa grande reduce errores de lectura espacial.
2. **Inspiración fleet/dispatch** — Barra de métricas + lista de paradas + mapa es el patrón de WMS/TMS modernos.
3. **Datos existentes** — No hay ciudad/observaciones en `PlanificacionStoredOrder`; se muestra camión como contexto y texto claro en observaciones.
4. **Combustible estimado** — Referencia rápida para supervisión; constante configurable sin tocar backend.
5. **Sin regresión ORS** — Misma payload y mismo endpoint; solo presentación y heurísticas de UI.

---

## Mejoras futuras posibles

| Mejora | Beneficio |
|--------|-----------|
| Persistir `municipality` / observaciones en cola | Ciudad real y notas en panel |
| ETA por tramo ORS (`legs`) si API expone detalle | Llegada más precisa por parada |
| `flyTo` al seleccionar cliente | Foco automático en mapa |
| Leyenda de colores por camión | Varias rutas simultáneas |
| Panel derecho (detalle OC / monto) | Validación sin salir de pantalla |
| Modo pantalla completa solo mapa | Presentación en monitor de despacho |
| Config combustible (CLP/km, litros) en settings | Estimación alineada a flota |
| Export / PDF desde UI (sin cambiar generador actual) | Entrega al conductor |
| Resumen por camión colapsable bajo lista | Montos por ruta sin saturar top bar |

---

## No modificado (por diseño)

- `postDistribuidoraPlanificacionOrsRoutes` y router `distribuidora_planificacion`
- `route_planning_service` / geometría ORS
- `readPlanificacionPayload` / `writePlanificacionPayload`
- Generación PDF (si existe en otro flujo)

---

*Fase UI 3 — mayo 2026*
