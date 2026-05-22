# FASE UI 2 — Rediseño Pre-despacho OC

Documentación del rediseño visual del módulo **Pre-despacho OC** (`/distribuidora/orders`).

**Alcance:** solo UX/UI frontend. Sin cambios en sync, backend, SQL ni reglas de estado operacional.

---

## Mejoras realizadas

### 1. Layout moderno y operacional

- Contenedor ampliado (`max-w-[1400px]`) con espaciado uniforme (`gap-6`).
- Encabezado con contexto («Distribuidora · Operaciones»), título **Pre-despacho OC** y acciones primarias a la derecha.
- Secciones en cards con borde sutil (`border-border/70`, `bg-card`, `shadow-sm`).
- Eliminado el bloque duplicado «Resumen compacto» (repetía datos de «Por comuna»).

### 2. Encabezado operacional (KPI strip)

Cinco tarjetas compactas calculadas en cliente sobre `planningRows`:

| KPI | Fuente |
|-----|--------|
| Total órdenes | `planningRows.length` |
| Total monto | Suma de `total_amount` |
| Pendientes | `resolvePurchaseStatusCode` → `PENDIENTE` |
| Probables | Tiers `PROBABLE_FACTURADA_*` |
| Facturadas | `FACTURADA_CONFIRMADA` |

Componente: `PreDespachoKpiStrip.tsx` · lógica: `pre-despacho-stats.ts`.

### 3. Estados visuales (badges)

Paleta unificada en `purchase-invoice-status.ts`:

| Estado | Color | Etiqueta |
|--------|-------|----------|
| Facturada | Verde (`emerald`) | Facturada |
| Probable | Amarillo (`amber`) | Probable facturada |
| Pendiente | Gris (`slate`) | Pendiente |

Sin emojis en etiquetas; tooltips de negocio intactos.

### 4. Tabla mejorada

- Cabecera **sticky** con blur y sombra inferior.
- Celdas con `py-3`, tipografía jerárquica (OC en mono, montos en negrita).
- Hover suave en filas (`hover:bg-muted/50`).
- Contenedor con altura máxima y scroll (`max-h` ~ 70vh).
- Tabla extraída a `PreDespachoPlanningTable.tsx`.

### 5. Columna prioridad visual

Nueva columna **Prioridad** con heurísticas relativas al lote visible (`pre-despacho-priority.ts`):

| Flag | Criterio (relativo al conjunto cargado) |
|------|----------------------------------------|
| Reciente | OC en percentil 75+ |
| Monto alto | `total_amount` en percentil 75+ |
| Pend. antigua | Pendiente + OC en percentil 25− |

Badges discretos; filas con resalte leve según prioridad principal.

### 6. Filtros rápidos

Chips: **Todas · Pendientes · Probables · Facturadas** con contadores.

- Filtrado **solo en UI** (`filterPlanningRowsByStatus`); mismas llamadas API.
- No altera `only_not_invoiced` ni sync.

Componente: `PreDespachoStatusChips.tsx`.

### 7. UX operacional

| Elemento | Implementación |
|----------|----------------|
| Refresh visual | Botones «Recargar vista» (re-fetch) y «Sync Bsale» (flujo existente) |
| Loading | Spinner en header, fila «Cargando órdenes…» en tabla, KPIs en «—» |
| Empty states | `PreDespachoEmptyState` (sin datos vs. filtro vacío) |
| Logística en plan | Mini-KPIs compactos (camiones, monto en plan, comunas) |

---

## Componentes cambiados

| Archivo | Cambio |
|---------|--------|
| `frontend/app/(dashboard)/distribuidora/orders/page.tsx` | Layout, KPIs, chips, integración tabla nueva |
| `frontend/components/distribuidora/orders/PreDespachoKpiStrip.tsx` | **Nuevo** — strip de KPIs |
| `frontend/components/distribuidora/orders/PreDespachoStatusChips.tsx` | **Nuevo** — filtros rápidos |
| `frontend/components/distribuidora/orders/PreDespachoPlanningTable.tsx` | **Nuevo** — tabla sticky + prioridad |
| `frontend/components/distribuidora/orders/PreDespachoEmptyState.tsx` | **Nuevo** — estados vacíos |
| `frontend/lib/pre-despacho-stats.ts` | **Nuevo** — agregados y filtro UI |
| `frontend/lib/pre-despacho-priority.ts` | **Nuevo** — heurísticas de prioridad |
| `frontend/lib/purchase-invoice-status.ts` | Badges y colores unificados |
| `frontend/components/distribuidora/orders/OrdersTable.tsx` | Sticky header y spacing (lista OC en `/ordenes-compra`) |

---

## UX rationale

1. **Operación diaria** — El usuario entra a validar estado de facturación y asignar camiones; los KPIs y chips responden «¿cuánto hay pendiente?» sin abrir filtros avanzados.
2. **Jerarquía visual** — KPIs arriba, filtros debajo, tabla como superficie principal; comunas/observaciones como contexto lateral, no como dashboard pesado.
3. **Minimalismo industrial** — Bordes finos, fondos `card`/`muted`, sin cards gigantes ni emojis en KPIs logísticos.
4. **Prioridad sin backend** — Percentiles sobre el lote visible destacan excepciones sin nueva columna SQL.
5. **Filtros locales** — Los chips no disparan sync ni cambian parámetros de API; reducen riesgo de regresión en lógica de negocio.

---

## Futuras mejoras posibles

| Mejora | Beneficio |
|--------|-----------|
| `emission_date` en filas de planificación | Prioridad «antigua» por fecha real, no solo número OC |
| Persistir filtro rápido en `sessionStorage` | Retomar vista al volver al módulo |
| Skeleton rows en tabla | Percepción de velocidad en cargas lentas |
| Acciones por fila (abrir OC en Bsale) | Menos cambio de contexto |
| Vista compacta / densidad configurable | Más filas en pantallas de despacho |
| Unificar `/ordenes-compra` y `/orders` en un solo flujo con tabs | Menos duplicidad de pantallas |
| Export CSV del lote filtrado | Cierre operativo y auditoría |
| Indicador de `has_more` / paginación en UI | Cuando el backend devuelve más de `limit` filas |

---

## No modificado (por diseño)

- `getDistribuidoraDispatchPrep*`, `postDistribuidoraSyncOrders`, `waitDistribuidoraTypedSyncComplete`
- Asignación de camiones, `writePlanificacionPayload`, validación georef
- Servicios Python y vistas SQL
- Resolución de `purchase_status` / `estado_real` en backend

---

*Fase UI 2 — mayo 2026*
