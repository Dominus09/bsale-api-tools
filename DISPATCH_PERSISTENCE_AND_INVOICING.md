# FASE LOGÍSTICA 1.1 — Persistencia, facturación y fix Excel

## Objetivo

Las planificaciones ORS dejan de ser solo una cola temporal en `sessionStorage` y pasan a ser **entidades persistentes y auditables** en base de datos, con historial, dashboard de facturación y snapshot congelado para Excel.

---

## Identidad de planificación

Cada `dispatch_plan` confirmado recibe:

| Campo | Ejemplo | Descripción |
|-------|---------|-------------|
| `planning_code` | `PLAN-00014` | Secuencia `dispatch_plan_code_seq` |
| `planning_name` | Castro Sur | Nombre operativo (input al confirmar) |
| `truck_name` | Hino 3 | Nombre congelado del camión |
| `planning_date` | 2026-05-21 | Fecha operativa |
| `status` | `planned` | Estado del flujo |
| `created_at` / `confirmed_at` | timestamps | Auditoría |

Migración: `023_dispatch_plan_identity.sql`.

---

## Persistencia vs cola temporal

| Antes | Ahora |
|-------|-------|
| `sessionStorage` solo para armar rutas ORS | Cola sigue existiendo para el flujo en vivo |
| Sin historial global | `GET /distribuidora/dispatch-plans` lista planes confirmados |
| Re-abrir imposible | `/distribuidora/planificaciones` y `/planificaciones/[id]` |

**Regla:** Excel, facturación y picking históricos leen **solo** `dispatch_plan_orders`, no vistas vivas de OC.

---

## Snapshot congelado (`dispatch_plan_orders`)

Al confirmar, cada OC guarda:

- `client_name`, `address`, `city`
- `seller_name`
- `payment_method` (forma de pago)
- `document_type_to_generate`
- `oc_total_amount`, `route_order`, georef

### Enriquecimiento al confirmar

`_enrich_orders_snapshot` combina, en este orden:

1. Valores enviados desde el frontend (si vienen informados)
2. `v_orders_purchase` + `v_oc_attributes_flat`
3. `document_attributes` (FORMA DE PAGO, TIPO DE DOCUMENTO A GENERAR)
4. `document_sellers` (primer vendedor)
5. `documents` + `bsale.clients` (dirección, ciudad, nombre)

Esto corrige el bug de Excel vacío (forma pago, tipo doc, vendedor, ciudad, dirección).

### Planes históricos con Excel incompleto

`POST /distribuidora/dispatch-plans/{id}/repair-snapshot` rellena **solo campos vacíos** sin sobrescribir snapshot ya congelado.

---

## Historial frontend

**Ruta:** `/distribuidora/planificaciones`

Tabla con: código, nombre, camión, fecha, estado, total OCs, monto OCs, enlace **Abrir**.

**Detalle:** `/distribuidora/planificaciones/[id]`

- Dashboard facturación
- Costos de ruta y tripulación
- Acciones: Excel, reparar snapshot, revisar facturación, picking

Menú lateral: **Planificaciones**.

---

## Dashboard facturación

**API:** `GET /distribuidora/dispatch-plans/{id}/dashboard`

Métricas:

| Bloque | Fuente |
|--------|--------|
| Total OCs / monto | Suma `dispatch_plan_orders` (snapshot) |
| Confirmadas | `v_dispatch_plan_invoiced_documents` → `status = confirmed` (`document_related`) |
| Probables | `status = probable` (`probable_matches`, score ≥ 60) |
| Pendientes | Sin vínculo confirmado ni probable |

### UX

- Confirmadas — `document_related` real
- Probables — heurística, **no** usada para picking ni margen automático como venta real
- Pendientes — alerta: *OC aún sin documento facturado asociado*

Componente: `DispatchPlanInvoicingDashboard.tsx`.

---

## Margen final camión

Cuando **todas** las OCs están facturadas (sin pendientes) y hay al menos una confirmada:

```
margen_final = ventas_facturadas_reales - costos_ruta
```

- **Ventas reales:** suma `total_amount` de boletas/facturas **confirmadas** (`related_document_id`)
- **Costos ruta:** `dispatch_plan.total_route_cost_clp` (congelado al confirmar)

Campos: `invoiced_sales_clp`, `final_margin_clp`, `margin_calculated_at`.

### Visibilidad por rol

Solo roles: `admin`, `superadmin`, `super_admin`, `administrator`, `finanzas`, `finance`.

Otros usuarios ven: *Margen final oculto para su rol*.

El JWT del panel envía `role` en el token de login; el backend lo lee en el endpoint dashboard.

---

## Fix Excel facturación

**Causa:** el enrich anterior usaba solo `v_orders_purchase` y `setdefault`, que no rellenaba campos vacíos ya presentes como `null`/cadena vacía.

**Solución:**

1. Enrich robusto multi-fuente al confirmar
2. Excel lee exclusivamente columnas de `dispatch_plan_orders`
3. Reparación opcional para planes viejos

Columnas Excel: orden ruta, número orden, forma pago, tipo documento, cliente, total, vendedor, ciudad, dirección, camión, tripulación.

---

## Preparación pickings (sin UI final)

Tabla `dispatch_plan_picking_snapshots`:

- `picking_type`: `client` | `product`
- `payload` JSONB (resultado API al generar)

Endpoints existentes (documentos reales, no OC):

- `GET .../picking-by-client`
- `GET .../picking-by-product`

Al ejecutarlos se persiste snapshot JSON para auditoría. La UI detallada de impresión/listado queda para fase siguiente.

---

## API resumen 1.1

| Método | Ruta |
|--------|------|
| GET | `/distribuidora/dispatch-plans?limit=` |
| GET | `/distribuidora/dispatch-plans/{id}/dashboard` |
| POST | `/distribuidora/dispatch-plans/confirm` (+ `planning_name`) |
| POST | `/distribuidora/dispatch-plans/{id}/repair-snapshot` |

---

## Migraciones

- `023_dispatch_plan_identity.sql`
- `024_dispatch_plan_picking_snapshots.sql`

Ejecutar vía `sync_repo.ensure_distribuidora_schema`.

---

## Qué NO hace esta fase

- No recalcula snapshot histórico desde datos vivos en cada export (salvo repair de vacíos)
- No trata probables como confirmados
- No implementa UI impresa completa de picking (solo API + snapshots)
