# Carga de camiones — diseño futuro (sin implementación)

Documento de arquitectura. **No hay código de optimización aún.** La fuente de datos acordada es `bsale.v_product_logistics` (derivada de `products_master`).

## Principios

1. **Una fuente:** `v_product_logistics` para peso, volumen, CxC y proveedor.
2. **No recalcular picking desde OC** en este módulo; la carga parte de pickings / planes confirmados.
3. **Datos manuales protegidos:** el sync Bsale no pisa `weight_box_kg` ni dimensiones.

## Niveles de madurez

### Nivel 1 — Carga por cajas

**Entrada:** líneas de picking con `cajas` y `units_per_box` desde PM.

**Regla:** `total_cajas = SUM(cajas)` por camión / ruta.

**Límite:** `max_cajas` por tipo de camión (tabla config futura `logistics.truck_types`).

**Salida:** semáforo verde/amarillo/rojo si `total_cajas <= capacidad`.

**Dependencias:** solo `units_per_box` (ya en PM).

---

### Nivel 2 — Carga por peso

**Entrada:** `weight_box_kg`, `units_per_box`, unidades o cajas por línea.

**Fórmulas:**

- `peso_linea_kg = cajas * weight_box_kg`
- `peso_total_kg = SUM(peso_linea_kg)`

**Límite:** `max_payload_kg` del camión.

**Fallback:** si falta `weight_box_kg`, excluir línea del cálculo y marcar `missing_weight` en UI choferes.

---

### Nivel 3 — Carga por volumen

**Entrada:** `height_cm`, `width_cm`, `length_cm` → `volume_m3` por caja (vista).

**Fórmulas:**

- `volume_linea_m3 = cajas * volume_m3`
- `volume_total_m3 = SUM(volume_linea_m3)`

**Límite:** `max_volume_m3` (caja cerrada o carrocería).

**Combinación con Nivel 2:** restricción dual — el camión se llena por el **mínimo** de los dos límites que se alcance primero.

---

### Nivel 4 — Carga por pallets

**Entrada:** reglas de apilamiento (futuro `pallet_rules`: cajas por pallet, max altura, productos incompatibles).

**Heurística inicial:**

- `pallets = CEIL(total_cajas / cajas_por_pallet_estandar)`
- Ajuste por `volume_m3` si pallet estándar tiene dimensiones fijas.

**Salida:** número de pallets + distribución por zona del camión (adelante / atrás).

---

### Nivel 5 — Secuencia óptima de descarga

**Objetivo:** orden de paradas que minimice reordenamiento en bodega móvil (LIFO sobre pallets o zonas).

**Entradas:**

- Secuencia de clientes del plan / rutero.
- Peso y volumen acumulado por parada.
- Restricciones: frágil abajo, refrigerado aislado (flags futuros en PM).

**Algoritmo (borrador):**

1. Agrupar por parada las líneas del picking.
2. Ordenar paradas por **última entrega primero** al cargar (clásico LIFO).
3. Refinar con scoring: `score = w1*peso + w2*volumen + w3*prioridad_cliente`.
4. Validar que ningún tramo intermedio supere límites parciales de eje (opcional).

**Integración App Choferes:** mostrar orden de descarga invertido respecto a carga.

---

## Modelo de datos futuro (propuesta)

```
logistics.truck_types
  id, name, max_cajas, max_payload_kg, max_volume_m3

logistics.truck_load_plans
  id, dispatch_plan_id, truck_type_id, status, computed_at

logistics.truck_load_lines
  plan_id, barcode, cajas, peso_kg, volume_m3, stop_sequence
```

Lectura siempre vía join a `bsale.v_product_logistics`.

## API futura (borrador)

| Método | Ruta | Uso |
|--------|------|-----|
| POST | `/logistics/truck-load/simulate` | Niveles 1–3 sin persistir |
| POST | `/logistics/truck-load/plans` | Persistir plan ligado a `dispatch_plan` |
| GET | `/logistics/truck-load/plans/{id}` | App Choferes + panel Distribuidora |

## Orden de implementación recomendado

1. Nivel 1 (cajas) en panel planificación.
2. Nivel 2 cuando completitud de peso en PM > 70%.
3. Nivel 3 cuando completitud de dimensiones > 50%.
4. Niveles 4–5 con App Choferes en campo.

## Referencias actuales

- `backend/sql/033_products_master_logistics_canonical.sql` — vista `v_product_logistics`
- `GET /products-master/logistics-stats` — KPIs completitud
- `docs/PRODUCTS_MASTER_SYNC.md` — sync Bsale y protección de datos manuales
