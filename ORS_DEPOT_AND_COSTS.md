# FASE ORS 2.0 — Bodega como depot + costo logístico real

Documentación del motor ORS de planificación despacho (`/distribuidora/planificacion`).

---

## Depot (bodega)

Coordenadas fijas (inicio y fin de **cada** ruta por camión):

| | Valor |
|---|--------|
| **Lat** | -43.13147486008401 |
| **Lng** | -73.63921301814756 |

Constantes en `backend/services/distribuidora/planificacion_ors_service.py` (`BODEGA_LAT`, `BODEGA_LNG`).

### Secuencia enviada a ORS Directions

```
BODEGA → [clientes en orden optimizado] → BODEGA
```

Formato coordenadas: `[lon, lat]` (estándar ORS).

`includes_depot_return: true` en cada ruta de respuesta.

---

## Optimización de orden (no arbitrario)

1. Paradas del camión se convierten a «clientes» `{lat, lon, document_id}`.
2. Se aplica **`optimizar_secuencia_cerrado`** (`backend/utils/ruta_optimizador_local.py`):
   - preorden angular desde la bodega
   - 2-opt en tour cerrado  
   Mismo pipeline que el rutero / `_ors_optimize_from_base_clientes` en `distribuidora.py`.
3. Con el orden fijado, **`_ors_route_merge_chunks`** calcula geometría, **km** y **minutos** reales (OpenRouteService driving-car).

El frontend **ya no** define el orden de visita; envía `stops` y recibe `stops_ordered`.

---

## Métricas por ruta y totales

| Métrica | Origen |
|---------|--------|
| `distance_km` | Suma tramos ORS (incluye ida y vuelta a bodega) |
| `duration_min` | Duración ORS (conducción) |
| `liters_estimated` | `distance_km / km_per_liter` del camión |
| `fuel_cost_clp` | `liters_estimated × diesel_price_per_liter` |
| `km_per_liter_used` | Valor del camión usado en el cálculo |

**Totales** (`totals` en respuesta): suma de todas las rutas del request.

---

## Fórmulas combustible

```
litros_estimados = km_totales_ruta / km_per_liter_camión

costo_ruta_CLP = litros_estimados × valor_diesel_CLP_por_litro
```

- `km_per_liter`: columna `distribuidora.trucks.km_per_liter` (valores operativos en `018_trucks_real_consumption.sql`):

| Camión | km/L |
|--------|------|
| Hyundai | 7.0 |
| Hino 2 | 4.2 |
| Hino 3 | 4.2 |
| Hino 4 | 3.5 |
| Hino 5 | 2.8 |
- `fuel_type`: columna `distribuidora.trucks.fuel_type` (hoy `diesel`; extensible).
- `valor_diesel`: **`distribuidora.system_config`** clave `diesel_price_per_liter`, JSON `{"clp": 1200}`.

---

## Persistencia

### `distribuidora.system_config`

Migración: `backend/sql/distribuidora/016_system_config.sql`

| key | value_json ejemplo |
|-----|-------------------|
| `diesel_price_per_liter` | `{"clp": 1200}` |

Servicio: `backend/services/distribuidora/system_config_service.py`

- `get_diesel_price_per_liter()`
- `set_diesel_price_per_liter(clp)` — editable semanal/mensual desde UI

Clave `logistics_cost_settings` (`019_logistics_cost_settings.sql`) — preparada para:

| Módulo | Campo | Estado |
|--------|-------|--------|
| Combustible | `fuel` en `enabled_modules` | Activo |
| Tolerancia consumo | `consumption_tolerance_pct` | Preparado |
| Ferry/barcaza | `ferry_cost_clp` | Preparado |
| Peajes | `toll_cost_clp_per_km` | Preparado |
| Chofer | `driver_cost_clp_per_hour` | Preparado |

Cálculo unificado: `backend/services/distribuidora/logistics_cost_service.py`

### `distribuidora.trucks` (combustible)

Migración: `backend/sql/distribuidora/017_trucks_fuel.sql`

| Columna | Tipo | Notas |
|---------|------|--------|
| `km_per_liter` | NUMERIC(6,2) | Rendimiento km/L |
| `fuel_type` | TEXT | Default `diesel` |

---

## API

### `POST /distribuidora/planificacion/ors-routes`

**Request (nuevo):**

```json
{
  "routes": [
    {
      "camion": "Hino 3 (5600 kg)",
      "truck_id": 1,
      "stops": [
        { "document_id": 123, "lat": -43.1, "lng": -73.6 }
      ]
    }
  ]
}
```

**Response:**

```json
{
  "routes": [{ "camion", "distance_km", "duration_min", "geometry", "stops_ordered", "liters_estimated", "fuel_cost_clp", ... }],
  "depot": { "lat", "lng" },
  "diesel_price_per_liter": 1200,
  "totals": { "distance_km", "duration_min", "liters_estimated", "fuel_cost_clp" }
}
```

### `GET/PUT /distribuidora/planificacion/fuel-config`

Lectura y actualización del precio diesel (CLP/L).

---

## Frontend

| Pieza | Cambio |
|-------|--------|
| `planificacion/page.tsx` | Envía `stops` + `truck_id`; aplica `stops_ordered`; métricas desde `totals` |
| `OrsTopBar` | 5 cards: Km, Clientes, Tiempo, **Litros**, **Costo** |
| `OrsFuelConfigBar` | Edición diesel CLP/L persistida |
| `planificacion-despacho-map-client` | Marker **BD** bodega; fit bounds incluye depot |
| `ors-map-ui.ts` | Lista de visitas según `stops_ordered` |

---

## Qué no se modificó

- Cola `sessionStorage` / flujo pre-despacho → planificación
- Clusters (`buildClusterLabelByDocumentId`) en pre-despacho
- Generación PDF (sin referencias directas en este módulo)
- Endpoint `GET /planificacion/orders` (listado OC)

---

## Mejoras futuras

| Mejora | Beneficio |
|--------|-----------|
| ORS Optimization API (VRP nativo) | Alternativa al 2-opt local para flotas grandes |
| Precio gasoline en `system_config` | Camiones no diesel |
| Tiempo de servicio por parada en `duration_min` total | Minutos «reales» operación + conducción |
| Persistir `stops_ordered` en BD al confirmar ruta | Histórico y PDF con orden oficial |
| Costo por comuna / camión en reporte | Control de gestión |
| Depósito configurable en UI (no solo constante) | Multi-bodega |

---

## Archivos principales

- `backend/services/distribuidora/planificacion_ors_service.py`
- `backend/services/distribuidora/system_config_service.py`
- `backend/routers/distribuidora_planificacion.py`
- `backend/sql/distribuidora/016_system_config.sql`
- `backend/sql/distribuidora/017_trucks_fuel.sql`
- `frontend/app/(dashboard)/distribuidora/planificacion/page.tsx`

---

*Fase ORS 2.0 — mayo 2026*
