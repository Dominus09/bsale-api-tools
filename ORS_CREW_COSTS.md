# ORS — Costos operacionales chofer y peoneta

## Tarifas base (por vuelta)

| Rol | CLP / vuelta |
|-----|----------------|
| Chofer | 50.895 |
| Peoneta | 38.102 |

Configuración en `distribuidora.system_config` → clave `logistics_cost_settings` (migración `020_ors_route_crew_costs.sql`).

## Cálculo

```
costo_personal = (driver_count × driver_cost_clp) + (assistant_count × assistant_cost_clp)
total_ruta = combustible + ferry + peajes + chofer_horario + costo_personal + bonos + …
```

Módulo `crew` en `enabled_modules`. Extensiones preparadas (0 hoy): `bonus`, `per_diem`, `lodging`.

## Persistencia

Tabla `distribuidora.ors_plan_route_crew` (PK `plan_session_id` + `camion`):

- `driver_count`, `assistant_count`
- `driver_cost_clp`, `assistant_cost_clp` (tarifas usadas al guardar)

El `plan_session_id` viaja en `sessionStorage` (`planificacion-despacho-storage.ts`).

## API

| Método | Ruta | Uso |
|--------|------|-----|
| GET | `/distribuidora/planificacion/crew-config` | Tarifas globales |
| PUT | `/distribuidora/planificacion/crew-config` | Actualizar tarifas |
| GET | `/distribuidora/planificacion/route-crew?plan_session_id=` | Dotación guardada |
| PUT | `/distribuidora/planificacion/route-crew` | Guardar dotación |
| POST | `/distribuidora/planificacion/ors-routes` | `plan_session_id` + `driver_count` / `assistant_count` por ruta |

## Frontend

Panel **Ruta operacional** (filtro por camión): selectores choferes / peonetas, subtotal personal, total ruta. Barra superior: KPIs personal y costo total.

## Migración

Ejecutar sync/migraciones hasta incluir `020_ors_route_crew_costs.sql`.
