# Hallazgo: camiones Hyundai duplicados en catálogo

Consulta solo lectura (`distribuidora.trucks`, 2026-11-04):

| id | name | plate | max_weight_kg | active |
|----|------|-------|---------------|--------|
| 4 | Hyundai | SRJK-79 | 1500 | true |
| 8 | Hyundai | SRJK79 | 1500 | true |

Son **registros distintos** (PK diferente). La patente es casi idéntica (con/sin guion); el índice único es por `plate` textual, por eso coexisten.

**Acción UX:** los selectores muestran `nombre · patente · #id` cuando hay homónimos. El valor seleccionado es siempre `truck_id`.

**No** se eliminó ni fusionó ningún registro. Limpieza de maestro queda fuera de esta corrección UX (revisión operativa manual si corresponde).
