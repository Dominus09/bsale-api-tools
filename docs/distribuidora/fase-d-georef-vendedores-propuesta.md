# Propuesta: georreferenciación desde app de vendedores

Estado: **solo arquitectura** (Fase D). No implementar PWA, endpoints de escritura ni sync Bsale.

## Estado actual de coordenadas

Tabla `bsale.clients` (PK `(company_id, bsale_id)`):

| Campo | Uso |
|-------|-----|
| `address`, `municipality`, `city` | Dirección textual |
| `lat`, `lon` | Coordenadas (`double precision`, nullable) |
| `vendedor` | Texto libre de vendedor asignado (sin FK) |
| `updated` | Timestamp Bsale |

No existe hoy:

- Flag `georef_verified`
- Precisión GPS / fuente / usuario que actualizó
- Historial de cambios de coordenadas

Criterios verificables actuales:

1. **Sin dato:** `lat IS NULL OR lon IS NULL`
2. **Inválido / dudoso operativo:** `(0,0)` o fuera del bounding box Chile usado en ORS (`lat` ∉ [-56,-17] o `lon` ∉ [-76,-66])
3. **Presente:** coordenadas en rango → se usan en mapa/ORS

No marcar como “dudosa” una coordenada solo por antigüedad o por comuna: faltan criterios auditables.

Identidad canónica: `(company_id, bsale_id)`. Consolidar pendientes por cliente+dirección; no duplicar por OC.

Pendientes por vendedor (futuro):

```sql
SELECT c.company_id, c.bsale_id, c.vendedor, c.address, c.municipality, c.lat, c.lon
FROM bsale.clients c
WHERE c.company_id = :company_id
  AND (
    c.lat IS NULL OR c.lon IS NULL
    OR (c.lat = 0 AND c.lon = 0)
  )
  AND NULLIF(BTRIM(c.vendedor), '') = :vendedor_label;
```

Hasta normalizar `vendedor` a usuario PWA, la lista será aproximada.

## Flujo futuro propuesto (app.quillotana.cl)

1. Vendedor ve clientes asignados **sin ubicación verificada**.
2. En visita, captura GPS del dispositivo (`accuracy`, timestamp).
3. Envía **propuesta** (no escritura directa a `bsale.clients`):
   - `company_id`, `client_bsale_id`
   - `proposed_lat`, `proposed_lon`, `accuracy_m`
   - `captured_at`, `captured_by_user_id`
   - opcional: foto / nota
4. Tabla sugerida `distribuidora.client_georef_proposals` (nueva, fuera de Bsale):
   - estados: `pending` → `approved` | `rejected`
   - auditoría: actor, motivo, `previous_lat/lon`, `new_lat/lon`
5. Aprobación (supervisor / backoffice) escribe a `bsale.clients` **solo** si:
   - no hay coordenada válida, o
   - override explícito con motivo
6. Nunca sincronizar a Bsale API sin flujo autorizado aparte.

## Relación con planificación

- Mapa/ORS ya excluyen pendientes y etiquetan ruta parcial.
- Al aprobar GPS, el siguiente preview/cálculo ORS toma coords nuevas desde `bsale.clients`.
- Snapshots históricos de despacho **no** se reescriben.

## Fuera de alcance ahora

- Módulo PWA, endpoints write, jobs Bsale, UI de aprobación.
- Alterar crons, predespacho o analytics.
