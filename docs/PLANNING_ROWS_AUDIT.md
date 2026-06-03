# Auditoría `GET .../dispatch-prep/planning-rows`

## Diagnóstico (34 s en un día, limit=500)

Causas típicas en la versión anterior:

| Patrón | Impacto |
|--------|---------|
| `LEFT JOIN v_orders_purchase_status` | Vista con `v_documents_latest` + **LATERAL** por OC |
| `LEFT JOIN LATERAL` probables en fase 2 | **500×** subconsultas a `document_probable_matches` |
| `v_oc_attributes_flat` en enrich | Atributos vía `v_documents_latest` innecesarios en fase 2 |
| Fase 1 con join a vista de status | Filtra “no facturadas” después de materializar status |

`observaciones`, resumen por comuna y KPIs **no** están en este endpoint; el frontend los hace en paralelo o en local.

## SQL actual (optimizado)

1. **Fase `sql_ids`**: `documents` + filtro fecha + `NOT EXISTS` facturación (tablas base) + `LIMIT/OFFSET`.
2. **Fase `sql_enrich`**: `WITH page_ids` + `DISTINCT ON` facturación y probables en **una pasada** + `clients`.

Índices: `028_planning_rows_indexes.sql`, `029_planning_rows_sort_index.sql`.

## Medir en contenedor

```bash
export PLANNING_ROWS_EXPLAIN=true
python audit_planning_rows.py --from 2026-06-02 --to 2026-06-02 --limit 500
```

### Etapas `[PLANNING_ROWS_STAGE]` (activo por defecto)

| stage | Qué mide |
|-------|----------|
| `load_base_orders` | Paginación IDs + (modo staged) filas base `documents` |
| `load_purchase_status` | SQL facturación `document_related` |
| `load_probable_matches` | SQL `document_probable_matches` |
| `load_observaciones` | SQL atributos OBSERVACIONES (diagnóstico; no va en JSON items) |
| `load_georef` | SQL `bsale.clients` |
| `build_rows` | Merge Python + serialización filas |
| `build_summary` | Metadatos `has_more`, `range_days`, warning |
| `serialize_response` | Tamaño JSON respuesta |
| `request_total` | Tiempo total del endpoint |

Modos:

- **Por defecto** (`PLANNING_ROWS_STAGE=true`): consultas **desglosadas** por etapa + ranking en logs.
- **Histórico ~35s** (`PLANNING_ROWS_MONOLITH_ENRICH=1`): un solo SQL enrich; etapas 2–5 aparecen como `monolith_enrich_combined`.

Desactivar logs: `PLANNING_ROWS_STAGE=false`.

Logs adicionales:

- `[DISPATCH_PREP_DEBUG]` — `sql_ids_ms`, `sql_enrich_ms`, `slowest_stage`
- `[PLANNING_ROWS_DEBUG]` — `phase_ranking`, `explain` con Seq Scan / Nested Loop

Respuesta JSON incluye `_perf` si `PLANNING_ROWS_EXPLAIN=true` (solo diagnóstico; quitar en prod si molesta).

## Objetivo &lt; 2 s

Tras deploy + migraciones 028/029, esperar `sql_enrich` &lt; 1 s y `sql_ids` &lt; 500 ms para un día con ~500 filas. Si `sql_ids` sigue alto, revisar EXPLAIN: Seq Scan en `documents` → índice 028 no aplicado.
