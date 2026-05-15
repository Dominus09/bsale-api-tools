# FASE 7.8 — Investigación grafo `relateddetailid` (OC tipo 33)

## Objetivo

Validar si existe una **cadena operacional** solo con `GET /documents.json?relateddetailid=`:

`OC (33) → … → OC (33) → … → factura (6) / boleta (1) / NC (9)`

sin usar `references.json`, y sin escribir en `document_related` (solo investigación).

## Script

| | |
|---|---|
| Ruta | `backend/debug/debug_related_graph_oc.py` |
| Lectura | API Bsale + `SELECT` en `distribuidora.documents` |
| Escritura | Ninguna en BD; solo archivo JSON bajo `exports/` |

### Prerrequisitos

- Variables `PG_*` como el resto de jobs.
- `BSALE_TOKEN` o `BSALE_TOKEN_SPA` (y opcionalmente `.env` vía `python-dotenv`).

### Comandos

```bash
# Por número de OC (debe existir en BD como tipo 33, company 3, office 1)
python -m backend.debug.debug_related_graph_oc 66615

# Por document_id Bsale (si no está en BD, se valida tipo 33 solo con API)
python -m backend.debug.debug_related_graph_oc --document-id 3707537
```

### Parámetros fijos (investigación)

- `MAX_DEPTH = 5` (profundidad máxima de expansión de ramas OC 33).
- `visited_document_ids`: solo documentos **tipo 33** entran al conjunto al expandir; si se reintenta el mismo `document_id` 33 → `LOOP_DETECTED`.
- Filtro API: `officeId = 1` (alineado con sync related productivo).

### Salida

1. **Consola:** árbol con `document_id`, `number`, `document_type_id`, `emissionDate`, `totalAmount`, `client`, `detail_id` origen, profundidad y estado de rama (`TERMINAL_SALE`, `CONTINUE_OC_33`, `MAX_DEPTH`, etc.).
2. **JSON:** `exports/debug_related_graph_oc_<número>.json` (si no hay número conocido, usa `document_id` raíz en el nombre de archivo).

Estructura JSON principal: `root_oc`, `nodes`, `edges`, `terminal_documents`, `loops_detected`, `unresolved_branches`, `summary` (incluye `conclusion` en texto A/B/C).

### Conclusiones automáticas (campo `summary.conclusion`)

- **A)** Hay al menos un documento terminal tipo 1, 6 o 9 alcanzado por la cadena.
- **B)** Hay aristas `relateddetailid` pero **ningún** terminal 1/6/9 (p. ej. solo cadenas 33 u otros tipos no finales).
- **C)** No hay aristas (sin respuestas útiles desde `details.json` de la raíz).

---

## Cómo probar (casos sugeridos)

### 1. OC facturada con modificación

**Qué buscar:** OC inicial 33 → una o más OC 33 “hijas” o revisiones → al final factura/boleta/NC (1/6/9).

**Pasos:** identificar en Bsale/negocio un folio conocido con historial de modificación y factura emitida; ejecutar el script con ese número. **Éxito para hipótesis de cadena:** conclusión **A** y árbol que muestra intermedios 33 antes del terminal.

### 2. OC no facturada con modificación

**Qué buscar:** cadena 33→33 sin terminales 1/6/9.

**Éxito para hipótesis:** conclusión **B** o ramas `unresolved_branches` con `unsupported_related_type` / `max_depth` sin terminales.

### 3. OC facturada sin modificación

**Qué buscar:** desde la raíz, `relateddetailid` en líneas que apunten **directo** a 6/1/9 sin pasar por otra 33.

**Éxito:** conclusión **A** con profundidad baja y sin nodos 33 intermedios (o con uno solo según el caso real).

### 4. OC parcialmente facturada

**Qué buscar:** algunas líneas con `relateddetailid` a factura y otras sin relación o con 33/otros tipos.

**Éxito:** `edges` mezclando `TERMINAL_SALE` y `no_related_for_detail` o ramas distintas en `unresolved_branches`; conclusión **A** si al menos una línea llega a 1/6/9.

---

## Límites explícitos

- **No** sustituye al sync productivo: no escribe `document_related`.
- **No** usa `references.json` (FASE 7.8 acotada a `relateddetailid`).
- Profundidad y bucles están acotados para evitar explosión API en grafos raros.

---

## Siguiente paso (fuera de este script)

Si los casos 1–4 confirman que **A** es frecuente cuando el negocio dice “facturada”, se puede valorar lógica futura en `sync_related_service` (no implementada en FASE 7.8).

---

## FASE 7.9 — Análisis masivo por ventana de emisión

Script: `backend/debug/analyze_related_graph_patterns.py` (por defecto OCs 33 con emisión UTC **todo mayo 2026**).

```bash
python -m backend.debug.analyze_related_graph_patterns
python -m backend.debug.analyze_related_graph_patterns --date-from 2026-05-01 --date-to 2026-05-31
```

Salidas (nombres según ventana, por defecto todo mayo → sufijo `2026_05_01_2026_05_31`):

- `exports/related_graph_analysis_<desde>_<hasta>.json`
- `exports/related_graph_analysis_<desde>_<hasta>.xlsx`
- `exports/RELATED_GRAPH_PATTERN_REPORT_<desde>_<hasta>.md`
