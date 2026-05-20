# Auditoría: `live_sync_details` y warnings `'details_rows'`

## Síntoma observado

```text
details document_id=3755778: 'details_rows'
attributes document_id=3755778: 'attributes_rows'
references document_id=3755778: 'references_rows'
```

Job rápido (~30 s), sin logs claros de filas insertadas.

## Causa exacta (no era la API)

**No** era que Bsale devolviera otra estructura sin `items`.

El parser en `_refresh_document_children` usa:

```python
det = client.get(f"/documents/{document_id}/details.json")
items = det.get("items") or []
n = replace_document_details(cur, document_id, items)
stats["details_rows"] += n  # ← KeyError si stats no trae la clave
```

`live_sync_details` pasaba un `stats` **sin** las claves `details_rows`, `attributes_rows`, `references_rows`:

```python
stats = {
    "documents_reviewed": 0,
    "details_rows_written": 0,
    "document_errors": 0,
}
```

Secuencia real por documento:

1. `GET /documents/{id}/details.json` — OK  
2. `replace_document_details()` — **DELETE + INSERT** en `distribuidora.document_details` — OK  
3. `stats["details_rows"] += n` — **KeyError: 'details_rows'**  
4. Capturado en `except` interno → log `details document_id=...: 'details_rows'`  
5. Igual para `attributes_rows` y `references_rows` en OC (tipo 33)

Conclusión: **sí se llamaba `replace_document_details`**, pero los contadores fallaban y los warnings parecían fallo de API.

## Qué devuelve Bsale

Formato estándar (validado en dumps y sync existente):

```json
{
  "count": 4,
  "items": [
    { "id": 8791738, "quantity": 30.0, "variant": { "id": 23178 }, ... }
  ]
}
```

Parser: `det.get("items") or []` — correcto.

## Fixes aplicados

### 1. `sync_service._refresh_document_children`

Incremento seguro (evita KeyError en cualquier caller):

```python
stats["details_rows"] = int(stats.get("details_rows") or 0) + n
```

(idem `attributes_rows`, `references_rows`)

### 2. `live_sync_service.live_sync_details`

- Usa `child_stats = _child_sync_stats_template()` al llamar `_refresh_document_children`.
- Acumula `details_rows_written` desde `child_stats["details_rows"]` (retorno de `replace_document_details`).
- Contadores: `details_replace_calls`, `details_api_items_total`.
- Logs `[LIVE_DETAILS_DEBUG]` con `LIVE_DETAILS_DEBUG=1`:
  - `details_api_count`, `parser_key_detected`
  - `rows_written`, `rows_before`, `rows_after`
  - `replace_called=yes`
- Warning explícito si `replace` devuelve 0 líneas y BD sigue en 0.

### 3. Job `live_sync_details.py`

Resumen stdout extra: `details_replace_calls`, `details_rows_written`.

## Cómo validar en Coolify / BD

```bash
LIVE_DETAILS_DEBUG=1 python -m backend.jobs.live_sync_details
```

SQL antes/después (ejemplo OC en ventana):

```sql
SELECT COUNT(*) FROM distribuidora.document_details
WHERE document_id = 3755778;

-- Tras el job, debe coincidir con líneas en API (p. ej. 4)
```

Comparar con:

```sql
SELECT last_success_at, items_processed, error_summary
FROM distribuidora.sync_state
WHERE sync_type = 'details_live' AND mode = 'incremental';
```

`items_processed` = documentos revisados; `details_rows_written` en logs del job = suma de filas insertadas por `replace_document_details`.

## Comportamiento esperado post-fix

| Métrica | Significado |
|---------|-------------|
| `documents_reviewed` | Docs en ventana 24 h (BD) |
| `details_replace_calls` | Llamadas a `_refresh_document_children` |
| `details_rows_written` | Suma de filas insertadas (return de replace) |
| Sin warnings `'details_rows'` | Stats alineados con sync_service |

## Qué NO se cambió

- Backfills mayo  
- `live_sync_related` / `probable_matches`  
- Frontend  
