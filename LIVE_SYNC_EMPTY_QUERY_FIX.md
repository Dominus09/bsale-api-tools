# Fix: `live_sync_documents` — `can't execute an empty query`

## Síntoma

Coolify ejecuta:

```bash
python -m backend.jobs.live_sync_documents
```

y falla con:

```text
can't execute an empty query
```

## Causa exacta

**No era** el watermark ni `sync_state` vacío en la primera ejecución.

El fallo ocurría en `ensure_distribuidora_schema()` → `_run_sql_file()` al aplicar:

`backend/sql/distribuidora/014_document_probable_matches.sql`

Ese archivo terminaba así:

```sql
CREATE INDEX ... idx_document_probable_matches_candidate ...;
-- +go

-- Vistas de estado: ver 015_v_purchase_document_status_full.sql ...
-- +go
```

El runner parte el SQL en bloques separados por líneas que son **solo** `-- +go`.

El bloque entre el penúltimo y el último `+go` quedaba **únicamente**:

```sql
-- Vistas de estado: ver ``015_v_purchase_document_status_full.sql`` (aplicado después de esta tabla).
```

`sync_repo._run_sql_file` hacía:

```python
stmt = chunk.strip()
if not stmt:
    continue
cur.execute(stmt)  # ← psycopg2: can't execute an empty query
```

Para PostgreSQL/psycopg2, un string que solo tiene comentarios SQL **no** es una sentencia ejecutable válida → error *empty query*.

El bloque **después** del último `-- +go` sí estaba vacío y ya se omitía con `if not stmt`.

## Query involucrada

No era un `SELECT` de negocio ni de `sync_state`. Era el fragmento DDL comentado de `014_document_probable_matches.sql` (chunk solo comentarios).

## Por qué aparecía en `live_sync_documents`

`live_sync_documents` llama `ensure_distribuidora_schema(cur)` al inicio (antes de leer `sync_state`). Cualquier job live o backfill que reaplique migraciones dispara el mismo camino.

La primera ejecución **sin fila** en `sync_state` para `documents_live` es válida: `_compute_window` usa `now() - 2h` cuando `state is None`. Eso no causaba el error.

## Fix aplicado

### 1. `backend/repositories/distribuidora/sync_repo.py`

- Nueva función `_sql_chunk_has_executable_sql(stmt)` — ignora líneas vacías y `-- comentarios`.
- `_run_sql_file` solo ejecuta chunks con SQL real.
- Logs opcionales con `LIVE_SYNC_DEBUG=1`:
  - `[LIVE_SYNC_DEBUG] skip empty chunk`
  - `[LIVE_SYNC_DEBUG] skip comment-only chunk`
  - `[LIVE_SYNC_DEBUG] execute file=... idx=...`

### 2. `014_document_probable_matches.sql`

- Eliminado el `-- +go` final que dejaba un chunk solo comentario.
- Comentario de referencia a 015 queda al final del último `CREATE INDEX` (sin partidor extra).

### 3. `live_sync_service.live_sync_documents`

- Log `[LIVE_SYNC_DEBUG]` de ventana, watermark y `emissiondaterange` cuando `LIVE_SYNC_DEBUG=1` (diagnóstico; no cambia lógica).

## Comportamiento primera ejecución

| Paso | Comportamiento |
|------|----------------|
| `get_sync_state(documents_live)` | `None` (sin fila previa) |
| Ventana | `window_from = now() - 2h`, `window_to = now()` |
| Overlap | Aplicado solo si existe `last_watermark` posterior |
| DDL | Todos los `.sql` se aplican sin ejecutar chunks vacíos/comentario |
| Éxito | `update_sync_state_success` crea fila `documents_live` |

## Validación

```bash
# Opcional: ver qué chunks se ejecutan
LIVE_SYNC_DEBUG=1 python -m backend.jobs.live_sync_documents
```

Debe completar sin `can't execute an empty query` y mostrar resumen `LIVE SYNC DOCUMENTS — SUMMARY`.

## Prevención

Cualquier archivo en `backend/sql/distribuidora/*.sql`:

- No poner `-- +go` después de un bloque que sea **solo** comentarios.
- Cada segmento entre marcadores `-- +go` debe contener al menos una sentencia SQL (CREATE, ALTER, DROP, etc.).

El guard en `_run_sql_file` protege el resto del árbol SQL aunque vuelva a colarse un chunk similar.
