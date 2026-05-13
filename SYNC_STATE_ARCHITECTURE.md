# Arquitectura de estado de sync (`distribuidora`)

Este documento describe la **infraestructura base** añadida en FASE 7.3: tabla operacional `sync_state`, helpers en Python y relación con el resto del sistema. No define cron ni jobs automáticos nuevos.

## Tres capas de datos

| Capa | Tabla / vista | Rol |
|------|----------------|-----|
| Historial por corrida | `distribuidora.sync_status` | Append-only: `sync_type`, `last_run`, `records_processed`, `status`. Alimenta `v_sync_status` y métricas “última corrida por dominio”. **No se sustituye.** |
| Cursor incremental legado | `distribuidora.sync_process_cursor` | Una fila por `process_name` (`documents_incremental`, `documents_orders`, …): `last_sync`, `last_status`, `last_message`. Usada por `sync_service` y el panel de órdenes/ventas. **Antes se llamaba `sync_state`.** |
| Estado operacional nuevo | `distribuidora.sync_state` | Una fila por (`sync_type`, `mode`, `office_id`): watermarks, ventanas, overlap, errores. Fuente oficial para **futuros** jobs incremental / backfill (FASE 7.2). |

`sync_logs` no cambia: sigue registrando corridas con contadores y mensajes.

## Tabla `distribuidora.sync_state` (operacional)

- **Clave lógica:** `(sync_type, mode, office_id)` con `UNIQUE`. Ámbito actual: **office 1** (constante en jobs cuando se implementen).
- **`sync_type`:** dominio (`documents`, `details`, `related`, …); texto libre sin `CHECK` rígido para permitir evolución.
- **`mode`:** `incremental` | `backfill` (restricción `CHECK` en SQL).
- **`last_success_at`:** última finalización **exitosa** (UTC).
- **`last_window_from` / `last_window_to`:** ventana de negocio aplicada en esa corrida (p. ej. emisión UTC).
- **`last_watermark`:** cursor estable tras éxito (p. ej. `max(emission_date)` procesado); la próxima corrida incremental parte de `watermark − overlap`.
- **`overlap_seconds` / `overlap_days`:** overlap configurado en esa corrida (reproducibilidad y auditoría); pueden ser `NULL` si solo se usa uno.
- **`status`:** p. ej. `idle`, `success`, `error` (convención en código; no hay `CHECK` en BD).
- **`items_processed`:** contador agregado de la última corrida registrada.
- **`error_summary`:** texto corto en fallo; se limpia en `update_sync_state_success`.
- **`updated_at`:** última escritura.

Migración: `backend/sql/distribuidora/013_operational_sync_state.sql` renombra la tabla antigua `sync_state` → `sync_process_cursor` **solo si** sigue el esquema legado (columna `process_name`), luego crea la nueva `sync_state`.

**Despliegue:** aplicar esta migración (vía `ensure_distribuidora_schema` o ejecutando el SQL) **en el mismo despliegue** que el código que consulta `sync_process_cursor`; de lo contrario `get_last_sync` / el panel fallarán hasta que exista esa tabla.

## Helpers Python

`backend/utils/sync_state.py`:

- `get_sync_state(cur, sync_type=..., mode=..., office_id=1)` — lectura.
- `update_sync_state_success(...)` — `UPSERT` tras OK; limpia `error_summary`; actualiza watermark / ventana / overlap / `items_processed`.
- `update_sync_state_error(...)` — registra error sin borrar `last_success_at` ni watermarks previos.

Todas reciben un **cursor** de psycopg2 para participar en la misma transacción que el job.

Constantes: `MODE_INCREMENTAL`, `MODE_BACKFILL`.

## Conceptos

### Watermark

Valor monótono (típicamente tiempo de emisión o ID máximo) hasta el cual los datos se consideran **confirmados** en BD tras un sync exitoso. El siguiente incremental no debe avanzar el watermark si la corrida aborta a mitad.

### Overlap

Ventana intencional hacia atrás respecto al watermark (`overlap_days` o `overlap_seconds`) para **re-leer** un margen y absorber retrasos de API, correcciones en Bsale o clock skew. Idempotencia viene de **PK / `ON CONFLICT`** en tablas de hechos, no del estado solo.

### Incremental (live)

Modo `incremental`: lecturas acotadas (lookback corto + overlap), frecuencia alta, límites de volumen. El estado en `sync_state` guarda el último watermark y overlap usados.

### Backfill (manual)

Modo `backfill`: rangos explícitos (`last_window_from` / `last_window_to`), cargas mayores, posible chunking. Misma tabla; filas distintas por `(sync_type, 'backfill', office_id)` frente a `(sync_type, 'incremental', office_id)` si conviene separar cursores.

### Checkpoints

Tras cada subventana exitosa (p. ej. un día), el job puede llamar `update_sync_state_success` para persistir `last_window_to` y opcionalmente `last_watermark`. Si el proceso muere, la reanudación lee `get_sync_state` y continúa desde el checkpoint.

## Qué no hace esta fase

- No se programan cron ni Coolify schedules.
- No se conecta `sync_related` incremental ni backfill automático a esta tabla todavía.
- Los dashboards que listan filas por `process_name` siguen leyendo `sync_process_cursor` (misma forma JSON bajo la clave `sync_state` en `get_sync_status_payload` donde aplique).
