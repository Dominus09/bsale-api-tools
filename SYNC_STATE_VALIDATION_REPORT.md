# Informe de validación — FASE 7.3.1 (`sync_state` operacional)

**Alcance:** revisión estática del SQL, del código Python y de los consumidores (API / front). **No** se ejecutaron pruebas contra PostgreSQL en este entorno (sin credenciales de BD en la sesión). Las pruebas de integración deben correr en staging o local con `ensure_distribuidora_schema` aplicado.

---

## 1. Migración `013_operational_sync_state.sql`

| Comprobación | Resultado |
|--------------|-----------|
| **Rename legado** | `DO $rename_legacy$`: solo si existe `distribuidora.sync_state` **con** columna `process_name`, **y** no existe `sync_process_cursor`, entonces `ALTER TABLE ... RENAME TO sync_process_cursor`. Correcto. |
| **Idempotencia (2ª ejecución)** | Tras migración OK, `sync_state` es la tabla **nueva** (sin `process_name`). El bloque `DO` no renombra de nuevo (condición `process_name` falsa). `CREATE TABLE IF NOT EXISTS distribuidora.sync_state` no recrea. **OK.** |
| **Idempotencia (solo tabla nueva)** | Si en un entorno ya existe solo la `sync_state` nueva, el rename no corre; `CREATE IF NOT EXISTS` no altera. **OK.** |
| **`UNIQUE (sync_type, mode, office_id)`** | Implementado como `CONSTRAINT uq_distribuidora_sync_state_op_key UNIQUE (sync_type, mode, office_id)`. Coincide con `ON CONFLICT (sync_type, mode, office_id)` en `backend/utils/sync_state.py`. **OK.** |
| **`CHECK` de `mode`** | `chk_distribuidora_sync_state_mode`: solo `incremental` y `backfill` (minúsculas). Valores distintos fallarán en inserción; convención documentada en `SYNC_STATE_ARCHITECTURE.md`. **OK** con riesgo de typo (ver abajo). |
| **Índices** | `idx_distribuidora_sync_state_office_updated (office_id, updated_at DESC)` y `idx_distribuidora_sync_state_type_mode (sync_type, mode)` con `IF NOT EXISTS`. **OK.** |

### Riesgos detectados (SQL / despliegue)

1. **Orden despliegue código vs DDL:** Si el backend nuevo se despliega **antes** de ejecutar `013`, las consultas a `distribuidora.sync_process_cursor` fallan hasta aplicar la migración (tabla inexistente). Mitigación: desplegar DDL y código juntos o ejecutar `ensure_distribuidora_schema` al arranque del job/API que ya incluye `013`.
2. **BD atípica:** Si alguien crea manualmente la nueva `sync_state` sin haber renombrado la antigua, puede quedar sin `sync_process_cursor` y romper `get_last_sync` / paneles. Mitigación: solo usar el pipeline oficial `001` + … + `013`.
3. **Case-sensitive `mode`:** `Incremental` o `BACKFILL` violan el `CHECK`. Los helpers usan constantes `MODE_INCREMENTAL` / `MODE_BACKFILL`; los jobs futuros deben importarlas o usar exactamente esas cadenas.

---

## 2. `distribuidora.sync_process_cursor` (compatibilidad)

| Área | Verificación |
|------|----------------|
| **Repositorio** | `get_last_sync`, `set_sync_state`, `ensure_sync_state_row` en `backend/repositories/distribuidora/sync_repo.py` leen/escriben `distribuidora.sync_process_cursor`. **OK.** |
| **`distribuidora_sync_status_service`** | `_fetch_sync_state_row` hace `SELECT ... FROM distribuidora.sync_process_cursor`. Endpoint `GET /distribuidora/sync-status` sin cambio de forma de respuesta (`orders`, `sales`, `sync_lock_active`). **OK.** |
| **`orders_service.get_sync_status_payload`** | Lista de cursores desde `sync_process_cursor`; la clave JSON del payload sigue siendo **`sync_state`** (array de filas con `process_name`, `last_sync`, `last_status`, `last_message`, `updated_at`). **Compatible con consumidores que esperaban el mismo nombre de clave.** |
| **`v_sync_status`** | Definición en `007_document_related_sync_status_views.sql`: solo lee `distribuidora.sync_status`. **No tocada por 013.** **OK.** |
| **`sync_logs`** | Sin cambios de esquema ni de consultas en este trabajo. **OK.** |

### Frontend

| Ruta / cliente | Uso |
|----------------|-----|
| `GET /distribuidora/sync-status` | `frontend/lib/api.ts` → `getDistribuidoraSyncStatus` espera `{ orders, sales, sync_lock_active }`. No depende de la clave `sync_state` del payload ERP. **OK.** |
| Otros clientes de `GET .../erp/sync-distribuidora/status` | Reciben `sync_state` (lista desde `sync_process_cursor`) + `last_log` + `sync_lock_active` + `sync_last_by_domain`. Forma de cada elemento de lista **sin cambio** (mismas columnas seleccionadas). **OK** para compatibilidad si el front o scripts parsean esa lista. |

**Conclusión:** el dashboard tipado órdenes/ventas y el ERP status que listan cursores por `process_name` siguen alineados con la tabla renombrada; **no se requieren cambios de frontend** para FASE 7.3.1.

---

## 3. `distribuidora.sync_state` (nueva) — comportamiento esperado

Validación **por diseño de código** (pendiente confirmación en BD):

| Escenario | Comportamiento esperado |
|-----------|-------------------------|
| **Incremental / éxito** | `update_sync_state_success(..., mode=MODE_INCREMENTAL, ...)` → UPSERT; `error_summary` pasa a `NULL`; `last_success_at` actualizado. |
| **Backfill / éxito** | Misma función con `mode=MODE_BACKFILL` y otra fila lógica `(sync_type, 'backfill', office_id)`. |
| **Error** | `update_sync_state_error` → `status` y `error_summary` actualizados; `ON CONFLICT` no borra `last_success_at` ni ventanas previas en el `DO UPDATE` (solo status/error_summary/updated_at, y opcionalmente `items_processed`). |
| **Lectura** | `get_sync_state` devuelve diccionario con todas las columnas o `None` si no hay fila. |

**Pendiente en entorno real:** una transacción de prueba (BEGIN → success → ROLLBACK o COMMIT en BD de test) para validar `CHECK` y `UNIQUE` con datos reales.

---

## 4. `backend/utils/sync_state.py`

| Función | Revisión |
|---------|----------|
| `get_sync_state` | Columnas alineadas con DDL 013 (incl. `BIGINT` → Python `int`). **OK.** |
| `update_sync_state_success` | `ON CONFLICT (sync_type, mode, office_id)` coincide con el `UNIQUE` de la tabla. **OK.** |
| `update_sync_state_error` | Dos ramas (con/sin `items_processed`); `error_summary` truncado a 8000 caracteres. **OK.** |

**Compilación:** `python -m py_compile backend/utils/sync_state.py` ejecutado correctamente en el repo.

---

## 5. `orders_service.get_sync_status_payload` — JSON

- **`sync_state`:** sigue siendo `list[dict]` con las mismas claves por fila (`process_name`, `last_sync`, …). Solo cambia el **origen** de datos (`sync_process_cursor`). **Compatibilidad: OK.**
- **`sync_last_by_domain`:** sigue viniendo de `SELECT * FROM distribuidora.v_sync_status` (basada en `sync_status`). **OK.**
- **Frontend** que solo usa `/distribuidora/sync-status`: no usa este payload completo; **sin cambios necesarios.**

---

## 6. Resumen ejecutivo

| Ítem | Estado |
|------|--------|
| SQL 013 rename + idempotencia + constraints + índices | **OK** (revisión estática) |
| `sync_process_cursor` + servicios + repo | **OK** |
| `sync_status` + `v_sync_status` + `sync_logs` | **OK** (no modificados por 013) |
| Nueva `sync_state` + helpers | **OK** (código); **pendiente** smoke test en PostgreSQL |
| Compatibilidad frontend (`/distribuidora/sync-status`) | **OK** |
| Compatibilidad payload `sync_state` en ERP status | **OK** (misma forma) |

---

## 7. Pendientes antes de jobs incrementales reales

1. **Smoke test en BD:** Tras `ensure_distribuidora_schema`, verificar `SELECT tablename FROM pg_tables WHERE schemaname='distribuidora' AND tablename IN ('sync_state','sync_process_cursor');` y una corrida manual de `get_sync_state` / `update_sync_state_success` / `update_sync_state_error` en transacción.
2. **Monitorizar primer despliegue:** errores `relation "sync_process_cursor" does not exist` indican DDL 013 no aplicado.
3. **Convención `sync_type`:** acordar strings estables (`documents`, `details`, `related`) antes de escribir desde jobs; la tabla no impone `CHECK` en `sync_type` a propósito.
4. **Exponer estado operacional en API (opcional):** hoy la nueva `sync_state` no se expone en `get_sync_status_payload`; cuando haga falta observabilidad, añadir campo aparte (p. ej. `operational_sync_state`) sin reutilizar la clave `sync_state` del legado.

---

**Restricciones respetadas:** no se añadieron jobs, cron, backfill automático ni lógica incremental nueva; solo validación documentada.
